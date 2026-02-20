from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Callable

import aiosqlite

from pair_manager import PairManager


class Trader:
    def __init__(self, exchange, pair_manager: PairManager, db: aiosqlite.Connection, logger) -> None:
        self.exchange = exchange
        self.pair_manager = pair_manager
        self.db = db
        self.logger = logger
        self.active_orders: dict[str, dict[str, Any]] = {}
        self.position_tracker: dict[str, dict[str, Any]] = {}

    async def calculate_quantity(self, symbol: str, side: str, leverage: int) -> float:
        balance = await self.exchange.fetch_balance()
        usdt_info = balance.get("USDT", {}) if isinstance(balance, dict) else {}
        free_usdt = float(usdt_info.get("free", 0) or 0)

        if free_usdt <= 0:
            raise ValueError("Недостаточно свободного баланса USDT")

        # Безопасный лимит риска: не более 5% свободного баланса на сделку.
        max_margin = free_usdt * 0.05

        ticker = await self.exchange.fetch_ticker(symbol)
        current_price = float(ticker.get("last") or ticker.get("bid") or ticker.get("ask") or 0)
        if current_price <= 0:
            raise ValueError(f"Не удалось получить валидную цену для {symbol}")

        raw_quantity = (max_margin * leverage) / current_price
        if raw_quantity <= 0:
            raise ValueError(f"Некорректный размер позиции для {symbol} {side}")

        quantity = float(self.exchange.amount_to_precision(symbol, raw_quantity))
        if quantity <= 0:
            raise ValueError(f"Количество после округления равно 0 для {symbol}")

        return quantity

    async def calculate_position_size(self, symbol: str, side: str, leverage: int) -> float:
        return await self.calculate_quantity(symbol, side, leverage)

    async def get_limit_price(self, symbol: str, side: str) -> tuple[float, str]:
        order_book = await self.exchange.fetch_order_book(symbol)
        bid = float(order_book["bids"][0][0]) if order_book.get("bids") else 0.0
        ask = float(order_book["asks"][0][0]) if order_book.get("asks") else 0.0
        if bid <= 0 or ask <= 0:
            raise ValueError(f"Пустой стакан для {symbol}")

        normalized_side = side.upper()
        if normalized_side == "LONG":
            limit_price = bid * 1.001
            ccxt_side = "buy"
        elif normalized_side == "SHORT":
            limit_price = ask * 0.999
            ccxt_side = "sell"
        else:
            raise ValueError("side должен быть LONG или SHORT")

        return float(self.exchange.price_to_precision(symbol, limit_price)), ccxt_side

    async def place_limit_order(self, symbol: str, side: str, cancel_after: int) -> str:
        normalized_side = side.upper()
        if normalized_side not in {"LONG", "SHORT"}:
            raise ValueError("side должен быть LONG или SHORT")

        settings = self.pair_manager.get_pair_settings(symbol)
        if not settings:
            raise ValueError(f"Настройки для {symbol} не найдены")

        existing_for_symbol = [
            oid
            for oid, order_data in self.active_orders.items()
            if order_data.get("symbol") == symbol and order_data.get("status") == "open" and order_data.get("role") == "entry"
        ]
        if existing_for_symbol:
            self.logger.warning("Для %s уже есть активный входной ордер, новый не создается", symbol)
            return existing_for_symbol[0]

        leverage = int(settings["leverage"])
        quantity = await self.calculate_position_size(symbol, normalized_side, leverage)
        limit_price, ccxt_side = await self.get_limit_price(symbol, normalized_side)

        order = await self.exchange.create_limit_order(symbol, ccxt_side, quantity, limit_price)
        order_id = order["id"]
        created_at = datetime.utcnow().isoformat()

        self.active_orders[order_id] = {
            "symbol": symbol,
            "side": normalized_side,
            "ccxt_side": ccxt_side,
            "price": limit_price,
            "quantity": quantity,
            "status": "open",
            "created_at": created_at,
            "cancel_after": cancel_after,
            "role": "entry",
        }

        await self.db.execute(
            """
            INSERT OR REPLACE INTO orders (id, symbol, side, type, price, amount, status, created_at, cancel_after)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (order_id, symbol, normalized_side, "limit", limit_price, quantity, "open", created_at, int(cancel_after)),
        )
        await self.db.commit()

        self.logger.info("Ордер %s (%s %s) выставлен: qty=%s price=%s", order_id, symbol, normalized_side, quantity, limit_price)

        if cancel_after > 0:
            asyncio.create_task(self._auto_cancel(order_id, cancel_after))

        return order_id

    async def _auto_cancel(self, order_id: str, delay: int) -> None:
        await asyncio.sleep(delay)

        order_data = self.active_orders.get(order_id)
        if not order_data or order_data.get("status") != "open":
            return

        symbol = order_data["symbol"]
        try:
            exchange_order = await self.exchange.fetch_order(order_id, symbol)
            status = (exchange_order.get("status") or "").lower()

            if status in {"open", "new"}:
                await self.exchange.cancel_order(order_id, symbol)
                self.logger.info("Ордер %s отменен по таймеру (%s сек)", order_id, delay)
                new_status = "canceled"
            else:
                new_status = status or "closed"

            await self.db.execute("UPDATE orders SET status = ? WHERE id = ?", (new_status, order_id))
            await self.db.commit()
        except Exception as exc:
            self.logger.warning("Не удалось авто-отменить ордер %s: %s", order_id, exc)
        finally:
            self.active_orders.pop(order_id, None)

    async def set_stop_loss_take_profit(
        self,
        symbol: str,
        side: str,
        entry_price: float,
        quantity: float,
        sl_percent: float,
        tp_percent: float,
    ) -> tuple[str, str]:
        normalized_side = side.upper()
        if normalized_side not in {"LONG", "SHORT"}:
            raise ValueError("side должен быть LONG или SHORT")

        if normalized_side == "LONG":
            sl_price = entry_price * (1 - sl_percent / 100)
            tp_price = entry_price * (1 + tp_percent / 100)
            exit_side = "sell"
        else:
            sl_price = entry_price * (1 + sl_percent / 100)
            tp_price = entry_price * (1 - tp_percent / 100)
            exit_side = "buy"

        sl_price = float(self.exchange.price_to_precision(symbol, sl_price))
        tp_price = float(self.exchange.price_to_precision(symbol, tp_price))

        sl_order = await self.exchange.create_order(
            symbol,
            "stopMarket",
            exit_side,
            quantity,
            None,
            {"stopPrice": sl_price},
        )
        tp_order = await self.exchange.create_order(symbol, "limit", exit_side, quantity, tp_price)

        self.active_orders[sl_order["id"]] = {
            "symbol": symbol,
            "side": normalized_side,
            "ccxt_side": exit_side,
            "price": sl_price,
            "quantity": quantity,
            "status": "open",
            "created_at": datetime.utcnow().isoformat(),
            "cancel_after": 0,
            "role": "stop_loss",
        }
        self.active_orders[tp_order["id"]] = {
            "symbol": symbol,
            "side": normalized_side,
            "ccxt_side": exit_side,
            "price": tp_price,
            "quantity": quantity,
            "status": "open",
            "created_at": datetime.utcnow().isoformat(),
            "cancel_after": 0,
            "role": "take_profit",
        }

        self.logger.info("Установлены SL/TP для %s: SL=%s TP=%s", symbol, sl_price, tp_price)
        return sl_order["id"], tp_order["id"]

    async def monitor_positions(
        self,
        running_check: Callable[[], bool] | None = None,
        interval_seconds: int = 5,
    ) -> None:
        await self.check_positions_and_orders(running_check=running_check, interval_seconds=interval_seconds)

    async def check_positions_and_orders(
        self,
        running_check: Callable[[], bool] | None = None,
        interval_seconds: int = 5,
    ) -> None:
        while True:
            if running_check is not None and not running_check():
                break

            try:
                positions = await self.exchange.fetch_positions()
                open_symbols: set[str] = set()

                for pos in positions:
                    contracts = float(pos.get("contracts") or pos.get("positionAmt") or 0)
                    if contracts == 0:
                        continue

                    symbol = pos.get("symbol")
                    if not symbol:
                        continue

                    side = "LONG" if contracts > 0 else "SHORT"
                    entry_price = float(pos.get("entryPrice") or pos.get("markPrice") or 0)
                    quantity = abs(contracts)
                    open_symbols.add(symbol)

                    self.position_tracker[symbol] = {
                        "side": side,
                        "entry_price": entry_price,
                        "quantity": quantity,
                        "status": "open",
                    }

                    await self.db.execute(
                        """
                        INSERT INTO positions (symbol, side, entry_price, quantity, status)
                        VALUES (?, ?, ?, ?, 'open')
                        """,
                        (symbol, side, entry_price, quantity),
                    )

                for tracked_symbol in list(self.position_tracker.keys()):
                    if tracked_symbol not in open_symbols:
                        self.position_tracker.pop(tracked_symbol, None)
                        await self.db.execute(
                            "UPDATE positions SET status = 'closed', closed_at = CURRENT_TIMESTAMP WHERE symbol = ? AND status = 'open'",
                            (tracked_symbol,),
                        )

                await self.db.commit()
            except Exception as exc:
                self.logger.error("Ошибка в check_positions_and_orders: %s", exc)

            await asyncio.sleep(interval_seconds)


    def get_open_positions(self) -> dict[str, dict[str, Any]]:
        return dict(self.position_tracker)


    async def has_open_position(self, symbol: str) -> bool:
        if symbol in self.position_tracker:
            return True

        async with self.db.execute(
            "SELECT 1 FROM positions WHERE symbol = ? AND status = 'open' LIMIT 1",
            (symbol,),
        ) as cursor:
            row = await cursor.fetchone()
        return row is not None

    async def cancel_all_orders(self, symbol: str | None = None) -> None:
        order_ids = list(self.active_orders.keys())
        for order_id in order_ids:
            order_data = self.active_orders.get(order_id)
            if not order_data:
                continue

            if symbol is not None and order_data.get("symbol") != symbol:
                continue

            try:
                await self.exchange.cancel_order(order_id, order_data["symbol"])
                await self.db.execute("UPDATE orders SET status = 'canceled' WHERE id = ?", (order_id,))
                await self.db.commit()
                self.logger.info("Ордер %s отменен вручную", order_id)
            except Exception as exc:
                self.logger.warning("Не удалось отменить ордер %s: %s", order_id, exc)
            finally:
                self.active_orders.pop(order_id, None)
