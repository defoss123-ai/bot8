from __future__ import annotations

from typing import Any

import aiosqlite


class PairManager:
    def __init__(self, db: aiosqlite.Connection, logger) -> None:
        self.db = db
        self.logger = logger
        self.pairs: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        normalized = symbol.strip().upper()
        if not normalized:
            raise ValueError("Symbol must not be empty")
        return normalized

    @staticmethod
    def _validate_settings(
        leverage: int | None = None,
        tp_percent: float | None = None,
        sl_percent: float | None = None,
        cancel_time: int | None = None,
    ) -> None:
        if leverage is not None and not (1 <= int(leverage) <= 125):
            raise ValueError("Leverage must be between 1 and 125")

        if tp_percent is not None:
            tp_percent = float(tp_percent)
            if tp_percent <= 0 or tp_percent > 50:
                raise ValueError("tp_percent must be > 0 and <= 50")

        if sl_percent is not None:
            sl_percent = float(sl_percent)
            if sl_percent <= 0 or sl_percent > 50:
                raise ValueError("sl_percent must be > 0 and <= 50")

        if cancel_time is not None and int(cancel_time) < 0:
            raise ValueError("cancel_time must be >= 0")

    async def load_pairs(self) -> None:
        self.pairs.clear()
        async with self.db.execute(
            "SELECT symbol, enabled, leverage, tp_percent, sl_percent, cancel_time FROM pairs"
        ) as cursor:
            rows = await cursor.fetchall()

        for row in rows:
            symbol = str(row[0]).upper()
            self.pairs[symbol] = {
                "enabled": bool(row[1]),
                "leverage": int(row[2]),
                "tp_percent": float(row[3]),
                "sl_percent": float(row[4]),
                "cancel_time": int(row[5]),
            }

        self.logger.info("Загружено пар из БД: %s", len(self.pairs))

    async def add_pair(
        self,
        symbol: str,
        leverage: int,
        tp_percent: float,
        sl_percent: float,
        cancel_time: int,
        enabled: bool = True,
    ) -> None:
        normalized_symbol = self._normalize_symbol(symbol)
        self._validate_settings(leverage, tp_percent, sl_percent, cancel_time)

        if normalized_symbol in self.pairs:
            self.logger.warning("Пара %s уже существует, выполняется обновление", normalized_symbol)

        await self.db.execute(
            """
            INSERT OR REPLACE INTO pairs
            (symbol, enabled, leverage, tp_percent, sl_percent, cancel_time, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                normalized_symbol,
                1 if enabled else 0,
                int(leverage),
                float(tp_percent),
                float(sl_percent),
                int(cancel_time),
            ),
        )
        await self.db.commit()

        self.pairs[normalized_symbol] = {
            "enabled": bool(enabled),
            "leverage": int(leverage),
            "tp_percent": float(tp_percent),
            "sl_percent": float(sl_percent),
            "cancel_time": int(cancel_time),
        }
        self.logger.info("Пара %s добавлена/обновлена", normalized_symbol)

    async def remove_pair(self, symbol: str) -> None:
        normalized_symbol = self._normalize_symbol(symbol)
        await self.db.execute("DELETE FROM pairs WHERE symbol = ?", (normalized_symbol,))
        await self.db.commit()

        if self.pairs.pop(normalized_symbol, None) is None:
            self.logger.warning("Пара %s не найдена в памяти при удалении", normalized_symbol)
        else:
            self.logger.info("Пара %s удалена", normalized_symbol)

    async def update_pair(self, symbol: str, **kwargs: Any) -> None:
        normalized_symbol = self._normalize_symbol(symbol)
        current = self.pairs.get(normalized_symbol)
        if current is None:
            raise ValueError(f"Pair {normalized_symbol} not found")

        allowed_fields = {"enabled", "leverage", "tp_percent", "sl_percent", "cancel_time"}
        unknown = set(kwargs) - allowed_fields
        if unknown:
            raise ValueError(f"Unknown fields for update: {', '.join(sorted(unknown))}")

        merged = {**current, **kwargs}
        self._validate_settings(
            leverage=merged.get("leverage"),
            tp_percent=merged.get("tp_percent"),
            sl_percent=merged.get("sl_percent"),
            cancel_time=merged.get("cancel_time"),
        )

        await self.db.execute(
            """
            UPDATE pairs
            SET enabled = ?, leverage = ?, tp_percent = ?, sl_percent = ?, cancel_time = ?, updated_at = CURRENT_TIMESTAMP
            WHERE symbol = ?
            """,
            (
                1 if bool(merged["enabled"]) else 0,
                int(merged["leverage"]),
                float(merged["tp_percent"]),
                float(merged["sl_percent"]),
                int(merged["cancel_time"]),
                normalized_symbol,
            ),
        )
        await self.db.commit()

        self.pairs[normalized_symbol] = {
            "enabled": bool(merged["enabled"]),
            "leverage": int(merged["leverage"]),
            "tp_percent": float(merged["tp_percent"]),
            "sl_percent": float(merged["sl_percent"]),
            "cancel_time": int(merged["cancel_time"]),
        }
        self.logger.info("Пара %s обновлена", normalized_symbol)

    async def enable_pair(self, symbol: str) -> None:
        await self.update_pair(symbol, enabled=True)

    async def disable_pair(self, symbol: str) -> None:
        await self.update_pair(symbol, enabled=False)

    def get_active_pairs(self) -> list[str]:
        return [symbol for symbol, settings in self.pairs.items() if settings["enabled"]]

    def get_pair_settings(self, symbol: str) -> dict[str, Any] | None:
        normalized_symbol = self._normalize_symbol(symbol)
        settings = self.pairs.get(normalized_symbol)
        return dict(settings) if settings is not None else None
