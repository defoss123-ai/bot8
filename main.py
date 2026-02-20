import asyncio
import logging
import os
import signal
import sys
from dataclasses import dataclass, field
from typing import List, Optional

import aiosqlite
import ccxt.async_support as ccxt
from dotenv import load_dotenv

from db_logger import DBLogHandler, DatabaseLogger
from interface import UserInterface
from pair_manager import PairManager
from signal_generator import SignalGenerator
from trader import Trader


LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DB_PATH = "trading_bot.db"
OPERATIONS_LOG_PATH = "operations.log"


@dataclass
class BotContext:
    exchange: ccxt.Exchange
    db: aiosqlite.Connection
    logger: logging.Logger
    running: bool = True
    tasks: List[asyncio.Task] = field(default_factory=list)
    pair_manager: Optional[PairManager] = None
    signal_generator: Optional[SignalGenerator] = None
    trader: Optional[Trader] = None

    async def shutdown(self) -> None:
        self.running = False

        for task in self.tasks:
            if not task.done():
                task.cancel()

        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)

        self.logger.info("Graceful shutdown completed")


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("bot")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if logger.handlers:
        return logger

    formatter = logging.Formatter(LOG_FORMAT)

    file_handler = logging.FileHandler("bot.log", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


async def create_exchange(logger: logging.Logger) -> ccxt.mexc:
    api_key = os.getenv("MEXC_API_KEY")
    api_secret = os.getenv("MEXC_SECRET")

    if not api_key or not api_secret:
        logger.critical("Отсутствуют обязательные переменные окружения: MEXC_API_KEY и/или MEXC_SECRET")
        raise RuntimeError("Missing API credentials")

    exchange = ccxt.mexc(
        {
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
    )

    try:
        await exchange.fetch_balance()
        logger.info("Подключение к MEXC успешно проверено")
        return exchange
    except Exception:
        await exchange.close()
        logger.exception("Ошибка подключения к MEXC")
        raise


async def init_db(logger: logging.Logger) -> aiosqlite.Connection:
    db = await aiosqlite.connect(DB_PATH)

    await db.executescript(
        """
        CREATE TABLE IF NOT EXISTS pairs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT UNIQUE NOT NULL,
            enabled BOOLEAN DEFAULT 1,
            leverage INTEGER DEFAULT 10,
            tp_percent REAL DEFAULT 2.0,
            sl_percent REAL DEFAULT 1.0,
            cancel_time INTEGER DEFAULT 60,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            level TEXT,
            message TEXT
        );

        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT CHECK(side IN ('LONG','SHORT')),
            entry_price REAL,
            quantity REAL,
            status TEXT DEFAULT 'open',
            opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            closed_at TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS orders (
            id TEXT PRIMARY KEY,
            symbol TEXT,
            side TEXT,
            type TEXT,
            price REAL,
            amount REAL,
            status TEXT,
            created_at TIMESTAMP,
            cancel_after INTEGER
        );
        """
    )
    await db.commit()
    logger.info("База данных инициализирована: %s", DB_PATH)
    return db


async def signal_loop(context: BotContext) -> None:
    if not context.pair_manager or not context.signal_generator or not context.trader:
        return

    while context.running:
        try:
            active_pairs = context.pair_manager.get_active_pairs()
            for symbol in active_pairs:
                signal_value = await context.signal_generator.generate_signal(symbol)
                if signal_value and not await context.trader.has_open_position(symbol):
                    settings = context.pair_manager.get_pair_settings(symbol) or {}
                    cancel_after = int(settings.get("cancel_time", 60))
                    await context.trader.place_limit_order(symbol, signal_value, cancel_after)
                await asyncio.sleep(0.5)

            await asyncio.sleep(60)
        except Exception:
            context.logger.exception("Ошибка в signal_loop")
            await asyncio.sleep(5)


async def main() -> None:
    load_dotenv()
    logger = setup_logger()

    try:
        db = await init_db(logger)
    except Exception:
        logger.exception("Ошибка инициализации базы данных")
        return

    exchange = None
    context = None
    try:
        exchange = await create_exchange(logger)
        pair_manager = PairManager(db=db, logger=logger)
        await pair_manager.load_pairs()
        signal_generator = SignalGenerator(exchange=exchange, logger=logger)
        trader = Trader(exchange=exchange, pair_manager=pair_manager, db=db, logger=logger)

        context = BotContext(
            exchange=exchange,
            db=db,
            logger=logger,
            running=True,
            tasks=[],
            pair_manager=pair_manager,
            signal_generator=signal_generator,
            trader=trader,
        )

        def _handle_stop_signal(signum: int, _frame: object = None) -> None:
            logger.info("Получен сигнал остановки: %s", signum)
            if context:
                context.running = False

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _handle_stop_signal, sig)
            except NotImplementedError:
                signal.signal(sig, _handle_stop_signal)

        log_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()
        db_logger = DatabaseLogger(db=db, operations_file=OPERATIONS_LOG_PATH)
        queue_handler = DBLogHandler(log_queue=log_queue, loop=loop)
        queue_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        logger.addHandler(queue_handler)

        async def log_processor() -> None:
            while context.running or not log_queue.empty():
                try:
                    level, message = await asyncio.wait_for(log_queue.get(), timeout=1)
                    await db_logger.log(level, message)
                except asyncio.TimeoutError:
                    continue
                except Exception as exc:
                    logger.error("Ошибка в log_processor: %s", exc)

        ui = UserInterface(pair_manager, trader, logger, context, OPERATIONS_LOG_PATH)

        context.tasks = [
            asyncio.create_task(ui.start()),
            asyncio.create_task(log_processor()),
            asyncio.create_task(trader.monitor_positions(lambda: context.running)),
            asyncio.create_task(signal_loop(context)),
        ]

        await asyncio.gather(*context.tasks, return_exceptions=True)

    except asyncio.CancelledError:
        logger.info("Main task cancelled")
    except Exception as exc:
        logger.critical("Критическая ошибка в main: %s", exc)
    finally:
        if context:
            await context.shutdown()
        if exchange:
            await exchange.close()
        await db.close()
        logger.info("Бот остановлен")


if __name__ == "__main__":
    asyncio.run(main())
