import asyncio
import logging
from datetime import datetime
from pathlib import Path

import aiosqlite


class DatabaseLogger:
    def __init__(self, db: aiosqlite.Connection, operations_file: str = "operations.log") -> None:
        self.db = db
        self.operations_file = Path(operations_file)
        self._file_lock = asyncio.Lock()

    async def log(self, level: str, message: str) -> None:
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        await self.db.execute(
            "INSERT INTO logs (level, message) VALUES (?, ?)",
            (level, message),
        )
        await self.db.commit()

        line = f"{timestamp} | {level} | {message}\n"
        async with self._file_lock:
            with self.operations_file.open("a", encoding="utf-8") as file:
                file.write(line)

    async def get_recent(self, limit: int = 50) -> list[tuple[str, str, str]]:
        async with self.db.execute(
            "SELECT timestamp, level, message FROM logs ORDER BY id DESC LIMIT ?",
            (limit,),
        ) as cursor:
            rows = await cursor.fetchall()
        return list(rows)


class DBLogHandler(logging.Handler):
    def __init__(self, log_queue: asyncio.Queue[tuple[str, str]], loop: asyncio.AbstractEventLoop) -> None:
        super().__init__()
        self.log_queue = log_queue
        self.loop = loop

    def emit(self, record: logging.LogRecord) -> None:
        try:
            payload = (record.levelname, self.format(record))
            self.loop.call_soon_threadsafe(self.log_queue.put_nowait, payload)
        except Exception:
            self.handleError(record)
