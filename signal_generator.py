from __future__ import annotations

from typing import Any


class SignalGenerator:
    def __init__(self, exchange, logger, lookback: int = 20, volume_multiplier: float = 1.5) -> None:
        self.exchange = exchange
        self.logger = logger
        self.lookback = lookback
        self.volume_multiplier = volume_multiplier

    async def fetch_ohlcv(self, symbol: str, limit: int = 100) -> list[list[float]]:
        return await self.exchange.fetch_ohlcv(symbol, timeframe="1m", limit=limit)

    def calculate_indicators(self, ohlcv: list[list[float]]) -> dict[str, Any]:
        recent = ohlcv[-(self.lookback + 1) :]
        current = recent[-1]
        previous = recent[:-1]

        highs = [float(candle[2]) for candle in previous]
        lows = [float(candle[3]) for candle in previous]
        volumes = [float(candle[5]) for candle in previous]
        closes = [float(candle[4]) for candle in previous]

        local_high = max(highs)
        local_low = min(lows)
        average_volume = sum(volumes) / len(volumes)

        momentum = closes[-1] - closes[-3] if len(closes) >= 3 else 0.0

        return {
            "local_high": local_high,
            "local_low": local_low,
            "average_volume": average_volume,
            "momentum": momentum,
            "current_high": float(current[2]),
            "current_low": float(current[3]),
            "current_volume": float(current[5]),
        }

    async def generate_signal(self, symbol: str) -> str | None:
        try:
            ohlcv = await self.fetch_ohlcv(symbol)
            if len(ohlcv) < self.lookback + 5:
                self.logger.warning("Недостаточно данных для %s", symbol)
                return None

            indicators = self.calculate_indicators(ohlcv)

            long_condition = (
                indicators["current_high"] > indicators["local_high"]
                and indicators["current_volume"] > indicators["average_volume"] * self.volume_multiplier
                and indicators["momentum"] > 0
            )
            if long_condition:
                return "LONG"

            short_condition = (
                indicators["current_low"] < indicators["local_low"]
                and indicators["current_volume"] > indicators["average_volume"] * self.volume_multiplier
                and indicators["momentum"] < 0
            )
            if short_condition:
                return "SHORT"

            return None
        except Exception as exc:
            self.logger.error("Ошибка в generate_signal для %s: %s", symbol, exc)
            return None
