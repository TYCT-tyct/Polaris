from __future__ import annotations

import asyncio
import time


class AsyncTokenBucket:
    def __init__(self, rate_per_sec: float, burst: int) -> None:
        self.rate_per_sec = rate_per_sec
        self.capacity = float(burst)
        self.tokens = float(burst)
        self.updated_at = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                self._refill()
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                deficit = 1.0 - self.tokens
                sleep_for = deficit / self.rate_per_sec
            await asyncio.sleep(max(sleep_for, 0.01))

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.updated_at
        self.updated_at = now
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate_per_sec)

