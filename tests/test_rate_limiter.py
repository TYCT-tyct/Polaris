import time

import pytest

from polaris.infra.rate_limiter import AsyncTokenBucket


@pytest.mark.asyncio
async def test_rate_limiter_blocks_after_burst() -> None:
    limiter = AsyncTokenBucket(rate_per_sec=2.0, burst=1)
    await limiter.acquire()
    started = time.perf_counter()
    await limiter.acquire()
    elapsed = time.perf_counter() - started
    assert elapsed >= 0.45
