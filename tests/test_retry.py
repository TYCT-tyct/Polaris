import httpx
import pytest

from polaris.config import RetryConfig
from polaris.infra.retry import with_retry


@pytest.mark.asyncio
async def test_retry_succeeds_after_transient_error() -> None:
    attempts = {"count": 0}

    async def flaky() -> int:
        attempts["count"] += 1
        if attempts["count"] < 3:
            request = httpx.Request("GET", "https://example.com")
            response = httpx.Response(503, request=request)
            raise httpx.HTTPStatusError("temporary", request=request, response=response)
        return 7

    result = await with_retry(flaky, RetryConfig(min_seconds=0.01, max_seconds=0.05, attempts=5))
    assert result == 7
    assert attempts["count"] == 3

