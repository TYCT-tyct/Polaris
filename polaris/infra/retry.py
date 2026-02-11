from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

import httpx
from tenacity import AsyncRetrying, RetryError, retry_if_exception, stop_after_attempt, wait_random_exponential

from polaris.config import RetryConfig

T = TypeVar("T")


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TimeoutException):
        return True
    if isinstance(exc, httpx.NetworkError):
        return True
    if isinstance(exc, httpx.RemoteProtocolError):
        return True
    if isinstance(exc, httpx.ReadError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in {429, 500, 502, 503, 504}
    return False


async def with_retry(fn: Callable[[], Any], config: RetryConfig) -> T:
    try:
        async for attempt in AsyncRetrying(
            retry=retry_if_exception(_is_retryable),
            stop=stop_after_attempt(config.attempts),
            wait=wait_random_exponential(multiplier=config.min_seconds, max=config.max_seconds),
            reraise=True,
        ):
            with attempt:
                return await fn()
    except RetryError as exc:
        raise exc.last_attempt.exception() from exc
