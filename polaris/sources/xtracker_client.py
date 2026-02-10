from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any

import httpx

from polaris.config import RetryConfig
from polaris.infra.rate_limiter import AsyncTokenBucket
from polaris.infra.retry import with_retry
from polaris.sources.models import XTrackerMetricPoint, XTrackerPost, XTrackerTracking, XTrackerUser


class XTrackerClient:
    def __init__(
        self,
        limiter: AsyncTokenBucket,
        retry: RetryConfig,
        timeout_seconds: float = 20.0,
    ) -> None:
        self._limiter = limiter
        self._retry = retry
        self._client = httpx.AsyncClient(
            base_url="https://xtracker.polymarket.com",
            timeout=timeout_seconds,
            headers={"accept": "application/json", "accept-encoding": "gzip"},
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def get_user(self, handle: str) -> XTrackerUser:
        payload = await self._get_json(f"/api/users/{handle}")
        return XTrackerUser.from_payload(payload["data"])

    async def get_trackings(self, handle: str, active_only: bool = True) -> list[XTrackerTracking]:
        params = {"activeOnly": str(active_only).lower()}
        payload = await self._get_json(f"/api/users/{handle}/trackings", params=params)
        return [XTrackerTracking.model_validate(item) for item in payload.get("data", [])]

    async def get_metrics(
        self,
        user_id: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[XTrackerMetricPoint]:
        params = {
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
        }
        payload = await self._get_json(f"/api/metrics/{user_id}", params=params)
        return [XTrackerMetricPoint.from_payload(item) for item in payload.get("data", [])]

    async def get_posts(self, handle: str) -> tuple[str, list[XTrackerPost], dict[str, Any]]:
        raw, parsed = await self._get_raw_json(f"/api/users/{handle}/posts")
        posts = [XTrackerPost.model_validate(item) for item in parsed.get("data", [])]
        hash_value = hashlib.sha256(raw).hexdigest()
        return hash_value, posts, parsed

    async def _get_json(self, path: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        _, parsed = await self._get_raw_json(path, params=params)
        return parsed

    async def _get_raw_json(
        self,
        path: str,
        params: dict[str, str] | None = None,
    ) -> tuple[bytes, dict[str, Any]]:
        async def _do() -> tuple[bytes, dict[str, Any]]:
            await self._limiter.acquire()
            response = await self._client.get(path, params=params)
            response.raise_for_status()
            return response.content, response.json()

        return await with_retry(_do, self._retry)

