from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import httpx

from polaris.config import RetryConfig
from polaris.infra.rate_limiter import AsyncTokenBucket
from polaris.infra.retry import with_retry
from polaris.sources.models import TokenDescriptor


class GammaClient:
    def __init__(
        self,
        limiter: AsyncTokenBucket,
        retry: RetryConfig,
        timeout_seconds: float = 25.0,
    ) -> None:
        self._limiter = limiter
        self._retry = retry
        self._client = httpx.AsyncClient(
            base_url="https://gamma-api.polymarket.com",
            timeout=timeout_seconds,
            headers={"accept": "application/json", "accept-encoding": "gzip"},
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def fetch_markets_page(self, limit: int, offset: int) -> list[dict[str, Any]]:
        params = {
            "limit": str(limit),
            "offset": str(offset),
            "order": "id",
            "ascending": "false",
        }
        return await self._get_json("/markets", params=params)

    async def iter_markets(self, page_size: int = 500, max_pages: int | None = None) -> list[dict[str, Any]]:
        all_rows: list[dict[str, Any]] = []
        offset = 0
        fetched_pages = 0
        current_page_size = max(1, page_size)
        while True:
            if max_pages is not None and max_pages > 0 and fetched_pages >= max_pages:
                break
            try:
                rows = await self.fetch_markets_page(current_page_size, offset)
            except (httpx.RemoteProtocolError, httpx.ReadError):
                # Gamma occasionally closes large chunked responses early.
                # Reducing page size keeps discovery resilient instead of failing a whole cycle.
                if current_page_size <= 10:
                    raise
                current_page_size = max(10, current_page_size // 2)
                continue
            if not rows:
                break
            all_rows.extend(rows)
            fetched_pages += 1
            offset += len(rows)
            if len(rows) < current_page_size:
                break
        return all_rows

    @staticmethod
    def market_record(raw: dict[str, Any]) -> dict[str, Any]:
        market_id = str(raw.get("id"))
        event_id = None
        events = raw.get("events")
        event_slug = raw.get("eventSlug")
        if isinstance(events, list) and events:
            event_id = str(events[0].get("id")) if events[0].get("id") is not None else None
            event_slug = event_slug or events[0].get("slug")
        return {
            "market_id": market_id,
            "gamma_market_id": raw.get("id"),
            "condition_id": raw.get("conditionId"),
            "event_id": event_id,
            "question": raw.get("question") or "",
            "description": raw.get("description") or "",
            "slug": raw.get("slug") or f"market-{market_id}",
            "event_slug": event_slug,
            "category": raw.get("category"),
            "start_date": _parse_ts(raw.get("startDate")),
            "end_date": _parse_ts(raw.get("endDate")),
            "neg_risk": bool(raw.get("negRisk") or False),
            "neg_risk_augmented": bool(raw.get("negRiskOther") or False),
            "active": bool(raw.get("active") or False),
            "closed": bool(raw.get("closed") or False),
            "archived": bool(raw.get("archived") or False),
            "spread": _to_float(raw.get("spread")),
            "liquidity": _to_float(raw.get("liquidityNum") or raw.get("liquidity")),
            "volume": _to_float(raw.get("volumeNum") or raw.get("volume")),
            "resolution_source": raw.get("resolutionSource") or None,
            "updated_from_source_at": _parse_ts(raw.get("updatedAt")),
        }

    @staticmethod
    def event_records(raw: dict[str, Any]) -> list[dict[str, Any]]:
        events = raw.get("events")
        if not isinstance(events, list):
            return []
        rows: list[dict[str, Any]] = []
        for event in events:
            event_id = event.get("id")
            if event_id is None:
                continue
            rows.append(
                {
                    "event_id": str(event_id),
                    "event_slug": event.get("slug"),
                    "event_ticker": event.get("ticker"),
                    "title": event.get("title"),
                    "category": event.get("category"),
                    "active": bool(event.get("active") or False),
                    "closed": bool(event.get("closed") or False),
                    "archived": bool(event.get("archived") or False),
                    "start_date": _parse_ts(event.get("startDate")),
                    "end_date": _parse_ts(event.get("endDate")),
                }
            )
        return rows

    @staticmethod
    def token_descriptors(raw: dict[str, Any]) -> list[TokenDescriptor]:
        market_id = str(raw.get("id"))
        outcomes = _parse_json_list(raw.get("outcomes"))
        token_ids = _parse_json_list(raw.get("clobTokenIds"))
        tick_size = _to_float(raw.get("orderPriceMinTickSize"))
        min_order_size = _to_float(raw.get("orderMinSize"))
        descriptors: list[TokenDescriptor] = []
        for idx, token_id in enumerate(token_ids):
            if not token_id:
                continue
            label = outcomes[idx] if idx < len(outcomes) else f"OUTCOME_{idx}"
            side = "YES" if idx == 0 else "NO"
            normalized = str(label).strip().lower()
            is_other = normalized in {"other", "others", "none of the above", "none"}
            is_placeholder = "placeholder" in normalized or normalized == ""
            descriptors.append(
                TokenDescriptor(
                    token_id=str(token_id),
                    market_id=market_id,
                    outcome_label=str(label),
                    outcome_side=side,
                    tick_size=tick_size,
                    min_order_size=min_order_size,
                    outcome_index=idx,
                    is_other_outcome=is_other,
                    is_placeholder_outcome=is_placeholder,
                )
            )
        return descriptors

    async def _get_json(self, path: str, params: dict[str, str]) -> list[dict[str, Any]]:
        async def _do() -> list[dict[str, Any]]:
            await self._limiter.acquire()
            response = await self._client.get(path, params=params)
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, list):
                return payload
            return []

        return await with_retry(_do, self._retry)


def _parse_json_list(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            return []
    return []


def _parse_ts(raw: Any) -> datetime | None:
    if raw in (None, ""):
        return None
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, str):
        text = raw.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None
    return None


def _to_float(raw: Any) -> float | None:
    if raw in (None, ""):
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None
