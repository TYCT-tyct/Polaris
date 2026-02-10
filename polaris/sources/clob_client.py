from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from polaris.config import RetryConfig
from polaris.infra.rate_limiter import AsyncTokenBucket
from polaris.infra.retry import with_retry
from polaris.sources.models import ClobBook

logger = logging.getLogger(__name__)


class ClobClient:
    def __init__(
        self,
        limiter: AsyncTokenBucket,
        retry: RetryConfig,
        timeout_seconds: float = 20.0,
    ) -> None:
        self._limiter = limiter
        self._retry = retry
        self._client = httpx.AsyncClient(
            base_url="https://clob.polymarket.com",
            timeout=timeout_seconds,
            headers={"accept": "application/json", "accept-encoding": "gzip"},
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def get_book(self, token_id: str) -> ClobBook:
        book = await self._fetch_book(token_id, allow_missing=False)
        if book is None:
            raise RuntimeError(f"unexpected empty orderbook response for token={token_id}")
        return book

    async def get_book_optional(self, token_id: str) -> ClobBook | None:
        return await self._fetch_book(token_id, allow_missing=True)

    async def get_books(self, token_ids: list[str], batch_size: int = 500, max_concurrency: int = 4) -> list[ClobBook]:
        if not token_ids:
            return []
        batches = [token_ids[start : start + batch_size] for start in range(0, len(token_ids), batch_size)]
        semaphore = asyncio.Semaphore(max(1, max_concurrency))

        async def _run(batch: list[str]) -> list[ClobBook]:
            async with semaphore:
                try:
                    return await self._get_books_batch(batch)
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code != 404:
                        raise
                    logger.info(
                        "clob /books returned 404, fallback to per-token lookup",
                        extra={"batch_size": len(batch)},
                    )
                    return await self._get_books_batch_fallback(batch)

        results = await asyncio.gather(*[_run(batch) for batch in batches])
        books: list[ClobBook] = []
        for rows in results:
            books.extend(rows)
        return books

    async def _get_books_batch(self, token_ids: list[str]) -> list[ClobBook]:
        if not token_ids:
            return []

        async def _do() -> list[ClobBook]:
            await self._limiter.acquire()
            payload = [{"token_id": token_id} for token_id in token_ids]
            response = await self._client.post("/books", json=payload)
            response.raise_for_status()
            rows = response.json()
            result: list[ClobBook] = []
            for idx, row in enumerate(rows):
                asset_id = row.get("asset_id") or row.get("token_id")
                if not asset_id and idx < len(token_ids):
                    asset_id = token_ids[idx]
                row["asset_id"] = asset_id or ""
                result.append(ClobBook.model_validate(row))
            return result

        return await with_retry(_do, self._retry)

    async def _get_books_batch_fallback(self, token_ids: list[str], max_concurrency: int = 16) -> list[ClobBook]:
        semaphore = asyncio.Semaphore(max(1, max_concurrency))

        async def _run(token_id: str) -> ClobBook | None:
            async with semaphore:
                return await self.get_book_optional(token_id)

        rows = await asyncio.gather(*[_run(token_id) for token_id in token_ids])
        return [row for row in rows if row is not None]

    async def _fetch_book(self, token_id: str, allow_missing: bool) -> ClobBook | None:
        async def _do() -> ClobBook | None:
            await self._limiter.acquire()
            response = await self._client.get("/book", params={"token_id": token_id})
            if allow_missing and response.status_code == 404:
                return None
            response.raise_for_status()
            payload = response.json()
            payload["asset_id"] = payload.get("asset_id") or token_id
            return ClobBook.model_validate(payload)

        return await with_retry(_do, self._retry)

    @staticmethod
    def timestamp_to_datetime(raw_ts: str | None) -> datetime | None:
        if not raw_ts:
            return None
        try:
            if len(raw_ts) > 10:
                return datetime.fromtimestamp(int(raw_ts) / 1000, tz=timezone.utc)
            return datetime.fromtimestamp(int(raw_ts), tz=timezone.utc)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def best_bid_ask(book: ClobBook) -> tuple[float | None, float | None]:
        best_bid = max((lvl.price for lvl in book.bids), default=None)
        best_ask = min((lvl.price for lvl in book.asks), default=None)
        return best_bid, best_ask

    @staticmethod
    def depth_summary(book: ClobBook, best_bid: float | None, best_ask: float | None) -> dict[str, float]:
        bid_depth = _depth_by_pct(book.bids, best_bid, is_bid=True)
        ask_depth = _depth_by_pct(book.asks, best_ask, is_bid=False)
        bid_5 = bid_depth["5"]
        ask_5 = ask_depth["5"]
        imbalance = (bid_5 - ask_5) / (bid_5 + ask_5) if (bid_5 + ask_5) > 0 else 0.0
        return {
            "bid_depth_1pct": bid_depth["1"],
            "bid_depth_2pct": bid_depth["2"],
            "bid_depth_5pct": bid_5,
            "ask_depth_1pct": ask_depth["1"],
            "ask_depth_2pct": ask_depth["2"],
            "ask_depth_5pct": ask_5,
            "imbalance": imbalance,
        }

    @staticmethod
    def l2_levels(book: ClobBook, max_levels: int = 20) -> list[dict[str, float | int | str]]:
        levels: list[dict[str, float | int | str]] = []
        for idx, level in enumerate(sorted(book.bids, key=lambda x: x.price, reverse=True)[:max_levels]):
            levels.append({"side": "BID", "price": level.price, "size": level.size, "level_index": idx})
        for idx, level in enumerate(sorted(book.asks, key=lambda x: x.price)[:max_levels]):
            levels.append({"side": "ASK", "price": level.price, "size": level.size, "level_index": idx})
        return levels


def _depth_by_pct(levels: list[Any], best: float | None, is_bid: bool) -> dict[str, float]:
    if best is None:
        return {"1": 0.0, "2": 0.0, "5": 0.0}
    totals = {"1": 0.0, "2": 0.0, "5": 0.0}
    for level in levels:
        price = float(level.price)
        size = float(level.size)
        if is_bid:
            pct = ((best - price) / best) * 100 if best > 0 else 999
        else:
            pct = ((price - best) / best) * 100 if best > 0 else 999
        if pct <= 1:
            totals["1"] += size
        if pct <= 2:
            totals["2"] += size
        if pct <= 5:
            totals["5"] += size
    return totals
