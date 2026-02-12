from __future__ import annotations

import asyncio
import json
import logging
from contextlib import suppress
from datetime import datetime, timezone
from time import monotonic
from typing import Any

import httpx

from polaris.config import RetryConfig
from polaris.infra.rate_limiter import AsyncTokenBucket
from polaris.infra.retry import with_retry
from polaris.sources.models import ClobBook, ClobLevel

logger = logging.getLogger(__name__)


class ClobClient:
    def __init__(
        self,
        limiter: AsyncTokenBucket,
        retry: RetryConfig,
        timeout_seconds: float = 20.0,
        http2_enabled: bool = True,
        max_connections: int = 80,
        max_keepalive_connections: int = 40,
        keepalive_expiry_seconds: float = 30.0,
        ws_enabled: bool = False,
        ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        ws_book_max_age_sec: float = 2.5,
        book_cache_max_age_sec: float = 12.0,
        ws_max_subscribe_tokens: int = 3500,
        ws_reconnect_min_sec: float = 0.5,
        ws_reconnect_max_sec: float = 8.0,
    ) -> None:
        self._limiter = limiter
        self._retry = retry
        self._client = httpx.AsyncClient(
            base_url="https://clob.polymarket.com",
            http2=http2_enabled,
            limits=httpx.Limits(
                max_connections=max(4, max_connections),
                max_keepalive_connections=max(2, max_keepalive_connections),
                keepalive_expiry=max(5.0, keepalive_expiry_seconds),
            ),
            timeout=timeout_seconds,
            headers={"accept": "application/json", "accept-encoding": "gzip"},
            trust_env=False,
        )

        self._ws_enabled = ws_enabled
        self._ws_url = ws_url
        self._ws_book_max_age_sec = max(0.2, ws_book_max_age_sec)
        self._book_cache_max_age_sec = max(self._ws_book_max_age_sec, book_cache_max_age_sec)
        self._ws_max_subscribe_tokens = max(100, ws_max_subscribe_tokens)
        self._ws_reconnect_min_sec = max(0.1, ws_reconnect_min_sec)
        self._ws_reconnect_max_sec = max(self._ws_reconnect_min_sec, ws_reconnect_max_sec)
        self._ws_task: asyncio.Task[None] | None = None
        self._ws_stop = asyncio.Event()
        self._ws_ready = asyncio.Event()
        self._ws_lock = asyncio.Lock()
        self._ws_desired_tokens: set[str] = set()
        self._ws_subscribed_tokens: set[str] = set()
        self._ws_cache: dict[str, tuple[ClobBook, float]] = {}
        self._book_cache: dict[str, tuple[ClobBook, float]] = {}
        self._last_books_metrics: dict[str, int] = {
            "requested": 0,
            "ws_hits": 0,
            "cache_hits": 0,
            "rest_fetch": 0,
            "returned": 0,
        }

    async def close(self) -> None:
        if self._ws_task is not None:
            self._ws_stop.set()
            self._ws_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._ws_task
            self._ws_task = None
        await self._client.aclose()

    async def get_book(self, token_id: str) -> ClobBook:
        book = await self._fetch_book(token_id, allow_missing=False)
        if book is None:
            raise RuntimeError(f"unexpected empty orderbook response for token={token_id}")
        return book

    async def get_book_optional(self, token_id: str) -> ClobBook | None:
        return await self._fetch_book(token_id, allow_missing=True)

    async def get_books(self, token_ids: list[str], batch_size: int = 500, max_concurrency: int = 4) -> list[ClobBook]:
        token_ids = _unique_tokens(token_ids)
        if not token_ids:
            self._last_books_metrics = {"requested": 0, "ws_hits": 0, "cache_hits": 0, "rest_fetch": 0, "returned": 0}
            return []

        ws_books: dict[str, ClobBook] = {}
        if self._ws_enabled and len(token_ids) <= self._ws_max_subscribe_tokens:
            await self._ensure_ws_subscription(token_ids)
            ws_books = await self._get_ws_books(token_ids)

        missing = [token_id for token_id in token_ids if token_id not in ws_books]
        cached_books = await self._get_cached_books(missing)
        missing = [token_id for token_id in missing if token_id not in cached_books]
        rest_books: dict[str, ClobBook] = {}
        if missing:
            batches = [missing[start : start + batch_size] for start in range(0, len(missing), batch_size)]
            semaphore = asyncio.Semaphore(max(1, max_concurrency))

            async def _run(batch: list[str]) -> list[ClobBook]:
                async with semaphore:
                    return await self._get_books_batch_resilient(batch)

            results = await asyncio.gather(*[_run(batch) for batch in batches])
            now = monotonic()
            cache_updates: list[tuple[str, ClobBook, float]] = []
            for rows in results:
                for row in rows:
                    rest_books[row.asset_id] = row
                    cache_updates.append((row.asset_id, row, now))
            if cache_updates:
                async with self._ws_lock:
                    for token_id, book, ts in cache_updates:
                        self._book_cache[token_id] = (book, ts)

        books: list[ClobBook] = []
        for token_id in token_ids:
            book = ws_books.get(token_id) or cached_books.get(token_id) or rest_books.get(token_id)
            if book is not None:
                books.append(book)
        self._last_books_metrics = {
            "requested": len(token_ids),
            "ws_hits": len(ws_books),
            "cache_hits": len(cached_books),
            "rest_fetch": len(missing),
            "returned": len(books),
        }
        return books

    def last_books_metrics(self) -> dict[str, int]:
        return dict(self._last_books_metrics)

    async def get_prices(
        self,
        token_ids: list[str],
        batch_size: int = 500,
        max_concurrency: int = 4,
    ) -> dict[str, tuple[float | None, float | None]]:
        if not token_ids:
            return {}
        batches = [token_ids[start : start + batch_size] for start in range(0, len(token_ids), batch_size)]
        semaphore = asyncio.Semaphore(max(1, max_concurrency))

        async def _run(batch: list[str]) -> dict[str, tuple[float | None, float | None]]:
            async with semaphore:
                return await self._get_prices_batch_resilient(batch)

        chunks = await asyncio.gather(*[_run(batch) for batch in batches], return_exceptions=True)
        merged: dict[str, tuple[float | None, float | None]] = {}
        for item in chunks:
            if isinstance(item, Exception):
                logger.warning("clob prices batch failed", exc_info=item)
                continue
            merged.update(item)
        return merged

    async def _get_books_batch_resilient(self, token_ids: list[str]) -> list[ClobBook]:
        if not token_ids:
            return []
        try:
            return await self._get_books_batch(token_ids)
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code == 404:
                logger.info(
                    "clob /books returned 404, fallback to per-token lookup",
                    extra={"batch_size": len(token_ids)},
                )
                return await self._get_books_batch_fallback(token_ids)
            if status_code in {400, 413}:
                if len(token_ids) == 1:
                    single = await self.get_book_optional(token_ids[0])
                    return [single] if single is not None else []
                middle = max(1, len(token_ids) // 2)
                left, right = await asyncio.gather(
                    self._get_books_batch_resilient(token_ids[:middle]),
                    self._get_books_batch_resilient(token_ids[middle:]),
                )
                return [*left, *right]
            raise

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

    async def _get_prices_batch_resilient(
        self,
        token_ids: list[str],
    ) -> dict[str, tuple[float | None, float | None]]:
        if not token_ids:
            return {}
        try:
            return await self._get_prices_batch(token_ids)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in {400, 404, 413}:
                logger.info(
                    "clob /prices batch failed, fallback to empty",
                    extra={"batch_size": len(token_ids), "status_code": exc.response.status_code},
                )
                return {}
            raise

    async def _get_prices_batch(self, token_ids: list[str]) -> dict[str, tuple[float | None, float | None]]:
        async def _do() -> dict[str, tuple[float | None, float | None]]:
            await self._limiter.acquire()
            payload = [{"token_id": token_id, "side": "BUY"} for token_id in token_ids]
            response = await self._client.post("/prices", json=payload)
            response.raise_for_status()
            return _parse_prices_response(response.json(), token_ids)

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

    async def _ensure_ws_subscription(self, token_ids: list[str]) -> None:
        tokens = set(token_ids[: self._ws_max_subscribe_tokens])
        async with self._ws_lock:
            self._ws_desired_tokens.update(tokens)
            if self._ws_task is None or self._ws_task.done():
                self._ws_stop.clear()
                self._ws_task = asyncio.create_task(self._ws_loop(), name="clob-ws-loop")
        if not self._ws_ready.is_set():
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._ws_ready.wait(), timeout=0.35)

    async def _get_ws_books(self, token_ids: list[str]) -> dict[str, ClobBook]:
        now = monotonic()
        out: dict[str, ClobBook] = {}
        async with self._ws_lock:
            for token_id in token_ids:
                cached = self._ws_cache.get(token_id)
                if cached is None:
                    continue
                book, ts = cached
                if now - ts <= self._ws_book_max_age_sec:
                    out[token_id] = book
        return out

    async def _get_cached_books(self, token_ids: list[str]) -> dict[str, ClobBook]:
        now = monotonic()
        out: dict[str, ClobBook] = {}
        async with self._ws_lock:
            for token_id in token_ids:
                cached = self._book_cache.get(token_id)
                if cached is None:
                    continue
                book, ts = cached
                if now - ts <= self._book_cache_max_age_sec:
                    out[token_id] = book
        return out

    async def _ws_loop(self) -> None:
        backoff = self._ws_reconnect_min_sec
        while not self._ws_stop.is_set():
            desired = await self._ws_desired_snapshot()
            if not desired:
                self._ws_ready.clear()
                await asyncio.sleep(0.5)
                continue
            try:
                import websockets

                async with websockets.connect(
                    self._ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    max_queue=2048,
                ) as websocket:
                    self._ws_ready.set()
                    async with self._ws_lock:
                        self._ws_subscribed_tokens.clear()
                    await self._ws_subscribe_new(websocket)
                    backoff = self._ws_reconnect_min_sec

                    while not self._ws_stop.is_set():
                        await self._ws_subscribe_new(websocket)
                        try:
                            raw = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            continue
                        books = _extract_ws_books(raw)
                        if not books:
                            continue
                        now = monotonic()
                        async with self._ws_lock:
                            for book in books:
                                self._ws_cache[book.asset_id] = (book, now)
                                self._book_cache[book.asset_id] = (book, now)
                            self._prune_ws_cache(now)
            except asyncio.CancelledError:
                raise
            except Exception:
                self._ws_ready.clear()
                logger.warning("clob ws loop disconnected", exc_info=True)
                await asyncio.sleep(backoff)
                backoff = min(self._ws_reconnect_max_sec, backoff * 2)

    async def _ws_desired_snapshot(self) -> list[str]:
        async with self._ws_lock:
            return sorted(self._ws_desired_tokens)[: self._ws_max_subscribe_tokens]

    async def _ws_subscribe_new(self, websocket: Any) -> None:
        async with self._ws_lock:
            pending = sorted(self._ws_desired_tokens - self._ws_subscribed_tokens)
        if not pending:
            return

        chunk_size = 180
        for start in range(0, len(pending), chunk_size):
            batch = pending[start : start + chunk_size]
            payload = {"type": "market", "assets_ids": batch}
            await websocket.send(json.dumps(payload, separators=(",", ":")))
            async with self._ws_lock:
                self._ws_subscribed_tokens.update(batch)

    def _prune_ws_cache(self, now: float) -> None:
        max_age = self._ws_book_max_age_sec * 12
        stale_tokens = [token for token, (_, ts) in self._ws_cache.items() if now - ts > max_age]
        for token in stale_tokens:
            self._ws_cache.pop(token, None)
        cache_max_age = self._book_cache_max_age_sec * 8
        stale_books = [token for token, (_, ts) in self._book_cache.items() if now - ts > cache_max_age]
        for token in stale_books:
            self._book_cache.pop(token, None)

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


def _unique_tokens(token_ids: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for token in token_ids:
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


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


def _parse_prices_response(
    payload: Any,
    token_ids: list[str],
) -> dict[str, tuple[float | None, float | None]]:
    out: dict[str, tuple[float | None, float | None]] = {}
    if isinstance(payload, dict):
        for token_id, item in payload.items():
            if isinstance(item, dict):
                out[str(token_id)] = (_to_float(item.get("SELL")), _to_float(item.get("BUY")))
        return out
    if isinstance(payload, list):
        for index, item in enumerate(payload):
            if not isinstance(item, dict):
                continue
            token_id = item.get("token_id") or item.get("asset_id")
            if not token_id and index < len(token_ids):
                token_id = token_ids[index]
            if not token_id:
                continue
            bid = _to_float(item.get("SELL") or item.get("best_bid"))
            ask = _to_float(item.get("BUY") or item.get("best_ask"))
            out[str(token_id)] = (bid, ask)
    return out


def _extract_ws_books(raw_message: Any) -> list[ClobBook]:
    data: Any = raw_message
    if isinstance(raw_message, (bytes, bytearray)):
        with suppress(Exception):
            data = raw_message.decode("utf-8")
    if isinstance(data, str):
        with suppress(json.JSONDecodeError):
            data = json.loads(data)
    if not isinstance(data, (dict, list)):
        return []

    stack: list[Any] = [data]
    books: list[ClobBook] = []
    while stack:
        item = stack.pop()
        if isinstance(item, list):
            stack.extend(item)
            continue
        if not isinstance(item, dict):
            continue

        parsed = _parse_ws_book(item)
        if parsed is not None:
            books.append(parsed)
            continue

        for key in ("book", "books", "payload", "data", "message"):
            if key in item:
                stack.append(item[key])
    return books


def _parse_ws_book(item: dict[str, Any]) -> ClobBook | None:
    asset_id = (
        item.get("asset_id")
        or item.get("assetId")
        or item.get("token_id")
        or item.get("tokenId")
        or item.get("asset")
    )
    if not isinstance(asset_id, str) or not asset_id:
        return None

    bids = _extract_ws_levels(item, is_bid=True)
    asks = _extract_ws_levels(item, is_bid=False)
    if not bids and not asks:
        return None

    payload = {
        "market": item.get("market") or item.get("market_id") or item.get("condition_id"),
        "asset_id": asset_id,
        "timestamp": str(item.get("timestamp") or item.get("ts") or ""),
        "bids": bids,
        "asks": asks,
        "min_order_size": _to_str(item.get("min_order_size") or item.get("minOrderSize")),
        "tick_size": _to_str(item.get("tick_size") or item.get("tickSize")),
        "neg_risk": _to_bool(item.get("neg_risk") or item.get("negRisk")),
        "last_trade_price": _to_str(item.get("last_trade_price") or item.get("lastTradePrice")),
    }
    with suppress(Exception):
        return ClobBook.model_validate(payload)
    return None


def _extract_ws_levels(item: dict[str, Any], is_bid: bool) -> list[ClobLevel]:
    keys = ("bids", "buy", "buys") if is_bid else ("asks", "sell", "sells")
    raw_levels: Any = None
    for key in keys:
        if key in item:
            raw_levels = item[key]
            break
    if not isinstance(raw_levels, list):
        return []

    out: list[ClobLevel] = []
    for level in raw_levels:
        price: float | None = None
        size: float | None = None
        if isinstance(level, dict):
            price = _to_float(level.get("price") or level.get("p"))
            size = _to_float(level.get("size") or level.get("s") or level.get("quantity"))
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            price = _to_float(level[0])
            size = _to_float(level[1])
        if price is None or size is None or size < 0:
            continue
        out.append(ClobLevel(price=price, size=size))
    return out


def _to_bool(raw: Any) -> bool | None:
    if raw is None:
        return None
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        value = raw.strip().lower()
        if value in {"1", "true", "yes"}:
            return True
        if value in {"0", "false", "no"}:
            return False
    return None


def _to_str(raw: Any) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, str):
        return raw
    return str(raw)


def _to_float(raw: Any) -> float | None:
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None
