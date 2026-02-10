from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Awaitable, Callable

from psycopg.errors import DeadlockDetected

from polaris.config import PolarisSettings
from polaris.db.pool import Database
from polaris.harvest.collector_markets import MarketCollector
from polaris.harvest.collector_quotes import QuoteCollector
from polaris.harvest.collector_tweets import TweetCollector
from polaris.harvest.mapper_market_tracking import MarketTrackingMapper
from polaris.infra.scheduler import AsyncScheduler, TaskSpec
from polaris.ops.health import HealthAggregator

logger = logging.getLogger(__name__)


class HarvestRunner:
    def __init__(
        self,
        settings: PolarisSettings,
        db: Database,
        market_collector: MarketCollector,
        tweet_collector: TweetCollector,
        quote_collector: QuoteCollector,
        mapper: MarketTrackingMapper,
        health: HealthAggregator,
    ) -> None:
        self.settings = settings
        self.db = db
        self.market_collector = market_collector
        self.tweet_collector = tweet_collector
        self.quote_collector = quote_collector
        self.mapper = mapper
        self.health = health
        self._post_failures: dict[str, int] = {}
        self._post_resume_at: dict[str, datetime] = {}

    async def run_once(self, handles: list[str]) -> None:
        await self._run_markets()
        for handle in handles:
            user = await self._run_job("sync_account", "xtracker", lambda h=handle: self.tweet_collector.sync_account(h))
            if user is None:
                continue
            trackings = await self._run_job(
                "sync_trackings", "xtracker", lambda h=handle: self.tweet_collector.sync_trackings(h)
            )
            if trackings is None:
                continue
            await self._run_job(
                "sync_metrics",
                "xtracker",
                lambda u=user, t=trackings: self.tweet_collector.sync_metrics(u, t),
            )
            await self._run_posts(handle, user.account_id)
            await self._run_job("map_market_tracking", "internal", lambda h=handle: self.mapper.remap(h))
        await self._run_quotes_all_once()
        await self._run_job("agg_1m", "internal", self.quote_collector.aggregate_1m)
        await self._run_job("health_agg", "internal", self.health.aggregate_last_minute)

    async def run_forever(self, handles: list[str]) -> None:
        intervals = self.settings.intervals
        tasks: list[TaskSpec] = [
            TaskSpec(name="markets_discovery", interval_sec=intervals.markets_discovery, job=self._run_markets),
            TaskSpec(name="quote_top", interval_sec=intervals.quote_top_sync, job=self._run_quotes_top),
            TaskSpec(name="quote_depth", interval_sec=intervals.quote_depth_sync, job=self._run_quotes_depth),
            TaskSpec(name="agg_1m", interval_sec=intervals.agg_1m, job=lambda: self._run_job("agg_1m", "internal", self.quote_collector.aggregate_1m)),
            TaskSpec(name="health_agg", interval_sec=intervals.health_agg, job=lambda: self._run_job("health_agg", "internal", self.health.aggregate_last_minute)),
            TaskSpec(name="retention", interval_sec=intervals.retention, job=lambda: self._run_job("retention", "internal", lambda: self.quote_collector.prune_raw(self.settings.raw_retention_days))),
        ]
        if self.settings.enable_l2:
            tasks.append(
                TaskSpec(
                    name="quote_l2",
                    interval_sec=intervals.orderbook_l2_sync,
                    job=self._run_quotes_l2,
                )
            )
        for handle in handles:
            tasks.append(
                TaskSpec(
                    name=f"tracking_sync:{handle}",
                    interval_sec=intervals.tracking_sync,
                    job=lambda h=handle: self._sync_handle_tracking_and_metrics(h),
                )
            )
            tasks.append(
                TaskSpec(
                    name=f"post_sync:{handle}",
                    interval_sec=intervals.post_sync,
                    job=lambda h=handle: self._sync_handle_posts(h),
                )
            )
            tasks.append(
                TaskSpec(
                    name=f"mapping_sync:{handle}",
                    interval_sec=intervals.mapping_sync,
                    job=lambda h=handle: self._run_job("map_market_tracking", "internal", lambda: self.mapper.remap(h)),
                )
            )
        scheduler = AsyncScheduler(tasks)
        try:
            await scheduler.run()
        finally:
            await scheduler.stop()

    async def _run_markets(self) -> None:
        await self._run_job("markets_discovery", "gamma", self.market_collector.run_once)

    async def _run_quotes_all_once(self) -> None:
        tokens = await self.market_collector.list_active_tokens()
        if not tokens:
            return
        await self._run_job(
            "quote_collection_once",
            "clob",
            lambda: self.quote_collector.collect_top_depth_l2(tokens),
        )

    async def _run_quotes_top(self) -> None:
        tokens = await self.market_collector.list_active_tokens()
        if not tokens:
            return
        await self._run_job("quote_top_sync", "clob", lambda: self.quote_collector.collect_top_only(tokens))

    async def _run_quotes_depth(self) -> None:
        tokens = await self.market_collector.list_active_tokens()
        if not tokens:
            return
        await self._run_job("quote_depth_sync", "clob", lambda: self.quote_collector.collect_depth_only(tokens))

    async def _run_quotes_l2(self) -> None:
        tokens = await self.market_collector.list_active_tokens()
        if not tokens:
            return
        await self._run_job("quote_l2_sync", "clob", lambda: self.quote_collector.collect_l2_only(tokens))

    async def _sync_handle_tracking_and_metrics(self, handle: str) -> None:
        user = await self._run_job("sync_account", "xtracker", lambda: self.tweet_collector.sync_account(handle))
        if user is None:
            return
        trackings = await self._run_job("sync_trackings", "xtracker", lambda: self.tweet_collector.sync_trackings(handle))
        if trackings is None:
            return
        await self._run_job(
            "sync_metrics",
            "xtracker",
            lambda u=user, t=trackings: self.tweet_collector.sync_metrics(u, t),
        )

    async def _sync_handle_posts(self, handle: str) -> None:
        user = await self.tweet_collector.sync_account(handle)
        await self._run_posts(handle, user.account_id)

    async def _run_posts(self, handle: str, account_id: str) -> None:
        now = datetime.now(tz=UTC)
        resume_at = self._post_resume_at.get(handle)
        if resume_at and now < resume_at:
            return
        result = await self._run_job(
            "sync_posts",
            "xtracker",
            lambda: self.tweet_collector.sync_posts_incremental(handle, account_id),
        )
        if result is None:
            failures = self._post_failures.get(handle, 0) + 1
            self._post_failures[handle] = failures
            if failures >= self.settings.post_fail_backoff_threshold:
                self._post_resume_at[handle] = now + timedelta(seconds=self.settings.post_fail_backoff_interval)
        else:
            self._post_failures[handle] = 0
            self._post_resume_at.pop(handle, None)

    async def _run_job(
        self,
        job_name: str,
        source: str,
        fn: Callable[[], Awaitable[object]],
    ) -> object | None:
        started = datetime.now(tz=UTC)
        start_perf = time.perf_counter()
        status = "ok"
        rows_written = 0
        status_code = None
        error_code = None
        error_message = None
        metadata = {}
        result = None
        deadlock_retries = 0
        max_deadlock_retries = 5
        try:
            while True:
                try:
                    result = await fn()
                    rows_written = _infer_rows(result)
                    metadata = {"result_type": type(result).__name__, "deadlock_retries": deadlock_retries}
                    return result
                except DeadlockDetected as exc:
                    deadlock_retries += 1
                    if deadlock_retries > max_deadlock_retries:
                        status = "error"
                        error_code = type(exc).__name__
                        error_message = str(exc)
                        metadata = {"deadlock_retries": deadlock_retries}
                        logger.exception(
                            "collector job failed after deadlock retries",
                            extra={"job_name": job_name, "source": source, "deadlock_retries": deadlock_retries},
                        )
                        return None
                    logger.warning(
                        "collector job deadlock, retrying",
                        extra={"job_name": job_name, "source": source, "deadlock_retries": deadlock_retries},
                    )
                    sleep_seconds = min(5.0, 0.5 * (2 ** (deadlock_retries - 1)))
                    await asyncio.sleep(sleep_seconds)
                except Exception as exc:
                    status = "error"
                    error_code = type(exc).__name__
                    error_message = str(exc)
                    metadata = {"deadlock_retries": deadlock_retries}
                    logger.exception("collector job failed", extra={"job_name": job_name, "source": source})
                    return None
        finally:
            elapsed = int((time.perf_counter() - start_perf) * 1000)
            finished = datetime.now(tz=UTC)
            await self.db.execute(
                """
                insert into ops_collector_run(
                    job_name, source, status, status_code, rows_written, latency_ms, error_code,
                    error_message, started_at, finished_at, metadata
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    job_name,
                    source,
                    status,
                    status_code,
                    rows_written,
                    elapsed,
                    error_code,
                    error_message,
                    started,
                    finished,
                    _to_json(metadata),
                ),
            )


def _infer_rows(result: object) -> int:
    if result is None:
        return 0
    if isinstance(result, int):
        return result
    if isinstance(result, tuple):
        numeric = [x for x in result if isinstance(x, int)]
        return sum(numeric)
    if isinstance(result, dict):
        numeric = [int(v) for v in result.values() if isinstance(v, int)]
        return sum(numeric)
    return 1


def _to_json(payload: dict) -> str:
    import json

    return json.dumps(payload, ensure_ascii=True)
