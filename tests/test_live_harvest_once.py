from __future__ import annotations

import httpx
import pytest

from polaris.config import PolarisSettings
from polaris.harvest.collector_markets import MarketCollector
from polaris.harvest.collector_quotes import QuoteCollector
from polaris.harvest.collector_tweets import TweetCollector
from polaris.harvest.mapper_market_tracking import MarketTrackingMapper
from polaris.harvest.runner import HarvestRunner
from polaris.infra.rate_limiter import AsyncTokenBucket
from polaris.ops.health import HealthAggregator
from polaris.sources.clob_client import ClobClient
from polaris.sources.gamma_client import GammaClient
from polaris.sources.xtracker_client import XTrackerClient


@pytest.mark.asyncio
@pytest.mark.live
async def test_live_harvest_once_e2e(db, postgres_dsn: str) -> None:
    await db.execute(
        """
        truncate table
            bridge_market_tracking,
            fact_orderbook_l2_raw,
            fact_quote_depth_raw,
            fact_quote_top_raw,
            fact_quote_1m,
            fact_market_state_snapshot,
            dim_token,
            dim_market,
            fact_tweet_post,
            fact_tweet_metric_daily,
            dim_tracking_window,
            dim_account,
            ops_collector_run,
            ops_api_health_minute,
            ops_cursor
        cascade
        """
    )
    settings = PolarisSettings(
        database_url=postgres_dsn,
        market_discovery_scope="all",
        gamma_page_size=200,
        gamma_max_pages=2,
        enable_l2=False,
    )
    try:
        async with httpx.AsyncClient(timeout=8.0) as probe:
            response = await probe.get("https://xtracker.polymarket.com/api/users/elonmusk")
            response.raise_for_status()
    except httpx.HTTPError:
        pytest.skip("live xtracker unavailable in current network")
    xtracker = XTrackerClient(AsyncTokenBucket(settings.xtracker_rate, settings.xtracker_burst), settings.retry)
    gamma = GammaClient(AsyncTokenBucket(settings.gamma_rate, settings.gamma_burst), settings.retry)
    clob = ClobClient(AsyncTokenBucket(settings.clob_rate, settings.clob_burst), settings.retry)
    market_collector = MarketCollector(
        db,
        gamma,
        market_scope=settings.market_discovery_scope,
        market_state=settings.market_discovery_state,
        market_tweet_targets=settings.market_tweet_targets,
        gamma_page_size=settings.gamma_page_size,
        gamma_max_pages=settings.gamma_max_pages,
    )
    tweet_collector = TweetCollector(db, xtracker)
    quote_collector = QuoteCollector(db, clob, enable_l2=settings.enable_l2)
    mapper = MarketTrackingMapper(db)
    health = HealthAggregator(db)
    runner = HarvestRunner(settings, db, market_collector, tweet_collector, quote_collector, mapper, health)
    try:
        await runner.run_once(["elonmusk"])
    finally:
        await xtracker.close()
        await gamma.close()
        await clob.close()

    market_count = await db.fetch_one("select count(*) as c from dim_market")
    token_count = await db.fetch_one("select count(*) as c from dim_token")
    post_count = await db.fetch_one("select count(*) as c from fact_tweet_post")
    quote_count = await db.fetch_one("select count(*) as c from fact_quote_top_raw")
    assert int(market_count["c"]) > 0
    assert int(token_count["c"]) > 0
    assert int(post_count["c"]) > 0
    assert int(quote_count["c"]) > 0
