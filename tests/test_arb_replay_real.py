from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from polaris.arb.ai.gate import AiGate
from polaris.arb.config import arb_config_from_settings
from polaris.arb.orchestrator import ArbOrchestrator
from polaris.config import PolarisSettings, RetryConfig
from polaris.harvest.collector_markets import MarketCollector
from polaris.harvest.collector_quotes import QuoteCollector
from polaris.infra.rate_limiter import AsyncTokenBucket
from polaris.sources.clob_client import ClobClient
from polaris.sources.gamma_client import GammaClient


@pytest.mark.asyncio
@pytest.mark.live
async def test_arb_replay_uses_real_history(postgres_dsn: str, db) -> None:
    settings = PolarisSettings(database_url=postgres_dsn, market_discovery_scope="all")
    gamma = GammaClient(AsyncTokenBucket(settings.gamma_rate, settings.gamma_burst), RetryConfig())
    clob = ClobClient(AsyncTokenBucket(settings.clob_rate, settings.clob_burst), RetryConfig())
    collector = MarketCollector(db, gamma, market_scope="all")
    quote_collector = QuoteCollector(db, clob, enable_l2=False)
    try:
        _, _ = await collector.run_once()
        tokens = await collector.list_active_tokens()
        assert tokens, "no active tokens for replay"

        await quote_collector.collect_top_only(tokens[:50])
        await quote_collector.collect_depth_only(tokens[:50])

        arb_config = arb_config_from_settings(settings)
        gate = AiGate.from_settings(settings, arb_config)
        orchestrator = ArbOrchestrator(db=db, clob_client=clob, config=arb_config, ai_gate=gate)

        end_ts = datetime.now(tz=UTC)
        start_ts = end_ts - timedelta(minutes=15)
        stats = await orchestrator.run_replay(start_ts, end_ts)
        assert stats["strategies"] >= 0

        row = await db.fetch_one("select count(*) as c from arb_replay_run")
        assert row and int(row["c"]) > 0
    finally:
        await gamma.close()
        await clob.close()
