from __future__ import annotations

import pytest

from polaris.arb.ai.gate import AiGate
from polaris.arb.config import arb_config_from_settings
from polaris.arb.contracts import RunMode
from polaris.arb.orchestrator import ArbOrchestrator
from polaris.config import PolarisSettings, RetryConfig
from polaris.harvest.collector_markets import MarketCollector
from polaris.infra.rate_limiter import AsyncTokenBucket
from polaris.sources.clob_client import ClobClient
from polaris.sources.gamma_client import GammaClient


@pytest.mark.asyncio
@pytest.mark.live
async def test_arb_live_scan_runs(postgres_dsn: str, db) -> None:
    settings = PolarisSettings(database_url=postgres_dsn, market_discovery_scope="all")
    gamma = GammaClient(AsyncTokenBucket(settings.gamma_rate, settings.gamma_burst), RetryConfig())
    clob = ClobClient(AsyncTokenBucket(settings.clob_rate, settings.clob_burst), RetryConfig())
    collector = MarketCollector(db, gamma, market_scope="all")
    try:
        market_count, token_count = await collector.run_once()
        assert market_count > 0
        assert token_count > 0

        arb_config = arb_config_from_settings(settings)
        gate = AiGate.from_settings(settings, arb_config)
        orchestrator = ArbOrchestrator(db=db, clob_client=clob, config=arb_config, ai_gate=gate)

        stats = await orchestrator.run_once(RunMode.SHADOW)
        assert stats["signals"] >= 0

        row = await db.fetch_one("select count(*) as c from arb_signal")
        assert row and int(row["c"]) >= 0
    finally:
        await gamma.close()
        await clob.close()
