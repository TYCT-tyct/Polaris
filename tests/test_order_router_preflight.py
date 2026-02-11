from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from polaris.arb.config import arb_config_from_settings
from polaris.arb.contracts import OrderIntent, PriceLevel, RunMode, StrategyCode, TokenSnapshot
from polaris.arb.execution.order_router import OrderRouter
from polaris.config import PolarisSettings, RetryConfig
from polaris.infra.rate_limiter import AsyncTokenBucket
from polaris.sources.clob_client import ClobClient


def _intent(token_id: str) -> OrderIntent:
    return OrderIntent(
        intent_id=uuid4(),
        signal_id=uuid4(),
        mode=RunMode.LIVE,
        strategy_code=StrategyCode.A,
        source_code="polymarket",
        order_index=0,
        market_id="m1",
        token_id=token_id,
        side="BUY",
        order_type="IOC",
        limit_price=0.5,
        shares=2.0,
        notional_usd=1.0,
        payload={},
    )


def _snapshot(token_id: str, captured_at: datetime) -> TokenSnapshot:
    return TokenSnapshot(
        token_id=token_id,
        market_id="m1",
        event_id="e1",
        market_question="q",
        market_end=None,
        outcome_label="YES",
        outcome_side="YES",
        outcome_index=0,
        min_order_size=1.0,
        tick_size=0.01,
        best_bid=0.49,
        best_ask=0.50,
        bids=(PriceLevel(price=0.49, size=50.0),),
        asks=(PriceLevel(price=0.50, size=50.0),),
        captured_at=captured_at,
    )


@pytest.mark.asyncio
async def test_live_preflight_uses_fresh_snapshot_without_refresh(db) -> None:
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_live_preflight_force_refresh=False,
        arb_live_preflight_max_age_ms=2000,
    )
    router = OrderRouter(
        db=db,
        config=arb_config_from_settings(settings),
        clob_client=ClobClient(AsyncTokenBucket(5.0, 8), RetryConfig()),
    )
    now = datetime.now(tz=UTC)
    decision = router._should_refresh_preflight([_intent("t1")], {"t1": _snapshot("t1", now)})
    assert decision is False


@pytest.mark.asyncio
async def test_live_preflight_refreshes_on_stale_or_missing_snapshot(db) -> None:
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_live_preflight_force_refresh=False,
        arb_live_preflight_max_age_ms=1000,
    )
    router = OrderRouter(
        db=db,
        config=arb_config_from_settings(settings),
        clob_client=ClobClient(AsyncTokenBucket(5.0, 8), RetryConfig()),
    )
    stale = datetime.now(tz=UTC) - timedelta(seconds=3)
    assert router._should_refresh_preflight([_intent("t1")], {"t1": _snapshot("t1", stale)}) is True
    assert router._should_refresh_preflight([_intent("t2")], {"t1": _snapshot("t1", stale)}) is True


@pytest.mark.asyncio
async def test_live_preflight_force_refresh_always_true(db) -> None:
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_live_preflight_force_refresh=True,
        arb_live_preflight_max_age_ms=0,
    )
    router = OrderRouter(
        db=db,
        config=arb_config_from_settings(settings),
        clob_client=ClobClient(AsyncTokenBucket(5.0, 8), RetryConfig()),
    )
    now = datetime.now(tz=UTC)
    decision = router._should_refresh_preflight([_intent("t1")], {"t1": _snapshot("t1", now)})
    assert decision is True
