from __future__ import annotations

from uuid import uuid4

import pytest

from polaris.arb.config import arb_config_from_settings
from polaris.arb.contracts import ArbSignal, ExecutionPlan, OrderIntent, PriceLevel, RunMode, StrategyCode, TokenSnapshot
from polaris.arb.execution.order_router import OrderRouter
from polaris.config import PolarisSettings, RetryConfig
from polaris.infra.rate_limiter import AsyncTokenBucket
from polaris.sources.clob_client import ClobClient


class _NoopDb:
    async def execute(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        return None

    async def executemany(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        return None


def _router(settings: PolarisSettings) -> OrderRouter:
    return OrderRouter(
        db=_NoopDb(),
        config=arb_config_from_settings(settings),
        clob_client=ClobClient(AsyncTokenBucket(5.0, 8), RetryConfig()),
    )


def _signal() -> ArbSignal:
    return ArbSignal(
        strategy_code=StrategyCode.A,
        mode=RunMode.PAPER_LIVE,
        source_code="paper-parity",
        event_id="evt-1",
        market_ids=["m1"],
        token_ids=["t1"],
        edge_pct=0.02,
        expected_pnl_usd=0.02,
        ttl_ms=15_000,
        features={"expected_hold_minutes": 0, "expected_edge_pct": 0.02},
        decision_note="parity",
    )


def _plan(order_type: str, price: float, shares: float) -> ExecutionPlan:
    signal = _signal()
    return ExecutionPlan(
        signal=signal,
        intents=[
            OrderIntent(
                intent_id=uuid4(),
                signal_id=signal.signal_id,
                mode=signal.mode,
                strategy_code=signal.strategy_code,
                source_code=signal.source_code,
                order_index=0,
                market_id="m1",
                token_id="t1",
                side="BUY",
                order_type=order_type,
                limit_price=price,
                shares=shares,
                notional_usd=None,
                payload={},
            )
        ],
    )


def _snapshot(asks: tuple[PriceLevel, ...], best_bid: float, best_ask: float) -> TokenSnapshot:
    return TokenSnapshot(
        token_id="t1",
        market_id="m1",
        event_id="e1",
        market_question="q",
        market_end=None,
        outcome_label="YES",
        outcome_side="YES",
        outcome_index=0,
        min_order_size=1.0,
        tick_size=0.01,
        best_bid=best_bid,
        best_ask=best_ask,
        bids=(PriceLevel(price=best_bid, size=100.0),),
        asks=asks,
    )


@pytest.mark.asyncio
async def test_paper_fok_rejects_when_limit_depth_insufficient() -> None:
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_slippage_bps=500,
    )
    router = _router(settings)
    plan = _plan(order_type="FOK", price=0.50, shares=2.0)
    snap = _snapshot(
        asks=(PriceLevel(price=0.50, size=1.0), PriceLevel(price=0.60, size=20.0)),
        best_bid=0.49,
        best_ask=0.50,
    )
    result = await router.simulate_paper(plan, {"t1": snap})
    assert result.status == "rejected"
    assert not result.fills
    assert result.capital_used_usd == pytest.approx(0.0, abs=1e-9)


@pytest.mark.asyncio
async def test_paper_fak_allows_partial_fill_under_limit() -> None:
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_slippage_bps=500,
    )
    router = _router(settings)
    plan = _plan(order_type="FAK", price=0.50, shares=2.0)
    snap = _snapshot(
        asks=(PriceLevel(price=0.50, size=1.0), PriceLevel(price=0.60, size=20.0)),
        best_bid=0.49,
        best_ask=0.50,
    )
    result = await router.simulate_paper(plan, {"t1": snap})
    assert result.status == "filled"
    assert len(result.fills) == 1
    assert result.fills[0].fill_size == pytest.approx(1.0, abs=1e-9)
    assert result.capital_used_usd == pytest.approx(0.50, abs=1e-9)


@pytest.mark.asyncio
async def test_paper_limit_preflight_rejects_bad_price_drift() -> None:
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_slippage_bps=40,
    )
    router = _router(settings)
    plan = _plan(order_type="FAK", price=0.50, shares=2.0)
    snap = _snapshot(
        asks=(PriceLevel(price=0.51, size=20.0),),
        best_bid=0.49,
        best_ask=0.51,
    )
    result = await router.simulate_paper(plan, {"t1": snap})
    assert result.status == "rejected"
    assert not result.fills
