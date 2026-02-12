from __future__ import annotations

from uuid import uuid4

import pytest

from polaris.arb.config import arb_config_from_settings
from polaris.arb.contracts import ArbSignal, ExecutionPlan, OrderIntent, RunMode, StrategyCode
from polaris.arb.execution.order_router import OrderRouter
from polaris.arb.contracts import FillEvent, PriceLevel, TokenSnapshot
from polaris.arb.execution.order_router import _estimate_trade_pnl, _resolve_realized_gross
from polaris.config import PolarisSettings, RetryConfig
from polaris.infra.rate_limiter import AsyncTokenBucket
from polaris.sources.clob_client import ClobClient


def _snapshot(token_id: str, bid: float, ask: float) -> TokenSnapshot:
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
        best_bid=bid,
        best_ask=ask,
        bids=(PriceLevel(price=bid, size=100.0),),
        asks=(PriceLevel(price=ask, size=100.0),),
    )


def test_estimate_trade_pnl_uses_mark_to_book_for_buy_fill() -> None:
    fills = [
        FillEvent(
            token_id="t1",
            market_id="m1",
            side="BUY",
            fill_price=0.50,
            fill_size=2.0,
            fill_notional_usd=1.0,
            fee_usd=0.0,
        )
    ]
    snapshots = {"t1": _snapshot("t1", bid=0.49, ask=0.51)}
    gross, fees, slip, expected_gross = _estimate_trade_pnl(
        {"expected_edge_pct": 0.10},
        fills,
        snapshots,
        fee_bps=10,
    )
    assert gross == pytest.approx(-0.02, abs=1e-9)
    assert fees == pytest.approx(0.001, abs=1e-9)
    assert slip == 0.0
    assert expected_gross == pytest.approx(0.10, abs=1e-9)


def test_estimate_trade_pnl_uses_mark_to_book_for_sell_fill() -> None:
    fills = [
        FillEvent(
            token_id="t2",
            market_id="m1",
            side="SELL",
            fill_price=0.60,
            fill_size=2.0,
            fill_notional_usd=1.2,
            fee_usd=0.0,
        )
    ]
    snapshots = {"t2": _snapshot("t2", bid=0.58, ask=0.61)}
    gross, fees, slip, expected_gross = _estimate_trade_pnl(
        {"expected_edge_pct": 0.05},
        fills,
        snapshots,
        fee_bps=10,
    )
    assert gross == pytest.approx(-0.02, abs=1e-9)
    assert fees == pytest.approx(0.0012, abs=1e-9)
    assert slip == 0.0
    assert expected_gross == pytest.approx(0.06, abs=1e-9)


def test_estimate_trade_pnl_missing_snapshot_keeps_expected_only() -> None:
    fills = [
        FillEvent(
            token_id="missing",
            market_id="m1",
            side="BUY",
            fill_price=0.40,
            fill_size=2.5,
            fill_notional_usd=1.0,
            fee_usd=0.0,
        )
    ]
    gross, fees, slip, expected_gross = _estimate_trade_pnl(
        {"expected_edge_pct": 0.08},
        fills,
        {},
        fee_bps=10,
    )
    assert gross == 0.0
    assert fees == pytest.approx(0.001, abs=1e-9)
    assert slip == 0.0
    assert expected_gross == pytest.approx(0.08, abs=1e-9)


def test_resolve_realized_gross_entry_only_ignores_mark_to_book() -> None:
    gross, mode = _resolve_realized_gross(mark_to_book=-0.35, hold_minutes=0.2, realized_mode="entry_only")
    assert mode == "entry_only"
    assert gross == pytest.approx(0.0, abs=1e-9)


def test_resolve_realized_gross_hybrid_marks_only_short_hold() -> None:
    gross_short, mode_short = _resolve_realized_gross(mark_to_book=-0.35, hold_minutes=0.2, realized_mode="hybrid")
    gross_hold, mode_hold = _resolve_realized_gross(mark_to_book=-0.35, hold_minutes=5.0, realized_mode="hybrid")
    assert mode_short == "mark_to_book"
    assert gross_short == pytest.approx(-0.35, abs=1e-9)
    assert mode_hold == "entry_only"
    assert gross_hold == pytest.approx(0.0, abs=1e-9)


class _NoopDb:
    async def execute(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        return None

    async def executemany(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        return None


@pytest.mark.asyncio
async def test_simulate_paper_uses_entry_only_for_holding_signals() -> None:
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_single_risk_usd=5.0,
        arb_min_order_notional_usd=1.0,
        arb_fee_bps=10,
    )
    router = OrderRouter(
        db=_NoopDb(),
        config=arb_config_from_settings(settings),
        clob_client=ClobClient(AsyncTokenBucket(5.0, 8), RetryConfig()),
    )
    signal = ArbSignal(
        strategy_code=StrategyCode.F,
        mode=RunMode.PAPER_LIVE,
        source_code="paper-test",
        event_id="event-1",
        market_ids=["m1"],
        token_ids=["t1"],
        edge_pct=0.05,
        expected_pnl_usd=0.25,
        ttl_ms=30_000,
        features={
            "expected_hold_minutes": 447,
            "expected_edge_pct": 0.05,
            "legs": [
                {
                    "market_id": "m1",
                    "token_id": "t1",
                    "side": "BUY",
                    "price": 0.94,
                    "shares": 5.0,
                    "notional_usd": 4.7,
                }
            ],
        },
        decision_note="hold",
    )
    plan = ExecutionPlan(
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
                order_type="PAPER",
                limit_price=0.94,
                shares=5.0,
                notional_usd=4.7,
                payload={},
            )
        ],
    )
    snapshot = TokenSnapshot(
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
        best_bid=0.05,
        best_ask=0.94,
        bids=(PriceLevel(price=0.05, size=100.0),),
        asks=(PriceLevel(price=0.94, size=100.0),),
    )
    result = await router.simulate_paper(plan, {"t1": snapshot})
    assert result.status == "filled"
    assert result.gross_pnl_usd == pytest.approx(0.0, abs=1e-9)
    assert result.net_pnl_usd == pytest.approx(-0.0047, abs=1e-9)
    assert result.metadata["pnl_model"] == "entry_only"
    assert result.metadata["mark_to_book_gross_pnl_usd"] < -4.0


@pytest.mark.asyncio
async def test_simulate_paper_allows_strategy_c_mark_to_book_when_unlocked() -> None:
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_single_risk_usd=5.0,
        arb_min_order_notional_usd=1.0,
        arb_fee_bps=0,
        arb_paper_realized_pnl_mode="mark_to_book",
        arb_c_force_entry_only_in_paper=False,
    )
    router = OrderRouter(
        db=_NoopDb(),
        config=arb_config_from_settings(settings),
        clob_client=ClobClient(AsyncTokenBucket(5.0, 8), RetryConfig()),
    )
    signal = ArbSignal(
        strategy_code=StrategyCode.C,
        mode=RunMode.PAPER_LIVE,
        source_code="paper-test",
        event_id="event-1",
        market_ids=["m1"],
        token_ids=["t1"],
        edge_pct=0.03,
        expected_pnl_usd=0.03,
        ttl_ms=30_000,
        features={
            "expected_hold_minutes": 45,
            "expected_edge_pct": 0.03,
            "legs": [
                {
                    "market_id": "m1",
                    "token_id": "t1",
                    "side": "BUY",
                    "price": 0.5,
                    "shares": 2.0,
                    "notional_usd": 1.0,
                }
            ],
        },
        decision_note="c_mark",
    )
    plan = ExecutionPlan(
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
                order_type="PAPER",
                limit_price=0.5,
                shares=2.0,
                notional_usd=1.0,
                payload={},
            )
        ],
    )
    snapshot = TokenSnapshot(
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
        best_bid=0.49,
        best_ask=0.5,
        bids=(PriceLevel(price=0.49, size=100.0),),
        asks=(PriceLevel(price=0.5, size=100.0),),
    )
    result = await router.simulate_paper(plan, {"t1": snapshot})
    assert result.status == "filled"
    assert result.metadata["pnl_model"] == "mark_to_book"
    assert result.gross_pnl_usd == pytest.approx(-0.02, abs=1e-9)
    assert result.net_pnl_usd == pytest.approx(-0.02, abs=1e-9)
