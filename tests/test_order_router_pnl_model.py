from __future__ import annotations

import pytest

from polaris.arb.contracts import FillEvent, PriceLevel, TokenSnapshot
from polaris.arb.execution.order_router import _estimate_trade_pnl


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
    )
    assert gross == 0.0
    assert fees == pytest.approx(0.001, abs=1e-9)
    assert slip == 0.0
    assert expected_gross == pytest.approx(0.08, abs=1e-9)
