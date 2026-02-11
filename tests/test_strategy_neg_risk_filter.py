from __future__ import annotations

from datetime import UTC, datetime, timedelta

from polaris.arb.config import arb_config_from_settings
from polaris.arb.contracts import PriceLevel, RunMode, TokenSnapshot
from polaris.arb.strategies.strategy_a import StrategyA
from polaris.arb.strategies.strategy_c import StrategyC
from polaris.config import PolarisSettings


def _cfg():
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_single_risk_usd=20.0,
        arb_c_min_edge_pct=0.01,
    )
    return arb_config_from_settings(settings)


def _token(
    token_id: str,
    market_id: str,
    event_id: str,
    side: str,
    ask: float,
    *,
    is_neg_risk: bool,
) -> TokenSnapshot:
    now = datetime.now(tz=UTC)
    return TokenSnapshot(
        token_id=token_id,
        market_id=market_id,
        event_id=event_id,
        market_question="q",
        market_end=now + timedelta(hours=6),
        outcome_label=side,
        outcome_side=side,
        outcome_index=0 if side == "YES" else 1,
        min_order_size=1.0,
        tick_size=0.01,
        best_bid=max(0.01, ask - 0.01),
        best_ask=ask,
        is_neg_risk=is_neg_risk,
        bids=(PriceLevel(price=max(0.01, ask - 0.01), size=100.0),),
        asks=(PriceLevel(price=ask, size=100.0),),
    )


def test_strategy_a_ignores_non_neg_risk_markets() -> None:
    strategy = StrategyA(_cfg())
    snapshots = [
        _token("t1", "m1", "e1", "YES", 0.20, is_neg_risk=False),
        _token("t2", "m2", "e1", "YES", 0.25, is_neg_risk=False),
        _token("t3", "m3", "e1", "YES", 0.30, is_neg_risk=False),
    ]
    assert strategy.scan(RunMode.SHADOW, "polymarket", snapshots) == []


def test_strategy_c_ignores_non_neg_risk_markets() -> None:
    strategy = StrategyC(_cfg())
    snapshots = [
        _token("m1-yes", "m1", "e1", "YES", 0.18, is_neg_risk=False),
        _token("m1-no", "m1", "e1", "NO", 0.06, is_neg_risk=False),
        _token("m2-yes", "m2", "e1", "YES", 0.20, is_neg_risk=False),
        _token("m2-no", "m2", "e1", "NO", 0.08, is_neg_risk=False),
        _token("m3-yes", "m3", "e1", "YES", 0.22, is_neg_risk=False),
        _token("m3-no", "m3", "e1", "NO", 0.09, is_neg_risk=False),
    ]
    assert strategy.scan(RunMode.SHADOW, "polymarket", snapshots) == []

