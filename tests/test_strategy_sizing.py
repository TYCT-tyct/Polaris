from __future__ import annotations

from datetime import UTC, datetime, timedelta

from polaris.arb.config import ArbConfig, arb_config_from_settings
from polaris.arb.contracts import PriceLevel, RunMode, TokenSnapshot
from polaris.arb.strategies.strategy_a import StrategyA
from polaris.arb.strategies.strategy_f import StrategyF
from polaris.config import PolarisSettings


def _make_config() -> ArbConfig:
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_single_risk_usd=20.0,
    )
    return arb_config_from_settings(settings)


def _token(
    token_id: str,
    market_id: str,
    event_id: str,
    ask: float,
    min_order_size: float | None = None,
    end_after_hours: float = 6.0,
) -> TokenSnapshot:
    now = datetime.now(tz=UTC)
    return TokenSnapshot(
        token_id=token_id,
        market_id=market_id,
        event_id=event_id,
        market_question="q",
        market_end=now + timedelta(hours=end_after_hours),
        outcome_label="YES",
        outcome_side="YES",
        outcome_index=0,
        min_order_size=min_order_size,
        tick_size=0.01,
        best_bid=max(0.01, ask - 0.01),
        best_ask=ask,
        bids=(PriceLevel(price=max(0.01, ask - 0.01), size=100.0),),
        asks=(PriceLevel(price=ask, size=100.0),),
        captured_at=now,
    )


def test_strategy_a_respects_min_order_constraints() -> None:
    config = _make_config()
    strategy = StrategyA(config)
    event_id = "event-1"
    snapshots = [
        _token("t1", "m1", event_id, ask=0.20, min_order_size=1.0),
        _token("t2", "m2", event_id, ask=0.25, min_order_size=5.0),
        _token("t3", "m3", event_id, ask=0.30, min_order_size=1.0),
    ]

    signals = strategy.scan(RunMode.SHADOW, "polymarket", snapshots)
    assert len(signals) == 1
    legs = signals[0].features["legs"]
    shares = float(legs[0]["shares"])
    assert shares >= 5.0
    for leg in legs:
        assert float(leg["shares"]) == shares
        assert float(leg["price"]) * shares >= 1.0


def test_strategy_f_raises_size_to_min_order_size() -> None:
    config = _make_config()
    strategy = StrategyF(config)
    token = _token("f1", "fm1", "event-f", ask=0.95, min_order_size=7.0, end_after_hours=2.0)

    signals = strategy.scan(RunMode.SHADOW, "polymarket", [token])
    assert len(signals) == 1
    leg = signals[0].features["legs"][0]
    assert float(leg["shares"]) >= 7.0
    assert float(leg["notional_usd"]) >= float(leg["price"]) * 7.0
