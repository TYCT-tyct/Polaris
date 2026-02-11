from __future__ import annotations

from polaris.arb.contracts import ArbSignal, RunMode, StrategyCode
from polaris.arb.orchestrator import _filter_safe_mode_signals


def _signal(code: StrategyCode) -> ArbSignal:
    return ArbSignal(
        strategy_code=code,
        mode=RunMode.PAPER_LIVE,
        source_code="test",
        event_id="e1",
        market_ids=["m1"],
        token_ids=["t1"],
        edge_pct=0.01,
        expected_pnl_usd=0.01,
        ttl_ms=30_000,
        features={"legs": []},
        decision_note="test",
    )


def test_safe_mode_filters_non_arbitrage_strategies() -> None:
    signals = [_signal(StrategyCode.A), _signal(StrategyCode.F), _signal(StrategyCode.G)]
    filtered = _filter_safe_mode_signals(RunMode.PAPER_LIVE, signals, safe_arbitrage_only=True)
    assert [item.strategy_code for item in filtered] == [StrategyCode.A]


def test_safe_mode_does_not_filter_shadow_mode() -> None:
    signals = [_signal(StrategyCode.A), _signal(StrategyCode.F), _signal(StrategyCode.G)]
    filtered = _filter_safe_mode_signals(RunMode.SHADOW, signals, safe_arbitrage_only=True)
    assert [item.strategy_code for item in filtered] == [StrategyCode.A, StrategyCode.F, StrategyCode.G]
