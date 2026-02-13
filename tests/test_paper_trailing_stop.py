from __future__ import annotations

from polaris.arb.paper.trailing_stop import TrailingStopConfig, update_peak_and_check_trigger


def test_trailing_stop_disabled_updates_peak_only() -> None:
    cfg = TrailingStopConfig(enabled=False, activate_profit_pct=0.01, drawdown_pct=0.01)
    peak, trig = update_peak_and_check_trigger(cfg=cfg, current_pnl_pct=0.02, prev_peak_pct=None)
    assert peak == 0.02
    assert trig is False


def test_trailing_stop_not_active_before_activation() -> None:
    cfg = TrailingStopConfig(enabled=True, activate_profit_pct=0.03, drawdown_pct=0.01)
    peak, trig = update_peak_and_check_trigger(cfg=cfg, current_pnl_pct=0.02, prev_peak_pct=0.02)
    assert peak == 0.02
    assert trig is False


def test_trailing_stop_triggers_on_drawdown_from_peak() -> None:
    cfg = TrailingStopConfig(enabled=True, activate_profit_pct=0.01, drawdown_pct=0.012)
    peak, trig = update_peak_and_check_trigger(cfg=cfg, current_pnl_pct=0.02, prev_peak_pct=0.02)
    assert peak == 0.02
    assert trig is False

    peak, trig = update_peak_and_check_trigger(cfg=cfg, current_pnl_pct=0.007, prev_peak_pct=peak)
    assert peak == 0.02
    assert trig is True

