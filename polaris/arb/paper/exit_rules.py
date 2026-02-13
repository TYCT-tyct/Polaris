from __future__ import annotations

from dataclasses import dataclass

from polaris.arb.config import ArbConfig


@dataclass(frozen=True, slots=True)
class ExitRule:
    min_hold_minutes: int
    max_hold_minutes: int
    take_profit_pct: float
    stop_loss_pct: float


def exit_rule_for_strategy(config: ArbConfig, strategy_code: str) -> ExitRule | None:
    if not config.paper_exit_enabled:
        return None
    normalized = (strategy_code or "").strip().upper()
    if normalized not in set(config.paper_exit_strategies):
        return None
    min_hold = max(0, int(config.paper_exit_min_hold_minutes))
    if normalized == "F":
        return ExitRule(
            min_hold_minutes=min_hold,
            max_hold_minutes=max(1, int(config.paper_exit_f_max_hold_minutes)),
            take_profit_pct=max(0.0, float(config.paper_exit_f_take_profit_pct)),
            stop_loss_pct=max(0.0, float(config.paper_exit_f_stop_loss_pct)),
        )
    if normalized == "G":
        return ExitRule(
            min_hold_minutes=min_hold,
            max_hold_minutes=max(1, int(config.paper_exit_g_max_hold_minutes)),
            take_profit_pct=max(0.0, float(config.paper_exit_g_take_profit_pct)),
            stop_loss_pct=max(0.0, float(config.paper_exit_g_stop_loss_pct)),
        )
    return ExitRule(
        min_hold_minutes=min_hold,
        max_hold_minutes=max(1, int(config.paper_exit_f_max_hold_minutes)),
        take_profit_pct=max(0.0, float(config.paper_exit_f_take_profit_pct)),
        stop_loss_pct=max(0.0, float(config.paper_exit_f_stop_loss_pct)),
    )


def decide_exit_reason(
    rule: ExitRule,
    *,
    hold_minutes: float,
    unrealized_pnl_usd: float,
    open_notional_usd: float,
) -> tuple[str | None, float]:
    denom = max(1e-9, float(open_notional_usd))
    pnl_pct = float(unrealized_pnl_usd) / denom
    if hold_minutes < float(rule.min_hold_minutes):
        return None, pnl_pct
    if hold_minutes >= float(rule.max_hold_minutes):
        return "timeout", pnl_pct
    if pnl_pct >= float(rule.take_profit_pct):
        return "take_profit", pnl_pct
    if pnl_pct <= -float(rule.stop_loss_pct):
        return "stop_loss", pnl_pct
    return None, pnl_pct

