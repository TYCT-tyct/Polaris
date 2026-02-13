from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TrailingStopConfig:
    enabled: bool
    activate_profit_pct: float
    drawdown_pct: float


def update_peak_and_check_trigger(
    *,
    cfg: TrailingStopConfig,
    current_pnl_pct: float,
    prev_peak_pct: float | None,
) -> tuple[float, bool]:
    """
    追踪止损（Trailing Stop）逻辑：
    - 只在盈利达到激活阈值后才生效
    - 峰值回撤超过阈值触发退出

    返回: (new_peak_pct, triggered)
    """
    peak = current_pnl_pct if prev_peak_pct is None else max(float(prev_peak_pct), float(current_pnl_pct))
    if not cfg.enabled:
        return peak, False

    activate = max(0.0, float(cfg.activate_profit_pct))
    drawdown = max(0.0, float(cfg.drawdown_pct))
    if peak < activate:
        return peak, False
    if current_pnl_pct <= (peak - drawdown):
        return peak, True
    return peak, False

