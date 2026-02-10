from __future__ import annotations


def capped_notional(edge_pct: float, min_notional_usd: float, max_single_risk_usd: float) -> float:
    if edge_pct <= 0:
        return 0.0
    scaled = min_notional_usd * (1.0 + min(edge_pct * 10.0, 3.0))
    return min(max_single_risk_usd, max(min_notional_usd, scaled))
