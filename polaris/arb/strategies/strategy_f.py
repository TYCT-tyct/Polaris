from __future__ import annotations

from polaris.arb.config import ArbConfig
from polaris.arb.contracts import ArbSignal, RunMode, StrategyCode, TokenSnapshot
from polaris.arb.strategies.common import resolve_hours_left


class StrategyF:
    def __init__(self, config: ArbConfig) -> None:
        self.config = config

    def scan(self, mode: RunMode, source_code: str, snapshots: list[TokenSnapshot]) -> list[ArbSignal]:
        if not self.config.enable_strategy_f:
            return []

        signals: list[ArbSignal] = []
        for token in snapshots:
            if token.outcome_side != "YES":
                continue
            if token.best_ask is None:
                continue
            price = float(token.best_ask)
            if price < self.config.f_min_prob or price > 0.995:
                continue
            hours_left = resolve_hours_left(token.market_end)
            if hours_left is None or hours_left <= 0 or hours_left > self.config.f_max_hours_to_resolve:
                continue

            gross_return = (1.0 - price) / max(price, 1e-6)
            annualized = gross_return * 0.98 * (24.0 / hours_left) * 365.0 / 24.0
            if annualized < self.config.f_min_annualized_return:
                continue

            edge_pct = (1.0 - price) * 0.98
            notional = min(self.config.single_risk_usd, max(self.config.min_order_notional_usd, 1.0))
            shares = notional / price
            signals.append(
                ArbSignal(
                    strategy_code=StrategyCode.F,
                    mode=mode,
                    source_code=source_code,
                    event_id=token.event_id,
                    market_ids=[token.market_id],
                    token_ids=[token.token_id],
                    edge_pct=edge_pct,
                    expected_pnl_usd=notional * edge_pct,
                    ttl_ms=30000,
                    features={
                        "legs": [
                            {
                                "market_id": token.market_id,
                                "token_id": token.token_id,
                                "side": "BUY",
                                "price": price,
                                "shares": shares,
                                "notional_usd": notional,
                            }
                        ],
                        "hours_left": hours_left,
                        "annualized_return": annualized,
                        "expected_edge_pct": edge_pct,
                        "expected_hold_minutes": int(hours_left * 60),
                    },
                    decision_note="high_prob_short_bond",
                )
            )
        return signals
