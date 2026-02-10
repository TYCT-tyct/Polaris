from __future__ import annotations

from polaris.arb.config import ArbConfig
from polaris.arb.contracts import ArbSignal, RunMode, StrategyCode, TokenSnapshot
from polaris.arb.strategies.common import group_by_market, resolve_hours_left


class StrategyG:
    def __init__(self, config: ArbConfig) -> None:
        self.config = config

    def scan(self, mode: RunMode, source_code: str, snapshots: list[TokenSnapshot]) -> list[ArbSignal]:
        if not self.config.enable_strategy_g:
            return []

        signals: list[ArbSignal] = []
        grouped = group_by_market(snapshots)
        for market_id, tokens in grouped.items():
            if len(tokens) < 2:
                continue
            market_end = tokens[0].market_end
            hours_left = resolve_hours_left(market_end)
            if hours_left is None or hours_left <= 0 or hours_left > self.config.g_max_hours_to_resolve:
                continue

            priced = [token for token in tokens if token.best_ask is not None and token.best_ask > 0]
            if len(priced) < 2:
                continue
            ranked = sorted(priced, key=lambda item: item.best_ask or 0.0, reverse=True)
            top = ranked[0]
            second = ranked[1]
            top_price = float(top.best_ask or 0.0)
            second_price = float(second.best_ask or 0.0)
            dominance = max(0.0, top_price - second_price)
            time_factor = max(0.0, min(1.0, (self.config.g_max_hours_to_resolve - hours_left) / self.config.g_max_hours_to_resolve))
            confidence = min(0.999, top_price + dominance * (0.15 + 0.35 * time_factor))
            expected_edge = confidence - top_price
            if confidence < self.config.g_min_confidence:
                continue
            if expected_edge < self.config.g_min_expected_edge_pct:
                continue
            if top_price >= 0.97:
                continue

            notional = min(self.config.single_risk_usd, max(self.config.min_order_notional_usd, 1.0))
            shares = notional / top_price
            signals.append(
                ArbSignal(
                    strategy_code=StrategyCode.G,
                    mode=mode,
                    source_code=source_code,
                    event_id=top.event_id,
                    market_ids=[market_id],
                    token_ids=[top.token_id],
                    edge_pct=expected_edge,
                    expected_pnl_usd=notional * expected_edge,
                    ttl_ms=5000,
                    features={
                        "legs": [
                            {
                                "market_id": top.market_id,
                                "token_id": top.token_id,
                                "side": "BUY",
                                "price": top_price,
                                "shares": shares,
                                "notional_usd": notional,
                            }
                        ],
                        "hours_left": hours_left,
                        "dominance": dominance,
                        "confidence": confidence,
                        "expected_edge_pct": expected_edge,
                        "expected_hold_minutes": max(5, int(hours_left * 60 * 0.5)),
                    },
                    decision_note="tail_convergence_global",
                )
            )
        return signals
