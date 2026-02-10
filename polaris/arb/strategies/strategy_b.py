from __future__ import annotations

from polaris.arb.config import ArbConfig
from polaris.arb.contracts import ArbSignal, RunMode, StrategyCode, TokenSnapshot
from polaris.arb.strategies.common import executable_buy_price, group_by_market


class StrategyB:
    def __init__(self, config: ArbConfig) -> None:
        self.config = config

    def scan(self, mode: RunMode, source_code: str, snapshots: list[TokenSnapshot]) -> list[ArbSignal]:
        if not self.config.enable_strategy_b:
            return []

        signals: list[ArbSignal] = []
        grouped = group_by_market(snapshots)
        for market_id, tokens in grouped.items():
            if len(tokens) != 2:
                continue
            yes = next((t for t in tokens if t.outcome_side == "YES"), None)
            no = next((t for t in tokens if t.outcome_side == "NO"), None)
            if not yes or not no:
                continue
            shares = max(1.0, self.config.min_order_notional_usd)
            yes_price = executable_buy_price(yes, shares)
            no_price = executable_buy_price(no, shares)
            if yes_price is None or no_price is None:
                continue
            total = yes_price + no_price
            edge_pct = 1.0 - total
            if edge_pct < self.config.b_min_edge_pct:
                continue

            legs = [
                {"market_id": yes.market_id, "token_id": yes.token_id, "side": "BUY", "price": yes_price, "shares": shares},
                {"market_id": no.market_id, "token_id": no.token_id, "side": "BUY", "price": no_price, "shares": shares},
            ]
            signals.append(
                ArbSignal(
                    strategy_code=StrategyCode.B,
                    mode=mode,
                    source_code=source_code,
                    event_id=yes.event_id,
                    market_ids=[market_id],
                    token_ids=[yes.token_id, no.token_id],
                    edge_pct=edge_pct,
                    expected_pnl_usd=shares * edge_pct,
                    ttl_ms=5000,
                    features={
                        "legs": legs,
                        "total_cost_per_share": total,
                        "target_shares": shares,
                        "expected_edge_pct": edge_pct,
                        "expected_hold_minutes": 20,
                    },
                    decision_note="binary_yes_no_sum",
                )
            )
        return signals
