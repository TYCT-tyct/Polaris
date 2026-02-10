from __future__ import annotations

from polaris.arb.config import ArbConfig
from polaris.arb.contracts import ArbSignal, RunMode, StrategyCode, TokenSnapshot
from polaris.arb.strategies.common import executable_buy_price, group_by_event


class StrategyA:
    def __init__(self, config: ArbConfig) -> None:
        self.config = config

    def scan(self, mode: RunMode, source_code: str, snapshots: list[TokenSnapshot]) -> list[ArbSignal]:
        if not self.config.enable_strategy_a:
            return []

        signals: list[ArbSignal] = []
        grouped = group_by_event(snapshots)
        for event_id, tokens in grouped.items():
            yes_tokens = [
                token
                for token in tokens
                if token.outcome_side == "YES" and not token.is_other_outcome and not token.is_placeholder_outcome
            ]
            if len(yes_tokens) < 3:
                continue

            # 每条腿使用同一份额，成本使用深度加权成交价，不用单点 best ask。
            target_shares = max(1.0, self.config.min_order_notional_usd)
            total_cost_per_share = 0.0
            legs: list[dict[str, float | str]] = []
            executable = True
            market_ids: list[str] = []
            token_ids: list[str] = []
            for token in sorted(yes_tokens, key=lambda item: item.outcome_index):
                price = executable_buy_price(token, target_shares)
                if price is None:
                    executable = False
                    break
                total_cost_per_share += price
                market_ids.append(token.market_id)
                token_ids.append(token.token_id)
                legs.append(
                    {
                        "market_id": token.market_id,
                        "token_id": token.token_id,
                        "side": "BUY",
                        "price": round(price, 6),
                        "shares": target_shares,
                    }
                )
            if not executable:
                continue

            edge_pct = 1.0 - total_cost_per_share
            if edge_pct < self.config.a_min_edge_pct:
                continue

            expected_pnl = target_shares * edge_pct
            signal = ArbSignal(
                strategy_code=StrategyCode.A,
                mode=mode,
                source_code=source_code,
                event_id=event_id,
                market_ids=market_ids,
                token_ids=token_ids,
                edge_pct=edge_pct,
                expected_pnl_usd=expected_pnl,
                ttl_ms=8000,
                features={
                    "legs": legs,
                    "total_cost_per_share": total_cost_per_share,
                    "target_shares": target_shares,
                    "expected_edge_pct": edge_pct,
                    "expected_hold_minutes": 30,
                },
                decision_note="negrisk_yes_sum",
            )
            signals.append(signal)
        return signals
