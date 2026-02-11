from __future__ import annotations

from polaris.arb.config import ArbConfig
from polaris.arb.contracts import ArbSignal, RunMode, StrategyCode, TokenSnapshot
from polaris.arb.strategies.common import executable_buy_price, group_neg_risk, uniform_basket_shares


class StrategyA:
    def __init__(self, config: ArbConfig) -> None:
        self.config = config

    def scan(self, mode: RunMode, source_code: str, snapshots: list[TokenSnapshot]) -> list[ArbSignal]:
        if not self.config.enable_strategy_a:
            return []

        signals: list[ArbSignal] = []
        grouped = group_neg_risk(snapshots)
        for group_key, tokens in grouped.items():
            yes_tokens = [
                token
                for token in tokens
                if token.outcome_side == "YES" and not token.is_other_outcome and not token.is_placeholder_outcome
            ]
            if len(yes_tokens) < 3:
                continue

            top_prices = {
                token.token_id: float(token.best_ask or 0.0)
                for token in yes_tokens
                if token.best_ask is not None and float(token.best_ask) > 0
            }
            if len(top_prices) < len(yes_tokens):
                continue
            # 对套利篮子使用统一份额，避免腿间名义金额不一致破坏对冲。
            target_shares = uniform_basket_shares(yes_tokens, top_prices, self.config.min_order_notional_usd)
            if target_shares <= 0:
                continue
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

            capital_used = target_shares * total_cost_per_share
            if capital_used > self.config.single_risk_usd:
                continue
            expected_pnl = target_shares * edge_pct
            event_id = next((token.event_id for token in yes_tokens if token.event_id), None)
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
                    "capital_used_usd": capital_used,
                    "expected_edge_pct": edge_pct,
                    "expected_hold_minutes": 30,
                    "group_key": group_key,
                },
                decision_note="negrisk_yes_sum",
            )
            signals.append(signal)
        return signals
