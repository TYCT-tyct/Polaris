from __future__ import annotations

from polaris.arb.config import ArbConfig
from polaris.arb.contracts import ArbSignal, RunMode, StrategyCode, TokenSnapshot
from polaris.arb.strategies.common import executable_buy_price, min_shares_for_order, resolve_hours_left


class StrategyF:
    def __init__(self, config: ArbConfig) -> None:
        self.config = config

    def scan(self, mode: RunMode, source_code: str, snapshots: list[TokenSnapshot]) -> list[ArbSignal]:
        if not self.config.enable_strategy_f:
            return []

        candidates: list[tuple[float, float, ArbSignal]] = []
        for token in snapshots:
            if token.outcome_side != "YES":
                continue
            if token.best_ask is None or token.best_bid is None:
                continue
            # 纸面/实盘一致性：用可执行深度的平均成交价估计成本，避免只看 best_ask 导致误判。
            best_ask = float(token.best_ask)
            best_bid = float(token.best_bid)
            spread = max(0.0, best_ask - best_bid)
            if spread > self.config.f_max_spread:
                continue
            if best_ask < self.config.f_min_prob or best_ask > 0.995:
                continue
            hours_left = resolve_hours_left(token.market_end)
            if hours_left is None or hours_left <= 0 or hours_left > self.config.f_max_hours_to_resolve:
                continue

            gross_return = (1.0 - best_ask) / max(best_ask, 1e-6)
            annualized = gross_return * 0.98 * (24.0 / hours_left) * 365.0 / 24.0
            if annualized < self.config.f_min_annualized_return:
                continue

            edge_pct = (1.0 - best_ask) * 0.98
            notional = min(self.config.single_risk_usd, max(self.config.min_order_notional_usd, 1.0))
            shares = max(notional / best_ask, min_shares_for_order(token, best_ask, self.config.min_order_notional_usd))
            buy_price = executable_buy_price(token, shares) or best_ask
            notional = shares * buy_price
            if notional > self.config.single_risk_usd:
                continue
            # BUY 检查 asks 深度（旧版本误用 bids 会导致“流动性不足”误判或错误放行）。
            if token.asks and float(token.asks[0].size) < shares:
                continue
            signal = ArbSignal(
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
                            "price": buy_price,
                            "shares": shares,
                            "notional_usd": notional,
                        }
                    ],
                    "hours_left": hours_left,
                    "annualized_return": annualized,
                    "spread": spread,
                    "expected_edge_pct": edge_pct,
                    "expected_hold_minutes": int(hours_left * 60),
                },
                decision_note="high_prob_short_bond",
            )
            candidates.append((annualized, spread, signal))
        candidates.sort(key=lambda item: (-item[0], item[1], -item[2].edge_pct))
        cap = max(0, self.config.f_max_signals_per_cycle)
        if cap > 0:
            candidates = candidates[:cap]
        return [signal for _, _, signal in candidates]
