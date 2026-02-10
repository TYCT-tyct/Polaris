from __future__ import annotations

from collections import defaultdict

from polaris.arb.config import ArbConfig
from polaris.arb.contracts import ArbSignal, RunMode, StrategyCode, TokenSnapshot
from polaris.arb.strategies.common import executable_buy_price, min_shares_for_order


class StrategyC:
    def __init__(self, config: ArbConfig) -> None:
        self.config = config

    def scan(self, mode: RunMode, source_code: str, snapshots: list[TokenSnapshot]) -> list[ArbSignal]:
        if not self.config.enable_strategy_c:
            return []

        by_event_market: dict[str, dict[str, dict[str, TokenSnapshot]]] = defaultdict(dict)
        for token in snapshots:
            if not token.event_id:
                continue
            per_market = by_event_market[token.event_id].setdefault(token.market_id, {})
            per_market[token.outcome_side] = token

        signals: list[ArbSignal] = []
        shares = max(1.0, self.config.min_order_notional_usd)
        max_candidates = max(1, self.config.c_max_candidates_per_event)
        for event_id, markets in by_event_market.items():
            if len(markets) < 3:
                continue

            market_ids = sorted(markets.keys())
            yes_prices: dict[str, float] = {}
            no_prices: dict[str, float] = {}
            for market_id, sides in markets.items():
                yes = sides.get("YES")
                no = sides.get("NO")
                if yes is None or no is None:
                    continue
                yes_price = executable_buy_price(yes, shares)
                no_price = executable_buy_price(no, shares)
                if yes_price is not None:
                    yes_prices[market_id] = yes_price
                if no_price is not None:
                    no_prices[market_id] = no_price

            if len(yes_prices) < 3 or len(no_prices) < 3:
                continue

            event_candidates: list[tuple[float, str, ArbSignal]] = []
            for base_market in market_ids:
                no_price = no_prices.get(base_market)
                if no_price is None:
                    continue
                complement_cost = sum(price for m, price in yes_prices.items() if m != base_market)
                edge_pct = complement_cost - no_price
                if edge_pct < self.config.c_min_edge_pct:
                    continue
                base_no = markets[base_market].get("NO")
                if base_no is None:
                    continue
                notional = min(self.config.single_risk_usd, max(self.config.min_order_notional_usd, 1.0))
                sized_shares = max(
                    notional / max(no_price, 1e-6),
                    min_shares_for_order(base_no, no_price, self.config.min_order_notional_usd),
                )
                notional = sized_shares * no_price
                if notional > self.config.single_risk_usd:
                    continue
                event_candidates.append(
                    (
                        edge_pct,
                        base_market,
                        ArbSignal(
                            strategy_code=StrategyCode.C,
                            mode=mode,
                            source_code=source_code,
                            event_id=event_id,
                            market_ids=[base_market],
                            token_ids=[base_no.token_id],
                            edge_pct=edge_pct,
                            expected_pnl_usd=notional * edge_pct,
                            ttl_ms=9000,
                            features={
                                "legs": [
                                    {
                                        "market_id": base_market,
                                        "token_id": base_no.token_id,
                                        "side": "BUY",
                                        "price": no_price,
                                        "shares": sized_shares,
                                        "notional_usd": notional,
                                    }
                                ],
                                "requires_conversion": True,
                                "conversion_market": base_market,
                                "complement_yes_cost": complement_cost,
                                "expected_edge_pct": edge_pct,
                                "expected_hold_minutes": 45,
                            },
                            decision_note="neg_risk_no_to_yes_conversion",
                        ),
                    )
                )
            if not event_candidates:
                continue
            event_candidates.sort(key=lambda item: (-item[0], item[1]))
            signals.extend(signal for _, _, signal in event_candidates[:max_candidates])
        return signals
