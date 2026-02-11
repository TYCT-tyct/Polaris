from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from typing import Iterable

from polaris.arb.contracts import PriceLevel, TokenSnapshot
from polaris.arb.execution.fill_simulator import simulate_buy_fill


def group_by_event(tokens: Iterable[TokenSnapshot]) -> dict[str, list[TokenSnapshot]]:
    grouped: dict[str, list[TokenSnapshot]] = defaultdict(list)
    for token in tokens:
        if token.event_id:
            grouped[token.event_id].append(token)
    return dict(grouped)


def group_neg_risk(tokens: Iterable[TokenSnapshot]) -> dict[str, list[TokenSnapshot]]:
    grouped: dict[str, list[TokenSnapshot]] = defaultdict(list)
    for token in tokens:
        if not token.is_neg_risk:
            continue
        if token.event_id:
            grouped[f"event:{token.event_id}"].append(token)
            continue
        if token.condition_id:
            grouped[f"condition:{token.condition_id}"].append(token)
    return dict(grouped)


def group_by_market(tokens: Iterable[TokenSnapshot]) -> dict[str, list[TokenSnapshot]]:
    grouped: dict[str, list[TokenSnapshot]] = defaultdict(list)
    for token in tokens:
        grouped[token.market_id].append(token)
    return dict(grouped)


def executable_buy_price(token: TokenSnapshot, shares: float) -> float | None:
    sim = simulate_buy_fill(token.asks, shares)
    if sim is None or sim.filled_size < shares:
        return None
    return sim.avg_price


def resolve_hours_left(market_end: datetime | None, now: datetime | None = None) -> float | None:
    if market_end is None:
        return None
    ts = now or datetime.now(tz=UTC)
    diff = (market_end - ts).total_seconds() / 3600
    return diff


def top_of_book_price(levels: tuple[PriceLevel, ...], ask: bool) -> float | None:
    if not levels:
        return None
    if ask:
        return min(level.price for level in levels)
    return max(level.price for level in levels)


def min_shares_for_order(token: TokenSnapshot, price: float, min_notional_usd: float) -> float:
    if price <= 0:
        return 0.0
    notional_shares = min_notional_usd / price
    min_size = float(token.min_order_size or 0.0)
    return max(notional_shares, min_size)


def uniform_basket_shares(
    tokens: list[TokenSnapshot],
    prices: dict[str, float],
    min_notional_usd: float,
) -> float:
    required = 0.0
    for token in tokens:
        price = float(prices.get(token.token_id) or 0.0)
        if price <= 0:
            continue
        required = max(required, min_shares_for_order(token, price, min_notional_usd))
    return required
