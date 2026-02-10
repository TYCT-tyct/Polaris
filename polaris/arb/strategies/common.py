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
