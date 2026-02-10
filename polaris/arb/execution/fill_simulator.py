from __future__ import annotations

from dataclasses import dataclass

from polaris.arb.contracts import PriceLevel


@dataclass(slots=True, frozen=True)
class SimFill:
    avg_price: float
    filled_size: float
    notional: float


def simulate_buy_fill(levels: tuple[PriceLevel, ...], target_size: float) -> SimFill | None:
    if target_size <= 0:
        return None
    remaining = target_size
    filled = 0.0
    cost = 0.0
    for level in sorted(levels, key=lambda item: item.price):
        if remaining <= 0:
            break
        take = min(level.size, remaining)
        if take <= 0:
            continue
        cost += take * level.price
        filled += take
        remaining -= take
    if filled <= 0:
        return None
    return SimFill(avg_price=cost / filled, filled_size=filled, notional=cost)


def simulate_sell_fill(levels: tuple[PriceLevel, ...], target_size: float) -> SimFill | None:
    if target_size <= 0:
        return None
    remaining = target_size
    filled = 0.0
    proceeds = 0.0
    for level in sorted(levels, key=lambda item: item.price, reverse=True):
        if remaining <= 0:
            break
        take = min(level.size, remaining)
        if take <= 0:
            continue
        proceeds += take * level.price
        filled += take
        remaining -= take
    if filled <= 0:
        return None
    return SimFill(avg_price=proceeds / filled, filled_size=filled, notional=proceeds)
