from __future__ import annotations

import json
import random
from dataclasses import dataclass
from statistics import fmean
from time import perf_counter_ns


@dataclass(frozen=True)
class BookUpdate:
    side: str
    price: float
    size: float


def generate_t2t_payload(
    *,
    levels_per_side: int = 200,
    updates: int = 1000,
    iterations: int = 100,
    seed: int = 42,
) -> dict[str, object]:
    rng = random.Random(seed)
    mid = 0.50
    initial_bids: list[dict[str, float]] = []
    initial_asks: list[dict[str, float]] = []

    for idx in range(levels_per_side):
        bid_price = round(mid - 0.0005 * (idx + 1), 6)
        ask_price = round(mid + 0.0005 * (idx + 1), 6)
        initial_bids.append({"price": bid_price, "size": round(rng.uniform(20, 150), 6)})
        initial_asks.append({"price": ask_price, "size": round(rng.uniform(20, 150), 6)})

    update_rows: list[dict[str, float | str]] = []
    for _ in range(max(1, updates)):
        side = "BID" if rng.random() < 0.5 else "ASK"
        base = mid - 0.01 if side == "BID" else mid + 0.01
        direction = 1 if side == "ASK" else -1
        level_shift = rng.randint(0, max(5, levels_per_side // 2))
        price = round(base + direction * 0.0005 * level_shift, 6)
        # 12% 概率表示撤单。
        if rng.random() < 0.12:
            size = 0.0
        else:
            size = round(rng.uniform(5, 200), 6)
        update_rows.append({"side": side, "price": price, "size": size})

    return {
        "iterations": max(1, iterations),
        "depth_pct": 1.0,
        "initial_bids": initial_bids,
        "initial_asks": initial_asks,
        "updates": update_rows,
    }


def run_python_t2t_benchmark(payload: dict[str, object]) -> dict[str, float | int]:
    decode_started = perf_counter_ns()
    iterations = int(payload.get("iterations", 100) or 100)
    depth_pct = float(payload.get("depth_pct", 1.0) or 1.0)
    initial_bids = _load_levels(payload.get("initial_bids", []))
    initial_asks = _load_levels(payload.get("initial_asks", []))
    updates = _load_updates(payload.get("updates", []))
    decode_ms = (perf_counter_ns() - decode_started) / 1_000_000.0

    if not updates:
        return {
            "iterations": iterations,
            "updates_per_iteration": 0,
            "total_updates": 0,
            "decode_ms": round(decode_ms, 3),
            "process_ms": 0.0,
            "total_ms": round(decode_ms, 3),
            "avg_update_us": 0.0,
            "p50_update_us": 0.0,
            "p95_update_us": 0.0,
            "p99_update_us": 0.0,
            "max_update_us": 0.0,
            "spread_avg": 0.0,
            "bid_depth_1pct_avg": 0.0,
            "ask_depth_1pct_avg": 0.0,
        }

    update_cost_us: list[float] = []
    spreads: list[float] = []
    bid_depths: list[float] = []
    ask_depths: list[float] = []

    process_started = perf_counter_ns()
    for _ in range(max(1, iterations)):
        bids = dict(initial_bids)
        asks = dict(initial_asks)
        for upd in updates:
            tick_started = perf_counter_ns()
            book = bids if upd.side == "BID" else asks
            if upd.size <= 0:
                book.pop(upd.price, None)
            else:
                book[upd.price] = upd.size

            best_bid = max(bids) if bids else 0.0
            best_ask = min(asks) if asks else 0.0
            if best_bid > 0 and best_ask > 0:
                spreads.append(best_ask - best_bid)
                bid_depths.append(_depth_within_pct(bids, best_bid, depth_pct, is_bid=True))
                ask_depths.append(_depth_within_pct(asks, best_ask, depth_pct, is_bid=False))

            update_cost_us.append((perf_counter_ns() - tick_started) / 1_000.0)
    process_ms = (perf_counter_ns() - process_started) / 1_000_000.0
    total_ms = decode_ms + process_ms

    return {
        "iterations": iterations,
        "updates_per_iteration": len(updates),
        "total_updates": iterations * len(updates),
        "decode_ms": round(decode_ms, 3),
        "process_ms": round(process_ms, 3),
        "total_ms": round(total_ms, 3),
        "avg_update_us": round(fmean(update_cost_us), 6),
        "p50_update_us": round(_percentile(update_cost_us, 50), 6),
        "p95_update_us": round(_percentile(update_cost_us, 95), 6),
        "p99_update_us": round(_percentile(update_cost_us, 99), 6),
        "max_update_us": round(max(update_cost_us), 6),
        "spread_avg": round(fmean(spreads), 8) if spreads else 0.0,
        "bid_depth_1pct_avg": round(fmean(bid_depths), 6) if bid_depths else 0.0,
        "ask_depth_1pct_avg": round(fmean(ask_depths), 6) if ask_depths else 0.0,
    }


def dump_payload_json(payload: dict[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=True)


def load_payload_json(raw: str) -> dict[str, object]:
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("payload must be json object")
    return parsed


def _load_levels(raw: object) -> dict[float, float]:
    rows: dict[float, float] = {}
    if not isinstance(raw, list):
        return rows
    for item in raw:
        if not isinstance(item, dict):
            continue
        price = float(item.get("price", 0.0) or 0.0)
        size = float(item.get("size", 0.0) or 0.0)
        if price <= 0 or size <= 0:
            continue
        rows[price] = size
    return rows


def _load_updates(raw: object) -> list[BookUpdate]:
    rows: list[BookUpdate] = []
    if not isinstance(raw, list):
        return rows
    for item in raw:
        if not isinstance(item, dict):
            continue
        side = str(item.get("side", "")).upper().strip()
        if side not in {"BID", "ASK"}:
            continue
        price = float(item.get("price", 0.0) or 0.0)
        if price <= 0:
            continue
        size = float(item.get("size", 0.0) or 0.0)
        rows.append(BookUpdate(side=side, price=price, size=size))
    return rows


def _depth_within_pct(levels: dict[float, float], best: float, pct: float, *, is_bid: bool) -> float:
    if best <= 0:
        return 0.0
    total = 0.0
    for price, size in levels.items():
        if is_bid:
            dist = ((best - price) / best) * 100 if best > 0 else 1000
        else:
            dist = ((price - best) / best) * 100 if best > 0 else 1000
        if dist <= pct:
            total += size
    return total


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    idx = int(round((p / 100.0) * (len(sorted_values) - 1)))
    idx = max(0, min(idx, len(sorted_values) - 1))
    return float(sorted_values[idx])
