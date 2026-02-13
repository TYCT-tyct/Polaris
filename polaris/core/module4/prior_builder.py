from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from polaris.db.pool import Database

from .buckets import parse_bucket_label, representative_count
from .types import PriorBuildResult


@dataclass(frozen=True)
class PriorBuilderConfig:
    spread_ref: float = 0.08
    depth_ref: float = 250.0
    sample_ref: int = 20
    tail_default_short: int = 8
    tail_default_week: int = 20


class MarketPriorBuilder:
    def __init__(self, db: Database, config: PriorBuilderConfig | None = None) -> None:
        self._db = db
        self._cfg = config or PriorBuilderConfig()

    async def build(self, market_id: str, *, window_code: str) -> PriorBuildResult | None:
        quote_rows = await self._db.fetch_all(
            """
            select
                t.token_id,
                t.outcome_label,
                q.best_bid,
                q.best_ask,
                q.mid,
                q.spread
            from dim_token t
            left join view_quote_latest q on q.token_id = t.token_id
            where t.market_id = %s
            order by t.outcome_index asc, t.token_id asc
            """,
            (market_id,),
        )
        if not quote_rows:
            return None

        depth_rows = await self._db.fetch_all(
            """
            with latest as (
                select distinct on (token_id)
                    token_id,
                    bid_depth_2pct,
                    ask_depth_2pct
                from fact_quote_depth_raw
                where market_id = %s
                order by token_id, captured_at desc
            )
            select token_id, bid_depth_2pct, ask_depth_2pct
            from latest
            """,
            (market_id,),
        )
        depth_by_token = {
            str(row["token_id"]): (float(row["bid_depth_2pct"] or 0.0) + float(row["ask_depth_2pct"] or 0.0))
            for row in depth_rows
        }
        token_ids = [str(row["token_id"]) for row in quote_rows]
        sample_count_row = await self._db.fetch_one(
            """
            select count(*)::int as c
            from fact_quote_top_raw
            where token_id = any(%s::text[])
              and captured_at >= now() - interval '10 minutes'
            """,
            (token_ids,),
        )
        sample_count_10m = int(sample_count_row["c"] if sample_count_row else 0)

        label_prices: dict[str, float] = {}
        spread_values: list[float] = []
        depth_values: list[float] = []
        for row in quote_rows:
            label = str(row.get("outcome_label") or "").strip()
            if not label:
                continue
            price = _resolve_price(row)
            if price is None:
                continue
            label_prices[label] = max(price, label_prices.get(label, 0.0))
            spread = row.get("spread")
            if spread is not None:
                spread_values.append(float(spread))
            token_depth = depth_by_token.get(str(row["token_id"]))
            if token_depth is not None:
                depth_values.append(token_depth)

        if not label_prices:
            return None
        pmf = _normalize_distribution(label_prices)
        spread_mean = (sum(spread_values) / len(spread_values)) if spread_values else self._cfg.spread_ref
        depth_mean = (sum(depth_values) / len(depth_values)) if depth_values else 0.0

        spread_quality = max(0.0, 1.0 - (spread_mean / max(0.0001, self._cfg.spread_ref)))
        depth_quality = min(1.0, depth_mean / max(1.0, self._cfg.depth_ref))
        sample_quality = min(1.0, sample_count_10m / max(1, self._cfg.sample_ref))
        coverage_quality = min(1.0, len(pmf) / max(2.0, len(quote_rows) / 2.0))
        reliability = (
            (0.45 * spread_quality)
            + (0.25 * depth_quality)
            + (0.20 * sample_quality)
            + (0.10 * coverage_quality)
        )
        reliability = max(0.0, min(1.0, reliability))

        tail_default = self._cfg.tail_default_short if window_code == "short" else self._cfg.tail_default_week
        expected_count = 0.0
        for label, p in pmf.items():
            bucket = parse_bucket_label(label)
            expected_count += p * representative_count(bucket, tail_default=tail_default)
        return PriorBuildResult(
            market_id=market_id,
            pmf=pmf,
            prior_reliability=reliability,
            expected_count=expected_count,
            spread_mean=spread_mean,
            sample_count_10m=sample_count_10m,
            metadata={
                "depth_mean_2pct": depth_mean,
                "spread_quality": spread_quality,
                "depth_quality": depth_quality,
                "sample_quality": sample_quality,
                "coverage_quality": coverage_quality,
                "window_code": window_code,
            },
        )


def _resolve_price(row: dict[str, Any]) -> float | None:
    mid = row.get("mid")
    if mid is not None:
        return _clip_price(float(mid))
    bid = row.get("best_bid")
    ask = row.get("best_ask")
    if bid is not None and ask is not None:
        return _clip_price((float(bid) + float(ask)) / 2.0)
    if ask is not None:
        return _clip_price(float(ask))
    if bid is not None:
        return _clip_price(float(bid))
    return None


def _clip_price(value: float) -> float:
    return max(0.0001, min(0.9999, value))


def _normalize_distribution(values: dict[str, float]) -> dict[str, float]:
    total = sum(max(0.0, x) for x in values.values())
    if total <= 0:
        n = max(1, len(values))
        uniform = 1.0 / n
        return {k: uniform for k in values}
    return {k: max(0.0, v) / total for k, v in values.items()}
