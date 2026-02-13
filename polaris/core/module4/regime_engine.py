from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from polaris.db.pool import Database

from .types import RegimeAdjustment


def compute_progress(now: datetime, start_ts: datetime, end_ts: datetime) -> float:
    if now <= start_ts:
        return 0.0
    if now >= end_ts:
        return 1.0
    total = (end_ts - start_ts).total_seconds()
    if total <= 0:
        return 1.0
    done = (now - start_ts).total_seconds()
    return max(0.0, min(1.0, done / total))


class RegimeEngine:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def estimate(
        self,
        *,
        account_id: str,
        progress: float,
        as_of: datetime | None = None,
    ) -> RegimeAdjustment:
        now = as_of or datetime.now(tz=UTC)
        row = await self._db.fetch_one(
            """
            select
                count(*) filter (where posted_at >= %s) as c30,
                count(*) filter (where posted_at >= %s) as c120
            from fact_tweet_post
            where account_id = %s
              and posted_at >= %s
            """,
            (
                now - timedelta(minutes=30),
                now - timedelta(minutes=120),
                account_id,
                now - timedelta(minutes=120),
            ),
        )
        posts_30m = int(row["c30"] if row and row["c30"] is not None else 0)
        posts_120m = int(row["c120"] if row and row["c120"] is not None else 0)
        progress_eff = float(max(0.01, min(0.995, progress + _progress_adjust(posts_30m, posts_120m))))
        tail_multiplier = float(_tail_multiplier(posts_30m, posts_120m))
        return RegimeAdjustment(
            posts_30m=posts_30m,
            posts_120m=posts_120m,
            progress=progress,
            progress_effective=progress_eff,
            tail_multiplier=tail_multiplier,
            metadata={
                "burst": posts_30m >= 4,
                "quiet": posts_120m == 0,
            },
        )


def _progress_adjust(posts_30m: int, posts_120m: int) -> float:
    adjust = 0.0
    if posts_30m >= 4:
        adjust -= 0.08
    elif posts_30m >= 2:
        adjust -= 0.04
    if posts_120m == 0 and posts_30m == 0:
        adjust += 0.03
    return adjust


def _tail_multiplier(posts_30m: int, posts_120m: int) -> float:
    if posts_30m >= 4:
        return 1.2
    if posts_30m >= 2:
        return 1.1
    if posts_120m == 0:
        return 0.85
    return 1.0


def apply_uncertainty_mix(
    pmf: dict[str, float],
    uncertainty_delta: float,
) -> dict[str, float]:
    epsilon = max(0.0, min(0.5, uncertainty_delta))
    if epsilon <= 0:
        return dict(pmf)
    n = max(1, len(pmf))
    uniform = 1.0 / n
    out = {k: ((1.0 - epsilon) * float(v)) + (epsilon * uniform) for k, v in pmf.items()}
    total = sum(max(0.0, x) for x in out.values())
    if total <= 0:
        return {k: uniform for k in pmf}
    return {k: max(0.0, v) / total for k, v in out.items()}


def as_jsonable(value: RegimeAdjustment) -> dict[str, Any]:
    return {
        "posts_30m": value.posts_30m,
        "posts_120m": value.posts_120m,
        "progress": value.progress,
        "progress_effective": value.progress_effective,
        "tail_multiplier": value.tail_multiplier,
        "metadata": value.metadata,
    }
