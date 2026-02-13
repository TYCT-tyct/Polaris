from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from polaris.db.pool import Database


class EvidenceTools:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def fetch_recent_posts(
        self,
        *,
        account_id: str,
        lookback_minutes: int,
        as_of: datetime | None = None,
        limit: int = 120,
    ) -> list[dict[str, Any]]:
        now = as_of or datetime.now(tz=UTC)
        rows = await self._db.fetch_all(
            """
            select platform_post_id, content, posted_at
            from fact_tweet_post
            where account_id = %s
              and posted_at >= %s
              and posted_at <= %s
            order by posted_at desc
            limit %s
            """,
            (
                account_id,
                now - timedelta(minutes=max(1, lookback_minutes)),
                now,
                max(1, limit),
            ),
        )
        return rows

    async def keyword_hits(
        self,
        *,
        account_id: str,
        lookback_minutes: int,
        keywords: tuple[str, ...],
        as_of: datetime | None = None,
    ) -> int:
        if not keywords:
            return 0
        now = as_of or datetime.now(tz=UTC)
        clauses = " or ".join(["content ilike %s"] * len(keywords))
        params: list[Any] = [
            account_id,
            now - timedelta(minutes=max(1, lookback_minutes)),
            now,
        ]
        params.extend([f"%{k}%" for k in keywords])
        row = await self._db.fetch_one(
            f"""
            select count(*)::int as c
            from fact_tweet_post
            where account_id = %s
              and posted_at >= %s
              and posted_at <= %s
              and ({clauses})
            """,
            tuple(params),
        )
        return int(row["c"] if row else 0)
