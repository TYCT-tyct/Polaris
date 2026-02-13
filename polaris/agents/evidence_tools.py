from __future__ import annotations

from datetime import UTC, datetime, timedelta

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
    ) -> list[dict]:
        now = as_of or datetime.now(tz=UTC)
        rows = await self._db.fetch_all(
            """
            select
                platform_post_id,
                source_post_id,
                content,
                posted_at,
                imported_at,
                char_len,
                is_reply,
                is_retweet
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
