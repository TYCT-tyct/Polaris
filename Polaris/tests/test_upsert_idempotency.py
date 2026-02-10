from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from polaris.harvest.collector_tweets import TweetCollector
from polaris.sources.models import XTrackerMetricPoint, XTrackerPost, XTrackerTracking, XTrackerUser


class FakeXTrackerClient:
    async def get_user(self, handle: str) -> XTrackerUser:
        return XTrackerUser.model_validate(
            {
                "id": "account-1",
                "handle": handle,
                "platformId": "44196397",
                "name": "Elon Musk",
                "_count": {"posts": 2},
                "lastSync": datetime.now(tz=UTC).isoformat(),
            }
        )

    async def get_trackings(self, handle: str, active_only: bool = True) -> list[XTrackerTracking]:
        return [
            XTrackerTracking.model_validate(
                {
                    "id": "tracking-1",
                    "userId": "account-1",
                    "title": "Window",
                    "startDate": (datetime.now(tz=UTC) - timedelta(days=2)).isoformat(),
                    "endDate": datetime.now(tz=UTC).isoformat(),
                    "isActive": True,
                }
            )
        ]

    async def get_metrics(self, user_id: str, start_date: datetime, end_date: datetime) -> list[XTrackerMetricPoint]:
        return [
            XTrackerMetricPoint.model_validate(
                {
                    "id": "m-1",
                    "userId": user_id,
                    "date": datetime.now(tz=UTC).isoformat(),
                    "type": "daily",
                    "tweet_count": 2,
                    "cumulative_count": 5,
                    "tracking_id": "tracking-1",
                }
            )
        ]

    async def get_posts(self, handle: str) -> tuple[str, list[XTrackerPost], dict]:
        posts = [
            XTrackerPost.model_validate(
                {
                    "id": "source-2",
                    "userId": "account-1",
                    "platformId": "2",
                    "content": "hello",
                    "createdAt": datetime.now(tz=UTC).isoformat(),
                    "importedAt": datetime.now(tz=UTC).isoformat(),
                }
            ),
            XTrackerPost.model_validate(
                {
                    "id": "source-1",
                    "userId": "account-1",
                    "platformId": "1",
                    "content": "world",
                    "createdAt": datetime.now(tz=UTC).isoformat(),
                    "importedAt": datetime.now(tz=UTC).isoformat(),
                }
            ),
        ]
        return "fixed-hash", posts, {"success": True}


@pytest.mark.asyncio
async def test_tweet_upserts_are_idempotent(db) -> None:
    await db.execute(
        "truncate table fact_tweet_post, fact_tweet_metric_daily, dim_tracking_window, dim_account, ops_cursor cascade"
    )
    collector = TweetCollector(db, FakeXTrackerClient())
    user = await collector.sync_account("elonmusk")
    trackings = await collector.sync_trackings("elonmusk")
    await collector.sync_metrics(user, trackings)
    rows_first = await collector.sync_posts_incremental("elonmusk", user.account_id)
    rows_second = await collector.sync_posts_incremental("elonmusk", user.account_id)
    count = await db.fetch_one("select count(*) as c from fact_tweet_post")
    assert rows_first == 2
    assert rows_second == 0
    assert int(count["c"]) == 2
