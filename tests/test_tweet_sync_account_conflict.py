from __future__ import annotations

from datetime import UTC, datetime

import pytest

from polaris.harvest.collector_tweets import TweetCollector
from polaris.sources.models import XTrackerUser


class _UserOnlyClient:
    async def get_user(self, handle: str) -> XTrackerUser:
        return XTrackerUser.model_validate(
            {
                "id": "account-new",
                "handle": handle,
                "platformId": "44196397",
                "name": "Elon Musk Updated",
                "post_count": 3,
                "_count": {"posts": 3},
                "lastSync": datetime.now(tz=UTC).isoformat(),
            }
        )


@pytest.mark.asyncio
async def test_sync_account_reuses_existing_handle_owner(db) -> None:
    await db.execute("truncate table dim_account cascade")
    await db.execute(
        """
        insert into dim_account(
            account_id, handle, platform_id, display_name, avatar_url, bio, verified,
            post_count, last_sync, created_at, updated_at
        )
        values (
            'account-old', 'elonmusk', 'legacy', 'Legacy Name', null, null, false,
            1, now(), now(), now()
        )
        """
    )

    collector = TweetCollector(db, _UserOnlyClient())
    user = await collector.sync_account("elonmusk")
    assert user.account_id == "account-old"

    row = await db.fetch_one("select account_id, display_name, post_count from dim_account where handle = 'elonmusk'")
    assert row is not None
    assert row["account_id"] == "account-old"
    assert row["display_name"] == "Elon Musk Updated"
    assert int(row["post_count"]) == 3
