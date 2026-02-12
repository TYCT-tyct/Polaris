from __future__ import annotations

import re
from datetime import datetime, timezone

from polaris.db.pool import Database
from polaris.sources.models import XTrackerPost, XTrackerTracking, XTrackerUser
from polaris.sources.xtracker_client import XTrackerClient

URL_REGEX = re.compile(r"https?://\S+|t\.co/\S+", re.IGNORECASE)


class TweetCollector:
    def __init__(self, db: Database, client: XTrackerClient) -> None:
        self.db = db
        self.client = client

    async def sync_account(self, handle: str) -> XTrackerUser:
        user = await self.client.get_user(handle)
        row = await self.db.fetch_one(
            """
            update dim_account
            set handle = %s,
                platform_id = %s,
                display_name = %s,
                avatar_url = %s,
                bio = %s,
                verified = %s,
                post_count = %s,
                last_sync = %s,
                updated_at = now()
            where account_id = %s
            returning account_id
            """,
            (
                user.handle,
                user.platform_id,
                user.name,
                user.avatar_url,
                user.bio,
                user.verified,
                user.post_count,
                user.last_sync,
                user.account_id,
            ),
        )
        if row is None:
            row = await self.db.fetch_one(
                """
                insert into dim_account(
                    account_id, handle, platform_id, display_name, avatar_url, bio, verified,
                    post_count, last_sync, created_at, updated_at
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
                on conflict (handle) do update
                set platform_id = excluded.platform_id,
                    display_name = excluded.display_name,
                    avatar_url = excluded.avatar_url,
                    bio = excluded.bio,
                    verified = excluded.verified,
                    post_count = excluded.post_count,
                    last_sync = excluded.last_sync,
                    updated_at = now()
                returning account_id
                """,
                (
                    user.account_id,
                    user.handle,
                    user.platform_id,
                    user.name,
                    user.avatar_url,
                    user.bio,
                    user.verified,
                    user.post_count,
                    user.last_sync,
                ),
            )
        canonical_account_id = row["account_id"] if row else user.account_id
        if canonical_account_id != user.account_id:
            return user.model_copy(update={"account_id": canonical_account_id})
        return user

    async def sync_trackings(self, handle: str, account_id: str | None = None) -> list[XTrackerTracking]:
        trackings = await self.client.get_trackings(handle, active_only=True)
        rows = [
            (
                t.tracking_id,
                account_id or t.account_id,
                t.title,
                t.start_date,
                t.end_date,
                t.is_active,
            )
            for t in trackings
        ]
        if rows:
            await self.db.executemany(
                """
                insert into dim_tracking_window(
                    tracking_id, account_id, title, start_date, end_date, is_active, created_at, updated_at
                )
                values (%s, %s, %s, %s, %s, %s, now(), now())
                on conflict (tracking_id, account_id) do update
                set title = excluded.title,
                    start_date = excluded.start_date,
                    end_date = excluded.end_date,
                    is_active = excluded.is_active,
                    updated_at = now()
                """,
                rows,
            )
        return trackings

    async def sync_metrics(self, user: XTrackerUser, trackings: list[XTrackerTracking]) -> int:
        total_rows = 0
        for tracking in trackings:
            points = await self.client.get_metrics(user.account_id, tracking.start_date, tracking.end_date)
            rows = [
                (
                    p.account_id,
                    p.tracking_id or tracking.tracking_id,
                    p.metric_date.date(),
                    p.tweet_count,
                    p.cumulative_count,
                    p.metric_id,
                )
                for p in points
            ]
            if not rows:
                continue
            total_rows += len(rows)
            await self.db.executemany(
                """
                insert into fact_tweet_metric_daily(
                    account_id, tracking_id, metric_date, tweet_count, cumulative_count, source_metric_id, captured_at
                )
                values (%s, %s, %s, %s, %s, %s, now())
                on conflict (account_id, tracking_id, metric_date) do update
                set tweet_count = excluded.tweet_count,
                    cumulative_count = excluded.cumulative_count,
                    source_metric_id = excluded.source_metric_id,
                    captured_at = excluded.captured_at
                """,
                rows,
            )
        return total_rows

    async def sync_posts_incremental(self, handle: str, account_id: str) -> int:
        payload_hash, posts, raw_payload = await self.client.get_posts(handle)
        hash_key = f"xtracker:{handle}:posts_hash"
        last_id_key = f"xtracker:{handle}:last_post_id"
        old_hash = await self.db.get_cursor(hash_key)
        if old_hash and old_hash["cursor_value"] == payload_hash:
            await self.db.set_cursor(
                f"xtracker:{handle}:posts_heartbeat",
                datetime.now(tz=timezone.utc).isoformat(),
                {"state": "no_change"},
            )
            return 0

        old_last = await self.db.get_cursor(last_id_key)
        last_seen = int(old_last["cursor_value"]) if old_last else None
        new_posts = _collect_new_posts(posts, last_seen)
        rows = [to_post_row(post, account_id) for post in new_posts]
        if rows:
            await self.db.executemany(
                """
                insert into fact_tweet_post(
                    platform_post_id, account_id, source_post_id, content, posted_at, imported_at,
                    is_reply, is_retweet, has_url, language_guess, char_len, content_hash, raw_payload, captured_at
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, now())
                on conflict (platform_post_id) do update
                set content = excluded.content,
                    posted_at = excluded.posted_at,
                    imported_at = excluded.imported_at,
                    is_reply = excluded.is_reply,
                    is_retweet = excluded.is_retweet,
                    has_url = excluded.has_url,
                    language_guess = excluded.language_guess,
                    char_len = excluded.char_len,
                    content_hash = excluded.content_hash,
                    raw_payload = excluded.raw_payload,
                    captured_at = excluded.captured_at
                """,
                rows,
            )
        latest_id = posts[0].platform_post_id if posts else (str(last_seen) if last_seen else "0")
        await self.db.set_cursor(hash_key, payload_hash, {"post_count": len(posts)})
        await self.db.set_cursor(last_id_key, latest_id, {"new_rows": len(rows)})
        return len(rows)


def _collect_new_posts(posts: list[XTrackerPost], last_seen_id: int | None) -> list[XTrackerPost]:
    if last_seen_id is None:
        return posts
    fresh: list[XTrackerPost] = []
    for post in posts:
        current_id = int(post.platform_post_id)
        if current_id <= last_seen_id:
            break
        fresh.append(post)
    return fresh


def to_post_row(post: XTrackerPost, account_id: str) -> tuple:
    content = post.content or ""
    normalized = content.strip()
    is_reply = normalized.startswith("@")
    is_retweet = normalized.upper().startswith("RT ")
    has_url = bool(URL_REGEX.search(normalized))
    language_guess = _guess_language(normalized)
    return (
        post.platform_post_id,
        account_id,
        post.post_id,
        normalized,
        post.posted_at,
        post.imported_at,
        is_reply,
        is_retweet,
        has_url,
        language_guess,
        len(normalized),
        _sha1(normalized),
        _raw_payload_json(post),
    )


def _guess_language(text: str) -> str:
    if not text:
        return "unknown"
    if re.search(r"[\u4e00-\u9fff]", text):
        return "zh"
    if re.search(r"[а-яА-Я]", text):
        return "ru"
    return "en"


def _sha1(text: str) -> str:
    import hashlib

    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _raw_payload_json(post: XTrackerPost) -> str:
    return post.model_dump_json(by_alias=True, exclude_none=True)
