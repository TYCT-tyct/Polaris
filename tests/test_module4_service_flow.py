from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from polaris.config import PolarisSettings
from polaris.core.module4.service import Module4Service


@pytest.mark.asyncio
async def test_module4_service_run_and_score(db) -> None:
    now = datetime.now(tz=UTC)
    start = now - timedelta(hours=6)
    end = now - timedelta(minutes=10)

    await db.execute(
        """
        insert into dim_account(account_id, handle, platform_id, display_name, verified, created_at, updated_at)
        values (%s, %s, %s, %s, %s, now(), now())
        on conflict (account_id) do nothing
        """,
        ("acct-elon", "elonmusk", "elon-platform", "Elon Musk", True),
    )
    await db.execute(
        """
        insert into dim_tracking_window(
            tracking_id, account_id, title, start_date, end_date, is_active, created_at, updated_at
        )
        values (%s, %s, %s, %s, %s, %s, now(), now())
        on conflict (tracking_id, account_id) do update
        set start_date = excluded.start_date,
            end_date = excluded.end_date,
            is_active = excluded.is_active,
            updated_at = now()
        """,
        ("trk-elon", "acct-elon", "Elon window", start, end, True),
    )
    await db.execute(
        """
        insert into dim_market(
            market_id, gamma_market_id, condition_id, question, slug, event_slug, category,
            end_date, start_date, neg_risk, active, closed, archived, spread, liquidity, volume,
            updated_from_source_at, captured_at, description
        )
        values (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            now(), now(), %s
        )
        on conflict (market_id) do update
        set question = excluded.question,
            slug = excluded.slug,
            start_date = excluded.start_date,
            end_date = excluded.end_date,
            active = excluded.active,
            closed = excluded.closed,
            description = excluded.description
        """,
        (
            "mkt-elon-1",
            101,
            "cond-elon-1",
            "How many tweets will Elon post?",
            "elon-tweet-count-test",
            "elon-event",
            "social",
            end,
            start,
            False,
            True,
            False,
            False,
            0.02,
            1000.0,
            10000.0,
            "Main feed posts, quote posts and reposts count. Replies do not count. Resolution source: XTracker Post Counter.",
        ),
    )
    await db.execute(
        """
        insert into dim_token(
            token_id, market_id, outcome_label, outcome_side, tick_size, min_order_size, captured_at,
            outcome_index, is_other_outcome, is_placeholder_outcome
        )
        values
            (%s, %s, %s, %s, %s, %s, now(), %s, false, false),
            (%s, %s, %s, %s, %s, %s, now(), %s, false, false)
        on conflict (token_id) do update
        set outcome_label = excluded.outcome_label,
            outcome_side = excluded.outcome_side
        """,
        (
            "tok-elon-low",
            "mkt-elon-1",
            "0-49",
            "yes",
            0.01,
            5.0,
            0,
            "tok-elon-high",
            "mkt-elon-1",
            "50+",
            "yes",
            0.01,
            5.0,
            1,
        ),
    )
    await db.execute(
        """
        insert into bridge_market_tracking(
            market_id, tracking_id, account_id, match_score, match_reason, mapped_at
        )
        values (%s, %s, %s, %s, %s, now())
        on conflict (market_id, tracking_id) do update
        set account_id = excluded.account_id,
            match_score = excluded.match_score,
            match_reason = excluded.match_reason,
            mapped_at = excluded.mapped_at
        """,
        ("mkt-elon-1", "trk-elon", "acct-elon", 0.99, "test_map"),
    )
    await db.execute(
        """
        insert into fact_quote_top_raw(
            token_id, market_id, best_bid, best_ask, mid, spread, last_trade_price, source_ts, captured_at
        )
        values
            (%s, %s, 0.58, 0.62, 0.60, 0.04, 0.60, now(), now()),
            (%s, %s, 0.38, 0.42, 0.40, 0.04, 0.40, now(), now())
        """,
        ("tok-elon-low", "mkt-elon-1", "tok-elon-high", "mkt-elon-1"),
    )
    await db.execute(
        """
        insert into fact_quote_depth_raw(
            token_id, market_id, bid_depth_2pct, ask_depth_2pct, bid_depth_5pct, ask_depth_5pct, source_ts, captured_at
        )
        values
            (%s, %s, 300, 280, 400, 360, now(), now()),
            (%s, %s, 280, 260, 360, 340, now(), now())
        """,
        ("tok-elon-low", "mkt-elon-1", "tok-elon-high", "mkt-elon-1"),
    )
    await db.execute(
        """
        insert into fact_quote_1m(
            minute_bucket, token_id, market_id, bid_open, bid_close, ask_open, ask_close, mid_open, mid_close,
            spread_avg, spread_min, spread_max, snapshots, source_sample, created_at
        )
        values
            (%s, %s, %s, 0.58, 0.58, 0.62, 0.62, 0.60, 0.60, 0.04, 0.03, 0.05, 5, 5, now()),
            (%s, %s, %s, 0.38, 0.38, 0.42, 0.42, 0.40, 0.40, 0.04, 0.03, 0.05, 5, 5, now())
        """,
        (
            now.replace(second=0, microsecond=0) - timedelta(minutes=1),
            "tok-elon-low",
            "mkt-elon-1",
            now.replace(second=0, microsecond=0) - timedelta(minutes=1),
            "tok-elon-high",
            "mkt-elon-1",
        ),
    )
    await db.execute(
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
        ("acct-elon", "trk-elon", end.date(), 8, 58, "metric-test"),
    )
    await db.execute(
        """
        insert into fact_tweet_metric_1m(
            account_id, tracking_id, minute_bucket, cumulative_count_est, cumulative_count_metric,
            cumulative_count_posts, source_confidence, metadata, captured_at
        )
        values (%s, %s, %s, %s, %s, %s, %s, '{}'::jsonb, now())
        on conflict (account_id, tracking_id, minute_bucket) do update
        set cumulative_count_est = excluded.cumulative_count_est
        """,
        ("acct-elon", "trk-elon", end.replace(second=0, microsecond=0), 58, 58, 58, 0.9),
    )
    await db.execute(
        """
        insert into fact_tweet_post(
            platform_post_id, account_id, source_post_id, content, posted_at, imported_at,
            is_reply, is_retweet, has_url, language_guess, char_len, content_hash, raw_payload, captured_at
        )
        values
            (%s, %s, %s, %s, %s, now(), false, false, false, 'en', %s, %s, '{}'::jsonb, now()),
            (%s, %s, %s, %s, %s, now(), false, false, false, 'en', %s, %s, '{}'::jsonb, now())
        on conflict (platform_post_id) do nothing
        """,
        (
            "post-1",
            "acct-elon",
            "source-post-1",
            "Starship update soon",
            end - timedelta(minutes=30),
            len("Starship update soon"),
            "hash-1",
            "post-2",
            "acct-elon",
            "source-post-2",
            "Interesting policy thread",
            end - timedelta(minutes=20),
            len("Interesting policy thread"),
            "hash-2",
        ),
    )

    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        m4_agent_enabled=True,
    )
    service = Module4Service(db, settings)
    compiled = await service.compile_rules(force=True)
    assert compiled, "rule compiler should emit at least one version"

    summary = await service.run_once(
        mode="paper_live",
        source_code="module4",
        run_tag="m4-test",
        market_id="mkt-elon-1",
        use_agent=True,
    )
    assert summary.markets_total >= 1
    assert summary.snapshots_written >= 3
    assert summary.decisions_written >= 1

    score = await service.score(
        mode="paper_live",
        source_code="module4",
        run_tag="m4-test",
        window_code="fused",
        since_hours=24,
        persist=True,
    )
    assert score.markets_scored >= 1
    assert score.log_score is not None

    score_rows = await db.fetch_all(
        """
        select run_tag, mode, source_code, window_code, score_total
        from m4_model_score
        where run_tag = %s
        order by created_at desc
        """,
        ("m4-test",),
    )
    assert score_rows, "score should be persisted"

