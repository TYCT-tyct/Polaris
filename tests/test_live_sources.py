from __future__ import annotations

import pytest

from polaris.config import RetryConfig
from polaris.harvest.discovery import discover_target_markets
from polaris.infra.rate_limiter import AsyncTokenBucket
from polaris.sources.clob_client import ClobClient
from polaris.sources.gamma_client import GammaClient
from polaris.sources.xtracker_client import XTrackerClient


@pytest.mark.asyncio
@pytest.mark.live
async def test_live_xtracker_endpoints() -> None:
    client = XTrackerClient(AsyncTokenBucket(1.0, 2), RetryConfig())
    try:
        user = await client.get_user("elonmusk")
        assert user.account_id
        trackings = await client.get_trackings("elonmusk", active_only=True)
        assert isinstance(trackings, list)
        posts_hash, posts, _ = await client.get_posts("elonmusk")
        assert len(posts_hash) == 64
        assert len(posts) > 0
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.live
async def test_live_gamma_and_clob_endpoints() -> None:
    gamma = GammaClient(AsyncTokenBucket(2.0, 4), RetryConfig())
    clob = ClobClient(AsyncTokenBucket(5.0, 8), RetryConfig())
    try:
        rows = await discover_target_markets(gamma, scope="all")
        assert rows, "no target markets discovered"
        tokens = []
        for row in rows:
            tokens.extend(gamma.token_descriptors(row))
        assert tokens, "no token descriptors found"
        sample_ids = [token.token_id for token in tokens[:100]]
        books = await clob.get_books(sample_ids)
        assert books, "no active orderbooks returned for sampled tokens"
    finally:
        await gamma.close()
        await clob.close()
