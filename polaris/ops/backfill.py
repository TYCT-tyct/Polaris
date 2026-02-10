from __future__ import annotations

from datetime import datetime

from polaris.harvest.collector_markets import MarketCollector
from polaris.harvest.collector_tweets import TweetCollector


class BackfillService:
    def __init__(self, market_collector: MarketCollector, tweet_collector: TweetCollector) -> None:
        self.market_collector = market_collector
        self.tweet_collector = tweet_collector

    async def run(self, handle: str, start: datetime, end: datetime) -> dict[str, int]:
        user = await self.tweet_collector.sync_account(handle)
        trackings = await self.tweet_collector.sync_trackings(handle)
        trackings = [t for t in trackings if _overlap(t.start_date, t.end_date, start, end)]
        metrics = await self.tweet_collector.sync_metrics(user, trackings)
        posts = await self.tweet_collector.sync_posts_incremental(handle, user.account_id)
        markets, tokens = await self.market_collector.run_once()
        return {
            "metrics_rows": metrics,
            "posts_rows": posts,
            "markets_rows": markets,
            "tokens_rows": tokens,
        }


def _overlap(start1: datetime, end1: datetime, start2: datetime, end2: datetime) -> bool:
    return min(end1, end2) >= max(start1, start2)

