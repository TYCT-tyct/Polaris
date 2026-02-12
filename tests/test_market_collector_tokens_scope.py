from __future__ import annotations

import pytest

from polaris.harvest.collector_markets import MarketCollector


class _FakeDb:
    def __init__(self, mapped_rows: list[dict], candidate_rows: list[dict], fallback_rows: list[dict]) -> None:
        self.mapped_rows = mapped_rows
        self.candidate_rows = candidate_rows
        self.fallback_rows = fallback_rows
        self.calls: list[str] = []

    async def fetch_all(self, sql: str, params=None):  # pragma: no cover - simple test double
        self.calls.append(sql)
        if "bridge_market_tracking" in sql:
            return self.mapped_rows
        if "m.question" in sql and "m.slug" in sql:
            return self.candidate_rows
        return self.fallback_rows


@pytest.mark.asyncio
async def test_list_active_tokens_watchlist_uses_mapping_first() -> None:
    mapped = [{"token_id": "t1", "market_id": "m1", "outcome_label": "Yes", "outcome_side": "YES"}]
    fallback = [{"token_id": "t2", "market_id": "m2", "outcome_label": "No", "outcome_side": "NO"}]
    db = _FakeDb(mapped_rows=mapped, candidate_rows=[], fallback_rows=fallback)
    collector = MarketCollector(
        db=db,
        gamma_client=object(),
        market_scope="watchlist_tweet",
        market_tweet_targets=["elon musk", "andrew tate", "donald trump"],
    )

    rows = await collector.list_active_tokens()

    assert rows == mapped
    assert len(db.calls) == 2
    assert "bridge_market_tracking" in db.calls[0]


@pytest.mark.asyncio
async def test_list_active_tokens_watchlist_filters_by_targets_when_mapping_empty() -> None:
    candidates = [
        {
            "token_id": "t2",
            "market_id": "m2",
            "outcome_label": "No",
            "outcome_side": "NO",
            "question": "Elon Musk # tweets Feb 10 - Feb 17?",
            "slug": "elon-musk-tweets",
        },
        {
            "token_id": "t3",
            "market_id": "m3",
            "outcome_label": "Yes",
            "outcome_side": "YES",
            "question": "NBA finals winner?",
            "slug": "nba-finals-winner",
        },
    ]
    db = _FakeDb(mapped_rows=[], candidate_rows=candidates, fallback_rows=[])
    collector = MarketCollector(
        db=db,
        gamma_client=object(),
        market_scope="watchlist_tweet",
        market_tweet_targets=["elon musk", "andrew tate", "donald trump"],
    )

    rows = await collector.list_active_tokens()

    assert rows == [{"token_id": "t2", "market_id": "m2", "outcome_label": "No", "outcome_side": "NO"}]
    assert len(db.calls) == 2
    assert "bridge_market_tracking" in db.calls[0]
    assert "m.question" in db.calls[1]


@pytest.mark.asyncio
async def test_list_active_tokens_all_scope_skips_mapping_query() -> None:
    fallback = [{"token_id": "t2", "market_id": "m2", "outcome_label": "No", "outcome_side": "NO"}]
    db = _FakeDb(mapped_rows=[], candidate_rows=[], fallback_rows=fallback)
    collector = MarketCollector(db=db, gamma_client=object(), market_scope="all")

    rows = await collector.list_active_tokens()

    assert rows == fallback
    assert len(db.calls) == 1
    assert "bridge_market_tracking" not in db.calls[0]
