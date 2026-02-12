from datetime import UTC, datetime

from polaris.harvest.mapper_market_tracking import _handle_keywords, _market_matches_handle, overlap_score


def test_overlap_score() -> None:
    m_start = datetime(2026, 2, 1, tzinfo=UTC)
    m_end = datetime(2026, 2, 3, tzinfo=UTC)
    t_start = datetime(2026, 2, 1, 12, tzinfo=UTC)
    t_end = datetime(2026, 2, 2, 12, tzinfo=UTC)
    score = overlap_score(m_start, m_end, t_start, t_end)
    assert 0.4 <= score <= 0.6


def test_handle_keywords_known_handles() -> None:
    assert "elon musk" in _handle_keywords("elonmusk")
    assert "andrew tate" in _handle_keywords("cobratate")
    assert "donald trump" in _handle_keywords("realdonaldtrump")


def test_market_matches_handle() -> None:
    market = {"question": "Elon Musk # tweets Feb 10 - Feb 17?", "slug": "elon-musk-tweets-feb-10-17"}
    assert _market_matches_handle(market, _handle_keywords("elonmusk"))
    assert not _market_matches_handle(market, _handle_keywords("cobratate"))
