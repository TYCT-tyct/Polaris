from datetime import UTC, datetime

from polaris.harvest.mapper_market_tracking import overlap_score


def test_overlap_score() -> None:
    m_start = datetime(2026, 2, 1, tzinfo=UTC)
    m_end = datetime(2026, 2, 3, tzinfo=UTC)
    t_start = datetime(2026, 2, 1, 12, tzinfo=UTC)
    t_end = datetime(2026, 2, 2, 12, tzinfo=UTC)
    score = overlap_score(m_start, m_end, t_start, t_end)
    assert 0.4 <= score <= 0.6

