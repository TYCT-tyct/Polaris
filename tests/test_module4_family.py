from __future__ import annotations

from datetime import UTC, datetime, timedelta

from polaris.core.module4.buckets import split_count_bucket_from_slug
from polaris.core.module4.family import aggregate_market_family_rows


def test_split_count_bucket_from_slug() -> None:
    family, bucket = split_count_bucket_from_slug("elon-musk-of-tweets-february-14-february-16-0-39")
    assert family == "elon-musk-of-tweets-february-14-february-16"
    assert bucket == "0-39"

    family2, bucket2 = split_count_bucket_from_slug("elon-musk-of-tweets-february-14-february-16-240plus")
    assert family2 == "elon-musk-of-tweets-february-14-february-16"
    assert bucket2 == "240+"

    family3, bucket3 = split_count_bucket_from_slug("not-a-bucket-slug")
    assert family3 is None
    assert bucket3 is None


def test_aggregate_market_family_rows_yes_no_to_bucket_distribution() -> None:
    now = datetime.now(tz=UTC)
    rows = [
        {
            "run_tag": "r1",
            "market_id": "m0",
            "slug": "elon-musk-of-tweets-february-14-february-16-0-39",
            "prior_pmf": {"Yes": 0.30, "No": 0.70},
            "posterior_pmf": {"Yes": 0.20, "No": 0.80},
            "baseline_pmf": {"Yes": 0.25, "No": 0.75},
            "metadata": {"semantic_applied": True},
            "as_of_ts": now,
            "end_date": now + timedelta(hours=3),
            "final_count": 45,
        },
        {
            "run_tag": "r1",
            "market_id": "m1",
            "slug": "elon-musk-of-tweets-february-14-february-16-40-64",
            "prior_pmf": {"Yes": 0.70, "No": 0.30},
            "posterior_pmf": {"Yes": 0.80, "No": 0.20},
            "baseline_pmf": {"Yes": 0.75, "No": 0.25},
            "metadata": {"semantic_applied": False},
            "as_of_ts": now,
            "end_date": now + timedelta(hours=3),
            "final_count": 45,
        },
    ]
    out = aggregate_market_family_rows(rows, include_baseline=True)
    assert len(out.rows) == 1
    unit = out.rows[0]
    assert unit["is_family_aggregated"] is True
    assert unit["market_id"].startswith("family:")
    assert set(unit["posterior_pmf"].keys()) == {"0-39", "40-64"}
    assert abs(sum(unit["posterior_pmf"].values()) - 1.0) < 1e-9
    assert unit["semantic_applied"] is True
    assert out.stats["family_rows"] == 1


def test_aggregate_market_family_rows_keeps_bucket_distribution_rows() -> None:
    now = datetime.now(tz=UTC)
    rows = [
        {
            "run_tag": "r2",
            "market_id": "mx",
            "slug": "",
            "prior_pmf": {"0-39": 0.6, "40-64": 0.4},
            "posterior_pmf": {"0-39": 0.55, "40-64": 0.45},
            "metadata": {"semantic_applied": False},
            "as_of_ts": now,
            "end_date": now + timedelta(hours=2),
            "final_count": 22,
        }
    ]
    out = aggregate_market_family_rows(rows, include_baseline=False)
    assert len(out.rows) == 1
    unit = out.rows[0]
    assert unit["is_family_aggregated"] is False
    assert unit["market_id"] == "mx"
    assert out.stats["passthrough_rows"] == 1


def test_aggregate_market_family_rows_falls_back_baseline_to_prior() -> None:
    now = datetime.now(tz=UTC)
    rows = [
        {
            "run_tag": "r3",
            "market_id": "m0",
            "slug": "elon-musk-of-tweets-february-14-february-16-0-39",
            "prior_pmf": {"Yes": 0.40, "No": 0.60},
            "posterior_pmf": {"Yes": 0.45, "No": 0.55},
            "baseline_pmf": {},
            "metadata": {"semantic_applied": False},
            "as_of_ts": now,
            "end_date": now + timedelta(hours=1),
            "final_count": 20,
        },
        {
            "run_tag": "r3",
            "market_id": "m1",
            "slug": "elon-musk-of-tweets-february-14-february-16-40-64",
            "prior_pmf": {"Yes": 0.60, "No": 0.40},
            "posterior_pmf": {"Yes": 0.55, "No": 0.45},
            "baseline_pmf": {},
            "metadata": {"semantic_applied": True},
            "as_of_ts": now,
            "end_date": now + timedelta(hours=1),
            "final_count": 20,
        },
    ]
    out = aggregate_market_family_rows(rows, include_baseline=True)
    assert len(out.rows) == 1
    unit = out.rows[0]
    assert abs(sum(unit["baseline_pmf"].values()) - 1.0) < 1e-9
    assert out.stats["baseline_missing_fallback_prior"] == 2
