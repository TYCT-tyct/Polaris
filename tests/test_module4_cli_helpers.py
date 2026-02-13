from __future__ import annotations

from datetime import UTC, datetime, timedelta

from polaris.cli import _argmax_label, _audit_m4_dataset_rows, _classify_m4_dataset_row, _parse_pmf, _pick_stage


def test_pick_stage_boundaries() -> None:
    bins = [
        ("0-30m", 0.0, 0.5),
        ("30m-1h", 0.5, 1.0),
        ("1h+", 1.0, 9999.0),
    ]
    assert _pick_stage(bins, 0.0) == "0-30m"
    assert _pick_stage(bins, 0.49) == "0-30m"
    assert _pick_stage(bins, 0.5) == "30m-1h"
    assert _pick_stage(bins, 1.0) == "1h+"
    assert _pick_stage(bins, -0.1) is None


def test_parse_pmf_accepts_dict_and_json() -> None:
    direct = _parse_pmf({"a": 0.7, "b": "0.3"})
    assert direct == {"a": 0.7, "b": 0.3}

    encoded = _parse_pmf('{"x": 0.2, "y": "0.8"}')
    assert encoded == {"x": 0.2, "y": 0.8}

    assert _parse_pmf('{"x": "bad"}') == {}
    assert _parse_pmf("not-json") == {}


def test_argmax_label_is_stable() -> None:
    pmf = {"b": 0.51, "a": 0.51, "c": 0.1}
    # Same probability uses sorted label order for deterministic output.
    assert _argmax_label(pmf) == "a"


def test_classify_m4_dataset_row_reasons() -> None:
    now = datetime.now(tz=UTC)
    good = {
        "as_of_ts": now,
        "end_date": now + timedelta(hours=3),
        "prior_pmf": {"0-5": 0.7, "6-10": 0.3},
        "posterior_pmf": {"0-5": 0.4, "6-10": 0.6},
        "final_count": 7,
    }
    assert _classify_m4_dataset_row(good) is None

    bad_time = dict(good, as_of_ts="bad")
    assert _classify_m4_dataset_row(bad_time) == "invalid_time"

    closed = dict(good, as_of_ts=now + timedelta(hours=4))
    assert _classify_m4_dataset_row(closed) == "non_positive_hours_to_close"

    missing_pmf = dict(good, posterior_pmf={})
    assert _classify_m4_dataset_row(missing_pmf) == "pmf_missing"

    non_bucket = dict(good, posterior_pmf={"YES": 0.8, "NO": 0.2})
    assert _classify_m4_dataset_row(non_bucket) == "true_label_not_in_bucket_space"


def test_audit_m4_dataset_rows_outputs_coverage() -> None:
    now = datetime.now(tz=UTC)
    rows = [
        {
            "run_tag": "r1",
            "market_id": "m1",
            "as_of_ts": now,
            "end_date": now + timedelta(hours=2),
            "prior_pmf": {"0-5": 0.5, "6-10": 0.5},
            "posterior_pmf": {"0-5": 0.6, "6-10": 0.4},
            "final_count": 4,
        },
        {
            "run_tag": "r1",
            "market_id": "m2",
            "as_of_ts": now,
            "end_date": now + timedelta(hours=1),
            "prior_pmf": {"YES": 1.0},
            "posterior_pmf": {"YES": 1.0},
            "final_count": 1,
        },
        {
            "run_tag": "r2",
            "market_id": "m3",
            "as_of_ts": now,
            "end_date": now + timedelta(hours=1),
            "prior_pmf": {"0-5": 1.0},
            "posterior_pmf": {},
            "final_count": 1,
        },
    ]
    out = _audit_m4_dataset_rows(rows)
    assert out["rows_total"] == 3
    assert out["rows_scorable"] == 1
    assert out["scorable_market_count"] == 1
    assert out["unscorable_reason_distribution"] == {
        "true_label_not_in_bucket_space": 1,
        "pmf_missing": 1,
    }
    run_rows = out["run_tag_coverage"]
    assert isinstance(run_rows, list)
    assert len(run_rows) == 2
    by_tag = {str(row["run_tag"]): row for row in run_rows}
    assert by_tag["r1"]["rows_total"] == 2
    assert by_tag["r1"]["rows_scorable"] == 1
    assert by_tag["r2"]["rows_total"] == 1
    assert by_tag["r2"]["rows_scorable"] == 0
