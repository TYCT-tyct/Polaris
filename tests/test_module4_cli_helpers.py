from __future__ import annotations

from polaris.cli import _argmax_label, _parse_pmf, _pick_stage


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
