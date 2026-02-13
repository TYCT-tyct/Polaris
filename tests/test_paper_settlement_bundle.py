from __future__ import annotations

from polaris.arb.paper.settlement import build_settlement_bundle, is_locked_full_set_bundle


def test_settlement_bundle_b_yes_no_equal_shares() -> None:
    outcome = {"t_yes": "YES", "t_no": "NO"}
    shares = {"t_yes": 10.0, "t_no": 10.0}
    assert is_locked_full_set_bundle(strategy_code="B", outcome_sides=outcome, shares_by_token=shares) is True
    bundle = build_settlement_bundle(strategy_code="B", token_ids=["t_yes", "t_no"], shares_by_token=shares)
    assert bundle is not None
    assert bundle.gross_payout_usd == 10.0


def test_settlement_bundle_rejects_mismatched_shares() -> None:
    outcome = {"t_yes": "YES", "t_no": "NO"}
    shares = {"t_yes": 10.0, "t_no": 9.0}
    assert is_locked_full_set_bundle(strategy_code="B", outcome_sides=outcome, shares_by_token=shares) is False


def test_settlement_bundle_a_yes_only() -> None:
    outcome = {"t1": "YES", "t2": "YES", "t3": "YES"}
    shares = {"t1": 5.0, "t2": 5.0, "t3": 5.0}
    assert is_locked_full_set_bundle(strategy_code="A", outcome_sides=outcome, shares_by_token=shares) is True


def test_settlement_bundle_c_disabled() -> None:
    outcome = {"t": "NO"}
    shares = {"t": 1.0}
    assert is_locked_full_set_bundle(strategy_code="C", outcome_sides=outcome, shares_by_token=shares) is False

