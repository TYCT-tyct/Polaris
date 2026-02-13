from __future__ import annotations

from polaris.arb.experiments import ExperimentScoreInput, compute_experiment_score, parse_experiment_dimensions
from polaris.cli import _resolve_matrix_cases


def test_parse_experiment_dimensions_from_source_code() -> None:
    known_profiles = {"strict_50", "balanced_50", "aggressive_50"}

    shared = parse_experiment_dimensions(
        "polymarket_balanced_50_shared_abcfg50",
        known_profiles=known_profiles,
    )
    assert shared.profile == "balanced_50"
    assert shared.scope == "shared"
    assert shared.strategy_set == "ABCFG"
    assert shared.variant == "base"

    g_low = parse_experiment_dimensions(
        "polymarket_strict_50_shared_abcf_g_low50",
        known_profiles=known_profiles,
    )
    assert g_low.profile == "strict_50"
    assert g_low.scope == "shared"
    assert g_low.strategy_set == "ABCF"
    assert g_low.variant == "g_low"

    isolated = parse_experiment_dimensions(
        "polymarket_aggressive_50_isolated_a50",
        known_profiles=known_profiles,
    )
    assert isolated.profile == "aggressive_50"
    assert isolated.scope == "isolated"
    assert isolated.strategy_set == "A"


def test_compute_experiment_score_penalizes_risk_and_errors() -> None:
    strong_score, _ = compute_experiment_score(
        ExperimentScoreInput(
            bankroll_usd=50.0,
            evaluation_net_pnl_usd=3.0,
            expected_net_pnl_usd=3.2,
            max_drawdown_usd=1.0,
            reject_rate=0.20,
            execution_rate=0.05,
            system_error_rate=0.0,
            resource_penalty=0.0,
        )
    )
    weak_score, _ = compute_experiment_score(
        ExperimentScoreInput(
            bankroll_usd=50.0,
            evaluation_net_pnl_usd=3.0,
            expected_net_pnl_usd=8.0,
            max_drawdown_usd=8.0,
            reject_rate=0.90,
            execution_rate=0.005,
            system_error_rate=0.10,
            resource_penalty=1.0,
        )
    )
    assert strong_score > weak_score


def test_resolve_matrix_cases_honors_include_c_flag() -> None:
    cases = _resolve_matrix_cases(
        strategy_sets="",
        strategies="",
        include_c=False,
    )
    names = [name for name, *_ in cases]
    assert "isolated_c" not in names
    assert any(name.startswith("shared_") for name in names)

