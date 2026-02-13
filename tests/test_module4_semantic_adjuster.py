from __future__ import annotations

import math

from polaris.core.module4.semantic_adjuster import SemanticAdjuster
from polaris.core.module4.types import M4Evidence, M4Posterior


def _posterior() -> M4Posterior:
    return M4Posterior(
        market_id="m1",
        window_code="short",
        observed_count=5,
        progress=0.4,
        progress_effective=0.45,
        pmf_prior={"0-5": 0.7, "6-10": 0.3},
        pmf_posterior={"0-5": 0.8, "6-10": 0.2},
        expected_count=4.0,
        entropy=0.5,
        quantiles={"p10": 1.0, "p50": 4.0, "p90": 8.0},
        metadata={},
    )


def _evidence(confidence: float, mean_shift_hint: float, uncertainty_delta: float) -> M4Evidence:
    return M4Evidence(
        event_tags=("llm",),
        confidence=confidence,
        delta_progress=0.0,
        uncertainty_delta=uncertainty_delta,
        mean_shift_hint=mean_shift_hint,
        calls_used=1,
        timed_out=False,
        degraded=False,
        parse_ok=True,
        llm_model="gpt-5-mini",
        prompt_version="m4-semantic-v1",
        error_code=None,
        latency_ms=12.0,
        sources=(),
        metadata={},
    )


def test_semantic_adjuster_no_evidence_returns_baseline() -> None:
    adjuster = SemanticAdjuster()
    baseline = _posterior()
    out, diag = adjuster.apply(baseline, None)
    assert out == baseline
    assert diag.applied is False
    assert diag.metadata["reason"] == "no_evidence"


def test_semantic_adjuster_low_confidence_skips_adjustment() -> None:
    adjuster = SemanticAdjuster()
    baseline = _posterior()
    evidence = _evidence(confidence=0.2, mean_shift_hint=1.0, uncertainty_delta=0.2)
    out, diag = adjuster.apply(baseline, evidence)
    assert out == baseline
    assert diag.applied is False
    assert diag.metadata["reason"] == "confidence_below_floor"


def test_semantic_adjuster_applies_shift_and_uncertainty() -> None:
    adjuster = SemanticAdjuster()
    baseline = _posterior()
    evidence = _evidence(confidence=0.9, mean_shift_hint=1.0, uncertainty_delta=0.15)
    out, diag = adjuster.apply(baseline, evidence)

    assert diag.applied is True
    assert diag.mean_shift_applied > 0.0
    assert math.isclose(sum(out.pmf_posterior.values()), 1.0, abs_tol=1e-9)
    assert out.pmf_posterior["6-10"] > baseline.pmf_posterior["6-10"]
    assert out.expected_count > 0.0
    assert out.expected_count != baseline.expected_count
