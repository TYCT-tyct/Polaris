from __future__ import annotations

from polaris.core.module4.collapse_filter import CollapseFilter


def test_collapse_filter_normalizes_and_updates_distribution() -> None:
    engine = CollapseFilter()
    prior = {"0-49": 0.52, "50+": 0.48}
    posterior = engine.update(
        market_id="mkt-1",
        window_code="short",
        prior_pmf=prior,
        observed_count=68,
        progress=0.70,
        progress_effective=0.72,
        uncertainty_boost=0.03,
    )
    assert posterior.market_id == "mkt-1"
    assert posterior.window_code == "short"
    assert abs(sum(posterior.pmf_posterior.values()) - 1.0) < 1e-8
    assert posterior.expected_count > 0
    assert posterior.quantiles["p90"] >= posterior.quantiles["p50"] >= posterior.quantiles["p10"]
    # Given observed_count already above 50, high bucket should dominate.
    assert posterior.pmf_posterior["50+"] > posterior.pmf_posterior["0-49"]

