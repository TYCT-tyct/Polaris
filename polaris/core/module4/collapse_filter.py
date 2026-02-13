from __future__ import annotations

import math
from dataclasses import dataclass

from .buckets import infer_true_bucket_label, parse_bucket_label, representative_count
from .types import BucketRange, M4Posterior


@dataclass(frozen=True)
class CollapseFilterConfig:
    dispersion: float = 25.0
    tail_default_short: int = 8
    tail_default_week: int = 20
    max_tail_extension: int = 80


class CollapseFilter:
    def __init__(self, config: CollapseFilterConfig | None = None) -> None:
        self._cfg = config or CollapseFilterConfig()

    def update(
        self,
        *,
        market_id: str,
        window_code: str,
        prior_pmf: dict[str, float],
        observed_count: int,
        progress: float,
        progress_effective: float,
        uncertainty_boost: float = 0.0,
    ) -> M4Posterior:
        prior = _normalize(prior_pmf)
        progress_eff = max(0.001, min(0.999, progress_effective))
        concentration = max(2.0, self._cfg.dispersion) * (1.0 + max(0.0, uncertainty_boost))
        alpha = max(1e-6, progress_eff * concentration)
        beta = max(1e-6, (1.0 - progress_eff) * concentration)

        tail_default = self._cfg.tail_default_short if window_code == "short" else self._cfg.tail_default_week
        likelihoods: dict[str, float] = {}
        for label, p in prior.items():
            if p <= 0:
                continue
            bucket = parse_bucket_label(label)
            candidates = _bucket_candidates(
                bucket,
                observed_count=observed_count,
                tail_default=tail_default,
                max_tail_extension=self._cfg.max_tail_extension,
            )
            if not candidates:
                likelihoods[label] = 1e-18
                continue
            vals = [beta_binomial_pmf(observed_count, n, alpha, beta) for n in candidates if n >= observed_count]
            likelihoods[label] = max(1e-18, (sum(vals) / len(vals)) if vals else 1e-18)

        posterior_unnorm = {label: prior.get(label, 0.0) * likelihoods.get(label, 1e-18) for label in prior}
        posterior = _normalize(posterior_unnorm)
        expected_count = 0.0
        for label, p in posterior.items():
            expected_count += p * representative_count(parse_bucket_label(label), tail_default=tail_default)

        quantiles = _compute_quantiles(posterior, tail_default=tail_default)
        entropy = _entropy(posterior)
        return M4Posterior(
            market_id=market_id,
            window_code=window_code,
            observed_count=int(max(0, observed_count)),
            progress=float(max(0.0, min(1.0, progress))),
            progress_effective=progress_eff,
            pmf_prior=prior,
            pmf_posterior=posterior,
            expected_count=float(expected_count),
            entropy=entropy,
            quantiles=quantiles,
            metadata={
                "alpha": alpha,
                "beta": beta,
                "uncertainty_boost": uncertainty_boost,
            },
        )

    @staticmethod
    def true_bucket_prob(posterior: dict[str, float], final_count: int) -> float | None:
        label = infer_true_bucket_label(final_count, posterior.keys())
        if label is None:
            return None
        return float(posterior.get(label, 0.0))


def beta_binomial_pmf(k: int, n: int, alpha: float, beta: float) -> float:
    if k < 0 or n < 0 or k > n:
        return 0.0
    log_p = (
        _log_comb(n, k)
        + _log_beta(k + alpha, n - k + beta)
        - _log_beta(alpha, beta)
    )
    return float(math.exp(log_p))


def _bucket_candidates(
    bucket: BucketRange,
    *,
    observed_count: int,
    tail_default: int,
    max_tail_extension: int,
) -> list[int]:
    low = bucket.lower
    high = bucket.upper
    if low is None and high is None:
        return [max(observed_count, 0)]
    if low is None:
        low = 0
    if high is None:
        high = low + max_tail_extension
    low = max(low, observed_count)
    if high < low:
        high = low
    if high - low <= 24:
        return list(range(low, high + 1))
    # Sample sparse grid for very wide bins to keep runtime bounded.
    step = max(1, (high - low) // 24)
    values = list(range(low, high + 1, step))
    if values[-1] != high:
        values.append(high)
    return values


def _compute_quantiles(pmf: dict[str, float], *, tail_default: int) -> dict[str, float]:
    points = []
    for label, p in pmf.items():
        points.append((representative_count(parse_bucket_label(label), tail_default=tail_default), p))
    points.sort(key=lambda x: x[0])
    return {
        "p10": _quantile(points, 0.10),
        "p50": _quantile(points, 0.50),
        "p90": _quantile(points, 0.90),
    }


def _quantile(points: list[tuple[float, float]], q: float) -> float:
    if not points:
        return 0.0
    acc = 0.0
    target = max(0.0, min(1.0, q))
    for value, prob in points:
        acc += prob
        if acc >= target:
            return float(value)
    return float(points[-1][0])


def _normalize(values: dict[str, float]) -> dict[str, float]:
    total = sum(max(0.0, x) for x in values.values())
    if total <= 0:
        n = max(1, len(values))
        p = 1.0 / n
        return {k: p for k in values}
    return {k: max(0.0, v) / total for k, v in values.items()}


def _entropy(pmf: dict[str, float]) -> float:
    out = 0.0
    for p in pmf.values():
        if p > 0:
            out -= p * math.log(p)
    return out


def _log_comb(n: int, k: int) -> float:
    return math.lgamma(n + 1) - math.lgamma(k + 1) - math.lgamma(n - k + 1)


def _log_beta(a: float, b: float) -> float:
    return math.lgamma(a) + math.lgamma(b) - math.lgamma(a + b)
