from __future__ import annotations

import math
from dataclasses import dataclass

from .buckets import parse_bucket_label, representative_count
from .semantic_types import M4SemanticDiagnostics
from .types import M4Evidence, M4Posterior


@dataclass(frozen=True)
class SemanticAdjusterConfig:
    confidence_floor: float = 0.55
    mean_shift_cap: float = 0.08
    uncertainty_cap: float = 0.20


class SemanticAdjuster:
    def __init__(self, config: SemanticAdjusterConfig | None = None) -> None:
        self._cfg = config or SemanticAdjusterConfig()

    def apply(self, posterior: M4Posterior, evidence: M4Evidence | None) -> tuple[M4Posterior, M4SemanticDiagnostics]:
        if evidence is None:
            return posterior, M4SemanticDiagnostics(
                applied=False,
                confidence_used=0.0,
                mean_shift_applied=0.0,
                uncertainty_applied=0.0,
                metadata={"reason": "no_evidence"},
            )
        if evidence.confidence < self._cfg.confidence_floor:
            return posterior, M4SemanticDiagnostics(
                applied=False,
                confidence_used=evidence.confidence,
                mean_shift_applied=0.0,
                uncertainty_applied=0.0,
                metadata={"reason": "confidence_below_floor"},
            )

        pmf = dict(posterior.pmf_posterior)
        shifted, mean_shift_applied = _apply_mean_shift(
            pmf,
            hint=evidence.mean_shift_hint,
            cap=self._cfg.mean_shift_cap,
            tail_default=8 if posterior.window_code == "short" else 20,
        )
        uncertainty = max(0.0, min(self._cfg.uncertainty_cap, evidence.uncertainty_delta))
        mixed = _mix_uniform(shifted, uncertainty)
        entropy = _entropy(mixed)
        expected_count = _expected_count(mixed, tail_default=8 if posterior.window_code == "short" else 20)
        q = _quantiles(mixed, tail_default=8 if posterior.window_code == "short" else 20)

        adjusted = M4Posterior(
            market_id=posterior.market_id,
            window_code=posterior.window_code,
            observed_count=posterior.observed_count,
            progress=posterior.progress,
            progress_effective=posterior.progress_effective,
            pmf_prior=posterior.pmf_prior,
            pmf_posterior=mixed,
            expected_count=expected_count,
            entropy=entropy,
            quantiles=q,
            metadata={**posterior.metadata, "semantic_applied": True},
        )
        return adjusted, M4SemanticDiagnostics(
            applied=True,
            confidence_used=evidence.confidence,
            mean_shift_applied=mean_shift_applied,
            uncertainty_applied=uncertainty,
            metadata={"reason": "applied"},
        )


def _apply_mean_shift(
    pmf: dict[str, float],
    *,
    hint: float,
    cap: float,
    tail_default: int,
) -> tuple[dict[str, float], float]:
    if not pmf:
        return pmf, 0.0
    hint_clip = max(-1.0, min(1.0, float(hint)))
    if abs(hint_clip) < 1e-9:
        return dict(pmf), 0.0
    reps = {label: representative_count(parse_bucket_label(label), tail_default=tail_default) for label in pmf}
    rep_min = min(reps.values())
    rep_max = max(reps.values())
    denom = max(1e-6, rep_max - rep_min)
    scale = max(0.0, min(1.0, abs(hint_clip))) * max(0.0, min(0.5, cap))
    out: dict[str, float] = {}
    for label, prob in pmf.items():
        pos = (reps[label] - rep_min) / denom
        tilt = pos if hint_clip > 0 else (1.0 - pos)
        out[label] = max(0.0, prob * (1.0 + (scale * tilt)))
    return _normalize(out), hint_clip * scale


def _mix_uniform(pmf: dict[str, float], epsilon: float) -> dict[str, float]:
    if not pmf:
        return {}
    eps = max(0.0, min(0.5, epsilon))
    if eps <= 0:
        return dict(pmf)
    n = len(pmf)
    uni = 1.0 / max(1, n)
    mixed = {k: ((1.0 - eps) * float(v)) + (eps * uni) for k, v in pmf.items()}
    return _normalize(mixed)


def _expected_count(pmf: dict[str, float], *, tail_default: int) -> float:
    total = 0.0
    for label, prob in pmf.items():
        total += prob * representative_count(parse_bucket_label(label), tail_default=tail_default)
    return total


def _quantiles(pmf: dict[str, float], *, tail_default: int) -> dict[str, float]:
    points = []
    for label, prob in pmf.items():
        points.append((representative_count(parse_bucket_label(label), tail_default=tail_default), prob))
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

