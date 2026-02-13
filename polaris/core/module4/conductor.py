from __future__ import annotations

import math
from dataclasses import dataclass

from .types import M4Decision, M4Posterior


@dataclass(frozen=True)
class ConductorConfig:
    min_reliability: float = 0.35
    min_net_edge: float = 0.01
    fee_bps: float = 0.0
    slippage_bps: float = 20.0
    latency_buffer_bps: float = 10.0
    es95_soft_limit: float = 0.08
    base_notional_usd: float = 10.0
    max_notional_usd: float = 50.0
    window_weight_short: float = 0.65
    window_weight_week: float = 0.35


class Conductor:
    def __init__(self, config: ConductorConfig | None = None) -> None:
        self._cfg = config or ConductorConfig()

    def fuse_posteriors(self, posteriors: list[M4Posterior]) -> M4Posterior:
        if not posteriors:
            raise ValueError("posteriors cannot be empty")
        if len(posteriors) == 1:
            return posteriors[0]

        windows = {p.window_code: p for p in posteriors}
        weights: dict[str, float] = {}
        weights["short"] = self._cfg.window_weight_short if "short" in windows else 0.0
        weights["week"] = self._cfg.window_weight_week if "week" in windows else 0.0
        for p in posteriors:
            if p.window_code not in ("short", "week"):
                weights[p.window_code] = 0.0

        missing_weight = 1.0 - sum(weights.values())
        if missing_weight > 0:
            extras = [p.window_code for p in posteriors if p.window_code not in ("short", "week")]
            if extras:
                extra_w = missing_weight / len(extras)
                for code in extras:
                    weights[code] = extra_w

        total = sum(weights.get(p.window_code, 0.0) for p in posteriors)
        if total <= 0:
            uniform = 1.0 / len(posteriors)
            for p in posteriors:
                weights[p.window_code] = uniform
        else:
            for p in posteriors:
                weights[p.window_code] = weights.get(p.window_code, 0.0) / total

        labels = sorted(
            {
                *posteriors[0].pmf_prior.keys(),
                *posteriors[0].pmf_posterior.keys(),
                *[label for p in posteriors for label in p.pmf_prior.keys()],
                *[label for p in posteriors for label in p.pmf_posterior.keys()],
            }
        )
        prior = _weighted_log_pool(posteriors, labels, weights, attr="pmf_prior")
        posterior = _weighted_log_pool(posteriors, labels, weights, attr="pmf_posterior")

        expected_count = sum(p.expected_count * weights.get(p.window_code, 0.0) for p in posteriors)
        entropy = _entropy(posterior)
        progress = sum(p.progress * weights.get(p.window_code, 0.0) for p in posteriors)
        progress_effective = sum(p.progress_effective * weights.get(p.window_code, 0.0) for p in posteriors)
        observed_count = max(p.observed_count for p in posteriors)
        quantiles = _weighted_quantiles(posteriors, weights)

        return M4Posterior(
            market_id=posteriors[0].market_id,
            window_code="fused",
            observed_count=observed_count,
            progress=progress,
            progress_effective=progress_effective,
            pmf_prior=prior,
            pmf_posterior=posterior,
            expected_count=expected_count,
            entropy=entropy,
            quantiles=quantiles,
            metadata={
                "component_windows": [p.window_code for p in posteriors],
                "weights": {k: round(v, 6) for k, v in weights.items()},
            },
        )

    def decide(
        self,
        *,
        posterior: M4Posterior,
        prior_reliability: float,
    ) -> M4Decision:
        target_bucket = _argmax_label(posterior.pmf_posterior)
        prior_bucket = _argmax_label(posterior.pmf_prior)
        target_prob_post = float(posterior.pmf_posterior.get(target_bucket, 0.0))
        target_prob_prior = float(posterior.pmf_prior.get(target_bucket, 0.0))
        edge_expected = target_prob_post - target_prob_prior

        # Digital payout approximation: buy target bucket token at prior probability.
        # Worst case (token resolves false) loses the token price.
        es95_loss = _digital_es95_loss(price=target_prob_prior, success_prob=target_prob_post)

        size_usd = _suggest_size_usd(
            reliability=prior_reliability,
            edge=max(0.0, edge_expected),
            base=self._cfg.base_notional_usd,
            max_size=self._cfg.max_notional_usd,
        )
        fee_est = size_usd * (self._cfg.fee_bps / 10_000.0)
        slippage_est = size_usd * ((self._cfg.slippage_bps + self._cfg.latency_buffer_bps) / 10_000.0)
        net_edge = edge_expected - ((fee_est + slippage_est) / max(1.0, size_usd))

        reason_codes: list[str] = []
        action = "NO_TRADE"
        if prior_reliability < self._cfg.min_reliability:
            reason_codes.append("prior_reliability_low")
        if net_edge < self._cfg.min_net_edge:
            reason_codes.append("net_edge_low")

        if not reason_codes:
            if es95_loss > (self._cfg.es95_soft_limit * 2.0):
                action = "NO_TRADE"
                reason_codes.append("es95_hard_block")
                size_usd = 0.0
                fee_est = 0.0
                slippage_est = 0.0
            elif es95_loss > self._cfg.es95_soft_limit:
                action = "LIMIT_MAKE"
                reason_codes.append("es95_soft_reduce")
                size_usd *= 0.6
                fee_est = size_usd * (self._cfg.fee_bps / 10_000.0)
                slippage_est = size_usd * ((self._cfg.slippage_bps + self._cfg.latency_buffer_bps) / 10_000.0)
                net_edge = edge_expected - ((fee_est + slippage_est) / max(1.0, size_usd))
            elif net_edge >= (self._cfg.min_net_edge * 2.5):
                action = "LIMIT_TAKE"
                reason_codes.append("net_edge_strong")
            else:
                action = "LIMIT_MAKE"
                reason_codes.append("net_edge_positive")

        if action == "NO_TRADE":
            size_usd = 0.0
            fee_est = 0.0
            slippage_est = 0.0

        return M4Decision(
            action=action,
            reason_codes=tuple(reason_codes),
            prior_bucket=prior_bucket,
            target_bucket=target_bucket,
            target_prob_prior=target_prob_prior,
            target_prob_posterior=target_prob_post,
            edge_expected=edge_expected,
            es95_loss=es95_loss,
            net_edge=net_edge,
            size_usd=size_usd,
            fee_est_usd=fee_est,
            slippage_est_usd=slippage_est,
            metadata={
                "prior_reliability": prior_reliability,
                "entropy": posterior.entropy,
                "window_code": posterior.window_code,
            },
        )


def _weighted_log_pool(
    posteriors: list[M4Posterior],
    labels: list[str],
    weights: dict[str, float],
    *,
    attr: str,
) -> dict[str, float]:
    eps = 1e-9
    raw: dict[str, float] = {}
    for label in labels:
        score = 0.0
        for p in posteriors:
            dist = getattr(p, attr)
            w = weights.get(p.window_code, 0.0)
            score += w * math.log(max(eps, float(dist.get(label, 0.0))))
        raw[label] = math.exp(score)
    return _normalize(raw)


def _weighted_quantiles(posteriors: list[M4Posterior], weights: dict[str, float]) -> dict[str, float]:
    out: dict[str, float] = {"p10": 0.0, "p50": 0.0, "p90": 0.0}
    for q in ("p10", "p50", "p90"):
        out[q] = sum(float(p.quantiles.get(q, 0.0)) * weights.get(p.window_code, 0.0) for p in posteriors)
    return out


def _argmax_label(pmf: dict[str, float]) -> str:
    if not pmf:
        return ""
    return max(sorted(pmf.keys()), key=lambda key: float(pmf.get(key, 0.0)))


def _digital_es95_loss(*, price: float, success_prob: float) -> float:
    p_loss = 1.0 - max(0.0, min(1.0, success_prob))
    if p_loss <= 1e-9:
        return 0.0
    price_clip = max(0.0, min(1.0, price))
    if p_loss >= 0.05:
        return price_clip
    return price_clip * (p_loss / 0.05)


def _suggest_size_usd(*, reliability: float, edge: float, base: float, max_size: float) -> float:
    rel = max(0.0, min(1.0, reliability))
    edge_scale = max(0.0, min(1.0, edge / 0.05))
    size = base * (0.4 + (1.6 * rel * edge_scale))
    return max(0.0, min(max_size, size))


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

