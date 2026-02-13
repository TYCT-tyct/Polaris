from __future__ import annotations

from dataclasses import dataclass
import re


@dataclass(frozen=True)
class ExperimentDimensions:
    profile: str
    strategy_set: str
    scope: str
    variant: str


@dataclass(frozen=True)
class ExperimentScoreInput:
    bankroll_usd: float
    evaluation_net_pnl_usd: float
    expected_net_pnl_usd: float
    max_drawdown_usd: float
    reject_rate: float
    execution_rate: float
    system_error_rate: float
    resource_penalty: float


def parse_experiment_dimensions(
    source_code: str,
    *,
    known_profiles: set[str] | None = None,
) -> ExperimentDimensions:
    value = (source_code or "").strip().lower()
    profiles = {p.strip().lower() for p in (known_profiles or set()) if p and p.strip()}

    profile = "unknown"
    if profiles:
        for item in sorted(profiles, key=len, reverse=True):
            if f"_{item}_" in value or value.endswith(f"_{item}"):
                profile = item
                break

    scope = "shared"
    strategy_set = "unknown"
    variant = "base"

    m_iso = re.search(r"_isolated_([abcfg])(?:\d+)?$", value)
    if m_iso:
        scope = "isolated"
        strategy_set = m_iso.group(1).upper()
        return ExperimentDimensions(profile=profile, strategy_set=strategy_set, scope=scope, variant=variant)

    m_shared = re.search(r"_shared_([a-z_]+?)(?:\d+)?$", value)
    if m_shared:
        raw = m_shared.group(1).upper()
        if raw.endswith("_G_LOW"):
            variant = "g_low"
            raw = raw[: -len("_G_LOW")]
        strategy_set = "".join(ch for ch in raw if ch in "ABCFG")
        strategy_set = strategy_set or "UNKNOWN"
        return ExperimentDimensions(profile=profile, strategy_set=strategy_set, scope=scope, variant=variant)

    compact = re.sub(r"[^A-Z]", "", value.upper())
    if compact in {"A", "B", "C", "F", "G"}:
        return ExperimentDimensions(profile=profile, strategy_set=compact, scope="isolated", variant=variant)
    if compact in {"ABCF", "ABCFG"}:
        return ExperimentDimensions(profile=profile, strategy_set=compact, scope="shared", variant=variant)

    return ExperimentDimensions(profile=profile, strategy_set=strategy_set, scope=scope, variant=variant)


def compute_experiment_score(value: ExperimentScoreInput) -> tuple[float, dict[str, float]]:
    bankroll = max(1.0, float(value.bankroll_usd))
    evaluation_net = float(value.evaluation_net_pnl_usd)
    expected_net = float(value.expected_net_pnl_usd)
    max_drawdown = max(0.0, float(value.max_drawdown_usd))
    reject_rate = min(1.0, max(0.0, float(value.reject_rate)))
    execution_rate = min(1.0, max(0.0, float(value.execution_rate)))
    system_error_rate = min(1.0, max(0.0, float(value.system_error_rate)))
    resource_penalty_input = max(0.0, float(value.resource_penalty))

    drawdown_budget = bankroll * 0.08
    drawdown_penalty = max(0.0, max_drawdown - drawdown_budget) * 1.8
    pnl_gap_penalty = abs(expected_net - evaluation_net) * 0.22
    reject_rate_penalty = max(0.0, reject_rate - 0.60) * bankroll * 0.70
    system_error_penalty = system_error_rate * bankroll * 1.20
    execution_penalty = max(0.0, 0.02 - execution_rate) * bankroll * 0.80
    resource_penalty = resource_penalty_input

    score = evaluation_net
    score -= drawdown_penalty
    score -= pnl_gap_penalty
    score -= reject_rate_penalty
    score -= system_error_penalty
    score -= execution_penalty
    score -= resource_penalty

    breakdown = {
        "evaluation_net_pnl_usd": round(evaluation_net, 6),
        "drawdown_penalty": round(drawdown_penalty, 6),
        "pnl_gap_penalty": round(pnl_gap_penalty, 6),
        "reject_rate_penalty": round(reject_rate_penalty, 6),
        "system_error_penalty": round(system_error_penalty, 6),
        "execution_penalty": round(execution_penalty, 6),
        "resource_penalty": round(resource_penalty, 6),
    }
    return round(score, 6), breakdown
