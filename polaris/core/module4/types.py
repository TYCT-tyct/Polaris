from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class RulePolicy:
    include_main: bool = True
    include_reply: bool = False
    include_quote: bool = True
    include_repost: bool = True
    count_deleted_if_captured: bool = True
    resolution_source: str = "xtracker"
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class RuleVersion:
    rule_version_id: str
    market_id: str
    rule_hash: str
    rule_text: str
    parsed_rule: RulePolicy


@dataclass(frozen=True)
class BucketRange:
    label: str
    lower: int | None
    upper: int | None


@dataclass(frozen=True)
class PriorBuildResult:
    market_id: str
    pmf: dict[str, float]
    prior_reliability: float
    expected_count: float
    spread_mean: float
    sample_count_10m: int
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RegimeAdjustment:
    posts_30m: int
    posts_120m: int
    progress: float
    progress_effective: float
    tail_multiplier: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class M4Evidence:
    event_tags: tuple[str, ...]
    confidence: float
    uncertainty_delta: float
    calls_used: int
    timed_out: bool
    degraded: bool
    sources: tuple[dict[str, Any], ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class M4Posterior:
    market_id: str
    window_code: str
    observed_count: int
    progress: float
    progress_effective: float
    pmf_prior: dict[str, float]
    pmf_posterior: dict[str, float]
    expected_count: float
    entropy: float
    quantiles: dict[str, float]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class M4Decision:
    action: str
    reason_codes: tuple[str, ...]
    prior_bucket: str
    target_bucket: str
    target_prob_prior: float
    target_prob_posterior: float
    edge_expected: float
    es95_loss: float
    net_edge: float
    size_usd: float
    fee_est_usd: float
    slippage_est_usd: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class M4Health:
    market_id: str
    rule_match: bool
    tracker_lag_sec: float
    data_gap_sec: float
    model_divergence: float
    trading_enabled: bool
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class M4ScoreResult:
    run_tag: str
    mode: str
    source_code: str
    window_code: str
    since_hours: int
    markets_scored: int
    log_score: float | None
    ece: float | None
    brier: float | None
    realized_net_pnl_usd: float
    max_drawdown_usd: float
    score_total: float
    score_breakdown: dict[str, float]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class M4RunContext:
    run_tag: str
    mode: str
    source_code: str
    created_at: datetime
