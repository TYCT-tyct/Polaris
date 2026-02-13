from __future__ import annotations

import hashlib
import json
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

from polaris.config import PolarisSettings

_RUN_TAG_ALLOWED = re.compile(r"[^a-z0-9_.-]+")
_ALLOWED_BACKENDS = {"python", "rust_daemon", "rust_subprocess", "rust_pyo3"}


@dataclass(frozen=True)
class ArbConfig:
    default_mode: str
    scan_interval_sec: int
    optimize_interval_sec: int
    optimize_replay_days: int
    optimize_paper_hours: int
    max_signals_per_cycle: int
    universe_max_hours: float
    universe_token_limit: int
    universe_refresh_sec: int
    universe_min_liquidity: float
    clob_books_batch_size: int
    clob_books_max_concurrency: int
    execution_concurrency: int
    live_preflight_max_age_ms: int
    live_preflight_force_refresh: bool
    execution_backend: str
    rust_bridge_enabled: bool
    rust_bridge_mode: str
    rust_bridge_bin: str
    rust_bridge_timeout_sec: int
    paper_realized_pnl_mode: str
    c_force_entry_only_in_paper: bool
    run_tag: str
    signal_dedupe_ttl_sec: int
    scope_block_cooldown_sec: int
    safe_arbitrage_only: bool
    strategy_health_gate_enabled: bool
    strategy_health_window_hours: int
    strategy_health_min_trades: int
    strategy_health_max_loss_usd: float
    strategy_health_min_win_rate: float
    strategy_health_min_avg_trade_pnl_usd: float

    min_order_notional_usd: float
    single_risk_usd: float
    max_exposure_usd: float
    daily_stop_loss_usd: float
    consecutive_fail_limit: int
    partial_fill_timeout_sec: int
    patch_fill_threshold: float
    unwind_fill_threshold: float
    slippage_bps: int
    fee_bps: int
    live_order_type: str
    paper_initial_bankroll_usd: float
    paper_split_by_strategy: bool
    paper_enforce_bankroll: bool
    paper_portfolio_snapshot_interval_sec: int

    enable_strategy_a: bool
    enable_strategy_b: bool
    enable_strategy_c: bool
    enable_strategy_f: bool
    enable_strategy_g: bool
    c_live_enabled: bool
    strategy_priority: tuple[str, ...]

    a_min_edge_pct: float
    a_min_total_cost_per_share: float
    b_min_edge_pct: float
    c_min_edge_pct: float
    c_max_candidates_per_event: int
    f_min_prob: float
    f_max_hours_to_resolve: float
    f_min_annualized_return: float
    f_max_spread: float
    f_max_signals_per_cycle: int
    g_max_hours_to_resolve: float
    g_min_confidence: float
    g_min_expected_edge_pct: float

    ai_enabled: bool
    ai_mode: str
    ai_quorum: int
    ai_provider_order: tuple[str, ...]
    ai_single_model: str



def arb_config_from_settings(settings: PolarisSettings) -> ArbConfig:
    execution_backend = _resolve_execution_backend(settings)
    rust_enabled, rust_mode = _resolve_rust_bridge(execution_backend, settings)
    return ArbConfig(
        default_mode=settings.arb_default_mode,
        scan_interval_sec=settings.arb_scan_interval_sec,
        optimize_interval_sec=settings.arb_optimize_interval_sec,
        optimize_replay_days=settings.arb_optimize_replay_days,
        optimize_paper_hours=settings.arb_optimize_paper_hours,
        max_signals_per_cycle=settings.arb_max_signals_per_cycle,
        universe_max_hours=settings.arb_universe_max_hours,
        universe_token_limit=settings.arb_universe_token_limit,
        universe_refresh_sec=settings.arb_universe_refresh_sec,
        universe_min_liquidity=settings.arb_universe_min_liquidity,
        clob_books_batch_size=settings.arb_clob_books_batch_size,
        clob_books_max_concurrency=settings.arb_clob_books_max_concurrency,
        execution_concurrency=settings.arb_execution_concurrency,
        live_preflight_max_age_ms=settings.arb_live_preflight_max_age_ms,
        live_preflight_force_refresh=settings.arb_live_preflight_force_refresh,
        execution_backend=execution_backend,
        rust_bridge_enabled=rust_enabled,
        rust_bridge_mode=rust_mode,
        rust_bridge_bin=settings.arb_rust_bridge_bin,
        rust_bridge_timeout_sec=settings.arb_rust_bridge_timeout_sec,
        paper_realized_pnl_mode=(settings.arb_paper_realized_pnl_mode or "entry_only").strip().lower(),
        c_force_entry_only_in_paper=settings.arb_c_force_entry_only_in_paper,
        run_tag=resolve_run_tag(settings),
        signal_dedupe_ttl_sec=settings.arb_signal_dedupe_ttl_sec,
        scope_block_cooldown_sec=settings.arb_scope_block_cooldown_sec,
        safe_arbitrage_only=settings.arb_safe_arbitrage_only,
        strategy_health_gate_enabled=settings.arb_strategy_health_gate_enabled,
        strategy_health_window_hours=settings.arb_strategy_health_window_hours,
        strategy_health_min_trades=settings.arb_strategy_health_min_trades,
        strategy_health_max_loss_usd=settings.arb_strategy_health_max_loss_usd,
        strategy_health_min_win_rate=settings.arb_strategy_health_min_win_rate,
        strategy_health_min_avg_trade_pnl_usd=settings.arb_strategy_health_min_avg_trade_pnl_usd,
        min_order_notional_usd=settings.arb_min_order_notional_usd,
        single_risk_usd=settings.arb_single_risk_usd,
        max_exposure_usd=settings.arb_max_exposure_usd,
        daily_stop_loss_usd=settings.arb_daily_stop_loss_usd,
        consecutive_fail_limit=settings.arb_consecutive_fail_limit,
        partial_fill_timeout_sec=settings.arb_partial_fill_timeout_sec,
        patch_fill_threshold=settings.arb_patch_fill_threshold,
        unwind_fill_threshold=settings.arb_unwind_fill_threshold,
        slippage_bps=settings.arb_slippage_bps,
        fee_bps=settings.arb_fee_bps,
        live_order_type=settings.arb_live_order_type.strip().upper(),
        paper_initial_bankroll_usd=settings.arb_paper_initial_bankroll_usd,
        paper_split_by_strategy=settings.arb_paper_split_by_strategy,
        paper_enforce_bankroll=settings.arb_paper_enforce_bankroll,
        paper_portfolio_snapshot_interval_sec=settings.arb_paper_portfolio_snapshot_interval_sec,
        enable_strategy_a=settings.arb_enable_strategy_a,
        enable_strategy_b=settings.arb_enable_strategy_b,
        enable_strategy_c=settings.arb_enable_strategy_c,
        enable_strategy_f=settings.arb_enable_strategy_f,
        enable_strategy_g=settings.arb_enable_strategy_g,
        c_live_enabled=settings.arb_c_live_enabled,
        strategy_priority=tuple(settings.arb_priority),
        a_min_edge_pct=settings.arb_a_min_edge_pct,
        a_min_total_cost_per_share=settings.arb_a_min_total_cost_per_share,
        b_min_edge_pct=settings.arb_b_min_edge_pct,
        c_min_edge_pct=settings.arb_c_min_edge_pct,
        c_max_candidates_per_event=settings.arb_c_max_candidates_per_event,
        f_min_prob=settings.arb_f_min_prob,
        f_max_hours_to_resolve=settings.arb_f_max_hours_to_resolve,
        f_min_annualized_return=settings.arb_f_min_annualized_return,
        f_max_spread=settings.arb_f_max_spread,
        f_max_signals_per_cycle=settings.arb_f_max_signals_per_cycle,
        g_max_hours_to_resolve=settings.arb_g_max_hours_to_resolve,
        g_min_confidence=settings.arb_g_min_confidence,
        g_min_expected_edge_pct=settings.arb_g_min_expected_edge_pct,
        ai_enabled=settings.arb_ai_enabled,
        ai_mode=settings.arb_ai_mode,
        ai_quorum=settings.arb_ai_quorum,
        ai_provider_order=tuple(settings.ai_provider_order),
        ai_single_model=settings.arb_ai_single_model,
    )


def resolve_run_tag(settings: PolarisSettings) -> str:
    raw = (settings.arb_run_tag or "").strip()
    if raw and raw.lower() != "auto":
        return _sanitize_run_tag(raw)

    code_rev = _current_code_revision()
    signature = _arb_config_signature(settings)
    return _sanitize_run_tag(f"auto-{code_rev}-{signature}")


def _resolve_execution_backend(settings: PolarisSettings) -> str:
    raw = (settings.arb_execution_backend or "").strip().lower()
    aliases = {
        "python": "python",
        "rust-daemon": "rust_daemon",
        "rust_daemon": "rust_daemon",
        "daemon": "rust_daemon",
        "rust-subprocess": "rust_subprocess",
        "rust_subprocess": "rust_subprocess",
        "subprocess": "rust_subprocess",
        "rust-pyo3": "rust_pyo3",
        "rust_pyo3": "rust_pyo3",
        "pyo3": "rust_pyo3",
    }
    if raw in aliases:
        return aliases[raw]
    if raw in _ALLOWED_BACKENDS:
        return raw

    if settings.arb_rust_bridge_enabled:
        mode = (settings.arb_rust_bridge_mode or "").strip().lower()
        if mode == "subprocess":
            return "rust_subprocess"
        if mode == "pyo3":
            return "rust_pyo3"
        return "rust_daemon"
    return "python"


def _resolve_rust_bridge(execution_backend: str, settings: PolarisSettings) -> tuple[bool, str]:
    if execution_backend == "python":
        return False, "daemon"
    if execution_backend == "rust_subprocess":
        return True, "subprocess"
    if execution_backend == "rust_pyo3":
        return True, "pyo3"
    if execution_backend == "rust_daemon":
        return True, "daemon"
    mode = (settings.arb_rust_bridge_mode or "daemon").strip().lower()
    if mode not in {"daemon", "subprocess", "pyo3"}:
        mode = "daemon"
    return bool(settings.arb_rust_bridge_enabled), mode


def _arb_config_signature(settings: PolarisSettings) -> str:
    payload: dict[str, object] = {}
    for key, value in settings.model_dump().items():
        if not key.startswith("arb_"):
            continue
        if "api_key" in key:
            continue
        payload[key] = value
    canonical = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(canonical.encode("utf-8")).hexdigest()[:10]


def _current_code_revision() -> str:
    root = Path(__file__).resolve().parents[2]
    try:
        output = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=root,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=2,
        )
        value = output.strip().lower()
        if value:
            return value
    except Exception:
        pass
    return "unknown"


def _sanitize_run_tag(value: str) -> str:
    normalized = value.strip().lower()
    normalized = _RUN_TAG_ALLOWED.sub("-", normalized)
    normalized = normalized.strip("-")
    if not normalized:
        return "default"
    return normalized[:64]
