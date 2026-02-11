from __future__ import annotations

from dataclasses import dataclass

from polaris.config import PolarisSettings


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
    clob_books_batch_size: int
    clob_books_max_concurrency: int
    execution_concurrency: int
    live_preflight_max_age_ms: int
    live_preflight_force_refresh: bool

    min_order_notional_usd: float
    single_risk_usd: float
    max_exposure_usd: float
    daily_stop_loss_usd: float
    consecutive_fail_limit: int
    partial_fill_timeout_sec: int
    patch_fill_threshold: float
    unwind_fill_threshold: float
    slippage_bps: int
    live_order_type: str

    enable_strategy_a: bool
    enable_strategy_b: bool
    enable_strategy_c: bool
    enable_strategy_f: bool
    enable_strategy_g: bool
    c_live_enabled: bool
    strategy_priority: tuple[str, ...]

    a_min_edge_pct: float
    b_min_edge_pct: float
    c_min_edge_pct: float
    c_max_candidates_per_event: int
    f_min_prob: float
    f_max_hours_to_resolve: float
    f_min_annualized_return: float
    g_max_hours_to_resolve: float
    g_min_confidence: float
    g_min_expected_edge_pct: float

    ai_enabled: bool
    ai_mode: str
    ai_quorum: int
    ai_provider_order: tuple[str, ...]
    ai_single_model: str



def arb_config_from_settings(settings: PolarisSettings) -> ArbConfig:
    return ArbConfig(
        default_mode=settings.arb_default_mode,
        scan_interval_sec=settings.arb_scan_interval_sec,
        optimize_interval_sec=settings.arb_optimize_interval_sec,
        optimize_replay_days=settings.arb_optimize_replay_days,
        optimize_paper_hours=settings.arb_optimize_paper_hours,
        max_signals_per_cycle=settings.arb_max_signals_per_cycle,
        universe_max_hours=settings.arb_universe_max_hours,
        universe_token_limit=settings.arb_universe_token_limit,
        clob_books_batch_size=settings.arb_clob_books_batch_size,
        clob_books_max_concurrency=settings.arb_clob_books_max_concurrency,
        execution_concurrency=settings.arb_execution_concurrency,
        live_preflight_max_age_ms=settings.arb_live_preflight_max_age_ms,
        live_preflight_force_refresh=settings.arb_live_preflight_force_refresh,
        min_order_notional_usd=settings.arb_min_order_notional_usd,
        single_risk_usd=settings.arb_single_risk_usd,
        max_exposure_usd=settings.arb_max_exposure_usd,
        daily_stop_loss_usd=settings.arb_daily_stop_loss_usd,
        consecutive_fail_limit=settings.arb_consecutive_fail_limit,
        partial_fill_timeout_sec=settings.arb_partial_fill_timeout_sec,
        patch_fill_threshold=settings.arb_patch_fill_threshold,
        unwind_fill_threshold=settings.arb_unwind_fill_threshold,
        slippage_bps=settings.arb_slippage_bps,
        live_order_type=settings.arb_live_order_type.strip().upper(),
        enable_strategy_a=settings.arb_enable_strategy_a,
        enable_strategy_b=settings.arb_enable_strategy_b,
        enable_strategy_c=settings.arb_enable_strategy_c,
        enable_strategy_f=settings.arb_enable_strategy_f,
        enable_strategy_g=settings.arb_enable_strategy_g,
        c_live_enabled=settings.arb_c_live_enabled,
        strategy_priority=tuple(settings.arb_priority),
        a_min_edge_pct=settings.arb_a_min_edge_pct,
        b_min_edge_pct=settings.arb_b_min_edge_pct,
        c_min_edge_pct=settings.arb_c_min_edge_pct,
        c_max_candidates_per_event=settings.arb_c_max_candidates_per_event,
        f_min_prob=settings.arb_f_min_prob,
        f_max_hours_to_resolve=settings.arb_f_max_hours_to_resolve,
        f_min_annualized_return=settings.arb_f_min_annualized_return,
        g_max_hours_to_resolve=settings.arb_g_max_hours_to_resolve,
        g_min_confidence=settings.arb_g_min_confidence,
        g_min_expected_edge_pct=settings.arb_g_min_expected_edge_pct,
        ai_enabled=settings.arb_ai_enabled,
        ai_mode=settings.arb_ai_mode,
        ai_quorum=settings.arb_ai_quorum,
        ai_provider_order=tuple(settings.ai_provider_order),
        ai_single_model=settings.arb_ai_single_model,
    )
