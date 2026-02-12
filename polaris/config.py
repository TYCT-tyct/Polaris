from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Iterable

from dotenv import dotenv_values
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RateLimitConfig(BaseModel):
    rate: float
    burst: int


class RetryConfig(BaseModel):
    min_seconds: float = 0.5
    max_seconds: float = 20.0
    attempts: int = 5


class PollIntervals(BaseModel):
    markets_discovery: int = 300
    tracking_sync: int = 300
    metric_sync: int = 120
    post_sync: int = 120
    quote_top_sync: int = 10
    quote_depth_sync: int = 30
    orderbook_l2_sync: int = 60
    mapping_sync: int = 300
    agg_1m: int = 60
    health_agg: int = 60
    retention: int = 86400


class PolarisSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="POLARIS_",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8",
    )

    database_url: str = Field(default="postgresql://postgres:postgres@localhost:55432/polaris")
    log_level: str = Field(default="INFO")
    default_handles: str = Field(default="elonmusk")
    market_discovery_scope: str = Field(default="all")
    market_discovery_state: str = Field(default="open")
    market_discovery_tweet_targets: str = Field(
        default="elon musk,andrew tate,donald j trump,donald trump"
    )
    gamma_page_size: int = Field(default=500)
    gamma_max_pages: int = Field(default=0)

    xtracker_rate: float = Field(default=1.0)
    xtracker_burst: int = Field(default=2)
    gamma_rate: float = Field(default=2.0)
    gamma_burst: int = Field(default=4)
    clob_rate: float = Field(default=5.0)
    clob_burst: int = Field(default=8)

    markets_discovery_interval: int = Field(default=300)
    tracking_sync_interval: int = Field(default=300)
    metric_sync_interval: int = Field(default=120)
    post_sync_interval: int = Field(default=120)
    quote_top_sync_interval: int = Field(default=10)
    quote_depth_sync_interval: int = Field(default=30)
    orderbook_l2_sync_interval: int = Field(default=60)
    mapping_sync_interval: int = Field(default=300)
    agg_1m_interval: int = Field(default=60)
    health_agg_interval: int = Field(default=60)
    retention_interval: int = Field(default=86400)

    post_fail_backoff_threshold: int = Field(default=8)
    post_fail_backoff_interval: int = Field(default=300)
    raw_retention_days: int = Field(default=14)
    enable_l2: bool = Field(default=True)

    # module2: global mode and cadence
    arb_default_mode: str = Field(default="paper_live")
    arb_scan_interval_sec: int = Field(default=20)
    arb_optimize_interval_sec: int = Field(default=86400)
    arb_optimize_replay_days: int = Field(default=14)
    arb_optimize_paper_hours: int = Field(default=24)
    arb_max_signals_per_cycle: int = Field(default=48)
    arb_universe_max_hours: float = Field(default=72.0)
    arb_universe_token_limit: int = Field(default=2000)
    arb_universe_refresh_sec: int = Field(default=180)
    arb_universe_min_liquidity: float = Field(default=0.0)
    arb_clob_books_batch_size: int = Field(default=500)
    arb_clob_books_max_concurrency: int = Field(default=4)
    arb_execution_concurrency: int = Field(default=3)
    arb_live_preflight_max_age_ms: int = Field(default=2000)
    arb_live_preflight_force_refresh: bool = Field(default=False)
    arb_execution_backend: str = Field(default="python")
    arb_rust_bridge_enabled: bool = Field(default=False)
    arb_rust_bridge_mode: str = Field(default="daemon")
    arb_rust_bridge_bin: str = Field(default="polaris-book-sim")
    arb_rust_bridge_timeout_sec: int = Field(default=5)
    arb_paper_realized_pnl_mode: str = Field(default="entry_only")
    arb_c_force_entry_only_in_paper: bool = Field(default=True)
    arb_run_tag: str = Field(default="auto")
    arb_signal_dedupe_ttl_sec: int = Field(default=30)
    arb_scope_block_cooldown_sec: int = Field(default=300)
    arb_safe_arbitrage_only: bool = Field(default=True)
    arb_strategy_health_gate_enabled: bool = Field(default=True)
    arb_strategy_health_window_hours: int = Field(default=24)
    arb_strategy_health_min_trades: int = Field(default=12)
    arb_strategy_health_max_loss_usd: float = Field(default=0.8)
    arb_strategy_health_min_win_rate: float = Field(default=0.35)
    arb_strategy_health_min_avg_trade_pnl_usd: float = Field(default=0.0)

    # module2: risk and sizing
    arb_min_order_notional_usd: float = Field(default=1.0)
    arb_single_risk_usd: float = Field(default=2.0)
    arb_max_exposure_usd: float = Field(default=6.0)
    arb_daily_stop_loss_usd: float = Field(default=0.5)
    arb_consecutive_fail_limit: int = Field(default=2)
    arb_partial_fill_timeout_sec: int = Field(default=30)
    arb_patch_fill_threshold: float = Field(default=0.8)
    arb_unwind_fill_threshold: float = Field(default=0.5)
    arb_slippage_bps: int = Field(default=40)
    arb_fee_bps: int = Field(default=0)
    arb_live_order_type: str = Field(default="FAK")
    arb_paper_initial_bankroll_usd: float = Field(default=10.0)
    arb_paper_split_by_strategy: bool = Field(default=False)
    arb_paper_enforce_bankroll: bool = Field(default=True)

    # module2: strategy switches
    arb_enable_strategy_a: bool = Field(default=True)
    arb_enable_strategy_b: bool = Field(default=True)
    arb_enable_strategy_c: bool = Field(default=True)
    arb_enable_strategy_f: bool = Field(default=False)
    arb_enable_strategy_g: bool = Field(default=False)
    arb_c_live_enabled: bool = Field(default=False)
    arb_strategy_priority: str = Field(default="A,B,G,F")

    # module2: strategy thresholds
    arb_a_min_edge_pct: float = Field(default=0.015)
    arb_b_min_edge_pct: float = Field(default=0.012)
    arb_c_min_edge_pct: float = Field(default=0.015)
    arb_c_max_candidates_per_event: int = Field(default=1)
    arb_f_min_prob: float = Field(default=0.94)
    arb_f_max_hours_to_resolve: float = Field(default=12.0)
    arb_f_min_annualized_return: float = Field(default=0.08)
    arb_f_max_spread: float = Field(default=0.03)
    arb_f_max_signals_per_cycle: int = Field(default=20)
    arb_g_max_hours_to_resolve: float = Field(default=4.0)
    arb_g_min_confidence: float = Field(default=0.90)
    arb_g_min_expected_edge_pct: float = Field(default=0.02)

    # module2: ai gate
    arb_ai_mode: str = Field(default="cascade_quorum")
    arb_ai_quorum: int = Field(default=2)
    arb_ai_provider_order: str = Field(default="google,anthropic,openai,minimax,zhipu")
    arb_ai_enabled: bool = Field(default=False)
    arb_ai_single_model: str = Field(default="openai")
    arb_openai_api_key: str | None = Field(default=None)
    arb_anthropic_api_key: str | None = Field(default=None)
    arb_google_api_key: str | None = Field(default=None)
    arb_minimax_api_key: str | None = Field(default=None)
    arb_zhipu_api_key: str | None = Field(default=None)
    arb_openai_model: str = Field(default="gpt-5-mini")
    arb_anthropic_model: str = Field(default="claude-3-7-sonnet-latest")
    arb_google_model: str = Field(default="gemini-2.0-flash")
    arb_minimax_model: str = Field(default="MiniMax-Text-01")
    arb_zhipu_model: str = Field(default="glm-4-flash")

    clob_http2_enabled: bool = Field(default=True)
    clob_max_connections: int = Field(default=80)
    clob_max_keepalive_connections: int = Field(default=40)
    clob_keepalive_expiry_sec: float = Field(default=30.0)
    clob_ws_enabled: bool = Field(default=False)
    clob_ws_url: str = Field(default="wss://ws-subscriptions-clob.polymarket.com/ws/market")
    clob_ws_book_max_age_sec: float = Field(default=2.5)
    clob_book_cache_max_age_sec: float = Field(default=12.0)
    clob_ws_max_subscribe_tokens: int = Field(default=3500)
    clob_ws_reconnect_min_sec: float = Field(default=0.5)
    clob_ws_reconnect_max_sec: float = Field(default=8.0)

    @property
    def handles(self) -> list[str]:
        return [part.strip() for part in self.default_handles.split(",") if part.strip()]

    @property
    def retry(self) -> RetryConfig:
        return RetryConfig()

    @property
    def intervals(self) -> PollIntervals:
        return PollIntervals(
            markets_discovery=self.markets_discovery_interval,
            tracking_sync=self.tracking_sync_interval,
            metric_sync=self.metric_sync_interval,
            post_sync=self.post_sync_interval,
            quote_top_sync=self.quote_top_sync_interval,
            quote_depth_sync=self.quote_depth_sync_interval,
            orderbook_l2_sync=self.orderbook_l2_sync_interval,
            mapping_sync=self.mapping_sync_interval,
            agg_1m=self.agg_1m_interval,
            health_agg=self.health_agg_interval,
            retention=self.retention_interval,
        )

    def rate_limit(self, source: str) -> RateLimitConfig:
        source_map = {
            "xtracker": RateLimitConfig(rate=self.xtracker_rate, burst=self.xtracker_burst),
            "gamma": RateLimitConfig(rate=self.gamma_rate, burst=self.gamma_burst),
            "clob": RateLimitConfig(rate=self.clob_rate, burst=self.clob_burst),
        }
        return source_map[source]

    def with_handles(self, handles: Iterable[str] | None = None) -> list[str]:
        if handles:
            cleaned = [h.strip().lower() for h in handles if h and h.strip()]
            if cleaned:
                return cleaned
        return self.handles

    @property
    def market_tweet_targets(self) -> list[str]:
        return [s.strip().lower() for s in self.market_discovery_tweet_targets.split(",") if s.strip()]

    @property
    def arb_priority(self) -> list[str]:
        return [s.strip().upper() for s in self.arb_strategy_priority.split(",") if s.strip()]

    @property
    def ai_provider_order(self) -> list[str]:
        return [s.strip().lower() for s in self.arb_ai_provider_order.split(",") if s.strip()]


@lru_cache(maxsize=1)
def load_settings() -> PolarisSettings:
    return PolarisSettings()


def refresh_process_env_from_file(
    path: str | Path = ".env",
    prefix: str = "POLARIS_",
    preserve_existing: bool = False,
) -> bool:
    env_path = Path(path)
    if not env_path.exists():
        return False
    changed = False
    values = dotenv_values(env_path)
    for key, value in values.items():
        if value is None or not key.startswith(prefix):
            continue
        if preserve_existing and key in os.environ:
            continue
        if os.environ.get(key) != value:
            os.environ[key] = value
            changed = True
    return changed
