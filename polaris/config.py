from __future__ import annotations

from functools import lru_cache
from typing import Iterable

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
    model_config = SettingsConfigDict(env_prefix="POLARIS_", case_sensitive=False)

    database_url: str = Field(default="postgresql://postgres:postgres@localhost:55432/polaris")
    log_level: str = Field(default="INFO")
    default_handles: str = Field(default="elonmusk")

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


@lru_cache(maxsize=1)
def load_settings() -> PolarisSettings:
    return PolarisSettings()

