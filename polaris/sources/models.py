from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class XTrackerUser(BaseModel):
    account_id: str = Field(alias="id")
    handle: str
    platform_id: str | None = Field(default=None, alias="platformId")
    name: str | None = None
    avatar_url: str | None = Field(default=None, alias="avatarUrl")
    bio: str | None = None
    verified: bool | None = None
    last_sync: datetime | None = Field(default=None, alias="lastSync")
    post_count: int | None = None

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "XTrackerUser":
        count_data = payload.get("_count", {})
        return cls.model_validate({**payload, "post_count": count_data.get("posts")})


class XTrackerTracking(BaseModel):
    tracking_id: str = Field(alias="id")
    account_id: str = Field(alias="userId")
    title: str
    start_date: datetime = Field(alias="startDate")
    end_date: datetime = Field(alias="endDate")
    is_active: bool = Field(alias="isActive")


class XTrackerMetricPoint(BaseModel):
    metric_id: str = Field(alias="id")
    account_id: str = Field(alias="userId")
    metric_date: datetime = Field(alias="date")
    metric_type: str = Field(alias="type")
    tweet_count: int
    cumulative_count: int
    tracking_id: str | None

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "XTrackerMetricPoint":
        data = payload.get("data", {})
        return cls.model_validate(
            {
                **payload,
                "tweet_count": int(data.get("count", 0)),
                "cumulative_count": int(data.get("cumulative", 0)),
                "tracking_id": data.get("trackingId"),
            }
        )


class XTrackerPost(BaseModel):
    post_id: str = Field(alias="id")
    account_id: str = Field(alias="userId")
    platform_post_id: str = Field(alias="platformId")
    content: str
    posted_at: datetime = Field(alias="createdAt")
    imported_at: datetime | None = Field(default=None, alias="importedAt")
    metrics: dict[str, Any] | None = None


class ClobLevel(BaseModel):
    price: float
    size: float


class ClobBook(BaseModel):
    market: str | None = None
    asset_id: str
    timestamp: str | None = None
    bids: list[ClobLevel]
    asks: list[ClobLevel]
    min_order_size: str | None = None
    tick_size: str | None = None
    neg_risk: bool | None = None
    last_trade_price: str | None = None


@dataclass(slots=True)
class TokenDescriptor:
    token_id: str
    market_id: str
    outcome_label: str
    outcome_side: str
    tick_size: float | None = None
    min_order_size: float | None = None
    outcome_index: int = 0
    is_other_outcome: bool = False
    is_placeholder_outcome: bool = False
