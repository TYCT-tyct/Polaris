from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4


class RunMode(StrEnum):
    SHADOW = "shadow"
    PAPER_LIVE = "paper_live"
    PAPER_REPLAY = "paper_replay"
    LIVE = "live"


class StrategyCode(StrEnum):
    A = "A"
    B = "B"
    C = "C"
    F = "F"
    G = "G"


class SignalStatus(StrEnum):
    NEW = "new"
    REJECTED = "rejected"
    EXECUTED = "executed"
    EXPIRED = "expired"


class RiskLevel(StrEnum):
    INFO = "info"
    WARN = "warn"
    HARD_STOP = "hard_stop"


@dataclass(slots=True, frozen=True)
class PriceLevel:
    price: float
    size: float


@dataclass(slots=True, frozen=True)
class TokenSnapshot:
    token_id: str
    market_id: str
    event_id: str | None
    market_question: str
    market_end: datetime | None
    outcome_label: str
    outcome_side: str
    outcome_index: int
    min_order_size: float | None
    tick_size: float | None
    best_bid: float | None
    best_ask: float | None
    condition_id: str | None = None
    is_neg_risk: bool = True
    is_other_outcome: bool = False
    is_placeholder_outcome: bool = False
    bids: tuple[PriceLevel, ...] = ()
    asks: tuple[PriceLevel, ...] = ()
    captured_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))


@dataclass(slots=True)
class ArbSignal:
    strategy_code: StrategyCode
    mode: RunMode
    source_code: str
    event_id: str | None
    market_ids: list[str]
    token_ids: list[str]
    edge_pct: float
    expected_pnl_usd: float
    ttl_ms: int
    features: dict[str, Any]
    decision_note: str
    signal_id: UUID = field(default_factory=uuid4)
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
    status: SignalStatus = SignalStatus.NEW

    def is_expired(self, now: datetime | None = None) -> bool:
        now_ts = now or datetime.now(tz=UTC)
        return now_ts > self.created_at + timedelta(milliseconds=self.ttl_ms)


@dataclass(slots=True, frozen=True)
class OrderIntent:
    intent_id: UUID
    signal_id: UUID
    mode: RunMode
    strategy_code: StrategyCode
    source_code: str
    order_index: int
    market_id: str
    token_id: str
    side: str
    order_type: str
    limit_price: float | None
    shares: float | None
    notional_usd: float | None
    payload: dict[str, Any]


@dataclass(slots=True, frozen=True)
class FillEvent:
    token_id: str
    market_id: str
    side: str
    fill_price: float
    fill_size: float
    fill_notional_usd: float
    fee_usd: float


@dataclass(slots=True)
class ExecutionPlan:
    signal: ArbSignal
    intents: list[OrderIntent]


@dataclass(slots=True, frozen=True)
class RiskDecision:
    allowed: bool
    level: RiskLevel
    reason: str
    payload: dict[str, Any]


@dataclass(slots=True)
class TradeResult:
    signal: ArbSignal
    status: str
    gross_pnl_usd: float
    fees_usd: float
    slippage_usd: float
    net_pnl_usd: float
    capital_used_usd: float
    hold_minutes: float | None
    fills: list[FillEvent]
    metadata: dict[str, Any]


@dataclass(slots=True, frozen=True)
class AiGateDecision:
    allow: bool
    confidence: float
    mode: str
    votes: dict[str, bool]
    reason: str


def now_utc() -> datetime:
    return datetime.now(tz=UTC)
