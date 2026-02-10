from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime

from polaris.arb.config import ArbConfig
from polaris.arb.contracts import ArbSignal, RiskDecision, RiskLevel, RunMode, StrategyCode
from polaris.db.pool import Database


@dataclass(slots=True)
class RiskRuntimeState:
    mode: RunMode
    source_code: str
    exposure_usd: float
    day_pnl_usd: float
    consecutive_failures: int
    loaded_at: datetime


class RiskGate:
    def __init__(self, db: Database, config: ArbConfig) -> None:
        self.db = db
        self.config = config
        self._consecutive_failures = 0

    async def load_state(self, mode: RunMode, source_code: str) -> RiskRuntimeState:
        row = await self.db.fetch_one(
            """
            select
                coalesce((
                    select sum(open_notional_usd)
                    from arb_position_lot
                    where mode = %s
                      and source_code = %s
                      and status = 'open'
                ), 0) as exposure,
                coalesce((
                    select sum(net_pnl_usd)
                    from arb_trade_result
                    where mode = %s
                      and source_code = %s
                      and created_at >= date_trunc('day', now())
                ), 0) as day_pnl
            """,
            (mode.value, source_code, mode.value, source_code),
        )
        exposure = float(row["exposure"]) if row else 0.0
        day_pnl = float(row["day_pnl"]) if row else 0.0
        return RiskRuntimeState(
            mode=mode,
            source_code=source_code,
            exposure_usd=exposure,
            day_pnl_usd=day_pnl,
            consecutive_failures=self._consecutive_failures,
            loaded_at=datetime.now(tz=UTC),
        )

    async def assess(
        self,
        signal: ArbSignal,
        capital_required_usd: float,
        state: RiskRuntimeState | None = None,
    ) -> RiskDecision:
        runtime_state = state or await self.load_state(signal.mode, signal.source_code)

        if signal.mode == RunMode.LIVE and signal.strategy_code == StrategyCode.C and not self.config.c_live_enabled:
            return RiskDecision(False, RiskLevel.HARD_STOP, "strategy_c_live_disabled", {})

        if capital_required_usd > self.config.single_risk_usd:
            return RiskDecision(
                False,
                RiskLevel.WARN,
                "single_trade_risk_exceeded",
                {"capital_required_usd": capital_required_usd, "limit_usd": self.config.single_risk_usd},
            )

        if runtime_state.exposure_usd + capital_required_usd > self.config.max_exposure_usd:
            return RiskDecision(
                False,
                RiskLevel.WARN,
                "max_exposure_exceeded",
                {
                    "current_exposure_usd": runtime_state.exposure_usd,
                    "capital_required_usd": capital_required_usd,
                    "limit_usd": self.config.max_exposure_usd,
                },
            )

        if runtime_state.day_pnl_usd <= -abs(self.config.daily_stop_loss_usd):
            return RiskDecision(
                False,
                RiskLevel.HARD_STOP,
                "daily_stop_loss_triggered",
                {"day_pnl_usd": runtime_state.day_pnl_usd, "limit_usd": self.config.daily_stop_loss_usd},
            )

        if runtime_state.consecutive_failures >= self.config.consecutive_fail_limit:
            return RiskDecision(
                False,
                RiskLevel.HARD_STOP,
                "consecutive_fail_limit_triggered",
                {"failures": runtime_state.consecutive_failures, "limit": self.config.consecutive_fail_limit},
            )

        return RiskDecision(True, RiskLevel.INFO, "ok", {})

    async def current_day_pnl_usd(self, mode: RunMode, source_code: str) -> float:
        row = await self.db.fetch_one(
            """
            select coalesce(sum(net_pnl_usd), 0) as pnl
            from arb_trade_result
            where mode = %s
              and source_code = %s
              and created_at >= date_trunc('day', now())
            """,
            (mode.value, source_code),
        )
        return float(row["pnl"]) if row else 0.0

    async def current_exposure_usd(self, mode: RunMode, source_code: str) -> float:
        row = await self.db.fetch_one(
            """
            select coalesce(sum(open_notional_usd), 0) as exposure
            from arb_position_lot
            where mode = %s
              and source_code = %s
              and status = 'open'
            """,
            (mode.value, source_code),
        )
        return float(row["exposure"]) if row else 0.0

    def reserve_exposure(self, state: RiskRuntimeState, capital_required_usd: float) -> None:
        state.exposure_usd += max(0.0, capital_required_usd)

    def settle_execution(
        self,
        state: RiskRuntimeState,
        success: bool,
        capital_required_usd: float,
        realized_pnl_usd: float = 0.0,
    ) -> None:
        state.exposure_usd = max(0.0, state.exposure_usd - max(0.0, capital_required_usd))
        if success:
            state.day_pnl_usd += realized_pnl_usd
            self._consecutive_failures = 0
            state.consecutive_failures = 0
            return
        self._consecutive_failures += 1
        state.consecutive_failures = self._consecutive_failures

    async def record_risk_event(
        self,
        mode: RunMode,
        strategy_code: StrategyCode | None,
        source_code: str,
        decision: RiskDecision,
    ) -> None:
        await self.db.execute(
            """
            insert into arb_risk_event(
                mode, strategy_code, source_code, event_type, severity, reason, payload, created_at
            )
            values (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
            """,
            (
                mode.value,
                strategy_code.value if strategy_code else None,
                source_code,
                "risk_gate",
                decision.level.value,
                decision.reason,
                json.dumps(decision.payload, ensure_ascii=True),
                datetime.now(tz=UTC),
            ),
        )

    def note_execution_outcome(self, success: bool) -> None:
        if success:
            self._consecutive_failures = 0
            return
        self._consecutive_failures += 1
