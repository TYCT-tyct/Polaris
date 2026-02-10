from __future__ import annotations

import json
from datetime import UTC, datetime

from polaris.arb.config import ArbConfig
from polaris.arb.contracts import ArbSignal, RiskDecision, RiskLevel, RunMode, StrategyCode
from polaris.db.pool import Database


class RiskGate:
    def __init__(self, db: Database, config: ArbConfig) -> None:
        self.db = db
        self.config = config
        self._consecutive_failures = 0

    async def assess(self, signal: ArbSignal, capital_required_usd: float) -> RiskDecision:
        if signal.mode == RunMode.LIVE and signal.strategy_code == StrategyCode.C and not self.config.c_live_enabled:
            return RiskDecision(False, RiskLevel.HARD_STOP, "strategy_c_live_disabled", {})

        if capital_required_usd > self.config.single_risk_usd:
            return RiskDecision(
                False,
                RiskLevel.WARN,
                "single_trade_risk_exceeded",
                {"capital_required_usd": capital_required_usd, "limit_usd": self.config.single_risk_usd},
            )

        exposure = await self.current_exposure_usd(signal.mode, signal.source_code)
        if exposure + capital_required_usd > self.config.max_exposure_usd:
            return RiskDecision(
                False,
                RiskLevel.WARN,
                "max_exposure_exceeded",
                {
                    "current_exposure_usd": exposure,
                    "capital_required_usd": capital_required_usd,
                    "limit_usd": self.config.max_exposure_usd,
                },
            )

        day_pnl = await self.current_day_pnl_usd(signal.mode, signal.source_code)
        if day_pnl <= -abs(self.config.daily_stop_loss_usd):
            return RiskDecision(
                False,
                RiskLevel.HARD_STOP,
                "daily_stop_loss_triggered",
                {"day_pnl_usd": day_pnl, "limit_usd": self.config.daily_stop_loss_usd},
            )

        if self._consecutive_failures >= self.config.consecutive_fail_limit:
            return RiskDecision(
                False,
                RiskLevel.HARD_STOP,
                "consecutive_fail_limit_triggered",
                {"failures": self._consecutive_failures, "limit": self.config.consecutive_fail_limit},
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
        else:
            self._consecutive_failures += 1
