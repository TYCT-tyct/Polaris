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
    strategy_code: StrategyCode | None
    exposure_usd: float
    day_pnl_usd: float
    cash_balance_usd: float
    consecutive_failures: int
    loaded_at: datetime


class RiskGate:
    def __init__(self, db: Database, config: ArbConfig) -> None:
        self.db = db
        self.config = config
        self._consecutive_failures_by_scope: dict[str, int] = {}

    async def load_state(
        self,
        mode: RunMode,
        source_code: str,
        strategy_code: StrategyCode | None = None,
    ) -> RiskRuntimeState:
        strategy_filter = strategy_code.value if strategy_code else None
        # paper shared 资金池的现金账本使用 strategy_code='shared' 单独记录，避免混入单策略账本。
        ledger_filter = strategy_filter
        if ledger_filter is None and mode in {RunMode.PAPER_LIVE, RunMode.PAPER_REPLAY}:
            ledger_filter = "shared"
        row = await self.db.fetch_one(
            """
            select
                coalesce((
                    select sum(open_notional_usd)
                    from arb_position_lot
                    where mode = %s
                      and source_code = %s
                      and run_tag = %s
                      and status = 'open'
                      and (%s::text is null or strategy_code = %s::text)
                ), 0) as exposure,
                coalesce((
                    select sum(net_pnl_usd)
                    from arb_trade_result
                    where mode = %s
                      and source_code = %s
                      and (%s::text is null or strategy_code = %s::text)
                      and coalesce(metadata->>'run_tag', '') = %s
                      and created_at >= date_trunc('day', now())
                ), 0) as day_pnl,
                (
                    select balance_after_usd
                    from arb_cash_ledger
                    where mode = %s
                      and source_code = %s
                      and (%s::text is null or strategy_code = %s::text)
                      and coalesce(payload->>'run_tag', '') = %s
                    order by created_at desc
                    limit 1
                ) as cash_balance,
                exists(
                    select 1
                    from arb_cash_ledger
                    where mode = %s
                      and source_code = %s
                      and (%s::text is null or strategy_code = %s::text)
                      and coalesce(payload->>'run_tag', '') = %s
                      and entry_type in ('buy_cost', 'sell_proceeds', 'fee', 'slippage')
                    limit 1
                ) as has_cash_flow
            """,
            (
                # exposure
                mode.value,
                source_code,
                self.config.run_tag,
                strategy_filter,
                strategy_filter,
                # day_pnl
                mode.value,
                source_code,
                strategy_filter,
                strategy_filter,
                self.config.run_tag,
                # cash_balance
                mode.value,
                source_code,
                ledger_filter,
                ledger_filter,
                self.config.run_tag,
                # has_cash_flow
                mode.value,
                source_code,
                ledger_filter,
                ledger_filter,
                self.config.run_tag,
            ),
        )
        exposure = float(row["exposure"]) if row else 0.0
        day_pnl = float(row["day_pnl"]) if row else 0.0
        base_cash_balance = _resolve_cash_balance(mode, row["cash_balance"] if row else None, self.config.paper_initial_bankroll_usd)
        cash_balance = base_cash_balance
        has_cash_flow = bool(row["has_cash_flow"]) if row else False
        if self.config.paper_enforce_bankroll and mode in {RunMode.PAPER_LIVE, RunMode.PAPER_REPLAY}:
            # 兼容两种 paper 口径：
            # 1) 旧口径：cash_ledger 余额基本不变，用 exposure 表示资金占用 => 可用现金 = 余额 - exposure
            # 2) 新口径：cash_ledger 记录 buy_cost/sell_proceeds/fee 等现金流 => 余额已是“未占用现金”，不要再减 exposure
            if not has_cash_flow:
                cash_balance = max(0.0, base_cash_balance - exposure)
        return RiskRuntimeState(
            mode=mode,
            source_code=source_code,
            strategy_code=strategy_code,
            exposure_usd=exposure,
            day_pnl_usd=day_pnl,
            cash_balance_usd=cash_balance,
            consecutive_failures=self._consecutive_failures_by_scope.get(
                _failure_scope_key(mode, source_code, strategy_code),
                0,
            ),
            loaded_at=datetime.now(tz=UTC),
        )

    async def assess(
        self,
        signal: ArbSignal,
        capital_required_usd: float,
        state: RiskRuntimeState | None = None,
    ) -> RiskDecision:
        runtime_state = state or await self.load_state(
            signal.mode,
            signal.source_code,
            strategy_code=signal.strategy_code
            if self.config.paper_split_by_strategy and signal.mode in {RunMode.PAPER_LIVE, RunMode.PAPER_REPLAY}
            else None,
        )

        if signal.mode == RunMode.LIVE and signal.strategy_code == StrategyCode.C and not self.config.c_live_enabled:
            return RiskDecision(False, RiskLevel.HARD_STOP, "strategy_c_live_disabled", {})

        if (
            self.config.paper_enforce_bankroll
            and signal.mode in {RunMode.PAPER_LIVE, RunMode.PAPER_REPLAY}
            and capital_required_usd > runtime_state.cash_balance_usd
        ):
            return RiskDecision(
                False,
                RiskLevel.WARN,
                "insufficient_bankroll",
                {
                    "cash_balance_usd": runtime_state.cash_balance_usd,
                    "capital_required_usd": capital_required_usd,
                },
            )

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
              and coalesce(metadata->>'run_tag', '') = %s
              and created_at >= date_trunc('day', now())
            """,
            (mode.value, source_code, self.config.run_tag),
        )
        return float(row["pnl"]) if row else 0.0

    async def current_exposure_usd(self, mode: RunMode, source_code: str) -> float:
        row = await self.db.fetch_one(
            """
            select coalesce(sum(open_notional_usd), 0) as exposure
            from arb_position_lot
            where mode = %s
              and source_code = %s
              and run_tag = %s
              and status = 'open'
            """,
            (mode.value, source_code, self.config.run_tag),
        )
        return float(row["exposure"]) if row else 0.0

    def reserve_exposure(self, state: RiskRuntimeState, capital_required_usd: float) -> None:
        state.exposure_usd += max(0.0, capital_required_usd)
        if self.config.paper_enforce_bankroll and state.mode in {RunMode.PAPER_LIVE, RunMode.PAPER_REPLAY}:
            state.cash_balance_usd = max(0.0, state.cash_balance_usd - max(0.0, capital_required_usd))

    def settle_execution(
        self,
        state: RiskRuntimeState,
        success: bool,
        capital_required_usd: float,
        realized_pnl_usd: float = 0.0,
        release_exposure: bool = True,
    ) -> None:
        if release_exposure:
            state.exposure_usd = max(0.0, state.exposure_usd - max(0.0, capital_required_usd))
            if self.config.paper_enforce_bankroll and state.mode in {RunMode.PAPER_LIVE, RunMode.PAPER_REPLAY}:
                state.cash_balance_usd += max(0.0, capital_required_usd)
        if success:
            state.day_pnl_usd += realized_pnl_usd
            if self.config.paper_enforce_bankroll and state.mode in {RunMode.PAPER_LIVE, RunMode.PAPER_REPLAY}:
                state.cash_balance_usd += realized_pnl_usd
            self._consecutive_failures_by_scope[_failure_scope_key(state.mode, state.source_code, state.strategy_code)] = 0
            state.consecutive_failures = 0
            return
        scope_key = _failure_scope_key(state.mode, state.source_code, state.strategy_code)
        next_value = self._consecutive_failures_by_scope.get(scope_key, 0) + 1
        self._consecutive_failures_by_scope[scope_key] = next_value
        state.consecutive_failures = next_value

    async def record_risk_event(
        self,
        mode: RunMode,
        strategy_code: StrategyCode | None,
        source_code: str,
        decision: RiskDecision,
    ) -> None:
        payload = dict(decision.payload)
        payload["run_tag"] = self.config.run_tag
        payload["execution_backend"] = self.config.execution_backend
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
                json.dumps(payload, ensure_ascii=True),
                datetime.now(tz=UTC),
            ),
        )

    def note_execution_outcome(self, success: bool) -> None:
        if success:
            self._consecutive_failures_by_scope.clear()
            return
        # 保留兼容入口；不再用于细粒度风控。
        return


def _load_state_params(
    mode_value: str,
    source_code: str,
    strategy_filter: str | None,
    run_tag: str,
) -> tuple[object, ...]:
    if strategy_filter is None:
        return (
            mode_value,
            source_code,
            run_tag,
            mode_value,
            source_code,
            run_tag,
            mode_value,
            source_code,
            run_tag,
        )
    return (
        mode_value,
        source_code,
        strategy_filter,
        run_tag,
        mode_value,
        source_code,
        strategy_filter,
        run_tag,
        mode_value,
        source_code,
        strategy_filter,
        run_tag,
    )


def _resolve_cash_balance(mode: RunMode, value: object, initial: float) -> float:
    if value is not None:
        return float(value)
    if mode in {RunMode.PAPER_LIVE, RunMode.PAPER_REPLAY}:
        return float(initial)
    return 0.0


def _failure_scope_key(mode: RunMode, source_code: str, strategy_code: StrategyCode | None) -> str:
    strategy_part = strategy_code.value if strategy_code else "shared"
    return f"{mode.value}:{source_code}:{strategy_part}"
