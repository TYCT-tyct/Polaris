from __future__ import annotations

from datetime import UTC, datetime

from polaris.arb.config import arb_config_from_settings
from polaris.arb.contracts import RunMode
from polaris.arb.execution.risk_gate import RiskGate, RiskRuntimeState
from polaris.config import PolarisSettings


class _NoopDb:
    pass


def _state() -> RiskRuntimeState:
    return RiskRuntimeState(
        mode=RunMode.PAPER_LIVE,
        source_code="polymarket_shared10",
        strategy_code=None,
        exposure_usd=0.0,
        day_pnl_usd=0.0,
        cash_balance_usd=10.0,
        consecutive_failures=0,
        loaded_at=datetime.now(tz=UTC),
    )


def test_settle_execution_hold_position_keeps_exposure_locked() -> None:
    config = arb_config_from_settings(PolarisSettings(database_url="postgresql://example"))
    gate = RiskGate(_NoopDb(), config)
    state = _state()

    gate.reserve_exposure(state, 2.0)
    gate.settle_execution(
        state=state,
        success=True,
        capital_required_usd=2.0,
        realized_pnl_usd=0.1,
        release_exposure=False,
    )

    assert state.exposure_usd == 2.0
    assert state.cash_balance_usd == 8.1
    assert state.day_pnl_usd == 0.1


def test_settle_execution_immediate_position_releases_exposure() -> None:
    config = arb_config_from_settings(PolarisSettings(database_url="postgresql://example"))
    gate = RiskGate(_NoopDb(), config)
    state = _state()

    gate.reserve_exposure(state, 2.0)
    gate.settle_execution(
        state=state,
        success=True,
        capital_required_usd=2.0,
        realized_pnl_usd=0.1,
        release_exposure=True,
    )

    assert state.exposure_usd == 0.0
    assert state.cash_balance_usd == 10.1
    assert state.day_pnl_usd == 0.1
