from __future__ import annotations

from datetime import UTC, datetime

import pytest

from polaris.arb.config import arb_config_from_settings
from polaris.arb.contracts import ArbSignal, RunMode, StrategyCode
from polaris.arb.execution.risk_gate import RiskGate
from polaris.config import PolarisSettings


def _signal() -> ArbSignal:
    return ArbSignal(
        strategy_code=StrategyCode.A,
        mode=RunMode.PAPER_LIVE,
        source_code="polymarket",
        event_id="event-x",
        market_ids=["m1"],
        token_ids=["t1"],
        edge_pct=0.03,
        expected_pnl_usd=0.03,
        ttl_ms=5_000,
        features={"legs": [{"market_id": "m1", "token_id": "t1", "side": "BUY", "price": 0.5, "shares": 2.0}]},
        decision_note="test",
    )


@pytest.mark.asyncio
async def test_risk_gate_blocks_after_daily_stop(db) -> None:
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_daily_stop_loss_usd=0.5,
        arb_single_risk_usd=2.0,
    )
    config = arb_config_from_settings(settings)
    gate = RiskGate(db, config)

    await db.execute(
        """
        insert into arb_signal(
            signal_id, mode, strategy_code, source_code, event_id, market_ids, token_ids,
            edge_pct, expected_pnl_usd, ttl_ms, features, status, decision_note, created_at
        ) values (
            gen_random_uuid(), 'paper_live', 'A', 'polymarket', 'event-x', '{m1}', '{t1}',
            0.03, 0.03, 5000, '{}'::jsonb, 'executed', 'seed', now()
        )
        """
    )
    await db.execute(
        """
        insert into arb_trade_result(
            signal_id, mode, strategy_code, source_code, status, gross_pnl_usd, fees_usd,
            slippage_usd, net_pnl_usd, capital_used_usd, hold_minutes, opened_at, closed_at, metadata, created_at
        )
        select signal_id, 'paper_live', 'A', 'polymarket', 'filled', -0.60, 0, 0, -0.60, 1, 1, now(), now(), '{}'::jsonb, now()
        from arb_signal
        where mode = 'paper_live' and strategy_code = 'A' and source_code = 'polymarket'
        order by created_at desc
        limit 1
        """
    )

    state = await gate.load_state(RunMode.PAPER_LIVE, "polymarket")
    decision = await gate.assess(_signal(), 1.0, state=state)
    assert not decision.allowed
    assert decision.reason == "daily_stop_loss_triggered"


@pytest.mark.asyncio
async def test_risk_gate_uses_in_memory_state_after_reserve(db) -> None:
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_max_exposure_usd=2.0,
        arb_single_risk_usd=2.0,
    )
    config = arb_config_from_settings(settings)
    gate = RiskGate(db, config)
    state = await gate.load_state(RunMode.PAPER_LIVE, "polymarket")
    state.day_pnl_usd = 0.0
    state.exposure_usd = 0.0
    state.consecutive_failures = 0
    state.loaded_at = datetime.now(tz=UTC)

    gate.reserve_exposure(state, 1.6)
    decision = await gate.assess(_signal(), 0.6, state=state)
    assert not decision.allowed
    assert decision.reason == "max_exposure_exceeded"
