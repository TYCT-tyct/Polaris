from __future__ import annotations

import pytest

from polaris.arb.config import arb_config_from_settings
from polaris.arb.contracts import RunMode
from polaris.arb.execution.risk_gate import RiskGate
from polaris.config import PolarisSettings


@pytest.mark.asyncio
async def test_risk_gate_exposure_reads_open_position_lots(db) -> None:
    run_tag = "unit-risk-open-lot"
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_run_tag=run_tag,
    )
    gate = RiskGate(db, arb_config_from_settings(settings))

    await db.execute(
        """
        insert into arb_position_lot(
            mode, strategy_code, source_code, market_id, token_id, side,
            open_notional_usd, remaining_size, status, run_tag, opened_at
        ) values
        ('paper_live', 'G', 'polymarket', 'm1', 't1', 'BUY', 7.25, 1.0, 'open', %s, now()),
        ('paper_live', 'G', 'polymarket', 'm1', 't1', 'BUY', 3.10, 0.0, 'closed', %s, now()),
        ('paper_live', 'G', 'other_source', 'm2', 't2', 'BUY', 9.00, 1.0, 'open', %s, now()),
        ('paper_live', 'G', 'polymarket', 'm3', 't3', 'BUY', 11.0, 1.0, 'open', 'other-tag', now())
        """,
        (run_tag, run_tag, run_tag),
    )

    state = await gate.load_state(RunMode.PAPER_LIVE, "polymarket")
    assert state.exposure_usd == pytest.approx(7.25)

    exposure = await gate.current_exposure_usd(RunMode.PAPER_LIVE, "polymarket")
    assert exposure == pytest.approx(7.25)

