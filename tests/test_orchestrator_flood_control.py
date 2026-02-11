from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from polaris.arb.config import arb_config_from_settings
from polaris.arb.contracts import ArbSignal, PriceLevel, RiskDecision, RiskLevel, RunMode, StrategyCode, TokenSnapshot
from polaris.arb.execution.risk_gate import RiskRuntimeState
from polaris.arb.orchestrator import ArbOrchestrator
from polaris.config import PolarisSettings


def _snapshot(token_id: str) -> TokenSnapshot:
    now = datetime.now(tz=UTC)
    return TokenSnapshot(
        token_id=token_id,
        market_id="m1",
        event_id="e1",
        market_question="q",
        market_end=now,
        outcome_label="YES",
        outcome_side="YES",
        outcome_index=0,
        min_order_size=1.0,
        tick_size=0.01,
        best_bid=0.49,
        best_ask=0.50,
        bids=(PriceLevel(price=0.49, size=50.0),),
        asks=(PriceLevel(price=0.50, size=50.0),),
        captured_at=now,
    )


def _signal(source_code: str, token_id: str) -> ArbSignal:
    return ArbSignal(
        strategy_code=StrategyCode.F,
        mode=RunMode.PAPER_LIVE,
        source_code=source_code,
        event_id="event-x",
        market_ids=["m1"],
        token_ids=[token_id],
        edge_pct=0.03,
        expected_pnl_usd=0.03,
        ttl_ms=30_000,
        features={
            "expected_edge_pct": 0.03,
            "expected_hold_minutes": 60,
            "legs": [
                {
                    "market_id": "m1",
                    "token_id": token_id,
                    "side": "BUY",
                    "price": 0.5,
                    "shares": 2.0,
                    "notional_usd": 1.0,
                }
            ],
        },
        decision_note="flood-test",
    )


class _NoopDb:
    async def execute(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        return None

    async def executemany(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        return None

    async def fetch_one(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return None

    async def fetch_all(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return []


class _NoAiGate:
    async def evaluate(self, signal: ArbSignal, context: dict) -> SimpleNamespace:  # noqa: ARG002
        return SimpleNamespace(allow=True, reason="ok")


@pytest.mark.asyncio
async def test_hard_stop_rejection_records_once_per_scope(monkeypatch) -> None:
    source_code = "flood-test"
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_scope_block_cooldown_sec=300,
        arb_signal_dedupe_ttl_sec=30,
        arb_max_signals_per_cycle=100,
    )
    config = arb_config_from_settings(settings)
    orchestrator = ArbOrchestrator(
        db=_NoopDb(),
        clob_client=SimpleNamespace(),
        config=config,
        ai_gate=_NoAiGate(),
    )

    snapshots = [_snapshot("seed-token")]
    signals = [_signal(source_code, f"t{i}") for i in range(8)]

    async def _load_live_snapshots() -> list[TokenSnapshot]:
        return snapshots

    async def _load_state(mode: RunMode, source: str, strategy_code=None) -> RiskRuntimeState:  # noqa: ANN001
        return RiskRuntimeState(
            mode=mode,
            source_code=source,
            strategy_code=strategy_code,
            exposure_usd=0.0,
            day_pnl_usd=-1.0,
            cash_balance_usd=10.0,
            consecutive_failures=0,
            loaded_at=datetime.now(tz=UTC),
        )

    async def _assess(signal: ArbSignal, capital_required_usd: float, state=None) -> RiskDecision:  # noqa: ANN001
        return RiskDecision(
            allowed=False,
            level=RiskLevel.HARD_STOP,
            reason="daily_stop_loss_triggered",
            payload={"capital_required_usd": capital_required_usd},
        )

    calls = {"risk_event": 0, "rejected_rows": 0}

    async def _record_risk_event(*args, **kwargs) -> None:  # noqa: ANN002, ANN003
        calls["risk_event"] += 1

    async def _load_balance(*args, **kwargs) -> float:  # noqa: ANN002, ANN003
        return 10.0

    async def _record_signals(rows):  # noqa: ANN001
        calls["rejected_rows"] += len(rows)

    monkeypatch.setattr(orchestrator, "_load_live_snapshots", _load_live_snapshots)
    monkeypatch.setattr(orchestrator, "_scan_all", lambda mode, source, snaps: list(signals))
    monkeypatch.setattr(orchestrator, "_order_signals", lambda rows: rows)
    monkeypatch.setattr(orchestrator, "_record_signals", _record_signals)
    monkeypatch.setattr(orchestrator, "_load_latest_cash_balance", _load_balance)
    monkeypatch.setattr(orchestrator.risk_gate, "load_state", _load_state)
    monkeypatch.setattr(orchestrator.risk_gate, "assess", _assess)
    monkeypatch.setattr(orchestrator.risk_gate, "record_risk_event", _record_risk_event)

    stats = await orchestrator.run_once(RunMode.PAPER_LIVE, source_code=source_code)
    assert stats["signals"] == 8
    assert calls["rejected_rows"] == 1
    assert calls["risk_event"] == 1
