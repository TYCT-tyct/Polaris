from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from polaris.arb.config import arb_config_from_settings
from polaris.arb.contracts import ArbSignal, PriceLevel, RunMode, SignalStatus, StrategyCode, TokenSnapshot
from polaris.arb.orchestrator import ArbOrchestrator
from polaris.config import PolarisSettings


def _snapshot(token_id: str = "t1") -> TokenSnapshot:
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
        bids=(PriceLevel(price=0.49, size=100.0),),
        asks=(PriceLevel(price=0.50, size=100.0),),
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
        decision_note="health-test",
    )


class _NoopDb:
    def __init__(self, row: dict | None = None) -> None:
        self._row = row or {}

    async def fetch_one(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return self._row

    async def fetch_all(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return []

    async def execute(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        return None

    async def executemany(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        return None


class _NoAiGate:
    async def evaluate(self, signal: ArbSignal, context: dict) -> SimpleNamespace:  # noqa: ARG002
        return SimpleNamespace(allow=True, reason="ok")


@pytest.mark.asyncio
async def test_strategy_health_decision_blocks_lossing_strategy() -> None:
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_strategy_health_gate_enabled=True,
        arb_strategy_health_window_hours=24,
        arb_strategy_health_min_trades=12,
        arb_strategy_health_max_loss_usd=0.8,
    )
    orchestrator = ArbOrchestrator(
        db=_NoopDb(
            {
                "trades": 20,
                "net_pnl_usd": -1.25,
                "avg_trade_pnl_usd": -0.06,
                "win_rate": 0.25,
            }
        ),
        clob_client=SimpleNamespace(),
        config=arb_config_from_settings(settings),
        ai_gate=_NoAiGate(),
    )
    ok, payload = await orchestrator._strategy_health_decision(_signal("paper-health", "t1"))
    assert not ok
    assert payload["trades"] == 20
    assert payload["net_pnl_usd"] == pytest.approx(-1.25)


@pytest.mark.asyncio
async def test_run_once_blocks_scope_when_strategy_health_bad(monkeypatch) -> None:
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_strategy_health_gate_enabled=True,
        arb_scope_block_cooldown_sec=300,
        arb_signal_dedupe_ttl_sec=30,
    )
    orchestrator = ArbOrchestrator(
        db=_NoopDb(),
        clob_client=SimpleNamespace(),
        config=arb_config_from_settings(settings),
        ai_gate=_NoAiGate(),
    )
    source_code = "paper-health-scope"
    signals = [_signal(source_code, f"t{i}") for i in range(6)]
    rejected_rows: list[tuple[ArbSignal, SignalStatus, str | None]] = []
    risk_calls: list[str] = []

    async def _load_live_snapshots() -> list[TokenSnapshot]:
        return [_snapshot()]

    async def _record_signals(rows):  # noqa: ANN001
        rejected_rows.extend(rows)

    async def _strategy_health_decision(signal: ArbSignal):  # noqa: ANN001
        return False, {"trades": 20.0, "net_pnl_usd": -1.25}

    async def _record_risk_event(mode, strategy_code, source_code, decision):  # noqa: ANN001
        risk_calls.append(decision.reason)

    monkeypatch.setattr(orchestrator, "_load_live_snapshots", _load_live_snapshots)
    monkeypatch.setattr(orchestrator, "_scan_all", lambda mode, source, snaps: list(signals))
    monkeypatch.setattr(orchestrator, "_order_signals", lambda rows: rows)
    monkeypatch.setattr(orchestrator, "_record_signals", _record_signals)
    monkeypatch.setattr(orchestrator, "_strategy_health_decision", _strategy_health_decision)
    monkeypatch.setattr(orchestrator.risk_gate, "record_risk_event", _record_risk_event)

    stats = await orchestrator.run_once(RunMode.PAPER_LIVE, source_code=source_code)
    assert stats["signals"] == 6
    assert stats["executed"] == 0
    assert len(rejected_rows) == 1
    assert rejected_rows[0][2] == "strategy_health_blocked"
    assert risk_calls == ["strategy_health_blocked"]
