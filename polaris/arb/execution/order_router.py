from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from polaris.arb.config import ArbConfig
from polaris.arb.contracts import ExecutionPlan, FillEvent, RunMode, TokenSnapshot, TradeResult
from polaris.arb.execution.fill_simulator import simulate_buy_fill, simulate_sell_fill
from polaris.db.pool import Database


class OrderRouter:
    def __init__(self, db: Database, config: ArbConfig) -> None:
        self.db = db
        self.config = config

    async def execute(self, plan: ExecutionPlan, snapshots: dict[str, TokenSnapshot]) -> TradeResult:
        signal = plan.signal
        starts_at = datetime.now(tz=UTC)
        await self._record_intents(plan)

        if signal.mode in {RunMode.SHADOW, RunMode.PAPER_LIVE, RunMode.PAPER_REPLAY}:
            result = await self._execute_paper(plan, snapshots)
        else:
            result = await self._execute_live(plan, snapshots)

        await self._record_trade_result(result, starts_at)
        return result

    async def _execute_paper(self, plan: ExecutionPlan, snapshots: dict[str, TokenSnapshot]) -> TradeResult:
        fills: list[FillEvent] = []
        total_notional = 0.0
        for intent in plan.intents:
            snap = snapshots.get(intent.token_id)
            if snap is None:
                await self._record_order_event(intent.intent_id, "reject", 404, "missing_snapshot", {})
                continue
            fill = _simulate_intent_fill(intent.limit_price, intent.shares, intent.notional_usd, intent.side, snap)
            if fill is None:
                await self._record_order_event(intent.intent_id, "reject", 422, "insufficient_liquidity", {})
                continue
            fills.append(fill)
            total_notional += fill.fill_notional_usd
            await self._record_order_event(intent.intent_id, "fill", 200, "paper_fill", {
                "price": fill.fill_price,
                "size": fill.fill_size,
                "notional": fill.fill_notional_usd,
            })
            await self._record_fill(intent.intent_id, fill)

        gross, fees, slip = _estimate_trade_pnl(plan.signal.features, fills)
        return TradeResult(
            signal=plan.signal,
            status="filled" if fills else "rejected",
            gross_pnl_usd=gross,
            fees_usd=fees,
            slippage_usd=slip,
            net_pnl_usd=gross - fees - slip,
            capital_used_usd=total_notional,
            hold_minutes=float(plan.signal.features.get("expected_hold_minutes", 0) or 0),
            fills=fills,
            metadata={"mode": plan.signal.mode.value, "intent_count": len(plan.intents)},
        )

    async def _execute_live(self, plan: ExecutionPlan, snapshots: dict[str, TokenSnapshot]) -> TradeResult:
        # live mode keeps the same execution contract, but requires py-clob-client at runtime.
        try:
            from py_clob_client.client import ClobClient as LiveClient
            from py_clob_client.clob_types import OrderArgs, OrderType
        except Exception as exc:  # pragma: no cover - runtime dependency path
            for intent in plan.intents:
                await self._record_order_event(intent.intent_id, "error", 500, "py_clob_client_missing", {"error": str(exc)})
            return TradeResult(
                signal=plan.signal,
                status="error",
                gross_pnl_usd=0.0,
                fees_usd=0.0,
                slippage_usd=0.0,
                net_pnl_usd=0.0,
                capital_used_usd=0.0,
                hold_minutes=None,
                fills=[],
                metadata={"error": "py_clob_client_missing"},
            )

        # live credentials are read from process env to keep secrets out of source control.
        import os

        host = os.getenv("POLARIS_ARB_LIVE_HOST", "https://clob.polymarket.com")
        key = os.getenv("POLARIS_ARB_LIVE_PRIVATE_KEY")
        chain_id = int(os.getenv("POLARIS_ARB_LIVE_CHAIN_ID", "137"))
        if not key:
            for intent in plan.intents:
                await self._record_order_event(intent.intent_id, "error", 500, "missing_live_key", {})
            return TradeResult(
                signal=plan.signal,
                status="error",
                gross_pnl_usd=0.0,
                fees_usd=0.0,
                slippage_usd=0.0,
                net_pnl_usd=0.0,
                capital_used_usd=0.0,
                hold_minutes=None,
                fills=[],
                metadata={"error": "missing_live_key"},
            )

        client = LiveClient(host=host, key=key, chain_id=chain_id)
        client.set_api_creds(client.create_or_derive_api_creds())
        fills: list[FillEvent] = []
        total_notional = 0.0
        for intent in plan.intents:
            snap = snapshots.get(intent.token_id)
            if snap is None:
                await self._record_order_event(intent.intent_id, "reject", 404, "missing_snapshot", {})
                continue
            shares = _resolve_shares(intent.limit_price, intent.shares, intent.notional_usd, intent.side, snap)
            if shares <= 0:
                await self._record_order_event(intent.intent_id, "reject", 422, "invalid_size", {})
                continue
            price = _resolve_price(intent.limit_price, intent.side, snap)
            args = OrderArgs(token_id=intent.token_id, side=intent.side, price=price, size=shares)
            try:
                signed = client.create_order(args)
                resp = client.post_order(signed, OrderType.GTC)
                await self._record_order_event(intent.intent_id, "submit", 200, "live_submit", {"response": resp})
                fill = FillEvent(
                    token_id=intent.token_id,
                    market_id=intent.market_id,
                    side=intent.side,
                    fill_price=price,
                    fill_size=shares,
                    fill_notional_usd=shares * price,
                    fee_usd=0.0,
                )
                fills.append(fill)
                total_notional += fill.fill_notional_usd
                await self._record_fill(intent.intent_id, fill)
            except Exception as exc:  # pragma: no cover - network/runtime path
                await self._record_order_event(intent.intent_id, "error", 500, "live_submit_error", {"error": str(exc)})

        gross, fees, slip = _estimate_trade_pnl(plan.signal.features, fills)
        return TradeResult(
            signal=plan.signal,
            status="filled" if fills else "error",
            gross_pnl_usd=gross,
            fees_usd=fees,
            slippage_usd=slip,
            net_pnl_usd=gross - fees - slip,
            capital_used_usd=total_notional,
            hold_minutes=float(plan.signal.features.get("expected_hold_minutes", 0) or 0),
            fills=fills,
            metadata={"mode": plan.signal.mode.value, "intent_count": len(plan.intents)},
        )

    async def _record_intents(self, plan: ExecutionPlan) -> None:
        rows = [
            (
                str(i.intent_id),
                str(i.signal_id),
                i.mode.value,
                i.strategy_code.value,
                i.source_code,
                i.order_index,
                i.market_id,
                i.token_id,
                i.side,
                i.order_type,
                i.limit_price,
                i.shares,
                i.notional_usd,
                json.dumps(i.payload, ensure_ascii=True),
            )
            for i in plan.intents
        ]
        if not rows:
            return
        await self.db.executemany(
            """
            insert into arb_order_intent(
                intent_id, signal_id, mode, strategy_code, source_code, order_index, market_id,
                token_id, side, order_type, limit_price, shares, notional_usd, payload, status, created_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, 'created', now())
            on conflict (intent_id) do nothing
            """,
            rows,
        )

    async def _record_order_event(
        self,
        intent_id: Any,
        event_type: str,
        status_code: int,
        message: str,
        payload: dict[str, Any],
    ) -> None:
        await self.db.execute(
            """
            insert into arb_order_event(intent_id, event_type, status_code, message, payload, created_at)
            values (%s, %s, %s, %s, %s::jsonb, now())
            """,
            (str(intent_id), event_type, status_code, message, json.dumps(payload, ensure_ascii=True)),
        )

    async def _record_fill(self, intent_id: Any, fill: FillEvent) -> None:
        await self.db.execute(
            """
            insert into arb_fill(
                intent_id, market_id, token_id, side, fill_price, fill_size, fill_notional_usd, fee_usd, created_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, now())
            """,
            (
                str(intent_id),
                fill.market_id,
                fill.token_id,
                fill.side,
                fill.fill_price,
                fill.fill_size,
                fill.fill_notional_usd,
                fill.fee_usd,
            ),
        )

    async def _record_trade_result(self, result: TradeResult, started_at: datetime) -> None:
        signal = result.signal
        await self.db.execute(
            """
            insert into arb_trade_result(
                signal_id, mode, strategy_code, source_code, status, gross_pnl_usd, fees_usd,
                slippage_usd, net_pnl_usd, capital_used_usd, hold_minutes, opened_at, closed_at, metadata, created_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s::jsonb, now())
            """,
            (
                str(signal.signal_id),
                signal.mode.value,
                signal.strategy_code.value,
                signal.source_code,
                result.status,
                result.gross_pnl_usd,
                result.fees_usd,
                result.slippage_usd,
                result.net_pnl_usd,
                result.capital_used_usd,
                result.hold_minutes,
                started_at,
                json.dumps(result.metadata, ensure_ascii=True),
            ),
        )


def _resolve_price(limit_price: float | None, side: str, snap: TokenSnapshot) -> float:
    if limit_price is not None:
        return limit_price
    if side.upper() == "BUY":
        return snap.best_ask or 0.0
    return snap.best_bid or 0.0


def _resolve_shares(
    limit_price: float | None,
    shares: float | None,
    notional_usd: float | None,
    side: str,
    snap: TokenSnapshot,
) -> float:
    if shares is not None and shares > 0:
        return shares
    if notional_usd is None or notional_usd <= 0:
        return 0.0
    base_price = _resolve_price(limit_price, side, snap)
    if base_price <= 0:
        return 0.0
    return notional_usd / base_price


def _simulate_intent_fill(
    limit_price: float | None,
    shares: float | None,
    notional_usd: float | None,
    side: str,
    snap: TokenSnapshot,
) -> FillEvent | None:
    wanted = _resolve_shares(limit_price, shares, notional_usd, side, snap)
    if wanted <= 0:
        return None
    if side.upper() == "BUY":
        sim = simulate_buy_fill(snap.asks, wanted)
    else:
        sim = simulate_sell_fill(snap.bids, wanted)
    if sim is None:
        return None
    return FillEvent(
        token_id=snap.token_id,
        market_id=snap.market_id,
        side=side.upper(),
        fill_price=sim.avg_price,
        fill_size=sim.filled_size,
        fill_notional_usd=sim.notional,
        fee_usd=0.0,
    )


def _estimate_trade_pnl(features: dict[str, Any], fills: list[FillEvent]) -> tuple[float, float, float]:
    if not fills:
        return 0.0, 0.0, 0.0
    expected_edge = float(features.get("expected_edge_pct") or 0.0)
    notional = sum(f.fill_notional_usd for f in fills)
    gross = notional * expected_edge
    fees = sum(f.fill_notional_usd for f in fills) * 0.001
    slippage = float(features.get("expected_slippage_usd") or 0.0)
    return gross, fees, slippage
