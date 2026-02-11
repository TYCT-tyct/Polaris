from __future__ import annotations

import asyncio
import json
import logging
import os
from math import ceil, floor
from datetime import UTC, datetime
from typing import Any

from polaris.arb.config import ArbConfig
from polaris.arb.contracts import ExecutionPlan, FillEvent, RunMode, TokenSnapshot, TradeResult
from polaris.arb.execution.fill_simulator import simulate_buy_fill, simulate_sell_fill
from polaris.db.pool import Database
from polaris.sources.clob_client import ClobClient

logger = logging.getLogger(__name__)

class OrderRouter:
    def __init__(self, db: Database, config: ArbConfig, clob_client: ClobClient) -> None:
        self.db = db
        self.config = config
        self.clob_client = clob_client
        self._live_client: Any = None

    async def execute(self, plan: ExecutionPlan, snapshots: dict[str, TokenSnapshot]) -> TradeResult:
        signal = plan.signal
        starts_at = datetime.now(tz=UTC)
        await self._record_intents(plan)

        if signal.mode in {RunMode.SHADOW, RunMode.PAPER_LIVE, RunMode.PAPER_REPLAY}:
            result = await self._execute_paper(plan, snapshots, persist=True)
        else:
            result = await self._execute_live(plan, snapshots)

        await self._record_trade_result(result, starts_at)
        return result

    async def simulate_paper(self, plan: ExecutionPlan, snapshots: dict[str, TokenSnapshot]) -> TradeResult:
        return await self._execute_paper(plan, snapshots, persist=False)

    async def _execute_paper(
        self,
        plan: ExecutionPlan,
        snapshots: dict[str, TokenSnapshot],
        persist: bool,
    ) -> TradeResult:
        fills: list[FillEvent] = []
        total_notional = 0.0
        event_rows: list[tuple[str, str, int, str, str]] = []
        fill_rows: list[tuple[str, str, str, str, float, float, float, float]] = []
        for intent in plan.intents:
            snap = snapshots.get(intent.token_id)
            if snap is None:
                if persist:
                    event_rows.append(
                        (
                            str(intent.intent_id),
                            "reject",
                            404,
                            "missing_snapshot",
                            json.dumps({}, ensure_ascii=True),
                        )
                    )
                continue
            fill = _simulate_intent_fill(intent.limit_price, intent.shares, intent.notional_usd, intent.side, snap)
            if fill is None:
                if persist:
                    event_rows.append(
                        (
                            str(intent.intent_id),
                            "reject",
                            422,
                            "insufficient_liquidity",
                            json.dumps({}, ensure_ascii=True),
                        )
                    )
                continue
            fills.append(fill)
            total_notional += fill.fill_notional_usd
            if persist:
                event_rows.append(
                    (
                        str(intent.intent_id),
                        "fill",
                        200,
                        "paper_fill",
                        json.dumps(
                            {
                                "price": fill.fill_price,
                                "size": fill.fill_size,
                                "notional": fill.fill_notional_usd,
                            },
                            ensure_ascii=True,
                        ),
                    )
                )
                fill_rows.append(
                    (
                        str(intent.intent_id),
                        fill.market_id,
                        fill.token_id,
                        fill.side,
                        fill.fill_price,
                        fill.fill_size,
                        fill.fill_notional_usd,
                        fill.fee_usd,
                    )
                )
        if persist:
            await self._record_order_events(event_rows)
            await self._record_fills(fill_rows)

        mark_to_book, fees, slip, expected_gross = _estimate_trade_pnl(
            plan.signal.features,
            fills,
            snapshots,
            self.config.fee_bps,
        )
        hold_minutes = float(plan.signal.features.get("expected_hold_minutes", 0) or 0)
        if hold_minutes > 1:
            # 持有型策略在开仓时不确认浮盈亏，避免把未实现波动误记成已实现盈亏。
            gross = 0.0
            pnl_model = "entry_only"
        else:
            gross = mark_to_book
            pnl_model = "mark_to_book"
        return TradeResult(
            signal=plan.signal,
            status="filled" if fills else "rejected",
            gross_pnl_usd=gross,
            fees_usd=fees,
            slippage_usd=slip,
            net_pnl_usd=gross - fees - slip,
            capital_used_usd=total_notional,
            hold_minutes=hold_minutes,
            fills=fills,
            metadata={
                "mode": plan.signal.mode.value,
                "intent_count": len(plan.intents),
                "pnl_model": pnl_model,
                "expected_gross_pnl_usd": expected_gross,
                "mark_to_book_gross_pnl_usd": mark_to_book,
                "entry_gross_pnl_usd": gross,
            },
        )

    async def _execute_live(self, plan: ExecutionPlan, snapshots: dict[str, TokenSnapshot]) -> TradeResult:
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType, PostOrdersArgs
        except Exception as exc:  # pragma: no cover - runtime dependency path
            event_rows = [
                (
                    str(intent.intent_id),
                    "error",
                    500,
                    "py_clob_client_missing",
                    json.dumps({"error": str(exc)}, ensure_ascii=True),
                )
                for intent in plan.intents
            ]
            await self._record_order_events(event_rows)
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

        client = self._get_live_client()
        if client is None:
            event_rows = [
                (
                    str(intent.intent_id),
                    "error",
                    500,
                    "missing_live_key",
                    json.dumps({}, ensure_ascii=True),
                )
                for intent in plan.intents
            ]
            await self._record_order_events(event_rows)
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

        refresh_preflight = self._should_refresh_preflight(plan.intents, snapshots)
        live_books: dict[str, TokenSnapshot] = {}
        if refresh_preflight:
            live_books = await self._load_preflight_books(plan.intents)
        fills: list[FillEvent] = []
        total_notional = 0.0
        event_rows: list[tuple[str, str, int, str, str]] = []
        fill_rows: list[tuple[str, str, str, str, float, float, float, float]] = []

        submit_payload: list[dict[str, Any]] = []
        for intent in plan.intents:
            baseline = snapshots.get(intent.token_id)
            if baseline is None:
                event_rows.append(
                    (
                        str(intent.intent_id),
                        "reject",
                        404,
                        "missing_snapshot",
                        json.dumps({}, ensure_ascii=True),
                    )
                )
                continue

            live_snap = live_books.get(intent.token_id, baseline)
            shares = _resolve_shares(intent.limit_price, intent.shares, intent.notional_usd, intent.side, live_snap)
            if shares <= 0:
                event_rows.append(
                    (
                        str(intent.intent_id),
                        "reject",
                        422,
                        "invalid_size",
                        json.dumps({}, ensure_ascii=True),
                    )
                )
                continue
            price = _resolve_price(intent.limit_price, intent.side, live_snap)
            if price <= 0:
                event_rows.append(
                    (
                        str(intent.intent_id),
                        "reject",
                        422,
                        "invalid_price",
                        json.dumps({}, ensure_ascii=True),
                    )
                )
                continue
            if not _preflight_price_ok(intent.side, price, live_snap, self.config.slippage_bps):
                event_rows.append(
                    (
                        str(intent.intent_id),
                        "reject",
                        409,
                        "preflight_price_drift",
                        json.dumps(
                            {
                                "limit_price": price,
                                "best_bid": live_snap.best_bid,
                                "best_ask": live_snap.best_ask,
                                "slippage_bps": self.config.slippage_bps,
                            },
                            ensure_ascii=True,
                        ),
                    )
                )
                continue
            normalized = _normalize_order_params(price, shares, intent.side, live_snap)
            if normalized is None:
                event_rows.append(
                    (
                        str(intent.intent_id),
                        "reject",
                        422,
                        "below_min_order_size",
                        json.dumps({}, ensure_ascii=True),
                    )
                )
                continue
            norm_price, norm_shares = normalized
            submit_payload.append(
                {
                    "intent": intent,
                    "price": norm_price,
                    "shares": norm_shares,
                    "market_id": live_snap.market_id,
                }
            )

        if not submit_payload:
            await self._record_order_events(event_rows)
            return TradeResult(
                signal=plan.signal,
                status="rejected",
                gross_pnl_usd=0.0,
                fees_usd=0.0,
                slippage_usd=0.0,
                net_pnl_usd=0.0,
                capital_used_usd=0.0,
                hold_minutes=None,
                fills=[],
                metadata={"mode": plan.signal.mode.value, "intent_count": len(plan.intents), "submitted": 0},
            )

        order_type = _parse_order_type(self.config.live_order_type, OrderType)
        chunks = [submit_payload[i : i + 15] for i in range(0, len(submit_payload), 15)]
        submitted = 0
        rejected = 0
        for chunk in chunks:
            try:
                rows = await asyncio.to_thread(_submit_live_orders, client, chunk, order_type, OrderArgs, PostOrdersArgs)
            except Exception as exc:  # pragma: no cover - network/runtime path
                for item in chunk:
                    event_rows.append(
                        (
                            str(item["intent"].intent_id),
                            "error",
                            500,
                            "live_submit_error",
                            json.dumps({"error": str(exc)}, ensure_ascii=True),
                        )
                    )
                continue

            for item, row in zip(chunk, rows, strict=False):
                intent = item["intent"]
                payload = row if isinstance(row, dict) else {"response": row}
                success = bool(payload.get("success", False))
                status = str(payload.get("status", "")).lower()
                code = 200 if success else 422
                event = "submit" if success else "reject"
                message = payload.get("errorMsg") or status or ("live_submit" if success else "live_reject")
                event_rows.append(
                    (
                        str(intent.intent_id),
                        event,
                        code,
                        message,
                        json.dumps(payload, ensure_ascii=True),
                    )
                )
                if not success:
                    rejected += 1
                    continue
                submitted += 1
                if status in {"matched", "filled"}:
                    fill = FillEvent(
                        token_id=intent.token_id,
                        market_id=str(item["market_id"]),
                        side=intent.side,
                        fill_price=float(item["price"]),
                        fill_size=float(item["shares"]),
                        fill_notional_usd=float(item["price"]) * float(item["shares"]),
                        fee_usd=0.0,
                    )
                    fills.append(fill)
                    total_notional += fill.fill_notional_usd
                    fill_rows.append(
                        (
                            str(intent.intent_id),
                            fill.market_id,
                            fill.token_id,
                            fill.side,
                            fill.fill_price,
                            fill.fill_size,
                            fill.fill_notional_usd,
                            fill.fee_usd,
                        )
                    )
                elif status in {"live", "delayed"}:
                    logger.info("live order accepted but not immediately matched", extra={"intent_id": str(intent.intent_id), "status": status})
                else:
                    rejected += 1

        await self._record_order_events(event_rows)
        await self._record_fills(fill_rows)

        snapshot_for_pnl = live_books if live_books else snapshots
        gross, fees, slip, expected_gross = _estimate_trade_pnl(
            plan.signal.features,
            fills,
            snapshot_for_pnl,
            self.config.fee_bps,
        )
        return TradeResult(
            signal=plan.signal,
            status="filled" if fills else ("submitted" if submitted > 0 else "error"),
            gross_pnl_usd=gross,
            fees_usd=fees,
            slippage_usd=slip,
            net_pnl_usd=gross - fees - slip,
            capital_used_usd=total_notional,
            hold_minutes=float(plan.signal.features.get("expected_hold_minutes", 0) or 0),
            fills=fills,
            metadata={
                "mode": plan.signal.mode.value,
                "intent_count": len(plan.intents),
                "submitted": submitted,
                "rejected": rejected,
                "live_order_type": self.config.live_order_type,
                "preflight_refresh": refresh_preflight,
                "pnl_model": "mark_to_book",
                "expected_gross_pnl_usd": expected_gross,
                "mark_to_book_gross_pnl_usd": gross,
            },
        )

    async def _load_preflight_books(self, intents: list[Any]) -> dict[str, TokenSnapshot]:
        token_ids = sorted({intent.token_id for intent in intents if intent.token_id})
        books = await self.clob_client.get_books(token_ids)
        snapshots: dict[str, TokenSnapshot] = {}
        now = datetime.now(tz=UTC)
        for book in books:
            best_bid, best_ask = self.clob_client.best_bid_ask(book)
            snapshots[book.asset_id] = TokenSnapshot(
                token_id=book.asset_id,
                market_id=book.market or "",
                event_id=None,
                market_question="",
                market_end=None,
                outcome_label="",
                outcome_side="",
                outcome_index=0,
                min_order_size=float(book.min_order_size) if book.min_order_size else None,
                tick_size=float(book.tick_size) if book.tick_size else None,
                best_bid=best_bid,
                best_ask=best_ask,
                captured_at=now,
            )
        return snapshots

    def _should_refresh_preflight(
        self,
        intents: list[Any],
        snapshots: dict[str, TokenSnapshot],
    ) -> bool:
        if self.config.live_preflight_force_refresh:
            return True
        max_age_ms = max(0, self.config.live_preflight_max_age_ms)
        if max_age_ms == 0:
            return False
        now = datetime.now(tz=UTC)
        for intent in intents:
            snap = snapshots.get(intent.token_id)
            if snap is None:
                return True
            age_ms = (now - snap.captured_at).total_seconds() * 1000
            if age_ms > max_age_ms:
                return True
        return False

    def _get_live_client(self) -> Any | None:
        if self._live_client is not None:
            return self._live_client

        key = os.getenv("POLARIS_ARB_LIVE_PRIVATE_KEY")
        if not key:
            return None

        from py_clob_client.client import ClobClient as LiveClient  # pragma: no cover - runtime dependency path

        host = os.getenv("POLARIS_ARB_LIVE_HOST", "https://clob.polymarket.com")
        chain_id = int(os.getenv("POLARIS_ARB_LIVE_CHAIN_ID", "137"))
        client = LiveClient(host=host, key=key, chain_id=chain_id)
        client.set_api_creds(client.create_or_derive_api_creds())
        self._live_client = client
        return client

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
        await self._record_order_events(
            [
                (
                    str(intent_id),
                    event_type,
                    status_code,
                    message,
                    json.dumps(payload, ensure_ascii=True),
                )
            ]
        )

    async def _record_fill(self, intent_id: Any, fill: FillEvent) -> None:
        await self._record_fills(
            [
                (
                    str(intent_id),
                    fill.market_id,
                    fill.token_id,
                    fill.side,
                    fill.fill_price,
                    fill.fill_size,
                    fill.fill_notional_usd,
                    fill.fee_usd,
                )
            ]
        )

    async def _record_order_events(self, rows: list[tuple[str, str, int, str, str]]) -> None:
        if not rows:
            return
        await self.db.executemany(
            """
            insert into arb_order_event(intent_id, event_type, status_code, message, payload, created_at)
            values (%s, %s, %s, %s, %s::jsonb, now())
            """,
            rows,
        )

    async def _record_fills(self, rows: list[tuple[str, str, str, str, float, float, float, float]]) -> None:
        if not rows:
            return
        await self.db.executemany(
            """
            insert into arb_fill(
                intent_id, market_id, token_id, side, fill_price, fill_size, fill_notional_usd, fee_usd, created_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, now())
            """,
            rows,
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
        return max(shares, float(snap.min_order_size or 0.0))
    if notional_usd is None or notional_usd <= 0:
        return 0.0
    base_price = _resolve_price(limit_price, side, snap)
    if base_price <= 0:
        return 0.0
    computed = notional_usd / base_price
    return max(computed, float(snap.min_order_size or 0.0))


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


def _estimate_trade_pnl(
    features: dict[str, Any],
    fills: list[FillEvent],
    snapshots: dict[str, TokenSnapshot],
    fee_bps: int,
) -> tuple[float, float, float, float]:
    if not fills:
        return 0.0, 0.0, 0.0, 0.0
    expected_edge = float(features.get("expected_edge_pct") or 0.0)
    notional = sum(f.fill_notional_usd for f in fills)
    expected_gross = notional * expected_edge
    mark_to_book = 0.0
    for fill in fills:
        snap = snapshots.get(fill.token_id)
        if snap is None:
            continue
        if fill.side == "BUY":
            unwind = simulate_sell_fill(snap.bids, fill.fill_size)
            unwind_notional = unwind.notional if unwind is not None else 0.0
            mark_to_book += unwind_notional - fill.fill_notional_usd
        else:
            unwind = simulate_buy_fill(snap.asks, fill.fill_size)
            unwind_notional = unwind.notional if unwind is not None else fill.fill_notional_usd
            mark_to_book += fill.fill_notional_usd - unwind_notional
    gross = mark_to_book
    fees = notional * max(0, fee_bps) / 10_000.0
    slippage = float(features.get("expected_slippage_usd") or 0.0)
    return gross, fees, slippage, expected_gross


def _normalize_order_params(price: float, shares: float, side: str, snap: TokenSnapshot) -> tuple[float, float] | None:
    if shares <= 0 or price <= 0:
        return None
    min_size = float(snap.min_order_size or 0.0)
    shares = max(shares, min_size)
    tick = float(snap.tick_size or 0.0)
    if tick > 0:
        ratio = price / tick
        steps = ceil(ratio) if side.upper() == "BUY" else floor(ratio)
        price = max(tick, steps * tick)
    return price, shares


def _preflight_price_ok(side: str, intended_price: float, live: TokenSnapshot, slippage_bps: int) -> bool:
    side_upper = side.upper()
    ratio = slippage_bps / 10_000.0
    if side_upper == "BUY":
        if live.best_ask is None:
            return False
        worst_allowed = intended_price * (1.0 + ratio)
        return float(live.best_ask) <= worst_allowed
    if live.best_bid is None:
        return False
    worst_allowed = intended_price * (1.0 - ratio)
    return float(live.best_bid) >= worst_allowed


def _parse_order_type(name: str, order_type_cls: Any) -> Any:
    normalized = (name or "FAK").strip().upper()
    if hasattr(order_type_cls, normalized):
        return getattr(order_type_cls, normalized)
    return getattr(order_type_cls, "FAK")


def _submit_live_orders(
    client: Any,
    chunk: list[dict[str, Any]],
    order_type: Any,
    order_args_cls: Any,
    post_orders_args_cls: Any,
) -> list[dict[str, Any]]:
    args = []
    for item in chunk:
        intent = item["intent"]
        order_args = order_args_cls(
            token_id=intent.token_id,
            side=intent.side,
            price=float(item["price"]),
            size=float(item["shares"]),
        )
        signed = client.create_order(order_args)
        args.append(post_orders_args_cls(order=signed, orderType=order_type))
    result = client.post_orders(args)
    if isinstance(result, list):
        return [row if isinstance(row, dict) else {"response": row} for row in result]
    if isinstance(result, dict):
        return [result]
    return [{"response": result}]
