from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from polaris.arb.ai.gate import AiGate
from polaris.arb.config import ArbConfig
from polaris.arb.contracts import (
    ArbSignal,
    ExecutionPlan,
    OrderIntent,
    PriceLevel,
    RunMode,
    SignalStatus,
    StrategyCode,
    TokenSnapshot,
)
from polaris.arb.execution.order_router import OrderRouter
from polaris.arb.execution.risk_gate import RiskGate
from polaris.arb.execution.sizer import capped_notional
from polaris.arb.strategies.strategy_a import StrategyA
from polaris.arb.strategies.strategy_b import StrategyB
from polaris.arb.strategies.strategy_c import StrategyC
from polaris.arb.strategies.strategy_f import StrategyF
from polaris.arb.strategies.strategy_g import StrategyG
from polaris.db.pool import Database
from polaris.sources.clob_client import ClobClient

logger = logging.getLogger(__name__)


class ArbOrchestrator:
    def __init__(
        self,
        db: Database,
        clob_client: ClobClient,
        config: ArbConfig,
        ai_gate: AiGate,
    ) -> None:
        self.db = db
        self.clob_client = clob_client
        self.config = config
        self.ai_gate = ai_gate
        self.order_router = OrderRouter(db, config, clob_client)
        self.risk_gate = RiskGate(db, config)
        self._last_optimize_at: datetime | None = None
        self._missing_token_resume_at: dict[str, datetime] = {}

        self.strategy_a = StrategyA(config)
        self.strategy_b = StrategyB(config)
        self.strategy_c = StrategyC(config)
        self.strategy_f = StrategyF(config)
        self.strategy_g = StrategyG(config)

    async def run_once(self, mode: RunMode, source_code: str = "polymarket") -> dict[str, int]:
        snapshots = await self._load_live_snapshots()
        if not snapshots:
            return {"signals": 0, "executed": 0}

        all_signals = self._scan_all(mode, source_code, snapshots)
        ordered = self._order_signals(all_signals)
        executed = 0
        for signal in ordered:
            if signal.is_expired():
                await self._record_signal(signal, SignalStatus.EXPIRED)
                continue

            if signal.strategy_code == StrategyCode.G:
                ai_decision = await self.ai_gate.evaluate(signal, context={"token_count": len(snapshots)})
                if not ai_decision.allow:
                    await self._record_signal(signal, SignalStatus.REJECTED, note=f"ai_reject:{ai_decision.reason}")
                    continue

            notional = _estimate_capital(signal)
            decision = await self.risk_gate.assess(signal, notional)
            if not decision.allowed:
                await self._record_signal(signal, SignalStatus.REJECTED, note=decision.reason)
                await self.risk_gate.record_risk_event(signal.mode, signal.strategy_code, signal.source_code, decision)
                continue

            plan = self._build_plan(signal)
            await self._record_signal(signal, SignalStatus.NEW)
            result = await self.order_router.execute(plan, {item.token_id: item for item in snapshots})
            success = result.status == "filled"
            self.risk_gate.note_execution_outcome(success)
            if success:
                executed += 1
                await self._record_cash_ledger(result)
                await self._record_position_lots(result)
                await self._mark_signal_status(signal.signal_id, SignalStatus.EXECUTED)
            else:
                await self._mark_signal_status(signal.signal_id, SignalStatus.REJECTED)

        if mode == RunMode.PAPER_LIVE:
            await self._maybe_run_optimization()

        return {"signals": len(all_signals), "executed": executed}

    async def run_forever(self, mode: RunMode, source_code: str = "polymarket") -> None:
        while True:
            started = datetime.now(tz=UTC)
            try:
                stats = await self.run_once(mode, source_code)
                logger.info("arb loop finished", extra={"mode": mode.value, "stats": stats})
            except Exception:
                logger.exception("arb loop failed", extra={"mode": mode.value})
            elapsed = (datetime.now(tz=UTC) - started).total_seconds()
            sleep_sec = max(1.0, self.config.scan_interval_sec - elapsed)
            await _sleep(sleep_sec)

    async def run_replay(self, window_start: datetime, window_end: datetime, source_code: str = "polymarket") -> dict[str, float]:
        replay_run_id = str(uuid4())
        await self.db.execute(
            """
            insert into arb_replay_run(
                replay_run_id, mode, status, window_start, window_end, metadata, started_at
            ) values (%s, 'paper_replay', 'running', %s, %s, '{}'::jsonb, now())
            """,
            (replay_run_id, window_start, window_end),
        )

        per_strategy: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        buckets = await self._load_replay_buckets(window_start, window_end)
        for bucket_rows in buckets.values():
            snapshots = list(bucket_rows)
            signals = self._scan_all(RunMode.PAPER_REPLAY, source_code, snapshots)
            for signal in signals:
                await self._record_signal(signal, SignalStatus.NEW)
                plan = self._build_plan(signal)
                result = await self.order_router.execute(plan, {item.token_id: item for item in snapshots})
                strategy = signal.strategy_code.value
                per_strategy[strategy]["signals"] += 1
                if result.status == "filled":
                    per_strategy[strategy]["trades"] += 1
                    per_strategy[strategy]["gross"] += result.gross_pnl_usd
                    per_strategy[strategy]["net"] += result.net_pnl_usd
                    per_strategy[strategy]["turnover"] += result.capital_used_usd
                    if result.net_pnl_usd >= 0:
                        per_strategy[strategy]["wins"] += 1
                    else:
                        per_strategy[strategy]["losses"] += 1

        for strategy, metric in per_strategy.items():
            await self.db.execute(
                """
                insert into arb_replay_metric(
                    replay_run_id, strategy_code, signals, trades, wins, losses,
                    gross_pnl_usd, net_pnl_usd, max_drawdown_usd, turnover_usd, created_at
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, 0, %s, now())
                """,
                (
                    replay_run_id,
                    strategy,
                    int(metric.get("signals", 0)),
                    int(metric.get("trades", 0)),
                    int(metric.get("wins", 0)),
                    int(metric.get("losses", 0)),
                    float(metric.get("gross", 0.0)),
                    float(metric.get("net", 0.0)),
                    float(metric.get("turnover", 0.0)),
                ),
            )

        await self.db.execute(
            """
            update arb_replay_run
            set status = 'done',
                sample_count = %s,
                finished_at = now()
            where replay_run_id = %s
            """,
            (sum(len(items) for items in buckets.values()), replay_run_id),
        )
        total_net = sum(metric.get("net", 0.0) for metric in per_strategy.values())
        return {"strategies": float(len(per_strategy)), "net_pnl": total_net}

    async def _load_live_snapshots(self) -> list[TokenSnapshot]:
        rows = await self.db.fetch_all(
            """
            select
                t.token_id,
                t.market_id,
                t.outcome_label,
                t.outcome_side,
                t.outcome_index,
                t.min_order_size,
                t.tick_size,
                t.is_other_outcome,
                t.is_placeholder_outcome,
                m.event_id,
                m.question,
                m.end_date
            from dim_token t
            join dim_market m on m.market_id = t.market_id
            where m.active = true and m.closed = false and m.archived = false
            """
        )
        if not rows:
            return []

        now = datetime.now(tz=UTC)
        token_ids = []
        for row in rows:
            token_id = str(row["token_id"])
            resume_at = self._missing_token_resume_at.get(token_id)
            if resume_at and now < resume_at:
                continue
            token_ids.append(token_id)
        if not token_ids:
            return []

        books = await self.clob_client.get_books(token_ids)
        by_token = {book.asset_id: book for book in books if book.asset_id}
        returned = set(by_token.keys())
        for token_id in token_ids:
            if token_id in returned:
                self._missing_token_resume_at.pop(token_id, None)
                continue
            self._missing_token_resume_at[token_id] = now + timedelta(minutes=5)

        snapshots: list[TokenSnapshot] = []
        for row in rows:
            book = by_token.get(row["token_id"])
            if not book:
                continue
            best_bid, best_ask = self.clob_client.best_bid_ask(book)
            snapshots.append(
                TokenSnapshot(
                    token_id=row["token_id"],
                    market_id=row["market_id"],
                    event_id=row["event_id"],
                    market_question=row["question"],
                    market_end=row["end_date"],
                    outcome_label=row["outcome_label"],
                    outcome_side=row["outcome_side"],
                    outcome_index=int(row["outcome_index"]),
                    min_order_size=float(row["min_order_size"]) if row["min_order_size"] is not None else None,
                    tick_size=float(row["tick_size"]) if row["tick_size"] is not None else None,
                    best_bid=best_bid,
                    best_ask=best_ask,
                    is_other_outcome=bool(row["is_other_outcome"]),
                    is_placeholder_outcome=bool(row["is_placeholder_outcome"]),
                    bids=tuple(PriceLevel(price=float(level.price), size=float(level.size)) for level in book.bids),
                    asks=tuple(PriceLevel(price=float(level.price), size=float(level.size)) for level in book.asks),
                    captured_at=datetime.now(tz=UTC),
                )
            )
        return snapshots

    async def _load_replay_buckets(self, window_start: datetime, window_end: datetime) -> dict[datetime, list[TokenSnapshot]]:
        rows = await self.db.fetch_all(
            """
            with top as (
                select
                    date_trunc('minute', q.captured_at) as bucket_minute,
                    q.token_id,
                    avg(q.best_bid) as best_bid,
                    avg(q.best_ask) as best_ask
                from fact_quote_top_raw q
                where q.captured_at >= %s and q.captured_at <= %s
                group by 1, 2
            ), depth as (
                select
                    date_trunc('minute', d.captured_at) as bucket_minute,
                    d.token_id,
                    avg(d.bid_depth_1pct) as bid_depth,
                    avg(d.ask_depth_1pct) as ask_depth
                from fact_quote_depth_raw d
                where d.captured_at >= %s and d.captured_at <= %s
                group by 1, 2
            )
            select
                top.bucket_minute,
                t.token_id,
                t.market_id,
                t.outcome_label,
                t.outcome_side,
                t.outcome_index,
                t.min_order_size,
                t.tick_size,
                t.is_other_outcome,
                t.is_placeholder_outcome,
                m.event_id,
                m.question,
                m.end_date,
                top.best_bid,
                top.best_ask,
                coalesce(depth.bid_depth, 10) as bid_depth,
                coalesce(depth.ask_depth, 10) as ask_depth
            from top
            join dim_token t on t.token_id = top.token_id
            join dim_market m on m.market_id = t.market_id
            left join depth on depth.bucket_minute = top.bucket_minute and depth.token_id = top.token_id
            order by top.bucket_minute asc
            """,
            (window_start, window_end, window_start, window_end),
        )
        grouped: dict[datetime, list[TokenSnapshot]] = defaultdict(list)
        for row in rows:
            ask_price = float(row["best_ask"]) if row["best_ask"] is not None else None
            bid_price = float(row["best_bid"]) if row["best_bid"] is not None else None
            if ask_price is None or bid_price is None:
                continue
            grouped[row["bucket_minute"]].append(
                TokenSnapshot(
                    token_id=row["token_id"],
                    market_id=row["market_id"],
                    event_id=row["event_id"],
                    market_question=row["question"],
                    market_end=row["end_date"],
                    outcome_label=row["outcome_label"],
                    outcome_side=row["outcome_side"],
                    outcome_index=int(row["outcome_index"]),
                    min_order_size=float(row["min_order_size"]) if row["min_order_size"] is not None else None,
                    tick_size=float(row["tick_size"]) if row["tick_size"] is not None else None,
                    best_bid=bid_price,
                    best_ask=ask_price,
                    is_other_outcome=bool(row["is_other_outcome"]),
                    is_placeholder_outcome=bool(row["is_placeholder_outcome"]),
                    bids=(PriceLevel(price=bid_price, size=float(row["bid_depth"])),),
                    asks=(PriceLevel(price=ask_price, size=float(row["ask_depth"])),),
                    captured_at=row["bucket_minute"],
                )
            )
        return grouped

    def _scan_all(self, mode: RunMode, source_code: str, snapshots: list[TokenSnapshot]) -> list[ArbSignal]:
        signals: list[ArbSignal] = []
        if self.config.enable_strategy_a:
            signals.extend(self.strategy_a.scan(mode, source_code, snapshots))
        if self.config.enable_strategy_b:
            signals.extend(self.strategy_b.scan(mode, source_code, snapshots))
        if self.config.enable_strategy_f:
            signals.extend(self.strategy_f.scan(mode, source_code, snapshots))
        if self.config.enable_strategy_g:
            signals.extend(self.strategy_g.scan(mode, source_code, snapshots))
        if self.config.enable_strategy_c:
            signals.extend(self.strategy_c.scan(mode, source_code, snapshots))
        return signals

    def _order_signals(self, signals: list[ArbSignal]) -> list[ArbSignal]:
        priority = {name: idx for idx, name in enumerate(self.config.strategy_priority)}
        return sorted(
            signals,
            key=lambda signal: _signal_rank_key(signal, priority),
        )

    def _build_plan(self, signal: ArbSignal) -> ExecutionPlan:
        legs = signal.features.get("legs", [])
        intents: list[OrderIntent] = []
        for idx, leg in enumerate(legs):
            notional = leg.get("notional_usd")
            shares = leg.get("shares")
            if notional is None:
                notional = capped_notional(signal.edge_pct, self.config.min_order_notional_usd, self.config.single_risk_usd)
            intents.append(
                OrderIntent(
                    intent_id=uuid4(),
                    signal_id=signal.signal_id,
                    mode=signal.mode,
                    strategy_code=signal.strategy_code,
                    source_code=signal.source_code,
                    order_index=idx,
                    market_id=str(leg["market_id"]),
                    token_id=str(leg["token_id"]),
                    side=str(leg.get("side", "BUY")).upper(),
                    order_type="IOC" if signal.mode == RunMode.LIVE else "PAPER",
                    limit_price=float(leg.get("price")) if leg.get("price") is not None else None,
                    shares=float(shares) if shares is not None else None,
                    notional_usd=float(notional) if notional is not None else None,
                    payload={"signal_edge_pct": signal.edge_pct},
                )
            )
        return ExecutionPlan(signal=signal, intents=intents)

    async def _record_signal(self, signal: ArbSignal, status: SignalStatus, note: str | None = None) -> None:
        await self.db.execute(
            """
            insert into arb_signal(
                signal_id, mode, strategy_code, source_code, event_id, market_ids, token_ids,
                edge_pct, expected_pnl_usd, ttl_ms, features, status, decision_note, created_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
            on conflict (signal_id) do update
            set status = excluded.status,
                decision_note = excluded.decision_note
            """,
            (
                str(signal.signal_id),
                signal.mode.value,
                signal.strategy_code.value,
                signal.source_code,
                signal.event_id,
                signal.market_ids,
                signal.token_ids,
                signal.edge_pct,
                signal.expected_pnl_usd,
                signal.ttl_ms,
                json.dumps(signal.features, ensure_ascii=True),
                status.value,
                note or signal.decision_note,
                signal.created_at,
            ),
        )

    async def _mark_signal_status(self, signal_id, status: SignalStatus) -> None:
        await self.db.execute("update arb_signal set status = %s where signal_id = %s", (status.value, str(signal_id)))

    async def _record_cash_ledger(self, result) -> None:
        signal = result.signal
        row = await self.db.fetch_one(
            """
            select balance_after_usd
            from arb_cash_ledger
            where mode = %s and source_code = %s
            order by created_at desc
            limit 1
            """,
            (signal.mode.value, signal.source_code),
        )
        before = float(row["balance_after_usd"]) if row else 0.0
        after = before + result.net_pnl_usd
        await self.db.execute(
            """
            insert into arb_cash_ledger(
                mode, strategy_code, source_code, entry_type, amount_usd,
                balance_before_usd, balance_after_usd, ref_signal_id, payload, created_at
            ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, now())
            """,
            (
                signal.mode.value,
                signal.strategy_code.value,
                signal.source_code,
                "trade_net_pnl",
                result.net_pnl_usd,
                before,
                after,
                str(signal.signal_id),
                json.dumps({"status": result.status}, ensure_ascii=True),
            ),
        )

    async def _record_position_lots(self, result) -> None:
        signal = result.signal
        for fill in result.fills:
            await self.db.execute(
                """
                insert into arb_position_lot(
                    mode, strategy_code, source_code, market_id, token_id, side,
                    open_price, open_size, open_notional_usd, remaining_size,
                    status, opened_at, closed_at, close_price, realized_pnl_usd
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, 0, 'closed', now(), now(), %s, %s)
                """,
                (
                    signal.mode.value,
                    signal.strategy_code.value,
                    signal.source_code,
                    fill.market_id,
                    fill.token_id,
                    fill.side,
                    fill.fill_price,
                    fill.fill_size,
                    fill.fill_notional_usd,
                    fill.fill_price,
                    result.net_pnl_usd,
                ),
            )

    async def _maybe_run_optimization(self) -> None:
        now = datetime.now(tz=UTC)
        if self._last_optimize_at and (now - self._last_optimize_at).total_seconds() < self.config.optimize_interval_sec:
            return
        await self.optimize_parameters()
        self._last_optimize_at = now

    async def optimize_parameters(self) -> None:
        # 候选参数基于当前阈值做小幅扰动，评分来源：最近回放 + 最近实时 paper。
        base = {
            "a_min_edge_pct": self.config.a_min_edge_pct,
            "b_min_edge_pct": self.config.b_min_edge_pct,
            "f_min_prob": self.config.f_min_prob,
            "g_min_confidence": self.config.g_min_confidence,
            "g_min_expected_edge_pct": self.config.g_min_expected_edge_pct,
        }
        candidates = [
            base,
            {**base, "a_min_edge_pct": round(base["a_min_edge_pct"] * 0.9, 6)},
            {**base, "g_min_expected_edge_pct": round(base["g_min_expected_edge_pct"] * 1.1, 6)},
        ]

        paper = await self.db.fetch_one(
            """
            select
                coalesce(sum(net_pnl_usd),0) as pnl,
                coalesce(sum(case when net_pnl_usd < 0 then abs(net_pnl_usd) else 0 end),0) as losses,
                count(*) as trades
            from arb_trade_result
            where mode = 'paper_live'
              and created_at >= now() - (%s || ' hours')::interval
            """,
            (self.config.optimize_paper_hours,),
        )
        replay = await self.db.fetch_one(
            """
            select
                coalesce(sum(net_pnl_usd),0) as pnl,
                coalesce(sum(max_drawdown_usd),0) as drawdown,
                coalesce(sum(trades),0) as trades
            from arb_replay_metric
            where replay_run_id in (
                select replay_run_id
                from arb_replay_run
                where started_at >= now() - (%s || ' days')::interval
            )
            """,
            (self.config.optimize_replay_days,),
        )
        paper_pnl = float(paper["pnl"]) if paper else 0.0
        paper_loss = float(paper["losses"]) if paper else 0.0
        replay_pnl = float(replay["pnl"]) if replay else 0.0
        replay_drawdown = float(replay["drawdown"]) if replay else 0.0

        baseline_score = paper_pnl + replay_pnl - paper_loss - replay_drawdown
        best_version = None
        best_score = baseline_score
        for idx, params in enumerate(candidates):
            penalty = abs(params["a_min_edge_pct"] - base["a_min_edge_pct"]) * 10
            score = baseline_score - penalty
            version = f"m2-{datetime.now(tz=UTC).strftime('%Y%m%d')}-{idx}"
            status = "candidate"
            if score >= best_score:
                best_score = score
                best_version = version
            await self.db.execute(
                """
                insert into arb_param_snapshot(
                    strategy_scope, version, status, params, score_total, score_breakdown,
                    source_paper_window_start, source_paper_window_end, created_at
                )
                values (%s, %s, %s, %s::jsonb, %s, %s::jsonb, now() - (%s || ' hours')::interval, now(), now())
                on conflict (version) do nothing
                """,
                (
                    "module2",
                    version,
                    status,
                    json.dumps(params, ensure_ascii=True),
                    score,
                    json.dumps(
                        {
                            "paper_pnl": paper_pnl,
                            "paper_loss": paper_loss,
                            "replay_pnl": replay_pnl,
                            "replay_drawdown": replay_drawdown,
                        },
                        ensure_ascii=True,
                    ),
                    self.config.optimize_paper_hours,
                ),
            )

        if best_version:
            await self.db.execute("update arb_param_snapshot set status='retired', deactivated_at=now() where status='active'")
            await self.db.execute(
                "update arb_param_snapshot set status='active', activated_at=now() where version = %s",
                (best_version,),
            )



def _estimate_capital(signal: ArbSignal) -> float:
    total = 0.0
    for leg in signal.features.get("legs", []):
        if leg.get("notional_usd") is not None:
            total += float(leg["notional_usd"])
            continue
        price = float(leg.get("price", 0.0) or 0.0)
        shares = float(leg.get("shares", 0.0) or 0.0)
        total += price * shares
    return total


def _signal_rank_key(signal: ArbSignal, priority: dict[str, int]) -> tuple[float, float, float, float]:
    capital = max(_estimate_capital(signal), 1e-6)
    hold_minutes = float(signal.features.get("expected_hold_minutes") or 60.0)
    roi = signal.expected_pnl_usd / capital
    turnover = signal.expected_pnl_usd / max(hold_minutes, 1.0)
    return (
        float(priority.get(signal.strategy_code.value, 999)),
        -roi,
        -turnover,
        -signal.edge_pct,
    )


async def _sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)
