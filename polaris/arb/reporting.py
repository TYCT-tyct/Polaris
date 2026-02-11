from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from polaris.db.pool import Database
from polaris.ops.exporter import ExportFormat, export_table


class ArbReporter:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def report(self, group_by: str = "strategy,mode,source", run_tag: str | None = None) -> list[dict]:
        groups = [part.strip().lower() for part in group_by.split(",") if part.strip()]
        group_cols: list[str] = []
        mapping = {
            "strategy": "strategy_code",
            "mode": "mode",
            "source": "source_code",
            "day": "date_trunc('day', created_at)",
        }
        for item in groups:
            column = mapping.get(item)
            if column:
                group_cols.append(column)
        if not group_cols:
            group_cols = ["strategy_code", "mode", "source_code"]
        select_cols = ", ".join(group_cols)
        sql = f"""
            select
                {select_cols},
                count(*) as trades,
                sum(net_pnl_usd) as net_pnl_usd,
                sum(gross_pnl_usd) as gross_pnl_usd,
                sum(capital_used_usd) as turnover_usd
            from arb_trade_result
            where (%s::text is null or coalesce(metadata->>'run_tag', '') = %s::text)
            group by {select_cols}
            order by net_pnl_usd desc nulls last
        """
        return await self.db.fetch_all(sql, (run_tag, run_tag))

    async def summary(
        self,
        since_hours: int = 12,
        mode: str | None = "paper_live",
        source_code: str | None = "polymarket",
        run_tag: str | None = None,
    ) -> dict[str, Any]:
        now_utc = datetime.now(tz=UTC)
        window_start = now_utc - timedelta(hours=max(1, since_hours))

        totals_row = await self.db.fetch_one(
            """
            with window_signals as (
                select *
                from arb_signal
                where created_at >= %s
                  and (%s::text is null or mode = %s::text)
                  and (%s::text is null or source_code = %s::text)
                  and (%s::text is null or coalesce(features->>'run_tag', '') = %s::text)
            ),
            window_trades as (
                select *
                from arb_trade_result
                where created_at >= %s
                  and (%s::text is null or mode = %s::text)
                  and (%s::text is null or source_code = %s::text)
                  and (%s::text is null or coalesce(metadata->>'run_tag', '') = %s::text)
            )
            select
                coalesce((select count(*) from window_signals), 0) as signals_found,
                coalesce((select count(*) from window_signals where status = 'executed'), 0) as signals_executed,
                coalesce((select count(*) from window_signals where status = 'rejected'), 0) as signals_rejected,
                coalesce((select count(*) from window_signals where status = 'expired'), 0) as signals_expired,
                coalesce((select count(*) from window_signals where status = 'new'), 0) as signals_new,
                coalesce((select count(*) from window_trades), 0) as trades,
                coalesce((select sum(case when net_pnl_usd > 0 then 1 else 0 end) from window_trades), 0) as wins,
                coalesce((select sum(case when net_pnl_usd < 0 then 1 else 0 end) from window_trades), 0) as losses,
                coalesce((select sum(gross_pnl_usd) from window_trades), 0) as gross_pnl_usd,
                coalesce((select sum(net_pnl_usd) from window_trades), 0) as net_pnl_usd,
                coalesce((select sum(fees_usd) from window_trades), 0) as fees_usd,
                coalesce((select sum(slippage_usd) from window_trades), 0) as slippage_usd,
                coalesce((select sum(capital_used_usd) from window_trades), 0) as turnover_usd,
                (select min(created_at) from window_signals) as first_signal_at,
                (select max(created_at) from window_signals) as last_signal_at,
                (select min(created_at) from window_trades) as first_trade_at,
                (select max(created_at) from window_trades) as last_trade_at
            """,
            (
                window_start,
                mode,
                mode,
                source_code,
                source_code,
                run_tag,
                run_tag,
                window_start,
                mode,
                mode,
                source_code,
                source_code,
                run_tag,
                run_tag,
            ),
        )

        strategy_rows = await self.db.fetch_all(
            """
            with window_signals as (
                select *
                from arb_signal
                where created_at >= %s
                  and (%s::text is null or mode = %s::text)
                  and (%s::text is null or source_code = %s::text)
                  and (%s::text is null or coalesce(features->>'run_tag', '') = %s::text)
            ),
            window_trades as (
                select *
                from arb_trade_result
                where created_at >= %s
                  and (%s::text is null or mode = %s::text)
                  and (%s::text is null or source_code = %s::text)
                  and (%s::text is null or coalesce(metadata->>'run_tag', '') = %s::text)
            ),
            signal_stats as (
                select
                    strategy_code,
                    count(*) as signals_found,
                    sum((status = 'executed')::int) as signals_executed,
                    sum((status = 'rejected')::int) as signals_rejected,
                    sum((status = 'expired')::int) as signals_expired,
                    sum((status = 'new')::int) as signals_new,
                    min(created_at) as first_signal_at,
                    max(created_at) as last_signal_at
                from window_signals
                group by strategy_code
            ),
            trade_stats as (
                select
                    strategy_code,
                    count(*) as trades,
                    sum((net_pnl_usd > 0)::int) as wins,
                    sum((net_pnl_usd < 0)::int) as losses,
                    coalesce(sum(gross_pnl_usd), 0) as gross_pnl_usd,
                    coalesce(sum(net_pnl_usd), 0) as net_pnl_usd,
                    coalesce(sum(fees_usd), 0) as fees_usd,
                    coalesce(sum(slippage_usd), 0) as slippage_usd,
                    coalesce(sum(capital_used_usd), 0) as turnover_usd,
                    coalesce(avg(net_pnl_usd), 0) as avg_trade_pnl_usd,
                    coalesce(avg(capital_used_usd), 0) as avg_capital_used_usd,
                    coalesce(avg(hold_minutes), 0) as avg_hold_minutes,
                    min(created_at) as first_trade_at,
                    max(created_at) as last_trade_at
                from window_trades
                group by strategy_code
            ),
            first_events as (
                select
                    oi.signal_id,
                    min(oe.created_at) as first_event_at
                from arb_order_intent oi
                join arb_order_event oe on oe.intent_id = oi.intent_id
                join window_signals ws on ws.signal_id = oi.signal_id
                group by oi.signal_id
            ),
            latency_stats as (
                select
                    ws.strategy_code,
                    avg(extract(epoch from (fe.first_event_at - ws.created_at)) * 1000) as avg_detection_to_event_ms,
                    percentile_cont(0.5) within group (
                        order by extract(epoch from (fe.first_event_at - ws.created_at)) * 1000
                    ) as p50_detection_to_event_ms,
                    percentile_cont(0.95) within group (
                        order by extract(epoch from (fe.first_event_at - ws.created_at)) * 1000
                    ) as p95_detection_to_event_ms
                from window_signals ws
                join first_events fe on fe.signal_id = ws.signal_id
                where fe.first_event_at >= ws.created_at
                group by ws.strategy_code
            ),
            strategy_scope as (
                select strategy_code from signal_stats
                union
                select strategy_code from trade_stats
            )
            select
                sc.strategy_code,
                coalesce(ss.signals_found, 0) as signals_found,
                coalesce(ss.signals_executed, 0) as signals_executed,
                coalesce(ss.signals_rejected, 0) as signals_rejected,
                coalesce(ss.signals_expired, 0) as signals_expired,
                coalesce(ss.signals_new, 0) as signals_new,
                coalesce(ts.trades, 0) as trades,
                coalesce(ts.wins, 0) as wins,
                coalesce(ts.losses, 0) as losses,
                case
                    when coalesce(ts.trades, 0) = 0 then 0
                    else ts.wins::numeric / ts.trades::numeric
                end as win_rate,
                coalesce(ts.gross_pnl_usd, 0) as gross_pnl_usd,
                coalesce(ts.net_pnl_usd, 0) as net_pnl_usd,
                coalesce(ts.fees_usd, 0) as fees_usd,
                coalesce(ts.slippage_usd, 0) as slippage_usd,
                coalesce(ts.turnover_usd, 0) as turnover_usd,
                coalesce(ts.avg_trade_pnl_usd, 0) as avg_trade_pnl_usd,
                coalesce(ts.avg_capital_used_usd, 0) as avg_capital_used_usd,
                coalesce(ts.avg_hold_minutes, 0) as avg_hold_minutes,
                coalesce(ls.avg_detection_to_event_ms, 0) as avg_detection_to_event_ms,
                coalesce(ls.p50_detection_to_event_ms, 0) as p50_detection_to_event_ms,
                coalesce(ls.p95_detection_to_event_ms, 0) as p95_detection_to_event_ms,
                ss.first_signal_at,
                ss.last_signal_at,
                ts.first_trade_at,
                ts.last_trade_at
            from strategy_scope sc
            left join signal_stats ss on ss.strategy_code = sc.strategy_code
            left join trade_stats ts on ts.strategy_code = sc.strategy_code
            left join latency_stats ls on ls.strategy_code = sc.strategy_code
            order by net_pnl_usd desc, signals_found desc
            """,
            (
                window_start,
                mode,
                mode,
                source_code,
                source_code,
                run_tag,
                run_tag,
                window_start,
                mode,
                mode,
                source_code,
                source_code,
                run_tag,
                run_tag,
            ),
        )

        risk_rows = await self.db.fetch_all(
            """
            select
                reason,
                severity,
                count(*) as events
            from arb_risk_event
            where created_at >= %s
              and (%s::text is null or mode = %s::text)
              and (%s::text is null or source_code = %s::text)
              and (%s::text is null or coalesce(payload->>'run_tag', '') = %s::text)
            group by reason, severity
            order by events desc
            limit 15
            """,
            (window_start, mode, mode, source_code, source_code, run_tag, run_tag),
        )

        totals = _normalize_row(totals_row or {})
        if totals:
            totals = _attach_summary_metrics(totals)
        strategy_rows_normalized = []
        for row in strategy_rows:
            strategy_rows_normalized.append(_attach_summary_metrics(_normalize_row(row)))
        return {
            "window": {
                "since_hours": since_hours,
                "start_at": window_start,
                "end_at": now_utc,
                "mode": mode or "all",
                "source_code": source_code or "all",
                "run_tag": run_tag or "all",
            },
            "totals": totals,
            "by_strategy": strategy_rows_normalized,
            "risk_top": [_normalize_row(row) for row in risk_rows],
        }

    async def export_table(
        self,
        table: str,
        export_format: ExportFormat,
        output_path: Path,
        since_hours: int | None,
        limit: int,
    ) -> dict:
        result = await export_table(
            db=self.db,
            table=table,
            export_format=export_format,
            output_path=output_path,
            since_hours=since_hours,
            limit=limit,
        )
        return {
            "table": result.table,
            "rows": result.rows,
            "export_format": result.export_format,
            "output_path": str(result.output_path),
        }


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, Decimal):
            normalized[key] = float(value)
        else:
            normalized[key] = value
    return normalized


def _attach_summary_metrics(row: dict[str, Any]) -> dict[str, Any]:
    signals_found = float(row.get("signals_found", 0.0) or 0.0)
    signals_executed = float(row.get("signals_executed", 0.0) or 0.0)
    trades = float(row.get("trades", 0.0) or 0.0)
    wins = float(row.get("wins", 0.0) or 0.0)
    turnover_usd = float(row.get("turnover_usd", 0.0) or 0.0)
    net_pnl_usd = float(row.get("net_pnl_usd", 0.0) or 0.0)

    row["win_rate"] = _safe_ratio(wins, trades)
    row["execution_rate"] = _safe_ratio(signals_executed, signals_found)
    row["trade_conversion_rate"] = _safe_ratio(trades, signals_executed)
    row["net_margin_on_turnover"] = _safe_ratio(net_pnl_usd, turnover_usd)
    return row


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator
