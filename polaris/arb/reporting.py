from __future__ import annotations

from pathlib import Path

from polaris.db.pool import Database
from polaris.ops.exporter import ExportFormat, export_table


class ArbReporter:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def report(self, group_by: str = "strategy,mode,source") -> list[dict]:
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
            group by {select_cols}
            order by net_pnl_usd desc nulls last
        """
        return await self.db.fetch_all(sql)

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
