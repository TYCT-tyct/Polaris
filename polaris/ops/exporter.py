from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

from polaris.db.pool import Database

ExportFormat = Literal["csv", "json"]

EXPORTABLE_TABLES: tuple[str, ...] = (
    "dim_account",
    "dim_tracking_window",
    "fact_tweet_metric_daily",
    "fact_tweet_post",
    "dim_market",
    "dim_token",
    "bridge_market_tracking",
    "fact_market_state_snapshot",
    "fact_quote_top_raw",
    "fact_quote_depth_raw",
    "fact_orderbook_l2_raw",
    "fact_quote_1m",
    "fact_settlement",
    "ops_collector_run",
    "ops_api_health_minute",
    "ops_cursor",
    "view_quote_latest",
)

TIME_FILTER_COLUMNS: dict[str, str] = {
    "dim_account": "updated_at",
    "dim_tracking_window": "updated_at",
    "fact_tweet_metric_daily": "captured_at",
    "fact_tweet_post": "captured_at",
    "dim_market": "captured_at",
    "dim_token": "captured_at",
    "bridge_market_tracking": "mapped_at",
    "fact_market_state_snapshot": "captured_at",
    "fact_quote_top_raw": "captured_at",
    "fact_quote_depth_raw": "captured_at",
    "fact_orderbook_l2_raw": "captured_at",
    "fact_quote_1m": "bucket_minute",
    "fact_settlement": "settled_at",
    "ops_collector_run": "started_at",
    "ops_api_health_minute": "minute_bucket",
    "ops_cursor": "updated_at",
    "view_quote_latest": "captured_at",
}


@dataclass(slots=True)
class ExportResult:
    table: str
    export_format: ExportFormat
    rows: int
    output_path: Path


def list_exportable_tables() -> list[str]:
    return list(EXPORTABLE_TABLES)


async def export_table(
    db: Database,
    table: str,
    export_format: ExportFormat,
    output_path: Path,
    limit: int = 10_000,
    since_hours: int | None = None,
) -> ExportResult:
    table_name = table.strip().lower()
    if table_name not in EXPORTABLE_TABLES:
        raise ValueError(f"table not exportable: {table_name}")
    if export_format not in ("csv", "json"):
        raise ValueError(f"unsupported export format: {export_format}")
    if limit <= 0 or limit > 1_000_000:
        raise ValueError("limit must be between 1 and 1000000")

    columns = await _load_columns(db, table_name)
    if not columns:
        raise ValueError(f"no columns found for table/view: {table_name}")

    params: list[Any] = []
    where_clauses: list[str] = []
    time_col = TIME_FILTER_COLUMNS.get(table_name)
    if since_hours is not None:
        if since_hours <= 0:
            raise ValueError("since_hours must be > 0")
        if time_col is None:
            raise ValueError(f"since_hours not supported for table/view: {table_name}")
        where_clauses.append(f"{time_col} >= now() - make_interval(hours => %s)")
        params.append(since_hours)

    where_sql = f" where {' and '.join(where_clauses)}" if where_clauses else ""
    order_col = time_col if time_col else columns[0]
    column_sql = ", ".join(columns)
    sql = f"select {column_sql} from {table_name}{where_sql} order by {order_col} desc limit %s"
    params.append(limit)
    rows = await db.fetch_all(sql, params)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if export_format == "csv":
        _write_csv(output_path, columns, rows)
    else:
        _write_json(output_path, rows)
    return ExportResult(table=table_name, export_format=export_format, rows=len(rows), output_path=output_path)


async def _load_columns(db: Database, table_name: str) -> list[str]:
    rows = await db.fetch_all(
        """
        select column_name
        from information_schema.columns
        where table_schema = 'public'
          and table_name = %s
        order by ordinal_position
        """,
        (table_name,),
    )
    return [str(row["column_name"]) for row in rows]


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: _to_csv_cell(row.get(col)) for col in columns})


def _write_json(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as file:
        normalized = [{k: _to_jsonable(v) for k, v in row.items()} for row in rows]
        json.dump(normalized, file, ensure_ascii=False, indent=2)


def _to_csv_cell(value: Any) -> str:
    normalized = _to_jsonable(value)
    if normalized is None:
        return ""
    if isinstance(normalized, (list, dict)):
        return json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
    return str(normalized)


def _to_jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {key: _to_jsonable(item) for key, item in value.items()}
    return value
