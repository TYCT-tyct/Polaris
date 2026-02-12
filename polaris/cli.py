from __future__ import annotations

import asyncio
from collections import Counter
import importlib
import json
import re
import signal
import subprocess
import sys
import os
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import fmean
from typing import Annotated

import typer

from polaris.arb.bench.t2t import (
    dump_payload_json,
    generate_t2t_payload,
    load_payload_json,
    run_python_t2t_benchmark,
)
from polaris.arb.ai.gate import AiGate
from polaris.arb.cli import parse_iso_datetime, parse_run_mode
from polaris.arb.config import arb_config_from_settings
from polaris.arb.contracts import RunMode
from polaris.arb.orchestrator import ArbOrchestrator
from polaris.arb.reporting import ArbReporter
from polaris.config import PolarisSettings, load_settings, refresh_process_env_from_file
from polaris.db.pool import Database
from polaris.harvest.collector_markets import MarketCollector
from polaris.harvest.collector_quotes import QuoteCollector
from polaris.harvest.collector_tweets import TweetCollector
from polaris.harvest.discovery import discover_target_markets
from polaris.harvest.mapper_market_tracking import MarketTrackingMapper
from polaris.harvest.runner import HarvestRunner
from polaris.infra.rate_limiter import AsyncTokenBucket
from polaris.logging import setup_logging
from polaris.ops.backfill import BackfillService
from polaris.ops.backup import (
    DEFAULT_EXPORT_TABLES,
    BackupArtifact,
    backup_label,
    export_backup_tables,
    prune_backup_dirs,
    run_pg_dump,
    write_manifest,
)
from polaris.ops.exporter import ExportFormat, export_table, list_exportable_tables
from polaris.ops.health import HealthAggregator
from polaris.sources.clob_client import ClobClient
from polaris.sources.gamma_client import GammaClient
from polaris.sources.xtracker_client import XTrackerClient

app = typer.Typer(help="Polaris data harvester CLI.")


def _ensure_windows_selector_loop() -> None:
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


@dataclass
class RuntimeContext:
    settings: PolarisSettings
    db: Database
    xtracker: XTrackerClient
    gamma: GammaClient
    clob: ClobClient
    market_collector: MarketCollector
    tweet_collector: TweetCollector
    quote_collector: QuoteCollector
    mapper: MarketTrackingMapper
    health: HealthAggregator
    runner: HarvestRunner


@dataclass
class ArbRuntimeContext:
    settings: PolarisSettings
    db: Database
    clob: ClobClient
    orchestrator: ArbOrchestrator
    reporter: ArbReporter


def _create_database(settings: PolarisSettings) -> Database:
    return Database(
        settings.database_url,
        min_size=settings.db_pool_min_size,
        max_size=settings.db_pool_max_size,
    )


async def create_runtime(settings: PolarisSettings) -> RuntimeContext:
    db = _create_database(settings)
    await db.open()
    xtracker = XTrackerClient(
        limiter=AsyncTokenBucket(settings.xtracker_rate, settings.xtracker_burst),
        retry=settings.retry,
    )
    gamma = GammaClient(
        limiter=AsyncTokenBucket(settings.gamma_rate, settings.gamma_burst),
        retry=settings.retry,
    )
    clob = ClobClient(
        limiter=AsyncTokenBucket(settings.clob_rate, settings.clob_burst),
        retry=settings.retry,
        http2_enabled=settings.clob_http2_enabled,
        max_connections=settings.clob_max_connections,
        max_keepalive_connections=settings.clob_max_keepalive_connections,
        keepalive_expiry_seconds=settings.clob_keepalive_expiry_sec,
        ws_enabled=settings.clob_ws_enabled,
        ws_url=settings.clob_ws_url,
        ws_book_max_age_sec=settings.clob_ws_book_max_age_sec,
        book_cache_max_age_sec=settings.clob_book_cache_max_age_sec,
        ws_max_subscribe_tokens=settings.clob_ws_max_subscribe_tokens,
        ws_reconnect_min_sec=settings.clob_ws_reconnect_min_sec,
        ws_reconnect_max_sec=settings.clob_ws_reconnect_max_sec,
    )
    market_collector = MarketCollector(
        db,
        gamma,
        market_scope=settings.market_discovery_scope,
        market_state=settings.market_discovery_state,
        market_tweet_targets=settings.market_tweet_targets,
        gamma_page_size=settings.gamma_page_size,
        gamma_max_pages=settings.gamma_max_pages,
    )
    tweet_collector = TweetCollector(db, xtracker)
    quote_collector = QuoteCollector(db, clob, enable_l2=settings.enable_l2)
    mapper = MarketTrackingMapper(db)
    health = HealthAggregator(db)
    runner = HarvestRunner(
        settings=settings,
        db=db,
        market_collector=market_collector,
        tweet_collector=tweet_collector,
        quote_collector=quote_collector,
        mapper=mapper,
        health=health,
    )
    return RuntimeContext(
        settings=settings,
        db=db,
        xtracker=xtracker,
        gamma=gamma,
        clob=clob,
        market_collector=market_collector,
        tweet_collector=tweet_collector,
        quote_collector=quote_collector,
        mapper=mapper,
        health=health,
        runner=runner,
    )


async def create_arb_runtime(settings: PolarisSettings) -> ArbRuntimeContext:
    db = _create_database(settings)
    await db.open()
    clob = ClobClient(
        limiter=AsyncTokenBucket(settings.clob_rate, settings.clob_burst),
        retry=settings.retry,
        http2_enabled=settings.clob_http2_enabled,
        max_connections=settings.clob_max_connections,
        max_keepalive_connections=settings.clob_max_keepalive_connections,
        keepalive_expiry_seconds=settings.clob_keepalive_expiry_sec,
        ws_enabled=settings.clob_ws_enabled,
        ws_url=settings.clob_ws_url,
        ws_book_max_age_sec=settings.clob_ws_book_max_age_sec,
        book_cache_max_age_sec=settings.clob_book_cache_max_age_sec,
        ws_max_subscribe_tokens=settings.clob_ws_max_subscribe_tokens,
        ws_reconnect_min_sec=settings.clob_ws_reconnect_min_sec,
        ws_reconnect_max_sec=settings.clob_ws_reconnect_max_sec,
    )
    arb_config = arb_config_from_settings(settings)
    ai_gate = AiGate.from_settings(settings, arb_config)
    orchestrator = ArbOrchestrator(db=db, clob_client=clob, config=arb_config, ai_gate=ai_gate)
    reporter = ArbReporter(db)
    return ArbRuntimeContext(settings=settings, db=db, clob=clob, orchestrator=orchestrator, reporter=reporter)


async def close_runtime(ctx: RuntimeContext) -> None:
    await ctx.xtracker.close()
    await ctx.gamma.close()
    await ctx.clob.close()
    await ctx.db.close()


async def close_arb_runtime(ctx: ArbRuntimeContext) -> None:
    await ctx.orchestrator.close()
    await ctx.clob.close()
    await ctx.db.close()


_RUN_TAG_ALLOWED = re.compile(r"[^a-z0-9_.-]+")


def _sanitize_run_tag(value: str) -> str:
    normalized = value.strip().lower()
    normalized = _RUN_TAG_ALLOWED.sub("-", normalized)
    normalized = normalized.strip("-")
    if not normalized:
        raise typer.BadParameter("run-tag cannot be empty after normalization")
    return normalized[:64]


def _resolve_run_tag_filter(raw: str, current_tag: str) -> str | None:
    """Resolve run_tag filter without DB access.

    NOTE: CLI commands should prefer _resolve_run_tag_filter_db() so that
    `--run-tag current` maps to the *latest* historical run_tag instead of the
    ephemeral auto run_tag created for the current CLI process.
    """
    value = (raw or "").strip().lower()
    if not value or value == "current":
        return current_tag
    if value == "all":
        return None
    return _sanitize_run_tag(raw)


async def _resolve_run_tag_filter_db(
    db: Database,
    raw: str,
    *,
    mode_filter: str | None,
    source_filter: str | None,
    current_tag: str,
) -> str | None:
    """Resolve run_tag filter using DB data when needed.

    Semantics:
    - all -> no filter
    - custom -> sanitized exact match
    - current -> latest non-empty run_tag observed in DB for the given (mode, source)
      (fallback to current_tag if it's not an ephemeral auto tag).
    """
    value = (raw or "").strip().lower()
    if not value or value == "current":
        # If we're running inside a long-lived process (paper/live), keep its run_tag.
        if current_tag and not str(current_tag).startswith("auto-"):
            return current_tag
        # Otherwise, find the most recent run_tag from DB so "current" is useful.
        latest = await _arb_find_latest_run_tag(db, mode_filter=mode_filter, source_filter=source_filter)
        return latest or current_tag
    if value == "all":
        return None
    return _sanitize_run_tag(raw)


async def _arb_find_latest_run_tag(
    db: Database,
    *,
    mode_filter: str | None,
    source_filter: str | None,
) -> str | None:
    params = (
        mode_filter,
        mode_filter,
        source_filter,
        source_filter,
    )
    row = await db.fetch_one(
        """
        select coalesce(metadata->>'run_tag', '') as run_tag
        from arb_trade_result
        where (%s::text is null or mode = %s::text)
          and (%s::text is null or source_code = %s::text)
          and coalesce(metadata->>'run_tag', '') <> ''
        order by created_at desc
        limit 1
        """,
        params,
    )
    if row and row.get("run_tag"):
        return str(row["run_tag"])
    row = await db.fetch_one(
        """
        select coalesce(features->>'run_tag', '') as run_tag
        from arb_signal
        where (%s::text is null or mode = %s::text)
          and (%s::text is null or source_code = %s::text)
          and coalesce(features->>'run_tag', '') <> ''
        order by created_at desc
        limit 1
        """,
        params,
    )
    if row and row.get("run_tag"):
        return str(row["run_tag"])
    return None


_PAPER_PROFILE_PRESETS: dict[str, dict[str, str]] = {
    "trigger_safe_50": {
        "POLARIS_ARB_PAPER_INITIAL_BANKROLL_USD": "50",
        "POLARIS_ARB_SINGLE_RISK_USD": "6",
        "POLARIS_ARB_MAX_EXPOSURE_USD": "24",
        "POLARIS_ARB_DAILY_STOP_LOSS_USD": "8",
        "POLARIS_ARB_A_MIN_EDGE_PCT": "0.005",
        "POLARIS_ARB_B_MIN_EDGE_PCT": "0.004",
        "POLARIS_ARB_C_MIN_EDGE_PCT": "0.008",
        "POLARIS_ARB_F_MIN_PROB": "0.85",
        "POLARIS_ARB_F_MAX_HOURS_TO_RESOLVE": "72",
        "POLARIS_ARB_F_MIN_ANNUALIZED_RETURN": "0.02",
        "POLARIS_ARB_F_MAX_SPREAD": "0.03",
        "POLARIS_ARB_G_MAX_HOURS_TO_RESOLVE": "72",
        # G (尾部收敛) 非无风险套利：必须覆盖 bid/ask 与噪声，否则 paper 会稳定亏损。
        "POLARIS_ARB_G_MIN_CONFIDENCE": "0.90",
        "POLARIS_ARB_G_MIN_EXPECTED_EDGE_PCT": "0.02",
        "POLARIS_ARB_SAFE_ARBITRAGE_ONLY": "false",
        "POLARIS_ARB_ENABLE_STRATEGY_A": "true",
        "POLARIS_ARB_ENABLE_STRATEGY_B": "true",
        "POLARIS_ARB_ENABLE_STRATEGY_C": "true",
        "POLARIS_ARB_ENABLE_STRATEGY_F": "true",
        "POLARIS_ARB_ENABLE_STRATEGY_G": "true",
    },
    # 更保守且可触发：专门用来避免 G 过度交易拖垮组合（shared 口径）。
    "trigger_safe_50_v2": {
        "POLARIS_ARB_PAPER_INITIAL_BANKROLL_USD": "50",
        "POLARIS_ARB_SINGLE_RISK_USD": "6",
        "POLARIS_ARB_MAX_EXPOSURE_USD": "24",
        "POLARIS_ARB_DAILY_STOP_LOSS_USD": "8",
        # 限制每轮最大处理信号，避免行情噪声导致过度成交与 DB 写入压力。
        "POLARIS_ARB_MAX_SIGNALS_PER_CYCLE": "4",
        "POLARIS_ARB_A_MIN_EDGE_PCT": "0.005",
        "POLARIS_ARB_B_MIN_EDGE_PCT": "0.004",
        "POLARIS_ARB_C_MIN_EDGE_PCT": "0.008",
        "POLARIS_ARB_F_MIN_PROB": "0.85",
        "POLARIS_ARB_F_MAX_HOURS_TO_RESOLVE": "72",
        "POLARIS_ARB_F_MIN_ANNUALIZED_RETURN": "0.02",
        "POLARIS_ARB_F_MAX_SPREAD": "0.03",
        # G 只在更短窗口、更高置信度、更高边际下触发：否则 mark-to-book 会稳定为负。
        "POLARIS_ARB_G_MAX_HOURS_TO_RESOLVE": "12",
        "POLARIS_ARB_G_MIN_CONFIDENCE": "0.95",
        "POLARIS_ARB_G_MIN_EXPECTED_EDGE_PCT": "0.05",
        "POLARIS_ARB_SAFE_ARBITRAGE_ONLY": "false",
        "POLARIS_ARB_ENABLE_STRATEGY_A": "true",
        "POLARIS_ARB_ENABLE_STRATEGY_B": "true",
        "POLARIS_ARB_ENABLE_STRATEGY_C": "true",
        "POLARIS_ARB_ENABLE_STRATEGY_F": "true",
        "POLARIS_ARB_ENABLE_STRATEGY_G": "true",
    },
    "conservative_50": {
        "POLARIS_ARB_PAPER_INITIAL_BANKROLL_USD": "50",
        "POLARIS_ARB_SINGLE_RISK_USD": "4",
        "POLARIS_ARB_MAX_EXPOSURE_USD": "16",
        "POLARIS_ARB_DAILY_STOP_LOSS_USD": "6",
        "POLARIS_ARB_A_MIN_EDGE_PCT": "0.015",
        "POLARIS_ARB_B_MIN_EDGE_PCT": "0.012",
        "POLARIS_ARB_C_MIN_EDGE_PCT": "0.015",
        "POLARIS_ARB_F_MIN_PROB": "0.94",
        "POLARIS_ARB_F_MAX_HOURS_TO_RESOLVE": "72",
        "POLARIS_ARB_F_MIN_ANNUALIZED_RETURN": "0.08",
        "POLARIS_ARB_F_MAX_SPREAD": "0.03",
        "POLARIS_ARB_G_MAX_HOURS_TO_RESOLVE": "72",
        "POLARIS_ARB_G_MIN_CONFIDENCE": "0.90",
        "POLARIS_ARB_G_MIN_EXPECTED_EDGE_PCT": "0.02",
        "POLARIS_ARB_SAFE_ARBITRAGE_ONLY": "false",
        "POLARIS_ARB_ENABLE_STRATEGY_A": "true",
        "POLARIS_ARB_ENABLE_STRATEGY_B": "true",
        "POLARIS_ARB_ENABLE_STRATEGY_C": "true",
        "POLARIS_ARB_ENABLE_STRATEGY_F": "true",
        "POLARIS_ARB_ENABLE_STRATEGY_G": "true",
    },
}


def _resolve_paper_profile(name: str) -> dict[str, str]:
    key = name.strip().lower()
    if key not in _PAPER_PROFILE_PRESETS:
        available = ", ".join(sorted(_PAPER_PROFILE_PRESETS.keys()))
        raise typer.BadParameter(f"unknown profile={name!r}, available: {available}")
    return dict(_PAPER_PROFILE_PRESETS[key])


def _apply_profile_to_env_file(env_path: Path, profile: dict[str, str]) -> int:
    existing: list[str] = []
    if env_path.exists():
        existing = env_path.read_text(encoding="utf-8").splitlines()
    changed = 0
    matched: set[str] = set()
    updated: list[str] = []
    for line in existing:
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in line:
            updated.append(line)
            continue
        key = line.split("=", 1)[0].strip()
        if key in profile:
            value = profile[key]
            newline = f"{key}={value}"
            if line != newline:
                changed += 1
            updated.append(newline)
            matched.add(key)
        else:
            updated.append(line)
    for key, value in profile.items():
        if key in matched:
            continue
        updated.append(f"{key}={value}")
        changed += 1
    env_path.write_text("\n".join(updated).rstrip() + "\n", encoding="utf-8")
    return changed


@app.command("migrate")
def migrate() -> None:
    """Apply SQL migrations."""
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    async def _run() -> None:
        db = _create_database(settings)
        await db.open()
        try:
            applied = await db.apply_migrations()
            typer.echo(f"Applied migrations: {applied if applied else 'none'}")
        finally:
            await db.close()

    asyncio.run(_run())


@app.command("harvest-once")
def harvest_once(
    handle: Annotated[str, typer.Option("--handle", help="X handle to harvest")] = "elonmusk",
) -> None:
    """Run one full harvest cycle."""
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    async def _run() -> None:
        ctx = await create_runtime(settings)
        try:
            await ctx.runner.run_once([handle.lower()])
            typer.echo("harvest-once completed")
        finally:
            await close_runtime(ctx)

    asyncio.run(_run())


@app.command("run")
def run(
    handle: Annotated[str, typer.Option("--handle", help="Comma separated handles")] = "elonmusk",
    hot_reload: Annotated[
        bool,
        typer.Option("--hot-reload/--no-hot-reload", help="Reload runtime when .env changes or SIGHUP arrives"),
    ] = True,
    reload_poll_sec: Annotated[int, typer.Option("--reload-poll-sec", min=1, max=60)] = 2,
) -> None:
    """Run continuous scheduler."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()
    requested_handles = handle.split(",")

    async def _run() -> None:
        stop_event = asyncio.Event()
        reload_event = asyncio.Event()
        _install_signal_handlers(stop_event, reload_event if hot_reload else None)
        watch_task = (
            asyncio.create_task(_watch_env_file(Path(".env"), reload_event, stop_event, reload_poll_sec))
            if hot_reload
            else None
        )
        try:
            while not stop_event.is_set():
                refresh_process_env_from_file(preserve_existing=True)
                load_settings.cache_clear()
                current_settings = load_settings()
                setup_logging(current_settings.log_level)
                current_handles = current_settings.with_handles(requested_handles)
                ctx = await create_runtime(current_settings)
                runner_task = asyncio.create_task(ctx.runner.run_forever(current_handles))
                stop_wait = asyncio.create_task(stop_event.wait())
                reload_wait = asyncio.create_task(reload_event.wait())
                done, pending = await asyncio.wait(
                    [runner_task, stop_wait, reload_wait],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

                if stop_wait in done:
                    runner_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await runner_task
                    await close_runtime(ctx)
                    break

                if hot_reload and reload_wait in done:
                    reload_event.clear()
                    runner_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await runner_task
                    await close_runtime(ctx)
                    typer.echo("hot reload applied")
                    continue

                if runner_task in done:
                    exc = runner_task.exception()
                    await close_runtime(ctx)
                    if exc:
                        raise exc
                    break
        finally:
            if watch_task:
                watch_task.cancel()
                await asyncio.gather(watch_task, return_exceptions=True)

    asyncio.run(_run())


@app.command("backfill")
def backfill(
    handle: Annotated[str, typer.Option("--handle")] = "elonmusk",
    start: Annotated[str, typer.Option("--start", help="YYYY-MM-DD")] = "",
    end: Annotated[str, typer.Option("--end", help="YYYY-MM-DD")] = "",
) -> None:
    """Backfill metrics/posts within a date range."""
    if not start or not end:
        raise typer.BadParameter("start and end are required")
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    async def _run() -> None:
        ctx = await create_runtime(settings)
        try:
            start_dt = datetime.fromisoformat(f"{start}T00:00:00+00:00")
            end_dt = datetime.fromisoformat(f"{end}T23:59:59+00:00")
            service = BackfillService(ctx.market_collector, ctx.tweet_collector)
            result = await service.run(handle.lower(), start_dt, end_dt)
            typer.echo(f"backfill completed: {result}")
        finally:
            await close_runtime(ctx)

    asyncio.run(_run())


@app.command("doctor")
def doctor(
    handle: Annotated[str, typer.Option("--handle")] = "elonmusk",
) -> None:
    """Run connectivity checks for DB + XTracker + Gamma + CLOB."""
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    async def _run() -> None:
        ctx = await create_runtime(settings)
        checks: list[str] = []
        try:
            await ctx.db.fetch_one("select now() as now")
            checks.append("db:ok")

            user = await ctx.xtracker.get_user(handle.lower())
            checks.append(f"xtracker_user:ok account_id={user.account_id}")

            trackings = await ctx.xtracker.get_trackings(handle.lower(), active_only=True)
            checks.append(f"xtracker_trackings:ok count={len(trackings)}")

            posts_hash, posts, _ = await ctx.xtracker.get_posts(handle.lower())
            checks.append(f"xtracker_posts:ok count={len(posts)} hash={posts_hash[:8]}")

            # doctor 只做只读探活，避免与在线采集进程并发写库导致锁竞争。
            rows = await discover_target_markets(
                ctx.gamma,
                scope="all",
                state="open",
                page_size=200,
                max_pages=1,
            )
            checks.append(f"gamma_markets:ok count={len(rows)}")

            tokens = []
            for row in rows:
                tokens.extend(ctx.gamma.token_descriptors(row))
            if tokens:
                picked = None
                for token_id in list(dict.fromkeys(t.token_id for t in tokens))[:20]:
                    book = await ctx.clob.get_book_optional(token_id)
                    if book is not None:
                        picked = token_id
                        break
                if picked:
                    checks.append(f"clob_book:ok token_id={picked}")
                else:
                    checks.append("clob_book:warn_no_available_token")
            else:
                checks.append("clob_book:skip_no_tokens")

            typer.echo("\n".join(checks))
        finally:
            await close_runtime(ctx)

    asyncio.run(_run())


@app.command("export-tables")
def export_tables() -> None:
    """List exportable tables/views."""
    for name in list_exportable_tables():
        typer.echo(name)


@app.command("export")
def export_data(
    table: Annotated[str, typer.Option("--table", help="Table/view name to export")] = "fact_quote_top_raw",
    export_format: Annotated[str, typer.Option("--format", help="csv or json")] = "csv",
    output: Annotated[str, typer.Option("--output", help="Target file path")] = "",
    limit: Annotated[int, typer.Option("--limit", min=1, max=1_000_000)] = 10000,
    since_hours: Annotated[int, typer.Option("--since-hours", min=0)] = 0,
) -> None:
    """Export one table/view to CSV or JSON."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    fmt = export_format.strip().lower()
    if fmt not in ("csv", "json"):
        raise typer.BadParameter("format must be csv or json")
    fmt_typed: ExportFormat = "csv" if fmt == "csv" else "json"
    out_path = Path(output).expanduser() if output else _default_export_path(table, fmt_typed)

    async def _run() -> None:
        db = _create_database(settings)
        await db.open()
        try:
            result = await export_table(
                db=db,
                table=table,
                export_format=fmt_typed,
                output_path=out_path,
                limit=limit,
                since_hours=since_hours or None,
            )
            typer.echo(
                f"export completed: table={result.table} rows={result.rows} "
                f"format={result.export_format} file={result.output_path}"
            )
        finally:
            await db.close()

    asyncio.run(_run())


@app.command("backup")
def backup(
    output_dir: Annotated[str, typer.Option("--output-dir", help="Backup root directory")] = "backups",
    label: Annotated[str, typer.Option("--label", help="Optional label suffix")] = "",
    pg_dump_bin: Annotated[str, typer.Option("--pg-dump-bin", help="pg_dump executable path")] = "pg_dump",
    timeout_sec: Annotated[int, typer.Option("--timeout-sec", min=30, max=14400)] = 1800,
    include_exports: Annotated[
        bool,
        typer.Option("--include-exports/--no-include-exports", help="Include key table CSV exports"),
    ] = True,
    export_since_hours: Annotated[int, typer.Option("--export-since-hours", min=1, max=168)] = 24,
    export_limit: Annotated[int, typer.Option("--export-limit", min=100, max=1_000_000)] = 200_000,
    keep_last: Annotated[int, typer.Option("--keep-last", min=1, max=365)] = 14,
) -> None:
    """Create a consistent backup snapshot."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    backup_root = Path(output_dir).expanduser()
    run_label = backup_label(label or None)
    backup_dir = backup_root / run_label
    dump_file = backup_dir / "polaris_db.dump"
    artifacts: list[BackupArtifact] = []

    try:
        run_pg_dump(
            database_url=settings.database_url,
            output_file=dump_file,
            pg_dump_bin=pg_dump_bin,
            timeout_sec=timeout_sec,
        )
    except FileNotFoundError as exc:
        raise typer.BadParameter(
            f"pg_dump not found: {pg_dump_bin}. install postgresql-client or set --pg-dump-bin"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise typer.BadParameter(f"pg_dump timeout after {timeout_sec}s") from exc

    artifacts.append(BackupArtifact.from_path(dump_file))

    async def _run_exports() -> list[Path]:
        db = _create_database(settings)
        await db.open()
        try:
            return await export_backup_tables(
                db=db,
                backup_dir=backup_dir,
                tables=list(DEFAULT_EXPORT_TABLES),
                since_hours=export_since_hours,
                limit=export_limit,
            )
        finally:
            await db.close()

    if include_exports:
        export_files = asyncio.run(_run_exports())
        artifacts.extend(BackupArtifact.from_path(path) for path in export_files)

    manifest_file = write_manifest(backup_dir, artifacts)
    artifacts.append(BackupArtifact.from_path(manifest_file))
    pruned = prune_backup_dirs(backup_root, keep_last)

    typer.echo(f"backup completed: {backup_dir}")
    typer.echo(f"artifacts={len(artifacts)} keep_last={keep_last} pruned={len(pruned)}")
    typer.echo(f"manifest={manifest_file}")


@app.command("arb-run")
def arb_run(
    mode: Annotated[str, typer.Option("--mode", help="shadow|paper_live|live")] = "paper_live",
    source: Annotated[str, typer.Option("--source", help="Portfolio source tag")] = "polymarket",
    run_tag: Annotated[str, typer.Option("--run-tag", help="auto|current|custom")] = "auto",
    paper_capital_scope: Annotated[
        str,
        typer.Option("--paper-capital-scope", help="shared|strategy (paper mode only)"),
    ] = "shared",
    once: Annotated[bool, typer.Option("--once/--loop", help="Run one scan cycle or keep running")] = False,
) -> None:
    """Run Module2 arbitrage engine."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    run_tag_value = run_tag.strip().lower()
    if run_tag_value and run_tag_value not in {"auto", "current"}:
        settings = settings.model_copy(update={"arb_run_tag": _sanitize_run_tag(run_tag)})
    elif run_tag_value == "current":
        settings = settings.model_copy(update={"arb_run_tag": arb_config_from_settings(settings).run_tag})
    scope = paper_capital_scope.strip().lower()
    if scope not in {"shared", "strategy"}:
        raise typer.BadParameter("paper-capital-scope must be shared or strategy")
    settings = settings.model_copy(update={"arb_paper_split_by_strategy": scope == "strategy"})
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()
    run_mode = parse_run_mode(mode)
    source_code = source.strip().lower() or "polymarket"

    async def _run() -> None:
        ctx = await create_arb_runtime(settings)
        try:
            if once:
                stats = await ctx.orchestrator.run_once(run_mode, source_code=source_code)
                typer.echo(f"arb-run once completed: {stats}")
            else:
                await ctx.orchestrator.run_forever(run_mode, source_code=source_code)
        finally:
            await close_arb_runtime(ctx)

    asyncio.run(_run())


@app.command("arb-paper-profile")
def arb_paper_profile(
    name: Annotated[str, typer.Option("--name")] = "trigger_safe_50_v2",
    env_file: Annotated[str, typer.Option("--env-file")] = ".env",
    apply: Annotated[bool, typer.Option("--apply/--print")] = True,
) -> None:
    """Apply a paper parameter profile for triggerability/risk control."""
    profile = _resolve_paper_profile(name)
    payload: dict[str, object] = {"profile": name.strip().lower(), "values": profile, "applied": False}
    if apply:
        path = Path(env_file).expanduser()
        changed = _apply_profile_to_env_file(path, profile)
        refresh_process_env_from_file(path, preserve_existing=False)
        load_settings.cache_clear()
        payload["applied"] = True
        payload["env_file"] = str(path)
        payload["changed"] = changed
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


@app.command("arb-doctor")
def arb_doctor(
    mode: Annotated[str, typer.Option("--mode", help="shadow|paper_live|live")] = "paper_live",
    source: Annotated[str, typer.Option("--source")] = "polymarket",
) -> None:
    """Check triggerability before paper/live run and explain why signals may be zero."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()
    run_mode = parse_run_mode(mode)
    source_code = source.strip().lower() or "polymarket"

    async def _run() -> int:
        ctx = await create_arb_runtime(settings)
        try:
            report = await _build_arb_doctor_report(ctx, run_mode, source_code)
            typer.echo(json.dumps(report, ensure_ascii=False, indent=2, default=str))
            return 2 if bool(report.get("likely_zero_signal")) else 0
        finally:
            await close_arb_runtime(ctx)

    code = asyncio.run(_run())
    raise typer.Exit(code=code)


@app.command("arb-paper-matrix-start")
def arb_paper_matrix_start(
    duration_hours: Annotated[int, typer.Option("--duration-hours", min=1, max=72)] = 4,
    source_prefix: Annotated[str, typer.Option("--source-prefix")] = "polymarket",
    profile: Annotated[str, typer.Option("--profile")] = "trigger_safe_50_v2",
    bankroll_usd: Annotated[float, typer.Option("--bankroll-usd", min=1.0)] = 50.0,
    include_c: Annotated[bool, typer.Option("--include-c/--no-include-c")] = True,
) -> None:
    """Start paper matrix:
    - A/B/C/F/G each isolated bankroll
    - ABCFG shared bankroll
    """
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    profile_env = _resolve_paper_profile(profile)

    strategies = ["A", "B", "F", "G"] + (["C"] if include_c else [])
    timestamp = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
    matrix_tag = _sanitize_run_tag(f"paper-{profile}-{timestamp}")
    logs_dir = Path("logs")
    logs_dir.mkdir(parents=True, exist_ok=True)
    run_dir = Path("run")
    run_dir.mkdir(parents=True, exist_ok=True)

    bankroll_tag = str(int(bankroll_usd)) if float(bankroll_usd).is_integer() else f"{bankroll_usd:g}"

    def _spawn(name: str, enabled: set[str], split_by_strategy: bool) -> tuple[int, Path, str]:
        source = f"{source_prefix}_{profile}_{name}{bankroll_tag}".lower()
        log_path = logs_dir / f"arb_paper_{name}_{timestamp}.log"
        env = os.environ.copy()
        env.update(profile_env)
        env["POLARIS_ARB_RUN_TAG"] = matrix_tag
        env["POLARIS_ARB_PAPER_INITIAL_BANKROLL_USD"] = f"{bankroll_usd:.4f}"
        env["POLARIS_ARB_PAPER_SPLIT_BY_STRATEGY"] = "true" if split_by_strategy else "false"
        env["POLARIS_ARB_ENABLE_STRATEGY_A"] = "true" if "A" in enabled else "false"
        env["POLARIS_ARB_ENABLE_STRATEGY_B"] = "true" if "B" in enabled else "false"
        env["POLARIS_ARB_ENABLE_STRATEGY_C"] = "true" if "C" in enabled else "false"
        env["POLARIS_ARB_ENABLE_STRATEGY_F"] = "true" if "F" in enabled else "false"
        env["POLARIS_ARB_ENABLE_STRATEGY_G"] = "true" if "G" in enabled else "false"

        cmd: list[str] = [
            sys.executable,
            "-m",
            "polaris.cli",
            "arb-run",
            "--mode",
            "paper_live",
            "--source",
            source,
            "--run-tag",
            matrix_tag,
            "--paper-capital-scope",
            "strategy" if split_by_strategy else "shared",
        ]
        if not sys.platform.startswith("win"):
            cmd = ["timeout", f"{duration_hours}h"] + cmd

        log_handle = log_path.open("a", encoding="utf-8")
        proc = subprocess.Popen(
            cmd,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            env=env,
            cwd=str(Path.cwd()),
            start_new_session=True,
        )
        log_handle.close()
        pid_path = run_dir / f"arb_paper_{name}_{timestamp}.pid"
        pid_path.write_text(str(proc.pid), encoding="utf-8")
        return proc.pid, log_path, source

    started: list[tuple[str, int, Path, str]] = []
    shared_name = "shared_abcfg"
    started.append((shared_name, *_spawn(shared_name, set(strategies), False)))
    for strategy in strategies:
        name = f"isolated_{strategy.lower()}"
        started.append((name, *_spawn(name, {strategy}, True)))

    typer.echo("arb-paper-matrix started")
    typer.echo(f"profile={profile} run_tag={matrix_tag} bankroll={bankroll_usd:g}")
    for name, pid, log_path, source in started:
        typer.echo(f"{name}: pid={pid} source={source} log={log_path}")


@app.command("arb-replay-matrix")
def arb_replay_matrix(
    start: Annotated[str, typer.Option("--start", help="ISO datetime")] = "",
    end: Annotated[str, typer.Option("--end", help="ISO datetime")] = "",
    profile: Annotated[str, typer.Option("--profile")] = "trigger_safe_50_v2",
    bankroll_usd: Annotated[float, typer.Option("--bankroll-usd", min=1.0)] = 50.0,
    source_prefix: Annotated[str, typer.Option("--source-prefix")] = "polymarket_replay",
    include_c: Annotated[bool, typer.Option("--include-c/--no-include-c")] = True,
    fast: Annotated[
        bool,
        typer.Option("--fast/--full", help="Fast replay skips detailed signal/order/fill writes"),
    ] = False,
) -> None:
    """Run replay matrix:
    - A/B/C/F/G each isolated bankroll
    - ABCFG shared bankroll
    """
    if not start or not end:
        raise typer.BadParameter("start and end are required")
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    profile_env = _resolve_paper_profile(profile)

    strategies = ["A", "B", "F", "G"] + (["C"] if include_c else [])
    timestamp = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
    matrix_tag = _sanitize_run_tag(f"replay-{profile}-{timestamp}")
    logs_dir = Path("logs")
    logs_dir.mkdir(parents=True, exist_ok=True)
    bankroll_tag = str(int(bankroll_usd)) if float(bankroll_usd).is_integer() else f"{bankroll_usd:g}"

    cases: list[tuple[str, set[str], bool]] = [("shared_abcfg", set(strategies), False)]
    for strategy in strategies:
        cases.append((f"isolated_{strategy.lower()}", {strategy}, True))

    rows: list[dict[str, object]] = []
    for name, enabled, split_by_strategy in cases:
        source = f"{source_prefix}_{profile}_{name}{bankroll_tag}".lower()
        run_tag = _sanitize_run_tag(f"{matrix_tag}-{name}")
        log_path = logs_dir / f"arb_replay_{name}_{timestamp}.log"

        env = os.environ.copy()
        env.update(profile_env)
        env["POLARIS_ARB_RUN_TAG"] = run_tag
        env["POLARIS_ARB_PAPER_INITIAL_BANKROLL_USD"] = f"{bankroll_usd:.4f}"
        env["POLARIS_ARB_PAPER_SPLIT_BY_STRATEGY"] = "true" if split_by_strategy else "false"
        env["POLARIS_ARB_ENABLE_STRATEGY_A"] = "true" if "A" in enabled else "false"
        env["POLARIS_ARB_ENABLE_STRATEGY_B"] = "true" if "B" in enabled else "false"
        env["POLARIS_ARB_ENABLE_STRATEGY_C"] = "true" if "C" in enabled else "false"
        env["POLARIS_ARB_ENABLE_STRATEGY_F"] = "true" if "F" in enabled else "false"
        env["POLARIS_ARB_ENABLE_STRATEGY_G"] = "true" if "G" in enabled else "false"

        cmd: list[str] = [
            sys.executable,
            "-m",
            "polaris.cli",
            "arb-replay",
            "--start",
            start,
            "--end",
            end,
            "--source",
            source,
            "--run-tag",
            run_tag,
        ]
        cmd.append("--fast" if fast else "--full")
        completed = subprocess.run(
            cmd,
            env=env,
            cwd=str(Path.cwd()),
            capture_output=True,
            text=True,
            check=False,
        )
        log_path.write_text((completed.stdout or "") + (completed.stderr or ""), encoding="utf-8")
        rows.append(
            {
                "name": name,
                "source": source,
                "run_tag": run_tag,
                "split_by_strategy": split_by_strategy,
                "enabled": sorted(enabled),
                "exit_code": int(completed.returncode),
                "log": str(log_path),
            }
        )

    typer.echo(json.dumps({"matrix_tag": matrix_tag, "rows": rows}, ensure_ascii=False, indent=2))


@app.command("arb-paper-matrix-stop")
def arb_paper_matrix_stop() -> None:
    """Stop background matrix paper runs if pid files exist."""
    run_dir = Path("run")
    stopped = 0
    for pid_file in run_dir.glob("arb_paper_*.pid"):
        try:
            pid = int(pid_file.read_text(encoding="utf-8").strip())
        except Exception:
            pid_file.unlink(missing_ok=True)
            continue
        with suppress(ProcessLookupError):
            if sys.platform.startswith("win"):
                subprocess.run(["taskkill", "/PID", str(pid), "/F"], check=False, capture_output=True, text=True)
            else:
                os.kill(pid, signal.SIGTERM)
            stopped += 1
        pid_file.unlink(missing_ok=True)
    typer.echo(f"arb-paper-matrix stopped processes={stopped}")


@app.command("arb-replay")
def arb_replay(
    start: Annotated[str, typer.Option("--start", help="ISO datetime")] = "",
    end: Annotated[str, typer.Option("--end", help="ISO datetime")] = "",
    source: Annotated[str, typer.Option("--source", help="Replay source code tag")] = "polymarket",
    run_tag: Annotated[str, typer.Option("--run-tag", help="auto|current|custom")] = "auto",
    fast: Annotated[
        bool,
        typer.Option("--fast/--full", help="Fast replay skips detailed signal/order/fill writes"),
    ] = True,
) -> None:
    """Run historical replay with real captured market data."""
    if not start or not end:
        raise typer.BadParameter("start and end are required")
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    run_tag_value = run_tag.strip().lower()
    if run_tag_value and run_tag_value not in {"auto", "current"}:
        settings = settings.model_copy(update={"arb_run_tag": _sanitize_run_tag(run_tag)})
    elif run_tag_value == "current":
        settings = settings.model_copy(update={"arb_run_tag": arb_config_from_settings(settings).run_tag})
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()
    start_ts = parse_iso_datetime(start)
    end_ts = parse_iso_datetime(end)

    async def _run() -> None:
        ctx = await create_arb_runtime(settings)
        try:
            source_code = source.strip().lower() or "polymarket"
            stats = await ctx.orchestrator.run_replay(start_ts, end_ts, source_code=source_code, fast=fast)
            typer.echo(f"arb-replay completed: {stats}")
        finally:
            await close_arb_runtime(ctx)

    asyncio.run(_run())


@app.command("arb-optimize")
def arb_optimize() -> None:
    """Run one parameter evolution cycle for Module2."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    async def _run() -> None:
        ctx = await create_arb_runtime(settings)
        try:
            await ctx.orchestrator.optimize_parameters()
            typer.echo("arb-optimize completed")
        finally:
            await close_arb_runtime(ctx)

    asyncio.run(_run())


@app.command("arb-report")
def arb_report(
    group_by: Annotated[str, typer.Option("--group-by", help="strategy,mode,source,day")] = "strategy,mode,source",
    run_tag: Annotated[str, typer.Option("--run-tag", help="current|all|custom")] = "current",
) -> None:
    """Show aggregated performance report for Module2."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    async def _run() -> None:
        ctx = await create_arb_runtime(settings)
        try:
            run_tag_filter = await _resolve_run_tag_filter_db(
                ctx.db,
                run_tag,
                mode_filter=None,
                source_filter=None,
                current_tag=ctx.orchestrator.config.run_tag,
            )
            rows = await ctx.reporter.report(group_by=group_by, run_tag=run_tag_filter)
            typer.echo(json.dumps(rows, ensure_ascii=False, indent=2, default=str))
        finally:
            await close_arb_runtime(ctx)

    asyncio.run(_run())


@app.command("arb-summary")
def arb_summary(
    since_hours: Annotated[int, typer.Option("--since-hours", min=1, max=24 * 30)] = 12,
    mode: Annotated[str, typer.Option("--mode", help="all|shadow|paper_live|paper_replay|live")] = "paper_live",
    source_code: Annotated[str, typer.Option("--source", help="source code, or all")] = "polymarket",
    run_tag: Annotated[str, typer.Option("--run-tag", help="current|all|custom")] = "current",
    output: Annotated[str, typer.Option("--output", help="Optional JSON output path")] = "",
) -> None:
    """Show strategy-level overnight summary for Module2."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    allowed_modes = {"all", "shadow", "paper_live", "paper_replay", "live"}
    mode_normalized = mode.strip().lower()
    if mode_normalized not in allowed_modes:
        raise typer.BadParameter("mode must be one of: all, shadow, paper_live, paper_replay, live")
    source_normalized = source_code.strip().lower()
    mode_filter = None if mode_normalized == "all" else mode_normalized
    source_filter = None if source_normalized == "all" else source_normalized

    async def _run() -> None:
        ctx = await create_arb_runtime(settings)
        try:
            run_tag_filter = await _resolve_run_tag_filter_db(
                ctx.db,
                run_tag,
                mode_filter=mode_filter,
                source_filter=source_filter,
                current_tag=ctx.orchestrator.config.run_tag,
            )
            result = await ctx.reporter.summary(
                since_hours=since_hours,
                mode=mode_filter,
                source_code=source_filter,
                run_tag=run_tag_filter,
            )
            text = json.dumps(result, ensure_ascii=False, indent=2, default=str)
            if output:
                out_path = Path(output).expanduser()
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(text, encoding="utf-8")
                typer.echo(f"arb-summary saved: {out_path}")
            typer.echo(text)
        finally:
            await close_arb_runtime(ctx)

    asyncio.run(_run())


@app.command("arb-go-live-check")
def arb_go_live_check(
    since_hours: Annotated[int, typer.Option("--since-hours", min=1, max=24 * 30)] = 24,
    source_code: Annotated[str, typer.Option("--source", help="source code, or all")] = "all",
    run_tag: Annotated[str, typer.Option("--run-tag", help="current|all|custom")] = "current",
    min_trades: Annotated[int, typer.Option("--min-trades", min=1)] = 20,
    min_net_pnl_usd: Annotated[float, typer.Option("--min-net-pnl-usd")] = 0.0,
    min_win_rate: Annotated[float, typer.Option("--min-win-rate", min=0.0, max=1.0)] = 0.45,
    max_negative_strategies: Annotated[int, typer.Option("--max-negative-strategies", min=0)] = 0,
    strategy_min_trades: Annotated[int, typer.Option("--strategy-min-trades", min=1)] = 8,
) -> None:
    """Evaluate whether paper results are healthy enough before live funds."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    source_normalized = source_code.strip().lower()
    source_filter = None if source_normalized == "all" else source_normalized

    async def _run() -> int:
        ctx = await create_arb_runtime(settings)
        try:
            run_tag_filter = await _resolve_run_tag_filter_db(
                ctx.db,
                run_tag,
                mode_filter="paper_live",
                source_filter=source_filter,
                current_tag=ctx.orchestrator.config.run_tag,
            )
            summary = await ctx.reporter.summary(
                since_hours=since_hours,
                mode="paper_live",
                source_code=source_filter,
                run_tag=run_tag_filter,
            )
        finally:
            await close_arb_runtime(ctx)

        totals = summary.get("totals", {}) or {}
        by_strategy = summary.get("by_strategy", []) or []
        reasons: list[str] = []

        trades = int(totals.get("trades", 0) or 0)
        net_pnl = float(totals.get("evaluation_net_pnl_usd", totals.get("net_pnl_usd", 0.0)) or 0.0)
        win_rate = float(totals.get("win_rate", 0.0) or 0.0)

        if trades < min_trades:
            reasons.append(f"trades<{min_trades} (actual={trades})")
        if net_pnl < min_net_pnl_usd:
            reasons.append(f"evaluation_net_pnl_usd<{min_net_pnl_usd} (actual={net_pnl:.6f})")
        if win_rate < min_win_rate:
            reasons.append(f"win_rate<{min_win_rate:.3f} (actual={win_rate:.3f})")

        negative_strategies: list[str] = []
        for row in by_strategy:
            s_code = str(row.get("strategy_code", ""))
            s_trades = int(row.get("trades", 0) or 0)
            s_net = float(row.get("evaluation_net_pnl_usd", row.get("net_pnl_usd", 0.0)) or 0.0)
            if s_trades >= strategy_min_trades and s_net < 0:
                negative_strategies.append(f"{s_code}:{s_net:.6f}")
        if len(negative_strategies) > max_negative_strategies:
            reasons.append(
                f"negative_strategies>{max_negative_strategies} "
                f"(actual={len(negative_strategies)}; {', '.join(negative_strategies)})"
            )

        result = {
            "passed": len(reasons) == 0,
            "window_hours": since_hours,
            "source": source_filter or "all",
            "run_tag": run_tag_filter or "all",
            "thresholds": {
                "min_trades": min_trades,
                "min_net_pnl_usd": min_net_pnl_usd,
                "min_win_rate": min_win_rate,
                "max_negative_strategies": max_negative_strategies,
                "strategy_min_trades": strategy_min_trades,
            },
            "totals": totals,
            "negative_strategies": negative_strategies,
            "reasons": reasons,
        }
        typer.echo(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return 0 if result["passed"] else 2

    code = asyncio.run(_run())
    raise typer.Exit(code=code)


@app.command("arb-export")
def arb_export(
    table: Annotated[str, typer.Option("--table", help="arb table or view name")] = "arb_trade_result",
    export_format: Annotated[str, typer.Option("--format", help="csv or json")] = "csv",
    output: Annotated[str, typer.Option("--output", help="Target file path")] = "",
    limit: Annotated[int, typer.Option("--limit", min=1, max=1_000_000)] = 10000,
    since_hours: Annotated[int, typer.Option("--since-hours", min=0)] = 24,
) -> None:
    """Export Module2 tables and views."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    fmt = export_format.strip().lower()
    if fmt not in ("csv", "json"):
        raise typer.BadParameter("format must be csv or json")
    fmt_typed: ExportFormat = "csv" if fmt == "csv" else "json"
    out_path = Path(output).expanduser() if output else _default_export_path(table, fmt_typed)

    async def _run() -> None:
        ctx = await create_arb_runtime(settings)
        try:
            result = await ctx.reporter.export_table(
                table=table,
                export_format=fmt_typed,
                output_path=out_path,
                since_hours=since_hours if since_hours > 0 else None,
                limit=limit,
            )
            typer.echo(
                f"arb-export completed: table={result['table']} rows={result['rows']} "
                f"format={result['export_format']} file={result['output_path']}"
            )
        finally:
            await close_arb_runtime(ctx)

    asyncio.run(_run())


@app.command("arb-clean")
def arb_clean(
    mode: Annotated[str, typer.Option("--mode", help="all|shadow|paper_live|paper_replay|live")] = "all",
    source_code: Annotated[str, typer.Option("--source", help="source code, or all")] = "all",
    run_tag: Annotated[str, typer.Option("--run-tag", help="current|all|custom")] = "all",
    since_hours: Annotated[int, typer.Option("--since-hours", min=0, max=24 * 365)] = 0,
    include_position_lot: Annotated[bool, typer.Option("--include-position-lot/--no-include-position-lot")] = False,
    include_param_snapshot: Annotated[
        bool,
        typer.Option("--include-param-snapshot/--no-include-param-snapshot"),
    ] = False,
    apply: Annotated[bool, typer.Option("--apply/--dry-run")] = False,
) -> None:
    """Clean historical Module2 data to avoid old-version contamination."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    allowed_modes = {"all", "shadow", "paper_live", "paper_replay", "live"}
    mode_normalized = mode.strip().lower()
    if mode_normalized not in allowed_modes:
        raise typer.BadParameter("mode must be one of: all, shadow, paper_live, paper_replay, live")
    source_normalized = source_code.strip().lower()
    mode_filter = None if mode_normalized == "all" else mode_normalized
    source_filter = None if source_normalized == "all" else source_normalized

    async def _run() -> None:
        ctx = await create_arb_runtime(settings)
        try:
            run_tag_filter = await _resolve_run_tag_filter_db(
                ctx.db,
                run_tag,
                mode_filter=mode_filter,
                source_filter=source_filter,
                current_tag=ctx.orchestrator.config.run_tag,
            )
            since_start = datetime.now(tz=UTC) - timedelta(hours=since_hours) if since_hours > 0 else None

            counts = await _arb_cleanup_counts(
                ctx.db,
                mode_filter=mode_filter,
                source_filter=source_filter,
                run_tag_filter=run_tag_filter,
                since_start=since_start,
                include_position_lot=include_position_lot,
                include_param_snapshot=include_param_snapshot,
            )
            payload: dict[str, object] = {
                "mode": mode_filter or "all",
                "source_code": source_filter or "all",
                "run_tag": run_tag_filter or "all",
                "since_start": since_start,
                "include_position_lot": include_position_lot,
                "include_param_snapshot": include_param_snapshot,
                "counts": counts,
                "applied": apply,
            }
            if apply:
                deleted = await _arb_cleanup_apply(
                    ctx.db,
                    mode_filter=mode_filter,
                    source_filter=source_filter,
                    run_tag_filter=run_tag_filter,
                    since_start=since_start,
                    include_position_lot=include_position_lot,
                    include_param_snapshot=include_param_snapshot,
                )
                payload["deleted"] = deleted
            typer.echo(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        finally:
            await close_arb_runtime(ctx)

    asyncio.run(_run())


@app.command("arb-benchmark")
def arb_benchmark(
    mode: Annotated[str, typer.Option("--mode", help="shadow|paper_live|live")] = "paper_live",
    rounds: Annotated[int, typer.Option("--rounds", min=1, max=500)] = 20,
    warmup: Annotated[int, typer.Option("--warmup", min=0, max=100)] = 2,
    output: Annotated[str, typer.Option("--output", help="Optional output json path")] = "",
) -> None:
    """Run repeated Module2 cycles and print latency percentiles."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()
    run_mode = parse_run_mode(mode)
    out_path = Path(output).expanduser() if output else None

    async def _run() -> None:
        ctx = await create_arb_runtime(settings)
        try:
            if run_mode == RunMode.PAPER_LIVE:
                # 基准只评估扫描与执行链路，不把每日参数进化耗时混进结果。
                ctx.orchestrator._last_optimize_at = datetime.now(tz=UTC)
            for _ in range(warmup):
                await ctx.orchestrator.run_once(run_mode)

            rows: list[dict[str, int]] = []
            for _ in range(rounds):
                rows.append(await ctx.orchestrator.run_once(run_mode))

            report = _summarize_cycle_stats(rows)
            text = json.dumps(report, ensure_ascii=False, indent=2)
            typer.echo(text)
            if out_path:
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(text, encoding="utf-8")
                typer.echo(f"benchmark report saved: {out_path}")
        finally:
            await close_arb_runtime(ctx)

    asyncio.run(_run())


async def _watch_env_file(
    path: Path,
    reload_event: asyncio.Event,
    stop_event: asyncio.Event,
    poll_sec: int,
) -> None:
    try:
        last_mtime = path.stat().st_mtime if path.exists() else None
        while not stop_event.is_set():
            await asyncio.sleep(poll_sec)
            mtime = path.stat().st_mtime if path.exists() else None
            if mtime != last_mtime:
                last_mtime = mtime
                reload_event.set()
    except asyncio.CancelledError:
        raise


def _install_signal_handlers(stop_event: asyncio.Event, reload_event: asyncio.Event | None) -> None:
    try:
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, stop_event.set)
        loop.add_signal_handler(signal.SIGTERM, stop_event.set)
        if reload_event and hasattr(signal, "SIGHUP"):
            loop.add_signal_handler(signal.SIGHUP, reload_event.set)
    except NotImplementedError:
        return


async def _arb_cleanup_counts(
    db: Database,
    *,
    mode_filter: str | None,
    source_filter: str | None,
    run_tag_filter: str | None,
    since_start: datetime | None,
    include_position_lot: bool,
    include_param_snapshot: bool,
) -> dict[str, int]:
    signal_params = (
        mode_filter,
        mode_filter,
        source_filter,
        source_filter,
        run_tag_filter,
        run_tag_filter,
        since_start,
        since_start,
    )
    replay_params = (
        mode_filter,
        mode_filter,
        source_filter,
        source_filter,
        run_tag_filter,
        run_tag_filter,
        since_start,
        since_start,
    )
    counts: dict[str, int] = {}

    def _as_int(row: dict[str, object] | None) -> int:
        if not row:
            return 0
        return int(row.get("c", 0) or 0)

    counts["arb_signal"] = _as_int(
        await db.fetch_one(
            """
            select count(*) as c
            from arb_signal
            where (%s::text is null or mode = %s::text)
              and (%s::text is null or source_code = %s::text)
              and (%s::text is null or coalesce(features->>'run_tag', '') = %s::text)
              and (%s::timestamptz is null or created_at >= %s::timestamptz)
            """,
            signal_params,
        )
    )
    counts["arb_order_intent"] = _as_int(
        await db.fetch_one(
            """
            select count(*) as c
            from arb_order_intent i
            join arb_signal s on s.signal_id = i.signal_id
            where (%s::text is null or s.mode = %s::text)
              and (%s::text is null or s.source_code = %s::text)
              and (%s::text is null or coalesce(s.features->>'run_tag', '') = %s::text)
              and (%s::timestamptz is null or s.created_at >= %s::timestamptz)
            """,
            signal_params,
        )
    )
    counts["arb_order_event"] = _as_int(
        await db.fetch_one(
            """
            select count(*) as c
            from arb_order_event e
            join arb_order_intent i on i.intent_id = e.intent_id
            join arb_signal s on s.signal_id = i.signal_id
            where (%s::text is null or s.mode = %s::text)
              and (%s::text is null or s.source_code = %s::text)
              and (%s::text is null or coalesce(s.features->>'run_tag', '') = %s::text)
              and (%s::timestamptz is null or s.created_at >= %s::timestamptz)
            """,
            signal_params,
        )
    )
    counts["arb_fill"] = _as_int(
        await db.fetch_one(
            """
            select count(*) as c
            from arb_fill f
            join arb_order_intent i on i.intent_id = f.intent_id
            join arb_signal s on s.signal_id = i.signal_id
            where (%s::text is null or s.mode = %s::text)
              and (%s::text is null or s.source_code = %s::text)
              and (%s::text is null or coalesce(s.features->>'run_tag', '') = %s::text)
              and (%s::timestamptz is null or s.created_at >= %s::timestamptz)
            """,
            signal_params,
        )
    )
    counts["arb_trade_result"] = _as_int(
        await db.fetch_one(
            """
            select count(*) as c
            from arb_trade_result
            where (%s::text is null or mode = %s::text)
              and (%s::text is null or source_code = %s::text)
              and (%s::text is null or coalesce(metadata->>'run_tag', '') = %s::text)
              and (%s::timestamptz is null or created_at >= %s::timestamptz)
            """,
            signal_params,
        )
    )
    counts["arb_cash_ledger"] = _as_int(
        await db.fetch_one(
            """
            select count(*) as c
            from arb_cash_ledger
            where (%s::text is null or mode = %s::text)
              and (%s::text is null or source_code = %s::text)
              and (%s::text is null or coalesce(payload->>'run_tag', '') = %s::text)
              and (%s::timestamptz is null or created_at >= %s::timestamptz)
            """,
            signal_params,
        )
    )
    counts["arb_risk_event"] = _as_int(
        await db.fetch_one(
            """
            select count(*) as c
            from arb_risk_event
            where (%s::text is null or mode = %s::text)
              and (%s::text is null or source_code = %s::text)
              and (%s::text is null or coalesce(payload->>'run_tag', '') = %s::text)
              and (%s::timestamptz is null or created_at >= %s::timestamptz)
            """,
            signal_params,
        )
    )
    counts["arb_replay_run"] = _as_int(
        await db.fetch_one(
            """
            select count(*) as c
            from arb_replay_run
            where (%s::text is null or mode = %s::text)
              and (%s::text is null or coalesce(metadata->>'source_code', '') = %s::text)
              and (%s::text is null or coalesce(metadata->>'run_tag', '') = %s::text)
              and (%s::timestamptz is null or started_at >= %s::timestamptz)
            """,
            replay_params,
        )
    )
    counts["arb_replay_metric"] = _as_int(
        await db.fetch_one(
            """
            select count(*) as c
            from arb_replay_metric m
            join arb_replay_run r on r.replay_run_id = m.replay_run_id
            where (%s::text is null or r.mode = %s::text)
              and (%s::text is null or coalesce(r.metadata->>'source_code', '') = %s::text)
              and (%s::text is null or coalesce(r.metadata->>'run_tag', '') = %s::text)
              and (%s::timestamptz is null or r.started_at >= %s::timestamptz)
            """,
            replay_params,
        )
    )
    if include_position_lot:
        counts["arb_position_lot"] = _as_int(
            await db.fetch_one(
                """
                select count(*) as c
                from arb_position_lot
                where (%s::text is null or mode = %s::text)
                  and (%s::text is null or source_code = %s::text)
                  and (%s::text is null or coalesce(run_tag, '') = %s::text)
                  and (%s::timestamptz is null or opened_at >= %s::timestamptz)
                """,
                signal_params,
            )
        )
    if include_param_snapshot:
        counts["arb_param_snapshot"] = _as_int(
            await db.fetch_one(
                """
                select count(*) as c
                from arb_param_snapshot
                where (%s::text is null or coalesce(score_breakdown->>'run_tag', '') = %s::text)
                  and (%s::timestamptz is null or created_at >= %s::timestamptz)
                """,
                (run_tag_filter, run_tag_filter, since_start, since_start),
            )
        )
    return counts


async def _arb_cleanup_apply(
    db: Database,
    *,
    mode_filter: str | None,
    source_filter: str | None,
    run_tag_filter: str | None,
    since_start: datetime | None,
    include_position_lot: bool,
    include_param_snapshot: bool,
) -> dict[str, int]:
    signal_params = (
        mode_filter,
        mode_filter,
        source_filter,
        source_filter,
        run_tag_filter,
        run_tag_filter,
        since_start,
        since_start,
    )
    replay_params = (
        mode_filter,
        mode_filter,
        source_filter,
        source_filter,
        run_tag_filter,
        run_tag_filter,
        since_start,
        since_start,
    )
    deleted = await _arb_cleanup_counts(
        db,
        mode_filter=mode_filter,
        source_filter=source_filter,
        run_tag_filter=run_tag_filter,
        since_start=since_start,
        include_position_lot=include_position_lot,
        include_param_snapshot=include_param_snapshot,
    )

    await db.execute(
        """
        delete from arb_replay_run
        where (%s::text is null or mode = %s::text)
          and (%s::text is null or coalesce(metadata->>'source_code', '') = %s::text)
          and (%s::text is null or coalesce(metadata->>'run_tag', '') = %s::text)
          and (%s::timestamptz is null or started_at >= %s::timestamptz)
        """,
        replay_params,
    )

    if include_param_snapshot:
        await db.execute(
            """
            delete from arb_param_snapshot
            where (%s::text is null or coalesce(score_breakdown->>'run_tag', '') = %s::text)
              and (%s::timestamptz is null or created_at >= %s::timestamptz)
            """,
            (run_tag_filter, run_tag_filter, since_start, since_start),
        )
    if include_position_lot:
        await db.execute(
            """
            delete from arb_position_lot
            where (%s::text is null or mode = %s::text)
              and (%s::text is null or source_code = %s::text)
              and (%s::text is null or coalesce(run_tag, '') = %s::text)
              and (%s::timestamptz is null or opened_at >= %s::timestamptz)
            """,
            signal_params,
        )

    await db.execute(
        """
        delete from arb_risk_event
        where (%s::text is null or mode = %s::text)
          and (%s::text is null or source_code = %s::text)
          and (%s::text is null or coalesce(payload->>'run_tag', '') = %s::text)
          and (%s::timestamptz is null or created_at >= %s::timestamptz)
        """,
        signal_params,
    )
    await db.execute(
        """
        delete from arb_cash_ledger
        where (%s::text is null or mode = %s::text)
          and (%s::text is null or source_code = %s::text)
          and (%s::text is null or coalesce(payload->>'run_tag', '') = %s::text)
          and (%s::timestamptz is null or created_at >= %s::timestamptz)
        """,
        signal_params,
    )
    await db.execute(
        """
        delete from arb_trade_result
        where (%s::text is null or mode = %s::text)
          and (%s::text is null or source_code = %s::text)
          and (%s::text is null or coalesce(metadata->>'run_tag', '') = %s::text)
          and (%s::timestamptz is null or created_at >= %s::timestamptz)
        """,
        signal_params,
    )
    await db.execute(
        """
        delete from arb_signal
        where (%s::text is null or mode = %s::text)
          and (%s::text is null or source_code = %s::text)
          and (%s::text is null or coalesce(features->>'run_tag', '') = %s::text)
          and (%s::timestamptz is null or created_at >= %s::timestamptz)
        """,
        signal_params,
    )

    return deleted


async def _build_arb_doctor_report(
    ctx: ArbRuntimeContext,
    mode: RunMode,
    source_code: str,
) -> dict[str, object]:
    config = ctx.orchestrator.config
    snapshots = await ctx.orchestrator._load_live_snapshots()
    now = datetime.now(tz=UTC)

    market_hours: dict[str, float] = {}
    min_order_notionals: list[float] = []
    for snapshot in snapshots:
        if snapshot.market_end:
            market_hours.setdefault(snapshot.market_id, (snapshot.market_end - now).total_seconds() / 3600.0)
        if snapshot.best_ask is not None and snapshot.min_order_size is not None:
            required = float(snapshot.best_ask) * float(snapshot.min_order_size)
            if required > 0:
                min_order_notionals.append(required)

    def _hours_count(limit: float) -> int:
        return sum(1 for value in market_hours.values() if value <= limit)

    raw_strategy_counts: dict[str, int] = {}
    strategy_scanners = (
        ("A", config.enable_strategy_a, ctx.orchestrator.strategy_a),
        ("B", config.enable_strategy_b, ctx.orchestrator.strategy_b),
        ("C", config.enable_strategy_c, ctx.orchestrator.strategy_c),
        ("F", config.enable_strategy_f, ctx.orchestrator.strategy_f),
        ("G", config.enable_strategy_g, ctx.orchestrator.strategy_g),
    )
    for code, enabled, scanner in strategy_scanners:
        if not enabled:
            continue
        raw_strategy_counts[code] = len(scanner.scan(mode, source_code, snapshots))

    active_signals = ctx.orchestrator._scan_all(mode, source_code, snapshots)
    active_counts: Counter[str] = Counter(signal.strategy_code.value for signal in active_signals)

    allow_counts: Counter[str] = Counter()
    reject_by_strategy: dict[str, Counter[str]] = {}
    state_cache: dict[str, object] = {}
    for signal in active_signals:
        if signal.is_expired():
            bucket = reject_by_strategy.setdefault(signal.strategy_code.value, Counter())
            bucket["expired"] += 1
            continue
        scope_key = ctx.orchestrator._state_scope_key(signal)
        runtime_state = state_cache.get(scope_key)
        if runtime_state is None:
            runtime_state = await ctx.orchestrator.risk_gate.load_state(
                signal.mode,
                signal.source_code,
                strategy_code=signal.strategy_code if ctx.orchestrator._paper_strategy_split(signal.mode) else None,
            )
            state_cache[scope_key] = runtime_state
        plan = ctx.orchestrator._build_plan(signal)
        notional = sum(float(intent.notional_usd or 0.0) for intent in plan.intents)
        decision = await ctx.orchestrator.risk_gate.assess(signal, notional, state=runtime_state)
        if decision.allowed:
            allow_counts[signal.strategy_code.value] += 1
        else:
            bucket = reject_by_strategy.setdefault(signal.strategy_code.value, Counter())
            bucket[decision.reason] += 1

    p50_min_order = _pct(min_order_notionals, 50) if min_order_notionals else 0.0
    p90_min_order = _pct(min_order_notionals, 90) if min_order_notionals else 0.0
    suggestions: list[str] = []
    if p50_min_order > 0 and config.single_risk_usd < p50_min_order:
        suggestions.append(
            f"single_risk_usd({config.single_risk_usd:.3f}) 低于 p50(min_order_notional={p50_min_order:.3f})，会导致大量风险门拒绝"
        )
    if config.max_exposure_usd < (3.0 * config.single_risk_usd):
        suggestions.append(
            f"max_exposure_usd({config.max_exposure_usd:.3f}) < 3 * single_risk_usd({config.single_risk_usd:.3f})，并发空间过窄"
        )
    if config.safe_arbitrage_only and (config.enable_strategy_f or config.enable_strategy_g):
        suggestions.append("safe_arbitrage_only=true 会在非 shadow 模式过滤 F/G")
    if _hours_count(12.0) == 0 and (config.enable_strategy_f or config.enable_strategy_g):
        suggestions.append("当前 <=12h 到期市场为 0，F/G 很难触发")

    likely_zero_signal = (sum(active_counts.values()) == 0) or (sum(allow_counts.values()) == 0)
    rejection_top = {
        strategy: dict(sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:5])
        for strategy, counter in reject_by_strategy.items()
    }
    return {
        "mode": mode.value,
        "source_code": source_code,
        "run_tag": config.run_tag,
        "execution_backend": config.execution_backend,
        "snapshot": {
            "token_count": len(snapshots),
            "market_count": len(market_hours),
            "market_hours_le_4": _hours_count(4.0),
            "market_hours_le_12": _hours_count(12.0),
            "market_hours_le_72": _hours_count(72.0),
            "min_order_notional_p50": round(p50_min_order, 6),
            "min_order_notional_p90": round(p90_min_order, 6),
            "metrics": dict(ctx.orchestrator._last_snapshot_metrics),
        },
        "raw_signal_counts": dict(raw_strategy_counts),
        "active_signal_counts": dict(active_counts),
        "risk_allowed_counts": dict(allow_counts),
        "risk_reject_topn": rejection_top,
        "checks": {
            "single_risk_usd": config.single_risk_usd,
            "max_exposure_usd": config.max_exposure_usd,
            "daily_stop_loss_usd": config.daily_stop_loss_usd,
            "safe_arbitrage_only": config.safe_arbitrage_only,
            "single_risk_meets_p50_min_order": config.single_risk_usd >= p50_min_order if p50_min_order > 0 else True,
            "exposure_ge_3x_single_risk": config.max_exposure_usd >= (3.0 * config.single_risk_usd),
        },
        "likely_zero_signal": likely_zero_signal,
        "suggestions": suggestions,
    }


def _default_export_path(table: str, export_format: ExportFormat) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "csv" if export_format == "csv" else "json"
    return Path("exports") / f"{table.strip().lower()}_{stamp}.{suffix}"


def _pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    idx = int(round((p / 100.0) * (len(sorted_values) - 1)))
    return float(sorted_values[idx])


def _summarize_cycle_stats(rows: list[dict[str, int]]) -> dict[str, object]:
    metric_keys = [
        "signals",
        "processed",
        "executed",
        "snapshot_load_ms",
        "scan_ms",
        "process_ms",
        "total_ms",
    ]
    summary: dict[str, object] = {
        "rounds": len(rows),
        "generated_at": datetime.now().isoformat(),
    }
    for key in metric_keys:
        values = [float(row.get(key, 0)) for row in rows]
        summary[key] = {
            "avg": round(fmean(values), 2) if values else 0.0,
            "p50": round(_pct(values, 50), 2),
            "p95": round(_pct(values, 95), 2),
            "max": round(max(values), 2) if values else 0.0,
        }
    return summary


def _run_pyo3_t2t_benchmark(payload_json: str) -> dict[str, object]:
    try:
        module = importlib.import_module("polaris_rs")
    except Exception:
        return {"error": "pyo3_module_not_found", "module": "polaris_rs"}

    try:
        raw = module.bench_orderbook(payload_json)
    except Exception as exc:
        return {
            "error": "pyo3_benchmark_failed",
            "message": str(exc)[:400],
        }

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {"error": "pyo3_output_invalid_json"}

    if not isinstance(parsed, dict):
        return {"error": "pyo3_output_invalid_type"}
    return parsed


@app.command("arb-benchmark-t2t")
def arb_benchmark_t2t(
    backend: Annotated[str, typer.Option("--backend", help="python|rust|pyo3|both|all")] = "both",
    input_path: Annotated[str, typer.Option("--input", help="Optional JSON payload path")] = "",
    output: Annotated[str, typer.Option("--output", help="Optional output json path")] = "",
    iterations: Annotated[int, typer.Option("--iterations", min=1, max=500)] = 120,
    updates: Annotated[int, typer.Option("--updates", min=100, max=5000)] = 1000,
    levels_per_side: Annotated[int, typer.Option("--levels-per-side", min=50, max=2000)] = 250,
    seed: Annotated[int, typer.Option("--seed")] = 42,
) -> None:
    """Benchmark tick-to-trade hot path: decode -> orderbook update -> spread/depth."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    normalized = backend.strip().lower()
    if normalized not in {"python", "rust", "pyo3", "both", "all"}:
        raise typer.BadParameter("backend must be python|rust|pyo3|both|all")

    if input_path:
        raw = Path(input_path).expanduser().read_text(encoding="utf-8")
        payload = load_payload_json(raw)
        payload["iterations"] = iterations
    else:
        payload = generate_t2t_payload(
            levels_per_side=levels_per_side,
            updates=updates,
            iterations=iterations,
            seed=seed,
        )

    payload_json = dump_payload_json(payload)
    report: dict[str, object] = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "payload": {
            "iterations": int(payload.get("iterations", 0) or 0),
            "updates": len(payload.get("updates", [])) if isinstance(payload.get("updates"), list) else 0,
            "levels_per_side": len(payload.get("initial_bids", [])) if isinstance(payload.get("initial_bids"), list) else 0,
        },
        "results": {},
    }

    if normalized in {"python", "both", "all"}:
        report["results"]["python"] = run_python_t2t_benchmark(payload)

    if normalized in {"rust", "both", "all"}:
        rust_bin = settings.arb_rust_bridge_bin
        try:
            completed = subprocess.run(
                [rust_bin, "bench-orderbook"],
                input=payload_json,
                text=True,
                check=False,
                capture_output=True,
                timeout=max(10, iterations * 2),
            )
        except FileNotFoundError:
            report["results"]["rust"] = {"error": "rust_binary_not_found", "binary": rust_bin}
        else:
            if completed.returncode != 0:
                report["results"]["rust"] = {
                    "error": "rust_benchmark_failed",
                    "binary": rust_bin,
                    "returncode": completed.returncode,
                    "stderr": completed.stderr.strip()[:400],
                }
            else:
                try:
                    report["results"]["rust"] = json.loads(completed.stdout)
                except json.JSONDecodeError:
                    report["results"]["rust"] = {
                        "error": "rust_output_invalid_json",
                        "binary": rust_bin,
                    }

    if normalized in {"pyo3", "all"}:
        report["results"]["pyo3"] = _run_pyo3_t2t_benchmark(payload_json)

    if "python" in report["results"] and "rust" in report["results"]:
        py = report["results"]["python"]
        ru = report["results"]["rust"]
        if isinstance(py, dict) and isinstance(ru, dict):
            py_avg = float(py.get("avg_update_us", 0.0) or 0.0)
            ru_avg = float(ru.get("avg_update_us", 0.0) or 0.0)
            if py_avg > 0 and ru_avg > 0:
                report["comparison"] = {
                    "python_avg_update_us": round(py_avg, 6),
                    "rust_avg_update_us": round(ru_avg, 6),
                    "rust_vs_python_speedup": round(py_avg / ru_avg, 6),
                }

    if "python" in report["results"] and "pyo3" in report["results"]:
        py = report["results"]["python"]
        pyo3 = report["results"]["pyo3"]
        if isinstance(py, dict) and isinstance(pyo3, dict):
            py_avg = float(py.get("avg_update_us", 0.0) or 0.0)
            pyo3_avg = float(pyo3.get("avg_update_us", 0.0) or 0.0)
            if py_avg > 0 and pyo3_avg > 0:
                report.setdefault("comparison", {})
                report["comparison"]["pyo3_avg_update_us"] = round(pyo3_avg, 6)
                report["comparison"]["pyo3_vs_python_speedup"] = round(py_avg / pyo3_avg, 6)

    if "rust" in report["results"] and "pyo3" in report["results"]:
        ru = report["results"]["rust"]
        pyo3 = report["results"]["pyo3"]
        if isinstance(ru, dict) and isinstance(pyo3, dict):
            ru_avg = float(ru.get("avg_update_us", 0.0) or 0.0)
            pyo3_avg = float(pyo3.get("avg_update_us", 0.0) or 0.0)
            if ru_avg > 0 and pyo3_avg > 0:
                report.setdefault("comparison", {})
                report["comparison"]["pyo3_vs_rust_speedup"] = round(ru_avg / pyo3_avg, 6)

    text = json.dumps(report, ensure_ascii=False, indent=2)
    typer.echo(text)
    if output:
        out_path = Path(output).expanduser()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text, encoding="utf-8")
        typer.echo(f"t2t benchmark report saved: {out_path}")


if __name__ == "__main__":
    app()
