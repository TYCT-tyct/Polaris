from __future__ import annotations

import asyncio
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


async def create_runtime(settings: PolarisSettings) -> RuntimeContext:
    db = Database(settings.database_url)
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
    db = Database(settings.database_url)
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
    value = (raw or "").strip().lower()
    if not value or value == "current":
        return current_tag
    if value == "all":
        return None
    return _sanitize_run_tag(raw)


@app.command("migrate")
def migrate() -> None:
    """Apply SQL migrations."""
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    async def _run() -> None:
        db = Database(settings.database_url)
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
        db = Database(settings.database_url)
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
        db = Database(settings.database_url)
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


@app.command("arb-paper-matrix-start")
def arb_paper_matrix_start(
    duration_hours: Annotated[int, typer.Option("--duration-hours", min=1, max=72)] = 8,
    source_prefix: Annotated[str, typer.Option("--source-prefix")] = "polymarket",
    bankroll_usd: Annotated[float, typer.Option("--bankroll-usd", min=1.0)] = 10.0,
    include_c: Annotated[bool, typer.Option("--include-c/--no-include-c")] = True,
) -> None:
    """Start two background paper runs:
    1) shared capital pool (all strategies share one bankroll)
    2) isolated capital pool (each strategy has its own bankroll)
    """
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)

    strategies = ["A", "B", "F", "G"] + (["C"] if include_c else [])
    timestamp = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
    logs_dir = Path("logs")
    logs_dir.mkdir(parents=True, exist_ok=True)
    run_dir = Path("run")
    run_dir.mkdir(parents=True, exist_ok=True)

    def _spawn(scope: str) -> tuple[int, Path, str]:
        source = f"{source_prefix}_{scope}10".lower()
        log_path = logs_dir / f"arb_paper_{scope}_{timestamp}.log"
        env = os.environ.copy()
        env["POLARIS_ARB_PAPER_INITIAL_BANKROLL_USD"] = f"{bankroll_usd:.4f}"
        env["POLARIS_ARB_PAPER_SPLIT_BY_STRATEGY"] = "true" if scope == "isolated" else "false"
        env["POLARIS_ARB_ENABLE_STRATEGY_A"] = "true" if "A" in strategies else "false"
        env["POLARIS_ARB_ENABLE_STRATEGY_B"] = "true" if "B" in strategies else "false"
        env["POLARIS_ARB_ENABLE_STRATEGY_C"] = "true" if "C" in strategies else "false"
        env["POLARIS_ARB_ENABLE_STRATEGY_F"] = "true" if "F" in strategies else "false"
        env["POLARIS_ARB_ENABLE_STRATEGY_G"] = "true" if "G" in strategies else "false"

        cmd: list[str] = [
            sys.executable,
            "-m",
            "polaris.cli",
            "arb-run",
            "--mode",
            "paper_live",
            "--source",
            source,
            "--paper-capital-scope",
            "strategy" if scope == "isolated" else "shared",
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
        (run_dir / f"arb_paper_{scope}.pid").write_text(str(proc.pid), encoding="utf-8")
        return proc.pid, log_path, source

    shared_pid, shared_log, shared_source = _spawn("shared")
    isolated_pid, isolated_log, isolated_source = _spawn("isolated")

    typer.echo("arb-paper-matrix started")
    typer.echo(f"shared:   pid={shared_pid} source={shared_source} log={shared_log}")
    typer.echo(f"isolated: pid={isolated_pid} source={isolated_source} log={isolated_log}")


@app.command("arb-paper-matrix-stop")
def arb_paper_matrix_stop() -> None:
    """Stop background matrix paper runs if pid files exist."""
    run_dir = Path("run")
    stopped = 0
    for scope in ("shared", "isolated"):
        pid_file = run_dir / f"arb_paper_{scope}.pid"
        if not pid_file.exists():
            continue
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
            stats = await ctx.orchestrator.run_replay(start_ts, end_ts, fast=fast)
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
            run_tag_filter = _resolve_run_tag_filter(run_tag, ctx.orchestrator.config.run_tag)
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
            run_tag_filter = _resolve_run_tag_filter(run_tag, ctx.orchestrator.config.run_tag)
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
            run_tag_filter = _resolve_run_tag_filter(run_tag, ctx.orchestrator.config.run_tag)
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
        net_pnl = float(totals.get("net_pnl_usd", 0.0) or 0.0)
        win_rate = float(totals.get("win_rate", 0.0) or 0.0)

        if trades < min_trades:
            reasons.append(f"trades<{min_trades} (actual={trades})")
        if net_pnl < min_net_pnl_usd:
            reasons.append(f"net_pnl_usd<{min_net_pnl_usd} (actual={net_pnl:.6f})")
        if win_rate < min_win_rate:
            reasons.append(f"win_rate<{min_win_rate:.3f} (actual={win_rate:.3f})")

        negative_strategies: list[str] = []
        for row in by_strategy:
            s_code = str(row.get("strategy_code", ""))
            s_trades = int(row.get("trades", 0) or 0)
            s_net = float(row.get("net_pnl_usd", 0.0) or 0.0)
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
            run_tag_filter = _resolve_run_tag_filter(run_tag, ctx.orchestrator.config.run_tag)
            since_start = datetime.now(tz=UTC) - timedelta(hours=since_hours) if since_hours > 0 else None

            counts = await _arb_cleanup_counts(
                ctx.db,
                mode_filter=mode_filter,
                source_filter=source_filter,
                run_tag_filter=run_tag_filter,
                since_start=since_start,
            )
            payload: dict[str, object] = {
                "mode": mode_filter or "all",
                "source_code": source_filter or "all",
                "run_tag": run_tag_filter or "all",
                "since_start": since_start,
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
) -> dict[str, int]:
    params = (
        mode_filter,
        mode_filter,
        source_filter,
        source_filter,
        run_tag_filter,
        run_tag_filter,
        since_start,
        since_start,
    )
    tables = {
        "arb_signal": "coalesce(features->>'run_tag', '')",
        "arb_trade_result": "coalesce(metadata->>'run_tag', '')",
        "arb_cash_ledger": "coalesce(payload->>'run_tag', '')",
        "arb_risk_event": "coalesce(payload->>'run_tag', '')",
        "arb_replay_run": "coalesce(metadata->>'run_tag', '')",
    }
    counts: dict[str, int] = {}
    for table, run_tag_expr in tables.items():
        row = await db.fetch_one(
            f"""
            select count(*) as c
            from {table}
            where (%s::text is null or mode = %s::text)
              and (%s::text is null or source_code = %s::text)
              and (%s::text is null or {run_tag_expr} = %s::text)
              and (%s::timestamptz is null or created_at >= %s::timestamptz)
            """,
            params,
        )
        counts[table] = int(row["c"] or 0) if row else 0

    replay_metric_row = await db.fetch_one(
        """
        select count(*) as c
        from arb_replay_metric m
        join arb_replay_run r on r.replay_run_id = m.replay_run_id
        where (%s::text is null or r.mode = %s::text)
          and (%s::text is null or coalesce(r.metadata->>'source_code', '') = %s::text)
          and (%s::text is null or coalesce(r.metadata->>'run_tag', '') = %s::text)
          and (%s::timestamptz is null or r.started_at >= %s::timestamptz)
        """,
        params,
    )
    counts["arb_replay_metric"] = int(replay_metric_row["c"] or 0) if replay_metric_row else 0
    return counts


async def _arb_cleanup_apply(
    db: Database,
    *,
    mode_filter: str | None,
    source_filter: str | None,
    run_tag_filter: str | None,
    since_start: datetime | None,
) -> dict[str, int]:
    params = (
        mode_filter,
        mode_filter,
        source_filter,
        source_filter,
        run_tag_filter,
        run_tag_filter,
        since_start,
        since_start,
    )
    deleted: dict[str, int] = {}

    replay_runs = await db.fetch_all(
        """
        select replay_run_id
        from arb_replay_run
        where (%s::text is null or mode = %s::text)
          and (%s::text is null or coalesce(metadata->>'source_code', '') = %s::text)
          and (%s::text is null or coalesce(metadata->>'run_tag', '') = %s::text)
          and (%s::timestamptz is null or started_at >= %s::timestamptz)
        """,
        params,
    )
    replay_ids = [row["replay_run_id"] for row in replay_runs]
    if replay_ids:
        await db.execute("delete from arb_replay_run where replay_run_id = any(%s)", (replay_ids,))
    deleted["arb_replay_run"] = len(replay_ids)

    row = await db.fetch_one(
        """
        with gone as (
            delete from arb_param_snapshot
            where (%s::text is null or coalesce(score_breakdown->>'run_tag', '') = %s::text)
              and (%s::timestamptz is null or created_at >= %s::timestamptz)
            returning 1
        )
        select count(*) as c from gone
        """,
        (run_tag_filter, run_tag_filter, since_start, since_start),
    )
    deleted["arb_param_snapshot"] = int(row["c"] or 0) if row else 0

    for table, run_tag_expr in (
        ("arb_risk_event", "coalesce(payload->>'run_tag', '')"),
        ("arb_cash_ledger", "coalesce(payload->>'run_tag', '')"),
        ("arb_trade_result", "coalesce(metadata->>'run_tag', '')"),
        ("arb_signal", "coalesce(features->>'run_tag', '')"),
    ):
        row = await db.fetch_one(
            f"""
            with gone as (
                delete from {table}
                where (%s::text is null or mode = %s::text)
                  and (%s::text is null or source_code = %s::text)
                  and (%s::text is null or {run_tag_expr} = %s::text)
                  and (%s::timestamptz is null or created_at >= %s::timestamptz)
                returning 1
            )
            select count(*) as c from gone
            """,
            params,
        )
        deleted[table] = int(row["c"] or 0) if row else 0

    return deleted


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
