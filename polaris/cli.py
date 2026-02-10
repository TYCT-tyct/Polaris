from __future__ import annotations

import asyncio
import json
import signal
import subprocess
import sys
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from polaris.arb.ai.gate import AiGate
from polaris.arb.cli import parse_iso_datetime, parse_run_mode
from polaris.arb.config import arb_config_from_settings
from polaris.arb.orchestrator import ArbOrchestrator
from polaris.arb.reporting import ArbReporter
from polaris.config import PolarisSettings, load_settings, refresh_process_env_from_file
from polaris.db.pool import Database
from polaris.harvest.collector_markets import MarketCollector
from polaris.harvest.collector_quotes import QuoteCollector
from polaris.harvest.collector_tweets import TweetCollector
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
    )
    market_collector = MarketCollector(db, gamma, market_scope=settings.market_discovery_scope)
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
    await ctx.clob.close()
    await ctx.db.close()


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
    refresh_process_env_from_file()
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
                refresh_process_env_from_file()
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

            markets, _ = await ctx.market_collector.run_once()
            checks.append(f"gamma_markets:ok count={markets}")

            tokens = await ctx.market_collector.list_active_tokens()
            if tokens:
                _ = await ctx.clob.get_book(tokens[0]["token_id"])
                checks.append("clob_book:ok")
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
    refresh_process_env_from_file()
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
    refresh_process_env_from_file()
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
    once: Annotated[bool, typer.Option("--once/--loop", help="Run one scan cycle or keep running")] = False,
) -> None:
    """Run Module2 arbitrage engine."""
    refresh_process_env_from_file()
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()
    run_mode = parse_run_mode(mode)

    async def _run() -> None:
        ctx = await create_arb_runtime(settings)
        try:
            if once:
                stats = await ctx.orchestrator.run_once(run_mode)
                typer.echo(f"arb-run once completed: {stats}")
            else:
                await ctx.orchestrator.run_forever(run_mode)
        finally:
            await close_arb_runtime(ctx)

    asyncio.run(_run())


@app.command("arb-replay")
def arb_replay(
    start: Annotated[str, typer.Option("--start", help="ISO datetime")] = "",
    end: Annotated[str, typer.Option("--end", help="ISO datetime")] = "",
) -> None:
    """Run historical replay with real captured market data."""
    if not start or not end:
        raise typer.BadParameter("start and end are required")
    refresh_process_env_from_file()
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()
    start_ts = parse_iso_datetime(start)
    end_ts = parse_iso_datetime(end)

    async def _run() -> None:
        ctx = await create_arb_runtime(settings)
        try:
            stats = await ctx.orchestrator.run_replay(start_ts, end_ts)
            typer.echo(f"arb-replay completed: {stats}")
        finally:
            await close_arb_runtime(ctx)

    asyncio.run(_run())


@app.command("arb-optimize")
def arb_optimize() -> None:
    """Run one parameter evolution cycle for Module2."""
    refresh_process_env_from_file()
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
) -> None:
    """Show aggregated performance report for Module2."""
    refresh_process_env_from_file()
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    async def _run() -> None:
        ctx = await create_arb_runtime(settings)
        try:
            rows = await ctx.reporter.report(group_by=group_by)
            typer.echo(json.dumps(rows, ensure_ascii=False, indent=2, default=str))
        finally:
            await close_arb_runtime(ctx)

    asyncio.run(_run())


@app.command("arb-export")
def arb_export(
    table: Annotated[str, typer.Option("--table", help="arb table or view name")] = "arb_trade_result",
    export_format: Annotated[str, typer.Option("--format", help="csv or json")] = "csv",
    output: Annotated[str, typer.Option("--output", help="Target file path")] = "",
    limit: Annotated[int, typer.Option("--limit", min=1, max=1_000_000)] = 10000,
    since_hours: Annotated[int, typer.Option("--since-hours", min=0)] = 24,
) -> None:
    """Export Module2 tables and views."""
    refresh_process_env_from_file()
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


def _default_export_path(table: str, export_format: ExportFormat) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "csv" if export_format == "csv" else "json"
    return Path("exports") / f"{table.strip().lower()}_{stamp}.{suffix}"


if __name__ == "__main__":
    app()
