from __future__ import annotations

import asyncio
from collections import Counter
import importlib
import json
import math
import re
import signal
import subprocess
import sys
import os
from uuid import uuid4
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
from polaris.arb.experiments import ExperimentScoreInput, compute_experiment_score, parse_experiment_dimensions
from polaris.arb.orchestrator import ArbOrchestrator
from polaris.arb.reporting import ArbReporter
from polaris.config import PolarisSettings, load_settings, refresh_process_env_from_file
from polaris.core.module4 import Module4Service, expected_calibration_error, multiclass_brier
from polaris.core.module4.buckets import infer_true_bucket_label
from polaris.core.module4.family import aggregate_market_family_rows
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

# 并行实验的标准层级（Strict / Balanced / Aggressive）
_PAPER_PROFILE_PRESETS["strict_50"] = dict(_PAPER_PROFILE_PRESETS["conservative_50"])
_PAPER_PROFILE_PRESETS["strict_50"].update(
    {
        "POLARIS_ARB_MAX_SIGNALS_PER_CYCLE": "2",
        "POLARIS_ARB_G_MIN_CONFIDENCE": "0.97",
        "POLARIS_ARB_G_MIN_EXPECTED_EDGE_PCT": "0.06",
        "POLARIS_ARB_G_MAX_HOURS_TO_RESOLVE": "8",
    }
)
_PAPER_PROFILE_PRESETS["balanced_50"] = dict(_PAPER_PROFILE_PRESETS["trigger_safe_50_v2"])
_PAPER_PROFILE_PRESETS["balanced_50"].update(
    {
        "POLARIS_ARB_MAX_SIGNALS_PER_CYCLE": "4",
        "POLARIS_ARB_G_MIN_CONFIDENCE": "0.95",
        "POLARIS_ARB_G_MIN_EXPECTED_EDGE_PCT": "0.05",
        "POLARIS_ARB_G_MAX_HOURS_TO_RESOLVE": "12",
    }
)
_PAPER_PROFILE_PRESETS["aggressive_50"] = dict(_PAPER_PROFILE_PRESETS["trigger_safe_50"])
_PAPER_PROFILE_PRESETS["aggressive_50"].update(
    {
        "POLARIS_ARB_MAX_SIGNALS_PER_CYCLE": "8",
        "POLARIS_ARB_G_MIN_CONFIDENCE": "0.90",
        "POLARIS_ARB_G_MIN_EXPECTED_EDGE_PCT": "0.03",
        "POLARIS_ARB_G_MAX_HOURS_TO_RESOLVE": "24",
    }
)


def _resolve_paper_profile(name: str) -> dict[str, str]:
    key = name.strip().lower()
    if key not in _PAPER_PROFILE_PRESETS:
        available = ", ".join(sorted(_PAPER_PROFILE_PRESETS.keys()))
        raise typer.BadParameter(f"unknown profile={name!r}, available: {available}")
    return dict(_PAPER_PROFILE_PRESETS[key])


def _resolve_profile_list(profiles: str, fallback_profile: str) -> list[str]:
    raw = [part.strip().lower() for part in profiles.split(",") if part.strip()]
    resolved = raw or [fallback_profile.strip().lower()]
    unique: list[str] = []
    seen: set[str] = set()
    for item in resolved:
        if item in seen:
            continue
        _resolve_paper_profile(item)
        unique.append(item)
        seen.add(item)
    return unique


def _canonical_strategy_set(enabled: set[str]) -> str:
    order = ["A", "B", "C", "F", "G"]
    return "".join(code for code in order if code in enabled)


def _apply_variant_overrides(env: dict[str, str], variant: str) -> None:
    if variant != "g_low":
        return
    g_conf = float(env.get("POLARIS_ARB_G_MIN_CONFIDENCE", "0.90") or 0.90)
    g_edge = float(env.get("POLARIS_ARB_G_MIN_EXPECTED_EDGE_PCT", "0.02") or 0.02)
    g_hours = float(env.get("POLARIS_ARB_G_MAX_HOURS_TO_RESOLVE", "24") or 24.0)
    env["POLARIS_ARB_G_MIN_CONFIDENCE"] = f"{min(0.99, g_conf + 0.03):.6f}"
    env["POLARIS_ARB_G_MIN_EXPECTED_EDGE_PCT"] = f"{g_edge * 1.5:.6f}"
    env["POLARIS_ARB_G_MAX_HOURS_TO_RESOLVE"] = f"{max(4.0, min(g_hours, 12.0)):.6f}"
    max_signals = int(float(env.get("POLARIS_ARB_MAX_SIGNALS_PER_CYCLE", "6") or 6))
    env["POLARIS_ARB_MAX_SIGNALS_PER_CYCLE"] = str(max(1, min(max_signals, 3)))


def _parse_strategy_case_token(token: str) -> tuple[str, set[str], bool, str]:
    allowed = {"A", "B", "C", "F", "G"}
    value = token.strip().upper()
    if not value:
        raise typer.BadParameter("strategy set token cannot be empty")

    split_by_strategy = False
    variant = "base"
    payload = value

    if ":" in value:
        scope, payload = value.split(":", 1)
        scope = scope.strip()
        payload = payload.strip()
        if scope not in {"SHARED", "ISOLATED"}:
            raise typer.BadParameter(f"invalid scope in strategy set token={token!r}")
        split_by_strategy = scope == "ISOLATED"

    if payload.endswith("_G_LOW"):
        variant = "g_low"
        payload = payload[: -len("_G_LOW")]

    payload_codes = [ch for ch in payload if ch in allowed]
    enabled = set(payload_codes)
    if variant == "g_low":
        enabled.add("G")
    if not enabled:
        raise typer.BadParameter(f"strategy set token={token!r} has no valid strategies")

    if len(enabled) == 1 and ":" not in value:
        split_by_strategy = True
    if split_by_strategy and len(enabled) != 1:
        raise typer.BadParameter(f"isolated strategy set must contain exactly one strategy: token={token!r}")

    strategy_set = _canonical_strategy_set(enabled)
    if split_by_strategy:
        name = f"isolated_{strategy_set.lower()}"
    else:
        suffix = "_g_low" if variant == "g_low" else ""
        name = f"shared_{strategy_set.lower()}{suffix}"
    return name, enabled, split_by_strategy, variant


def _resolve_matrix_cases(
    *,
    strategy_sets: str,
    strategies: str,
    include_c: bool,
) -> list[tuple[str, set[str], bool, str]]:
    tokens: list[str] = []
    if strategy_sets.strip():
        tokens = [part.strip() for part in strategy_sets.split(";") if part.strip()]
    elif strategies.strip():
        selected = [part.strip().upper() for part in strategies.split(",") if part.strip()]
        selected = [s for s in selected if s in {"A", "B", "C", "F", "G"}]
        if selected:
            tokens = ["SHARED:" + "".join(selected)] + [f"ISOLATED:{s}" for s in selected]
    else:
        selected = ["A", "B", "F", "G"] + (["C"] if include_c else [])
        selected_token = "".join(selected)
        no_g_selected = [s for s in selected if s != "G"]
        no_g_token = "".join(no_g_selected)
        g_low_token = f"{no_g_token}_G_LOW" if "G" in selected else no_g_token
        # 默认并行：主组合 + G降权组合 + 无G对照 + 单策略隔离
        tokens = [
            f"SHARED:{selected_token}",
            f"SHARED:{g_low_token}",
            f"SHARED:{no_g_token}",
        ]
        for strategy in selected:
            tokens.append(f"ISOLATED:{strategy}")

    cases: list[tuple[str, set[str], bool, str]] = []
    seen_names: set[str] = set()
    for token in tokens:
        name, enabled, split, variant = _parse_strategy_case_token(token)
        if name in seen_names:
            continue
        seen_names.add(name)
        cases.append((name, enabled, split, variant))
    return cases


async def _upsert_experiment_runs(
    db: Database,
    *,
    mode: str,
    run_tag: str,
    rows: list[dict[str, object]],
    started_at: datetime,
) -> None:
    for row in rows:
        run_tag_value = str(row.get("run_tag") or run_tag)
        params_json = json.dumps(row.get("params", {}), ensure_ascii=True)
        metadata_json = json.dumps(row.get("metadata", {}), ensure_ascii=True)
        await db.execute(
            """
            insert into arb_experiment_run(
                experiment_run_id, mode, run_tag, source_code, profile, strategy_set, scope, variant,
                bankroll_usd, status, params, metadata, started_at, created_at, updated_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'running', %s::jsonb, %s::jsonb, %s, now(), now())
            on conflict (mode, run_tag, source_code) do update
            set profile = excluded.profile,
                strategy_set = excluded.strategy_set,
                scope = excluded.scope,
                variant = excluded.variant,
                bankroll_usd = excluded.bankroll_usd,
                status = 'running',
                params = excluded.params,
                metadata = excluded.metadata,
                started_at = excluded.started_at,
                updated_at = now()
            """,
            (
                str(row.get("experiment_run_id") or str(uuid4())),
                mode,
                run_tag_value,
                str(row.get("source_code") or ""),
                str(row.get("profile") or "unknown"),
                str(row.get("strategy_set") or "unknown"),
                str(row.get("scope") or "shared"),
                str(row.get("variant") or "base"),
                float(row.get("bankroll_usd") or 50.0),
                params_json,
                metadata_json,
                started_at,
            ),
        )


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


@app.command("m4-rule-compile")
def m4_rule_compile(
    market_id: Annotated[str, typer.Option("--market-id", help="Optional market_id filter")] = "",
    force: Annotated[bool, typer.Option("--force/--no-force")] = False,
) -> None:
    """Compile and version market rules for Module4."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    async def _run() -> None:
        db = _create_database(settings)
        await db.open()
        try:
            service = Module4Service(db, settings)
            rows = await service.compile_rules(market_id=(market_id.strip() or None), force=force)
            payload = {
                "count": len(rows),
                "market_id": market_id or "all",
                "force": force,
                "rows": [
                    {
                        "rule_version_id": row.rule_version_id,
                        "market_id": row.market_id,
                        "rule_hash": row.rule_hash,
                        "resolution_source": row.parsed_rule.resolution_source,
                        "notes": list(row.parsed_rule.notes),
                    }
                    for row in rows
                ],
            }
            typer.echo(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        finally:
            await db.close()

    asyncio.run(_run())


@app.command("m4-run-once")
def m4_run_once(
    mode: Annotated[str, typer.Option("--mode", help="paper_live|paper_replay|shadow|live")] = "paper_live",
    source: Annotated[str, typer.Option("--source", help="Source code tag")] = "module4",
    run_tag: Annotated[str, typer.Option("--run-tag", help="auto|custom")] = "auto",
    market_id: Annotated[str, typer.Option("--market-id", help="Optional single market_id")] = "",
    use_agent: Annotated[bool, typer.Option("--agent/--no-agent")] = True,
    require_count_buckets: Annotated[bool, typer.Option("--require-count-buckets/--all-tweet-markets")] = False,
) -> None:
    """Run one Module4 prediction/decision cycle."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    allowed_modes = {"paper_live", "paper_replay", "shadow", "live"}
    mode_norm = mode.strip().lower()
    if mode_norm not in allowed_modes:
        raise typer.BadParameter("mode must be one of: paper_live, paper_replay, shadow, live")

    async def _run() -> None:
        db = _create_database(settings)
        await db.open()
        try:
            service = Module4Service(db, settings)
            summary = await service.run_once(
                mode=mode_norm,
                source_code=source.strip().lower() or settings.m4_source_code,
                run_tag=run_tag.strip() or "auto",
                market_id=market_id.strip() or None,
                use_agent=use_agent,
                require_count_buckets=require_count_buckets,
            )
            payload = {
                "run_tag": summary.run_tag,
                "mode": summary.mode,
                "source_code": summary.source_code,
                "markets_total": summary.markets_total,
                "markets_with_prior": summary.markets_with_prior,
                "snapshots_written": summary.snapshots_written,
                "decisions_written": summary.decisions_written,
                "evidence_written": summary.evidence_written,
                "skipped": summary.skipped,
            }
            typer.echo(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        finally:
            await db.close()

    asyncio.run(_run())


@app.command("m4-score")
def m4_score(
    since_hours: Annotated[int, typer.Option("--since-hours", min=1, max=24 * 60)] = 24,
    mode: Annotated[str, typer.Option("--mode", help="paper_live|paper_replay|shadow|live")] = "paper_live",
    source: Annotated[str, typer.Option("--source", help="source_code")] = "module4",
    run_tag: Annotated[str, typer.Option("--run-tag", help="current|all|custom")] = "current",
    window_code: Annotated[str, typer.Option("--window", help="fused|short|week")] = "fused",
    persist: Annotated[bool, typer.Option("--persist/--no-persist")] = True,
) -> None:
    """Compute Module4 quality and risk score."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    allowed_modes = {"paper_live", "paper_replay", "shadow", "live"}
    mode_norm = mode.strip().lower()
    if mode_norm not in allowed_modes:
        raise typer.BadParameter("mode must be one of: paper_live, paper_replay, shadow, live")
    source_norm = source.strip().lower() or settings.m4_source_code
    window_norm = window_code.strip().lower()
    if window_norm not in {"fused", "short", "week"}:
        raise typer.BadParameter("window must be one of: fused, short, week")

    async def _run() -> None:
        db = _create_database(settings)
        await db.open()
        try:
            service = Module4Service(db, settings)
            run_tag_filter = await service.resolve_run_tag_filter(raw=run_tag, mode=mode_norm, source_code=source_norm)
            result = await service.score(
                mode=mode_norm,
                source_code=source_norm,
                run_tag=run_tag_filter,
                window_code=window_norm,
                since_hours=since_hours,
                persist=persist,
            )
            payload = {
                "run_tag": result.run_tag,
                "mode": result.mode,
                "source_code": result.source_code,
                "window_code": result.window_code,
                "since_hours": result.since_hours,
                "markets_scored": result.markets_scored,
                "log_score": result.log_score,
                "ece": result.ece,
                "brier": result.brier,
                "realized_net_pnl_usd": result.realized_net_pnl_usd,
                "max_drawdown_usd": result.max_drawdown_usd,
                "score_total": result.score_total,
                "score_breakdown": result.score_breakdown,
                "metadata": result.metadata,
            }
            typer.echo(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        finally:
            await db.close()

    asyncio.run(_run())


@app.command("m4-promote")
def m4_promote(
    since_hours: Annotated[int, typer.Option("--since-hours", min=1, max=24 * 60)] = 24,
    mode: Annotated[str, typer.Option("--mode", help="paper_live|paper_replay|shadow|live")] = "paper_live",
    source: Annotated[str, typer.Option("--source", help="source_code")] = "module4",
    top_n: Annotated[int, typer.Option("--top-n", min=1, max=20)] = 2,
    min_score: Annotated[float, typer.Option("--min-score")] = -1.5,
    max_ece: Annotated[float, typer.Option("--max-ece", min=0.0, max=1.0)] = 0.06,
    min_log_score: Annotated[float, typer.Option("--min-log-score")] = -2.3,
) -> None:
    """Pick top Module4 candidates that pass score gates."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    mode_norm = mode.strip().lower()
    source_norm = source.strip().lower() or settings.m4_source_code
    if mode_norm not in {"paper_live", "paper_replay", "shadow", "live"}:
        raise typer.BadParameter("mode must be one of: paper_live, paper_replay, shadow, live")

    async def _run() -> None:
        db = _create_database(settings)
        await db.open()
        try:
            service = Module4Service(db, settings)
            rows = await service.top_candidates(
                mode=mode_norm,
                source_code=source_norm,
                since_hours=since_hours,
                top_n=top_n,
                min_score=min_score,
                max_ece=max_ece,
                min_log_score=min_log_score,
            )
            payload = {
                "mode": mode_norm,
                "source_code": source_norm,
                "since_hours": since_hours,
                "top_n": top_n,
                "gates": {
                    "min_score": min_score,
                    "max_ece": max_ece,
                    "min_log_score": min_log_score,
                },
                "candidates": rows,
            }
            typer.echo(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        finally:
            await db.close()

    asyncio.run(_run())


@app.command("m4-replay")
def m4_replay(
    start: Annotated[str, typer.Option("--start", help="ISO datetime, e.g. 2026-02-10T00:00:00Z")],
    end: Annotated[str, typer.Option("--end", help="ISO datetime, e.g. 2026-02-13T00:00:00Z")],
    step_minutes: Annotated[int, typer.Option("--step-minutes", min=5, max=720)] = 60,
    source: Annotated[str, typer.Option("--source", help="source_code")] = "module4",
    run_tag: Annotated[str, typer.Option("--run-tag", help="auto|custom")] = "auto",
    market_id: Annotated[str, typer.Option("--market-id", help="optional single market_id")] = "",
    use_agent: Annotated[bool, typer.Option("--agent/--no-agent")] = False,
    include_closed: Annotated[bool, typer.Option("--include-closed/--open-only")] = True,
    require_count_buckets: Annotated[bool, typer.Option("--require-count-buckets/--all-tweet-markets")] = False,
    max_steps: Annotated[int, typer.Option("--max-steps", min=1, max=5000)] = 1000,
) -> None:
    """Replay Module4 predictions over historical timestamps (no future leakage)."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    try:
        start_ts = parse_iso_datetime(start)
    except ValueError as exc:
        raise typer.BadParameter(f"invalid --start datetime: {start}") from exc
    try:
        end_ts = parse_iso_datetime(end)
    except ValueError as exc:
        raise typer.BadParameter(f"invalid --end datetime: {end}") from exc
    if end_ts <= start_ts:
        raise typer.BadParameter("end must be later than start")

    async def _run() -> None:
        db = _create_database(settings)
        await db.open()
        try:
            service = Module4Service(db, settings)
            await service.compile_rules(force=False)
            cursor = start_ts
            steps = 0
            aggregate = {
                "markets_total": 0,
                "markets_with_prior": 0,
                "snapshots_written": 0,
                "decisions_written": 0,
                "evidence_written": 0,
            }
            skipped_total: Counter[str] = Counter()
            while cursor <= end_ts:
                steps += 1
                summary = await service.run_once(
                    mode="paper_replay",
                    source_code=source.strip().lower() or "module4",
                    run_tag=run_tag.strip() or "auto",
                    market_id=market_id.strip() or None,
                    use_agent=use_agent,
                    as_of=cursor,
                    include_closed=include_closed,
                    require_count_buckets=require_count_buckets,
                )
                aggregate["markets_total"] += summary.markets_total
                aggregate["markets_with_prior"] += summary.markets_with_prior
                aggregate["snapshots_written"] += summary.snapshots_written
                aggregate["decisions_written"] += summary.decisions_written
                aggregate["evidence_written"] += summary.evidence_written
                skipped_total.update(summary.skipped)
                if steps >= max_steps:
                    break
                cursor += timedelta(minutes=step_minutes)

            payload = {
                "mode": "paper_replay",
                "source_code": source.strip().lower() or "module4",
                "run_tag": summary.run_tag if steps > 0 else run_tag,
                "start": start_ts.isoformat(),
                "end": end_ts.isoformat(),
                "step_minutes": step_minutes,
                "steps": steps,
                "max_steps": max_steps,
                "include_closed": include_closed,
                "require_count_buckets": require_count_buckets,
                "agent_enabled": use_agent,
                "aggregate": aggregate,
                "skipped": dict(skipped_total),
            }
            typer.echo(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        finally:
            await db.close()

    asyncio.run(_run())


@app.command("m4-stage-report")
def m4_stage_report(
    mode: Annotated[str, typer.Option("--mode", help="paper_replay|paper_live|shadow|live")] = "paper_replay",
    source: Annotated[str, typer.Option("--source", help="source_code")] = "module4",
    run_tag: Annotated[str, typer.Option("--run-tag", help="current|all|custom")] = "current",
    since_hours: Annotated[int, typer.Option("--since-hours", min=1, max=24 * 120)] = 24 * 14,
    ece_bins: Annotated[int, typer.Option("--ece-bins", min=5, max=30)] = 10,
) -> None:
    """Stage-wise accuracy report by time-to-close; compares prior vs posterior."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    mode_norm = mode.strip().lower()
    if mode_norm not in {"paper_replay", "paper_live", "shadow", "live"}:
        raise typer.BadParameter("mode must be one of: paper_replay, paper_live, shadow, live")
    source_norm = source.strip().lower() or "module4"
    since_start = datetime.now(tz=UTC) - timedelta(hours=since_hours)
    # stage boundaries in hours to close: [0, 0.5), [0.5, 1), [1, 3), [3, 6), [6, 12), [12, 24), [24, 48), [48, inf)
    stage_bins: list[tuple[str, float, float]] = [
        ("0-30m", 0.0, 0.5),
        ("30m-1h", 0.5, 1.0),
        ("1h-3h", 1.0, 3.0),
        ("3h-6h", 3.0, 6.0),
        ("6h-12h", 6.0, 12.0),
        ("12h-24h", 12.0, 24.0),
        ("24h-48h", 24.0, 48.0),
        ("48h+", 48.0, 10_000.0),
    ]

    async def _run() -> None:
        db = _create_database(settings)
        await db.open()
        try:
            service = Module4Service(db, settings)
            run_tag_filter = await service.resolve_run_tag_filter(raw=run_tag, mode=mode_norm, source_code=source_norm)
            params: list[object] = [since_start, mode_norm, source_norm]
            run_clause = ""
            if run_tag_filter:
                run_clause = "and ps.run_tag = %s"
                params.append(run_tag_filter)
            rows = await db.fetch_all(
                f"""
                select
                    ps.run_tag,
                    ps.market_id,
                    m.slug,
                    ps.tracking_id,
                    ps.account_id,
                    ps.prior_pmf,
                    ps.posterior_pmf,
                    ps.created_at,
                    coalesce((ps.metadata->>'replay_as_of')::timestamptz, ps.created_at) as as_of_ts,
                    tw.end_date,
                    coalesce(
                        (
                            select fm.cumulative_count_est
                            from fact_tweet_metric_1m fm
                            where fm.account_id = ps.account_id
                              and fm.tracking_id = ps.tracking_id
                              and fm.minute_bucket <= tw.end_date
                            order by fm.minute_bucket desc
                            limit 1
                        ),
                        (
                            select max(fd.cumulative_count)::int
                            from fact_tweet_metric_daily fd
                            where fd.account_id = ps.account_id
                              and fd.tracking_id = ps.tracking_id
                              and fd.metric_date <= tw.end_date::date
                        ),
                        0
                    ) as final_count
                from m4_posterior_snapshot ps
                join dim_market m
                  on m.market_id = ps.market_id
                join dim_tracking_window tw
                  on tw.tracking_id = ps.tracking_id
                 and tw.account_id = ps.account_id
                where ps.created_at >= %s
                  and ps.mode = %s
                  and ps.source_code = %s
                  and ps.window_code = 'fused'
                  {run_clause}
                  and tw.end_date is not null
                order by ps.created_at asc
                """,
                tuple(params),
            )
            prepared = aggregate_market_family_rows(rows, include_baseline=False)

            grouped: dict[str, dict[str, list[float] | list[tuple[float, int]] | int]] = {
                name: {
                    "count": 0,
                    "prior_log": [],
                    "post_log": [],
                    "prior_brier": [],
                    "post_brier": [],
                    "prior_points": [],
                    "post_points": [],
                }
                for name, _, _ in stage_bins
            }
            total_points = 0
            skipped = 0
            skipped_by_reason: Counter[str] = Counter()
            for row in prepared.rows:
                end_date = row.get("end_date")
                as_of_ts = row.get("as_of_ts")
                if not isinstance(end_date, datetime) or not isinstance(as_of_ts, datetime):
                    skipped += 1
                    skipped_by_reason["invalid_time"] += 1
                    continue
                hours_to_close = (end_date - as_of_ts).total_seconds() / 3600.0
                if hours_to_close <= 0:
                    skipped += 1
                    skipped_by_reason["non_positive_hours_to_close"] += 1
                    continue
                stage_name = _pick_stage(stage_bins, hours_to_close)
                if stage_name is None:
                    skipped += 1
                    skipped_by_reason["stage_not_mapped"] += 1
                    continue
                prior = _parse_pmf(row.get("prior_pmf"))
                posterior = _parse_pmf(row.get("posterior_pmf"))
                if not prior or not posterior:
                    skipped += 1
                    skipped_by_reason["pmf_missing"] += 1
                    continue
                final_count = int(row.get("final_count") or 0)
                true_label = infer_true_bucket_label(final_count, posterior.keys())
                if true_label is None:
                    skipped += 1
                    skipped_by_reason["true_label_not_in_bucket_space"] += 1
                    continue
                p_prior = max(1e-9, float(prior.get(true_label, 0.0)))
                p_post = max(1e-9, float(posterior.get(true_label, 0.0)))
                g = grouped[stage_name]
                g["count"] = int(g["count"]) + 1
                g["prior_log"].append(math.log(p_prior))
                g["post_log"].append(math.log(p_post))
                g["prior_brier"].append(multiclass_brier(prior, true_label))
                g["post_brier"].append(multiclass_brier(posterior, true_label))
                pred_prior = _argmax_label(prior)
                pred_post = _argmax_label(posterior)
                g["prior_points"].append((float(prior.get(pred_prior, 0.0)), 1 if pred_prior == true_label else 0))
                g["post_points"].append((float(posterior.get(pred_post, 0.0)), 1 if pred_post == true_label else 0))
                total_points += 1

            report_rows: list[dict[str, object]] = []
            for name, _, _ in stage_bins:
                g = grouped[name]
                count = int(g["count"])
                if count <= 0:
                    report_rows.append({"stage": name, "count": 0})
                    continue
                prior_log = sum(g["prior_log"]) / count
                post_log = sum(g["post_log"]) / count
                prior_brier = sum(g["prior_brier"]) / count
                post_brier = sum(g["post_brier"]) / count
                prior_ece = expected_calibration_error(g["prior_points"], bins=ece_bins)
                post_ece = expected_calibration_error(g["post_points"], bins=ece_bins)
                report_rows.append(
                    {
                        "stage": name,
                        "count": count,
                        "prior_log_score": round(prior_log, 8),
                        "posterior_log_score": round(post_log, 8),
                        "log_score_gain_pct": round(((post_log - prior_log) / max(1e-9, abs(prior_log))) * 100.0, 4),
                        "prior_ece": round(prior_ece, 8),
                        "posterior_ece": round(post_ece, 8),
                        "ece_delta": round(post_ece - prior_ece, 8),
                        "prior_brier": round(prior_brier, 8),
                        "posterior_brier": round(post_brier, 8),
                        "brier_delta": round(post_brier - prior_brier, 8),
                    }
                )

            payload = {
                "mode": mode_norm,
                "source_code": source_norm,
                "run_tag": run_tag_filter or "all",
                "since_hours": since_hours,
                "ece_bins": ece_bins,
                "points_total": total_points,
                "rows_loaded": len(rows),
                "rows_after_aggregation": len(prepared.rows),
                "skipped": skipped,
                "skipped_by_reason": dict(skipped_by_reason),
                "aggregation": prepared.stats,
                "stage_report": report_rows,
            }
            typer.echo(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        finally:
            await db.close()

    asyncio.run(_run())


@app.command("m4-semantic-dryrun")
def m4_semantic_dryrun(
    market_id: Annotated[str, typer.Option("--market-id", help="Target market_id")],
    mode: Annotated[str, typer.Option("--mode", help="paper_live|paper_replay|shadow|live")] = "paper_live",
    source: Annotated[str, typer.Option("--source", help="source_code")] = "module4",
    run_tag: Annotated[str, typer.Option("--run-tag", help="auto|custom")] = "auto",
    as_of: Annotated[str, typer.Option("--as-of", help="ISO datetime (optional)")] = "",
) -> None:
    """Run one semantic cycle and show before/after posterior plus semantic diagnostics."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    mode_norm = mode.strip().lower()
    if mode_norm not in {"paper_live", "paper_replay", "shadow", "live"}:
        raise typer.BadParameter("mode must be one of: paper_live, paper_replay, shadow, live")
    as_of_ts = None
    if as_of.strip():
        as_of_ts = parse_iso_datetime(as_of.strip())

    async def _run() -> None:
        db = _create_database(settings)
        await db.open()
        try:
            service = Module4Service(db, settings)
            await service.compile_rules(market_id=market_id, force=False)
            tag_value = run_tag.strip() or "auto"
            summary = await service.run_once(
                mode=mode_norm,
                source_code=source.strip().lower() or "module4",
                run_tag=tag_value,
                market_id=market_id.strip(),
                use_agent=True,
                as_of=as_of_ts,
                include_closed=True,
                require_count_buckets=False,
            )
            rows = await db.fetch_all(
                """
                select
                    ps.snapshot_id,
                    ps.run_tag,
                    ps.market_id,
                    ps.window_code,
                    ps.posterior_pmf,
                    ps.metadata,
                    ps.created_at
                from m4_posterior_snapshot ps
                where ps.run_tag = %s
                  and ps.market_id = %s
                  and ps.window_code = 'fused'
                order by ps.created_at desc
                limit 1
                """,
                (summary.run_tag, market_id.strip()),
            )
            evidence = await db.fetch_one(
                """
                select
                    confidence, delta_uncertainty, event_tags, timed_out, degraded,
                    llm_model, prompt_version, latency_ms, parse_ok, applied, error_code, metadata, created_at
                from m4_evidence_log
                where run_tag = %s
                  and market_id = %s
                order by created_at desc
                limit 1
                """,
                (summary.run_tag, market_id.strip()),
            )
            decision = await db.fetch_one(
                """
                select action, reason_codes, edge_expected, es95_loss, net_edge, size_usd, metadata, created_at
                from m4_decision_log
                where run_tag = %s
                  and market_id = %s
                  and window_code = 'fused'
                order by created_at desc
                limit 1
                """,
                (summary.run_tag, market_id.strip()),
            )

            fused = rows[0] if rows else None
            after_pmf = _parse_pmf(fused.get("posterior_pmf")) if fused else {}
            metadata = fused.get("metadata") if fused else {}
            baseline_pmf = _parse_pmf((metadata or {}).get("baseline_posterior_pmf")) if isinstance(metadata, dict) else {}
            payload = {
                "summary": {
                    "run_tag": summary.run_tag,
                    "mode": summary.mode,
                    "source_code": summary.source_code,
                    "markets_total": summary.markets_total,
                    "decisions_written": summary.decisions_written,
                    "snapshots_written": summary.snapshots_written,
                    "skipped": summary.skipped,
                },
                "market_id": market_id.strip(),
                "as_of": as_of_ts.isoformat() if isinstance(as_of_ts, datetime) else datetime.now(tz=UTC).isoformat(),
                "baseline_posterior": baseline_pmf,
                "semantic_posterior": after_pmf,
                "semantic_metadata": metadata if isinstance(metadata, dict) else {},
                "latest_evidence": evidence,
                "latest_decision": decision,
            }
            typer.echo(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        finally:
            await db.close()

    asyncio.run(_run())


@app.command("m4-semantic-eval")
def m4_semantic_eval(
    mode: Annotated[str, typer.Option("--mode", help="paper_replay|paper_live|shadow|live")] = "paper_replay",
    source: Annotated[str, typer.Option("--source", help="source_code")] = "module4",
    run_tag: Annotated[str, typer.Option("--run-tag", help="current|all|custom")] = "all",
    since_hours: Annotated[int, typer.Option("--since-hours", min=1, max=24 * 120)] = 24 * 14,
    ece_bins: Annotated[int, typer.Option("--ece-bins", min=5, max=30)] = 10,
) -> None:
    """Evaluate baseline vs semantic-adjusted posterior quality on settled windows."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    mode_norm = mode.strip().lower()
    if mode_norm not in {"paper_replay", "paper_live", "shadow", "live"}:
        raise typer.BadParameter("mode must be one of: paper_replay, paper_live, shadow, live")
    source_norm = source.strip().lower() or "module4"
    since_start = datetime.now(tz=UTC) - timedelta(hours=since_hours)

    async def _run() -> None:
        db = _create_database(settings)
        await db.open()
        try:
            service = Module4Service(db, settings)
            run_tag_filter = await service.resolve_run_tag_filter(raw=run_tag, mode=mode_norm, source_code=source_norm)
            params: list[object] = [since_start, mode_norm, source_norm]
            run_clause = ""
            if run_tag_filter:
                run_clause = "and ps.run_tag = %s"
                params.append(run_tag_filter)
            rows = await db.fetch_all(
                f"""
                select
                    ps.run_tag,
                    ps.market_id,
                    m.slug,
                    ps.posterior_pmf,
                    coalesce(ps.metadata->'baseline_posterior_pmf', '{{}}'::jsonb) as baseline_pmf,
                    ps.metadata,
                    tw.end_date,
                    coalesce((ps.metadata->>'replay_as_of')::timestamptz, ps.created_at) as as_of_ts,
                    coalesce(
                        (
                            select fm.cumulative_count_est
                            from fact_tweet_metric_1m fm
                            where fm.account_id = ps.account_id
                              and fm.tracking_id = ps.tracking_id
                              and fm.minute_bucket <= tw.end_date
                            order by fm.minute_bucket desc
                            limit 1
                        ),
                        (
                            select max(fd.cumulative_count)::int
                            from fact_tweet_metric_daily fd
                            where fd.account_id = ps.account_id
                              and fd.tracking_id = ps.tracking_id
                              and fd.metric_date <= tw.end_date::date
                        ),
                        0
                    ) as final_count
                from m4_posterior_snapshot ps
                join dim_market m
                  on m.market_id = ps.market_id
                join dim_tracking_window tw
                  on tw.tracking_id = ps.tracking_id
                 and tw.account_id = ps.account_id
                where ps.created_at >= %s
                  and ps.mode = %s
                  and ps.source_code = %s
                  and ps.window_code = 'fused'
                  {run_clause}
                  and tw.end_date is not null
                order by ps.created_at asc
                """,
                tuple(params),
            )
            prepared = aggregate_market_family_rows(rows, include_baseline=True)

            baseline_log: list[float] = []
            semantic_log: list[float] = []
            baseline_points: list[tuple[float, int]] = []
            semantic_points: list[tuple[float, int]] = []
            applied_count = 0
            skipped = 0
            skipped_by_reason: Counter[str] = Counter()
            for row in prepared.rows:
                end_date = row.get("end_date")
                as_of_ts = row.get("as_of_ts")
                if not isinstance(end_date, datetime) or not isinstance(as_of_ts, datetime):
                    skipped += 1
                    skipped_by_reason["invalid_time"] += 1
                    continue
                if (end_date - as_of_ts).total_seconds() <= 0:
                    skipped += 1
                    skipped_by_reason["non_positive_hours_to_close"] += 1
                    continue
                semantic = _parse_pmf(row.get("posterior_pmf"))
                baseline = _parse_pmf(row.get("baseline_pmf"))
                if not baseline or not semantic:
                    skipped += 1
                    skipped_by_reason["pmf_missing"] += 1
                    continue
                final_count = int(row.get("final_count") or 0)
                true_label = infer_true_bucket_label(final_count, semantic.keys())
                if true_label is None:
                    skipped += 1
                    skipped_by_reason["true_label_not_in_bucket_space"] += 1
                    continue
                p_base = max(1e-9, float(baseline.get(true_label, 0.0)))
                p_sem = max(1e-9, float(semantic.get(true_label, 0.0)))
                baseline_log.append(math.log(p_base))
                semantic_log.append(math.log(p_sem))
                pred_base = _argmax_label(baseline)
                pred_sem = _argmax_label(semantic)
                baseline_points.append((float(baseline.get(pred_base, 0.0)), 1 if pred_base == true_label else 0))
                semantic_points.append((float(semantic.get(pred_sem, 0.0)), 1 if pred_sem == true_label else 0))
                if bool(row.get("semantic_applied")):
                    applied_count += 1

            base_log_score = (sum(baseline_log) / len(baseline_log)) if baseline_log else None
            sem_log_score = (sum(semantic_log) / len(semantic_log)) if semantic_log else None
            base_ece = expected_calibration_error(baseline_points, bins=ece_bins) if baseline_points else None
            sem_ece = expected_calibration_error(semantic_points, bins=ece_bins) if semantic_points else None
            gain_pct = None
            if base_log_score is not None and sem_log_score is not None:
                gain_pct = ((sem_log_score - base_log_score) / max(1e-9, abs(base_log_score))) * 100.0
            payload = {
                "mode": mode_norm,
                "source_code": source_norm,
                "run_tag": run_tag_filter or "all",
                "since_hours": since_hours,
                "rows_loaded": len(rows),
                "rows_after_aggregation": len(prepared.rows),
                "points_total": len(semantic_log),
                "semantic_applied_points": applied_count,
                "baseline_log_score": base_log_score,
                "semantic_log_score": sem_log_score,
                "log_score_gain_pct": gain_pct,
                "baseline_ece": base_ece,
                "semantic_ece": sem_ece,
                "ece_delta": (sem_ece - base_ece) if (sem_ece is not None and base_ece is not None) else None,
                "skipped": skipped,
                "skipped_by_reason": dict(skipped_by_reason),
                "aggregation": prepared.stats,
            }
            typer.echo(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        finally:
            await db.close()

    asyncio.run(_run())


@app.command("m4-semantic-health")
def m4_semantic_health(
    mode: Annotated[str, typer.Option("--mode", help="paper_replay|paper_live|shadow|live|all")] = "all",
    source: Annotated[str, typer.Option("--source", help="source_code|all")] = "module4",
    run_tag: Annotated[str, typer.Option("--run-tag", help="current|all|custom")] = "all",
    since_hours: Annotated[int, typer.Option("--since-hours", min=1, max=24 * 120)] = 24 * 7,
) -> None:
    """Show semantic LLM runtime health: timeout/parse-fail/applied rates and latency p95."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    mode_norm = mode.strip().lower()
    if mode_norm not in {"paper_replay", "paper_live", "shadow", "live", "all"}:
        raise typer.BadParameter("mode must be one of: paper_replay, paper_live, shadow, live, all")
    source_norm = source.strip().lower() or "module4"
    source_filter = None if source_norm == "all" else source_norm
    mode_filter = None if mode_norm == "all" else mode_norm
    since_start = datetime.now(tz=UTC) - timedelta(hours=since_hours)

    async def _run() -> None:
        db = _create_database(settings)
        await db.open()
        try:
            service = Module4Service(db, settings)
            run_tag_filter: str | None
            if run_tag.strip().lower() in {"", "all"}:
                run_tag_filter = None
            elif run_tag.strip().lower() == "current":
                if mode_filter is None or source_filter is None:
                    raise typer.BadParameter("--run-tag current requires --mode and --source to be specific (not 'all')")
                run_tag_filter = await service.resolve_run_tag_filter(raw=run_tag, mode=mode_filter, source_code=source_filter)
            else:
                run_tag_filter = _sanitize_run_tag(run_tag)

            where = ["created_at >= %s"]
            params: list[object] = [since_start]
            if mode_filter is not None:
                where.append("mode = %s")
                params.append(mode_filter)
            if source_filter is not None:
                where.append("source_code = %s")
                params.append(source_filter)
            if run_tag_filter:
                where.append("run_tag = %s")
                params.append(run_tag_filter)
            row = await db.fetch_one(
                f"""
                select
                    count(*)::int as total,
                    coalesce(avg(case when timed_out then 1.0 else 0.0 end), 0) as timeout_rate,
                    coalesce(avg(case when coalesce(parse_ok, false) then 0.0 else 1.0 end), 0) as parse_fail_rate,
                    coalesce(avg(case when coalesce(applied, false) then 1.0 else 0.0 end), 0) as applied_rate,
                    percentile_cont(0.50) within group (order by coalesce(latency_ms, 0)) as p50_latency_ms,
                    percentile_cont(0.95) within group (order by coalesce(latency_ms, 0)) as p95_latency_ms
                from m4_evidence_log
                where {' and '.join(where)}
                """,
                tuple(params),
            )
            model_rows = await db.fetch_all(
                f"""
                select llm_model, count(*)::int as c
                from m4_evidence_log
                where {' and '.join(where)}
                group by llm_model
                order by c desc, llm_model asc
                """,
                tuple(params),
            )
            payload = {
                "mode": mode_filter or "all",
                "source_code": source_filter or "all",
                "run_tag": run_tag_filter or "all",
                "since_hours": since_hours,
                "total": int(row["total"] if row else 0),
                "timeout_rate": float(row["timeout_rate"] if row and row["timeout_rate"] is not None else 0.0),
                "parse_fail_rate": float(row["parse_fail_rate"] if row and row["parse_fail_rate"] is not None else 0.0),
                "applied_rate": float(row["applied_rate"] if row and row["applied_rate"] is not None else 0.0),
                "p50_latency_ms": float(row["p50_latency_ms"] if row and row["p50_latency_ms"] is not None else 0.0),
                "p95_latency_ms": float(row["p95_latency_ms"] if row and row["p95_latency_ms"] is not None else 0.0),
                "model_distribution": [{str(r["llm_model"]): int(r["c"])} for r in model_rows],
            }
            typer.echo(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        finally:
            await db.close()

    asyncio.run(_run())


@app.command("m4-dataset-audit")
def m4_dataset_audit(
    mode: Annotated[str, typer.Option("--mode", help="paper_replay|paper_live|shadow|live|all")] = "paper_replay",
    source: Annotated[str, typer.Option("--source", help="source_code|all")] = "module4",
    run_tag: Annotated[str, typer.Option("--run-tag", help="current|all|custom")] = "all",
    since_hours: Annotated[int, typer.Option("--since-hours", min=1, max=24 * 120)] = 24 * 14,
    require_count_buckets: Annotated[bool, typer.Option("--require-count-buckets/--all-markets")] = False,
) -> None:
    """Audit scorable Module4 dataset coverage and unscorable reason distribution."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    mode_norm = mode.strip().lower()
    if mode_norm not in {"paper_replay", "paper_live", "shadow", "live", "all"}:
        raise typer.BadParameter("mode must be one of: paper_replay, paper_live, shadow, live, all")
    source_norm = source.strip().lower() or "module4"
    source_filter = None if source_norm == "all" else source_norm
    mode_filter = None if mode_norm == "all" else mode_norm
    run_norm = run_tag.strip().lower()
    since_start = datetime.now(tz=UTC) - timedelta(hours=since_hours)

    async def _run() -> None:
        db = _create_database(settings)
        await db.open()
        try:
            service = Module4Service(db, settings)
            run_tag_filter: str | None
            if run_norm in ("", "all"):
                run_tag_filter = None
            elif run_norm == "current":
                if mode_filter is None or source_filter is None:
                    raise typer.BadParameter("--run-tag current requires --mode and --source to be specific (not 'all')")
                run_tag_filter = await service.resolve_run_tag_filter(raw=run_tag, mode=mode_filter, source_code=source_filter)
            else:
                run_tag_filter = _sanitize_run_tag(run_tag)

            where_clauses = ["ps.created_at >= %s", "tw.end_date is not null"]
            params: list[object] = [since_start]
            if mode_filter is not None:
                where_clauses.append("ps.mode = %s")
                params.append(mode_filter)
            if source_filter is not None:
                where_clauses.append("ps.source_code = %s")
                params.append(source_filter)
            if run_tag_filter:
                where_clauses.append("ps.run_tag = %s")
                params.append(run_tag_filter)
            if require_count_buckets:
                where_clauses.append(
                    """
                    (
                        m.slug ~ '-[0-9]+-[0-9]+$'
                        or m.slug ~ '-[0-9]+(plus|\\+)$'
                        or m.slug ~ '-[0-9]+-plus$'
                    )
                    """
                )

            rows = await db.fetch_all(
                f"""
                select
                    ps.run_tag,
                    ps.market_id,
                    m.slug,
                    ps.prior_pmf,
                    ps.posterior_pmf,
                    coalesce((ps.metadata->>'replay_as_of')::timestamptz, ps.created_at) as as_of_ts,
                    tw.end_date,
                    coalesce(
                        (
                            select fm.cumulative_count_est
                            from fact_tweet_metric_1m fm
                            where fm.account_id = ps.account_id
                              and fm.tracking_id = ps.tracking_id
                              and fm.minute_bucket <= tw.end_date
                            order by fm.minute_bucket desc
                            limit 1
                        ),
                        (
                            select max(fd.cumulative_count)::int
                            from fact_tweet_metric_daily fd
                            where fd.account_id = ps.account_id
                              and fd.tracking_id = ps.tracking_id
                              and fd.metric_date <= tw.end_date::date
                        ),
                        0
                    ) as final_count
                from m4_posterior_snapshot ps
                join dim_market m
                  on m.market_id = ps.market_id
                join dim_tracking_window tw
                  on tw.tracking_id = ps.tracking_id
                 and tw.account_id = ps.account_id
                where {' and '.join(where_clauses)}
                order by ps.created_at asc
                """,
                tuple(params),
            )

            prepared = aggregate_market_family_rows(rows, include_baseline=False)
            audit = _audit_m4_dataset_rows(prepared.rows)
            payload = {
                "mode": mode_filter or "all",
                "source_code": source_filter or "all",
                "run_tag": run_tag_filter or "all",
                "since_hours": since_hours,
                "require_count_buckets": require_count_buckets,
                "aggregation": prepared.stats,
                **audit,
            }
            typer.echo(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        finally:
            await db.close()

    asyncio.run(_run())


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
    profiles: Annotated[str, typer.Option("--profiles", help="Comma separated profiles: strict_50,balanced_50,aggressive_50")] = "",
    strategy_sets: Annotated[
        str,
        typer.Option(
            "--strategy-sets",
            help="Semicolon separated tokens. Examples: SHARED:ABCFG;SHARED:ABCF_G_LOW;ISOLATED:A",
        ),
    ] = "",
    bankroll_usd: Annotated[float, typer.Option("--bankroll-usd", min=1.0)] = 50.0,
    strategies: Annotated[str, typer.Option("--strategies", help="Comma separated: A,B,C,F,G (default: A,B,F,G + optional C)")] = "",
    include_c: Annotated[bool, typer.Option("--include-c/--no-include-c")] = True,
) -> None:
    """Start paper matrix with cartesian runs (profiles x strategy sets)."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)

    profile_names = _resolve_profile_list(profiles, profile)
    cases = _resolve_matrix_cases(strategy_sets=strategy_sets, strategies=strategies, include_c=include_c)
    if not cases:
        raise typer.BadParameter("strategy-sets resolved to empty")

    timestamp = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
    profile_suffix = "multi" if len(profile_names) > 1 else profile_names[0]
    matrix_tag = _sanitize_run_tag(f"paper-{profile_suffix}-{timestamp}")
    logs_dir = Path("logs")
    logs_dir.mkdir(parents=True, exist_ok=True)
    run_dir = Path("run")
    run_dir.mkdir(parents=True, exist_ok=True)

    bankroll_tag = str(int(bankroll_usd)) if float(bankroll_usd).is_integer() else f"{bankroll_usd:g}"

    def _spawn(
        *,
        case_name: str,
        profile_name: str,
        enabled: set[str],
        split_by_strategy: bool,
        variant: str,
    ) -> tuple[int, Path, str]:
        source = f"{source_prefix}_{profile_name}_{case_name}{bankroll_tag}".lower()
        log_path = logs_dir / f"arb_paper_{profile_name}_{case_name}_{timestamp}.log"
        env = os.environ.copy()
        env.update(_resolve_paper_profile(profile_name))
        env["POLARIS_ARB_RUN_TAG"] = matrix_tag
        env["POLARIS_ARB_PAPER_INITIAL_BANKROLL_USD"] = f"{bankroll_usd:.4f}"
        env["POLARIS_ARB_PAPER_SPLIT_BY_STRATEGY"] = "true" if split_by_strategy else "false"
        env["POLARIS_ARB_ENABLE_STRATEGY_A"] = "true" if "A" in enabled else "false"
        env["POLARIS_ARB_ENABLE_STRATEGY_B"] = "true" if "B" in enabled else "false"
        env["POLARIS_ARB_ENABLE_STRATEGY_C"] = "true" if "C" in enabled else "false"
        env["POLARIS_ARB_ENABLE_STRATEGY_F"] = "true" if "F" in enabled else "false"
        env["POLARIS_ARB_ENABLE_STRATEGY_G"] = "true" if "G" in enabled else "false"
        _apply_variant_overrides(env, variant)

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
        pid_path = run_dir / f"arb_paper_{profile_name}_{case_name}_{timestamp}.pid"
        pid_path.write_text(str(proc.pid), encoding="utf-8")
        return proc.pid, log_path, source

    started: list[tuple[str, int, Path, str, str, str, str]] = []
    experiment_rows: list[dict[str, object]] = []
    started_at = datetime.now(tz=UTC)
    for profile_name in profile_names:
        for case_name, enabled, split_by_strategy, variant in cases:
            pid, log_path, source = _spawn(
                case_name=case_name,
                profile_name=profile_name,
                enabled=enabled,
                split_by_strategy=split_by_strategy,
                variant=variant,
            )
            strategy_set = _canonical_strategy_set(enabled)
            scope = "isolated" if split_by_strategy else "shared"
            started.append((f"{profile_name}:{case_name}", pid, log_path, source, profile_name, strategy_set, scope))
            experiment_rows.append(
                {
                    "experiment_run_id": str(uuid4()),
                    "run_tag": matrix_tag,
                    "source_code": source,
                    "profile": profile_name,
                    "strategy_set": strategy_set,
                    "scope": scope,
                    "variant": variant,
                    "bankroll_usd": bankroll_usd,
                    "params": {
                        "duration_hours": duration_hours,
                        "source_prefix": source_prefix,
                        "profile": profile_name,
                        "strategy_set": strategy_set,
                        "variant": variant,
                        "split_by_strategy": split_by_strategy,
                    },
                    "metadata": {
                        "launcher": "arb-paper-matrix-start",
                        "enabled_strategies": sorted(enabled),
                        "pid": pid,
                        "log_path": str(log_path),
                    },
                }
            )

    async def _record_runs() -> None:
        db = _create_database(settings)
        await db.open()
        try:
            await _upsert_experiment_runs(
                db,
                mode=RunMode.PAPER_LIVE.value,
                run_tag=matrix_tag,
                rows=experiment_rows,
                started_at=started_at,
            )
        finally:
            await db.close()

    asyncio.run(_record_runs())

    typer.echo("arb-paper-matrix started")
    typer.echo(
        f"profiles={','.join(profile_names)} cases={len(cases)} run_tag={matrix_tag} bankroll={bankroll_usd:g}"
    )
    for name, pid, log_path, source, profile_name, strategy_set, scope in started:
        typer.echo(
            f"{name}: pid={pid} source={source} profile={profile_name} strategy_set={strategy_set} scope={scope} log={log_path}"
        )


@app.command("arb-replay-matrix")
def arb_replay_matrix(
    start: Annotated[str, typer.Option("--start", help="ISO datetime")] = "",
    end: Annotated[str, typer.Option("--end", help="ISO datetime")] = "",
    profile: Annotated[str, typer.Option("--profile")] = "trigger_safe_50_v2",
    profiles: Annotated[str, typer.Option("--profiles", help="Comma separated profiles")] = "",
    strategy_sets: Annotated[
        str,
        typer.Option(
            "--strategy-sets",
            help="Semicolon separated tokens. Examples: SHARED:ABCFG;SHARED:ABCF_G_LOW;ISOLATED:A",
        ),
    ] = "",
    bankroll_usd: Annotated[float, typer.Option("--bankroll-usd", min=1.0)] = 50.0,
    source_prefix: Annotated[str, typer.Option("--source-prefix")] = "polymarket_replay",
    strategies: Annotated[str, typer.Option("--strategies", help="Comma separated: A,B,C,F,G (default: A,B,F,G + optional C)")] = "",
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
    profile_names = _resolve_profile_list(profiles, profile)
    cases = _resolve_matrix_cases(strategy_sets=strategy_sets, strategies=strategies, include_c=include_c)
    if not cases:
        raise typer.BadParameter("strategy-sets resolved to empty")

    timestamp = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
    profile_suffix = "multi" if len(profile_names) > 1 else profile_names[0]
    matrix_tag = _sanitize_run_tag(f"replay-{profile_suffix}-{timestamp}")
    logs_dir = Path("logs")
    logs_dir.mkdir(parents=True, exist_ok=True)
    bankroll_tag = str(int(bankroll_usd)) if float(bankroll_usd).is_integer() else f"{bankroll_usd:g}"

    rows: list[dict[str, object]] = []
    experiment_rows: list[dict[str, object]] = []
    started_at = datetime.now(tz=UTC)
    for profile_name in profile_names:
        for name, enabled, split_by_strategy, variant in cases:
            source = f"{source_prefix}_{profile_name}_{name}{bankroll_tag}".lower()
            run_tag_case = _sanitize_run_tag(f"{matrix_tag}-{profile_name}-{name}")
            log_path = logs_dir / f"arb_replay_{profile_name}_{name}_{timestamp}.log"

            env = os.environ.copy()
            env.update(_resolve_paper_profile(profile_name))
            env["POLARIS_ARB_RUN_TAG"] = run_tag_case
            env["POLARIS_ARB_PAPER_INITIAL_BANKROLL_USD"] = f"{bankroll_usd:.4f}"
            env["POLARIS_ARB_PAPER_SPLIT_BY_STRATEGY"] = "true" if split_by_strategy else "false"
            env["POLARIS_ARB_ENABLE_STRATEGY_A"] = "true" if "A" in enabled else "false"
            env["POLARIS_ARB_ENABLE_STRATEGY_B"] = "true" if "B" in enabled else "false"
            env["POLARIS_ARB_ENABLE_STRATEGY_C"] = "true" if "C" in enabled else "false"
            env["POLARIS_ARB_ENABLE_STRATEGY_F"] = "true" if "F" in enabled else "false"
            env["POLARIS_ARB_ENABLE_STRATEGY_G"] = "true" if "G" in enabled else "false"
            _apply_variant_overrides(env, variant)

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
                run_tag_case,
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
            strategy_set = _canonical_strategy_set(enabled)
            scope = "isolated" if split_by_strategy else "shared"
            run_status = "completed" if completed.returncode == 0 else "failed"
            experiment_run_id = str(uuid4())
            experiment_rows.append(
                {
                    "experiment_run_id": experiment_run_id,
                    "run_tag": run_tag_case,
                    "source_code": source,
                    "profile": profile_name,
                    "strategy_set": strategy_set,
                    "scope": scope,
                    "variant": variant,
                    "bankroll_usd": bankroll_usd,
                    "params": {
                        "start": start,
                        "end": end,
                        "fast": bool(fast),
                        "profile": profile_name,
                        "strategy_set": strategy_set,
                        "variant": variant,
                        "split_by_strategy": split_by_strategy,
                    },
                    "metadata": {
                        "launcher": "arb-replay-matrix",
                        "enabled_strategies": sorted(enabled),
                        "exit_code": int(completed.returncode),
                        "log_path": str(log_path),
                    },
                    "status": run_status,
                }
            )
            rows.append(
                {
                    "name": f"{profile_name}:{name}",
                    "source": source,
                    "run_tag": run_tag_case,
                    "profile": profile_name,
                    "strategy_set": strategy_set,
                    "scope": scope,
                    "variant": variant,
                    "split_by_strategy": split_by_strategy,
                    "enabled": sorted(enabled),
                    "exit_code": int(completed.returncode),
                    "log": str(log_path),
                }
            )

    async def _record_runs() -> None:
        db = _create_database(settings)
        await db.open()
        try:
            await _upsert_experiment_runs(
                db,
                mode=RunMode.PAPER_REPLAY.value,
                run_tag=matrix_tag,
                rows=experiment_rows,
                started_at=started_at,
            )
            for row in experiment_rows:
                await db.execute(
                    """
                    update arb_experiment_run
                    set status = %s::text, ended_at = now(), updated_at = now()
                    where experiment_run_id = %s::text
                    """,
                    (str(row.get("status") or "completed"), str(row.get("experiment_run_id"))),
                )
        finally:
            await db.close()

    asyncio.run(_record_runs())

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
    by_experiment: Annotated[bool, typer.Option("--by-experiment/--no-by-experiment")] = False,
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
            if by_experiment:
                result["by_experiment"] = await _arb_summary_by_experiment(
                    ctx.db,
                    since_hours=since_hours,
                    mode_filter=mode_filter,
                    source_filter=source_filter,
                    run_tag_filter=run_tag_filter,
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


@app.command("arb-experiment-score")
def arb_experiment_score(
    since_hours: Annotated[int, typer.Option("--since-hours", min=1, max=24 * 30)] = 8,
    mode: Annotated[str, typer.Option("--mode", help="shadow|paper_live|paper_replay|live")] = "paper_live",
    source_code: Annotated[str, typer.Option("--source", help="source code, or all")] = "all",
    run_tag: Annotated[str, typer.Option("--run-tag", help="current|all|custom")] = "current",
    default_bankroll_usd: Annotated[float, typer.Option("--default-bankroll-usd", min=1.0)] = 50.0,
    resource_penalty: Annotated[float, typer.Option("--resource-penalty", min=0.0)] = 0.0,
    persist: Annotated[bool, typer.Option("--persist/--no-persist")] = True,
    output: Annotated[str, typer.Option("--output", help="Optional JSON output path")] = "",
) -> None:
    """Compute experiment scores and rank all matrix sources."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    allowed_modes = {"shadow", "paper_live", "paper_replay", "live"}
    mode_normalized = mode.strip().lower()
    if mode_normalized not in allowed_modes:
        raise typer.BadParameter("mode must be one of: shadow, paper_live, paper_replay, live")
    source_normalized = source_code.strip().lower()
    source_filter = None if source_normalized == "all" else source_normalized

    async def _run() -> None:
        ctx = await create_arb_runtime(settings)
        try:
            run_tag_filter = await _resolve_run_tag_filter_db(
                ctx.db,
                run_tag,
                mode_filter=mode_normalized,
                source_filter=source_filter,
                current_tag=ctx.orchestrator.config.run_tag,
            )
            rows = await _compute_experiment_scores(
                ctx.db,
                since_hours=since_hours,
                mode_filter=mode_normalized,
                source_filter=source_filter,
                run_tag_filter=run_tag_filter,
                default_bankroll_usd=default_bankroll_usd,
                resource_penalty=resource_penalty,
                persist=persist,
            )
            payload = {
                "window": {
                    "since_hours": since_hours,
                    "mode": mode_normalized,
                    "source_code": source_filter or "all",
                    "run_tag": run_tag_filter or "all",
                },
                "rows": rows,
            }
            text = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
            if output:
                out_path = Path(output).expanduser()
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(text, encoding="utf-8")
                typer.echo(f"arb-experiment-score saved: {out_path}")
            typer.echo(text)
        finally:
            await close_arb_runtime(ctx)

    asyncio.run(_run())


@app.command("arb-experiment-promote")
def arb_experiment_promote(
    since_hours: Annotated[int, typer.Option("--since-hours", min=1, max=24 * 30)] = 8,
    mode: Annotated[str, typer.Option("--mode", help="shadow|paper_live|paper_replay|live")] = "paper_live",
    source_code: Annotated[str, typer.Option("--source", help="source code, or all")] = "all",
    run_tag: Annotated[str, typer.Option("--run-tag", help="current|all|custom")] = "current",
    top_n: Annotated[int, typer.Option("--top-n", min=1, max=20)] = 2,
    min_score: Annotated[float, typer.Option("--min-score")] = 0.0,
    max_drawdown_pct: Annotated[float, typer.Option("--max-drawdown-pct", min=0.0, max=1.0)] = 0.08,
    min_execution_rate: Annotated[float, typer.Option("--min-execution-rate", min=0.0, max=1.0)] = 0.02,
    max_system_error_rate: Annotated[float, typer.Option("--max-system-error-rate", min=0.0, max=1.0)] = 0.01,
    apply: Annotated[bool, typer.Option("--apply/--dry-run")] = False,
) -> None:
    """Promote top experiment candidates that pass go-live gates."""
    refresh_process_env_from_file(preserve_existing=True)
    load_settings.cache_clear()
    settings = load_settings()
    setup_logging(settings.log_level)
    _ensure_windows_selector_loop()

    allowed_modes = {"shadow", "paper_live", "paper_replay", "live"}
    mode_normalized = mode.strip().lower()
    if mode_normalized not in allowed_modes:
        raise typer.BadParameter("mode must be one of: shadow, paper_live, paper_replay, live")
    source_normalized = source_code.strip().lower()
    source_filter = None if source_normalized == "all" else source_normalized

    async def _run() -> None:
        ctx = await create_arb_runtime(settings)
        try:
            run_tag_filter = await _resolve_run_tag_filter_db(
                ctx.db,
                run_tag,
                mode_filter=mode_normalized,
                source_filter=source_filter,
                current_tag=ctx.orchestrator.config.run_tag,
            )
            rows = await _compute_experiment_scores(
                ctx.db,
                since_hours=since_hours,
                mode_filter=mode_normalized,
                source_filter=source_filter,
                run_tag_filter=run_tag_filter,
                default_bankroll_usd=50.0,
                resource_penalty=0.0,
                persist=True,
            )
            candidates: list[dict[str, object]] = []
            rejected: list[dict[str, object]] = []
            for row in rows:
                evaluation_net = float(row.get("evaluation_net_pnl_usd", 0.0) or 0.0)
                drawdown_pct = float(row.get("max_drawdown_pct", 0.0) or 0.0)
                execution_rate = float(row.get("execution_rate", 0.0) or 0.0)
                system_error_rate = float(row.get("system_error_rate", 0.0) or 0.0)
                score_total = float(row.get("score_total", 0.0) or 0.0)
                reasons: list[str] = []
                if evaluation_net <= 0:
                    reasons.append("evaluation_net_pnl_usd<=0")
                if drawdown_pct > max_drawdown_pct:
                    reasons.append("max_drawdown_pct_exceeded")
                if execution_rate < min_execution_rate:
                    reasons.append("execution_rate_too_low")
                if system_error_rate > max_system_error_rate:
                    reasons.append("system_error_rate_too_high")
                if score_total < min_score:
                    reasons.append("score_below_min")

                if reasons:
                    copy_row = dict(row)
                    copy_row["reject_reasons"] = reasons
                    rejected.append(copy_row)
                else:
                    candidates.append(row)

            candidates = sorted(candidates, key=lambda item: float(item.get("score_total", 0.0) or 0.0), reverse=True)
            promoted = candidates[:top_n]

            if apply and promoted:
                for row in promoted:
                    experiment_run_id = str(row.get("experiment_run_id") or "")
                    if not experiment_run_id:
                        continue
                    await ctx.db.execute(
                        """
                        update arb_experiment_run
                        set status = 'promoted',
                            metadata = coalesce(metadata, '{}'::jsonb) || jsonb_build_object(
                                'promoted_at', now(),
                                'promote_score', %s::numeric,
                                'promote_window_hours', %s::int
                            ),
                            updated_at = now()
                        where experiment_run_id = %s::text
                        """,
                        (
                            float(row.get("score_total", 0.0) or 0.0),
                            since_hours,
                            experiment_run_id,
                        ),
                    )

            payload = {
                "mode": mode_normalized,
                "source_code": source_filter or "all",
                "run_tag": run_tag_filter or "all",
                "since_hours": since_hours,
                "gates": {
                    "evaluation_net_pnl_usd_gt": 0.0,
                    "max_drawdown_pct_le": max_drawdown_pct,
                    "min_execution_rate": min_execution_rate,
                    "max_system_error_rate": max_system_error_rate,
                    "min_score": min_score,
                },
                "apply": apply,
                "promoted": promoted,
                "rejected": rejected,
            }
            typer.echo(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
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


def _to_float(value: object, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: object, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _compute_max_drawdown(bankroll_usd: float, pnl_series: list[float]) -> tuple[float, float]:
    equity = bankroll_usd
    peak = bankroll_usd
    max_drawdown = 0.0
    for pnl in pnl_series:
        equity += pnl
        if equity > peak:
            peak = equity
        drawdown = peak - equity
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    drawdown_pct = max_drawdown / bankroll_usd if bankroll_usd > 0 else 0.0
    return max_drawdown, drawdown_pct


def _pick_stage(stage_bins: list[tuple[str, float, float]], hours_to_close: float) -> str | None:
    for name, lower, upper in stage_bins:
        if lower <= hours_to_close < upper:
            return name
    return None


def _parse_pmf(value: object) -> dict[str, float]:
    if isinstance(value, dict):
        out: dict[str, float] = {}
        for key, raw in value.items():
            try:
                out[str(key)] = float(raw)
            except (TypeError, ValueError):
                continue
        return out
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if not isinstance(decoded, dict):
            return {}
        out: dict[str, float] = {}
        for key, raw in decoded.items():
            try:
                out[str(key)] = float(raw)
            except (TypeError, ValueError):
                continue
        return out
    return {}


def _argmax_label(pmf: dict[str, float]) -> str:
    if not pmf:
        return ""
    return max(sorted(pmf.keys()), key=lambda key: float(pmf.get(key, 0.0)))


def _classify_m4_dataset_row(row: dict[str, object]) -> str | None:
    end_date = row.get("end_date")
    as_of_ts = row.get("as_of_ts")
    if not isinstance(end_date, datetime) or not isinstance(as_of_ts, datetime):
        return "invalid_time"
    if (end_date - as_of_ts).total_seconds() <= 0:
        return "non_positive_hours_to_close"
    prior = _parse_pmf(row.get("prior_pmf"))
    posterior = _parse_pmf(row.get("posterior_pmf"))
    if not prior or not posterior:
        return "pmf_missing"
    final_count = int(row.get("final_count") or 0)
    true_label = infer_true_bucket_label(final_count, posterior.keys())
    if true_label is None:
        return "true_label_not_in_bucket_space"
    return None


def _audit_m4_dataset_rows(rows: list[dict[str, object]]) -> dict[str, object]:
    reasons_global: Counter[str] = Counter()
    run_stats: dict[str, dict[str, object]] = {}
    global_markets_total: set[str] = set()
    global_markets_scorable: set[str] = set()
    rows_total = 0
    rows_scorable = 0

    for row in rows:
        rows_total += 1
        run_tag = str(row.get("run_tag") or "_missing")
        market_id = str(row.get("market_id") or "_missing")
        global_markets_total.add(market_id)

        slot = run_stats.setdefault(
            run_tag,
            {
                "rows_total": 0,
                "rows_scorable": 0,
                "markets_total": set(),
                "markets_scorable": set(),
                "reasons": Counter(),
            },
        )
        slot["rows_total"] = int(slot["rows_total"]) + 1
        cast_markets_total = slot["markets_total"]
        if isinstance(cast_markets_total, set):
            cast_markets_total.add(market_id)

        reason = _classify_m4_dataset_row(row)
        if reason is None:
            rows_scorable += 1
            global_markets_scorable.add(market_id)
            slot["rows_scorable"] = int(slot["rows_scorable"]) + 1
            cast_markets_scorable = slot["markets_scorable"]
            if isinstance(cast_markets_scorable, set):
                cast_markets_scorable.add(market_id)
            continue
        reasons_global[reason] += 1
        cast_reasons = slot["reasons"]
        if isinstance(cast_reasons, Counter):
            cast_reasons[reason] += 1

    run_rows: list[dict[str, object]] = []
    for run_tag in sorted(run_stats.keys()):
        slot = run_stats[run_tag]
        per_rows_total = int(slot["rows_total"])
        per_rows_scorable = int(slot["rows_scorable"])
        per_markets_total = len(slot["markets_total"]) if isinstance(slot["markets_total"], set) else 0
        per_markets_scorable = len(slot["markets_scorable"]) if isinstance(slot["markets_scorable"], set) else 0
        cast_reasons = slot["reasons"]
        run_rows.append(
            {
                "run_tag": run_tag,
                "rows_total": per_rows_total,
                "rows_scorable": per_rows_scorable,
                "rows_coverage_pct": round((per_rows_scorable / per_rows_total) * 100.0, 4) if per_rows_total > 0 else 0.0,
                "markets_total": per_markets_total,
                "markets_scorable": per_markets_scorable,
                "markets_coverage_pct": round((per_markets_scorable / per_markets_total) * 100.0, 4)
                if per_markets_total > 0
                else 0.0,
                "unscorable_reason_distribution": dict(cast_reasons) if isinstance(cast_reasons, Counter) else {},
            }
        )

    run_rows.sort(key=lambda item: (-int(item.get("rows_total") or 0), str(item.get("run_tag") or "")))
    rows_unscorable = rows_total - rows_scorable
    markets_total = len(global_markets_total)
    markets_scorable = len(global_markets_scorable)
    return {
        "rows_total": rows_total,
        "rows_scorable": rows_scorable,
        "rows_unscorable": rows_unscorable,
        "rows_coverage_pct": round((rows_scorable / rows_total) * 100.0, 4) if rows_total > 0 else 0.0,
        "markets_total": markets_total,
        "scorable_market_count": markets_scorable,
        "markets_unscorable": markets_total - markets_scorable,
        "market_coverage_pct": round((markets_scorable / markets_total) * 100.0, 4) if markets_total > 0 else 0.0,
        "unscorable_reason_distribution": dict(reasons_global),
        "run_tag_coverage": run_rows,
    }


async def _arb_summary_by_experiment(
    db: Database,
    *,
    since_hours: int,
    mode_filter: str | None,
    source_filter: str | None,
    run_tag_filter: str | None,
) -> list[dict[str, object]]:
    rows = await _compute_experiment_scores(
        db,
        since_hours=since_hours,
        mode_filter=mode_filter,
        source_filter=source_filter,
        run_tag_filter=run_tag_filter,
        default_bankroll_usd=50.0,
        resource_penalty=0.0,
        persist=False,
    )
    summary_rows: list[dict[str, object]] = []
    for row in rows:
        summary_rows.append(
            {
                "source_code": row.get("source_code"),
                "profile": row.get("profile"),
                "strategy_set": row.get("strategy_set"),
                "scope": row.get("scope"),
                "variant": row.get("variant"),
                "bankroll_usd": row.get("bankroll_usd"),
                "signals_found": row.get("signals_found"),
                "signals_executed": row.get("signals_executed"),
                "signals_rejected": row.get("signals_rejected"),
                "trades": row.get("trades"),
                "evaluation_net_pnl_usd": row.get("evaluation_net_pnl_usd"),
                "expected_net_pnl_usd": row.get("expected_net_pnl_usd"),
                "mark_to_book_net_pnl_usd": row.get("mark_to_book_net_pnl_usd"),
                "realized_net_pnl_usd": row.get("realized_net_pnl_usd"),
                "max_drawdown_usd": row.get("max_drawdown_usd"),
                "max_drawdown_pct": row.get("max_drawdown_pct"),
                "execution_rate": row.get("execution_rate"),
                "reject_rate": row.get("reject_rate"),
                "system_error_rate": row.get("system_error_rate"),
                "score_total": row.get("score_total"),
            }
        )
    return summary_rows


async def _compute_experiment_scores(
    db: Database,
    *,
    since_hours: int,
    mode_filter: str | None,
    source_filter: str | None,
    run_tag_filter: str | None,
    default_bankroll_usd: float,
    resource_penalty: float,
    persist: bool,
) -> list[dict[str, object]]:
    now_utc = datetime.now(tz=UTC)
    window_start = now_utc - timedelta(hours=max(1, since_hours))
    known_profiles = set(_PAPER_PROFILE_PRESETS.keys())
    filters = (
        window_start,
        mode_filter,
        mode_filter,
        source_filter,
        source_filter,
        run_tag_filter,
        run_tag_filter,
    )

    signal_rows = await db.fetch_all(
        """
        select
            source_code,
            count(*) as signals_found,
            sum((status = 'executed')::int) as signals_executed,
            sum((status = 'rejected')::int) as signals_rejected,
            sum((status = 'expired')::int) as signals_expired
        from arb_signal
        where created_at >= %s::timestamptz
          and (%s::text is null or mode = %s::text)
          and (%s::text is null or source_code = %s::text)
          and (%s::text is null or coalesce(features->>'run_tag', '') = %s::text)
        group by source_code
        """,
        filters,
    )

    trade_rows = await db.fetch_all(
        """
        select
            source_code,
            count(*) as trades,
            sum(
                (
                    case
                        when strategy_code in ('A', 'B', 'C')
                        then coalesce(nullif(metadata->>'expected_net_pnl_usd', '')::numeric, 0)
                        else coalesce(nullif(metadata->>'mark_to_book_net_pnl_usd', '')::numeric, net_pnl_usd)
                    end > 0
                )::int
            ) as wins,
            sum(
                (
                    case
                        when strategy_code in ('A', 'B', 'C')
                        then coalesce(nullif(metadata->>'expected_net_pnl_usd', '')::numeric, 0)
                        else coalesce(nullif(metadata->>'mark_to_book_net_pnl_usd', '')::numeric, net_pnl_usd)
                    end < 0
                )::int
            ) as losses,
            coalesce(sum(net_pnl_usd), 0) as realized_net_pnl_usd,
            coalesce(
                sum(coalesce(nullif(metadata->>'mark_to_book_net_pnl_usd', '')::numeric, net_pnl_usd)),
                0
            ) as mark_to_book_net_pnl_usd,
            coalesce(
                sum(coalesce(nullif(metadata->>'expected_net_pnl_usd', '')::numeric, 0)),
                0
            ) as expected_net_pnl_usd,
            coalesce(
                sum(
                    case
                        when strategy_code in ('A', 'B', 'C')
                        then coalesce(nullif(metadata->>'expected_net_pnl_usd', '')::numeric, 0)
                        else coalesce(nullif(metadata->>'mark_to_book_net_pnl_usd', '')::numeric, net_pnl_usd)
                    end
                ),
                0
            ) as evaluation_net_pnl_usd,
            coalesce(sum(capital_used_usd), 0) as turnover_usd
        from arb_trade_result
        where created_at >= %s::timestamptz
          and (%s::text is null or mode = %s::text)
          and (%s::text is null or source_code = %s::text)
          and (%s::text is null or coalesce(metadata->>'run_tag', '') = %s::text)
        group by source_code
        """,
        filters,
    )

    timeline_rows = await db.fetch_all(
        """
        select
            source_code,
            created_at,
            case
                when strategy_code in ('A', 'B', 'C')
                then coalesce(nullif(metadata->>'expected_net_pnl_usd', '')::numeric, 0)
                else coalesce(nullif(metadata->>'mark_to_book_net_pnl_usd', '')::numeric, net_pnl_usd)
            end as evaluation_trade_pnl_usd
        from arb_trade_result
        where created_at >= %s::timestamptz
          and (%s::text is null or mode = %s::text)
          and (%s::text is null or source_code = %s::text)
          and (%s::text is null or coalesce(metadata->>'run_tag', '') = %s::text)
        order by source_code asc, created_at asc, trade_id asc
        """,
        filters,
    )

    risk_error_rows = await db.fetch_all(
        """
        select
            source_code,
            count(*) as system_errors
        from arb_risk_event
        where created_at >= %s::timestamptz
          and (%s::text is null or mode = %s::text)
          and (%s::text is null or source_code = %s::text)
          and (%s::text is null or coalesce(payload->>'run_tag', '') = %s::text)
          and lower(coalesce(severity, '')) in ('error', 'critical')
        group by source_code
        """,
        filters,
    )

    run_rows: list[dict[str, object]] = []
    has_experiment_run = await _table_exists(db, "arb_experiment_run")
    if has_experiment_run:
        run_rows = await db.fetch_all(
            """
            select distinct on (source_code)
                experiment_run_id,
                source_code,
                run_tag,
                profile,
                strategy_set,
                scope,
                variant,
                bankroll_usd
            from arb_experiment_run
            where (%s::text is null or mode = %s::text)
              and (%s::text is null or source_code = %s::text)
              and (%s::text is null or run_tag = %s::text)
            order by source_code, updated_at desc
            """,
            (
                mode_filter,
                mode_filter,
                source_filter,
                source_filter,
                run_tag_filter,
                run_tag_filter,
            ),
        )

    signal_map = {str(row["source_code"]): row for row in signal_rows}
    trade_map = {str(row["source_code"]): row for row in trade_rows}
    run_map = {str(row["source_code"]): row for row in run_rows}
    risk_map = {str(row["source_code"]): row for row in risk_error_rows}
    timeline_map: dict[str, list[float]] = {}
    for row in timeline_rows:
        source = str(row["source_code"])
        timeline_map.setdefault(source, []).append(_to_float(row.get("evaluation_trade_pnl_usd")))

    all_sources = sorted(
        set(signal_map.keys()) | set(trade_map.keys()) | set(run_map.keys()) | set(risk_map.keys()) | set(timeline_map.keys())
    )
    rows: list[dict[str, object]] = []
    missing_run_rows: list[dict[str, object]] = []
    for source in all_sources:
        signal_row = signal_map.get(source, {})
        trade_row = trade_map.get(source, {})
        risk_row = risk_map.get(source, {})
        run_row = run_map.get(source, {})

        dims = parse_experiment_dimensions(source, known_profiles=known_profiles)
        profile = str(run_row.get("profile") or dims.profile or "unknown")
        strategy_set = str(run_row.get("strategy_set") or dims.strategy_set or "unknown")
        scope = str(run_row.get("scope") or dims.scope or "shared")
        variant = str(run_row.get("variant") or dims.variant or "base")
        bankroll = _to_float(run_row.get("bankroll_usd"), default_bankroll_usd)
        if bankroll <= 0:
            bankroll = default_bankroll_usd

        signals_found = _to_int(signal_row.get("signals_found"))
        signals_executed = _to_int(signal_row.get("signals_executed"))
        signals_rejected = _to_int(signal_row.get("signals_rejected"))
        signals_expired = _to_int(signal_row.get("signals_expired"))
        trades = _to_int(trade_row.get("trades"))
        wins = _to_int(trade_row.get("wins"))
        losses = _to_int(trade_row.get("losses"))

        realized_net = _to_float(trade_row.get("realized_net_pnl_usd"))
        mark_to_book_net = _to_float(trade_row.get("mark_to_book_net_pnl_usd"))
        expected_net = _to_float(trade_row.get("expected_net_pnl_usd"))
        evaluation_net = _to_float(trade_row.get("evaluation_net_pnl_usd"))
        turnover = _to_float(trade_row.get("turnover_usd"))
        pnl_gap = evaluation_net - expected_net

        execution_rate = signals_executed / signals_found if signals_found > 0 else 0.0
        reject_rate = signals_rejected / signals_found if signals_found > 0 else 0.0
        win_rate = wins / trades if trades > 0 else 0.0
        system_error_count = _to_int(risk_row.get("system_errors"))
        system_error_rate = system_error_count / signals_found if signals_found > 0 else 0.0

        max_drawdown_usd, max_drawdown_pct = _compute_max_drawdown(bankroll, timeline_map.get(source, []))
        score_total, score_breakdown = compute_experiment_score(
            ExperimentScoreInput(
                bankroll_usd=bankroll,
                evaluation_net_pnl_usd=evaluation_net,
                expected_net_pnl_usd=expected_net,
                max_drawdown_usd=max_drawdown_usd,
                reject_rate=reject_rate,
                execution_rate=execution_rate,
                system_error_rate=system_error_rate,
                resource_penalty=resource_penalty,
            )
        )

        experiment_run_id = str(run_row.get("experiment_run_id") or "")
        if not experiment_run_id and has_experiment_run:
            experiment_run_id = str(uuid4())
            missing_run_rows.append(
                {
                    "experiment_run_id": experiment_run_id,
                    "run_tag": run_tag_filter or f"window-{since_hours}h",
                    "source_code": source,
                    "profile": profile,
                    "strategy_set": strategy_set,
                    "scope": scope,
                    "variant": variant,
                    "bankroll_usd": bankroll,
                    "params": {
                        "since_hours": since_hours,
                        "mode": mode_filter,
                        "source_code": source,
                    },
                    "metadata": {
                        "launcher": "arb-experiment-score",
                    },
                }
            )

        rows.append(
            {
                "experiment_run_id": experiment_run_id,
                "mode": mode_filter,
                "run_tag": str(run_row.get("run_tag") or run_tag_filter or "all"),
                "source_code": source,
                "profile": profile,
                "strategy_set": strategy_set,
                "scope": scope,
                "variant": variant,
                "bankroll_usd": bankroll,
                "signals_found": signals_found,
                "signals_executed": signals_executed,
                "signals_rejected": signals_rejected,
                "signals_expired": signals_expired,
                "trades": trades,
                "wins": wins,
                "losses": losses,
                "realized_net_pnl_usd": realized_net,
                "mark_to_book_net_pnl_usd": mark_to_book_net,
                "expected_net_pnl_usd": expected_net,
                "evaluation_net_pnl_usd": evaluation_net,
                "pnl_gap_vs_expected_usd": pnl_gap,
                "turnover_usd": turnover,
                "max_drawdown_usd": max_drawdown_usd,
                "max_drawdown_pct": max_drawdown_pct,
                "execution_rate": execution_rate,
                "reject_rate": reject_rate,
                "win_rate": win_rate,
                "system_error_rate": system_error_rate,
                "resource_penalty": resource_penalty,
                "score_total": score_total,
                "score_breakdown": score_breakdown,
            }
        )

    rows = sorted(rows, key=lambda item: float(item.get("score_total", 0.0) or 0.0), reverse=True)
    if has_experiment_run and missing_run_rows:
        await _upsert_experiment_runs(
            db,
            mode=mode_filter,
            run_tag=run_tag_filter or f"window-{since_hours}h",
            rows=missing_run_rows,
            started_at=now_utc,
        )
        missing_by_source = {str(row["source_code"]): str(row["experiment_run_id"]) for row in missing_run_rows}
        for row in rows:
            source = str(row.get("source_code") or "")
            if source in missing_by_source and not row.get("experiment_run_id"):
                row["experiment_run_id"] = missing_by_source[source]

    if persist and has_experiment_run and await _table_exists(db, "arb_experiment_metric"):
        for row in rows:
            await db.execute(
                """
                insert into arb_experiment_metric(
                    experiment_run_id, mode, run_tag, source_code, profile, strategy_set, scope, variant,
                    since_hours, signals_found, signals_executed, signals_rejected, signals_expired,
                    trades, wins, losses, realized_net_pnl_usd, mark_to_book_net_pnl_usd, expected_net_pnl_usd,
                    evaluation_net_pnl_usd, pnl_gap_vs_expected_usd, turnover_usd, max_drawdown_usd, max_drawdown_pct,
                    execution_rate, reject_rate, win_rate, system_error_rate, resource_penalty,
                    score_total, score_breakdown, metadata, created_at
                )
                values (
                    %s::text, %s::text, %s::text, %s::text, %s::text, %s::text, %s::text, %s::text,
                    %s::int, %s::int, %s::int, %s::int, %s::int,
                    %s::int, %s::int, %s::int, %s::numeric, %s::numeric, %s::numeric,
                    %s::numeric, %s::numeric, %s::numeric, %s::numeric, %s::numeric,
                    %s::numeric, %s::numeric, %s::numeric, %s::numeric, %s::numeric,
                    %s::numeric, %s::jsonb, %s::jsonb, now()
                )
                """,
                (
                    str(row.get("experiment_run_id") or ""),
                    mode_filter,
                    str(row.get("run_tag") or ""),
                    str(row.get("source_code") or ""),
                    str(row.get("profile") or "unknown"),
                    str(row.get("strategy_set") or "unknown"),
                    str(row.get("scope") or "shared"),
                    str(row.get("variant") or "base"),
                    int(since_hours),
                    int(row.get("signals_found") or 0),
                    int(row.get("signals_executed") or 0),
                    int(row.get("signals_rejected") or 0),
                    int(row.get("signals_expired") or 0),
                    int(row.get("trades") or 0),
                    int(row.get("wins") or 0),
                    int(row.get("losses") or 0),
                    float(row.get("realized_net_pnl_usd") or 0.0),
                    float(row.get("mark_to_book_net_pnl_usd") or 0.0),
                    float(row.get("expected_net_pnl_usd") or 0.0),
                    float(row.get("evaluation_net_pnl_usd") or 0.0),
                    float(row.get("pnl_gap_vs_expected_usd") or 0.0),
                    float(row.get("turnover_usd") or 0.0),
                    float(row.get("max_drawdown_usd") or 0.0),
                    float(row.get("max_drawdown_pct") or 0.0),
                    float(row.get("execution_rate") or 0.0),
                    float(row.get("reject_rate") or 0.0),
                    float(row.get("win_rate") or 0.0),
                    float(row.get("system_error_rate") or 0.0),
                    float(row.get("resource_penalty") or 0.0),
                    float(row.get("score_total") or 0.0),
                    json.dumps(row.get("score_breakdown", {}), ensure_ascii=True),
                    json.dumps({"window_start": window_start.isoformat(), "window_end": now_utc.isoformat()}, ensure_ascii=True),
                ),
            )
    return rows


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
    if await _table_exists(db, "arb_scan_diag"):
        counts["arb_scan_diag"] = _as_int(
            await db.fetch_one(
                """
                select count(*) as c
                from arb_scan_diag
                where (%s::text is null or mode = %s::text)
                  and (%s::text is null or source_code = %s::text)
                  and (%s::text is null or run_tag = %s::text)
                  and (%s::timestamptz is null or created_at >= %s::timestamptz)
                """,
                signal_params,
            )
        )
    if await _table_exists(db, "arb_portfolio_snapshot"):
        counts["arb_portfolio_snapshot"] = _as_int(
            await db.fetch_one(
                """
                select count(*) as c
                from arb_portfolio_snapshot
                where (%s::text is null or mode = %s::text)
                  and (%s::text is null or source_code = %s::text)
                  and (%s::text is null or run_tag = %s::text)
                  and (%s::timestamptz is null or created_at >= %s::timestamptz)
                """,
                signal_params,
            )
        )
    if await _table_exists(db, "arb_experiment_run"):
        counts["arb_experiment_run"] = _as_int(
            await db.fetch_one(
                """
                select count(*) as c
                from arb_experiment_run
                where (%s::text is null or mode = %s::text)
                  and (%s::text is null or source_code = %s::text)
                  and (%s::text is null or run_tag = %s::text)
                  and (%s::timestamptz is null or started_at >= %s::timestamptz)
                """,
                signal_params,
            )
        )
    if await _table_exists(db, "arb_experiment_metric"):
        counts["arb_experiment_metric"] = _as_int(
            await db.fetch_one(
                """
                select count(*) as c
                from arb_experiment_metric
                where (%s::text is null or mode = %s::text)
                  and (%s::text is null or source_code = %s::text)
                  and (%s::text is null or run_tag = %s::text)
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
        has_run_tag = await _table_has_column(db, "arb_position_lot", "run_tag")
        if has_run_tag:
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
        else:
            counts["arb_position_lot"] = _as_int(
                await db.fetch_one(
                    """
                    select count(*) as c
                    from arb_position_lot
                    where (%s::text is null or mode = %s::text)
                      and (%s::text is null or source_code = %s::text)
                      and (%s::timestamptz is null or opened_at >= %s::timestamptz)
                    """,
                    (
                        mode_filter,
                        mode_filter,
                        source_filter,
                        source_filter,
                        since_start,
                        since_start,
                    ),
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

    if await _table_exists(db, "arb_scan_diag"):
        await db.execute(
            """
            delete from arb_scan_diag
            where (%s::text is null or mode = %s::text)
              and (%s::text is null or source_code = %s::text)
              and (%s::text is null or run_tag = %s::text)
              and (%s::timestamptz is null or created_at >= %s::timestamptz)
            """,
            signal_params,
        )

    if await _table_exists(db, "arb_portfolio_snapshot"):
        await db.execute(
            """
            delete from arb_portfolio_snapshot
            where (%s::text is null or mode = %s::text)
              and (%s::text is null or source_code = %s::text)
              and (%s::text is null or run_tag = %s::text)
              and (%s::timestamptz is null or created_at >= %s::timestamptz)
            """,
            signal_params,
        )

    if await _table_exists(db, "arb_experiment_metric"):
        await db.execute(
            """
            delete from arb_experiment_metric
            where (%s::text is null or mode = %s::text)
              and (%s::text is null or source_code = %s::text)
              and (%s::text is null or run_tag = %s::text)
              and (%s::timestamptz is null or created_at >= %s::timestamptz)
            """,
            signal_params,
        )

    if await _table_exists(db, "arb_experiment_run"):
        await db.execute(
            """
            delete from arb_experiment_run
            where (%s::text is null or mode = %s::text)
              and (%s::text is null or source_code = %s::text)
              and (%s::text is null or run_tag = %s::text)
              and (%s::timestamptz is null or started_at >= %s::timestamptz)
            """,
            signal_params,
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
        has_run_tag = await _table_has_column(db, "arb_position_lot", "run_tag")
        if has_run_tag:
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
        else:
            await db.execute(
                """
                delete from arb_position_lot
                where (%s::text is null or mode = %s::text)
                  and (%s::text is null or source_code = %s::text)
                  and (%s::timestamptz is null or opened_at >= %s::timestamptz)
                """,
                (
                    mode_filter,
                    mode_filter,
                    source_filter,
                    source_filter,
                    since_start,
                    since_start,
                ),
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
    if config.max_exposure_usd < (4.0 * config.single_risk_usd):
        suggestions.append(
            f"max_exposure_usd({config.max_exposure_usd:.3f}) < 4 * single_risk_usd({config.single_risk_usd:.3f})，并发空间过窄"
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
            "exposure_ge_4x_single_risk": config.max_exposure_usd >= (4.0 * config.single_risk_usd),
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


async def _table_exists(db: Database, table: str) -> bool:
    row = await db.fetch_one("select to_regclass(%s) as reg", (f"public.{table}",))
    return bool(row and row.get("reg"))


async def _table_has_column(db: Database, table: str, column: str) -> bool:
    row = await db.fetch_one(
        """
        select 1
        from information_schema.columns
        where table_schema = 'public'
          and table_name = %s
          and column_name = %s
        limit 1
        """,
        (table, column),
    )
    return row is not None


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
