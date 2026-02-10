from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from polaris.db.pool import Database
from polaris.ops.exporter import export_table

DEFAULT_EXPORT_TABLES: tuple[str, ...] = (
    "fact_tweet_post",
    "fact_quote_1m",
    "fact_market_state_snapshot",
    "ops_collector_run",
    "view_quote_latest",
)


@dataclass(slots=True)
class BackupArtifact:
    path: Path
    size_bytes: int
    sha256: str

    @classmethod
    def from_path(cls, path: Path) -> BackupArtifact:
        return cls(path=path, size_bytes=path.stat().st_size, sha256=file_sha256(path))


@dataclass(slots=True)
class BackupResult:
    backup_dir: Path
    artifacts: list[BackupArtifact]
    pruned_dirs: list[Path]


@dataclass(slots=True)
class PgDumpPlan:
    command: list[str]
    env: dict[str, str]


def backup_label(prefix: str | None = None) -> str:
    stamp = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
    if not prefix:
        return stamp
    cleaned = re.sub(r"[^0-9A-Za-z_-]+", "-", prefix.strip())
    cleaned = cleaned.strip("-_")
    if not cleaned:
        return stamp
    return f"{stamp}_{cleaned}"


def build_pg_dump_plan(
    database_url: str,
    output_file: Path,
    pg_dump_bin: str = "pg_dump",
) -> PgDumpPlan:
    parsed = urlparse(database_url)
    if parsed.scheme not in {"postgresql", "postgres"}:
        raise ValueError("database_url must be postgresql:// or postgres://")
    if not parsed.path or parsed.path == "/":
        raise ValueError("database_url missing database name")

    username = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    host = parsed.hostname or "localhost"
    port = str(parsed.port or 5432)
    database = unquote(parsed.path.lstrip("/"))
    query = parse_qs(parsed.query)

    env = os.environ.copy()
    env["PGHOST"] = host
    env["PGPORT"] = port
    env["PGUSER"] = username
    env["PGDATABASE"] = database
    if password:
        env["PGPASSWORD"] = password
    sslmode = query.get("sslmode", [None])[0]
    if sslmode:
        env["PGSSLMODE"] = sslmode

    command = [
        pg_dump_bin,
        "--format=custom",
        "--compress=6",
        "--no-owner",
        "--no-privileges",
        "--file",
        str(output_file),
    ]
    return PgDumpPlan(command=command, env=env)


def run_pg_dump(
    database_url: str,
    output_file: Path,
    pg_dump_bin: str = "pg_dump",
    timeout_sec: int = 1800,
) -> Path:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    plan = build_pg_dump_plan(database_url, output_file, pg_dump_bin=pg_dump_bin)
    result = subprocess.run(
        plan.command,
        env=plan.env,
        text=True,
        capture_output=True,
        timeout=timeout_sec,
        check=False,
    )
    if result.returncode != 0:
        err = result.stderr.strip() or "pg_dump failed"
        raise RuntimeError(err)
    if not output_file.exists() or output_file.stat().st_size == 0:
        raise RuntimeError("pg_dump output not created or empty")
    return output_file


async def export_backup_tables(
    db: Database,
    backup_dir: Path,
    tables: list[str],
    since_hours: int,
    limit: int,
) -> list[Path]:
    export_dir = backup_dir / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    created: list[Path] = []
    for table in tables:
        out = export_dir / f"{table}.csv"
        await export_table(
            db=db,
            table=table,
            export_format="csv",
            output_path=out,
            limit=limit,
            since_hours=since_hours,
        )
        created.append(out)
    return created


def write_manifest(backup_dir: Path, artifacts: list[BackupArtifact]) -> Path:
    manifest_path = backup_dir / "manifest.json"
    payload = {
        "backup_dir": str(backup_dir),
        "created_at": datetime.now(tz=UTC).isoformat(),
        "artifacts": [
            {
                "path": str(item.path.relative_to(backup_dir)),
                "size_bytes": item.size_bytes,
                "sha256": item.sha256,
            }
            for item in artifacts
        ],
    }
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest_path


def prune_backup_dirs(root: Path, keep_last: int) -> list[Path]:
    if keep_last <= 0:
        return []
    dirs = [path for path in root.iterdir() if path.is_dir()]
    dirs.sort(key=lambda item: item.name, reverse=True)
    stale = dirs[keep_last:]
    for path in stale:
        shutil.rmtree(path, ignore_errors=True)
    return stale


def file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as file:
        while True:
            chunk = file.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()
