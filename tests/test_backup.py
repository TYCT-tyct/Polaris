from __future__ import annotations

from pathlib import Path

from polaris.ops.backup import (
    BackupArtifact,
    backup_label,
    build_pg_dump_plan,
    prune_backup_dirs,
    write_manifest,
)


def test_backup_label_sanitizes_user_prefix() -> None:
    name = backup_label("  aws nightly @ ireland  ")
    assert "aws-nightly-ireland" in name


def test_build_pg_dump_plan_from_database_url(tmp_path: Path) -> None:
    output_file = tmp_path / "dump.dump"
    plan = build_pg_dump_plan(
        "postgresql://polaris_user:secret@db.example.com:5432/polaris?sslmode=require",
        output_file=output_file,
        pg_dump_bin="pg_dump",
    )
    assert plan.command[0] == "pg_dump"
    assert str(output_file) in plan.command
    assert plan.env["PGHOST"] == "db.example.com"
    assert plan.env["PGPORT"] == "5432"
    assert plan.env["PGUSER"] == "polaris_user"
    assert plan.env["PGDATABASE"] == "polaris"
    assert plan.env["PGPASSWORD"] == "secret"
    assert plan.env["PGSSLMODE"] == "require"


def test_prune_backup_dirs_keeps_latest(tmp_path: Path) -> None:
    for name in ("20260210_090000", "20260210_100000", "20260210_110000"):
        (tmp_path / name).mkdir()

    removed = prune_backup_dirs(tmp_path, keep_last=2)

    assert len(removed) == 1
    assert removed[0].name == "20260210_090000"
    assert not (tmp_path / "20260210_090000").exists()


def test_write_manifest_contains_artifact_entry(tmp_path: Path) -> None:
    backup_dir = tmp_path / "20260210_120000"
    backup_dir.mkdir()
    file_path = backup_dir / "polaris_db.dump"
    file_path.write_text("hello", encoding="utf-8")

    artifact = BackupArtifact.from_path(file_path)
    manifest = write_manifest(backup_dir, [artifact])

    text = manifest.read_text(encoding="utf-8")
    assert "polaris_db.dump" in text
