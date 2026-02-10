from __future__ import annotations

from datetime import datetime

from polaris.arb.contracts import RunMode


def parse_run_mode(raw: str) -> RunMode:
    normalized = raw.strip().lower()
    if normalized == RunMode.SHADOW.value:
        return RunMode.SHADOW
    if normalized == RunMode.PAPER_LIVE.value:
        return RunMode.PAPER_LIVE
    if normalized == RunMode.PAPER_REPLAY.value:
        return RunMode.PAPER_REPLAY
    if normalized == RunMode.LIVE.value:
        return RunMode.LIVE
    raise ValueError(f"unsupported mode: {raw}")


def parse_iso_datetime(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return datetime.fromisoformat(text)
