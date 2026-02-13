from __future__ import annotations

import json
from datetime import datetime
from typing import Any


PROMPT_VERSION = "m4-semantic-v1"

SYSTEM_PROMPT = (
    "You are a strict semantic analyzer for short-horizon post-count prediction markets. "
    "You must only return valid JSON with no markdown. "
    "Never provide trading actions. Only output semantic adjustment signals."
)


def build_semantic_user_prompt(
    *,
    market_id: str,
    window_code: str,
    as_of: datetime | None,
    observed_count: int,
    progress: float,
    posts: list[dict[str, Any]],
) -> str:
    compact_posts: list[dict[str, Any]] = []
    for row in posts[:60]:
        compact_posts.append(
            {
                "post_id": str(row.get("platform_post_id") or ""),
                "posted_at": _iso(row.get("posted_at")),
                "imported_at": _iso(row.get("imported_at")),
                "content": str(row.get("content") or "")[:500],
                "char_len": int(row.get("char_len") or 0),
                "is_reply": bool(row.get("is_reply") or False),
                "is_retweet": bool(row.get("is_retweet") or False),
            }
        )

    payload = {
        "task": "semantic_adjustment",
        "constraints": {
            "no_trade_action": True,
            "delta_progress_range": [-0.15, 0.15],
            "delta_uncertainty_range": [0.0, 0.25],
            "mean_shift_hint_range": [-1.0, 1.0],
            "regime_keys": ["quiet", "active", "burst"],
            "reason_codes_max": 8,
            "evidence_span_max": 6,
        },
        "context": {
            "market_id": market_id,
            "window_code": window_code,
            "as_of": _iso(as_of),
            "observed_count": observed_count,
            "progress": progress,
            "posts": compact_posts,
        },
        "output_schema": {
            "regime_probs": {"quiet": "float", "active": "float", "burst": "float"},
            "delta_progress": "float",
            "delta_uncertainty": "float",
            "mean_shift_hint": "float",
            "confidence": "float",
            "evidence_span": ["string"],
            "reason_codes": ["string"],
        },
    }
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"), default=str)


def _iso(value: object) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return None

