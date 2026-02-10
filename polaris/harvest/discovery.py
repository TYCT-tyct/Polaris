from __future__ import annotations

import re
from typing import Any

from polaris.sources.gamma_client import GammaClient


TARGET_PATTERNS = [
    re.compile(r"\belon\b", re.IGNORECASE),
    re.compile(r"\bmusk\b", re.IGNORECASE),
    re.compile(r"\btweet", re.IGNORECASE),
]


def is_elon_tweet_market(question: str, slug: str) -> bool:
    text = f"{question} {slug}".lower()
    has_elon = "elon" in text or "musk" in text
    has_tweet = "tweet" in text
    return has_elon and has_tweet


def is_open_market(row: dict[str, Any]) -> bool:
    return bool(row.get("active")) and not bool(row.get("closed")) and not bool(row.get("archived"))


async def discover_target_markets(client: GammaClient, scope: str = "all") -> list[dict[str, Any]]:
    rows = await client.iter_markets()
    normalized_scope = scope.strip().lower()
    if normalized_scope == "elon_tweet":
        return [
            row
            for row in rows
            if is_open_market(row) and is_elon_tweet_market(row.get("question", ""), row.get("slug", ""))
        ]
    return [row for row in rows if is_open_market(row)]
