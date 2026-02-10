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


async def discover_target_markets(client: GammaClient) -> list[dict[str, Any]]:
    rows = await client.iter_markets()
    return [row for row in rows if is_elon_tweet_market(row.get("question", ""), row.get("slug", ""))]

