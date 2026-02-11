from __future__ import annotations

import re
from typing import Any

from polaris.sources.gamma_client import GammaClient


POST_TERMS = ("tweet", "tweets", "post", "posts")
DEFAULT_ELON_ALIASES = ("elon", "elon musk", "musk")
DEFAULT_WATCHLIST_ALIASES = (
    "elon",
    "elon musk",
    "musk",
    "andrew tate",
    "tate",
    "donald j trump",
    "donald trump",
    "trump",
)


def is_elon_tweet_market(question: str, slug: str) -> bool:
    return is_person_tweet_market(question, slug, aliases=DEFAULT_ELON_ALIASES)


def normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def has_any_alias(text_normalized: str, aliases: tuple[str, ...]) -> bool:
    padded = f" {text_normalized} "
    for alias in aliases:
        alias_norm = normalize_text(alias)
        if not alias_norm:
            continue
        if f" {alias_norm} " in padded:
            return True
    return False


def has_any_post_term(text_normalized: str) -> bool:
    padded = f" {text_normalized} "
    for term in POST_TERMS:
        if f" {term} " in padded:
            return True
    return False


def is_person_tweet_market(question: str, slug: str, aliases: tuple[str, ...]) -> bool:
    text_normalized = normalize_text(f"{question} {slug}")
    if not text_normalized:
        return False
    return has_any_alias(text_normalized, aliases) and has_any_post_term(text_normalized)


def is_open_market(row: dict[str, Any]) -> bool:
    return bool(row.get("active")) and not bool(row.get("closed")) and not bool(row.get("archived"))


def is_market_in_state(row: dict[str, Any], state: str) -> bool:
    normalized = state.strip().lower()
    if normalized == "all":
        return True
    return is_open_market(row)


async def discover_target_markets(
    client: GammaClient,
    scope: str = "all",
    state: str = "open",
    page_size: int = 500,
    max_pages: int | None = None,
    tweet_targets: list[str] | None = None,
) -> list[dict[str, Any]]:
    rows = await client.iter_markets(page_size=page_size, max_pages=max_pages)
    normalized_scope = scope.strip().lower()
    aliases = tuple(tweet_targets) if tweet_targets else DEFAULT_WATCHLIST_ALIASES
    if normalized_scope == "elon_tweet":
        return [
            row
            for row in rows
            if is_market_in_state(row, state) and is_elon_tweet_market(row.get("question", ""), row.get("slug", ""))
        ]
    if normalized_scope == "watchlist_tweet":
        return [
            row
            for row in rows
            if is_market_in_state(row, state)
            and is_person_tweet_market(row.get("question", ""), row.get("slug", ""), aliases=aliases)
        ]
    return [row for row in rows if is_market_in_state(row, state)]
