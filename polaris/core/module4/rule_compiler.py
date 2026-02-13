from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from typing import Any

from polaris.db.pool import Database

from .types import RulePolicy, RuleVersion


class RuleCompiler:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def compile_market(self, market_id: str, *, force: bool = False) -> RuleVersion | None:
        market = await self._db.fetch_one(
            """
            select market_id, description
            from dim_market
            where market_id = %s
            """,
            (market_id,),
        )
        if market is None:
            return None
        description = str(market.get("description") or "")
        return await self._compile_and_persist(market_id, description, force=force)

    async def compile_open_tweet_markets(self, *, force: bool = False) -> list[RuleVersion]:
        rows = await self._db.fetch_all(
            """
            select market_id, description
            from dim_market
            where active = true
              and closed = false
              and (
                question ilike '%%tweet%%'
                or slug ilike '%%tweet%%'
              )
            order by market_id
            """
        )
        out: list[RuleVersion] = []
        for row in rows:
            version = await self._compile_and_persist(
                str(row["market_id"]),
                str(row.get("description") or ""),
                force=force,
            )
            if version is not None:
                out.append(version)
        return out

    async def _compile_and_persist(self, market_id: str, description: str, *, force: bool) -> RuleVersion:
        policy = compile_rule_policy(description)
        rule_hash = compute_rule_hash(description, policy)
        now = datetime.now(tz=UTC)

        active = await self._db.fetch_one(
            """
            select rule_version_id, rule_hash
            from dim_market_rule_version
            where market_id = %s
              and valid_to is null
            order by created_at desc
            limit 1
            """,
            (market_id,),
        )
        if active and str(active["rule_hash"]) == rule_hash and not force:
            return RuleVersion(
                rule_version_id=str(active["rule_version_id"]),
                market_id=market_id,
                rule_hash=rule_hash,
                rule_text=description,
                parsed_rule=policy,
            )

        if active:
            await self._db.execute(
                """
                update dim_market_rule_version
                set valid_to = %s
                where rule_version_id = %s::uuid
                """,
                (now, str(active["rule_version_id"])),
            )

        row = await self._db.fetch_one(
            """
            insert into dim_market_rule_version(
                market_id, rule_hash, rule_text, parsed_rule, valid_from, valid_to, created_at
            )
            values (%s, %s, %s, %s::jsonb, %s, null, %s)
            on conflict (market_id, rule_hash) do update
            set rule_text = excluded.rule_text,
                parsed_rule = excluded.parsed_rule,
                valid_to = null
            returning rule_version_id
            """,
            (market_id, rule_hash, description, _to_json(_policy_to_json(policy)), now, now),
        )
        rule_version_id = str(row["rule_version_id"])
        return RuleVersion(
            rule_version_id=rule_version_id,
            market_id=market_id,
            rule_hash=rule_hash,
            rule_text=description,
            parsed_rule=policy,
        )


def compile_rule_policy(description: str) -> RulePolicy:
    text = _normalize_text(description)
    clauses = [chunk.strip() for chunk in re.split(r"[.;\n]+", text) if chunk.strip()]
    include_reply = False
    include_quote = True
    include_repost = True
    include_main = True
    count_deleted = True
    source = "xtracker"
    notes: list[str] = []

    for clause in clauses:
        has_not_count = _contains_any(clause, ("not count", "doesn't count", "do not count", "excluded"))
        has_count = _contains_any(clause, ("count", "included", "include"))
        if _contains_any(clause, ("reply", "replies")):
            if has_not_count:
                include_reply = False
                notes.append("replies_excluded")
            elif has_count:
                include_reply = True
                notes.append("replies_included")
        if _contains_any(clause, ("quote", "quoted")):
            if has_not_count:
                include_quote = False
                notes.append("quotes_excluded")
            elif has_count:
                include_quote = True
                notes.append("quotes_included")
        if _contains_any(clause, ("repost", "retweet", "retweets")):
            if has_not_count:
                include_repost = False
                notes.append("reposts_excluded")
            elif has_count:
                include_repost = True
                notes.append("reposts_included")

    if "main feed" in text and _contains_any(text, ("only", "count")):
        include_main = True
        notes.append("main_feed_only")

    if "deleted" in text and _contains_any(text, ("captured", "count")):
        count_deleted = True
        notes.append("deleted_if_captured")

    if "xtracker" in text or "post counter" in text:
        source = "xtracker"
        notes.append("resolution_xtracker")
    elif "twitter" in text or "x.com" in text:
        source = "x"
        notes.append("resolution_x")

    return RulePolicy(
        include_main=include_main,
        include_reply=include_reply,
        include_quote=include_quote,
        include_repost=include_repost,
        count_deleted_if_captured=count_deleted,
        resolution_source=source,
        notes=tuple(sorted(set(notes))),
    )


def compute_rule_hash(description: str, policy: RulePolicy) -> str:
    normalized = _normalize_text(description)
    payload = {"description": normalized, "policy": _policy_to_json(policy)}
    body = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _policy_to_json(policy: RulePolicy) -> dict[str, Any]:
    return {
        "include_main": policy.include_main,
        "include_reply": policy.include_reply,
        "include_quote": policy.include_quote,
        "include_repost": policy.include_repost,
        "count_deleted_if_captured": policy.count_deleted_if_captured,
        "resolution_source": policy.resolution_source,
        "notes": list(policy.notes),
    }


def _normalize_text(text: str) -> str:
    return " ".join((text or "").strip().lower().split())


def _contains_any(text: str, parts: tuple[str, ...]) -> bool:
    return any(part in text for part in parts)


def _to_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
