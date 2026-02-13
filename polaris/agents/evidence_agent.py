from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime

from polaris.core.module4.types import M4Evidence

from .evidence_tools import EvidenceTools


@dataclass(frozen=True)
class EvidenceAgentConfig:
    enabled: bool = True
    max_calls: int = 2
    timeout_sec: float = 3.0
    single_source_uncertainty: float = 0.06


class EvidenceAgent:
    """Evidence-first helper: enrich uncertainty and tags, never direct trade action."""

    def __init__(self, tools: EvidenceTools, config: EvidenceAgentConfig | None = None) -> None:
        self._tools = tools
        self._cfg = config or EvidenceAgentConfig()

    async def analyze(
        self,
        *,
        account_id: str,
        market_id: str,
        window_code: str,
        run_tag: str,
        as_of: datetime | None = None,
    ) -> M4Evidence:
        if not self._cfg.enabled:
            return M4Evidence(
                event_tags=(),
                confidence=0.0,
                uncertainty_delta=0.0,
                calls_used=0,
                timed_out=False,
                degraded=True,
                sources=(),
                metadata={"disabled": True},
            )

        calls_used = 0
        timed_out = False
        degraded = False
        tags: set[str] = set()
        sources: list[dict[str, object]] = []
        confidence = 0.0

        try:
            async with asyncio.timeout(max(0.2, self._cfg.timeout_sec)):
                if calls_used < self._cfg.max_calls:
                    posts = await self._tools.fetch_recent_posts(
                        account_id=account_id,
                        lookback_minutes=240 if window_code == "week" else 120,
                        as_of=as_of,
                        limit=160,
                    )
                    calls_used += 1
                    post_count = len(posts)
                    if post_count == 0:
                        tags.add("evidence_quiet")
                    elif post_count >= 10:
                        tags.add("evidence_burst")
                    elif post_count >= 4:
                        tags.add("evidence_active")

                    if post_count > 0:
                        max_chars = max(len(str(row.get("content") or "")) for row in posts)
                        if max_chars >= 220:
                            tags.add("long_post_seen")
                        confidence += min(0.45, post_count / 30.0)
                    sources.append(
                        {
                            "source": "fact_tweet_post",
                            "kind": "recent_posts",
                            "count": post_count,
                        }
                    )

                if calls_used < self._cfg.max_calls:
                    keyword_groups = {
                        "event_spacex": ("spacex", "starship", "launch"),
                        "event_politics": ("trump", "election", "policy", "senate"),
                        "event_crypto": ("bitcoin", "btc", "doge", "crypto"),
                    }
                    hits_total = 0
                    matched_groups: list[str] = []
                    for tag, words in keyword_groups.items():
                        hits = await self._tools.keyword_hits(
                            account_id=account_id,
                            lookback_minutes=240,
                            keywords=words,
                            as_of=as_of,
                        )
                        hits_total += hits
                        if hits > 0:
                            matched_groups.append(tag)
                    calls_used += 1
                    if matched_groups:
                        tags.update(matched_groups)
                        confidence += min(0.35, hits_total / 40.0)
                    sources.append(
                        {
                            "source": "fact_tweet_post",
                            "kind": "keyword_scan",
                            "hits_total": hits_total,
                            "groups": matched_groups,
                        }
                    )
        except TimeoutError:
            timed_out = True
            degraded = True
            tags.add("timeout_degraded")

        source_count = len(sources)
        if source_count <= 1:
            uncertainty_delta = self._cfg.single_source_uncertainty
        else:
            uncertainty_delta = min(0.10, self._cfg.single_source_uncertainty * 0.5)

        confidence = max(0.0, min(1.0, confidence))
        return M4Evidence(
            event_tags=tuple(sorted(tags)),
            confidence=confidence,
            uncertainty_delta=uncertainty_delta,
            calls_used=calls_used,
            timed_out=timed_out,
            degraded=degraded,
            sources=tuple(sources),
            metadata={
                "market_id": market_id,
                "run_tag": run_tag,
                "window_code": window_code,
                "max_calls": self._cfg.max_calls,
            },
        )
