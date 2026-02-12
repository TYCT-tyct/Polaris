from __future__ import annotations

from datetime import datetime

from polaris.db.pool import Database


class MarketTrackingMapper:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def remap(self, account_handle: str = "elonmusk", min_score: float = 0.4) -> int:
        account = await self.db.fetch_one("select account_id from dim_account where handle = %s", (account_handle,))
        if not account:
            return 0
        account_id = account["account_id"]
        handle_keywords = _handle_keywords(account_handle)
        markets = await self.db.fetch_all(
            """
            select market_id, question, start_date, end_date, slug
            from dim_market
            where (active = true or closed = false)
              and (
                question ilike '%%tweet%%'
                or slug ilike '%%tweet%%'
              )
            """
        )
        trackings = await self.db.fetch_all(
            """
            select tracking_id, start_date, end_date, title
            from dim_tracking_window
            where account_id = %s
            """,
            (account_id,),
        )
        mapped_rows: list[tuple] = []
        for market in markets:
            if not _market_matches_handle(market, handle_keywords):
                continue
            best_tracking = None
            best_score = 0.0
            for tracking in trackings:
                score = overlap_score(
                    market["start_date"], market["end_date"], tracking["start_date"], tracking["end_date"]
                )
                if score > best_score:
                    best_score = score
                    best_tracking = tracking
            if best_tracking and best_score >= min_score:
                mapped_rows.append(
                    (
                        market["market_id"],
                        best_tracking["tracking_id"],
                        account_id,
                        best_score,
                        f"window_overlap={best_score:.4f}",
                    )
                )
        await self.db.execute("delete from bridge_market_tracking where account_id = %s", (account_id,))
        if mapped_rows:
            await self.db.executemany(
                """
                insert into bridge_market_tracking(
                    market_id, tracking_id, account_id, match_score, match_reason, mapped_at
                )
                values (%s, %s, %s, %s, %s, now())
                on conflict (market_id, tracking_id) do update
                set account_id = excluded.account_id,
                    match_score = excluded.match_score,
                    match_reason = excluded.match_reason,
                    mapped_at = excluded.mapped_at
                """,
                mapped_rows,
            )
        return len(mapped_rows)


def overlap_score(
    market_start: datetime | None,
    market_end: datetime | None,
    tracking_start: datetime | None,
    tracking_end: datetime | None,
) -> float:
    if not market_start or not market_end or not tracking_start or not tracking_end:
        return 0.0
    left = max(market_start, tracking_start)
    right = min(market_end, tracking_end)
    overlap = (right - left).total_seconds()
    if overlap <= 0:
        return 0.0
    market_duration = (market_end - market_start).total_seconds()
    if market_duration <= 0:
        return 0.0
    return min(overlap / market_duration, 1.0)


def _handle_keywords(account_handle: str) -> tuple[str, ...]:
    handle = (account_handle or "").strip().lower()
    mapping = {
        "elonmusk": ("elon", "musk", "elon musk"),
        "cobratate": ("andrew tate", "tate", "cobra tate"),
        "realdonaldtrump": ("donald trump", "donald j. trump", "trump"),
    }
    return mapping.get(handle, (handle.replace("_", " "), handle))


def _market_matches_handle(market: dict, keywords: tuple[str, ...]) -> bool:
    haystack = f"{market.get('question', '')} {market.get('slug', '')}".lower()
    return any(keyword and keyword in haystack for keyword in keywords)
