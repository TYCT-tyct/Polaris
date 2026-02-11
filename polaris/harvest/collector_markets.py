from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from polaris.db.pool import Database
from polaris.harvest.discovery import discover_target_markets
from polaris.sources.gamma_client import GammaClient
from polaris.sources.models import TokenDescriptor


class MarketCollector:
    def __init__(
        self,
        db: Database,
        gamma_client: GammaClient,
        market_scope: str = "all",
        market_state: str = "open",
        market_tweet_targets: list[str] | None = None,
        gamma_page_size: int = 500,
        gamma_max_pages: int = 0,
    ) -> None:
        self.db = db
        self.gamma_client = gamma_client
        self.market_scope = market_scope
        self.market_state = market_state
        self.market_tweet_targets = market_tweet_targets or []
        self.gamma_page_size = max(1, gamma_page_size)
        self.gamma_max_pages = gamma_max_pages

    async def run_once(self) -> tuple[int, int]:
        max_pages = self.gamma_max_pages if self.gamma_max_pages > 0 else None
        raw_markets = await discover_target_markets(
            self.gamma_client,
            scope=self.market_scope,
            state=self.market_state,
            page_size=self.gamma_page_size,
            max_pages=max_pages,
            tweet_targets=self.market_tweet_targets,
        )
        markets = [self.gamma_client.market_record(row) for row in raw_markets]
        events = []
        for row in raw_markets:
            events.extend(self.gamma_client.event_records(row))
        token_rows: list[TokenDescriptor] = []
        for row in raw_markets:
            token_rows.extend(self.gamma_client.token_descriptors(row))
        await self._upsert_events(events)
        await self._upsert_markets(markets)
        await self._upsert_tokens(token_rows)
        await self._insert_market_snapshots(markets)
        return len(markets), len(token_rows)

    async def list_active_tokens(self) -> list[dict[str, Any]]:
        return await self.db.fetch_all(
            """
            select t.token_id, t.market_id, t.outcome_label, t.outcome_side
            from dim_token t
            join dim_market m on m.market_id = t.market_id
            where m.active = true and m.closed = false
            order by m.end_date nulls last
            """
        )

    async def _upsert_markets(self, markets: list[dict[str, Any]]) -> None:
        rows = [
            (
                m["market_id"],
                m["gamma_market_id"],
                m["condition_id"],
                m["event_id"],
                m["question"],
                m["slug"],
                m["event_slug"],
                m["category"],
                m["end_date"],
                m["start_date"],
                m["neg_risk"],
                m["active"],
                m["closed"],
                m["archived"],
                m["neg_risk_augmented"],
                m["spread"],
                m["liquidity"],
                m["volume"],
                m["resolution_source"],
                m["updated_from_source_at"],
                datetime.now(tz=timezone.utc),
            )
            for m in markets
        ]
        if not rows:
            return
        await self.db.executemany(
            """
            insert into dim_market(
                market_id, gamma_market_id, condition_id, event_id, question, slug, event_slug, category,
                end_date, start_date, neg_risk, active, closed, archived, neg_risk_augmented, spread,
                liquidity, volume, resolution_source, updated_from_source_at, captured_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict (market_id) do update
            set gamma_market_id = excluded.gamma_market_id,
                condition_id = excluded.condition_id,
                question = excluded.question,
                slug = excluded.slug,
                event_slug = excluded.event_slug,
                category = excluded.category,
                end_date = excluded.end_date,
                start_date = excluded.start_date,
                neg_risk = excluded.neg_risk,
                active = excluded.active,
                closed = excluded.closed,
                archived = excluded.archived,
                spread = excluded.spread,
                liquidity = excluded.liquidity,
                volume = excluded.volume,
                event_id = excluded.event_id,
                neg_risk_augmented = excluded.neg_risk_augmented,
                resolution_source = excluded.resolution_source,
                updated_from_source_at = excluded.updated_from_source_at,
                captured_at = excluded.captured_at
            where dim_market.gamma_market_id is distinct from excluded.gamma_market_id
               or dim_market.condition_id is distinct from excluded.condition_id
               or dim_market.question is distinct from excluded.question
               or dim_market.slug is distinct from excluded.slug
               or dim_market.event_slug is distinct from excluded.event_slug
               or dim_market.category is distinct from excluded.category
               or dim_market.end_date is distinct from excluded.end_date
               or dim_market.start_date is distinct from excluded.start_date
               or dim_market.neg_risk is distinct from excluded.neg_risk
               or dim_market.active is distinct from excluded.active
               or dim_market.closed is distinct from excluded.closed
               or dim_market.archived is distinct from excluded.archived
               or dim_market.spread is distinct from excluded.spread
               or dim_market.liquidity is distinct from excluded.liquidity
               or dim_market.volume is distinct from excluded.volume
               or dim_market.event_id is distinct from excluded.event_id
               or dim_market.neg_risk_augmented is distinct from excluded.neg_risk_augmented
               or dim_market.resolution_source is distinct from excluded.resolution_source
               or dim_market.updated_from_source_at is distinct from excluded.updated_from_source_at
            """,
            rows,
        )

    async def _upsert_events(self, events: list[dict[str, Any]]) -> None:
        if not events:
            return
        rows = [
            (
                e["event_id"],
                e["event_slug"],
                e["event_ticker"],
                e["title"],
                e["category"],
                e["active"],
                e["closed"],
                e["archived"],
                e["start_date"],
                e["end_date"],
            )
            for e in events
        ]
        await self.db.executemany(
            """
            insert into dim_event(
                event_id, event_slug, event_ticker, title, category, active, closed, archived,
                start_date, end_date, captured_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            on conflict (event_id) do update
            set event_slug = excluded.event_slug,
                event_ticker = excluded.event_ticker,
                title = excluded.title,
                category = excluded.category,
                active = excluded.active,
                closed = excluded.closed,
                archived = excluded.archived,
                start_date = excluded.start_date,
                end_date = excluded.end_date,
                captured_at = excluded.captured_at
            where dim_event.event_slug is distinct from excluded.event_slug
               or dim_event.event_ticker is distinct from excluded.event_ticker
               or dim_event.title is distinct from excluded.title
               or dim_event.category is distinct from excluded.category
               or dim_event.active is distinct from excluded.active
               or dim_event.closed is distinct from excluded.closed
               or dim_event.archived is distinct from excluded.archived
               or dim_event.start_date is distinct from excluded.start_date
               or dim_event.end_date is distinct from excluded.end_date
            """,
            rows,
        )

    async def _upsert_tokens(self, tokens: list[TokenDescriptor]) -> None:
        rows = [
            (
                t.token_id,
                t.market_id,
                t.outcome_label,
                t.outcome_side,
                t.tick_size,
                t.min_order_size,
                t.outcome_index,
                t.is_other_outcome,
                t.is_placeholder_outcome,
            )
            for t in tokens
        ]
        if not rows:
            return
        await self.db.executemany(
            """
            insert into dim_token(
                token_id, market_id, outcome_label, outcome_side,
                tick_size, min_order_size, outcome_index, is_other_outcome, is_placeholder_outcome
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict (token_id) do update
            set market_id = excluded.market_id,
                outcome_label = excluded.outcome_label,
                outcome_side = excluded.outcome_side,
                tick_size = excluded.tick_size,
                min_order_size = excluded.min_order_size,
                outcome_index = excluded.outcome_index,
                is_other_outcome = excluded.is_other_outcome,
                is_placeholder_outcome = excluded.is_placeholder_outcome,
                captured_at = now()
            where dim_token.market_id is distinct from excluded.market_id
               or dim_token.outcome_label is distinct from excluded.outcome_label
               or dim_token.outcome_side is distinct from excluded.outcome_side
               or dim_token.tick_size is distinct from excluded.tick_size
               or dim_token.min_order_size is distinct from excluded.min_order_size
               or dim_token.outcome_index is distinct from excluded.outcome_index
               or dim_token.is_other_outcome is distinct from excluded.is_other_outcome
               or dim_token.is_placeholder_outcome is distinct from excluded.is_placeholder_outcome
            """,
            rows,
        )

    async def _insert_market_snapshots(self, markets: list[dict[str, Any]]) -> None:
        rows = [
            (m["market_id"], m["active"], m["closed"], m["archived"], m["spread"], m["liquidity"], m["volume"])
            for m in markets
        ]
        if not rows:
            return
        await self.db.executemany(
            """
            insert into fact_market_state_snapshot(
                market_id, active, closed, archived, spread, liquidity, volume
            )
            values (%s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )
