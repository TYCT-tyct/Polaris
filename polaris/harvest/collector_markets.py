from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from polaris.db.pool import Database
from polaris.harvest.discovery import discover_target_markets
from polaris.sources.gamma_client import GammaClient
from polaris.sources.models import TokenDescriptor


class MarketCollector:
    def __init__(self, db: Database, gamma_client: GammaClient, market_scope: str = "all") -> None:
        self.db = db
        self.gamma_client = gamma_client
        self.market_scope = market_scope

    async def run_once(self) -> tuple[int, int]:
        raw_markets = await discover_target_markets(self.gamma_client, scope=self.market_scope)
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
