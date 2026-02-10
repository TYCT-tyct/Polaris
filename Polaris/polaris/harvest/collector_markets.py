from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from polaris.db.pool import Database
from polaris.harvest.discovery import discover_target_markets
from polaris.sources.gamma_client import GammaClient
from polaris.sources.models import TokenDescriptor


class MarketCollector:
    def __init__(self, db: Database, gamma_client: GammaClient) -> None:
        self.db = db
        self.gamma_client = gamma_client

    async def run_once(self) -> tuple[int, int]:
        raw_markets = await discover_target_markets(self.gamma_client)
        markets = [self.gamma_client.market_record(row) for row in raw_markets]
        token_rows: list[TokenDescriptor] = []
        for row in raw_markets:
            token_rows.extend(self.gamma_client.token_descriptors(row))
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
                m["spread"],
                m["liquidity"],
                m["volume"],
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
                market_id, gamma_market_id, condition_id, question, slug, event_slug, category,
                end_date, start_date, neg_risk, active, closed, archived, spread,
                liquidity, volume, updated_from_source_at, captured_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                updated_from_source_at = excluded.updated_from_source_at,
                captured_at = excluded.captured_at
            """,
            rows,
        )

    async def _upsert_tokens(self, tokens: list[TokenDescriptor]) -> None:
        rows = [(t.token_id, t.market_id, t.outcome_label, t.outcome_side) for t in tokens]
        if not rows:
            return
        await self.db.executemany(
            """
            insert into dim_token(token_id, market_id, outcome_label, outcome_side)
            values (%s, %s, %s, %s)
            on conflict (token_id) do update
            set market_id = excluded.market_id,
                outcome_label = excluded.outcome_label,
                outcome_side = excluded.outcome_side,
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

