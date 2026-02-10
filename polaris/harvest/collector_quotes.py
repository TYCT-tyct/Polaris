from __future__ import annotations

from polaris.db.pool import Database
from polaris.sources.clob_client import ClobClient


class QuoteCollector:
    def __init__(self, db: Database, clob_client: ClobClient, enable_l2: bool = True) -> None:
        self.db = db
        self.clob_client = clob_client
        self.enable_l2 = enable_l2

    async def collect_top_depth_l2(self, token_rows: list[dict]) -> tuple[int, int, int]:
        books = await self._fetch_books(token_rows)
        return await self._persist_from_books(books, include_top=True, include_depth=True, include_l2=self.enable_l2)

    async def collect_top_only(self, token_rows: list[dict]) -> int:
        books = await self._fetch_books(token_rows)
        top, _, _ = await self._persist_from_books(books, include_top=True, include_depth=False, include_l2=False)
        return top

    async def collect_depth_l2(self, token_rows: list[dict]) -> tuple[int, int]:
        books = await self._fetch_books(token_rows)
        _, depth, l2 = await self._persist_from_books(
            books,
            include_top=False,
            include_depth=True,
            include_l2=self.enable_l2,
        )
        return depth, l2

    async def collect_depth_only(self, token_rows: list[dict]) -> int:
        books = await self._fetch_books(token_rows)
        _, depth, _ = await self._persist_from_books(books, include_top=False, include_depth=True, include_l2=False)
        return depth

    async def collect_l2_only(self, token_rows: list[dict]) -> int:
        books = await self._fetch_books(token_rows)
        _, _, l2 = await self._persist_from_books(books, include_top=False, include_depth=False, include_l2=self.enable_l2)
        return l2

    async def _fetch_books(self, token_rows: list[dict]) -> list[tuple[dict, object]]:
        books: list[tuple[dict, object]] = []
        for token in token_rows:
            book = await self.clob_client.get_book(token["token_id"])
            books.append((token, book))
        return books

    async def _persist_from_books(
        self,
        books: list[tuple[dict, object]],
        include_top: bool,
        include_depth: bool,
        include_l2: bool,
    ) -> tuple[int, int, int]:
        top_rows: list[tuple] = []
        depth_rows: list[tuple] = []
        l2_rows: list[tuple] = []
        for token, book in books:
            token_id = token["token_id"]
            market_id = token["market_id"]
            source_ts = self.clob_client.timestamp_to_datetime(book.timestamp)
            best_bid, best_ask = self.clob_client.best_bid_ask(book)
            if include_top:
                if best_bid is not None and best_ask is not None:
                    mid = (best_bid + best_ask) / 2
                    spread = best_ask - best_bid
                else:
                    mid = None
                    spread = None
                top_rows.append(
                    (
                        token_id,
                        market_id,
                        best_bid,
                        best_ask,
                        mid,
                        spread,
                        float(book.last_trade_price) if book.last_trade_price else None,
                        source_ts,
                    )
                )
            if include_depth:
                depth = self.clob_client.depth_summary(book, best_bid, best_ask)
                depth_rows.append(
                    (
                        token_id,
                        market_id,
                        depth["bid_depth_1pct"],
                        depth["bid_depth_2pct"],
                        depth["bid_depth_5pct"],
                        depth["ask_depth_1pct"],
                        depth["ask_depth_2pct"],
                        depth["ask_depth_5pct"],
                        depth["imbalance"],
                        source_ts,
                    )
                )
            if include_l2:
                for level in self.clob_client.l2_levels(book):
                    l2_rows.append(
                        (
                            token_id,
                            market_id,
                            level["side"],
                            level["price"],
                            level["size"],
                            level["level_index"],
                            source_ts,
                        )
                    )
        if include_top:
            await self._insert_top(top_rows)
        if include_depth:
            await self._insert_depth(depth_rows)
        if include_l2:
            await self._insert_l2(l2_rows)
        return len(top_rows), len(depth_rows), len(l2_rows)

    async def aggregate_1m(self) -> int:
        await self.db.execute(
            """
            insert into fact_quote_1m(
                token_id, market_id, bucket_minute, open, high, low, close,
                avg_spread, min_spread, max_spread, sample_count, updated_at
            )
            with base as (
                select
                    token_id,
                    market_id,
                    date_trunc('minute', captured_at) as bucket_minute,
                    captured_at,
                    mid,
                    spread
                from fact_quote_top_raw
                where captured_at >= now() - interval '10 minutes'
                  and mid is not null
            ),
            ranked as (
                select *,
                    row_number() over(partition by token_id, bucket_minute order by captured_at asc) as rn_asc,
                    row_number() over(partition by token_id, bucket_minute order by captured_at desc) as rn_desc
                from base
            ),
            agg as (
                select
                    token_id,
                    market_id,
                    bucket_minute,
                    max(case when rn_asc = 1 then mid end) as open,
                    max(mid) as high,
                    min(mid) as low,
                    max(case when rn_desc = 1 then mid end) as close,
                    avg(spread) as avg_spread,
                    min(spread) as min_spread,
                    max(spread) as max_spread,
                    count(*)::int as sample_count
                from ranked
                group by token_id, market_id, bucket_minute
            )
            select
                token_id, market_id, bucket_minute, open, high, low, close,
                avg_spread, min_spread, max_spread, sample_count, now()
            from agg
            on conflict (token_id, bucket_minute) do update
            set market_id = excluded.market_id,
                open = excluded.open,
                high = excluded.high,
                low = excluded.low,
                close = excluded.close,
                avg_spread = excluded.avg_spread,
                min_spread = excluded.min_spread,
                max_spread = excluded.max_spread,
                sample_count = excluded.sample_count,
                updated_at = excluded.updated_at
            """
        )
        return 1

    async def prune_raw(self, retention_days: int) -> int:
        row = await self.db.fetch_one("select polaris_prune_raw(%s) as deleted_rows", (retention_days,))
        return int(row["deleted_rows"]) if row else 0

    async def _insert_top(self, rows: list[tuple]) -> None:
        if not rows:
            return
        await self.db.executemany(
            """
            insert into fact_quote_top_raw(
                token_id, market_id, best_bid, best_ask, mid, spread, last_trade_price, source_ts, captured_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, now())
            """,
            rows,
        )

    async def _insert_depth(self, rows: list[tuple]) -> None:
        if not rows:
            return
        await self.db.executemany(
            """
            insert into fact_quote_depth_raw(
                token_id, market_id, bid_depth_1pct, bid_depth_2pct, bid_depth_5pct,
                ask_depth_1pct, ask_depth_2pct, ask_depth_5pct, imbalance, source_ts, captured_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            """,
            rows,
        )

    async def _insert_l2(self, rows: list[tuple]) -> None:
        if not rows:
            return
        await self.db.executemany(
            """
            insert into fact_orderbook_l2_raw(
                token_id, market_id, side, price, size, level_index, source_ts, captured_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, now())
            """,
            rows,
        )
