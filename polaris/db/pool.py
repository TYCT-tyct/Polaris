from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Sequence

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool


MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"


class Database:
    def __init__(self, dsn: str, *, min_size: int = 1, max_size: int = 4) -> None:
        if min_size < 1:
            raise ValueError("min_size must be >= 1")
        if max_size < min_size:
            raise ValueError("max_size must be >= min_size")
        self._pool = AsyncConnectionPool(conninfo=dsn, open=False, min_size=min_size, max_size=max_size)

    async def open(self) -> None:
        await self._pool.open(wait=True)

    async def close(self) -> None:
        await self._pool.close()

    async def execute(self, sql: str, params: Sequence[Any] | None = None) -> None:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params or ())

    async def executemany(self, sql: str, rows: Iterable[Sequence[Any]]) -> None:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(sql, rows)

    async def fetch_all(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        async with self._pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(sql, params or ())
                return list(await cur.fetchall())

    async def fetch_one(self, sql: str, params: Sequence[Any] | None = None) -> dict[str, Any] | None:
        async with self._pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(sql, params or ())
                return await cur.fetchone()

    async def apply_migrations(self) -> list[str]:
        applied_now: list[str] = []
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    create table if not exists schema_migrations (
                        version text primary key,
                        applied_at timestamptz not null default now()
                    )
                    """
                )
                await conn.commit()
                await cur.execute("select version from schema_migrations")
                applied = {row[0] for row in await cur.fetchall()}
                for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
                    version = path.name
                    if version in applied:
                        continue
                    sql = path.read_text(encoding="utf-8")
                    await cur.execute(sql)
                    await cur.execute("insert into schema_migrations(version) values (%s)", (version,))
                    await conn.commit()
                    applied_now.append(version)
        return applied_now

    async def set_cursor(self, key: str, value: str, meta: dict[str, Any] | None = None) -> None:
        await self.execute(
            """
            insert into ops_cursor(cursor_key, cursor_value, meta, updated_at)
            values (%s, %s, %s::jsonb, now())
            on conflict (cursor_key) do update
            set cursor_value = excluded.cursor_value,
                meta = excluded.meta,
                updated_at = excluded.updated_at
            """,
            (key, value, _to_json(meta or {})),
        )

    async def get_cursor(self, key: str) -> dict[str, Any] | None:
        return await self.fetch_one("select * from ops_cursor where cursor_key = %s", (key,))


def _to_json(payload: dict[str, Any]) -> str:
    import json

    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
