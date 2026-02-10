from __future__ import annotations

from polaris.db.pool import Database


class HealthAggregator:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def aggregate_last_minute(self) -> int:
        await self.db.execute(
            """
            insert into ops_api_health_minute(
                minute_bucket, source, status, runs, error_runs, avg_latency_ms, p95_latency_ms
            )
            with base as (
                select
                    date_trunc('minute', started_at) as minute_bucket,
                    source,
                    status,
                    latency_ms
                from ops_collector_run
                where started_at >= now() - interval '2 minutes'
            ),
            agg as (
                select
                    minute_bucket,
                    source,
                    status,
                    count(*)::int as runs,
                    count(*) filter (where status <> 'ok')::int as error_runs,
                    avg(latency_ms)::numeric(12,4) as avg_latency_ms,
                    percentile_cont(0.95) within group (order by latency_ms)::numeric(12,4) as p95_latency_ms
                from base
                group by minute_bucket, source, status
            )
            select * from agg
            on conflict (minute_bucket, source, status) do update
            set runs = excluded.runs,
                error_runs = excluded.error_runs,
                avg_latency_ms = excluded.avg_latency_ms,
                p95_latency_ms = excluded.p95_latency_ms,
                created_at = now()
            """
        )
        return 1

