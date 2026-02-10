create table if not exists fact_quote_top_raw_default partition of fact_quote_top_raw default;
create table if not exists fact_quote_depth_raw_default partition of fact_quote_depth_raw default;
create table if not exists fact_orderbook_l2_raw_default partition of fact_orderbook_l2_raw default;

create or replace function polaris_create_partition(base_table text, partition_start timestamptz)
returns void
language plpgsql
as $$
declare
    part_name text;
    part_end timestamptz;
    sql_stmt text;
begin
    part_end := date_trunc('month', partition_start) + interval '1 month';
    part_name := format('%s_%s', base_table, to_char(partition_start, 'YYYYMM'));

    sql_stmt := format(
        'create table if not exists %I partition of %I for values from (%L) to (%L)',
        part_name,
        base_table,
        date_trunc('month', partition_start),
        part_end
    );
    execute sql_stmt;
end;
$$;

create or replace function polaris_ensure_partitions(months_ahead integer default 2)
returns void
language plpgsql
as $$
declare
    i integer;
    base_ts timestamptz;
begin
    for i in -1..months_ahead loop
        base_ts := date_trunc('month', now()) + (i || ' month')::interval;
        perform polaris_create_partition('fact_quote_top_raw', base_ts);
        perform polaris_create_partition('fact_quote_depth_raw', base_ts);
        perform polaris_create_partition('fact_orderbook_l2_raw', base_ts);
    end loop;
end;
$$;

select polaris_ensure_partitions(3);

create index if not exists idx_fact_quote_top_raw_token_time
on fact_quote_top_raw(token_id, captured_at desc);

create index if not exists idx_fact_quote_depth_raw_token_time
on fact_quote_depth_raw(token_id, captured_at desc);

create index if not exists idx_fact_orderbook_l2_raw_token_time
on fact_orderbook_l2_raw(token_id, captured_at desc);

create index if not exists brin_fact_quote_top_raw_time
on fact_quote_top_raw using brin(captured_at);

create index if not exists brin_fact_quote_depth_raw_time
on fact_quote_depth_raw using brin(captured_at);

create index if not exists brin_fact_orderbook_l2_raw_time
on fact_orderbook_l2_raw using brin(captured_at);

create or replace view view_quote_latest as
select distinct on (token_id)
    token_id,
    market_id,
    best_bid,
    best_ask,
    mid,
    spread,
    last_trade_price,
    source_ts,
    captured_at
from fact_quote_top_raw
order by token_id, captured_at desc;

create or replace function polaris_prune_raw(retention_days integer default 14)
returns integer
language plpgsql
as $$
declare
    rows_deleted integer := 0;
    cutoff timestamptz := now() - (retention_days || ' day')::interval;
begin
    with del1 as (
        delete from fact_quote_top_raw where captured_at < cutoff returning 1
    ), del2 as (
        delete from fact_quote_depth_raw where captured_at < cutoff returning 1
    ), del3 as (
        delete from fact_orderbook_l2_raw where captured_at < cutoff returning 1
    )
    select coalesce((select count(*) from del1), 0)
         + coalesce((select count(*) from del2), 0)
         + coalesce((select count(*) from del3), 0)
    into rows_deleted;

    return rows_deleted;
end;
$$;

