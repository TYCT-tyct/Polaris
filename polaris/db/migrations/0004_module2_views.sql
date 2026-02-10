create or replace view view_arb_pnl_daily as
select
    date_trunc('day', created_at) as day_bucket,
    strategy_code,
    mode,
    source_code,
    count(*) as trades,
    sum(net_pnl_usd) as net_pnl_usd,
    sum(gross_pnl_usd) as gross_pnl_usd,
    sum(capital_used_usd) as capital_used_usd
from arb_trade_result
group by 1, 2, 3, 4;

create or replace view view_arb_by_strategy_mode_source as
select
    strategy_code,
    mode,
    source_code,
    count(*) as trades,
    sum(case when net_pnl_usd > 0 then 1 else 0 end) as wins,
    sum(case when net_pnl_usd < 0 then 1 else 0 end) as losses,
    sum(net_pnl_usd) as net_pnl_usd,
    avg(net_pnl_usd) as avg_trade_pnl_usd,
    sum(capital_used_usd) as turnover_usd
from arb_trade_result
group by 1, 2, 3;

create or replace view view_arb_turnover as
select
    date_trunc('hour', created_at) as hour_bucket,
    strategy_code,
    mode,
    source_code,
    sum(capital_used_usd) as turnover_usd,
    count(*) as trades
from arb_trade_result
group by 1, 2, 3, 4;

create or replace view view_arb_hit_ratio as
select
    strategy_code,
    mode,
    source_code,
    count(*)::numeric as trades,
    sum(case when net_pnl_usd > 0 then 1 else 0 end)::numeric as wins,
    case
        when count(*) = 0 then 0
        else sum(case when net_pnl_usd > 0 then 1 else 0 end)::numeric / count(*)::numeric
    end as hit_ratio
from arb_trade_result
group by 1, 2, 3;
