-- Module2: paper 组合快照（用于真实资金/仓位模拟的可追溯报表）
create table if not exists arb_portfolio_snapshot (
    snapshot_id bigserial primary key,
    mode text not null,
    source_code text not null default 'polymarket',
    run_tag text not null default '',
    -- 资金池维度：shared 或单策略代码(A/B/C/F/G)
    scope text not null,

    cash_balance_usd numeric(20, 8) not null,
    exposure_usd numeric(20, 8) not null,
    nav_mtm_usd numeric(20, 8) not null,
    open_lots integer not null default 0,

    payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_arb_portfolio_snapshot_core
    on arb_portfolio_snapshot(created_at desc, mode, source_code, scope, run_tag);

