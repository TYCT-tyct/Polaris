create table if not exists arb_scan_diag (
    scan_diag_id bigserial primary key,
    mode text not null,
    strategy_code text not null,
    source_code text not null default 'polymarket',
    run_tag text not null default '',
    evaluated integer not null default 0,
    processed integer not null default 0,
    approved integer not null default 0,
    executed integer not null default 0,
    rejected integer not null default 0,
    expired integer not null default 0,
    blocked_reason_topn jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_arb_scan_diag_core
    on arb_scan_diag(created_at desc, mode, source_code, strategy_code, run_tag);

alter table arb_position_lot
    add column if not exists run_tag text not null default '';

create index if not exists idx_arb_position_lot_run_tag_created
    on arb_position_lot(run_tag, mode, source_code, opened_at desc);
