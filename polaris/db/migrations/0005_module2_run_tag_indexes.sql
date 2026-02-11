create index if not exists idx_arb_signal_run_tag_created
    on arb_signal ((coalesce(features->>'run_tag', '')), mode, source_code, created_at desc);

create index if not exists idx_arb_trade_result_run_tag_created
    on arb_trade_result ((coalesce(metadata->>'run_tag', '')), mode, source_code, created_at desc);

create index if not exists idx_arb_cash_ledger_run_tag_created
    on arb_cash_ledger ((coalesce(payload->>'run_tag', '')), mode, source_code, created_at desc);

create index if not exists idx_arb_risk_event_run_tag_created
    on arb_risk_event ((coalesce(payload->>'run_tag', '')), mode, source_code, created_at desc);

create index if not exists idx_arb_replay_run_run_tag_started
    on arb_replay_run ((coalesce(metadata->>'run_tag', '')), mode, started_at desc);

create index if not exists idx_arb_param_snapshot_run_tag_created
    on arb_param_snapshot ((coalesce(score_breakdown->>'run_tag', '')), created_at desc);
