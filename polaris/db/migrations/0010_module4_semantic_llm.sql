alter table m4_evidence_log
    add column if not exists llm_model text,
    add column if not exists prompt_version text,
    add column if not exists latency_ms numeric(20, 6),
    add column if not exists parse_ok boolean,
    add column if not exists applied boolean,
    add column if not exists error_code text;

create index if not exists idx_m4_evidence_log_semantic_health
    on m4_evidence_log(created_at desc, source_code, mode, run_tag, parse_ok, applied, timed_out);

