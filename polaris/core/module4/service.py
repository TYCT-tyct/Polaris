from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from polaris.config import PolarisSettings
from polaris.db.pool import Database

from polaris.agents.evidence_agent import EvidenceAgent, EvidenceAgentConfig
from polaris.agents.evidence_tools import EvidenceTools
from polaris.agents.llm_semantic_agent import LLMSemanticAgent, LLMSemanticAgentConfig

from .collapse_filter import CollapseFilter, CollapseFilterConfig
from .conductor import Conductor, ConductorConfig
from .prior_builder import MarketPriorBuilder, PriorBuilderConfig
from .regime_engine import RegimeEngine, compute_progress
from .rule_compiler import RuleCompiler
from .scorer import M4ScoreConfig, M4Scorer
from .semantic_adjuster import SemanticAdjuster, SemanticAdjusterConfig
from .types import M4Evidence, M4Posterior, M4ScoreResult, M4Decision, RuleVersion


@dataclass(frozen=True)
class Module4RunSummary:
    run_tag: str
    mode: str
    source_code: str
    markets_total: int
    markets_with_prior: int
    snapshots_written: int
    decisions_written: int
    evidence_written: int
    skipped: dict[str, int]


class Module4Service:
    def __init__(self, db: Database, settings: PolarisSettings) -> None:
        self._db = db
        self._settings = settings
        self._rules = RuleCompiler(db)
        self._prior = MarketPriorBuilder(
            db,
            PriorBuilderConfig(
                spread_ref=settings.m4_prior_spread_ref,
                depth_ref=settings.m4_prior_depth_ref,
                sample_ref=settings.m4_prior_sample_ref,
                tail_default_short=settings.m4_collapse_tail_default_short,
                tail_default_week=settings.m4_collapse_tail_default_week,
            ),
        )
        self._collapse = CollapseFilter(
            CollapseFilterConfig(
                dispersion=settings.m4_collapse_dispersion,
                tail_default_short=settings.m4_collapse_tail_default_short,
                tail_default_week=settings.m4_collapse_tail_default_week,
            )
        )
        self._regime = RegimeEngine(db)
        semantic_provider = (settings.m4_semantic_provider or "").strip().lower()
        semantic_api_key = settings.m4_semantic_api_key
        if not semantic_api_key:
            if semantic_provider == "minimax":
                semantic_api_key = settings.arb_minimax_api_key
            elif semantic_provider == "openai":
                semantic_api_key = settings.arb_openai_api_key
        semantic_llm = LLMSemanticAgent(
            LLMSemanticAgentConfig(
                enabled=settings.m4_semantic_llm_enabled and settings.m4_agent_enabled,
                provider=settings.m4_semantic_provider,
                model=settings.m4_semantic_model,
                api_key=semantic_api_key,
                base_url=settings.m4_semantic_base_url,
                timeout_sec=settings.m4_semantic_timeout_sec,
                max_calls=settings.m4_semantic_max_calls,
                confidence_floor=settings.m4_semantic_confidence_floor,
                json_strict=settings.m4_semantic_json_strict,
                scope=settings.m4_semantic_scope,
            )
        )
        self._conductor = Conductor(
            ConductorConfig(
                min_reliability=settings.m4_decision_min_reliability,
                min_net_edge=settings.m4_decision_min_net_edge,
                fee_bps=settings.m4_fee_bps,
                slippage_bps=settings.m4_slippage_bps,
                latency_buffer_bps=settings.m4_latency_buffer_bps,
                window_weight_short=settings.m4_window_weight_short,
                window_weight_week=settings.m4_window_weight_week,
            )
        )
        self._evidence = EvidenceAgent(
            EvidenceTools(db),
            semantic_llm,
            EvidenceAgentConfig(
                enabled=settings.m4_agent_enabled,
                max_calls=settings.m4_semantic_max_calls,
                timeout_sec=settings.m4_semantic_timeout_sec,
                confidence_floor=settings.m4_semantic_confidence_floor,
                fail_mode=settings.m4_semantic_fail_mode,
                model=settings.m4_semantic_model,
                provider=settings.m4_semantic_provider,
                api_key=semantic_api_key,
                json_strict=settings.m4_semantic_json_strict,
                scope=settings.m4_semantic_scope,
            ),
        )
        self._semantic = SemanticAdjuster(
            SemanticAdjusterConfig(
                confidence_floor=settings.m4_semantic_confidence_floor,
                mean_shift_cap=settings.m4_semantic_mean_shift_cap,
                uncertainty_cap=settings.m4_semantic_uncertainty_cap,
            )
        )
        self._scorer = M4Scorer(
            db,
            M4ScoreConfig(since_hours=settings.m4_score_since_hours),
        )

    async def compile_rules(
        self,
        *,
        market_id: str | None = None,
        force: bool = False,
    ) -> list[RuleVersion]:
        if market_id:
            compiled = await self._rules.compile_market(market_id=market_id, force=force)
            return [compiled] if compiled is not None else []
        return await self._rules.compile_open_tweet_markets(force=force)

    async def run_once(
        self,
        *,
        mode: str,
        source_code: str,
        run_tag: str,
        market_id: str | None = None,
        use_agent: bool | None = None,
        as_of: datetime | None = None,
        include_closed: bool = False,
        require_count_buckets: bool = False,
    ) -> Module4RunSummary:
        run_tag_norm = _normalize_run_tag(run_tag)
        now = as_of or datetime.now(tz=UTC)
        markets = await self._load_target_markets(
            market_id=market_id,
            as_of=now,
            include_closed=include_closed,
            require_count_buckets=require_count_buckets,
        )
        snapshots_written = 0
        decisions_written = 0
        evidence_written = 0
        markets_with_prior = 0
        skipped: dict[str, int] = {}
        use_agent_final = self._settings.m4_agent_enabled if use_agent is None else bool(use_agent)

        for market in markets:
            rule_version = await self._rules.compile_market(market["market_id"], force=False)
            if rule_version is None:
                _inc(skipped, "rule_missing")
                continue

            observed = await self._load_observed_count(
                account_id=market["account_id"],
                tracking_id=market["tracking_id"],
                as_of=now,
            )
            progress = compute_progress(now, market["start_ts"], market["end_ts"])
            regime = await self._regime.estimate(
                account_id=market["account_id"],
                progress=progress,
                as_of=now,
            )
            evidence = await self._resolve_evidence(
                enabled=use_agent_final,
                account_id=market["account_id"],
                market_id=market["market_id"],
                window_code="fused",
                run_tag=run_tag_norm,
                observed_count=observed,
                progress=progress,
                as_of=now,
            )

            if evidence is not None:
                await self._persist_evidence(
                    run_tag=run_tag_norm,
                    mode=mode,
                    source_code=source_code,
                    market=market,
                    evidence=evidence,
                )
                evidence_written += 1

            posteriors: list[M4Posterior] = []
            prior_reliabilities: list[float] = []
            for window_code in ("short", "week"):
                prior = await self._prior.build(
                    market["market_id"],
                    window_code=window_code,
                    as_of=now,
                )
                if prior is None:
                    _inc(skipped, f"prior_missing_{window_code}")
                    continue
                markets_with_prior += 1
                prior_reliabilities.append(prior.prior_reliability)
                uncertainty = (evidence.uncertainty_delta if evidence else 0.0) + ((1.0 - prior.prior_reliability) * 0.15)
                progress_effective = regime.progress_effective + (evidence.delta_progress if evidence else 0.0)
                progress_effective = max(0.001, min(0.999, progress_effective))
                posterior = self._collapse.update(
                    market_id=market["market_id"],
                    window_code=window_code,
                    prior_pmf=prior.pmf,
                    observed_count=observed,
                    progress=progress,
                    progress_effective=progress_effective,
                    uncertainty_boost=uncertainty,
                )
                posteriors.append(posterior)
                await self._persist_snapshot(
                    run_tag=run_tag_norm,
                    mode=mode,
                    source_code=source_code,
                    market=market,
                    rule_version=rule_version,
                    posterior=posterior,
                    prior_reliability=prior.prior_reliability,
                    metadata={
                        "prior_meta": prior.metadata,
                        "regime_meta": regime.metadata,
                        "evidence_tags": list(evidence.event_tags) if evidence else [],
                        "replay_as_of": now.isoformat(),
                    },
                )
                snapshots_written += 1

            if not posteriors:
                _inc(skipped, "no_window_posterior")
                continue

            fused = self._conductor.fuse_posteriors(posteriors)
            baseline_decision = self._conductor.decide(
                posterior=fused,
                prior_reliability=(sum(prior_reliabilities) / len(prior_reliabilities)) if prior_reliabilities else 0.0,
            )
            adjusted, semantic_diag = self._semantic.apply(fused, evidence)
            prior_reliability = (sum(prior_reliabilities) / len(prior_reliabilities)) if prior_reliabilities else 0.0
            decision = self._conductor.decide(posterior=adjusted, prior_reliability=prior_reliability)
            await self._persist_snapshot(
                run_tag=run_tag_norm,
                mode=mode,
                source_code=source_code,
                market=market,
                rule_version=rule_version,
                posterior=adjusted,
                prior_reliability=prior_reliability,
                metadata={
                    "component_windows": [p.window_code for p in posteriors],
                    "evidence_tags": list(evidence.event_tags) if evidence else [],
                    "baseline_posterior_pmf": fused.pmf_posterior,
                    "baseline_expected_count": fused.expected_count,
                    "baseline_entropy": fused.entropy,
                    "semantic_applied": semantic_diag.applied,
                    "semantic_confidence": semantic_diag.confidence_used,
                    "mean_shift_applied": semantic_diag.mean_shift_applied,
                    "uncertainty_applied": semantic_diag.uncertainty_applied,
                    "semantic_meta": semantic_diag.metadata,
                    "replay_as_of": now.isoformat(),
                },
            )
            snapshots_written += 1
            await self._persist_decision(
                run_tag=run_tag_norm,
                mode=mode,
                source_code=source_code,
                market=market,
                rule_version=rule_version,
                decision=decision,
                posterior=adjusted,
                metadata={
                    "evidence_tags": list(evidence.event_tags) if evidence else [],
                    "evidence_confidence": evidence.confidence if evidence else 0.0,
                    "semantic_delta_edge": decision.edge_expected - baseline_decision.edge_expected,
                    "semantic_delta_es95": decision.es95_loss - baseline_decision.es95_loss,
                    "replay_as_of": now.isoformat(),
                },
            )
            decisions_written += 1

        return Module4RunSummary(
            run_tag=run_tag_norm,
            mode=mode,
            source_code=source_code,
            markets_total=len(markets),
            markets_with_prior=markets_with_prior,
            snapshots_written=snapshots_written,
            decisions_written=decisions_written,
            evidence_written=evidence_written,
            skipped=skipped,
        )

    async def score(
        self,
        *,
        mode: str,
        source_code: str,
        run_tag: str | None,
        window_code: str = "fused",
        since_hours: int | None = None,
        persist: bool = True,
    ) -> M4ScoreResult:
        return await self._scorer.score(
            mode=mode,
            source_code=source_code,
            run_tag=run_tag,
            window_code=window_code,
            since_hours=since_hours,
            persist=persist,
        )

    async def top_candidates(
        self,
        *,
        mode: str,
        source_code: str,
        since_hours: int,
        top_n: int,
        min_score: float,
        max_ece: float,
        min_log_score: float,
    ) -> list[dict[str, object]]:
        return await self._scorer.top_candidates(
            mode=mode,
            source_code=source_code,
            since_hours=since_hours,
            top_n=top_n,
            min_score=min_score,
            max_ece=max_ece,
            min_log_score=min_log_score,
        )

    async def resolve_run_tag_filter(
        self,
        *,
        raw: str,
        mode: str,
        source_code: str,
    ) -> str | None:
        text = (raw or "").strip().lower()
        if text in ("", "all"):
            return None
        if text != "current":
            return _normalize_run_tag(raw)
        row = await self._db.fetch_one(
            """
            select run_tag
            from m4_posterior_snapshot
            where mode = %s
              and source_code = %s
            order by created_at desc
            limit 1
            """,
            (mode, source_code),
        )
        if row and row.get("run_tag"):
            return str(row["run_tag"])
        return None

    async def _load_target_markets(
        self,
        *,
        market_id: str | None = None,
        as_of: datetime | None = None,
        include_closed: bool = False,
        require_count_buckets: bool = False,
    ) -> list[dict[str, Any]]:
        as_of_ts = as_of or datetime.now(tz=UTC)
        params: list[Any] = [as_of_ts, as_of_ts, as_of_ts]
        market_clause = ""
        state_clause = ""
        bucket_clause = ""
        if not include_closed:
            state_clause = "and m.closed = false and m.active = true"
        if require_count_buckets:
            bucket_clause = """
              and exists (
                    select 1
                    from dim_token tk
                    where tk.market_id = m.market_id
                      and tk.outcome_label ~ '[0-9]'
              )
            """
        if market_id:
            market_clause = "and m.market_id = %s"
            params.append(market_id)
        rows = await self._db.fetch_all(
            f"""
            with ranked as (
                select
                    b.market_id,
                    b.tracking_id,
                    b.account_id,
                    b.match_score,
                    b.mapped_at,
                    row_number() over (partition by b.market_id order by b.match_score desc, b.mapped_at desc) as rn
                from bridge_market_tracking b
            )
            select
                m.market_id,
                m.question,
                coalesce(m.start_date, tw.start_date) as start_ts,
                coalesce(m.end_date, tw.end_date) as end_ts,
                r.tracking_id,
                r.account_id,
                r.match_score
            from dim_market m
            join ranked r on r.market_id = m.market_id and r.rn = 1
            left join dim_tracking_window tw
              on tw.tracking_id = r.tracking_id
             and tw.account_id = r.account_id
            where
                  coalesce(m.start_date, tw.start_date) <= %s
              and coalesce(m.end_date, tw.end_date) > %s
              and coalesce(m.end_date, tw.end_date) > %s - interval '8 days'
              {state_clause}
              and (
                    m.question ilike '%%tweet%%'
                 or m.slug ilike '%%tweet%%'
              )
              {bucket_clause}
              {market_clause}
            order by coalesce(m.end_date, tw.end_date) asc, m.market_id asc
            """,
            tuple(params),
        )
        out: list[dict[str, Any]] = []
        for row in rows:
            start_ts = row.get("start_ts")
            end_ts = row.get("end_ts")
            if not isinstance(start_ts, datetime) or not isinstance(end_ts, datetime):
                continue
            out.append(
                {
                    "market_id": str(row["market_id"]),
                    "question": str(row.get("question") or ""),
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "tracking_id": str(row["tracking_id"]),
                    "account_id": str(row["account_id"]),
                    "match_score": float(row.get("match_score") or 0.0),
                }
            )
        return out

    async def _load_observed_count(self, *, account_id: str, tracking_id: str, as_of: datetime) -> int:
        row = await self._db.fetch_one(
            """
            select cumulative_count_est
            from fact_tweet_metric_1m
            where account_id = %s
              and tracking_id = %s
              and minute_bucket <= %s
            order by minute_bucket desc
            limit 1
            """,
            (account_id, tracking_id, as_of),
        )
        if row and row.get("cumulative_count_est") is not None:
            return int(row["cumulative_count_est"])
        day = await self._db.fetch_one(
            """
            select max(cumulative_count)::int as c
            from fact_tweet_metric_daily
            where account_id = %s
              and tracking_id = %s
              and metric_date <= %s::date
            """,
            (account_id, tracking_id, as_of),
        )
        return int(day["c"] if day and day["c"] is not None else 0)

    async def _resolve_evidence(
        self,
        *,
        enabled: bool,
        account_id: str,
        market_id: str,
        window_code: str,
        run_tag: str,
        observed_count: int,
        progress: float,
        as_of: datetime,
    ) -> M4Evidence | None:
        if not enabled:
            return None
        return await self._evidence.analyze(
            account_id=account_id,
            market_id=market_id,
            window_code=window_code,
            run_tag=run_tag,
            observed_count=observed_count,
            progress=progress,
            as_of=as_of,
        )

    async def _persist_snapshot(
        self,
        *,
        run_tag: str,
        mode: str,
        source_code: str,
        market: dict[str, Any],
        rule_version: RuleVersion,
        posterior: M4Posterior,
        prior_reliability: float,
        metadata: dict[str, Any],
    ) -> None:
        await self._db.execute(
            """
            insert into m4_posterior_snapshot(
                run_tag, mode, source_code, window_code, market_id,
                tracking_id, account_id, rule_version_id,
                progress, progress_effective, observed_count,
                expected_count, entropy, prior_reliability,
                prior_pmf, posterior_pmf, quantiles, metadata, created_at
            )
            values (
                %s, %s, %s, %s, %s,
                %s, %s, %s::uuid,
                %s, %s, %s,
                %s, %s, %s,
                %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, now()
            )
            """,
            (
                run_tag,
                mode,
                source_code,
                posterior.window_code,
                market["market_id"],
                market["tracking_id"],
                market["account_id"],
                rule_version.rule_version_id,
                posterior.progress,
                posterior.progress_effective,
                posterior.observed_count,
                posterior.expected_count,
                posterior.entropy,
                prior_reliability,
                _to_json(posterior.pmf_prior),
                _to_json(posterior.pmf_posterior),
                _to_json(posterior.quantiles),
                _to_json(metadata),
            ),
        )

    async def _persist_decision(
        self,
        *,
        run_tag: str,
        mode: str,
        source_code: str,
        market: dict[str, Any],
        rule_version: RuleVersion,
        decision: M4Decision,
        posterior: M4Posterior,
        metadata: dict[str, Any],
    ) -> None:
        await self._db.execute(
            """
            insert into m4_decision_log(
                run_tag, mode, source_code, window_code, market_id,
                tracking_id, account_id, rule_version_id,
                action, reason_codes, prior_bucket, target_bucket,
                target_prob_prior, target_prob_posterior, edge_expected, es95_loss,
                net_edge, size_usd, fee_est_usd, slippage_est_usd, realized_net_pnl_usd,
                metadata, created_at
            )
            values (
                %s, %s, %s, %s, %s,
                %s, %s, %s::uuid,
                %s, %s::text[], %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s::jsonb, now()
            )
            """,
            (
                run_tag,
                mode,
                source_code,
                posterior.window_code,
                market["market_id"],
                market["tracking_id"],
                market["account_id"],
                rule_version.rule_version_id,
                decision.action,
                list(decision.reason_codes),
                decision.prior_bucket,
                decision.target_bucket,
                decision.target_prob_prior,
                decision.target_prob_posterior,
                decision.edge_expected,
                decision.es95_loss,
                decision.net_edge,
                decision.size_usd,
                decision.fee_est_usd,
                decision.slippage_est_usd,
                None,
                _to_json({**metadata, "decision_meta": decision.metadata}),
            ),
        )

    async def _persist_evidence(
        self,
        *,
        run_tag: str,
        mode: str,
        source_code: str,
        market: dict[str, Any],
        evidence: M4Evidence,
    ) -> None:
        await self._db.execute(
            """
            insert into m4_evidence_log(
                run_tag, mode, source_code, market_id, tracking_id, account_id,
                agent_name, calls_used, timed_out, degraded, confidence, uncertainty_delta,
                event_tags, sources, metadata,
                llm_model, prompt_version, latency_ms, parse_ok, applied, error_code,
                created_at
            )
            values (
                %s, %s, %s, %s, %s, %s,
                'llm_semantic_agent', %s, %s, %s, %s, %s,
                %s::text[], %s::jsonb, %s::jsonb,
                %s, %s, %s, %s, %s, %s,
                now()
            )
            """,
            (
                run_tag,
                mode,
                source_code,
                market["market_id"],
                market["tracking_id"],
                market["account_id"],
                evidence.calls_used,
                evidence.timed_out,
                evidence.degraded,
                evidence.confidence,
                evidence.uncertainty_delta,
                list(evidence.event_tags),
                _to_json(list(evidence.sources)),
                _to_json(evidence.metadata),
                evidence.llm_model,
                evidence.prompt_version,
                evidence.latency_ms,
                evidence.parse_ok,
                not evidence.degraded,
                evidence.error_code,
            ),
        )


def _inc(counter: dict[str, int], key: str) -> None:
    counter[key] = int(counter.get(key, 0)) + 1


def _normalize_run_tag(raw: str) -> str:
    text = (raw or "").strip()
    if not text or text.lower() == "auto":
        return datetime.now(tz=UTC).strftime("m4_%Y%m%d_%H%M%S")
    keep = []
    for ch in text.lower():
        if ch.isalnum() or ch in ("-", "_", "."):
            keep.append(ch)
        else:
            keep.append("-")
    out = "".join(keep).strip("-")
    return out[:80] if out else datetime.now(tz=UTC).strftime("m4_%Y%m%d_%H%M%S")


def _to_json(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"), default=str)
