from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from polaris.db.pool import Database

from .buckets import infer_true_bucket_label
from .types import M4ScoreResult


@dataclass(frozen=True)
class M4ScoreConfig:
    since_hours: int = 24
    ece_bins: int = 10
    score_log_weight: float = 1.0
    score_ece_weight: float = 2.0
    score_brier_weight: float = 0.5
    score_realized_weight: float = 0.25
    score_drawdown_weight: float = 0.4


class M4Scorer:
    def __init__(self, db: Database, config: M4ScoreConfig | None = None) -> None:
        self._db = db
        self._cfg = config or M4ScoreConfig()

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
        hours = int(since_hours or self._cfg.since_hours)
        since_ts = datetime.now(tz=UTC) - timedelta(hours=max(1, hours))
        rows = await self._load_scoring_rows(
            mode=mode,
            source_code=source_code,
            run_tag=run_tag,
            window_code=window_code,
            since_ts=since_ts,
        )

        log_values: list[float] = []
        ece_pairs: list[tuple[float, int]] = []
        brier_values: list[float] = []
        for row in rows:
            posterior = row["posterior"]
            if not posterior:
                continue
            true_label = infer_true_bucket_label(row["final_count"], posterior.keys())
            if true_label is None:
                continue
            p_true = max(1e-9, float(posterior.get(true_label, 0.0)))
            log_values.append(math.log(p_true))
            predicted_label = _argmax_label(posterior)
            pred_conf = max(0.0, min(1.0, float(posterior.get(predicted_label, 0.0))))
            ece_pairs.append((pred_conf, 1 if predicted_label == true_label else 0))
            brier_values.append(multiclass_brier(posterior, true_label))

        log_score = (sum(log_values) / len(log_values)) if log_values else None
        ece = expected_calibration_error(ece_pairs, bins=self._cfg.ece_bins) if ece_pairs else None
        brier = (sum(brier_values) / len(brier_values)) if brier_values else None

        realized_net, max_drawdown = await self._realized_metrics(
            mode=mode,
            source_code=source_code,
            run_tag=run_tag,
            since_ts=since_ts,
        )
        score_breakdown = self._compose_breakdown(
            log_score=log_score,
            ece=ece,
            brier=brier,
            realized_net=realized_net,
            max_drawdown=max_drawdown,
        )
        score_total = sum(score_breakdown.values())
        result = M4ScoreResult(
            run_tag=run_tag or "all",
            mode=mode,
            source_code=source_code,
            window_code=window_code,
            since_hours=hours,
            markets_scored=len(log_values),
            log_score=log_score,
            ece=ece,
            brier=brier,
            realized_net_pnl_usd=realized_net,
            max_drawdown_usd=max_drawdown,
            score_total=score_total,
            score_breakdown={k: round(v, 8) for k, v in score_breakdown.items()},
            metadata={
                "rows_loaded": len(rows),
                "scored_points": len(log_values),
            },
        )
        if persist:
            await self._persist(result)
        return result

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
        since_ts = datetime.now(tz=UTC) - timedelta(hours=max(1, since_hours))
        rows = await self._db.fetch_all(
            """
            select
                score_id,
                run_tag,
                mode,
                source_code,
                window_code,
                log_score,
                ece,
                brier,
                realized_net_pnl_usd,
                max_drawdown_usd,
                score_total,
                created_at
            from m4_model_score
            where created_at >= %s
              and mode = %s
              and source_code = %s
              and window_code = 'fused'
            order by score_total desc, created_at desc
            """,
            (since_ts, mode, source_code),
        )
        out: list[dict[str, object]] = []
        for row in rows:
            score_total = float(row.get("score_total") or 0.0)
            ece = float(row.get("ece")) if row.get("ece") is not None else None
            log_score = float(row.get("log_score")) if row.get("log_score") is not None else None
            reasons: list[str] = []
            if score_total < min_score:
                reasons.append("score_below_min")
            if ece is None or ece > max_ece:
                reasons.append("ece_too_high_or_missing")
            if log_score is None or log_score < min_log_score:
                reasons.append("log_score_too_low_or_missing")
            record = dict(row)
            record["reasons"] = reasons
            if not reasons:
                out.append(record)
            if len(out) >= top_n:
                break
        return out

    async def score_staged(
        self,
        *,
        mode: str,
        source_code: str,
        run_tag: str | None,
        window_code: str = "fused",
        since_hours: int | None = None,
    ) -> dict[str, M4ScoreResult]:
        hours = int(since_hours or self._cfg.since_hours)
        since_ts = datetime.now(tz=UTC) - timedelta(hours=max(1, hours))
        rows = await self._load_scoring_rows(
            mode=mode,
            source_code=source_code,
            run_tag=run_tag,
            window_code=window_code,
            since_ts=since_ts,
        )

        # Define stages (hours remaining)
        stages = {
            "24h+": lambda rem: rem >= 24.0,
            "12-24h": lambda rem: 12.0 <= rem < 24.0,
            "06-12h": lambda rem: 6.0 <= rem < 12.0,
            "01-06h": lambda rem: 1.0 <= rem < 6.0,
            "<01h": lambda rem: rem < 1.0,
        }

        buckets: dict[str, list[dict]] = {k: [] for k in stages}

        for row in rows:
            created_at = row["created_at"]
            end_date = row.get("end_date")
            if not isinstance(created_at, datetime) or not isinstance(end_date, datetime):
                continue

            rem_hours = (end_date - created_at).total_seconds() / 3600.0
            if rem_hours < 0:
                continue # Already settled

            for label, func in stages.items():
                if func(rem_hours):
                    buckets[label].append(row)
                    break

        results = {}
        for label, bucket_rows in buckets.items():
            if not bucket_rows:
                continue

            log_values: list[float] = []
            ece_pairs: list[tuple[float, int]] = []
            brier_values: list[float] = []

            for row in bucket_rows:
                posterior = row["posterior"]
                if not posterior:
                    continue
                true_label = infer_true_bucket_label(row["final_count"], posterior.keys())
                if true_label is None:
                    continue
                p_true = max(1e-9, float(posterior.get(true_label, 0.0)))
                log_values.append(math.log(p_true))
                predicted_label = _argmax_label(posterior)
                pred_conf = max(0.0, min(1.0, float(posterior.get(predicted_label, 0.0))))
                ece_pairs.append((pred_conf, 1 if predicted_label == true_label else 0))
                brier_values.append(multiclass_brier(posterior, true_label))

            log_score = (sum(log_values) / len(log_values)) if log_values else None
            ece = expected_calibration_error(ece_pairs, bins=self._cfg.ece_bins) if ece_pairs else None
            brier = (sum(brier_values) / len(brier_values)) if brier_values else None

            # Note: Realized PnL is hard to stage without granular trade logs, omitting for now
            # We can approximate or just leave it 0

            results[label] = M4ScoreResult(
                run_tag=run_tag or "staged",
                mode=mode,
                source_code=source_code,
                window_code=window_code,
                since_hours=hours,
                markets_scored=len(log_values),
                log_score=log_score,
                ece=ece,
                brier=brier,
                realized_net_pnl_usd=0.0,
                max_drawdown_usd=0.0,
                score_total=0.0, # Not used for staging
                score_breakdown={},
                metadata={"stage": label, "count": len(bucket_rows)},
            )

        return results

    async def _load_scoring_rows(
        self,
        *,
        mode: str,
        source_code: str,
        run_tag: str | None,
        window_code: str,
        since_ts: datetime,
    ) -> list[dict[str, object]]:
        params: list[object] = [since_ts, mode, source_code, window_code]
        run_clause = ""
        if run_tag:
            run_clause = "and ps.run_tag = %s"
            params.append(run_tag)
        rows = await self._db.fetch_all(
            f"""
            with latest as (
                select
                    ps.market_id,
                    ps.window_code,
                    ps.tracking_id,
                    ps.account_id,
                    ps.posterior_pmf,
                    ps.created_at,
                    tw.end_date
                from m4_posterior_snapshot ps
                left join dim_tracking_window tw
                  on tw.tracking_id = ps.tracking_id
                 and tw.account_id = ps.account_id
                where ps.created_at >= %s
                  and ps.mode = %s
                  and ps.source_code = %s
                  and ps.window_code = %s
                  {run_clause}
                -- For staging, we might want ALL snapshots, not just distinct/latest per market
                -- But distinct gives the 'latest view', which is biased if run_tag spans time
                -- If run_tag is a replay, we want ALL points.
                -- Let's remove distinct on to get dense data for replay.
            )
            select
                l.market_id,
                l.window_code,
                l.tracking_id,
                l.account_id,
                l.posterior_pmf,
                l.created_at,
                l.end_date,
                coalesce(
                    (
                        select fm.cumulative_count_est
                        from fact_tweet_metric_1m fm
                        where fm.account_id = l.account_id
                          and fm.tracking_id = l.tracking_id
                          and fm.minute_bucket <= coalesce(l.end_date, now())
                        order by fm.minute_bucket desc
                        limit 1
                    ),
                    (
                        select max(fd.cumulative_count)::int
                        from fact_tweet_metric_daily fd
                        where fd.account_id = l.account_id
                          and fd.tracking_id = l.tracking_id
                          and fd.metric_date <= coalesce(l.end_date::date, now()::date)
                    ),
                    0
                ) as final_count
            from latest l
            where l.end_date is not null
              and l.end_date <= now()
            order by l.created_at asc
            """,
            tuple(params),
        )
        parsed: list[dict[str, object]] = []
        for row in rows:
            posterior = _parse_json_obj(row.get("posterior_pmf"))
            parsed.append(
                {
                    "market_id": row["market_id"],
                    "window_code": row["window_code"],
                    "final_count": int(row.get("final_count") or 0),
                    "posterior": {str(k): float(v) for k, v in posterior.items()},
                    "created_at": row["created_at"],
                    "end_date": row["end_date"],
                }
            )
        return parsed

    async def _realized_metrics(
        self,
        *,
        mode: str,
        source_code: str,
        run_tag: str | None,
        since_ts: datetime,
    ) -> tuple[float, float]:
        params: list[object] = [since_ts, mode, source_code]
        run_clause = ""
        if run_tag:
            run_clause = "and run_tag = %s"
            params.append(run_tag)
        rows = await self._db.fetch_all(
            f"""
            select created_at, coalesce(realized_net_pnl_usd, 0)::float8 as pnl
            from m4_decision_log
            where created_at >= %s
              and mode = %s
              and source_code = %s
              {run_clause}
            order by created_at asc
            """,
            tuple(params),
        )
        realized = 0.0
        equity = 0.0
        peak = 0.0
        max_drawdown = 0.0
        for row in rows:
            pnl = float(row.get("pnl") or 0.0)
            realized += pnl
            equity += pnl
            peak = max(peak, equity)
            max_drawdown = max(max_drawdown, max(0.0, peak - equity))
        return float(realized), float(max_drawdown)

    async def _persist(self, result: M4ScoreResult) -> None:
        await self._db.execute(
            """
            insert into m4_model_score(
                run_tag, mode, source_code, window_code, since_hours, markets_scored,
                log_score, ece, brier, realized_net_pnl_usd, max_drawdown_usd, score_total,
                score_breakdown, metadata, created_at
            )
            values (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s::jsonb, %s::jsonb, now()
            )
            """,
            (
                result.run_tag,
                result.mode,
                result.source_code,
                result.window_code,
                result.since_hours,
                result.markets_scored,
                result.log_score,
                result.ece,
                result.brier,
                result.realized_net_pnl_usd,
                result.max_drawdown_usd,
                result.score_total,
                json.dumps(result.score_breakdown, ensure_ascii=True, separators=(",", ":")),
                json.dumps(result.metadata, ensure_ascii=True, separators=(",", ":")),
            ),
        )

    def _compose_breakdown(
        self,
        *,
        log_score: float | None,
        ece: float | None,
        brier: float | None,
        realized_net: float,
        max_drawdown: float,
    ) -> dict[str, float]:
        log_term = float(log_score if log_score is not None else -6.0) * self._cfg.score_log_weight
        ece_term = -float(ece if ece is not None else 1.0) * self._cfg.score_ece_weight
        brier_term = -float(brier if brier is not None else 1.0) * self._cfg.score_brier_weight
        realized_term = math.tanh(realized_net / 10.0) * self._cfg.score_realized_weight
        drawdown_term = -math.tanh(max_drawdown / 10.0) * self._cfg.score_drawdown_weight
        return {
            "log_score": log_term,
            "ece_penalty": ece_term,
            "brier_penalty": brier_term,
            "realized_bonus": realized_term,
            "drawdown_penalty": drawdown_term,
        }


def _parse_json_obj(value: object) -> dict[str, float]:
    if isinstance(value, dict):
        return {str(k): float(v) for k, v in value.items()}
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(decoded, dict):
            return {str(k): float(v) for k, v in decoded.items()}
    return {}


def _argmax_label(pmf: dict[str, float]) -> str:
    if not pmf:
        return ""
    return max(sorted(pmf.keys()), key=lambda key: float(pmf.get(key, 0.0)))


def multiclass_brier(pmf: dict[str, float], true_label: str) -> float:
    labels = list(pmf.keys())
    if not labels:
        return 1.0
    k = len(labels)
    err = 0.0
    for label in labels:
        p = float(pmf.get(label, 0.0))
        y = 1.0 if label == true_label else 0.0
        err += (p - y) ** 2
    return err / float(k)


def expected_calibration_error(points: list[tuple[float, int]], *, bins: int) -> float:
    if not points:
        return 1.0
    n = len(points)
    bin_stats: list[list[float]] = [[0.0, 0.0, 0.0] for _ in range(max(1, bins))]
    for confidence, correct in points:
        idx = min(max(0, int(confidence * bins)), bins - 1)
        bin_stats[idx][0] += 1.0
        bin_stats[idx][1] += confidence
        bin_stats[idx][2] += float(correct)
    ece = 0.0
    for count, conf_sum, corr_sum in bin_stats:
        if count <= 0:
            continue
        mean_conf = conf_sum / count
        mean_acc = corr_sum / count
        ece += (count / n) * abs(mean_conf - mean_acc)
    return float(ece)
