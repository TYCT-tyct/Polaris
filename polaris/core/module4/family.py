from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from statistics import fmean
from typing import Any

from .buckets import parse_bucket_label, split_count_bucket_from_slug


@dataclass(frozen=True)
class FamilyAggregationResult:
    rows: list[dict[str, Any]]
    stats: dict[str, int]


def aggregate_market_family_rows(
    rows: list[dict[str, Any]],
    *,
    include_baseline: bool = False,
) -> FamilyAggregationResult:
    stats: Counter[str] = Counter()
    grouped: dict[tuple[str, str, datetime, datetime], dict[str, Any]] = {}
    out: list[dict[str, Any]] = []

    for row in rows:
        stats["raw_rows"] += 1
        prior = _parse_pmf_like(row.get("prior_pmf"))
        post = _parse_pmf_like(row.get("posterior_pmf"))
        baseline = _parse_pmf_like(row.get("baseline_pmf")) if include_baseline else {}
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        semantic_applied = bool(metadata.get("semantic_applied"))

        if _is_bucket_distribution(post):
            copied = dict(row)
            copied["prior_pmf"] = prior
            copied["posterior_pmf"] = post
            if include_baseline:
                copied["baseline_pmf"] = baseline
            copied["semantic_applied"] = semantic_applied
            copied["is_family_aggregated"] = False
            out.append(copied)
            stats["passthrough_rows"] += 1
            continue

        slug = str(row.get("slug") or "")
        family_slug, bucket_label = split_count_bucket_from_slug(slug)
        if not family_slug or not bucket_label:
            stats["family_parse_failed"] += 1
            continue

        yes_prior = _extract_yes_probability(prior)
        yes_post = _extract_yes_probability(post)
        yes_base = _extract_yes_probability(baseline) if include_baseline else 0.0
        if yes_prior is None or yes_post is None or (include_baseline and yes_base is None):
            stats["yes_probability_missing"] += 1
            continue

        run_tag = str(row.get("run_tag") or "_missing")
        as_of_ts = row.get("as_of_ts")
        end_date = row.get("end_date")
        if not isinstance(as_of_ts, datetime) or not isinstance(end_date, datetime):
            stats["invalid_time"] += 1
            continue
        key = (run_tag, family_slug, as_of_ts, end_date)
        g = grouped.setdefault(
            key,
            {
                "run_tag": run_tag,
                "family_slug": family_slug,
                "as_of_ts": as_of_ts,
                "end_date": end_date,
                "final_count": int(row.get("final_count") or 0),
                "market_ids": set(),
                "prior_probs": defaultdict(list),
                "post_probs": defaultdict(list),
                "base_probs": defaultdict(list),
                "semantic_applied_any": False,
            },
        )
        g["market_ids"].add(str(row.get("market_id") or "_missing"))
        g["prior_probs"][bucket_label].append(float(yes_prior))
        g["post_probs"][bucket_label].append(float(yes_post))
        if include_baseline:
            g["base_probs"][bucket_label].append(float(yes_base))
        g["semantic_applied_any"] = bool(g["semantic_applied_any"] or semantic_applied)
        stats["grouped_rows"] += 1

    for group in grouped.values():
        prior_raw = {k: fmean(v) for k, v in group["prior_probs"].items() if v}
        post_raw = {k: fmean(v) for k, v in group["post_probs"].items() if v}
        base_raw = {k: fmean(v) for k, v in group["base_probs"].items() if v}

        if len(post_raw) < 2:
            stats["family_bucket_insufficient"] += 1
            continue
        prior_norm = _normalize_pmf(prior_raw)
        post_norm = _normalize_pmf(post_raw)
        if not prior_norm or not post_norm:
            stats["family_pmf_empty"] += 1
            continue

        family_id = f"family:{group['family_slug']}"
        unit: dict[str, Any] = {
            "run_tag": group["run_tag"],
            "market_id": family_id,
            "family_id": family_id,
            "as_of_ts": group["as_of_ts"],
            "end_date": group["end_date"],
            "final_count": int(group["final_count"]),
            "prior_pmf": prior_norm,
            "posterior_pmf": post_norm,
            "semantic_applied": bool(group["semantic_applied_any"]),
            "is_family_aggregated": True,
            "market_ids": sorted(group["market_ids"]),
        }
        if include_baseline:
            unit["baseline_pmf"] = _normalize_pmf(base_raw)
        out.append(unit)
        stats["family_rows"] += 1

    stats["output_rows"] = len(out)
    return FamilyAggregationResult(rows=out, stats=dict(stats))


def _parse_pmf_like(value: object) -> dict[str, float]:
    if isinstance(value, dict):
        out: dict[str, float] = {}
        for key, raw in value.items():
            try:
                out[str(key)] = float(raw)
            except (TypeError, ValueError):
                continue
        return out
    return {}


def _is_bucket_distribution(pmf: dict[str, float]) -> bool:
    if not pmf:
        return False
    return any(_is_bucket_label(label) for label in pmf.keys())


def _is_bucket_label(label: str) -> bool:
    bucket = parse_bucket_label(label)
    return bucket.lower is not None or bucket.upper is not None


def _extract_yes_probability(pmf: dict[str, float]) -> float | None:
    if not pmf:
        return None
    yes_keys = {"yes", "y", "true", "1"}
    no_keys = {"no", "n", "false", "0"}
    for key, value in pmf.items():
        if str(key).strip().lower() in yes_keys:
            return _clip01(value)
    for key, value in pmf.items():
        if str(key).strip().lower() in no_keys:
            return _clip01(1.0 - float(value))
    return None


def _normalize_pmf(raw: dict[str, float]) -> dict[str, float]:
    cleaned = {str(k): max(0.0, float(v)) for k, v in raw.items() if str(k).strip()}
    total = sum(cleaned.values())
    if total <= 0:
        return {}
    return {k: v / total for k, v in cleaned.items()}


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))

