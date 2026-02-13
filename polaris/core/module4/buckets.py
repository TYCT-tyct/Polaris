from __future__ import annotations

import math
import re
from collections.abc import Iterable

from .types import BucketRange

_RANGE = re.compile(r"^\s*(\d+)\s*[-~]\s*(\d+)\s*$")
_LT = re.compile(r"^\s*(?:<|<=)\s*(\d+)\s*$")
_GT = re.compile(r"^\s*(\d+)\s*(?:\+|or more|以上)\s*$", re.IGNORECASE)
_NUM = re.compile(r"^\s*(\d+)\s*$")
_SLUG_RANGE_SUFFIX = re.compile(r"^(?P<family>.+)-(?P<low>\d+)-(?P<high>\d+)$")
_SLUG_PLUS_SUFFIX = re.compile(r"^(?P<family>.+)-(?P<low>\d+)(?:\+|plus)$", re.IGNORECASE)
_SLUG_PLUS_SUFFIX_ALT = re.compile(r"^(?P<family>.+)-(?P<low>\d+)-plus$", re.IGNORECASE)


def parse_bucket_label(label: str) -> BucketRange:
    raw = (label or "").strip()
    lower_text = raw.lower()
    m = _RANGE.match(lower_text)
    if m:
        low = int(m.group(1))
        high = int(m.group(2))
        if high < low:
            low, high = high, low
        return BucketRange(label=raw, lower=low, upper=high)
    m = _LT.match(lower_text)
    if m:
        upper = max(0, int(m.group(1)) - 1)
        return BucketRange(label=raw, lower=0, upper=upper)
    m = _GT.match(lower_text)
    if m:
        return BucketRange(label=raw, lower=int(m.group(1)), upper=None)
    m = _NUM.match(lower_text)
    if m:
        v = int(m.group(1))
        return BucketRange(label=raw, lower=v, upper=v)
    return BucketRange(label=raw, lower=None, upper=None)


def representative_count(bucket: BucketRange, *, tail_default: int = 10) -> float:
    if bucket.lower is not None and bucket.upper is not None:
        return (bucket.lower + bucket.upper) / 2.0
    if bucket.lower is None and bucket.upper is not None:
        return max(0.0, float(bucket.upper - tail_default))
    if bucket.lower is not None and bucket.upper is None:
        return float(bucket.lower + tail_default)
    return 0.0


def infer_true_bucket_label(final_count: int, labels: Iterable[str]) -> str | None:
    ordered = sorted((parse_bucket_label(x) for x in labels), key=_bucket_order_key)
    for item in ordered:
        if item.lower is None and item.upper is None:
            continue
        low = item.lower if item.lower is not None else 0
        high = item.upper if item.upper is not None else math.inf
        if low <= final_count <= high:
            return item.label
    return None


def _bucket_order_key(item: BucketRange) -> tuple[int, int]:
    low = item.lower if item.lower is not None else -1
    up = item.upper if item.upper is not None else 10**9
    return low, up


def split_count_bucket_from_slug(slug: str) -> tuple[str | None, str | None]:
    text = (slug or "").strip().lower()
    if not text:
        return None, None
    m = _SLUG_RANGE_SUFFIX.match(text)
    if m:
        low = int(m.group("low"))
        high = int(m.group("high"))
        if high < low:
            low, high = high, low
        return m.group("family"), f"{low}-{high}"
    m = _SLUG_PLUS_SUFFIX.match(text) or _SLUG_PLUS_SUFFIX_ALT.match(text)
    if m:
        low = int(m.group("low"))
        return m.group("family"), f"{low}+"
    return None, None
