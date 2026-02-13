from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class M4SemanticSignal:
    regime_probs: dict[str, float]
    delta_progress: float
    delta_uncertainty: float
    mean_shift_hint: float
    confidence: float
    evidence_span: tuple[str, ...] = ()
    reason_codes: tuple[str, ...] = ()
    parse_ok: bool = False
    llm_model: str = ""
    prompt_version: str = ""
    error_code: str | None = None
    raw_text: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class M4SemanticDiagnostics:
    applied: bool
    confidence_used: float
    mean_shift_applied: float
    uncertainty_applied: float
    metadata: dict[str, Any] = field(default_factory=dict)

