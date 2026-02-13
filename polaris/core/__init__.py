from __future__ import annotations

from importlib import import_module

_MODULE4_EXPORTS = {
    "CollapseFilter",
    "CollapseFilterConfig",
    "Conductor",
    "ConductorConfig",
    "M4Decision",
    "M4Evidence",
    "M4Health",
    "M4Posterior",
    "M4RunContext",
    "M4ScoreResult",
    "M4Scorer",
    "MarketPriorBuilder",
    "Module4RunSummary",
    "Module4Service",
    "PriorBuildResult",
    "PriorBuilderConfig",
    "RegimeAdjustment",
    "RegimeEngine",
    "RuleCompiler",
    "RulePolicy",
    "RuleVersion",
    "compute_progress",
    "SemanticAdjuster",
    "SemanticAdjusterConfig",
    "M4SemanticSignal",
    "M4SemanticDiagnostics",
}

__all__ = sorted(_MODULE4_EXPORTS)


def __getattr__(name: str):
    if name in _MODULE4_EXPORTS:
        module4 = import_module("polaris.core.module4")
        return getattr(module4, name)
    raise AttributeError(f"module 'polaris.core' has no attribute '{name}'")

