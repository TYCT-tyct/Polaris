from __future__ import annotations

import importlib
import json

from polaris.arb.execution import rust_bridge


def test_run_pyo3_returns_none_when_module_not_found(monkeypatch) -> None:
    def _raise(_: str) -> object:
        raise ImportError("missing")

    monkeypatch.setattr(rust_bridge, "_PYO3_MODULE", None)
    monkeypatch.setattr(importlib, "import_module", _raise)
    assert rust_bridge._run_pyo3({"x": 1}) is None


def test_run_pyo3_parses_json_payload(monkeypatch) -> None:
    class DummyModule:
        @staticmethod
        def simulate_paper(_: str) -> str:
            return json.dumps({"fills": [], "events": [], "capital_used_usd": 1.25})

    monkeypatch.setattr(rust_bridge, "_PYO3_MODULE", DummyModule())
    data = rust_bridge._run_pyo3({"x": 1})
    assert isinstance(data, dict)
    assert float(data["capital_used_usd"]) == 1.25

