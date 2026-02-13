from __future__ import annotations

from polaris.core.module4.service import _provider_timeout_floor


def test_provider_timeout_floor_minimax() -> None:
    assert _provider_timeout_floor("minimax", 3.0) == 8.0
    assert _provider_timeout_floor("minimax", 12.0) == 12.0


def test_provider_timeout_floor_openai() -> None:
    assert _provider_timeout_floor("openai", 0.1) == 0.2
    assert _provider_timeout_floor("openai", 2.5) == 2.5
