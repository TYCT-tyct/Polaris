from __future__ import annotations

from polaris.arb.config import arb_config_from_settings
from polaris.config import PolarisSettings


def test_arb_config_rust_mode_is_normalized_to_lowercase() -> None:
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_rust_bridge_mode="SubProcess",
    )
    cfg = arb_config_from_settings(settings)
    assert cfg.rust_bridge_mode == "subprocess"


def test_arb_config_rust_mode_defaults_to_daemon() -> None:
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
    )
    cfg = arb_config_from_settings(settings)
    assert cfg.rust_bridge_mode == "daemon"


def test_arb_config_rust_mode_supports_pyo3() -> None:
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_rust_bridge_mode="PyO3",
    )
    cfg = arb_config_from_settings(settings)
    assert cfg.rust_bridge_mode == "pyo3"
