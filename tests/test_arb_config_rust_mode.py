from __future__ import annotations

from polaris.arb.config import arb_config_from_settings
from polaris.config import PolarisSettings


def test_arb_config_rust_mode_is_normalized_to_lowercase() -> None:
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_execution_backend="",
        arb_rust_bridge_enabled=True,
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
        arb_execution_backend="",
        arb_rust_bridge_enabled=True,
        arb_rust_bridge_mode="PyO3",
    )
    cfg = arb_config_from_settings(settings)
    assert cfg.rust_bridge_mode == "pyo3"


def test_arb_config_execution_backend_aliases() -> None:
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_execution_backend="rust-pyo3",
    )
    cfg = arb_config_from_settings(settings)
    assert cfg.execution_backend == "rust_pyo3"
    assert cfg.rust_bridge_enabled is True
    assert cfg.rust_bridge_mode == "pyo3"


def test_arb_config_execution_backend_compat_with_old_switch() -> None:
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_rust_bridge_enabled=True,
        arb_rust_bridge_mode="subprocess",
        arb_execution_backend="",
    )
    cfg = arb_config_from_settings(settings)
    assert cfg.execution_backend == "rust_subprocess"
    assert cfg.rust_bridge_enabled is True
    assert cfg.rust_bridge_mode == "subprocess"


def test_arb_config_custom_run_tag_is_sanitized() -> None:
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_run_tag="V2 Night#01",
    )
    cfg = arb_config_from_settings(settings)
    assert cfg.run_tag == "v2-night-01"


def test_arb_config_auto_run_tag_is_generated() -> None:
    settings = PolarisSettings(
        database_url="postgresql://postgres:postgres@localhost:55432/polaris",
        arb_run_tag="auto",
    )
    cfg = arb_config_from_settings(settings)
    assert cfg.run_tag.startswith("auto-")
    assert len(cfg.run_tag) <= 64
