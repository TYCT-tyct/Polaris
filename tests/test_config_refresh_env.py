from __future__ import annotations

import os
from pathlib import Path

from polaris.config import refresh_process_env_from_file


def test_refresh_env_preserves_existing_values(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "POLARIS_ARB_ENABLE_STRATEGY_G=true\n"
        "POLARIS_ARB_ENABLE_STRATEGY_F=true\n",
        encoding="utf-8",
    )
    os.environ["POLARIS_ARB_ENABLE_STRATEGY_G"] = "false"
    os.environ.pop("POLARIS_ARB_ENABLE_STRATEGY_F", None)

    changed = refresh_process_env_from_file(env_file, preserve_existing=True)

    assert changed
    assert os.environ["POLARIS_ARB_ENABLE_STRATEGY_G"] == "false"
    assert os.environ["POLARIS_ARB_ENABLE_STRATEGY_F"] == "true"


def test_refresh_env_can_override_when_requested(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("POLARIS_ARB_ENABLE_STRATEGY_G=true\n", encoding="utf-8")
    os.environ["POLARIS_ARB_ENABLE_STRATEGY_G"] = "false"

    changed = refresh_process_env_from_file(env_file, preserve_existing=False)

    assert changed
    assert os.environ["POLARIS_ARB_ENABLE_STRATEGY_G"] == "true"
