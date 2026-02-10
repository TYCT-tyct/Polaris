from __future__ import annotations

import os

from polaris.config import refresh_process_env_from_file


def test_refresh_process_env_from_file(tmp_path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "POLARIS_LOG_LEVEL=DEBUG",
                "POLARIS_QUOTE_TOP_SYNC_INTERVAL=11",
                "UNRELATED_KEY=ignored",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("POLARIS_LOG_LEVEL", "INFO")
    monkeypatch.setenv("POLARIS_QUOTE_TOP_SYNC_INTERVAL", "10")
    monkeypatch.delenv("UNRELATED_KEY", raising=False)

    changed = refresh_process_env_from_file(env_file)
    assert changed is True
    assert os.environ["POLARIS_LOG_LEVEL"] == "DEBUG"
    assert os.environ["POLARIS_QUOTE_TOP_SYNC_INTERVAL"] == "11"
    assert "UNRELATED_KEY" not in os.environ

    changed_again = refresh_process_env_from_file(env_file)
    assert changed_again is False
