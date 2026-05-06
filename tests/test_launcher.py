from __future__ import annotations

from pathlib import Path

from rag_catalog.cli import launcher


def test_shared_runtime_dir_uses_telemetry_parent(tmp_path: Path) -> None:
    cfg = {
        "telemetry_db_path": str(tmp_path / "shared" / "rag_telemetry.db"),
        "qdrant_db_path": str(tmp_path / "qdrant"),
    }

    runtime_dir = launcher._shared_runtime_dir(cfg)

    assert runtime_dir == (tmp_path / "shared" / ".launcher_runtime")


def test_start_bot_detects_existing_process(monkeypatch, tmp_path: Path) -> None:
    cfg = {
        "telegram_enabled": True,
        "telegram_bot_token": "token",
        "qdrant_url": "http://localhost:6333",
        "telemetry_db_path": str(tmp_path / "shared" / "rag_telemetry.db"),
        "qdrant_db_path": str(tmp_path / "qdrant"),
    }
    monkeypatch.setattr(launcher, "load_config", lambda: cfg)
    monkeypatch.setattr(launcher, "_find_python_module_pid", lambda module: 4242)
    monkeypatch.setattr(launcher, "_spawn_python_module", lambda *args, **kwargs: 0)

    result = launcher._start_bot("auto")

    assert "already-up" in result
    assert "4242" in result
    assert launcher._read_pid_payload(launcher._pid_file(cfg, "bot"))["pid"] == 4242
