from __future__ import annotations

import json
import zipfile
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


def test_start_bot_clears_stale_pid_before_spawn(monkeypatch, tmp_path: Path) -> None:
    cfg = {
        "telegram_enabled": True,
        "telegram_bot_token": "token",
        "qdrant_url": "http://localhost:6333",
        "telemetry_db_path": str(tmp_path / "shared" / "rag_telemetry.db"),
        "qdrant_db_path": str(tmp_path / "qdrant"),
    }
    pid_file = launcher._pid_file(cfg, "bot")
    launcher._write_pid(pid_file, 1111, {"module": "rag_catalog.integrations.telegram_bot"})
    monkeypatch.setattr(launcher, "load_config", lambda: cfg)
    monkeypatch.setattr(launcher, "_pid_alive", lambda pid: int(pid) == 2222)
    monkeypatch.setattr(launcher, "_find_python_module_pid", lambda module: 0)
    monkeypatch.setattr(launcher, "_spawn_python_module", lambda *args, **kwargs: 2222)

    result = launcher._start_bot("auto")

    assert "started" in result
    assert "2222" in result
    assert launcher._read_pid_payload(pid_file)["pid"] == 2222


def test_start_bot_reports_recent_log_error_on_failure(monkeypatch, tmp_path: Path) -> None:
    cfg = {
        "telegram_enabled": True,
        "telegram_bot_token": "token",
        "qdrant_url": "http://localhost:6333",
        "telemetry_db_path": str(tmp_path / "shared" / "rag_telemetry.db"),
        "qdrant_db_path": str(tmp_path / "qdrant"),
    }
    monkeypatch.setattr(launcher, "load_config", lambda: cfg)
    monkeypatch.setattr(launcher, "_pid_alive", lambda pid: False)
    monkeypatch.setattr(launcher, "_find_python_module_pid", lambda module: 0)
    monkeypatch.setattr(launcher, "_spawn_python_module", lambda *args, **kwargs: 3333)
    monkeypatch.setattr(launcher, "_last_log_error", lambda log_name: "sqlite3.OperationalError: disk I/O error")

    result = launcher._start_bot("auto")

    assert "failed-to-start" in result
    assert "disk I/O error" in result
    assert not launcher._pid_file(cfg, "bot").exists()


def test_restart_waits_for_web_port_to_close_before_start(monkeypatch) -> None:
    events: list[str] = []

    def fake_stop(args) -> int:
        events.append("stop")
        return 0

    def fake_wait(host: str, port: int) -> bool:
        events.append(f"wait:{host}:{port}")
        return True

    def fake_start(args) -> int:
        events.append("start")
        return 0

    monkeypatch.setattr(launcher, "_stop", fake_stop)
    monkeypatch.setattr(launcher, "_wait_port_closed", fake_wait)
    monkeypatch.setattr(launcher, "_start", fake_start)

    result = launcher._restart(
        type("Args", (), {"host": "127.0.0.1", "port": 8080, "with_qdrant": False})()
    )

    assert result == 0
    assert events == ["stop", "wait:127.0.0.1:8080", "start"]


def test_support_bundle_redacts_config_and_includes_status(monkeypatch, tmp_path: Path) -> None:
    cfg = {
        "telegram_bot_token": "123456:secret",
        "cloud_drive_s3_secret_key": "secret",
        "catalog_path": r"O:\Private Catalog",
        "qdrant_db_path": str(tmp_path / "state"),
    }
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    (runtime_dir / "web.pid").write_text('{"pid": 123}', encoding="utf-8")
    output = tmp_path / "support.zip"
    monkeypatch.setattr(launcher, "load_config", lambda: dict(cfg))
    monkeypatch.setattr(launcher, "_shared_runtime_dir", lambda _cfg: runtime_dir)
    monkeypatch.setattr(launcher, "_status", lambda host, port: print(f"status {host}:{port}"))
    log_tail = "\n".join(
        [
            'browser_event action=page details={"query":"secret query","excerpt":"document text"}',
            'ERROR failed path={"path":"O:\\\\Private Catalog\\\\Customer\\\\contract.pdf"}',
            "https://api.telegram.org/bot123456:secret/getUpdates",
        ]
    )
    monkeypatch.setattr(
        launcher,
        "read_history_tail",
        lambda name, max_chars=20000: log_tail if name == "nice_app.log" else "",
    )

    result = launcher.main(["support-bundle", "--output", str(output), "--host", "127.0.0.1", "--port", "8080"])

    assert result == 0
    with zipfile.ZipFile(output) as zf:
        names = set(zf.namelist())
        assert {"manifest.json", "config.redacted.json", "launcher_status.txt", "runtime/web.pid", "logs/nice_app.tail.log"} <= names
        redacted = json.loads(zf.read("config.redacted.json").decode("utf-8"))
        assert redacted["telegram_bot_token"] == "<redacted>"
        assert redacted["cloud_drive_s3_secret_key"] == "<redacted>"
        assert b"status 127.0.0.1:8080" in zf.read("launcher_status.txt")
        safe_log = zf.read("logs/nice_app.tail.log").decode("utf-8")
        assert "browser_event" not in safe_log
        assert "secret query" not in safe_log
        assert "document text" not in safe_log
        assert "123456:secret" not in safe_log
        assert "contract.pdf" not in safe_log
