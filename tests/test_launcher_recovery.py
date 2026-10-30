from __future__ import annotations

import json
import socket
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest
from filelock import Timeout

from rag_catalog.cli import launcher
from rag_catalog.cli import launcher_recovery as recovery


@pytest.fixture
def cfg(monkeypatch, tmp_path):
    config = {"telemetry_db_path": str(tmp_path / "telemetry.db"),
              "qdrant_url": "http://127.0.0.1:6333", "telegram_enabled": True,
              "telegram_bot_token": "test-secret", "cloud_drive_storage": "s3",
              "cloud_drive_s3_endpoint": "http://localhost:9000"}
    monkeypatch.setattr(launcher, "load_config", lambda: config)
    monkeypatch.setattr(launcher, "RUNTIME_DIR", tmp_path / "logs")
    return config


def test_maintenance_expires_after_reboot(cfg, monkeypatch):
    boot = [100.0]
    monkeypatch.setattr(recovery.psutil, "boot_time", lambda: boot[0])
    assert not recovery.maintenance(cfg)
    assert recovery.maintenance(cfg, enabled=True)
    assert recovery.maintenance(cfg)
    boot[0] = 200.0
    assert not recovery.maintenance(cfg)
    recovery.maintenance(cfg, enabled=True)
    assert not recovery.maintenance(cfg, enabled=False)


def test_invalid_maintenance_fails_closed(cfg):
    path = launcher._shared_runtime_dir(cfg) / "maintenance.json"
    path.write_text("broken", encoding="utf-8")
    assert recovery.maintenance(cfg)


def test_control_lock_is_exclusive_and_released(cfg):
    with recovery.control_lock(cfg):
        with pytest.raises(Timeout):
            with recovery.control_lock(cfg):
                pass
    with recovery.control_lock(cfg):
        pass


def test_cli_stop_and_start_coordinate_with_watchdog(cfg, monkeypatch):
    events = []
    monkeypatch.setattr(launcher, "_stop", lambda args: events.append(recovery.maintenance(cfg)) or 0)
    monkeypatch.setattr(launcher, "_start", lambda args: 0)
    assert launcher.main(["stop"]) == 0
    assert events == [True]
    assert recovery.maintenance(cfg)
    assert launcher.main(["start"]) == 0
    assert not recovery.maintenance(cfg)


def test_failed_manual_start_keeps_maintenance(cfg, monkeypatch):
    recovery.maintenance(cfg, enabled=True)
    monkeypatch.setattr(launcher, "_start", lambda args: 1)
    assert launcher.main(["start"]) == 1
    assert recovery.maintenance(cfg)


def test_busy_watchdog_never_touches_services(cfg, monkeypatch):
    monkeypatch.setattr(recovery, "recover", lambda args: pytest.fail("must not run"))
    with recovery.control_lock(cfg):
        assert launcher.main(["recover"]) == 0
        assert launcher.main(["start"]) == 1


def test_running_loading_container_is_not_restarted(monkeypatch):
    calls = []
    def run(command, timeout=15):
        calls.append(command)
        return SimpleNamespace(returncode=0, stdout="abc123\n" if "ps" in command else "running\n")
    monkeypatch.setattr(recovery, "_run", run)
    assert recovery._ensure_container("qdrant", {}, 1000) == "running"
    assert all("up" not in command and "restart" not in command for command in calls)


@pytest.mark.parametrize("status", ["paused", "restarting", "removing"])
def test_container_control_states_are_respected(monkeypatch, status):
    monkeypatch.setattr(recovery, "_run", lambda command, timeout=15: SimpleNamespace(
        returncode=0, stdout="abc123\n" if "ps" in command else status))
    assert recovery._ensure_container("qdrant", {}, 1000) == "waiting-container"


def test_exited_container_uses_no_recreate_and_backoff(monkeypatch):
    calls = []
    def run(command, timeout=15):
        calls.append(command)
        return SimpleNamespace(returncode=0, stdout="abc123" if "ps" in command else "exited")
    monkeypatch.setattr(recovery, "_run", run)
    state = {}
    assert recovery._ensure_container("minio", state, 1000) == "start-requested"
    assert calls[-1] == ["docker", "compose", "up", "-d", "--no-recreate", "--no-build", "--no-deps", "minio"]
    assert recovery._ensure_container("minio", state, 1060) == "retry-backoff"
    assert sum("up" in command for command in calls) == 1


def test_healthy_dependencies_do_not_invoke_docker(cfg, monkeypatch):
    monkeypatch.setattr(recovery, "_ready", lambda url: True)
    monkeypatch.setattr(recovery, "_run", lambda *args: pytest.fail("unnecessary docker command"))
    assert recovery._recover_dependencies(cfg, {}, 1000) == {"qdrant": "ready", "storage": "ready"}


def test_docker_not_ready_requests_existing_task_only_once(cfg, monkeypatch):
    starts = []
    monkeypatch.setattr(recovery, "_ready", lambda url: False)
    monkeypatch.setattr(recovery, "_run", lambda *args: SimpleNamespace(returncode=1))
    monkeypatch.setattr(recovery, "_task_start", lambda name: starts.append(name) or "start-requested")
    state = {}
    assert recovery._recover_dependencies(cfg, state, 1000)["docker"] == "start-requested"
    assert recovery._recover_dependencies(cfg, state, 1060)["docker"] == "not-ready"
    assert starts == ["RAG Docker Desktop"]


def test_qdrant_recovery_is_pending_not_healthy(cfg, monkeypatch):
    monkeypatch.setattr(recovery, "_ready", lambda url: "minio" in url)
    monkeypatch.setattr(recovery, "_run", lambda *args: SimpleNamespace(returncode=0))
    monkeypatch.setattr(recovery, "_ensure_container", lambda *args: "running")
    assert recovery._recover_dependencies(cfg, {}, 1000) == {"qdrant": "not-ready:running", "storage": "ready"}


def test_external_dependencies_never_start_local_containers(cfg, monkeypatch):
    cfg.update(qdrant_url="https://qdrant.example", cloud_drive_s3_endpoint="https://s3.example")
    monkeypatch.setattr(recovery, "_ready", lambda url: False)
    monkeypatch.setattr(recovery, "_run", lambda *args: pytest.fail("must not run docker"))
    assert recovery._recover_dependencies(cfg, {}, 1000) == {"qdrant": "not-ready"}


def test_existing_services_are_adopted_without_native_imports(cfg, monkeypatch):
    monkeypatch.setattr(recovery, "_service_pid", lambda *args: 42)
    monkeypatch.setattr(recovery, "_ready", lambda url: True)
    monkeypatch.setattr(recovery, "_runtime_ready", lambda: pytest.fail("unnecessary native imports"))
    assert recovery._recover_services(cfg, {}, 1000, "127.0.0.1", 8080) == {"web": "ready", "bot": "process-running"}


def test_live_unready_service_is_not_killed(cfg, monkeypatch):
    monkeypatch.setattr(recovery, "_service_pid", lambda *args: 42)
    monkeypatch.setattr(recovery, "_ready", lambda url: False)
    monkeypatch.setattr(recovery, "_runtime_ready", lambda: pytest.fail("must not restart live process"))
    assert recovery._recover_services(cfg, {}, 1000, "127.0.0.1", 8080)["web"] == "not-ready"


def test_missing_bot_recovers_without_restarting_web(cfg, monkeypatch):
    started = []
    monkeypatch.setattr(recovery, "_service_pid", lambda cfg, kind, module: 42 if kind == "web" or started else 0)
    monkeypatch.setattr(recovery, "_ready", lambda url: True)
    monkeypatch.setattr(recovery, "_runtime_ready", lambda: True)
    monkeypatch.setattr(launcher, "_start_bot", lambda mode: started.append(mode))
    monkeypatch.setattr(launcher, "_start_web", lambda *args: pytest.fail("must preserve web"))
    assert recovery._recover_services(cfg, {}, 1000, "127.0.0.1", 8080)["bot"] == "process-running"
    assert started == ["auto"]


def test_missing_web_is_started_and_failure_backs_off(cfg, monkeypatch):
    started = []
    monkeypatch.setattr(recovery, "_service_pid", lambda cfg, kind, module: 42 if kind == "bot" else 0)
    monkeypatch.setattr(recovery, "_ready", lambda url: False)
    monkeypatch.setattr(launcher, "_port_open", lambda *args: False)
    monkeypatch.setattr(recovery, "_runtime_ready", lambda: True)
    monkeypatch.setattr(launcher, "_start_web", lambda *args: started.append("web"))
    state = {}
    assert recovery._recover_services(cfg, state, 1000, "127.0.0.1", 8080)["web"] == "start-pending"
    recovery._recover_services(cfg, state, 1060, "127.0.0.1", 8080)
    assert started == ["web"]


def test_foreign_listener_is_not_replaced(cfg, monkeypatch):
    monkeypatch.setattr(recovery, "_service_pid", lambda cfg, kind, module: 42 if kind == "bot" else 0)
    monkeypatch.setattr(recovery, "_ready", lambda url: False)
    monkeypatch.setattr(launcher, "_port_open", lambda *args: True)
    monkeypatch.setattr(recovery, "_runtime_ready", lambda: pytest.fail("must not start"))
    assert recovery._recover_services(cfg, {}, 1000, "127.0.0.1", 8080)["web"] == "not-ready"


def test_optional_import_hang_is_bounded(monkeypatch):
    def run(command, timeout):
        assert timeout == 60
        raise subprocess.TimeoutExpired(command, timeout)
    monkeypatch.setattr(recovery, "_run", run)
    assert not recovery._runtime_ready()


@pytest.mark.parametrize("task_state,expected,runs", [("Running", "already-running", 1), ("Ready", "start-requested", 2)])
def test_task_recovery_does_not_relaunch_running_proxy(monkeypatch, task_state, expected, runs):
    calls = []
    monkeypatch.setattr(recovery, "os", SimpleNamespace(name="nt"))
    def run(command, timeout=15):
        calls.append(command)
        return SimpleNamespace(returncode=0, stdout=task_state)
    monkeypatch.setattr(recovery, "_run", run)
    assert recovery._task_start("RAG Catalog HTTPS Proxy") == expected
    assert len(calls) == runs


def test_unavailable_native_runtime_prevents_service_spawn(cfg, monkeypatch):
    monkeypatch.setattr(recovery, "_service_pid", lambda *args: 0)
    monkeypatch.setattr(recovery, "_ready", lambda url: False)
    monkeypatch.setattr(launcher, "_port_open", lambda *args: False)
    monkeypatch.setattr(recovery, "_runtime_ready", lambda: False)
    monkeypatch.setattr(launcher, "_start_web", lambda *args: pytest.fail("must not spawn"))
    monkeypatch.setattr(launcher, "_start_bot", lambda *args: pytest.fail("must not spawn"))
    assert recovery._recover_services(cfg, {}, 1000, "127.0.0.1", 8080)["search_runtime"] == "not-ready"


def test_recover_maintenance_has_no_side_effects(cfg, monkeypatch):
    recovery.maintenance(cfg, enabled=True)
    monkeypatch.setattr(recovery, "_recover_dependencies", lambda *args: pytest.fail("must preserve maintenance"))
    monkeypatch.setattr(recovery, "_task_start", lambda *args: pytest.fail("must preserve maintenance"))
    assert launcher.main(["recover", "--proxy-task", "test"]) == 0
    state = json.loads((launcher._shared_runtime_dir(cfg) / "recovery.json").read_text())
    assert state["checked_at"] > 0
    assert state["status"] == {"maintenance": "operator-stop-until-start-or-reboot"}


def test_pending_dependencies_return_nonzero_and_log_no_secret(cfg, monkeypatch):
    monkeypatch.setattr(recovery, "_recover_dependencies", lambda *args: {"qdrant": "not-ready:running"})
    monkeypatch.setattr(recovery, "_recover_services", lambda *args: {"web": "ready", "bot": "process-running"})
    assert launcher.main(["recover"]) == 1
    log = (launcher.RUNTIME_DIR / "launcher-recovery.log").read_text()
    assert "not-ready:running" in log
    assert "test-secret" not in log


def test_recovery_errors_do_not_leak_dependency_secrets(cfg, monkeypatch):
    def fail(*args):
        raise RuntimeError("password=test-secret")
    monkeypatch.setattr(recovery, "_recover_dependencies", fail)
    assert launcher.main(["recover"]) == 1
    log = (launcher.RUNTIME_DIR / "launcher-recovery.log").read_text()
    assert "RuntimeError" in log
    assert "test-secret" not in log


def test_native_process_crash_recovers_without_duplicates(cfg, monkeypatch):
    """Use an isolated loopback server, never the production port or services."""
    cfg["telegram_enabled"] = False
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    children = []
    code = """
import http.server, sys
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"ok":true,"service":"rag-catalog"}')
    def log_message(self, *args):
        pass
http.server.HTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
"""
    def start(*args):
        process = subprocess.Popen([sys.executable, "-c", code, str(port)],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                   creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        children.append(process)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if recovery._ready(f"http://127.0.0.1:{port}/api/ping"):
                return
            time.sleep(0.05)
        pytest.fail("Isolated test server did not start")
    def pid(*args):
        return children[-1].pid if children and children[-1].poll() is None else 0
    monkeypatch.setattr(recovery, "_runtime_ready", lambda: True)
    monkeypatch.setattr(recovery, "_service_pid", pid)
    monkeypatch.setattr(launcher, "_start_web", start)
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("NO_PROXY", "")
    state = {}
    try:
        assert recovery._recover_services(cfg, state, 1000, "127.0.0.1", port)["web"] == "ready"
        first = children[-1]
        assert recovery._recover_services(cfg, state, 1060, "127.0.0.1", port)["web"] == "ready"
        assert len(children) == 1
        first.kill()
        first.wait(timeout=10)
        assert recovery._recover_services(cfg, state, 1121, "127.0.0.1", port)["web"] == "ready"
        assert len(children) == 2
        assert children[-1].pid != first.pid
    finally:
        for process in children:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=10)


@pytest.mark.parametrize("payload,expected", [
    (b'{"ok":true,"service":"rag-catalog"}', True),
    (b'{"ok":true,"service":"other"}', False), (b'[]', False), (b'<html>login</html>', False),
])
def test_health_probe_checks_service_identity(monkeypatch, payload, expected):
    class Response:
        status = 200
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def read(self, count):
            assert count == 4096
            return payload
    monkeypatch.setattr(recovery.urllib.request, "build_opener", lambda *args: SimpleNamespace(open=lambda *args, **kwargs: Response()))
    assert recovery._ready("http://127.0.0.1:8080/api/ping") is expected
