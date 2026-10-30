"""Bounded, idempotent recovery pass for Windows Task Scheduler."""

from __future__ import annotations

import contextlib
import io
import json
import logging
import os
import subprocess
import time
import urllib.request
from logging.handlers import RotatingFileHandler
from urllib.parse import urlparse

import psutil
from filelock import FileLock

from rag_catalog.cli import launcher


def control_lock(cfg: dict) -> FileLock:
    return FileLock(str(launcher._shared_runtime_dir(cfg) / "control.lock"), timeout=0)


def maintenance(cfg: dict, *, enabled: bool | None = None) -> bool:
    path = launcher._shared_runtime_dir(cfg) / "maintenance.json"
    if enabled is False:
        path.unlink(missing_ok=True)
    elif enabled is True:
        path.write_text(json.dumps({"boot": psutil.boot_time()}), encoding="utf-8")
    if not path.exists():
        return False
    try:
        return abs(float(json.loads(path.read_text(encoding="utf-8"))["boot"]) - psutil.boot_time()) < 2
    except (OSError, ValueError, KeyError, TypeError):
        # Do not override an operator's stop if its marker cannot be read.
        return True


def _run(command: list[str], timeout: int = 15) -> subprocess.CompletedProcess:
    return subprocess.run(
        command, cwd=str(launcher.PROJECT_ROOT), capture_output=True, text=True,
        encoding="utf-8", errors="replace", timeout=timeout,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )


def _local_url(url: str) -> bool:
    return urlparse(url).hostname in {"127.0.0.1", "localhost", "::1"}


def _ready(url: str) -> bool:
    try:
        # A desktop VPN/proxy must not affect loopback health checks.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({})) if _local_url(url) else urllib.request.build_opener()
        with opener.open(url, timeout=3) as response:
            if urlparse(url).path == "/api/ping":
                payload = json.loads(response.read(4096))
                return response.status == 200 and payload.get("ok") is True and payload.get("service") == "rag-catalog"
            return 200 <= response.status < 400
    except (OSError, ValueError, AttributeError):
        return False


def _due(state: dict, key: str, now: float, interval: int = 60) -> bool:
    attempts = state.setdefault("attempts", {})
    if now - float(attempts.get(key, 0)) < interval:
        return False
    attempts[key] = now
    return True


def _task_start(name: str) -> str:
    if os.name != "nt":
        return "not-windows"
    escaped = name.replace("'", "''")
    query = _run(["powershell.exe", "-NoProfile", "-NonInteractive", "-Command",
                  f"$ErrorActionPreference='Stop'; (Get-ScheduledTask -TaskName '{escaped}').State.ToString()"])
    if query.returncode:
        return "task-probe-failed"
    if query.stdout.strip() == "Running":
        return "already-running"
    result = _run(["schtasks.exe", "/Run", "/TN", name])
    return "start-requested" if result.returncode == 0 else "task-start-failed"


def _ensure_container(service: str, state: dict, now: float) -> str:
    result = _run(["docker", "compose", "ps", "--all", "--quiet", service])
    if result.returncode:
        return "inspect-failed"
    ids = result.stdout.split()
    if ids:
        result = _run(["docker", "inspect", "--format", "{{.State.Status}}", *ids])
        if result.returncode:
            return "inspect-failed"
        statuses = result.stdout.split()
        if statuses and all(value == "running" for value in statuses):
            return "running"
        if any(value in {"paused", "restarting", "removing"} for value in statuses):
            return "waiting-container"
    if not _due(state, service, now, 120):
        return "retry-backoff"
    # Never recreate a running container or run initialization/indexing services.
    result = _run(["docker", "compose", "up", "-d", "--no-recreate", "--no-build", "--no-deps", service], 90)
    return "start-requested" if result.returncode == 0 else "start-failed"


def _recover_dependencies(cfg: dict, state: dict, now: float) -> dict:
    targets = {}
    qdrant = launcher._qdrant_target(cfg)
    if qdrant["mode"] == "server":
        targets["qdrant"] = (str(qdrant["url"]).rstrip("/") + "/readyz", bool(qdrant["is_local_host"]))
    storage = str(cfg.get("cloud_drive_s3_endpoint") or "").rstrip("/")
    if cfg.get("cloud_drive_storage") == "s3" and storage and _local_url(storage):
        targets["storage"] = (storage + "/minio/health/ready", _local_url(storage))
    states = {key: "ready" if _ready(url) else "not-ready" for key, (url, _) in targets.items()}
    missing_local = [key for key, (_, local) in targets.items() if local and states[key] != "ready"]
    if not missing_local:
        return states
    try:
        docker = _run(["docker", "info", "--format", "{{.ServerVersion}}"], 10)
        if docker.returncode:
            states["docker"] = "not-ready"
            if _due(state, "docker", now, 300):
                states["docker"] = _task_start("RAG Docker Desktop")
            return states
        for key in missing_local:
            service = "minio" if key == "storage" else key
            states[key] = _ensure_container(service, state, now)
            # Running is not readiness: Qdrant can take minutes to recover its WAL.
            states[key] = "ready" if _ready(targets[key][0]) else "not-ready:" + states[key]
    except (OSError, subprocess.SubprocessError):
        states["docker"] = "probe-failed"
        if _due(state, "docker", now, 300):
            try:
                states["docker"] = _task_start("RAG Docker Desktop")
            except (OSError, subprocess.SubprocessError):
                states["docker"] = "task-start-failed"
    return states


def _service_pid(cfg: dict, kind: str, module: str) -> int:
    payload = launcher._read_pid_payload(launcher._pid_file(cfg, kind))
    pid = int(payload.get("pid") or 0)
    return pid if launcher._pid_runs_module(pid, module) else launcher._find_python_module_pid(module)


def _runtime_ready() -> bool:
    # Isolate optional native imports: a driver/import hang must not wedge the watchdog.
    code = "from rag_catalog.cli.launcher import load_config, _search_runtime_ready; raise SystemExit(0 if _search_runtime_ready(load_config()) else 1)"
    try:
        return _run([launcher.sys.executable, "-c", code], 60).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def _recover_services(cfg: dict, state: dict, now: float, host: str, port: int) -> dict:
    web_pid = _service_pid(cfg, "web", launcher.WEB_MODULE)
    bot_required = bool(cfg.get("telegram_enabled") and cfg.get("telegram_bot_token") and cfg.get("qdrant_url"))
    bot_pid = _service_pid(cfg, "bot", launcher.BOT_MODULE) if bot_required else 0
    host_url = f"http://{'[' + host + ']' if ':' in host else host}:{port}/api/ping"
    web_ready = _ready(host_url)
    result = {"web": "ready" if web_ready else "not-ready", "bot": "process-running" if bot_pid else "not-running"}
    if not bot_required:
        result["bot"] = "disabled-or-embedded"
    missing = []
    if not web_pid and not web_ready and not launcher._port_open(host, port):
        missing.append("web")
    if bot_required and not bot_pid:
        missing.append("bot")
    due = [key for key in missing if _due(state, key, now, 120)]
    if not due:
        return result
    with contextlib.redirect_stdout(io.StringIO()):
        if not _runtime_ready():
            result["search_runtime"] = "not-ready"
            return result
        for key in due:
            if key == "web":
                launcher._start_web(cfg, host, port)
                result[key] = "ready" if _ready(host_url) else "start-pending"
            else:
                launcher._start_bot("auto")
                result[key] = "process-running" if _service_pid(cfg, "bot", launcher.BOT_MODULE) else "start-failed"
    return result


def recover(args) -> int:
    """One pass only; Task Scheduler provides repetition and process isolation."""
    cfg = launcher.load_config()
    directory = launcher._shared_runtime_dir(cfg)
    path = directory / "recovery.json"
    state = launcher._read_pid_payload(path)
    now = time.time()
    if state.get("boot") != psutil.boot_time():
        state = {"boot": psutil.boot_time()}
    logger = logging.getLogger("rag.launcher.recovery")
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(launcher._runtime_dir() / "launcher-recovery.log", maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    try:
        if maintenance(cfg):
            status = {"maintenance": "operator-stop-until-start-or-reboot"}
        else:
            status = _recover_dependencies(cfg, state, now)
            status.update(_recover_services(cfg, state, now, args.host, args.port))
            if args.proxy_task:
                # IgnoreNew in the registered task prevents a second Caddy instance.
                status["proxy"] = _task_start(args.proxy_task)
        previous = state.get("status")
        state.update(checked_at=now, status=status)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(state, indent=2), encoding="utf-8")
        temporary.replace(path)
        if status != previous:
            logger.info("Recovery state: %s", json.dumps(status, sort_keys=True))
        print(json.dumps(status, sort_keys=True))
        return 0 if all(value in {"ready", "process-running", "disabled-or-embedded", "start-requested", "already-running", "operator-stop-until-start-or-reboot"} for value in status.values()) else 1
    except Exception as exc:
        # Exceptions may contain credentials from dependencies; log the class only.
        logger.error("Recovery pass failed: %s", type(exc).__name__)
        return 1
    finally:
        logger.removeHandler(handler)
        handler.close()
