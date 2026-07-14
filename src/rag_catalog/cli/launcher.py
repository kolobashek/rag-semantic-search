"""Unified launcher for web UI, Qdrant and Telegram bot."""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import platform
import re
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import psutil

from rag_catalog.core.log_history import last_error_from_history, open_run_log, read_history_tail, redact_sensitive_text

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_DIR = PROJECT_ROOT / "logs" / "runtime"


def load_config() -> Dict[str, Any]:
    """Load config lazily so `rag-launcher --help` works in minimal CI envs."""
    from rag_catalog.core.rag_core import load_config as _load_config

    return _load_config()


def _runtime_dir() -> Path:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    return RUNTIME_DIR


def _shared_runtime_dir(cfg: Dict[str, Any]) -> Path:
    telemetry_path = str(cfg.get("telemetry_db_path") or "").strip()
    if telemetry_path:
        base = Path(telemetry_path).resolve().parent
    else:
        qdrant_db_path = str(cfg.get("qdrant_db_path") or "").strip()
        base = Path(qdrant_db_path).resolve() if qdrant_db_path else PROJECT_ROOT.resolve()
    runtime_dir = base / ".launcher_runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    return runtime_dir


def _pid_file(cfg: Dict[str, Any], kind: str) -> Path:
    names = {
        "web": "web.pid",
        "bot": "telegram_bot.pid",
        "qdrant": "qdrant.managed",
    }
    return _shared_runtime_dir(cfg) / names[kind]


def _pid_alive(pid: int) -> bool:
    if int(pid or 0) <= 0:
        return False
    if os.name == "nt":
        try:
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {int(pid)}", "/FO", "CSV", "/NH"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            line = (result.stdout or "").strip()
            return bool(line) and "No tasks are running" not in line
        except Exception:
            return False
    try:
        os.kill(int(pid), 0)
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _pid_commandline(pid: int) -> str:
    if int(pid or 0) <= 0:
        return ""
    if os.name == "nt":
        try:
            cmd = (
                "$p=Get-CimInstance Win32_Process -Filter 'ProcessId={pid}';"
                "if($p){{$p.CommandLine}}else{{''}}"
            ).format(pid=int(pid))
            result = subprocess.run(
                ["powershell", "-NoProfile", "-Command", cmd],
                capture_output=True,
                text=True,
                timeout=6,
            )
            return str(result.stdout or "").strip()
        except Exception:
            return ""
    return ""


def _find_python_module_pid(module: str) -> int:
    try:
        for proc in psutil.process_iter(["pid", "cmdline"]):
            try:
                cmdline = [str(part) for part in (proc.info.get("cmdline") or [])]
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
            for idx, part in enumerate(cmdline[:-1]):
                if part == "-m" and cmdline[idx + 1] == module:
                    return int(proc.info.get("pid") or 0)
            if any(part.endswith("telegram_bot.py") for part in cmdline) and module.endswith("telegram_bot"):
                return int(proc.info.get("pid") or 0)
    except Exception:
        return 0
    return 0


def _read_pid(pid_path: Path) -> int:
    try:
        return int(pid_path.read_text(encoding="utf-8").strip())
    except Exception:
        return 0


def _write_pid(pid_path: Path, pid: int, meta: Optional[Dict[str, Any]] = None) -> None:
    payload = {"pid": int(pid), "ts": int(time.time())}
    if meta:
        payload.update(meta)
    pid_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_pid_payload(pid_path: Path) -> Dict[str, Any]:
    try:
        return json.loads(pid_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _remove_pid(pid_path: Path) -> None:
    try:
        pid_path.unlink(missing_ok=True)
    except Exception:
        pass


def _stale_pid_note(pid_path: Path, service: str) -> str:
    payload = _read_pid_payload(pid_path)
    pid = int(payload.get("pid") or 0)
    if pid <= 0 or _pid_alive(pid):
        return ""
    _remove_pid(pid_path)
    return f"{service}.note: cleared stale pid {pid}"


def _port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _http_ready(url: str, *, timeout: float = 1.0) -> bool:
    try:
        req = urllib.request.Request(str(url).rstrip("/") + "/collections", method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return 200 <= int(resp.status) < 500
    except (OSError, urllib.error.URLError, urllib.error.HTTPError):
        return False


def _wait_port_closed(host: str, port: int, *, timeout_sec: float = 8.0) -> bool:
    deadline = time.time() + max(0.1, float(timeout_sec))
    while time.time() < deadline:
        if not _port_open(host, port, timeout=0.2):
            return True
        time.sleep(0.2)
    return not _port_open(host, port, timeout=0.2)


def _windows_flags() -> int:
    flags = 0
    for name in ("CREATE_NO_WINDOW", "DETACHED_PROCESS", "CREATE_NEW_PROCESS_GROUP", "CREATE_BREAKAWAY_FROM_JOB"):
        flags |= int(getattr(subprocess, name, 0) or 0)
    return flags


def _log_path(log_name: str) -> Path:
    return RUNTIME_DIR / log_name


def _last_log_error(log_name: str, max_lines: int = 80) -> str:
    return last_error_from_history(log_name, max_lines=max_lines)


def _spawn_python_module(module: str, args: list[str], cwd: Path, log_name: str) -> int:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT / "src")
    env["PYTHONIOENCODING"] = "utf-8"
    env.setdefault("HF_HUB_OFFLINE", "1")
    env.setdefault("TRANSFORMERS_OFFLINE", "1")
    env["RAG_LOG_HISTORY_NAME"] = log_name
    env["RAG_LOG_LABEL"] = module
    _runtime_dir()
    log_fh = open_run_log(log_name, f"start {module} {time.strftime('%Y-%m-%d %H:%M:%S')}")
    proc = subprocess.Popen(
        [sys.executable, "-m", module, *args],
        cwd=str(cwd),
        env=env,
        stdout=log_fh,
        stderr=log_fh,
        creationflags=_windows_flags(),
    )
    log_fh.close()
    return int(proc.pid)


def _kill_pid(pid: int) -> bool:
    if not _pid_alive(pid):
        return False
    if os.name == "nt":
        try:
            result = subprocess.run(
                ["taskkill", "/PID", str(int(pid)), "/T", "/F"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            return result.returncode == 0
        except Exception:
            return False
    try:
        os.kill(pid, signal.SIGTERM)
    except Exception:
        return False
    for _ in range(20):
        if not _pid_alive(pid):
            return True
        time.sleep(0.15)
    try:
        os.kill(pid, signal.SIGKILL)
        return True
    except Exception:
        return False


def _qdrant_target(cfg: Dict[str, Any]) -> Dict[str, Any]:
    url = str(cfg.get("qdrant_url") or "").strip()
    if not url:
        return {"mode": "local"}
    parsed = urlparse(url if "://" in url else f"http://{url}")
    host = parsed.hostname or "localhost"
    port = int(parsed.port or 6333)
    is_local_host = host.lower() in {"localhost", "127.0.0.1", "::1"}
    return {
        "mode": "server",
        "url": url,
        "host": host,
        "port": port,
        "is_local_host": is_local_host,
    }


def _docker_available() -> bool:
    try:
        result = subprocess.run(
            ["docker", "--version"],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=8,
        )
        return result.returncode == 0
    except Exception:
        return False


def _start_qdrant_if_needed(cfg: Dict[str, Any], mode: str) -> str:
    qdrant_flag_file = _pid_file(cfg, "qdrant")
    target = _qdrant_target(cfg)
    if target["mode"] == "local":
        return "qdrant=local-mode (embedded via qdrant_db_path)"
    if mode == "off":
        return f"qdrant=skipped (url={target['url']})"
    if not bool(target.get("is_local_host")):
        return f"qdrant=external ({target['url']})"
    if _port_open(str(target["host"]), int(target["port"]), timeout=1.0):
        return f"qdrant=already-up ({target['host']}:{target['port']})"
    if mode == "auto" and not _docker_available():
        return "qdrant=down (docker is not available)"
    if not _docker_available():
        return "qdrant=failed (docker is not available)"
    compose_cmd = ["docker", "compose", "up", "-d", "qdrant"]
    result = subprocess.run(compose_cmd, cwd=str(PROJECT_ROOT), capture_output=True, text=True, timeout=180)
    if result.returncode != 0:
        return f"qdrant=failed ({result.stderr.strip() or result.stdout.strip()})"
    qdrant_flag_file.write_text("managed\n", encoding="utf-8")
    for _ in range(30):
        if _port_open(str(target["host"]), int(target["port"]), timeout=1.0):
            return f"qdrant=started ({target['host']}:{target['port']})"
        time.sleep(1.0)
    return f"qdrant=started-but-not-ready ({target['host']}:{target['port']})"


def _stop_qdrant_if_managed() -> str:
    cfg = load_config()
    qdrant_flag_file = _pid_file(cfg, "qdrant")
    if not qdrant_flag_file.exists():
        return "qdrant=not-managed"
    if not _docker_available():
        _remove_pid(qdrant_flag_file)
        return "qdrant=managed-flag-cleared (docker unavailable)"
    result = subprocess.run(
        ["docker", "compose", "stop", "qdrant"],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        timeout=60,
    )
    _remove_pid(qdrant_flag_file)
    if result.returncode == 0:
        return "qdrant=stopped"
    return f"qdrant=stop-failed ({result.stderr.strip() or result.stdout.strip()})"


def _start_web(cfg: Dict[str, Any], host: str, port: int) -> str:
    web_pid_file = _pid_file(cfg, "web")
    _stale_pid_note(web_pid_file, "web")
    payload = _read_pid_payload(web_pid_file)
    pid = int(payload.get("pid") or 0)
    if pid and _pid_alive(pid):
        return f"web=already-up (pid={pid}, {host}:{port})"
    if _port_open(host, port, timeout=1.0):
        return f"web=already-up (unmanaged process on {host}:{port})"
    new_pid = _spawn_python_module(
        "rag_catalog.ui.nice_app",
        ["--host", host, "--port", str(port), "--no-show"],
        PROJECT_ROOT,
        "web.log",
    )
    _write_pid(web_pid_file, new_pid, {"host": host, "port": port, "module": "rag_catalog.ui.nice_app"})
    try:
        ready_timeout = float(cfg.get("launcher_web_start_timeout_sec") or 30.0)
    except (TypeError, ValueError):
        ready_timeout = 30.0
    ready_timeout = max(10.0, min(120.0, ready_timeout))
    for _ in range(max(1, int(ready_timeout / 0.25))):
        if _port_open(host, port, timeout=1.0):
            return f"web=started (pid={new_pid}, {host}:{port})"
        time.sleep(0.25)
    return f"web=started-but-not-ready (pid={new_pid})"


def _stop_web() -> str:
    cfg = load_config()
    web_pid_file = _pid_file(cfg, "web")
    payload = _read_pid_payload(web_pid_file)
    pid = int(payload.get("pid") or 0)
    if pid <= 0:
        pid = _find_python_module_pid("rag_catalog.ui.nice_app")
        if pid <= 0:
            return "web=not-managed"
    stopped = _kill_pid(pid)
    _remove_pid(web_pid_file)
    return f"web={'stopped' if stopped else 'already-down'} (pid={pid})"


def _start_bot(enable_mode: str) -> str:
    cfg = load_config()
    bot_pid_file = _pid_file(cfg, "bot")
    _stale_pid_note(bot_pid_file, "bot")
    bot_enabled = bool(cfg.get("telegram_enabled"))
    token_set = bool(str(cfg.get("telegram_bot_token") or "").strip())
    q_mode = _qdrant_target(cfg).get("mode")
    if enable_mode == "off":
        return "bot=skipped"
    if q_mode == "local" and enable_mode != "on":
        return "bot=skipped (local qdrant mode: possible lock conflict with web)"
    if enable_mode == "auto" and (not bot_enabled or not token_set):
        return "bot=skipped (telegram_enabled=false or empty token)"
    payload = _read_pid_payload(bot_pid_file)
    pid = int(payload.get("pid") or 0)
    if pid and _pid_alive(pid):
        return f"bot=already-up (pid={pid})"
    running_pid = _find_python_module_pid("rag_catalog.integrations.telegram_bot")
    if running_pid:
        _write_pid(bot_pid_file, running_pid, {"module": "rag_catalog.integrations.telegram_bot", "discovered": True})
        return f"bot=already-up (pid={running_pid}, discovered)"
    new_pid = _spawn_python_module("rag_catalog.integrations.telegram_bot", [], PROJECT_ROOT, "telegram_bot.log")
    _write_pid(bot_pid_file, new_pid, {"module": "rag_catalog.integrations.telegram_bot"})
    for _ in range(12):
        time.sleep(0.5)
        if _pid_alive(new_pid):
            return f"bot=started (pid={new_pid})"
        discovered_pid = _find_python_module_pid("rag_catalog.integrations.telegram_bot")
        if discovered_pid:
            _write_pid(bot_pid_file, discovered_pid, {"module": "rag_catalog.integrations.telegram_bot", "discovered": True})
            return f"bot=started (pid={discovered_pid}, discovered)"
    _remove_pid(bot_pid_file)
    error = _last_log_error("telegram_bot.log")
    if error:
        return f"bot=failed-to-start ({error})"
    return "bot=failed-to-start"


def _stop_bot() -> str:
    cfg = load_config()
    bot_pid_file = _pid_file(cfg, "bot")
    payload = _read_pid_payload(bot_pid_file)
    pid = int(payload.get("pid") or 0)
    if pid <= 0:
        pid = _find_python_module_pid("rag_catalog.integrations.telegram_bot")
        if pid <= 0:
            return "bot=not-managed"
    stopped = _kill_pid(pid)
    _remove_pid(bot_pid_file)
    return f"bot={'stopped' if stopped else 'already-down'} (pid={pid})"


def _status(host: str, port: int) -> int:
    cfg = load_config()
    target = _qdrant_target(cfg)
    web_payload = _read_pid_payload(_pid_file(cfg, "web"))
    bot_pid_file = _pid_file(cfg, "bot")
    bot_payload = _read_pid_payload(bot_pid_file)
    web_pid = int(web_payload.get("pid") or 0)
    bot_pid = int(bot_payload.get("pid") or 0)
    web_alive = _pid_alive(web_pid)
    bot_alive = _pid_alive(bot_pid)
    notes: list[str] = []
    if web_pid and not web_alive:
        _remove_pid(_pid_file(cfg, "web"))
        notes.append(f"web.note: cleared stale pid {web_pid}")
        web_pid = 0
    if bot_pid and not bot_alive:
        _remove_pid(bot_pid_file)
        notes.append(f"bot.note: cleared stale pid {bot_pid}")
        bot_pid = 0
    bot_discovered = False
    if not bot_alive:
        discovered_pid = _find_python_module_pid("rag_catalog.integrations.telegram_bot")
        if discovered_pid:
            _write_pid(bot_pid_file, discovered_pid, {"module": "rag_catalog.integrations.telegram_bot", "discovered": True})
            bot_pid = discovered_pid
            bot_alive = True
            bot_discovered = True

    print("Launcher status")
    web_port_open = _port_open(host, port)
    print(f"- web.process: {'up' if web_alive else 'down'} (pid={web_pid or '-'})")
    print(f"- web.port: {'open' if web_port_open else 'closed'} ({host}:{port})")
    print(f"- web.managed: {'yes' if web_alive else 'no'}")
    if (not web_alive) and web_port_open:
        print("- web.note: port is open by unmanaged process")
    for note in [n for n in notes if n.startswith("web.")]:
        print(f"- {note}")

    if target["mode"] == "local":
        print(f"- qdrant.mode: local-file ({cfg.get('qdrant_db_path')})")
    else:
        q_host = str(target["host"])
        q_port = int(target["port"])
        q_port_open = _port_open(q_host, q_port)
        q_ready = _http_ready(str(target["url"])) if q_port_open else False
        print(f"- qdrant.mode: server ({target['url']})")
        print(f"- qdrant.port: {'open' if q_port_open else 'closed'} ({q_host}:{q_port})")
        print(f"- qdrant.ready: {'yes' if q_ready else 'no'}")
        print(f"- qdrant.managed: {'yes' if _pid_file(cfg, 'qdrant').exists() else 'no'}")

    print(f"- bot.process: {'up' if bot_alive else 'down'} (pid={bot_pid or '-'})")
    if bot_discovered:
        print("- bot.note: discovered running process from another worktree/runtime")
    for note in [n for n in notes if n.startswith("bot.")]:
        print(f"- {note}")
    if not bot_alive:
        last_error = last_error_from_history("telegram_bot.log", include_fallback=False)
        if last_error:
            print(f"- bot.last_error: {last_error}")
    print(f"- bot.enabled.config: {bool(cfg.get('telegram_enabled'))}")
    print(f"- bot.token.config: {'set' if str(cfg.get('telegram_bot_token') or '').strip() else 'empty'}")
    return 0


def _start(args: argparse.Namespace) -> int:
    _runtime_dir()
    cfg = load_config()
    qdrant_msg = _start_qdrant_if_needed(cfg, args.qdrant)
    web_msg = _start_web(cfg, args.host, int(args.port))
    bot_msg = _start_bot(args.bot)
    print(qdrant_msg)
    print(web_msg)
    print(bot_msg)
    return 0


def _stop(args: argparse.Namespace) -> int:
    _runtime_dir()
    print(_stop_bot())
    print(_stop_web())
    if args.with_qdrant:
        print(_stop_qdrant_if_managed())
    return 0


def _restart(args: argparse.Namespace) -> int:
    _stop(argparse.Namespace(with_qdrant=args.with_qdrant))
    _wait_port_closed(args.host, int(args.port))
    return _start(args)


def _restart_bot(args: argparse.Namespace) -> int:
    """Restart only Telegram without touching web, Qdrant, or index workers."""
    _runtime_dir()
    print(_stop_bot())
    result = _start_bot(args.bot)
    print(result)
    return 1 if "failed-to-start" in result else 0


def _restart_web(args: argparse.Namespace) -> int:
    """Restart only NiceGUI without touching Telegram, Qdrant, or index workers."""
    _runtime_dir()
    cfg = load_config()
    print(_stop_web())
    if not _wait_port_closed(args.host, int(args.port)):
        print(f"web=restart-failed (port still open: {args.host}:{int(args.port)})")
        return 1
    result = _start_web(cfg, args.host, int(args.port))
    print(result)
    return 1 if "not-ready" in result else 0


def _redact_config(value: Any) -> Any:
    if isinstance(value, dict):
        redacted: Dict[str, Any] = {}
        for key, item in value.items():
            lowered = str(key).lower()
            if any(marker in lowered for marker in ("token", "password", "secret", "access_key", "api_key")):
                redacted[str(key)] = "<redacted>" if str(item or "").strip() else ""
            else:
                redacted[str(key)] = _redact_config(item)
        return redacted
    if isinstance(value, list):
        return [_redact_config(item) for item in value]
    return value


def _platform_summary() -> str:
    try:
        return platform.platform()
    except Exception:
        return f"{platform.system()} {platform.release()}".strip()


_SUPPORT_JSON_FIELD_RE = re.compile(
    r'("(?:query|question|excerpt|filename|source_path|catalog_path|path|full_path|cloud_path)"\s*:\s*)"(?:[^"\\]|\\.)*"',
    re.IGNORECASE,
)


def _redact_support_log(cfg: Dict[str, Any], value: str) -> str:
    safe_lines: list[str] = []
    sensitive_values = [
        str(item or "").strip()
        for key, item in cfg.items()
        if any(marker in str(key).lower() for marker in ("token", "password", "secret", "access_key", "api_key"))
        and str(item or "").strip()
    ]
    private_roots = [
        str(cfg.get(key) or "").strip()
        for key in ("catalog_path", "cloud_drive_storage_root")
        if str(cfg.get(key) or "").strip()
    ]
    for raw_line in str(value or "").splitlines():
        if "browser_event action=" in raw_line:
            continue
        line = redact_sensitive_text(raw_line)
        for secret in sensitive_values:
            line = line.replace(secret, "<redacted>")
        for root in private_roots:
            line = line.replace(root, "<private-root>")
            line = line.replace(root.replace("\\", "/"), "<private-root>")
        line = _SUPPORT_JSON_FIELD_RE.sub(r'\1"<redacted>"', line)
        safe_lines.append(line)
    return "\n".join(safe_lines) + ("\n" if safe_lines else "")


def _support_bundle(args: argparse.Namespace) -> int:
    cfg = load_config()
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_arg = str(args.output or "").strip()
    output = Path(output_arg).expanduser() if output_arg else PROJECT_ROOT / "runtime" / "support" / f"rag-support-{timestamp}.zip"
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    status_buffer = io.StringIO()
    with contextlib.redirect_stdout(status_buffer):
        _status(str(args.host), int(args.port))

    manifest = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "kind": "rag-catalog-support-bundle",
        "version": 1,
        "python": sys.version,
        "platform": _platform_summary(),
    }
    runtime_dir = _shared_runtime_dir(cfg)
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
        zf.writestr("config.redacted.json", json.dumps(_redact_config(cfg), ensure_ascii=False, indent=2))
        zf.writestr("launcher_status.txt", status_buffer.getvalue())
        for name in ("nice_app.log", "telegram_bot.log", "index_rag.log", "ocr_pdfs.log", "qdrant.log"):
            tail = read_history_tail(name, max_chars=int(args.log_chars or 20000))
            if tail:
                safe_tail = _redact_support_log(cfg, tail)
                if safe_tail:
                    zf.writestr(f"logs/{Path(name).stem}.tail.log", safe_tail)
        if runtime_dir.exists():
            for path in runtime_dir.glob("*"):
                if path.is_file():
                    zf.write(path, f"runtime/{path.name}")
    print(json.dumps({"support_bundle": str(output)}, ensure_ascii=False, indent=2))
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Unified launcher for RAG web, Qdrant and Telegram bot.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_start = sub.add_parser("start", help="Start services")
    p_start.add_argument("--host", default="127.0.0.1")
    p_start.add_argument("--port", type=int, default=8080)
    p_start.add_argument("--qdrant", choices=["auto", "on", "off"], default="auto")
    p_start.add_argument("--bot", choices=["auto", "on", "off"], default="auto")

    p_stop = sub.add_parser("stop", help="Stop managed services")
    p_stop.add_argument("--with-qdrant", action="store_true", help="Also stop managed qdrant container")

    p_status = sub.add_parser("status", help="Show status")
    p_status.add_argument("--host", default="127.0.0.1")
    p_status.add_argument("--port", type=int, default=8080)

    p_restart = sub.add_parser("restart", help="Restart services")
    p_restart.add_argument("--host", default="127.0.0.1")
    p_restart.add_argument("--port", type=int, default=8080)
    p_restart.add_argument("--qdrant", choices=["auto", "on", "off"], default="auto")
    p_restart.add_argument("--bot", choices=["auto", "on", "off"], default="auto")
    p_restart.add_argument("--with-qdrant", action="store_true", help="Stop managed qdrant before restart")

    p_restart_bot = sub.add_parser("restart-bot", help="Restart only the Telegram bot")
    p_restart_bot.add_argument("--bot", choices=["auto", "on"], default="auto")

    p_restart_web = sub.add_parser("restart-web", help="Restart only the NiceGUI web service")
    p_restart_web.add_argument("--host", default="127.0.0.1")
    p_restart_web.add_argument("--port", type=int, default=8080)

    p_support = sub.add_parser("support-bundle", help="Write a redacted support diagnostic zip")
    p_support.add_argument("--output", default="", help="Output zip path")
    p_support.add_argument("--host", default="127.0.0.1")
    p_support.add_argument("--port", type=int, default=8080)
    p_support.add_argument("--log-chars", type=int, default=20000, help="Tail characters per logical log")

    args = parser.parse_args(argv)
    if args.cmd == "start":
        return _start(args)
    if args.cmd == "stop":
        return _stop(args)
    if args.cmd == "status":
        return _status(args.host, int(args.port))
    if args.cmd == "restart":
        return _restart(args)
    if args.cmd == "restart-bot":
        return _restart_bot(args)
    if args.cmd == "restart-web":
        return _restart_web(args)
    if args.cmd == "support-bundle":
        return _support_bundle(args)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
