"""Unified launcher for web UI, Qdrant and Telegram bot."""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from rag_catalog.core.rag_core import load_config

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_DIR = PROJECT_ROOT / "logs" / "runtime"


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
                "if($p){$p.CommandLine}else{''}"
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
    if os.name != "nt":
        return 0
    try:
        escaped = module.replace("'", "''")
        cmd = (
            "$procs=Get-CimInstance Win32_Process | "
            "Where-Object { $_.Name -match '^python(\\.exe)?$' -and $_.CommandLine -like '*-m "
            + escaped
            + "*' }; "
            "if($procs){($procs | Select-Object -First 1 -ExpandProperty ProcessId)}"
        )
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", cmd],
            capture_output=True,
            text=True,
            timeout=8,
        )
        value = str(result.stdout or "").strip()
        return int(value) if value.isdigit() else 0
    except Exception:
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


def _port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _windows_flags() -> int:
    flags = 0
    for name in ("CREATE_NO_WINDOW", "DETACHED_PROCESS", "CREATE_NEW_PROCESS_GROUP", "CREATE_BREAKAWAY_FROM_JOB"):
        flags |= int(getattr(subprocess, name, 0) or 0)
    return flags


def _spawn_python_module(module: str, args: list[str], cwd: Path, log_name: str) -> int:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT / "src")
    env["PYTHONIOENCODING"] = "utf-8"
    _runtime_dir()
    log_path = RUNTIME_DIR / log_name
    log_fh = open(log_path, "a", encoding="utf-8", errors="replace")  # noqa: WPS515
    log_fh.write(f"\n{'=' * 70}\nstart {module} {time.strftime('%Y-%m-%d %H:%M:%S')}\n{'=' * 70}\n")
    log_fh.flush()
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
    for _ in range(40):
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
        return "web=not-managed"
    stopped = _kill_pid(pid)
    _remove_pid(web_pid_file)
    return f"web={'stopped' if stopped else 'already-down'} (pid={pid})"


def _start_bot(enable_mode: str) -> str:
    cfg = load_config()
    bot_pid_file = _pid_file(cfg, "bot")
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
    time.sleep(1.0)
    if _pid_alive(new_pid):
        return f"bot=started (pid={new_pid})"
    return "bot=failed-to-start"


def _stop_bot() -> str:
    cfg = load_config()
    bot_pid_file = _pid_file(cfg, "bot")
    payload = _read_pid_payload(bot_pid_file)
    pid = int(payload.get("pid") or 0)
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

    if target["mode"] == "local":
        print(f"- qdrant.mode: local-file ({cfg.get('qdrant_db_path')})")
    else:
        q_host = str(target["host"])
        q_port = int(target["port"])
        print(f"- qdrant.mode: server ({target['url']})")
        print(f"- qdrant.port: {'open' if _port_open(q_host, q_port) else 'closed'} ({q_host}:{q_port})")
        print(f"- qdrant.managed: {'yes' if _pid_file(cfg, 'qdrant').exists() else 'no'}")

    print(f"- bot.process: {'up' if bot_alive else 'down'} (pid={bot_pid or '-'})")
    if bot_discovered:
        print("- bot.note: discovered running process from another worktree/runtime")
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
    return _start(args)


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

    args = parser.parse_args(argv)
    if args.cmd == "start":
        return _start(args)
    if args.cmd == "stop":
        return _stop(args)
    if args.cmd == "status":
        return _status(args.host, int(args.port))
    if args.cmd == "restart":
        return _restart(args)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
