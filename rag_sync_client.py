#!/usr/bin/env python3
"""
rag_sync_client.py — Standalone sync agent for RAG Catalog Cloud Drive.

Install deps:  pip install requests watchdog
Run:           python rag_sync_client.py --server http://host:8080 --token TOKEN
               python rag_sync_client.py        # uses saved ~/.rag_sync/config.json
               python rag_sync_client.py --status  # print status and exit
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import platform
import queue
import signal
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import webbrowser

import requests

try:
    from watchdog.events import FileSystemEventHandler, FileSystemEvent
    from watchdog.observers import Observer
    HAS_WATCHDOG = True
except ImportError:
    HAS_WATCHDOG = False

# ─── Constants ────────────────────────────────────────────────────────────────

DEFAULT_CONFIG_DIR = Path.home() / ".rag_sync"
DEFAULT_CONFIG_FILE = DEFAULT_CONFIG_DIR / "config.json"

POLL_INTERVAL = 30        # seconds between server change polls
HEARTBEAT_INTERVAL = 60   # seconds between heartbeats
UPLOAD_DEBOUNCE = 2.0     # seconds after last FS event before uploading
UPLOAD_WORKERS = 3        # parallel upload threads
REQUEST_TIMEOUT = 30      # seconds per HTTP request

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("rag_sync")

# ─── Config ───────────────────────────────────────────────────────────────────

_REGISTRY_KEY = r"Software\RAGSyncClient"


def _read_registry_config() -> Dict[str, Any]:
    """Read server/token written by MSI installer from HKCU registry (Windows only)."""
    if platform.system() != "Windows":
        return {}
    try:
        import winreg
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, _REGISTRY_KEY) as key:
            def _val(name: str) -> str:
                try:
                    return str(winreg.QueryValueEx(key, name)[0] or "")
                except FileNotFoundError:
                    return ""
            return {
                "server": _val("Server"),
                "token": _val("Token"),
                "device_id": _val("DeviceId"),
                "display_name": _val("DisplayName"),
            }
    except Exception:
        return {}


def load_config(path: Path) -> Dict[str, Any]:
    # Registry values are the baseline (written by MSI); file overrides them
    cfg = _read_registry_config()
    if path.exists():
        try:
            file_cfg = json.loads(path.read_text(encoding="utf-8"))
            cfg.update({k: v for k, v in file_cfg.items() if v})
        except Exception:
            pass
    return cfg


def save_config(path: Path, cfg: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")

# ─── Device Auth Flow ─────────────────────────────────────────────────────────

def device_auth_flow(server: str) -> Optional[str]:
    """
    Perform browser-based device authorization (RFC 8628-style).
    Opens the server's /auth/device page in the browser, displays the user
    code, then polls until the user approves or the code expires.
    Returns the session token on success, None on failure.
    """
    try:
        r = requests.post(f"{server}/api/auth/device/code", timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        log.error("Не удалось запросить код устройства: %s", exc)
        return None

    device_code = str(data.get("device_code") or "")
    user_code   = str(data.get("user_code") or "")
    verify_url  = str(data.get("verification_uri_complete") or data.get("verification_uri") or f"{server}/auth/device")
    expires_in  = int(data.get("expires_in") or 300)
    interval    = int(data.get("interval") or 5)

    print()
    print("━" * 55)
    print("  Для подключения устройства откройте в браузере:")
    print(f"  {verify_url}")
    print()
    print(f"  Код подтверждения:  {user_code}")
    print(f"  Действителен:       {expires_in // 60} мин")
    print("━" * 55)
    print()

    try:
        webbrowser.open(verify_url)
    except Exception:
        pass

    deadline = time.monotonic() + expires_in
    while time.monotonic() < deadline:
        time.sleep(interval)
        try:
            r = requests.get(
                f"{server}/api/auth/device/token",
                params={"device_code": device_code},
                timeout=10,
            )
            if r.status_code == 200:
                token = str(r.json().get("token") or "")
                if token:
                    log.info("Авторизация выполнена успешно.")
                    return token
            elif r.status_code == 428:
                log.debug("Ожидание подтверждения в браузере...")
            else:
                detail = r.json().get("detail", str(r.status_code))
                log.error("Авторизация отклонена: %s", detail)
                return None
        except Exception as exc:
            log.warning("Ошибка при проверке токена: %s", exc)

    log.error("Время ожидания подтверждения истекло (5 мин). Перезапустите клиент.")
    return None


# ─── API client ───────────────────────────────────────────────────────────────

class SyncAPIClient:
    def __init__(self, server: str, token: str) -> None:
        self.base = server.rstrip("/")
        self.token = token
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {token}"

    def _get(self, path: str, **params: Any) -> Any:
        r = self.session.get(f"{self.base}{path}", params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, **params: Any) -> Any:
        r = self.session.post(f"{self.base}{path}", params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()

    def register(self, device_id: str, display_name: str, platform_name: str) -> Dict[str, Any]:
        return self._post(
            "/api/cloud-drive/sync/clients",
            device_id=device_id,
            display_name=display_name,
            platform=platform_name,
            status="online",
        )

    def heartbeat(self, client_id: str, status: str = "online") -> None:
        self._post("/api/cloud-drive/sync/heartbeat", client_id=client_id, status=status)

    def get_pairs(self, client_id: str) -> List[Dict[str, Any]]:
        return self._get("/api/cloud-drive/sync/pairs", client_id=client_id, enabled_only=True)

    def get_changes(self, since: str, limit: int = 500) -> Dict[str, Any]:
        return self._get("/api/cloud-drive/changes", since=since, limit=limit)

    def download(self, cloud_path: str, dest: Path) -> None:
        url = f"{self.base}/api/cloud-drive/download"
        r = self.session.get(url, params={"path": cloud_path}, timeout=REQUEST_TIMEOUT, stream=True)
        r.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(delete=False, dir=dest.parent, suffix=".tmp") as tmp:
            for chunk in r.iter_content(chunk_size=65536):
                tmp.write(chunk)
            tmp_path = tmp.name
        Path(tmp_path).replace(dest)

    def upload(self, local_path: Path, cloud_parent_path: str) -> Dict[str, Any]:
        url = f"{self.base}/api/cloud-drive/upload"
        with local_path.open("rb") as fh:
            r = self.session.post(
                url,
                params={"parent_path": cloud_parent_path},
                files={"file": (local_path.name, fh)},
                timeout=REQUEST_TIMEOUT,
            )
        r.raise_for_status()
        return r.json()

    def record_conflict(self, client_id: str, pair_id: str, path: str,
                        conflict_type: str, local_path: str = "", cloud_path: str = "") -> Dict[str, Any]:
        return self._post(
            "/api/cloud-drive/sync/conflicts",
            client_id=client_id,
            pair_id=pair_id,
            path=path,
            conflict_type=conflict_type,
            local_path=local_path,
            cloud_path=cloud_path,
        )

# ─── Path helpers ─────────────────────────────────────────────────────────────

def _find_pair_for_local(pairs: List[Dict[str, Any]], local_file: Path) -> Optional[Dict[str, Any]]:
    best: Optional[Dict[str, Any]] = None
    best_len = 0
    for pair in pairs:
        local_root = Path(pair["local_path"])
        try:
            local_file.relative_to(local_root)
            depth = len(local_root.parts)
            if depth > best_len:
                best = pair
                best_len = depth
        except ValueError:
            pass
    return best


def _local_to_cloud_parent(pair: Dict[str, Any], local_file: Path) -> str:
    local_root = Path(pair["local_path"])
    cloud_root = pair.get("cloud_path", "").rstrip("/")
    rel = local_file.parent.relative_to(local_root)
    rel_str = rel.as_posix()
    if rel_str == ".":
        return cloud_root
    return f"{cloud_root}/{rel_str}" if cloud_root else rel_str


def _cloud_to_local(pair: Dict[str, Any], cloud_path: str) -> Path:
    cloud_root = pair.get("cloud_path", "").rstrip("/")
    local_root = Path(pair["local_path"])
    if cloud_root and cloud_path.startswith(cloud_root + "/"):
        rel = cloud_path[len(cloud_root) + 1:]
    elif cloud_root and cloud_path == cloud_root:
        rel = ""
    else:
        rel = cloud_path.lstrip("/")
    return local_root / rel if rel else local_root


def _find_pair_for_cloud(pairs: List[Dict[str, Any]], cloud_path: str) -> Optional[Dict[str, Any]]:
    best: Optional[Dict[str, Any]] = None
    best_len = 0
    for pair in pairs:
        cloud_root = pair.get("cloud_path", "").rstrip("/")
        if cloud_path == cloud_root or cloud_path.startswith(cloud_root + "/"):
            depth = len(cloud_root)
            if depth > best_len:
                best = pair
                best_len = depth
    return best


def _conflict_copy_path(dest: Path) -> Path:
    ts = time.strftime("%Y%m%d_%H%M%S")
    stem = dest.stem
    suffix = dest.suffix
    return dest.parent / f"{stem}_CONFLICT_{ts}{suffix}"

# ─── Upload debounce queue ─────────────────────────────────────────────────────

class _UploadTask:
    __slots__ = ("local_path", "cloud_parent", "retry")

    def __init__(self, local_path: Path, cloud_parent: str, retry: int = 0) -> None:
        self.local_path = local_path
        self.cloud_parent = cloud_parent
        self.retry = retry


class UploadQueue:
    """Debounced upload queue — coalesces rapid FS events for the same file."""

    def __init__(self, api: SyncAPIClient) -> None:
        self._api = api
        self._pending: Dict[Path, float] = {}  # path → scheduled_at
        self._cloud_parent: Dict[Path, str] = {}
        self._lock = threading.Lock()
        self._q: queue.Queue[_UploadTask] = queue.Queue()
        self._stop = threading.Event()

    def enqueue(self, local_path: Path, cloud_parent: str) -> None:
        with self._lock:
            self._pending[local_path] = time.monotonic() + UPLOAD_DEBOUNCE
            self._cloud_parent[local_path] = cloud_parent

    def _flush_loop(self) -> None:
        while not self._stop.is_set():
            now = time.monotonic()
            ready: List[Path] = []
            with self._lock:
                for path, due in list(self._pending.items()):
                    if now >= due:
                        ready.append(path)
                for path in ready:
                    parent = self._cloud_parent.pop(path, "")
                    self._pending.pop(path, None)
                    self._q.put(_UploadTask(local_path=path, cloud_parent=parent))
            time.sleep(0.5)

    def _worker(self) -> None:
        while True:
            task = self._q.get()
            if task is None:
                return
            try:
                if not task.local_path.is_file():
                    continue
                log.info("Загрузка: %s → %s", task.local_path.name, task.cloud_parent)
                self._api.upload(task.local_path, task.cloud_parent)
            except Exception as exc:
                if task.retry < 3:
                    log.warning("Ошибка загрузки %s: %s (попытка %d)", task.local_path.name, exc, task.retry + 1)
                    time.sleep(5 * (task.retry + 1))
                    self._q.put(_UploadTask(task.local_path, task.cloud_parent, task.retry + 1))
                else:
                    log.error("Не удалось загрузить %s после 3 попыток: %s", task.local_path.name, exc)
            finally:
                self._q.task_done()

    def start(self) -> None:
        threading.Thread(target=self._flush_loop, daemon=True, name="upload-flusher").start()
        for i in range(UPLOAD_WORKERS):
            threading.Thread(target=self._worker, daemon=True, name=f"uploader-{i}").start()

    def stop(self) -> None:
        self._stop.set()
        for _ in range(UPLOAD_WORKERS):
            self._q.put(None)  # type: ignore[arg-type]

# ─── Watchdog ─────────────────────────────────────────────────────────────────

class _PairEventHandler(FileSystemEventHandler):
    def __init__(self, pair: Dict[str, Any], upload_q: UploadQueue) -> None:
        self._pair = pair
        self._upload_q = upload_q

    def on_modified(self, event: FileSystemEvent) -> None:
        self._handle(event)

    def on_created(self, event: FileSystemEvent) -> None:
        self._handle(event)

    def _handle(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        local_path = Path(str(event.src_path))
        if not local_path.is_file():
            return
        cloud_parent = _local_to_cloud_parent(self._pair, local_path)
        self._upload_q.enqueue(local_path, cloud_parent)


def start_watchdog(pairs: List[Dict[str, Any]], upload_q: UploadQueue) -> Optional[Any]:
    if not HAS_WATCHDOG:
        log.warning("watchdog не установлен — загрузка по изменению файлов недоступна. pip install watchdog")
        return None
    observer = Observer()
    for pair in pairs:
        local_root = Path(pair["local_path"])
        if not local_root.is_dir():
            log.warning("Локальный каталог не найден: %s — пропускаю.", local_root)
            continue
        handler = _PairEventHandler(pair, upload_q)
        observer.schedule(handler, str(local_root), recursive=True)
        log.info("Слежение: %s → %s", local_root, pair.get("cloud_path", "(root)"))
    observer.start()
    return observer

# ─── Changes poller ───────────────────────────────────────────────────────────

def _apply_change(api: SyncAPIClient, change: Dict[str, Any],
                  pairs: List[Dict[str, Any]], client_id: str) -> None:
    cloud_path = str(change.get("path") or "")
    change_type = str(change.get("type") or change.get("change_type") or "")
    if not cloud_path or change_type in ("", "version"):
        return

    pair = _find_pair_for_cloud(pairs, cloud_path)
    if pair is None:
        return

    local_dest = _cloud_to_local(pair, cloud_path)
    policy = str(pair.get("conflict_policy") or "ask").lower()

    if change_type == "delete":
        if local_dest.is_file():
            local_dest.unlink(missing_ok=True)
            log.info("Удалён локально: %s", local_dest)
        return

    # download (new / modified)
    if local_dest.exists():
        if policy in ("keep_local", "local_wins"):
            log.debug("Пропущено (keep_local): %s", local_dest)
            return
        if policy in ("ask", "keep_both"):
            conflict_copy = _conflict_copy_path(local_dest)
            local_dest.rename(conflict_copy)
            log.info("Конфликт: сохранена локальная копия → %s", conflict_copy)
            try:
                api.record_conflict(
                    client_id=client_id,
                    pair_id=str(pair.get("id") or ""),
                    path=cloud_path,
                    conflict_type="content",
                    local_path=str(conflict_copy),
                    cloud_path=cloud_path,
                )
            except Exception:
                pass

    try:
        api.download(cloud_path, local_dest)
        log.info("Загружено с сервера: %s", local_dest)
    except Exception as exc:
        log.warning("Ошибка загрузки %s: %s", cloud_path, exc)


def changes_poll_loop(api: SyncAPIClient, pairs: List[Dict[str, Any]],
                      client_id: str, stop: threading.Event) -> None:
    cursor = ""
    while not stop.wait(POLL_INTERVAL):
        try:
            result = api.get_changes(since=cursor)
            changes: List[Dict[str, Any]] = result.get("changes", [])
            if changes:
                log.info("Получено изменений с сервера: %d", len(changes))
                for change in changes:
                    _apply_change(api, change, pairs, client_id)
                # advance cursor to the latest updated_at
                times = [c.get("updated_at") or c.get("created_at") or "" for c in changes]
                new_cursor = max((t for t in times if t), default=cursor)
                if new_cursor > cursor:
                    cursor = new_cursor
        except Exception as exc:
            log.warning("Ошибка опроса изменений: %s", exc)

# ─── Heartbeat loop ───────────────────────────────────────────────────────────

def heartbeat_loop(api: SyncAPIClient, client_id: str, stop: threading.Event) -> None:
    while not stop.wait(HEARTBEAT_INTERVAL):
        try:
            api.heartbeat(client_id)
            log.debug("Heartbeat OK")
        except Exception as exc:
            log.warning("Heartbeat ошибка: %s", exc)

# ─── CLI ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="RAG Catalog Cloud Drive sync agent",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--server", metavar="URL", help="Адрес сервера, напр. http://host:8080")
    p.add_argument("--token", metavar="TOKEN", help="API-токен (сессионный токен пользователя)")
    p.add_argument("--device-id", metavar="ID", help="Уникальный ID устройства (создаётся автоматически)")
    p.add_argument("--display-name", metavar="NAME", help="Отображаемое имя устройства")
    p.add_argument("--config", metavar="PATH", default=str(DEFAULT_CONFIG_FILE), help="Путь к файлу конфигурации")
    p.add_argument("--status", action="store_true", help="Показать статус и выйти")
    p.add_argument("--verbose", "-v", action="store_true", help="Подробный вывод")
    p.add_argument("--log-file", metavar="PATH", help="Записывать лог в файл")
    return p.parse_args()


def _setup_logging(verbose: bool, log_file: Optional[str]) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    log.setLevel(level)
    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logging.getLogger().addHandler(fh)


def main() -> None:
    args = parse_args()
    _setup_logging(args.verbose, args.log_file)

    config_path = Path(args.config)
    cfg = load_config(config_path)

    # Merge CLI args into config (CLI wins)
    if args.server:
        cfg["server"] = args.server
    if args.token:
        cfg["token"] = args.token
    if args.device_id:
        cfg["device_id"] = args.device_id
    if args.display_name:
        cfg["display_name"] = args.display_name

    server = str(cfg.get("server") or "").rstrip("/")
    token  = str(cfg.get("token") or "")

    if not server:
        print("Ошибка: нужно указать --server (или сохранить в конфиге).", file=sys.stderr)
        sys.exit(1)

    if not cfg.get("device_id"):
        cfg["device_id"] = str(uuid.uuid4())

    if not cfg.get("display_name"):
        cfg["display_name"] = f"{platform.node()} ({platform.system()})"

    # ── Auth: device flow on first run or after token expiry ──────────────────
    if not token:
        log.info("Токен не найден — запускаем авторизацию через браузер...")
        token = device_auth_flow(server)
        if not token:
            sys.exit(1)
        cfg["token"] = token
        save_config(config_path, cfg)

    api = SyncAPIClient(server, token)

    # Register / heartbeat ─────────────────────────────────────────────────────
    log.info("Подключение к серверу %s ...", server)
    try:
        client_info = api.register(
            device_id=cfg["device_id"],
            display_name=cfg["display_name"],
            platform_name=platform.system(),
        )
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 401:
            log.warning("Токен недействителен или истёк — повторная авторизация...")
            cfg.pop("token", None)
            save_config(config_path, cfg)
            token = device_auth_flow(server)
            if not token:
                sys.exit(1)
            cfg["token"] = token
            save_config(config_path, cfg)
            api = SyncAPIClient(server, token)
            client_info = api.register(
                device_id=cfg["device_id"],
                display_name=cfg["display_name"],
                platform_name=platform.system(),
            )
        else:
            log.error("Ошибка регистрации: %s", exc)
            sys.exit(1)
    except Exception as exc:
        log.error("Ошибка регистрации: %s", exc)
        sys.exit(1)

    client_id = str(client_info.get("id") or "")
    cfg["client_id"] = client_id
    save_config(config_path, cfg)
    log.info("Клиент зарегистрирован: %s (%s)", cfg["display_name"], client_id[:8])

    # Load sync pairs ──────────────────────────────────────────────────────────
    try:
        pairs = api.get_pairs(client_id)
    except Exception as exc:
        log.error("Ошибка получения sync-пар: %s", exc)
        sys.exit(1)

    if not pairs:
        log.warning("Sync-пары не настроены. Настройте их на сервере в разделе Cloud Drive → Clients.")

    if args.status:
        print(f"Сервер:   {server}")
        print(f"Клиент:   {cfg['display_name']} ({client_id[:8]}...)")
        print(f"Статус:   online")
        print(f"Пары ({len(pairs)}):")
        for p in pairs:
            print(f"  {p['local_path']} ↔ {p.get('cloud_path') or '(root)'} [{p.get('conflict_policy', 'ask')}]")
        return

    # Start upload queue + watchdog ────────────────────────────────────────────
    upload_q = UploadQueue(api)
    upload_q.start()

    observer = start_watchdog(pairs, upload_q) if pairs else None

    stop = threading.Event()

    # Start background loops ───────────────────────────────────────────────────
    poll_thread = threading.Thread(
        target=changes_poll_loop,
        args=(api, pairs, client_id, stop),
        daemon=True,
        name="changes-poller",
    )
    poll_thread.start()

    hb_thread = threading.Thread(
        target=heartbeat_loop,
        args=(api, client_id, stop),
        daemon=True,
        name="heartbeat",
    )
    hb_thread.start()

    log.info("Sync-агент запущен. Ctrl+C для остановки.")

    def _shutdown(signum: int, frame: Any) -> None:
        log.info("Остановка...")
        stop.set()
        if observer:
            observer.stop()
        upload_q.stop()
        try:
            api.heartbeat(client_id, status="offline")
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Main thread waits ────────────────────────────────────────────────────────
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        _shutdown(0, None)


if __name__ == "__main__":
    main()
