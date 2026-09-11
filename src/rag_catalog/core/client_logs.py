"""Приём журналов клиентских приложений на сервере.

Зачем: ошибки sync-клиента, нативного клиента и Cloud Files провайдера до сих
пор оставались на машине пользователя — сервер знал только «online/offline».
Разобрать поломку можно было, лишь попросив человека найти файл в
``%LOCALAPPDATA%`` и прислать его.

События складываются в ту же таблицу ``app_events``, что и браузерная
диагностика (`/api/ui-events`), с ``feature='client'`` — поэтому их сразу видно
в «Аналитике» рядом с остальными.

Три правила, которых здесь придерживаемся:

* **Журнал не должен ронять клиента.** Любая ошибка отправки гасится, очередь
  ограничена — лучше потерять запись, чем уронить синхронизацию.
* **Секреты не уезжают.** Строки чистятся от токенов, паролей и ключей: в
  логах клиентов они встречаются в URL и заголовках.
* **Объём ограничен.** Клиент не может забить телеметрию: лимиты на размер
  пачки, длину сообщения и длину деталей.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

__all__ = [
    "CLIENT_FEATURE",
    "KNOWN_CLIENTS",
    "MAX_EVENTS_PER_BATCH",
    "ClientLogUploader",
    "TelemetryLogHandler",
    "ingest_client_events",
    "normalise_event",
    "redact_secrets",
]

CLIENT_FEATURE = "client"
KNOWN_CLIENTS = ("sync", "desktop", "cfapi", "bot", "other")

MAX_EVENTS_PER_BATCH = 100
MAX_MESSAGE_CHARS = 2000
MAX_DETAIL_CHARS = 4000
MAX_LOGGER_CHARS = 120
MAX_DEVICE_CHARS = 120

_LEVELS = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}

# Токены и пароли попадают в логи через URL, заголовки и строки подключения.
_SECRET_PATTERNS = (
    re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._\-]+"),
    # Значение может быть в кавычках: token="...", password: '...'
    re.compile(
        r"(?i)\b(token|password|passwd|secret|api[_-]?key|access[_-]?key)\b\s*[=:]\s*[\"']?[^\s,;&\"']+[\"']?"
    ),
    re.compile(r"(?i)([?&](?:token|key|password|secret)=)[^&\s]+"),
    re.compile(r"(?i)://[^/\s:@]+:[^/\s@]+@"),  # user:pass@host
)


def redact_secrets(text: str) -> str:
    """Вырезает из строки то, что не должно уезжать на сервер."""
    value = str(text or "")
    for pattern in _SECRET_PATTERNS:
        if pattern.pattern.startswith("(?i)://"):
            value = pattern.sub("://<скрыто>@", value)
        elif "([?&]" in pattern.pattern:
            value = pattern.sub(r"\1<скрыто>", value)
        else:
            value = pattern.sub("<скрыто>", value)
    return value


def _clean(value: Any, limit: int) -> str:
    return redact_secrets(str(value or "")).strip()[:limit]


def _normalise_ts(raw: Any) -> str:
    """ISO-время события у клиента; при мусоре — пусто (останется время приёма)."""
    if isinstance(raw, (int, float)) and raw > 0:
        try:
            return datetime.fromtimestamp(float(raw), tz=timezone.utc).isoformat()
        except (OverflowError, OSError, ValueError):
            return ""
    text = str(raw or "").strip()[:64]
    return text


def normalise_event(raw: Any) -> Optional[Dict[str, Any]]:
    """Приводит запись клиента к общему виду; None — если запись пустая."""
    if not isinstance(raw, dict):
        raw = {"message": raw}
    message = _clean(raw.get("message") or raw.get("msg"), MAX_MESSAGE_CHARS)
    detail = _clean(raw.get("detail") or raw.get("traceback") or raw.get("exception"), MAX_DETAIL_CHARS)
    if not message and not detail:
        return None
    level = str(raw.get("level") or "ERROR").strip().upper()[:16]
    if level not in _LEVELS:
        level = "ERROR"
    event: Dict[str, Any] = {
        "level": level,
        "message": message,
        "logger": _clean(raw.get("logger"), MAX_LOGGER_CHARS),
        "ts": _normalise_ts(raw.get("ts") or raw.get("time")),
    }
    if detail:
        event["detail"] = detail
    return event


def ingest_client_events(
    telemetry: Any,
    *,
    username: str,
    client: str,
    device_id: str = "",
    events: Iterable[Any],
    client_host: str = "",
    app_version: str = "",
) -> int:
    """Записывает пачку клиентских событий в телеметрию. Возвращает число принятых."""
    kind = str(client or "").strip().lower()[:32] or "other"
    if kind not in KNOWN_CLIENTS:
        kind = "other"
    device = _clean(device_id, MAX_DEVICE_CHARS)
    version = _clean(app_version, 40)
    host = _clean(client_host, 64)

    stored = 0
    for raw in list(events or [])[:MAX_EVENTS_PER_BATCH]:
        event = normalise_event(raw)
        if event is None:
            continue
        details = dict(event)
        details["device_id"] = device
        if version:
            details["app_version"] = version
        if host:
            details["client_host"] = host
        action = f"client_{event['level'].lower()}"
        try:
            telemetry.log_app_event(
                username=username,
                screen=kind,
                feature=CLIENT_FEATURE,
                action=action,
                ok=event["level"] not in {"ERROR", "CRITICAL"},
                details=details,
            )
        except Exception:  # pragma: no cover - телеметрия не должна ломать приём
            continue
        stored += 1
    return stored


class _BufferingHandler(logging.Handler):
    """Общая часть: очередь ограниченной длины + фоновая выгрузка."""

    def __init__(
        self,
        *,
        level: int = logging.WARNING,
        batch_size: int = 20,
        flush_interval: float = 15.0,
        queue_size: int = 500,
    ) -> None:
        super().__init__(level=level)
        self._queue: deque = deque(maxlen=max(10, int(queue_size)))
        self._batch_size = max(1, int(batch_size))
        self._flush_interval = max(1.0, float(flush_interval))
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ── logging.Handler ────────────────────────────────────────────────

    def emit(self, record: logging.LogRecord) -> None:
        try:
            event = {
                "level": record.levelname,
                "message": record.getMessage(),
                "logger": record.name,
                "ts": record.created,
            }
            if record.exc_info:
                event["detail"] = self.format(record)
            with self._lock:
                self._queue.append(event)
        except Exception:  # pragma: no cover - журнал не должен ронять клиента
            pass

    # ── фоновая выгрузка ───────────────────────────────────────────────

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, name="client-log-uploader", daemon=True)
        self._thread.start()

    def _loop(self) -> None:  # pragma: no cover - таймерный цикл
        while not self._stop.wait(self._flush_interval):
            self.flush()

    def _take(self) -> List[Dict[str, Any]]:
        with self._lock:
            batch = list(self._queue)[: self._batch_size]
            for _ in range(len(batch)):
                self._queue.popleft()
        return batch

    def _requeue(self, batch: Sequence[Dict[str, Any]]) -> None:
        with self._lock:
            self._queue.extendleft(reversed(list(batch)))

    def flush(self) -> None:
        batch = self._take()
        if not batch:
            return
        try:
            self._deliver(batch)
        except Exception:
            # Не удалось — вернём в очередь: возможно, сервер просто недоступен.
            self._requeue(batch)

    def _deliver(self, batch: Sequence[Dict[str, Any]]) -> None:  # pragma: no cover - абстрактный
        raise NotImplementedError

    def close(self) -> None:
        self._stop.set()
        try:
            self.flush()
        except Exception:
            pass
        super().close()


class ClientLogUploader(_BufferingHandler):
    """Отправляет предупреждения и ошибки клиента на ``/api/client-logs``.

    ``send`` — любая функция, принимающая тело запроса; так обработчик можно
    тестировать без HTTP и переиспользовать из разных клиентов.
    """

    def __init__(
        self,
        send: Callable[[Dict[str, Any]], Any],
        *,
        client: str,
        device_id: str = "",
        app_version: str = "",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._send = send
        self._client = client
        self._device_id = device_id
        self._app_version = app_version

    def _deliver(self, batch: Sequence[Dict[str, Any]]) -> None:
        self._send(
            {
                "client": self._client,
                "device_id": self._device_id,
                "app_version": self._app_version,
                "events": list(batch),
            }
        )


class TelemetryLogHandler(_BufferingHandler):
    """Пишет ошибки прямо в телеметрию — для клиентов рядом с БД (нативное окно)."""

    def __init__(
        self,
        telemetry: Any,
        *,
        client: str,
        username: str = "",
        device_id: str = "",
        app_version: str = "",
        **kwargs: Any,
    ) -> None:
        kwargs.setdefault("flush_interval", 5.0)
        super().__init__(**kwargs)
        self._telemetry = telemetry
        self._client = client
        self._username = username
        self._device_id = device_id
        self._app_version = app_version

    def _deliver(self, batch: Sequence[Dict[str, Any]]) -> None:
        ingest_client_events(
            self._telemetry,
            username=self._username,
            client=self._client,
            device_id=self._device_id,
            events=batch,
            app_version=self._app_version,
        )


def install_local_file_log(
    log_path: Any,
    *,
    max_bytes: int = 5 * 1024 * 1024,
    backups: int = 4,
    level: int = logging.INFO,
) -> Optional[logging.Handler]:
    """Ротируемый файл журнала рядом с клиентом (как у C#-провайдера).

    Нужен, чтобы просьба «пришлите лог» вообще была выполнима: у нативного
    клиента до этого не было файла вовсе.
    """
    from logging.handlers import RotatingFileHandler  # noqa: PLC0415
    from pathlib import Path  # noqa: PLC0415

    try:
        path = Path(log_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(
            str(path), maxBytes=max_bytes, backupCount=backups, encoding="utf-8"
        )
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        logging.getLogger().addHandler(handler)
        return handler
    except Exception:
        return None


def default_client_log_path(app_name: str, file_name: str = "client.log") -> Any:
    """``%LOCALAPPDATA%\\<app>\\logs\\<file>`` — там же, где логи C#-провайдера."""
    import os  # noqa: PLC0415
    from pathlib import Path  # noqa: PLC0415

    base = os.environ.get("LOCALAPPDATA") or os.path.expanduser("~")
    return Path(base) / app_name / "logs" / file_name


def _now() -> float:  # pragma: no cover - тонкая обёртка для тестов
    return time.time()
