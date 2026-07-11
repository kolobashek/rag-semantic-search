"""Segmented log history helpers.

The application has several long-running processes. Plain append-only log files
quickly become hundreds of megabytes and are hard to inspect from the UI. This
module writes new logs into dated run segments while keeping old flat log files
readable as part of the same history.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import IO, Iterable, List, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MAX_BYTES = 25 * 1024 * 1024

_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9_.-]+")
_QDRANT_HTTP_OK_RE = re.compile(r'"HTTP/\d(?:\.\d)?\s+2\d\d\b')
_TELEGRAM_BOT_URL_RE = re.compile(r"(api\.telegram\.org/bot)[^/\s]+", re.IGNORECASE)
_TELEGRAM_BOT_TOKEN_RE = re.compile(r"\bbot\d{6,}:[A-Za-z0-9_-]+\b", re.IGNORECASE)
_BEARER_TOKEN_RE = re.compile(r"(Authorization\s*[:=]\s*Bearer\s+)[^\s,;]+", re.IGNORECASE)


def redact_sensitive_text(value: str) -> str:
    """Remove authentication material from persisted process and app logs."""
    text = _TELEGRAM_BOT_URL_RE.sub(r"\1<redacted>", str(value or ""))
    text = _TELEGRAM_BOT_TOKEN_RE.sub("bot<redacted>", text)
    return _BEARER_TOKEN_RE.sub(r"\1<redacted>", text)


class RedactingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return redact_sensitive_text(super().format(record))


def _safe_name(value: str) -> str:
    name = _SAFE_NAME_RE.sub("_", str(value or "").strip()).strip("._-")
    return name or "application"


def logical_log_name(path_or_name: str | Path) -> str:
    """Return a stable logical log name without the trailing .log suffix."""
    raw = str(path_or_name or "").strip()
    name = Path(raw).name if raw else "application.log"
    if name.lower().endswith(".log"):
        name = name[:-4]
    return _safe_name(name)


def log_history_root() -> Path:
    return PROJECT_ROOT / "logs" / "history"


def _legacy_candidates(path_or_name: str | Path) -> List[Path]:
    raw = Path(str(path_or_name))
    candidates = [
        raw,
        PROJECT_ROOT / "logs" / raw.name,
        PROJECT_ROOT / "logs" / "runtime" / raw.name,
    ]
    result: List[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        if path.exists() and path.is_file():
            result.append(path)
    return result


def new_log_segment_path(path_or_name: str | Path, *, suffix: str = "") -> Path:
    """Create a new dated segment path for a fresh process/server run."""
    logical = logical_log_name(path_or_name)
    day = datetime.now().strftime("%Y-%m-%d")
    stamp = datetime.now().strftime("%H%M%S")
    safe_suffix = _safe_name(suffix) if suffix else ""
    suffix_part = f"-{safe_suffix}" if safe_suffix else ""
    folder = log_history_root() / logical / day
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"{stamp}-p{os.getpid()}{suffix_part}-{logical}.log"


def list_log_segments(path_or_name: str | Path, *, include_legacy: bool = True) -> List[Path]:
    """List all known log segments for a logical log, oldest first."""
    logical = logical_log_name(path_or_name)
    paths: List[Path] = []
    root = log_history_root() / logical
    if root.exists():
        paths.extend(path for path in root.rglob("*.log") if path.is_file())
    if include_legacy:
        paths.extend(_legacy_candidates(path_or_name))

    seen: set[str] = set()
    unique: List[Path] = []
    for path in paths:
        key = str(path.resolve()) if path.exists() else str(path)
        if key in seen:
            continue
        seen.add(key)
        unique.append(path)
    return sorted(unique, key=lambda p: (p.stat().st_mtime if p.exists() else 0.0, str(p)))


def read_file_tail(path: Path, *, max_chars: int) -> str:
    """Read the tail of one file without loading huge legacy logs into memory."""
    if not path.exists() or not path.is_file():
        return ""
    size = path.stat().st_size
    if size <= 0:
        return ""
    read_size = min(size, max(4096, int(max_chars) * 4))
    with path.open("rb") as fh:
        if read_size < size:
            fh.seek(-read_size, os.SEEK_END)
        raw = fh.read(read_size)
    text = raw.decode("utf-8", errors="replace")
    if read_size < size:
        nl = text.find("\n")
        if nl >= 0:
            text = text[nl + 1 :]
    return text[-max_chars:]


def read_history_tail(path_or_name: str | Path, *, max_chars: int = 12000) -> str:
    """Read a seamless tail across history segments and legacy logs."""
    chunks: List[str] = []
    remaining = max(1, int(max_chars))
    for path in reversed(list_log_segments(path_or_name)):
        if remaining <= 0:
            break
        text = read_file_tail(path, max_chars=remaining)
        if not text:
            continue
        chunks.append(text)
        remaining -= len(text)
    return "\n".join(reversed(chunks))[-max_chars:]


def read_history_tail_lines(
    path_or_name: str | Path,
    *,
    max_lines: int = 200,
    max_chars: int = 200_000,
) -> str:
    """Read tail lines across all segments, newest entries first."""
    lines: List[str] = []
    needed = max(1, int(max_lines))
    for path in reversed(list_log_segments(path_or_name)):
        if len(lines) >= needed:
            break
        text = read_file_tail(path, max_chars=max_chars)
        if not text:
            continue
        current = text.splitlines()
        take = max(0, needed - len(lines))
        lines.extend(reversed(current[-take:]))
    result = "\n".join(lines[:needed])
    if len(result) > max_chars:
        result = result[:max_chars]
    return result


def open_run_log(path_or_name: str | Path, label: str = "") -> IO[str]:
    """Open a fresh log file for a process/server run and write a header."""
    path = new_log_segment_path(path_or_name, suffix="run")
    fh = open(path, "a", encoding="utf-8", errors="replace")  # noqa: WPS515
    if label:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fh.write(f"\n{'=' * 70}\n{label}  {ts}\n{'=' * 70}\n")
        fh.flush()
    return fh


class SizeDateLogHandler(logging.Handler):
    """Logging handler that rotates by day and file size inside log history."""

    def __init__(self, path_or_name: str | Path, *, max_bytes: int = DEFAULT_MAX_BYTES, label: str = "") -> None:
        super().__init__()
        self.path_or_name = path_or_name
        self.max_bytes = max(1024 * 1024, int(max_bytes or DEFAULT_MAX_BYTES))
        self.label = label
        self._day = ""
        self._fh: IO[str] | None = None
        self._written = 0
        self._seq = 0

    def _open_next(self) -> None:
        if self._fh is not None:
            self._fh.close()
        self._seq += 1
        self._day = datetime.now().strftime("%Y-%m-%d")
        path = new_log_segment_path(self.path_or_name, suffix=f"part{self._seq:03d}")
        self._fh = open(path, "a", encoding="utf-8", errors="replace")  # noqa: WPS515
        header = f"\n{'=' * 70}\n{self.label or logical_log_name(self.path_or_name)}  {datetime.now():%Y-%m-%d %H:%M:%S}\n{'=' * 70}\n"
        self._fh.write(header)
        self._fh.flush()
        self._written = len(header.encode("utf-8", errors="replace"))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record) + "\n"
            today = datetime.now().strftime("%Y-%m-%d")
            if self._fh is None or today != self._day or self._written + len(msg.encode("utf-8", errors="replace")) > self.max_bytes:
                self._open_next()
            assert self._fh is not None
            self._fh.write(msg)
            self._fh.flush()
            self._written += len(msg.encode("utf-8", errors="replace"))
        except Exception:
            self.handleError(record)

    def close(self) -> None:
        try:
            if self._fh is not None:
                self._fh.close()
                self._fh = None
        finally:
            super().close()


def _is_successful_qdrant_http_request(record: logging.LogRecord) -> bool:
    message = record.getMessage()
    if not message.startswith("HTTP Request:"):
        return False
    if ":6333/" not in message and "localhost:6333" not in message and "127.0.0.1:6333" not in message:
        return False
    return bool(_QDRANT_HTTP_OK_RE.search(message))


class QdrantHttpNoiseFilter(logging.Filter):
    """Drop successful Qdrant client request noise from persisted app logs."""

    def filter(self, record: logging.LogRecord) -> bool:
        return not _is_successful_qdrant_http_request(record)


def build_log_handler(path_or_name: str | Path, *, max_bytes: int = DEFAULT_MAX_BYTES, label: str = "") -> logging.Handler:
    handler = SizeDateLogHandler(path_or_name, max_bytes=max_bytes, label=label)
    handler.setFormatter(RedactingFormatter("%(asctime)s - %(levelname)s - %(message)s"))
    handler.addFilter(QdrantHttpNoiseFilter())
    setattr(handler, "_rag_log_history_name", logical_log_name(path_or_name))
    return handler


def install_env_log_handler(*, logger: logging.Logger | None = None) -> bool:
    """Install a segmented handler when RAG_LOG_HISTORY_NAME is present."""
    name = os.environ.get("RAG_LOG_HISTORY_NAME", "").strip()
    if not name:
        return False
    target = logger or logging.getLogger()
    logical = logical_log_name(name)
    for handler in target.handlers:
        if getattr(handler, "_rag_log_history_name", "") == logical:
            return True
    max_bytes = int(os.environ.get("RAG_LOG_MAX_BYTES", "") or DEFAULT_MAX_BYTES)
    target.addHandler(build_log_handler(name, max_bytes=max_bytes, label=os.environ.get("RAG_LOG_LABEL", logical)))
    return True


def last_error_from_history(path_or_name: str | Path, *, max_lines: int = 120, include_fallback: bool = True) -> str:
    patterns: Sequence[str] = ("Traceback", "ERROR", "Error", "Exception", "OperationalError", "ProxyError")

    def _safe_line(value: str) -> str:
        return re.sub(r"\s+", " ", redact_sensitive_text(value)).strip()[:220]

    fallback = ""
    for path in reversed(list_log_segments(path_or_name)):
        text = read_file_tail(path, max_chars=300_000)
        if not text:
            continue
        tail = text.splitlines()[-max(1, int(max_lines)) :]
        for line in reversed(tail):
            if any(pattern in line for pattern in patterns):
                return _safe_line(line)
        if not fallback:
            for line in reversed(tail):
                value = line.strip()
                if value:
                    fallback = _safe_line(value)
                    break
        if fallback and include_fallback:
            return fallback
    return ""


def iter_history_texts(path_or_name: str | Path, *, newest_first: bool = False, max_chars_per_file: int = 4_000_000) -> Iterable[str]:
    paths = list_log_segments(path_or_name)
    if newest_first:
        paths = list(reversed(paths))
    for path in paths:
        text = read_file_tail(path, max_chars=max_chars_per_file)
        if text:
            yield text
