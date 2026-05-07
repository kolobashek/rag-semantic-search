"""
helpers.py — File, search, format, and explorer utility functions.

Depends on: .system, .state, core modules, nicegui.
Imported by: api.py, nice_app.py.
"""

from __future__ import annotations

import html
import re
import sqlite3
import subprocess
import time
from datetime import datetime
from pathlib import Path, PureWindowsPath
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from nicegui import ui

from rag_catalog.core.cloud_drive import CloudDriveService
from rag_catalog.core.index_state_db import IndexStateDB
from rag_catalog.core.rag_core import RAGSearcher

from .state import (
    PageState,
    _get_auth_db,
    _get_telemetry,
    _log_app_event,
    _username,
)
from .system import _STAGE_LABELS, _telemetry_db_path

# ─────────────────────────── constants ──────────────────────────────────────

_CADENCE_LABELS: Dict[str, str] = {
    "hourly": "Каждый час",
    "daily": "Ежедневно",
    "weekly": "Еженедельно",
}
_DAY_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
_DAY_RU = {"Mon": "Пн", "Tue": "Вт", "Wed": "Ср", "Thu": "Чт", "Fri": "Пт", "Sat": "Сб", "Sun": "Вс"}

FILE_PREVIEW_EXTENSIONS = {".txt", ".log", ".csv", ".json", ".md", ".py", ".ps1", ".xml", ".html", ".css"}
INLINE_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".svg"}
OFFICE_PREVIEW_EXTENSIONS = {".docx", ".xlsx", ".xls", ".rtf"}
PAGE_SIZE = 80
SYSTEM_FILE_EXTENSIONS = {
    ".dll",
    ".exe",
    ".msi",
    ".sys",
    ".inf",
    ".cat",
    ".pnf",
    ".bak",
    ".tmp",
    ".temp",
    ".dat",
    ".db",
    ".db3",
    ".sqlite",
    ".sqlite-journal",
    ".db-shm",
    ".db-wal",
    ".lock",
    ".cfg",
    ".ini",
    ".config",
    ".lnk",
    ".chm",
    ".toc",
    ".cldbin",
}


# ─────────────────────────── URL / path helpers ─────────────────────────────

def _file_url(full_path: str) -> str:
    try:
        p = PureWindowsPath(full_path)
        if not p.parts:
            return ""
        drive = p.drive
        encoded = "/".join(quote(part, safe="") for part in p.parts[1:])
        return "file:///" + drive + "/" + encoded
    except Exception:
        return ""


def _folder_url(full_path: str) -> str:
    try:
        p = PureWindowsPath(full_path).parent
        if not p.parts:
            return ""
        drive = p.drive
        encoded = "/".join(quote(part, safe="") for part in p.parts[1:])
        return "file:///" + drive + "/" + encoded
    except Exception:
        return ""


def _viewer_file_url(full_path: str) -> str:
    value = str(full_path or "").strip()
    if not value:
        return ""
    return f"/api/view-file?path={quote(value, safe='')}"


def _schedule_display_label(sched: Dict[str, Any]) -> str:
    label = str(sched.get("label") or "").strip()
    if label:
        return label
    stage_label = _STAGE_LABELS.get(str(sched.get("stage") or "all"), str(sched.get("stage") or "Все этапы"))
    cadence_label = _CADENCE_LABELS.get(str(sched.get("cadence") or "daily"), str(sched.get("cadence") or "ежедневно"))
    return f"{stage_label} · {cadence_label}"


def _resolve_catalog_file(cfg: Dict[str, Any], raw_path: str) -> Optional[Path]:
    try:
        catalog = Path(str(cfg.get("catalog_path") or "")).resolve()
        if not catalog.exists() or not catalog.is_dir():
            return None
        candidate = Path(str(raw_path or "")).resolve()
        candidate.relative_to(catalog)
    except Exception:
        return None
    if not candidate.exists() or not candidate.is_file():
        return None
    return candidate


# ─────────────────────────── file preview ───────────────────────────────────

def _preview_file(path: Path, limit: int = 6000) -> str:
    if not path.exists() or not path.is_file():
        return "Файл недоступен."
    if path.suffix.lower() not in FILE_PREVIEW_EXTENSIONS:
        return "Для этого типа файла доступно открытие или скачивание."
    try:
        return path.read_text(encoding="utf-8", errors="replace")[:limit]
    except Exception as exc:
        return f"Не удалось прочитать файл: {exc}"


def _preview_office_file(path: Path, limit: int = 12000) -> str:
    ext = path.suffix.lower()
    if ext == ".docx":
        try:
            from docx import Document  # noqa: PLC0415

            doc = Document(path)
            parts: List[str] = [p.text for p in doc.paragraphs if p.text]
            for table in doc.tables:
                for row in table.rows:
                    row_cells = [str(cell.text or "").strip() for cell in row.cells]
                    line = " | ".join(item for item in row_cells if item)
                    if line:
                        parts.append(line)
            return "\n".join(parts)[:limit] or "Документ пуст."
        except Exception as exc:
            return f"Не удалось прочитать DOCX: {exc}"
    if ext == ".xlsx":
        try:
            from openpyxl import load_workbook  # noqa: PLC0415

            wb = load_workbook(path, read_only=True, data_only=True)
            parts: List[str] = []
            for sheet_name in wb.sheetnames:
                ws = wb[sheet_name]
                parts.append(f"Лист: {sheet_name}")
                for row in ws.iter_rows(values_only=True):
                    row_text = " | ".join(str(c) if c is not None else "" for c in row)
                    if row_text.strip():
                        parts.append(row_text)
                    if len("\n".join(parts)) >= limit:
                        return "\n".join(parts)[:limit]
            return "\n".join(parts)[:limit] or "Таблица пуста."
        except Exception as exc:
            return f"Не удалось прочитать XLSX: {exc}"
    if ext == ".xls":
        try:
            import xlrd  # type: ignore  # noqa: PLC0415

            book = xlrd.open_workbook(str(path))
            parts = []
            for sheet in book.sheets():
                parts.append(f"Лист: {sheet.name}")
                for row_idx in range(sheet.nrows):
                    row = sheet.row_values(row_idx)
                    row_text = " | ".join(str(v) if v not in ("", None) else "" for v in row)
                    if row_text.strip():
                        parts.append(row_text)
                    if len("\n".join(parts)) >= limit:
                        return "\n".join(parts)[:limit]
            return "\n".join(parts)[:limit] or "Таблица пуста."
        except Exception as exc:
            return f"Не удалось прочитать XLS: {exc}"
    if ext == ".rtf":
        try:
            raw = path.read_text(encoding="utf-8", errors="replace")
            no_ctrl = re.sub(r"\\[a-z]+\d* ?", " ", raw)
            no_braces = re.sub(r"[{}]", "", no_ctrl)
            clean = re.sub(r"\s+", " ", no_braces).strip()
            return clean[:limit] or "Документ пуст."
        except Exception as exc:
            return f"Не удалось прочитать RTF: {exc}"
    return "Для этого типа файла доступно открытие или скачивание."


# ─────────────────────────── DB query helper ────────────────────────────────

def _db_query_dicts(db_path: Path, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(query, params or ())
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []
    finally:
        conn.close()


# ─────────────────────────── log readers ────────────────────────────────────

def _read_log_tail(path: Path, *, max_chars: int = 12000) -> str:
    try:
        if not path.exists():
            return "Лог-файл не найден."
        text = path.read_text(encoding="utf-8", errors="replace")
        if not text:
            return "Лог-файл пуст."
        return text[-max_chars:]
    except Exception as exc:
        return f"Не удалось прочитать лог: {exc}"


def _read_log_tail_lines(path: Path, *, max_lines: int = 200, max_chars: int = 200_000) -> str:
    try:
        if not path.exists():
            return "Лог-файл не найден."
        text = path.read_text(encoding="utf-8", errors="replace")
        if not text:
            return "Лог-файл пуст."
        lines = text.splitlines()
        tail = "\n".join(lines[-max(1, int(max_lines)):])
        if len(tail) > max_chars:
            tail = tail[-max_chars:]
        return tail
    except Exception as exc:
        return f"Не удалось прочитать лог: {exc}"


def _filter_log_text(text: str, level: str) -> str:
    level_key = str(level or "all").strip().lower()
    if level_key in {"", "all"}:
        return text
    token = f" - {level_key.upper()} - "
    lines = [line for line in str(text or "").splitlines() if token in line.upper()]
    return "\n".join(lines) if lines else "Записей для выбранного уровня не найдено."


# ─────────────────────────── query history helpers ──────────────────────────

def _dedupe_queries(values: List[str], limit: int = 12) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        query = re.sub(r"\s+", " ", str(value or "")).strip()
        key = query.lower()
        if not query or key in seen:
            continue
        seen.add(key)
        out.append(query)
        if len(out) >= limit:
            break
    return out


def _my_recent_queries(cfg: Dict[str, Any], username: str = "", limit: int = 12) -> List[str]:
    rows = _db_query_dicts(
        _telemetry_db_path(cfg),
        """
        SELECT COALESCE(NULLIF(query_original, ''), query) AS query
        FROM search_logs
        WHERE query <> '' AND lower(username) = ?
        ORDER BY id DESC
        LIMIT 80
        """,
        (username.strip().lower(),),
    )
    return _dedupe_queries([str(row.get("query") or "") for row in rows], limit=limit)


def _popular_queries(cfg: Dict[str, Any], exclude_username: str = "", limit: int = 10) -> List[str]:
    rows = _db_query_dicts(
        _telemetry_db_path(cfg),
        """
        SELECT COALESCE(NULLIF(query_original, ''), query) AS query, COUNT(*) AS cnt
        FROM search_logs
        WHERE query <> '' AND lower(username) != ?
        GROUP BY lower(COALESCE(NULLIF(query_original, ''), query))
        ORDER BY cnt DESC
        LIMIT 40
        """,
        (exclude_username.strip().lower(),),
    )
    return _dedupe_queries([str(row.get("query") or "") for row in rows], limit=limit)


def _search_suggestions(state: PageState, typed: str = "") -> List[str]:
    username = _username(state)
    personal = _dedupe_queries([*state.history, *_my_recent_queries(state.cfg, username, limit=12)], limit=24)
    needle = typed.strip().lower()
    if not needle:
        return personal[:12]
    starts = [item for item in personal if item.lower().startswith(needle)]
    contains = [item for item in personal if needle in item.lower() and item not in starts]
    return [*starts, *contains][:12]


def _telegram_deeplink(bot_link: str, purpose: str, token: str) -> str:
    base = str(bot_link or "").strip()
    value = str(token or "").strip()
    if not base or not value:
        return ""
    joiner = "&" if "?" in base else "?"
    return f"{base}{joiner}start={purpose}_{quote(value, safe='')}"


def _remember_query(state: PageState, query: str) -> None:
    clean = re.sub(r"\s+", " ", str(query or "")).strip()
    if clean:
        state.history = _dedupe_queries([clean, *state.history], limit=24)


# ─────────────────────────── search runners ─────────────────────────────────

def _normalize_search_results(results: Any) -> List[Dict[str, Any]]:
    if results is None:
        return []
    if not isinstance(results, list):
        return []
    return [item for item in results if isinstance(item, dict)]


def _run_catalog_search(
    searcher: RAGSearcher,
    *,
    query: str,
    query_original: str,
    query_used: str,
    limit: int,
    file_type: Optional[str],
    content_only: bool,
    title_only: bool,
    username: str = "",
) -> List[Dict[str, Any]]:
    results = _normalize_search_results(
        searcher.search(
            query,
            limit=limit,
            file_type=file_type,
            content_only=content_only,
            title_only=title_only,
            source="nicegui",
            username=username,
            query_original=query_original,
        )
    )
    if results or content_only or title_only:
        return results

    try:
        fallback = searcher._lexical_catalog_search(  # noqa: SLF001
            query=query_used,
            limit=max(limit, 10),
            file_type=file_type,
            content_only=False,
            title_only=title_only,
        )
    except Exception:
        return results
    return _normalize_search_results(fallback)[:limit]


def _run_quick_name_search(
    searcher: RAGSearcher,
    *,
    query: str,
    limit: int,
    file_type: Optional[str],
) -> List[Dict[str, Any]]:
    quick = _normalize_search_results(
        searcher._lexical_catalog_search(  # noqa: SLF001
            query=query,
            limit=max(limit * 3, 60),
            file_type=file_type,
            content_only=False,
            title_only=True,
        )
    )
    needle = re.sub(r"\s+", " ", str(query or "")).strip().lower().replace("ё", "е")

    def sort_key(item: Dict[str, Any]) -> tuple:
        name = str(item.get("filename") or "").lower().replace("ё", "е")
        path = str(item.get("path") or "").lower().replace("ё", "е")
        exact_name = 1 if needle and needle in name else 0
        exact_path = 1 if needle and needle in path else 0
        score = float(item.get("score") or 0.0)
        return (exact_name, exact_path, score, -len(path))

    quick.sort(key=sort_key, reverse=True)
    return quick[:limit]


def _count_exact_name_matches(query: str, results: List[Dict[str, Any]]) -> int:
    needle = re.sub(r"\s+", " ", str(query or "")).strip().lower().replace("ё", "е")
    if not needle:
        return 0
    count = 0
    for item in results:
        name = str(item.get("filename") or "").lower().replace("ё", "е")
        path = str(item.get("path") or "").lower().replace("ё", "е")
        if needle in name or needle in path:
            count += 1
    return count


def _merge_search_results(
    primary: List[Dict[str, Any]],
    secondary: List[Dict[str, Any]],
    *,
    limit: int,
) -> List[Dict[str, Any]]:
    merged_by_key: Dict[str, Dict[str, Any]] = {}

    def key_of(item: Dict[str, Any]) -> str:
        return f"{item.get('full_path')}::{item.get('chunk_index')}::{item.get('type')}"

    def rank_key(item: Dict[str, Any]) -> tuple:
        rank_score = float(item.get("rank_score", item.get("score") or 0) or 0)
        score = float(item.get("score") or 0)
        path = str(item.get("path") or item.get("full_path") or "")
        return (rank_score, score, -len(path))

    for item in [*primary, *secondary]:
        key = key_of(item)
        existing = merged_by_key.get(key)
        if existing is None or rank_key(item) > rank_key(existing):
            merged_by_key[key] = item

    ranked = sorted(merged_by_key.values(), key=rank_key, reverse=True)
    return ranked[:limit]


# ─────────────────────────── formatters ─────────────────────────────────────

def _format_file_size(size_b: int) -> str:
    if size_b >= 1_048_576:
        return f"{size_b / 1_048_576:.1f} МБ"
    if size_b >= 1024:
        return f"{size_b / 1024:.1f} КБ"
    return f"{size_b} Б"


def _clean_text(value: Any) -> str:
    raw = str(value or "")
    raw = re.sub(r"<[^>]{1,200}>", " ", raw)
    raw = html.unescape(raw)
    raw = re.sub(r"[ \t]+", " ", raw)
    raw = re.sub(r"\s*\n\s*", "\n", raw)
    return raw.strip()


def _format_bytes(value: Any) -> str:
    try:
        size = float(value or 0)
    except (TypeError, ValueError):
        size = 0.0
    units = ["Б", "КБ", "МБ", "ГБ", "ТБ"]
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024
        idx += 1
    if idx == 0:
        return f"{int(size)} {units[idx]}"
    return f"{size:.1f} {units[idx]}"


def _format_duration_seconds(value: Any) -> str:
    try:
        seconds = int(float(value or 0))
    except (TypeError, ValueError):
        seconds = 0
    if seconds <= 0:
        return "0 сек"
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours} ч {minutes:02d} мин"
    if minutes:
        return f"{minutes} мин {secs:02d} сек"
    return f"{secs} сек"


def _format_relative_time(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "не запускался"
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        try:
            dt = datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return raw[:16]
    if dt.tzinfo is None:
        now = datetime.now()
    else:
        now = datetime.now(dt.tzinfo)
    seconds = max(0, int((now - dt).total_seconds()))
    if seconds < 60:
        return "только что"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} мин назад"
    hours = minutes // 60
    if hours < 24:
        return f"{hours} ч назад"
    days = hours // 24
    if days < 30:
        return f"{days} дн назад"
    months = days // 30
    if months < 12:
        return f"{months} мес назад"
    years = months // 12
    return f"{years} г назад"


def _duration_between(start: Any, finish: Any = None) -> int:
    start_s = str(start or "")
    finish_s = str(finish or "")
    if not start_s:
        return 0
    try:
        start_dt = datetime.fromisoformat(start_s)
        finish_dt = datetime.fromisoformat(finish_s) if finish_s else datetime.now(start_dt.tzinfo)
        return max(0, int((finish_dt - start_dt).total_seconds()))
    except ValueError:
        return 0


# ─────────────────────────── file / directory helpers ───────────────────────

def _directory_children(path: str, limit: int = 60) -> Dict[str, Any]:
    p = Path(path)
    out: Dict[str, Any] = {"exists": p.exists(), "is_dir": p.is_dir(), "dirs": [], "files": []}
    if not p.exists() or not p.is_dir():
        return out
    try:
        entries = sorted(
            [x for x in p.iterdir() if not x.name.startswith(".") and not x.name.startswith("~$")],
            key=lambda x: (not x.is_dir(), x.name.lower()),
        )
    except Exception as exc:
        out["error"] = str(exc)
        return out
    for child in entries[:limit]:
        item = {"name": child.name, "path": str(child)}
        if child.is_dir():
            out["dirs"].append(item)
        elif child.is_file():
            try:
                item["size"] = _format_file_size(child.stat().st_size)
            except Exception:
                item["size"] = ""
            out["files"].append(item)
    out["truncated"] = len(entries) > limit
    return out


def _open_os_path(path: str) -> None:
    value = str(path or "").strip()
    if not value:
        return
    try:
        subprocess.Popen(["explorer", value])
    except Exception as exc:
        ui.notify(f"Не удалось открыть проводник ОС: {exc}", type="negative")


def _within_catalog(root: Path, candidate: Path) -> bool:
    try:
        candidate.resolve().relative_to(root.resolve())
        return True
    except Exception:
        return False


def _safe_explorer_path(state: PageState) -> Path:
    root = Path(str(state.cfg.get("catalog_path") or ""))
    if state.explorer_path:
        candidate = Path(state.explorer_path)
        if candidate.exists() and _within_catalog(root, candidate):
            return candidate
    state.explorer_path = str(root)
    return root


# ─────────────────────────── Cloud Drive helpers ─────────────────────────────

def _cd_get_service(cfg: Dict[str, Any]) -> Optional["CloudDriveService"]:
    try:
        if not cfg.get("cloud_drive_enabled") or not str(cfg.get("cloud_drive_db_path") or "").strip():
            return None
        return CloudDriveService.from_config(cfg)
    except Exception:
        return None


def _cd_list_children(
    service: "CloudDriveService", cd_path: str
) -> "tuple[list, list]":
    try:
        if cd_path:
            folder = service.registry.get_folder_by_path(cd_path)
        else:
            folder = service.registry.get_root_folder()
        if folder is None:
            return [], []
        folders = service.registry.list_child_folders(folder.id)
        files = service.registry.list_files_in_folder(folder.id)
        return folders, files
    except Exception:
        return [], []


def _cd_breadcrumb_chain(service: "CloudDriveService", cd_path: str) -> "list":
    if not cd_path:
        root = service.registry.get_root_folder()
        return [root] if root else []
    parts: list = []
    segments = cd_path.replace("\\", "/").split("/")
    built = ""
    for seg in segments:
        if not seg:
            continue
        built = (built + "/" + seg).lstrip("/")
        folder = service.registry.get_folder_by_path(built)
        if folder is not None:
            parts.append(folder)
    return parts


def _cd_file_size(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} Б"
    if size_bytes < 1024 ** 2:
        return f"{size_bytes / 1024:.1f} КБ"
    if size_bytes < 1024 ** 3:
        return f"{size_bytes / 1024 ** 2:.1f} МБ"
    return f"{size_bytes / 1024 ** 3:.2f} ГБ"


def _cd_search_by_name(
    registry: "Any",
    query: str,
    *,
    max_folders: int = 5,
    max_files: int = 5,
) -> "tuple[list, list]":
    pattern = f"%{query.lower()}%"
    try:
        with registry._connect() as conn:
            folder_rows = conn.execute(
                "SELECT * FROM cloud_folders WHERE lower(name) LIKE ? AND is_root=0 LIMIT ?",
                (pattern, max_folders),
            ).fetchall()
            file_rows = conn.execute(
                "SELECT * FROM cloud_files WHERE lower(name) LIKE ? AND deleted_at IS NULL LIMIT ?",
                (pattern, max_files),
            ).fetchall()
        folders = [registry._folder_from_row(r) for r in folder_rows]
        files = [registry._file_from_row(r) for r in file_rows]
        return folders, files
    except Exception:
        return [], []


def _cd_file_jobs_map(registry: "Any", file_ids: "list[str]") -> "Dict[str, Dict[str, str]]":
    """Single query: returns {file_id: {status, job_type, last_error}} for latest job per file."""
    if not file_ids:
        return {}
    try:
        placeholders = ",".join("?" for _ in file_ids)
        with registry._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT j.file_id, j.job_type, j.status, j.last_error
                FROM cloud_jobs j
                INNER JOIN (
                    SELECT file_id, MAX(created_at) AS max_ts
                    FROM cloud_jobs
                    WHERE file_id IN ({placeholders})
                      AND job_type IN ('reindex', 'cleanup')
                    GROUP BY file_id
                ) latest ON j.file_id = latest.file_id AND j.created_at = latest.max_ts
                """,
                file_ids,
            ).fetchall()
        return {
            str(row["file_id"]): {
                "status": str(row["status"] or ""),
                "job_type": str(row["job_type"] or ""),
                "last_error": str(row["last_error"] or ""),
            }
            for row in rows
        }
    except Exception:
        return {}


# ─────────────────────────── index stats / telemetry ────────────────────────

def _read_index_stats(cfg: Dict[str, Any]) -> Dict[str, Any]:
    state_file = Path(str(cfg.get("qdrant_db_path") or "")) / "index_state.db"
    out: Dict[str, Any] = {
        "found": False,
        "state_file": str(state_file),
        "total": 0,
        "total_size_bytes": 0,
        "by_ext": {},
        "by_ext_size": {},
        "by_stage": {},
    }
    if not state_file.exists():
        return out
    try:
        db = IndexStateDB(str(state_file))
        stats = db.stats()
    except Exception as exc:
        out["error"] = str(exc)
        return out
    out.update({
        "found": True,
        "total": int(stats.get("total") or 0),
        "total_size_bytes": int(stats.get("total_size_bytes") or 0),
        "by_ext": dict(stats.get("by_ext") or {}),
        "by_ext_size": dict(stats.get("by_ext_size") or {}),
        "by_stage": dict(stats.get("by_stage") or {}),
    })
    try:
        out["last_modified"] = time.strftime("%d.%m.%Y %H:%M", time.localtime(state_file.stat().st_mtime))
    except Exception:
        pass
    return out


def _read_index_telemetry(cfg: Dict[str, Any]) -> Dict[str, Any]:
    db_path = _telemetry_db_path(cfg)
    last_run = _db_query_dicts(
        db_path,
        """
        SELECT *,
               CAST((julianday(COALESCE(ts_finished, CURRENT_TIMESTAMP)) - julianday(ts_started)) * 86400 AS INTEGER) AS duration_sec
        FROM index_runs
        ORDER BY ts_started DESC
        LIMIT 1
        """,
    )
    active_runs = _db_query_dicts(
        db_path,
        """
        SELECT *,
               CAST((julianday(CURRENT_TIMESTAMP) - julianday(ts_started)) * 86400 AS INTEGER) AS duration_sec
        FROM index_runs
        WHERE status='running'
        ORDER BY ts_started DESC
        LIMIT 3
        """,
    )
    active_run_ids = [str(row.get("run_id") or "") for row in active_runs if str(row.get("run_id") or "")]
    active_stages: List[Dict[str, Any]] = []
    if active_run_ids:
        placeholders = ",".join("?" for _ in active_run_ids)
        active_stages = _db_query_dicts(
            db_path,
            f"""
            SELECT isp.*,
                   ir.status AS run_status,
                   ir.note AS run_note,
                   CAST((julianday(COALESCE(isp.ts_finished, CURRENT_TIMESTAMP)) - julianday(isp.ts_started)) * 86400 AS INTEGER) AS duration_sec
            FROM index_stage_progress AS isp
            LEFT JOIN index_runs AS ir ON ir.run_id = isp.run_id
            WHERE isp.run_id IN ({placeholders})
            ORDER BY isp.ts_started, isp.stage
            """,
            tuple(active_run_ids),
        )
    latest_stages = _db_query_dicts(
        db_path,
        """
        SELECT isp.*,
               ir.status AS run_status,
               ir.note AS run_note,
               CAST((julianday(COALESCE(isp.ts_finished, CURRENT_TIMESTAMP)) - julianday(isp.ts_started)) * 86400 AS INTEGER) AS duration_sec
        FROM index_stage_progress AS isp
        LEFT JOIN index_runs AS ir ON ir.run_id = isp.run_id
        WHERE isp.run_id=(SELECT run_id FROM index_runs ORDER BY ts_started DESC LIMIT 1)
        ORDER BY CASE isp.stage WHEN 'metadata' THEN 1 WHEN 'small' THEN 2 WHEN 'large' THEN 3 WHEN 'content' THEN 4 ELSE 9 END
        """,
    )
    stage_summary = _db_query_dicts(
        db_path,
        """
        WITH finished AS (
            SELECT stage,
                   status,
                   total_files,
                   processed_files,
                   added_files,
                   updated_files,
                   skipped_files,
                   error_files,
                   points_added,
                   ts_started,
                   ts_finished,
                   CAST((julianday(ts_finished) - julianday(ts_started)) * 86400 AS INTEGER) AS duration_sec
            FROM index_stage_progress
            WHERE ts_finished IS NOT NULL
        ),
        latest AS (
            SELECT f.*
            FROM finished f
            JOIN (
                SELECT stage, MAX(ts_started) AS ts_started
                FROM finished
                GROUP BY stage
            ) m ON m.stage=f.stage AND m.ts_started=f.ts_started
        ),
        avg_by_stage AS (
            SELECT stage,
                   COUNT(*) AS runs_count,
                   AVG(duration_sec) AS avg_duration_sec,
                   AVG(processed_files) AS avg_processed_files
            FROM finished
            GROUP BY stage
        )
        SELECT latest.stage,
               latest.status,
               latest.total_files,
               latest.processed_files,
               latest.added_files,
               latest.updated_files,
               latest.skipped_files,
               latest.error_files,
               latest.points_added,
               latest.ts_started,
               latest.ts_finished,
               latest.duration_sec AS last_duration_sec,
               avg_by_stage.runs_count,
               CAST(avg_by_stage.avg_duration_sec AS INTEGER) AS avg_duration_sec,
               CAST(avg_by_stage.avg_processed_files AS INTEGER) AS avg_processed_files
        FROM latest
        JOIN avg_by_stage ON avg_by_stage.stage=latest.stage
        ORDER BY CASE latest.stage WHEN 'metadata' THEN 1 WHEN 'small' THEN 2 WHEN 'large' THEN 3 WHEN 'content' THEN 4 ELSE 9 END
        """,
    )
    overall = _db_query_dicts(
        db_path,
        """
        SELECT COUNT(*) AS runs_count,
               CAST(AVG((julianday(ts_finished) - julianday(ts_started)) * 86400) AS INTEGER) AS avg_duration_sec,
               MAX(total_files) AS max_total_files,
               AVG(total_files) AS avg_total_files
        FROM index_runs
        WHERE ts_finished IS NOT NULL
        """,
    )
    active_ocr = _db_query_dicts(
        db_path,
        """
        SELECT *,
               CAST((julianday(CURRENT_TIMESTAMP) - julianday(ts_started)) * 86400 AS INTEGER) AS duration_sec
        FROM ocr_runs
        WHERE status='running'
        ORDER BY ts_started DESC
        LIMIT 1
        """,
    )
    last_ocr = _db_query_dicts(
        db_path,
        """
        SELECT *,
               CAST((julianday(COALESCE(ts_finished, CURRENT_TIMESTAMP)) - julianday(ts_started)) * 86400 AS INTEGER) AS duration_sec
        FROM ocr_runs
        ORDER BY ts_started DESC
        LIMIT 1
        """,
    )
    ocr_summary = _db_query_dicts(
        db_path,
        """
        SELECT COUNT(*) AS runs_count,
               CAST(AVG((julianday(ts_finished) - julianday(ts_started)) * 86400) AS INTEGER) AS avg_duration_sec,
               AVG(found_scanned) AS avg_found_scanned,
               AVG(processed_pdfs) AS avg_processed_pdfs
        FROM ocr_runs
        WHERE ts_finished IS NOT NULL
        """,
    )
    return {
        "last_run": last_run[0] if last_run else None,
        "active_runs": active_runs,
        "active_stages": active_stages,
        "latest_stages": latest_stages,
        "stage_summary": stage_summary,
        "overall": overall[0] if overall else {},
        "active_ocr": active_ocr[0] if active_ocr else None,
        "last_ocr": last_ocr[0] if last_ocr else None,
        "ocr_summary": ocr_summary[0] if ocr_summary else {},
    }


# ─────────────────────────── searcher / admin helpers ───────────────────────

def _ensure_searcher(state: PageState) -> Optional[RAGSearcher]:
    if state.searcher is not None:
        return state.searcher
    try:
        state.searcher = RAGSearcher(state.cfg)
    except Exception as exc:
        state.searcher_error = str(exc)
        return None
    if not state.searcher.connected:
        state.searcher_error = "Нет подключения к Qdrant."
    return state.searcher


def _is_admin(state: PageState) -> bool:
    return str((state.current_user or {}).get("role") or "") == "admin"


def _show_system_files(state: PageState) -> bool:
    if not _is_admin(state):
        return False
    try:
        auth = state.auth_db if state.auth_db is not None else _get_auth_db(state)
        return bool(auth.get_show_system_files_for_admin())
    except Exception:
        return False


def _is_system_file(path_or_ext: str | Path) -> bool:
    value = str(path_or_ext or "")
    ext = Path(value).suffix.lower() or value.lower()
    name = Path(value).name.lower()
    if not ext:
        return False
    if ext in SYSTEM_FILE_EXTENSIONS:
        return True
    if ext.endswith("_"):
        return True
    if ext in {".sample", ".backup", ".sav", ".asd"}:
        return True
    return name.startswith("~$") or name.endswith(".tmp")


# ─────────────────────────── result grouping / icons ────────────────────────

def _result_kind(result: Dict[str, Any]) -> str:
    if result.get("type") == "folder_metadata":
        return "Каталог"
    ext = str(result.get("extension") or "").lower()
    if ext in {".xlsx", ".xls", ".csv"}:
        return "Таблица"
    if ext == ".pdf":
        return "PDF"
    if ext in {".docx", ".doc"}:
        return "Документ"
    return "Файл"


def _result_group(result: Dict[str, Any]) -> str:
    text = " ".join(
        str(result.get(key, "") or "").lower()
        for key in ("filename", "path", "type", "text", "extension")
    )
    if str(result.get("type") or "") == "folder_metadata":
        return "Каталоги"
    if any(word in text for word in ("птс", "псм", "стс", "техпаспорт", "техническ", "паспорт транспорт", "экскаватор")):
        return "Техпаспорта ТС"
    if any(word in text for word in ("паспорт", "удостоверен")):
        return "Паспорта и удостоверения"
    if any(word in text for word in ("договор", "соглашен", "контракт")):
        return "Договоры"
    if any(word in text for word in ("счет", "счёт", "оплат", "платеж")):
        return "Счета и платежи"
    if str(result.get("extension") or "").lower() in {".xlsx", ".xls", ".csv"}:
        return "Таблицы"
    if str(result.get("extension") or "").lower() == ".pdf":
        return "PDF"
    return "Другие файлы"


def _grouped_results(results: List[Dict[str, Any]]) -> List[tuple[str, List[Dict[str, Any]]]]:
    order = [
        "Каталоги",
        "Техпаспорта ТС",
        "Паспорта и удостоверения",
        "Договоры",
        "Счета и платежи",
        "Таблицы",
        "PDF",
        "Другие файлы",
    ]
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for result in results:
        grouped.setdefault(_result_group(result), []).append(result)
    return [
        (group, sorted(grouped[group], key=lambda item: float(item.get("score") or 0), reverse=True))
        for group in order
        if group in grouped
    ]


def _file_icon_svg(path_or_ext: str, kind: str = "Файл") -> str:
    ext = Path(str(path_or_ext or "")).suffix.lower() or str(path_or_ext or "").lower()
    if kind == "Каталог":
        color, label = "#f2b84b", ""
    elif ext in {".doc", ".docx"}:
        color, label = "#2b579a", "W"
    elif ext in {".xls", ".xlsx", ".csv"}:
        color, label = "#217346", "X"
    elif ext in {".ppt", ".pptx"}:
        color, label = "#d24726", "P"
    elif ext == ".pdf":
        color, label = "#d32f2f", "PDF"
    elif ext in {".zip", ".rar", ".7z"}:
        color, label = "#d39a18", "ZIP"
    elif ext in {".jpg", ".jpeg", ".png", ".gif", ".webp"}:
        color, label = "#7b4ab8", "IMG"
    elif ext in {".txt", ".log", ".md"}:
        color, label = "#64748b", "TXT"
    elif ext in {".mp3", ".wav", ".flac"}:
        color, label = "#0e7490", "AUD"
    elif ext in {".mp4", ".avi", ".mov", ".mkv"}:
        color, label = "#7c3aed", "VID"
    elif ext in {".json", ".xml", ".html", ".htm", ".css", ".js", ".py", ".ps1", ".cmd", ".bat"}:
        color, label = "#475569", "DEV"
    elif ext in {".dwg", ".svg", ".psd", ".ico"}:
        color, label = "#0891b2", "ART"
    elif _is_system_file(path_or_ext):
        color, label = "#94a3b8", (ext.replace(".", "").upper()[:3] or "SYS")
    else:
        color, label = "#0f766e", (ext.replace(".", "").upper()[:3] or "FILE")

    if kind == "Каталог":
        svg = f"""
        <svg viewBox="0 0 56 56" aria-hidden="true">
          <path fill="#d99d2b" d="M5 15a6 6 0 0 1 6-6h12l5 6h17a6 6 0 0 1 6 6v4H5z"/>
          <path fill="{color}" d="M5 20h46v21a6 6 0 0 1-6 6H11a6 6 0 0 1-6-6z"/>
          <path fill="#ffd56d" opacity=".75" d="M8 23h40v4H8z"/>
        </svg>
        """
    else:
        font_size = "12" if len(label) <= 1 else "9"
        svg = f"""
        <svg viewBox="0 0 56 56" aria-hidden="true">
          <path fill="#ffffff" d="M12 4h22l10 10v36a4 4 0 0 1-4 4H12a4 4 0 0 1-4-4V8a4 4 0 0 1 4-4z"/>
          <path fill="#dbe4ef" d="M34 4v10h10z"/>
          <path fill="{color}" d="M6 29h37a4 4 0 0 1 4 4v12a4 4 0 0 1-4 4H6z"/>
          <text x="25" y="42" fill="#fff" text-anchor="middle" font-family="Arial, sans-serif" font-size="{font_size}" font-weight="700">{html.escape(label)}</text>
          <path fill="none" stroke="#cbd5e1" d="M12.5 4.5h21.3L43.5 14v35.5a4 4 0 0 1-4 4h-27a4 4 0 0 1-4-4v-41a4 4 0 0 1 4-4z"/>
        </svg>
        """
    icon_class = "rag-file-icon system" if kind != "Каталог" and _is_system_file(path_or_ext) else "rag-file-icon"
    return f'<span class="{icon_class}">{svg}</span>'


# ─────────────────────────── explorer helpers ────────────────────────────────

def _path_sort_key(path: Path, sort_by: str) -> Any:
    if sort_by == "По размеру":
        name = path.name.lower()
        if path.is_file():
            try:
                return (path.stat().st_size, name)
            except Exception:
                return (0, name)
        return (-1, name)
    if sort_by == "По дате":
        name = path.name.lower()
        try:
            return (path.stat().st_mtime, name)
        except Exception:
            return (0, name)
    return path.name.lower()


def _file_rows(path: Path, state: PageState) -> tuple[List[Path], List[Path], int]:
    try:
        entries = [x for x in path.iterdir() if not x.name.startswith(".") and not x.name.startswith("~$")]
    except Exception:
        return [], [], 0

    dirs = [x for x in entries if x.is_dir()]
    files = [x for x in entries if x.is_file()]
    if not _show_system_files(state):
        files = [x for x in files if not _is_system_file(x)]
    needle = state.explorer_filter.strip().lower()
    if needle:
        dirs = [x for x in dirs if needle in x.name.lower()]
        files = [x for x in files if needle in x.name.lower()]
    if state.explorer_ext != "Все":
        files = [x for x in files if x.suffix.lower() == state.explorer_ext.lower()]

    reverse = bool(state.explorer_desc)
    dirs.sort(key=lambda x: _path_sort_key(x, state.explorer_sort), reverse=reverse)
    files.sort(key=lambda x: _path_sort_key(x, state.explorer_sort), reverse=reverse)
    return dirs, files, len(files)


def _event_input_value(event: Any, fallback: Any = "") -> str:
    value = getattr(event, "value", None)
    if value is not None:
        return str(value)
    args = getattr(event, "args", None)
    if isinstance(args, dict):
        for key in ("value", "modelValue"):
            if args.get(key) is not None:
                return str(args[key])
        target = args.get("target")
        if isinstance(target, dict) and target.get("value") is not None:
            return str(target["value"])
    return str(fallback or "")


def _apply_explorer_filter_input(state: PageState, event: Any, fallback: Any = "") -> None:
    state.explorer_filter = _event_input_value(event, fallback)
    state.explorer_page = 0


# ─────────────────────────── user state persistence ─────────────────────────

def _load_user_state(state: PageState) -> None:
    username = _username(state)
    if not username:
        return
    auth_db = _get_auth_db(state)
    settings = auth_db.get_user_settings(username=username)
    explorer = settings.get("explorer") if isinstance(settings.get("explorer"), dict) else {}
    if explorer:
        state.explorer_view = str(explorer.get("view") or state.explorer_view)
        state.explorer_sort = str(explorer.get("sort") or state.explorer_sort)
        state.explorer_desc = bool(explorer.get("desc", state.explorer_desc))
        state.explorer_ext = str(explorer.get("ext") or state.explorer_ext)
    ui_settings = settings.get("ui") if isinstance(settings.get("ui"), dict) else {}
    if ui_settings:
        theme = str(ui_settings.get("theme") or "").strip().lower()
        if theme in {"light", "dark"}:
            state.theme = theme
        if "ai_search_expand" in ui_settings:
            state.ai_search_expand = bool(ui_settings.get("ai_search_expand"))
    state.favorites = auth_db.list_favorites(username=username)
    if not state.history:
        try:
            _get_telemetry(state)
        except Exception:
            pass
        state.history = _my_recent_queries(state.cfg, username, limit=24)


def _save_ui_settings(state: PageState) -> None:
    username = _username(state)
    if not username:
        return
    auth_db = _get_auth_db(state)
    settings = auth_db.get_user_settings(username=username)
    ui_settings = settings.get("ui") if isinstance(settings.get("ui"), dict) else {}
    ui_settings["theme"] = state.theme if state.theme in {"light", "dark"} else "light"
    ui_settings["ai_search_expand"] = bool(state.ai_search_expand)
    settings["ui"] = ui_settings
    auth_db.save_user_settings(username=username, settings=settings)


def _save_explorer_settings(state: PageState) -> None:
    username = _username(state)
    if not username:
        return
    auth_db = _get_auth_db(state)
    settings = auth_db.get_user_settings(username=username)
    settings["explorer"] = {
        "view": state.explorer_view,
        "sort": state.explorer_sort,
        "desc": state.explorer_desc,
        "ext": state.explorer_ext,
    }
    auth_db.save_user_settings(username=username, settings=settings)
    _log_app_event(state, "explorer", "save_settings", details=settings["explorer"])
