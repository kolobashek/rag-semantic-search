"""NiceGUI web frontend for RAG Catalog."""

from __future__ import annotations

import argparse
import html
import json
import mimetypes
import re
import sqlite3
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path, PureWindowsPath
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from nicegui import app, events, run, ui

from rag_catalog.core.rag_core import RAGSearcher, load_config
from rag_catalog.core.telemetry_db import TelemetryDB
from rag_catalog.core.user_auth_db import UserAuthDB


PROJECT_ROOT = Path(__file__).resolve().parents[3]
APP_ICON_PATH = PROJECT_ROOT / "assets" / "brand" / "ico" / "favicon.ico"
LOGO_PATH = PROJECT_ROOT / "assets" / "brand" / "png" / "app-badge-128.png"

SEARCH_PRESETS = [
    ("Договоры", "договор поставки"),
    ("Счета", "счет на оплату"),
    ("Паспорта", "паспорт техника"),
    ("PDF", "pdf скан"),
    ("Таблицы", "реестр xlsx"),
]

FILE_PREVIEW_EXTENSIONS = {".txt", ".log", ".csv", ".json", ".md", ".py", ".ps1", ".xml", ".html", ".css"}
PAGE_SIZE = 80

if LOGO_PATH.exists():
    app.add_static_file(local_file=LOGO_PATH, url_path="/rag-logo.png")


@dataclass
class PageState:
    cfg: Dict[str, Any]
    searcher: Optional[RAGSearcher] = None
    searcher_error: str = ""
    screen: str = "search"
    query: str = ""
    file_type: Optional[str] = None
    limit: int = 10
    content_only: bool = False
    history: List[str] = field(default_factory=list)
    results: List[Dict[str, Any]] = field(default_factory=list)
    search_error: str = ""
    searched_query: str = ""
    explorer_path: Optional[str] = None
    explorer_filter: str = ""
    explorer_ext: str = "Все"
    explorer_sort: str = "По имени"
    explorer_desc: bool = False
    explorer_view: str = "Таблица"
    explorer_page: int = 0
    auth_db: Optional[UserAuthDB] = None
    current_user: Optional[Dict[str, Any]] = None
    auth_token: str = ""
    favorites: List[Dict[str, Any]] = field(default_factory=list)
    header_explorer_actions: Optional[ui.row] = None
    header_breadcrumbs: Optional[ui.row] = None
    telemetry: Optional[TelemetryDB] = None


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


def _telemetry_db_path(cfg: Dict[str, Any]) -> Path:
    explicit = str(cfg.get("telemetry_db_path") or "").strip()
    if explicit:
        return Path(explicit)
    return Path(str(cfg.get("qdrant_db_path") or "")) / "rag_telemetry.db"


def _users_db_path(cfg: Dict[str, Any]) -> Path:
    explicit = str(cfg.get("users_db_path") or "").strip()
    if explicit:
        return Path(explicit)
    return Path(str(cfg.get("qdrant_db_path") or ".")) / "rag_users.db"


def _get_auth_db(state: PageState) -> UserAuthDB:
    path = _users_db_path(state.cfg)
    if state.auth_db is None or Path(getattr(state.auth_db, "db_path", "")) != path:
        state.auth_db = UserAuthDB(str(path))
    return state.auth_db


def _refresh_current_user(state: PageState) -> None:
    if not state.current_user:
        return
    user = _get_auth_db(state).get_user(username=str(state.current_user.get("username") or ""))
    if user:
        state.current_user = user


def _username(state: PageState) -> str:
    return str((state.current_user or {}).get("username") or "").strip().lower()


def _get_telemetry(state: PageState) -> TelemetryDB:
    path = _telemetry_db_path(state.cfg)
    if state.telemetry is None or Path(getattr(state.telemetry, "db_path", "")) != path:
        state.telemetry = TelemetryDB(str(path))
    return state.telemetry


def _log_app_event(
    state: PageState,
    feature: str,
    action: str,
    *,
    ok: bool = True,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    try:
        _get_telemetry(state).log_app_event(
            username=_username(state),
            screen=state.screen,
            feature=feature,
            action=action,
            ok=ok,
            details=details or {},
        )
    except Exception:
        pass


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
    state.favorites = auth_db.list_favorites(username=username)


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


def _favorite_key(path: str) -> str:
    return str(path or "").strip().casefold()


def _is_favorite(state: PageState, path: str) -> bool:
    key = _favorite_key(path)
    return any(_favorite_key(str(item.get("path") or "")) == key for item in state.favorites)


def _favorite_type(path: Path) -> str:
    return "folder" if path.is_dir() else "file"


def _toggle_favorite(state: PageState, path: Path, *, item_type: Optional[str] = None, title: str = "") -> bool:
    username = _username(state)
    if not username:
        ui.notify("Войдите, чтобы сохранять избранное.", type="warning")
        return False
    auth_db = _get_auth_db(state)
    path_value = str(path)
    active = _is_favorite(state, path_value)
    if active:
        auth_db.remove_favorite(username=username, path=path_value)
        _log_app_event(state, "favorites", "remove", details={"path": path_value})
    else:
        auth_db.add_favorite(
            username=username,
            item_type=item_type or _favorite_type(path),
            path=path_value,
            title=title or path.name or path_value,
        )
        _log_app_event(state, "favorites", "add", details={"path": path_value, "item_type": item_type or _favorite_type(path)})
    state.favorites = auth_db.list_favorites(username=username)
    return not active


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


def _recent_search_queries(cfg: Dict[str, Any], limit: int = 10) -> List[str]:
    rows = _db_query_dicts(
        _telemetry_db_path(cfg),
        """
        SELECT query
        FROM search_logs
        WHERE query <> ''
        ORDER BY id DESC
        LIMIT 80
        """,
    )
    return _dedupe_queries([str(row.get("query") or "") for row in rows], limit=limit)


def _search_suggestions(state: PageState, typed: str = "") -> List[str]:
    base = [*state.history, *_recent_search_queries(state.cfg, limit=12), *[query for _, query in SEARCH_PRESETS]]
    suggestions = _dedupe_queries(base, limit=24)
    needle = typed.strip().lower()
    if not needle:
        return suggestions[:12]
    starts = [item for item in suggestions if item.lower().startswith(needle)]
    contains = [item for item in suggestions if needle in item.lower() and item not in starts]
    return [*starts, *contains][:12]


def _remember_query(state: PageState, query: str) -> None:
    clean = re.sub(r"\s+", " ", str(query or "")).strip()
    if clean:
        state.history = _dedupe_queries([clean, *state.history], limit=24)


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


def _preview_file(path: Path, limit: int = 6000) -> str:
    if not path.exists() or not path.is_file():
        return "Файл недоступен."
    if path.suffix.lower() not in FILE_PREVIEW_EXTENSIONS:
        return "Для этого типа файла доступно открытие или скачивание."
    try:
        return path.read_text(encoding="utf-8", errors="replace")[:limit]
    except Exception as exc:
        return f"Не удалось прочитать файл: {exc}"


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


def _read_index_stats(cfg: Dict[str, Any]) -> Dict[str, Any]:
    state_file = Path(str(cfg.get("qdrant_db_path") or "")) / "index_state.json"
    out: Dict[str, Any] = {"found": False, "state_file": str(state_file), "total": 0, "by_ext": {}}
    if not state_file.exists():
        return out
    try:
        data = json.loads(state_file.read_text(encoding="utf-8"))
        files = data.get("files", {})
    except Exception as exc:
        out["error"] = str(exc)
        return out
    by_ext: Dict[str, int] = {}
    for key in files:
        ext = Path(str(key)).suffix.lower() or "(без расширения)"
        by_ext[ext] = by_ext.get(ext, 0) + 1
    out.update({"found": True, "total": len(files), "by_ext": dict(sorted(by_ext.items(), key=lambda x: x[1], reverse=True))})
    try:
        out["last_modified"] = time.strftime("%d.%m.%Y %H:%M", time.localtime(state_file.stat().st_mtime))
    except Exception:
        pass
    return out


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
    return f'<span class="rag-file-icon">{svg}</span>'


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


def _install_css() -> None:
    ui.add_css(
        """
        :root {
          --rag-bg: #f6f8fb;
          --rag-surface: #ffffff;
          --rag-border: #d9e0ea;
          --rag-text: #17202c;
          --rag-muted: #657385;
          --rag-accent: #146c94;
          --rag-accent-2: #178a6f;
          --rag-danger: #b42318;
        }
        body { background: var(--rag-bg); color: var(--rag-text); }
        .q-page { background: var(--rag-bg); }
        .rag-header {
          background: rgba(255, 255, 255, 0.96);
          color: var(--rag-text);
          border-bottom: 1px solid var(--rag-border);
          backdrop-filter: blur(16px);
        }
        .rag-header-breadcrumbs .q-btn { min-height: 32px; padding: 0 6px; }
        .rag-header-actions .q-btn { min-width: 34px; min-height: 34px; }
        .rag-drawer {
          background: #ffffff;
          border-right: 1px solid var(--rag-border);
        }
        .rag-drawer-body {
          min-height: calc(100vh - 92px);
          display: flex;
          flex-direction: column;
        }
        .rag-drawer-bottom {
          margin-top: auto;
          padding-top: 12px;
          border-top: 1px solid var(--rag-border);
        }
        .rag-page {
          width: min(1440px, calc(100vw - 32px));
          margin: 0 auto;
          padding: 28px 0 42px;
        }
        .rag-title { font-size: clamp(26px, 4vw, 42px); font-weight: 760; line-height: 1.05; letter-spacing: 0; }
        .rag-subtitle { color: var(--rag-muted); font-size: 15px; max-width: 820px; }
        .rag-card {
          background: var(--rag-surface);
          border: 1px solid var(--rag-border);
          border-radius: 8px;
          box-shadow: 0 10px 30px rgba(23, 32, 44, 0.06);
        }
        .rag-search-shell { position: relative; z-index: 5; }
        .rag-search-box {
          background: var(--rag-surface);
          border: 1px solid var(--rag-border);
          border-radius: 8px;
          box-shadow: 0 18px 42px rgba(23, 32, 44, 0.10);
        }
        .rag-suggest {
          position: absolute;
          left: 0;
          right: 0;
          top: calc(100% + 8px);
          background: #ffffff;
          border: 1px solid var(--rag-border);
          border-radius: 8px;
          box-shadow: 0 18px 48px rgba(23, 32, 44, 0.16);
          overflow: hidden;
          z-index: 30;
        }
        .rag-result {
          background: #ffffff;
          border: 1px solid var(--rag-border);
          border-radius: 8px;
          padding: 16px;
          box-shadow: 0 8px 24px rgba(23, 32, 44, 0.05);
          width: 100%;
          box-sizing: border-box;
        }
        .rag-meta { color: var(--rag-muted); font-size: 13px; }
        .rag-chip {
          display: inline-flex;
          align-items: center;
          min-height: 28px;
          padding: 0 10px;
          border: 1px solid var(--rag-border);
          border-radius: 8px;
          color: var(--rag-muted);
          background: #f8fafc;
          font-size: 13px;
        }
        .rag-path {
          word-break: break-word;
          overflow-wrap: anywhere;
          color: var(--rag-muted);
          font-size: 13px;
        }
        .rag-actions { display: flex; flex-wrap: wrap; gap: 8px; }
        .rag-nav-button { justify-content: flex-start; border-radius: 8px; text-align: left; }
        .rag-nav-button .q-btn__content { justify-content: flex-start; width: 100%; text-align: left; }
        .rag-nav-button .q-icon { margin-right: 10px; }
        .rag-group-panel {
          width: 100%;
          border: 1px solid var(--rag-border);
          border-radius: 8px;
          background: #ffffff;
          overflow: hidden;
        }
        .rag-file-icon {
          display: inline-flex;
          width: 42px;
          height: 42px;
          flex: 0 0 42px;
        }
        .rag-file-icon svg { width: 42px; height: 42px; display: block; }
        .rag-explorer-grid {
          display: grid;
          grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
          gap: 10px;
        }
        .rag-explorer-grid.medium { grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); }
        .rag-explorer-grid.small { grid-template-columns: repeat(auto-fill, minmax(96px, 1fr)); }
        .rag-explorer-item {
          width: 100%;
          min-width: 0;
          background: #ffffff;
          border: 1px solid transparent;
          border-radius: 8px;
          color: var(--rag-text);
        }
        .rag-explorer-item:hover {
          background: #eef6fb;
          border-color: #bdd7e9;
        }
        .rag-explorer-item { position: relative; }
        .rag-favorite-star {
          opacity: 0;
          color: rgba(0, 0, 0, 0.45);
          transition: opacity .12s ease, color .12s ease, transform .12s ease;
        }
        .rag-explorer-item:hover .rag-favorite-star,
        .rag-favorite-star.active {
          opacity: 1;
        }
        .rag-favorite-star:hover {
          color: #d89b00;
          transform: scale(1.08);
        }
        .rag-favorite-star.active {
          color: #f6b700;
        }
        .rag-favorite-star.header {
          opacity: .65;
        }
        .rag-favorite-star.header:hover {
          opacity: 1;
          color: #d89b00;
        }
        .rag-bookmarks {
          display: flex;
          width: 100%;
          gap: 8px;
          overflow-x: auto;
          padding: 8px 0;
          align-items: center;
          flex-wrap: nowrap;
        }
        .rag-bookmark {
          position: relative;
          flex: 0 0 auto;
          width: 220px;
          min-width: 160px;
          height: 42px;
          border: 1px solid var(--rag-border);
          background: #ffffff;
          border-radius: 8px;
          overflow: hidden;
          transition: background .12s ease, border-color .12s ease, box-shadow .12s ease;
        }
        .rag-bookmark:hover {
          background: #eef6fb;
          border-color: #bdd7e9;
        }
        .rag-bookmark-main {
          position: absolute;
          inset: 0 36px 0 0;
          display: flex;
          align-items: center;
          min-width: 0;
        }
        .rag-bookmark:hover .rag-bookmark-main {
          box-shadow: 18px 0 24px rgba(23, 32, 44, 0.12);
        }
        .rag-bookmark-remove {
          position: absolute;
          right: 0;
          top: 0;
          width: 36px;
          height: 100%;
          display: flex;
          align-items: center;
          justify-content: center;
          opacity: 0;
          background: #ffffff;
          border-left: 1px solid var(--rag-border);
          color: #7b8794;
          transition: opacity .12s ease, color .12s ease, background .12s ease;
        }
        .rag-bookmark:hover .rag-bookmark-remove {
          opacity: 1;
        }
        .rag-bookmark-remove:hover {
          background: #fff1f1;
          color: #b42318;
        }
        .rag-bookmark .q-btn {
          min-width: 0;
          width: 100%;
          height: 100%;
          padding-right: 4px;
        }
        .rag-bookmark .q-btn__content {
          min-width: 0;
          flex-wrap: nowrap;
          overflow: hidden;
        }
        .rag-bookmark .block {
          min-width: 0;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
        .rag-bookmark-more {
          flex: 0 0 auto;
          width: 42px;
          height: 42px;
        }
        .rag-context-menu {
          position: fixed;
          z-index: 10000;
          min-width: 220px;
          background: #ffffff;
          border: 1px solid var(--rag-border);
          border-radius: 8px;
          box-shadow: 0 18px 48px rgba(23, 32, 44, 0.18);
          padding: 6px;
          display: none;
        }
        .rag-context-menu button {
          display: block;
          width: 100%;
          padding: 8px 10px;
          border: 0;
          background: transparent;
          text-align: left;
          border-radius: 8px;
          color: var(--rag-text);
          cursor: pointer;
        }
        .rag-context-menu button:hover { background: #eef6fb; }
        .rag-favorites-dialog-row {
          display: grid;
          grid-template-columns: auto minmax(0, 1fr) auto;
          gap: 8px;
          align-items: center;
          width: 100%;
        }
        .rag-explorer-name {
          width: 100%;
          min-width: 0;
          overflow-wrap: anywhere;
          word-break: break-word;
          line-height: 1.2;
        }
        .rag-explorer-list {
          display: grid;
          grid-template-columns: 1fr;
          gap: 4px;
        }
        .rag-code {
          white-space: pre-wrap;
          word-break: break-word;
          font-family: ui-monospace, SFMono-Regular, Consolas, monospace;
          font-size: 12px;
          background: #f8fafc;
          border: 1px solid var(--rag-border);
          border-radius: 8px;
          padding: 12px;
        }
        @media (max-width: 760px) {
          .rag-page { width: calc(100vw - 20px); padding-top: 18px; }
          .rag-title { font-size: 28px; }
          .rag-actions .q-btn { width: 100%; }
          .rag-search-box { box-shadow: 0 12px 28px rgba(23, 32, 44, 0.10); }
        }
        """
    )
    ui.add_body_html(
        """
        <div id="rag-global-context-menu" class="rag-context-menu" role="menu"></div>
        <script>
        (() => {
          if (window.__ragContextMenuInstalled) return;
          window.__ragContextMenuInstalled = true;
          const menu = () => document.getElementById('rag-global-context-menu');
          const hide = () => { const m = menu(); if (m) m.style.display = 'none'; };
          const show = (event) => {
            const root = event.target.closest('.q-layout');
            if (!root) return;
            event.preventDefault();
            const m = menu();
            if (!m) return;
            const path = location.pathname;
            const buttons = [
              ['Обновить экран', () => location.reload()],
              ['Скопировать адрес экрана', () => navigator.clipboard && navigator.clipboard.writeText(location.href)],
              ['Настройки', () => { location.href = '/settings'; }]
            ];
            m.innerHTML = '';
            buttons.forEach(([label, action]) => {
              const b = document.createElement('button');
              b.textContent = label;
              b.onclick = () => { hide(); action(); };
              m.appendChild(b);
            });
            m.style.left = Math.min(event.clientX, window.innerWidth - 240) + 'px';
            m.style.top = Math.min(event.clientY, window.innerHeight - 160) + 'px';
            m.style.display = 'block';
          };
          document.addEventListener('contextmenu', show);
          document.addEventListener('click', hide);
          document.addEventListener('scroll', hide, true);
          document.addEventListener('keydown', (e) => { if (e.key === 'Escape') hide(); });
        })();
        </script>
        """
    )


def _build_page(initial_screen: str = "search") -> None:
    state = PageState(cfg=load_config())
    state.screen = initial_screen
    state.explorer_path = str(Path(str(state.cfg.get("catalog_path") or "")))
    _install_css()
    try:
        stored_token = str(app.storage.user.get("auth_token") or "")
        if stored_token:
            state.auth_token = stored_token
            state.current_user = _get_auth_db(state).get_user_by_session(stored_token)
            if state.current_user:
                _load_user_state(state)
                _get_auth_db(state).log_auth_event(username=_username(state), event_type="session_restore", ok=True)
    except Exception:
        pass

    with ui.header(fixed=True, elevated=False).classes("rag-header h-16 px-3 md:px-5"):
        ui.button(icon="menu", on_click=lambda: drawer.toggle(), color=None).props("flat round").classes("text-slate-700")
        ui.image("/rag-logo.png").classes("w-9 h-9 rounded") if LOGO_PATH.exists() else ui.icon("manage_search").classes("text-3xl")
        ui.label("RAG Каталог").classes("font-semibold text-lg")
        ui.icon("chevron_right").classes("text-slate-400")
        header_title = ui.label("").classes("font-semibold text-base text-slate-700")
        header_breadcrumbs = ui.row().classes("rag-header-breadcrumbs items-center gap-1 hidden md:flex")
        header_actions = ui.row().classes("rag-header-actions items-center gap-1")
        state.header_breadcrumbs = header_breadcrumbs
        state.header_explorer_actions = header_actions
        ui.space()
        status_text = "Qdrant готов" if _ensure_searcher(state) and state.searcher and state.searcher.connected else "Qdrant недоступен"
        ui.label(status_text).classes("hidden sm:block rag-chip")

    with ui.left_drawer(value=True, fixed=True, bordered=True).classes("rag-drawer w-80 p-4") as drawer:
        with ui.column().classes("rag-drawer-body w-full"):
            ui.label("Меню").classes("text-xl font-semibold mb-2")
            nav_area = ui.column().classes("w-full gap-2")
            settings_area = ui.column().classes("w-full gap-3 mt-4")
            bottom_nav_area = ui.column().classes("rag-drawer-bottom w-full gap-2")

    with ui.column().classes("rag-page gap-5"):
        content = ui.column().classes("w-full gap-5")

    def set_screen(screen: str) -> None:
        state.screen = screen
        ui.run_javascript(f"history.pushState(null, '', '/{screen}')")
        _log_app_event(state, "navigation", "open_screen", details={"screen": screen})
        render()

    def go_explorer(path: str) -> None:
        value = str(path or "").strip()
        if value:
            p = Path(value)
            state.explorer_path = str(p.parent if p.is_file() else p)
            state.explorer_page = 0
        set_screen("explorer")

    def update_nav() -> None:
        nav_area.clear()
        with nav_area:
            for screen, label, icon in [
                ("search", "Поиск", "search"),
                ("explorer", "Проводник", "folder"),
                ("index", "Индекс", "analytics"),
                ("telegram", "Telegram", "send"),
            ]:
                color = "primary" if state.screen == screen else None
                ui.button(label, icon=icon, on_click=lambda s=screen: set_screen(s), color=color).props("flat align=left no-caps").classes("rag-nav-button w-full")
            if str((state.current_user or {}).get("role") or "") == "admin":
                color = "primary" if state.screen == "stats" else None
                ui.button("Статистика", icon="query_stats", on_click=lambda: set_screen("stats"), color=color).props("flat align=left no-caps").classes("rag-nav-button w-full")

        settings_area.clear()
        with settings_area:
            ui.separator()
            with ui.expansion("Параметры поиска", icon="tune", value=True).classes("w-full"):
                ui.select(
                    ["Все", ".docx", ".xlsx", ".xls", ".pdf"],
                    label="Тип файла",
                    value=state.file_type or "Все",
                    on_change=lambda e: setattr(state, "file_type", None if e.value == "Все" else e.value),
                ).classes("w-full")
                ui.number("Лимит", value=state.limit, min=1, max=50, step=1, on_change=lambda e: setattr(state, "limit", int(e.value or 10))).classes("w-full")
                ui.checkbox("Искать только в содержимом", value=state.content_only, on_change=lambda e: setattr(state, "content_only", bool(e.value)))
            with ui.expansion("Быстрый поиск", icon="bolt").classes("w-full"):
                for label, query in SEARCH_PRESETS:
                    ui.button(label, on_click=lambda q=query: choose_query(q), color=None).props("flat dense align=left no-caps").classes("rag-nav-button w-full")
            with ui.expansion("Пути", icon="storage").classes("w-full"):
                ui.label(str(state.cfg.get("catalog_path") or "Каталог не задан")).classes("rag-path")
                ui.label(str(state.cfg.get("qdrant_url") or state.cfg.get("qdrant_db_path") or "Qdrant не задан")).classes("rag-path")

        bottom_nav_area.clear()
        with bottom_nav_area:
            color = "primary" if state.screen == "settings" else None
            user_label = "Настройки"
            if state.current_user:
                user_label = f"Настройки · {state.current_user.get('username')}"
            ui.button(user_label, icon="settings", on_click=lambda: set_screen("settings"), color=color).props("flat align=left no-caps").classes("rag-nav-button w-full")

    async def run_search() -> None:
        query = re.sub(r"\s+", " ", str(state.query or "")).strip()
        if not query:
            ui.notify("Введите запрос.", type="warning")
            return
        state.query = query
        state.search_error = ""
        state.results = []
        state.searched_query = query
        _remember_query(state, query)
        render_results_loading()
        searcher = _ensure_searcher(state)
        if searcher is None or not searcher.connected:
            state.search_error = state.searcher_error or "Нет подключения к Qdrant."
            render()
            return
        try:
            state.results = await run.io_bound(
                searcher.search,
                query,
                limit=state.limit,
                file_type=state.file_type,
                content_only=state.content_only,
                source="nicegui",
                username=_username(state),
            )
            _log_app_event(state, "search", "run", details={"query": query, "results": len(state.results)})
        except Exception as exc:
            state.search_error = str(exc)
            _log_app_event(state, "search", "run", ok=False, details={"query": query, "error": str(exc)})
        render()

    def choose_query(query: str) -> None:
        state.query = query
        ui.timer(0.05, run_search, once=True)

    def render_suggestions(area: ui.column, typed: str) -> None:
        area.clear()
        suggestions = _search_suggestions(state, typed)
        if not suggestions:
            return
        with area:
            with ui.column().classes("rag-suggest p-2 gap-1"):
                ui.label("История и подсказки").classes("rag-meta px-3 py-1")
                for item in suggestions:
                    icon = "history" if item in state.history else "north_east"
                    ui.button(item, icon=icon, on_click=lambda q=item: choose_query(q), color=None).props("flat align=left no-caps").classes("rag-nav-button w-full")

    def render_search_box() -> None:
        with ui.column().classes("rag-search-shell w-full max-w-5xl"):
            suggest_area = ui.column().classes("w-full")
            with ui.row().classes("rag-search-box w-full items-center gap-2 p-2"):
                search_input = ui.input(
                    placeholder="Введите название, номер, контрагента или фразу из документа",
                    value=state.query,
                    autocomplete=_search_suggestions(state),
                ).props("borderless dense clearable input-class=text-base").classes("flex-1")
                ui.button(icon="search", on_click=lambda: submit_from_input(), color="primary").props("unelevated round")

            def handle_input(_: events.GenericEventArguments | None = None) -> None:
                state.query = str(search_input.value or "")
                render_suggestions(suggest_area, state.query)

            def submit_from_input(_: events.GenericEventArguments | None = None) -> None:
                state.query = str(search_input.value or "")
                suggest_area.clear()
                ui.timer(0.01, run_search, once=True)

            search_input.on("focus", handle_input)
            search_input.on("input", handle_input)
            search_input.on("keyup.enter", submit_from_input)

    def render_results_loading() -> None:
        content.clear()
        with content:
            render_search_header()
            ui.spinner(size="lg").classes("mt-4")
            ui.label("Ищу совпадения...").classes("rag-meta")

    def render_search_header() -> None:
        with ui.column().classes("w-full gap-3"):
            ui.label("Поиск по каталогу").classes("rag-title")
            ui.label("История, подсказки и быстрые совпадения открываются при фокусе на поле ввода. Enter сразу запускает поиск.").classes("rag-subtitle")
            render_search_box()

    def render_result(result: Dict[str, Any], index: int) -> None:
        name = str(result.get("filename") or "Без имени")
        path = str(result.get("path") or "")
        full_path = str(result.get("full_path") or "")
        score = float(result.get("score") or 0)
        kind = _result_kind(result)
        text = _clean_text(result.get("text") or "")
        preview = text[:650] + ("..." if len(text) > 650 else "")
        p = Path(full_path) if full_path else None

        with ui.column().classes("rag-result gap-3"):
            with ui.row().classes("w-full items-start gap-3"):
                ui.html(_file_icon_svg(full_path or path, kind), sanitize=False)
                with ui.column().classes("flex-1 gap-1"):
                    ui.label(f"{index}. {name}").classes("text-lg font-semibold")
                    ui.label(path or full_path).classes("rag-path")
                ui.label(f"{kind} · {score:.3f}").classes("rag-chip")

            with ui.row().classes("rag-actions"):
                if full_path:
                    if kind == "Каталог":
                        ui.button("Открыть в приложении", icon="folder_open", on_click=lambda p=full_path: go_explorer(p)).props("outline")
                        url = _file_url(full_path)
                        if url:
                            ui.link("Папка ОС", url, new_tab=True).classes("q-btn q-btn--outline q-btn--rectangle q-btn--no-uppercase")
                    else:
                        url = _file_url(full_path)
                        if url:
                            ui.link("Открыть", url, new_tab=True).classes("q-btn q-btn--outline q-btn--rectangle q-btn--no-uppercase")
                        folder_url = _folder_url(full_path)
                        if folder_url:
                            ui.link("Папка ОС", folder_url, new_tab=True).classes("q-btn q-btn--outline q-btn--rectangle q-btn--no-uppercase")
                        ui.button("В проводник приложения", icon="folder", on_click=lambda p=full_path: go_explorer(p)).props("outline")
                        if p and p.exists() and p.is_file():
                            ui.button("Скачать", icon="download", on_click=lambda p=p: ui.download(p, filename=p.name)).props("outline")
                    ui.button("Проводник ОС", icon="open_in_new", on_click=lambda p=full_path: _open_os_path(p)).props("flat")

            if kind == "Каталог":
                with ui.expansion("Раскрыть каталог", icon="account_tree").classes("w-full"):
                    children = _directory_children(full_path)
                    if not children["exists"]:
                        ui.label("Каталог недоступен на диске.").classes("rag-meta")
                    elif children.get("error"):
                        ui.label(f"Не удалось прочитать каталог: {children['error']}").classes("text-red-700")
                    else:
                        if children["dirs"]:
                            ui.label("Папки").classes("font-semibold")
                            with ui.column().classes("w-full gap-1"):
                                for item in children["dirs"]:
                                    ui.button(item["name"], icon="folder", on_click=lambda p=item["path"]: go_explorer(p), color=None).props("flat align=left no-caps").classes("rag-nav-button w-full")
                        if children["files"]:
                            ui.label("Файлы").classes("font-semibold mt-2")
                            for item in children["files"]:
                                ui.label(f"{item['name']} · {item.get('size', '')}").classes("rag-meta")
                        if children.get("truncated"):
                            ui.label("Показаны первые элементы. Полный список доступен в проводнике приложения.").classes("rag-meta")
            else:
                if preview:
                    ui.label(preview).classes("rag-meta")
                with ui.expansion("Просмотреть в приложении", icon="visibility").classes("w-full"):
                    if p:
                        ui.label(_preview_file(p)).classes("rag-code")
                    elif text:
                        ui.label(text[:6000]).classes("rag-code")
                    else:
                        ui.label("Нет доступного фрагмента.").classes("rag-meta")

    def render_search_screen() -> None:
        render_search_header()
        if state.search_error:
            ui.label(state.search_error).classes("text-red-700 rag-card p-4")
        if not state.searched_query:
            with ui.row().classes("w-full gap-3"):
                for label, query in SEARCH_PRESETS:
                    ui.button(label, on_click=lambda q=query: choose_query(q)).props("outline")
            return
        ui.label(f"Результаты по запросу: {state.searched_query}").classes("text-xl font-semibold mt-2")
        if not state.results:
            ui.label("Совпадений не найдено.").classes("rag-card p-4 rag-meta")
            return
        grouped = _grouped_results(state.results)
        with ui.row().classes("w-full gap-2"):
            for group_name, items in grouped:
                ui.label(f"{group_name}: {len(items)}").classes("rag-chip")
        index = 1
        for group_name, items in grouped:
            with ui.expansion(f"{group_name} ({len(items)})", value=True).classes("rag-group-panel"):
                with ui.column().classes("w-full gap-3 p-3"):
                    for result in items:
                        render_result(result, index)
                        index += 1

    def render_explorer_screen() -> None:
        root = Path(str(state.cfg.get("catalog_path") or ""))
        if not root.exists():
            ui.label(f"Каталог не найден: {root}").classes("text-red-700 rag-card p-4")
            return

        toolbar = ui.column().classes("w-full gap-3")
        entries_area = ui.column().classes("w-full gap-3")

        def open_folder(path: Path) -> None:
            state.explorer_path = str(path)
            state.explorer_page = 0
            _get_auth_db(state).touch_favorite(username=_username(state), path=str(path))
            _log_app_event(state, "explorer", "open_folder", details={"path": str(path)})
            render()

        def open_file(path: Path) -> None:
            url = _file_url(str(path))
            if url:
                _get_auth_db(state).touch_favorite(username=_username(state), path=str(path))
                _log_app_event(state, "explorer", "open_file", details={"path": str(path)})
                ui.run_javascript(f"window.open({json.dumps(url)}, '_blank')")

        def copy_path(path: Path) -> None:
            ui.run_javascript(f"navigator.clipboard.writeText({json.dumps(str(path))})")
            ui.notify("Путь скопирован.", type="positive")

        def render_star(path: Path, *, item_type: Optional[str] = None) -> None:
            active = _is_favorite(state, str(path))
            icon = "star" if active else "star_border"
            star = ui.button(icon=icon, color=None).props("flat round dense")
            star.classes("rag-favorite-star active" if active else "rag-favorite-star")
            star.tooltip("Убрать из избранного" if active else "Добавить в избранное")

            def toggle() -> None:
                _toggle_favorite(state, path, item_type=item_type)
                render()

            star.on("click.stop", toggle)

        def open_favorites_dialog() -> None:
            with ui.dialog() as dialog, ui.card().classes("w-[min(900px,92vw)] max-h-[80vh] overflow-auto gap-3"):
                ui.label("Избранное").classes("text-xl font-semibold")
                if not state.favorites:
                    ui.label("Закладок пока нет.").classes("rag-meta")
                for fav in state.favorites:
                    fav_path = Path(str(fav.get("path") or ""))
                    item_type = str(fav.get("item_type") or "file")
                    label = str(fav.get("title") or fav_path.name or fav_path)
                    with ui.element("div").classes("rag-favorites-dialog-row"):
                        ui.icon("folder" if item_type == "folder" else "description")
                        with ui.column().classes("min-w-0 gap-0"):
                            ui.label(label).classes("font-medium truncate")
                            ui.label(str(fav_path)).classes("rag-path truncate")
                        action = (lambda p=fav_path: (dialog.close(), open_folder(p))) if item_type == "folder" else (lambda p=fav_path: (dialog.close(), open_file(p)))
                        ui.button("Открыть", on_click=action).props("outline dense")
                        ui.button(icon="close", on_click=lambda p=fav_path: (_toggle_favorite(state, p), dialog.close(), render())).props("flat round dense").tooltip("Убрать из избранного")
                ui.button("Закрыть", on_click=dialog.close).props("flat")
            dialog.open()

        def render_tile(path: Path, is_dir: bool, size_class: str) -> None:
            icon = _file_icon_svg(str(path), "Каталог" if is_dir else "Файл")
            click = (lambda p=path: open_folder(p)) if is_dir else (lambda p=path: open_file(p))
            with ui.column().classes(f"rag-explorer-item items-center gap-1 p-2 {size_class}"):
                with ui.row().classes("w-full justify-end"):
                    render_star(path, item_type="folder" if is_dir else "file")
                with ui.column().classes("items-center gap-1 cursor-pointer").on("click", click):
                    ui.html(icon, sanitize=False)
                    ui.label(path.name).classes("rag-explorer-name text-center text-sm")

        def render_row(path: Path, is_dir: bool, compact: bool = False) -> None:
            try:
                stat = path.stat()
                size = "" if is_dir else _format_file_size(stat.st_size)
                modified = time.strftime("%d.%m.%Y %H:%M", time.localtime(stat.st_mtime))
            except Exception:
                size, modified = "", ""
            with ui.row().classes("rag-explorer-item w-full p-2 items-center gap-3"):
                ui.html(_file_icon_svg(str(path), "Каталог" if is_dir else "Файл"), sanitize=False)
                action = (lambda p=path: open_folder(p)) if is_dir else (lambda p=path: open_file(p))
                with ui.column().classes("flex-1 gap-0"):
                    ui.button(path.name, on_click=action, color=None).props("flat align=left no-caps dense").classes("rag-nav-button w-full")
                    if not compact:
                        ui.label(f"{'Папка' if is_dir else path.suffix or 'без расширения'} · {size} · {modified}").classes("rag-meta")
                if not compact:
                    if not is_dir:
                        ui.button("Скачать", icon="download", on_click=lambda p=path: (_log_app_event(state, "explorer", "download", details={"path": str(p)}), ui.download(p, filename=p.name))).props("outline dense")
                    ui.button("ОС", icon="open_in_new", on_click=lambda p=path: _open_os_path(str(p.parent if p.is_file() else p))).props("flat dense")
                render_star(path, item_type="folder" if is_dir else "file")

        def render_entries() -> None:
            entries_area.clear()
            current = _safe_explorer_path(state)
            if not current.exists():
                state.explorer_path = str(root)
                current = root

            parts: List[Path] = []
            p = current
            while True:
                parts.append(p)
                if p == root or p == p.parent:
                    break
                p = p.parent
            parts.reverse()

            if state.header_breadcrumbs is not None:
                state.header_breadcrumbs.clear()
                with state.header_breadcrumbs:
                    for idx, part in enumerate(parts):
                        label = "Корень" if part == root else part.name
                        ui.button(label, on_click=lambda p=part: (_log_app_event(state, "explorer", "breadcrumb", details={"path": str(p)}), open_folder(p)), color=None).props("flat dense no-caps")
                        if idx < len(parts) - 1:
                            ui.icon("chevron_right").classes("text-slate-400")

            if state.header_explorer_actions is not None:
                state.header_explorer_actions.clear()
                with state.header_explorer_actions:
                    up_button = ui.button(icon="arrow_upward", on_click=lambda: (_log_app_event(state, "explorer", "up", details={"path": str(current.parent)}), open_folder(current.parent)), color=None).props("flat round dense")
                    up_button.tooltip("На уровень выше")
                    if current == root:
                        up_button.disable()
                    active = _is_favorite(state, str(current))
                    fav = ui.button(icon="star" if active else "star_border", color=None).props("flat round dense")
                    fav.classes("rag-favorite-star header active" if active else "rag-favorite-star header")
                    fav.tooltip("Убрать текущую папку из избранного" if active else "Добавить текущую папку в избранное")
                    fav.on("click", lambda p=current: (_toggle_favorite(state, p, item_type="folder"), render()))

            dirs, files, total_files = _file_rows(current, state)
            state.explorer_page = max(0, min(state.explorer_page, max(0, (len(files) - 1) // PAGE_SIZE)))
            page_files = files[state.explorer_page * PAGE_SIZE : (state.explorer_page + 1) * PAGE_SIZE]

            with entries_area:
                ui.label(f"{current} · папок {len(dirs)} · файлов {total_files}").classes("rag-path")

                if state.favorites:
                    with ui.row().classes("rag-bookmarks"):
                        for fav in state.favorites:
                            fav_path = Path(str(fav.get("path") or ""))
                            item_type = str(fav.get("item_type") or "file")
                            label = str(fav.get("title") or fav_path.name or fav_path)
                            icon = "folder" if item_type == "folder" else "description"
                            action = (lambda p=fav_path: open_folder(p)) if item_type == "folder" else (lambda p=fav_path: open_file(p))
                            with ui.element("div").classes("rag-bookmark"):
                                with ui.element("div").classes("rag-bookmark-main"):
                                    button = ui.button(label, icon=icon, on_click=action, color=None).props("flat dense no-caps").classes("rag-nav-button")
                                    button.tooltip(label)
                                with ui.element("div").classes("rag-bookmark-remove"):
                                    remove_button = ui.button(icon="close", color=None).props("flat round dense")
                                    remove_button.tooltip("Убрать из избранного")
                                    remove_button.on("click.stop", lambda p=fav_path: (_toggle_favorite(state, p), render()))
                        ui.button(icon="more_horiz", on_click=open_favorites_dialog, color=None).props("outline round dense").classes("rag-bookmark-more").tooltip("Показать все избранное")

                if not dirs and not files:
                    ui.label("Нет элементов, соответствующих фильтру.").classes("rag-card p-4 rag-meta")
                    return

                if state.explorer_view in {"Крупные значки", "Средние значки", "Мелкие значки"}:
                    grid_class = {
                        "Крупные значки": "",
                        "Средние значки": "medium",
                        "Мелкие значки": "small",
                    }[state.explorer_view]
                    with ui.element("div").classes(f"rag-explorer-grid {grid_class} w-full"):
                        for path in [*dirs, *page_files]:
                            render_tile(path, path.is_dir(), grid_class)
                elif state.explorer_view == "Список":
                    with ui.column().classes("rag-explorer-list w-full"):
                        for path in [*dirs, *page_files]:
                            render_row(path, path.is_dir(), compact=True)
                else:
                    with ui.column().classes("w-full gap-2"):
                        for path in [*dirs, *page_files]:
                            render_row(path, path.is_dir(), compact=False)

                if total_files > PAGE_SIZE:
                    with ui.row().classes("items-center gap-2"):
                        ui.button("Назад", on_click=lambda: (setattr(state, "explorer_page", max(0, state.explorer_page - 1)), render_entries())).props("outline")
                        ui.label(f"Страница {state.explorer_page + 1} из {(total_files + PAGE_SIZE - 1) // PAGE_SIZE}").classes("rag-meta")
                        ui.button("Вперед", on_click=lambda: (setattr(state, "explorer_page", state.explorer_page + 1), render_entries())).props("outline")

        with toolbar:
            with ui.row().classes("rag-card w-full p-3 gap-3 items-center"):
                filter_input = ui.input(placeholder="Фильтр по имени", value=state.explorer_filter).props("dense outlined clearable").classes("min-w-64 flex-1")

                def update_explorer_setting(attr: str, value: Any) -> None:
                    setattr(state, attr, value)
                    state.explorer_page = 0
                    _save_explorer_settings(state)
                    _log_app_event(state, "explorer", "change_setting", details={attr: value})
                    render_entries()

                ui.select(["Все", ".docx", ".xlsx", ".xls", ".pdf"], value=state.explorer_ext, on_change=lambda e: update_explorer_setting("explorer_ext", e.value)).props("dense outlined").classes("w-36")
                ui.select(["Крупные значки", "Средние значки", "Мелкие значки", "Список", "Таблица"], value=state.explorer_view, on_change=lambda e: update_explorer_setting("explorer_view", e.value)).props("dense outlined").classes("w-44")
                ui.select(["По имени", "По размеру", "По дате"], value=state.explorer_sort, on_change=lambda e: update_explorer_setting("explorer_sort", e.value)).props("dense outlined").classes("w-40")
                ui.select(["По возрастанию", "По убыванию"], value="По убыванию" if state.explorer_desc else "По возрастанию", on_change=lambda e: update_explorer_setting("explorer_desc", e.value == "По убыванию")).props("dense outlined").classes("w-44")

                def apply_filter(_: events.GenericEventArguments | None = None) -> None:
                    state.explorer_filter = str(filter_input.value or "")
                    state.explorer_page = 0
                    render_entries()

                filter_input.on("input", apply_filter)

        render_entries()

    def render_index_screen() -> None:
        stats = _read_index_stats(state.cfg)
        if not stats["found"]:
            ui.label(f"Состояние индекса не найдено: {stats['state_file']}").classes("rag-card p-4 rag-meta")
            return
        with ui.row().classes("w-full gap-3"):
            ui.label(f"Файлов: {stats['total']}").classes("rag-card p-4 text-xl font-semibold")
            ui.label(f"Обновлен: {stats.get('last_modified', 'неизвестно')}").classes("rag-card p-4 text-xl font-semibold")
        with ui.column().classes("rag-card w-full p-4 gap-2"):
            ui.label("Расширения").classes("text-lg font-semibold")
            for ext, count in list(stats.get("by_ext", {}).items())[:20]:
                ui.label(f"{ext}: {count}").classes("rag-meta")

    def render_index_dashboard() -> None:
        stats = _read_index_stats(state.cfg)
        with ui.column().classes("rag-card w-full p-4 gap-3"):
            ui.label("Дашборд индексирования").classes("text-xl font-semibold")
            if not stats["found"]:
                ui.label(f"Состояние индекса не найдено: {stats['state_file']}").classes("rag-meta")
                return
            with ui.row().classes("w-full gap-3"):
                ui.label(f"Файлов: {stats['total']}").classes("rag-chip")
                ui.label(f"Обновлен: {stats.get('last_modified', 'неизвестно')}").classes("rag-chip")
            if stats.get("by_ext"):
                for ext, count in list(stats["by_ext"].items())[:12]:
                    ui.label(f"{ext}: {count}").classes("rag-meta")

    def render_login_screen() -> None:
        auth_db = _get_auth_db(state)
        with ui.column().classes("w-full min-h-[70vh] items-center justify-center"):
            with ui.column().classes("rag-card w-full max-w-xl p-5 gap-3"):
                ui.label("Вход в RAG Каталог").classes("text-2xl font-semibold")
                ui.label("Введите учетные данные, чтобы открыть приложение.").classes("rag-meta")
                username_input = ui.input("Логин").props("dense outlined").classes("w-full")
                password_input = ui.input("Пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")

                def login() -> None:
                    username = str(username_input.value or "")
                    user = auth_db.login(username=username, password=str(password_input.value or ""))
                    if not user:
                        auth_db.log_auth_event(username=username, event_type="login_failed", ok=False, error="bad_credentials")
                        ui.notify("Неверный логин или пароль.", type="negative")
                        return
                    state.current_user = user
                    state.auth_token = auth_db.create_session(username=str(user.get("username") or ""))
                    auth_db.log_auth_event(username=_username(state), event_type="login", ok=True)
                    _load_user_state(state)
                    try:
                        app.storage.user["auth_token"] = state.auth_token
                    except Exception:
                        pass
                    ui.notify("Вход выполнен.", type="positive")
                    render()

                password_input.on("keyup.enter", lambda _: login())
                ui.button("Войти", icon="login", on_click=login).props("unelevated")

    def render_admin_users(auth_db: UserAuthDB) -> None:
        with ui.column().classes("rag-card w-full p-4 gap-4"):
            ui.label("Админ-панель пользователей").classes("text-xl font-semibold")
            with ui.expansion("Создать пользователя", icon="person_add").classes("w-full"):
                new_username = ui.input("Логин").props("dense outlined").classes("w-full")
                new_display = ui.input("Имя").props("dense outlined").classes("w-full")
                new_telegram = ui.input("Telegram chat id").props("dense outlined").classes("w-full")
                new_password = ui.input("Временный пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")
                new_role = ui.select(["user", "admin"], value="user", label="Роль").props("dense outlined").classes("w-full")
                new_status = ui.select(["active", "pending", "blocked"], value="active", label="Статус").props("dense outlined").classes("w-full")
                new_must_change = ui.checkbox("Потребовать смену пароля", value=True)

                def create_user() -> None:
                    ok = auth_db.admin_create_user(
                        username=str(new_username.value or ""),
                        display_name=str(new_display.value or ""),
                        telegram_chat_id=str(new_telegram.value or ""),
                        password=str(new_password.value or ""),
                        role=str(new_role.value or "user"),
                        status=str(new_status.value or "active"),
                        must_change_password=bool(new_must_change.value),
                    )
                    ui.notify("Пользователь создан." if ok else "Не удалось создать пользователя.", type="positive" if ok else "negative")
                    render()

                ui.button("Создать", icon="person_add", on_click=create_user).props("unelevated")

            users = auth_db.list_users()
            for user in users:
                username = str(user.get("username") or "")
                role = str(user.get("role") or "user")
                status = str(user.get("status") or "")
                with ui.expansion(f"{username} · {role} · {status}", icon="person").classes("w-full"):
                    display_input = ui.input("Имя", value=str(user.get("display_name") or "")).props("dense outlined").classes("w-full")
                    telegram_input = ui.input("Telegram chat id", value=str(user.get("telegram_chat_id") or "")).props("dense outlined").classes("w-full")
                    role_input = ui.select(["user", "admin"], value=role, label="Роль").props("dense outlined").classes("w-full")
                    status_input = ui.select(["active", "pending", "blocked"], value=status or "active", label="Статус").props("dense outlined").classes("w-full")
                    must_input = ui.checkbox("Потребовать смену пароля", value=bool(int(user.get("must_change_password") or 0)))
                    reset_password = ui.input("Новый временный пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")

                    def save_user(
                        username: str = username,
                        display_input: Any = display_input,
                        telegram_input: Any = telegram_input,
                        role_input: Any = role_input,
                        status_input: Any = status_input,
                        must_input: Any = must_input,
                    ) -> None:
                        ok = auth_db.admin_update_user(
                            username=username,
                            display_name=str(display_input.value or ""),
                            telegram_chat_id=str(telegram_input.value or ""),
                            role=str(role_input.value or "user"),
                            status=str(status_input.value or "active"),
                            must_change_password=bool(must_input.value),
                        )
                        ui.notify("Пользователь обновлен." if ok else "Не удалось обновить пользователя.", type="positive" if ok else "negative")
                        _refresh_current_user(state)
                        render()

                    def set_password(
                        username: str = username,
                        reset_password: Any = reset_password,
                    ) -> None:
                        ok = auth_db.admin_set_password(
                            username=username,
                            new_password=str(reset_password.value or ""),
                            must_change_password=True,
                        )
                        ui.notify("Пароль обновлен." if ok else "Введите новый пароль.", type="positive" if ok else "warning")
                        render()

                    with ui.row().classes("gap-2"):
                        ui.button("Сохранить", icon="save", on_click=save_user).props("outline")
                        ui.button("Сбросить пароль", icon="key", on_click=set_password).props("outline")

    def render_settings_screen() -> None:
        auth_db = _get_auth_db(state)
        ui.label("Настройки").classes("text-2xl font-semibold")
        if state.current_user is None:
            with ui.column().classes("rag-card w-full max-w-xl p-4 gap-3"):
                ui.label("Вход пользователя").classes("text-xl font-semibold")
                ui.label("Для первого входа администратора используйте admin / admin, затем смените пароль.").classes("rag-meta")
                username_input = ui.input("Логин").props("dense outlined").classes("w-full")
                password_input = ui.input("Пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")

                def login() -> None:
                    user = auth_db.login(username=str(username_input.value or ""), password=str(password_input.value or ""))
                    if not user:
                        ui.notify("Неверный логин или пароль.", type="negative")
                        return
                    state.current_user = user
                    state.auth_token = auth_db.create_session(username=str(user.get("username") or ""))
                    try:
                        app.storage.user["auth_token"] = state.auth_token
                    except Exception:
                        pass
                    ui.notify("Вход выполнен.", type="positive")
                    render()

                password_input.on("keyup.enter", lambda _: login())
                ui.button("Войти", icon="login", on_click=login).props("unelevated")
            return

        user = state.current_user
        is_admin = str(user.get("role") or "user") == "admin"
        with ui.column().classes("rag-card w-full p-4 gap-3"):
            ui.label("Настройки пользователя").classes("text-xl font-semibold")
            ui.label(f"Логин: {user.get('username')} · роль: {user.get('role')} · статус: {user.get('status')}").classes("rag-meta")
            display_input = ui.input("Имя", value=str(user.get("display_name") or "")).props("dense outlined").classes("w-full")
            telegram_input = ui.input("Telegram chat id", value=str(user.get("telegram_chat_id") or "")).props("dense outlined").classes("w-full")

            def save_profile() -> None:
                ok = auth_db.update_profile(
                    username=str(user.get("username") or ""),
                    display_name=str(display_input.value or ""),
                    telegram_chat_id=str(telegram_input.value or ""),
                )
                _refresh_current_user(state)
                ui.notify("Профиль сохранен." if ok else "Не удалось сохранить профиль.", type="positive" if ok else "negative")
                render()

            ui.button("Сохранить профиль", icon="save", on_click=save_profile).props("outline")

        with ui.column().classes("rag-card w-full p-4 gap-3"):
            ui.label("Настройки проводника").classes("text-xl font-semibold")
            ui.label(f"Вид: {state.explorer_view} · сортировка: {state.explorer_sort} · {'убывание' if state.explorer_desc else 'возрастание'} · тип: {state.explorer_ext}").classes("rag-meta")

            def reset_explorer_settings() -> None:
                auth_db.reset_user_settings(username=str(user.get("username") or ""))
                state.explorer_view = "Таблица"
                state.explorer_sort = "По имени"
                state.explorer_desc = False
                state.explorer_ext = "Все"
                _log_app_event(state, "settings", "reset_explorer")
                ui.notify("Настройки проводника сброшены.", type="positive")
                render()

            ui.button("Сбросить настройки проводника", icon="restart_alt", on_click=reset_explorer_settings).props("outline")

        with ui.column().classes("rag-card w-full p-4 gap-3"):
            ui.label("Избранное").classes("text-xl font-semibold")
            if not state.favorites:
                ui.label("Закладок пока нет. Добавьте файл или папку звездочкой в проводнике.").classes("rag-meta")
            for fav in state.favorites:
                fav_path = Path(str(fav.get("path") or ""))
                item_type = str(fav.get("item_type") or "")
                with ui.row().classes("w-full items-center gap-2"):
                    ui.icon("folder" if item_type == "folder" else "description")
                    ui.label(str(fav.get("title") or fav_path.name or fav_path)).classes("font-medium")
                    ui.label(str(fav_path)).classes("rag-path flex-1")
                    if item_type == "folder":
                        ui.button("Открыть", on_click=lambda p=fav_path: go_explorer(str(p))).props("outline dense")
                    else:
                        ui.button("Открыть", on_click=lambda p=fav_path: ui.run_javascript(f"window.open({json.dumps(_file_url(str(p)))}, '_blank')")).props("outline dense")
                    ui.button(icon="delete", on_click=lambda p=fav_path: (_toggle_favorite(state, p), render())).props("flat round dense")

        with ui.column().classes("rag-card w-full p-4 gap-3"):
            ui.label("Смена пароля").classes("text-xl font-semibold")
            old_password = ui.input("Текущий пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")
            new_password = ui.input("Новый пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")
            new_password2 = ui.input("Повторите пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")

            def change_password() -> None:
                if str(new_password.value or "") != str(new_password2.value or ""):
                    ui.notify("Новые пароли не совпадают.", type="warning")
                    return
                ok = auth_db.change_password(
                    username=str(user.get("username") or ""),
                    old_password=str(old_password.value or ""),
                    new_password=str(new_password.value or ""),
                )
                if ok:
                    _refresh_current_user(state)
                ui.notify("Пароль изменен." if ok else "Не удалось изменить пароль.", type="positive" if ok else "negative")
                render()

            with ui.row().classes("gap-2"):
                ui.button("Сменить пароль", icon="key", on_click=change_password).props("outline")
                def logout() -> None:
                    if state.auth_token:
                        auth_db.revoke_session(state.auth_token)
                    auth_db.log_auth_event(username=_username(state), event_type="logout", ok=True)
                    state.current_user = None
                    state.auth_token = ""
                    try:
                        app.storage.user.pop("auth_token", None)
                    except Exception:
                        pass
                    render()

                ui.button("Выйти", icon="logout", on_click=logout).props("flat")

        if is_admin:
            render_index_dashboard()
            render_admin_users(auth_db)

    def render_telegram_screen() -> None:
        enabled = bool(state.cfg.get("telegram_enabled"))
        token_set = bool(str(state.cfg.get("telegram_bot_token") or "").strip())
        with ui.column().classes("rag-card w-full p-4 gap-2"):
            ui.label(f"Статус: {'включен' if enabled else 'выключен'}").classes("text-lg font-semibold")
            ui.label(f"Токен: {'задан' if token_set else 'не задан'}").classes("rag-meta")
            bot_link = str(state.cfg.get("telegram_bot_link") or "").strip()
            if bot_link:
                ui.link("Открыть бота", bot_link, new_tab=True)

    def render_stats_screen() -> None:
        if str((state.current_user or {}).get("role") or "") != "admin":
            ui.label("Раздел доступен только администратору.").classes("rag-card p-4 text-red-700")
            return
        telemetry_path = _telemetry_db_path(state.cfg)
        auth_db = _get_auth_db(state)
        ui.label("Статистика использования").classes("text-2xl font-semibold")
        searches_by_day = _db_query_dicts(
            telemetry_path,
            """
            SELECT substr(ts, 1, 10) AS day, COUNT(*) AS count
            FROM search_logs
            GROUP BY substr(ts, 1, 10)
            ORDER BY day
            LIMIT 30
            """,
        )
        with ui.column().classes("rag-card w-full p-4 gap-3"):
            ui.label("Поиски по дням").classes("text-xl font-semibold")
            ui.echart({
                "tooltip": {"trigger": "axis"},
                "xAxis": {"type": "category", "data": [row["day"] for row in searches_by_day]},
                "yAxis": {"type": "value"},
                "series": [{"type": "bar", "data": [row["count"] for row in searches_by_day], "name": "Поиски"}],
            }).classes("w-full h-72")

        top_queries = _db_query_dicts(
            telemetry_path,
            """
            SELECT query, COUNT(*) AS count
            FROM search_logs
            WHERE query <> ''
            GROUP BY lower(query)
            ORDER BY count DESC
            LIMIT 20
            """,
        )
        top_users = _db_query_dicts(
            telemetry_path,
            """
            SELECT COALESCE(NULLIF(username, ''), source, 'unknown') AS username, COUNT(*) AS count
            FROM search_logs
            GROUP BY COALESCE(NULLIF(username, ''), source, 'unknown')
            ORDER BY count DESC
            LIMIT 20
            """,
        )
        top_features = _db_query_dicts(
            telemetry_path,
            """
            SELECT feature || ':' || action AS name, COUNT(*) AS count
            FROM app_events
            GROUP BY feature, action
            ORDER BY count DESC
            LIMIT 20
            """,
        )
        recent_searches = _db_query_dicts(
            telemetry_path,
            """
            SELECT ts, username, query, results_count, duration_ms, ok, error
            FROM search_logs
            ORDER BY id DESC
            LIMIT 50
            """,
        )
        auth_events = auth_db.list_auth_events(limit=50)

        with ui.row().classes("w-full gap-3 items-start"):
            with ui.column().classes("rag-card flex-1 p-4 gap-2"):
                ui.label("Топ запросов").classes("text-xl font-semibold")
                for row in top_queries:
                    ui.label(f"{row['query']}: {row['count']}").classes("rag-meta")
            with ui.column().classes("rag-card flex-1 p-4 gap-2"):
                ui.label("Топ пользователей").classes("text-xl font-semibold")
                for row in top_users:
                    ui.label(f"{row['username']}: {row['count']}").classes("rag-meta")
            with ui.column().classes("rag-card flex-1 p-4 gap-2"):
                ui.label("Функции").classes("text-xl font-semibold")
                for row in top_features:
                    ui.label(f"{row['name']}: {row['count']}").classes("rag-meta")

        with ui.expansion("История запросов", value=True).classes("rag-group-panel w-full"):
            ui.table(
                rows=recent_searches,
                columns=[
                    {"name": "ts", "label": "Время", "field": "ts"},
                    {"name": "username", "label": "Пользователь", "field": "username"},
                    {"name": "query", "label": "Запрос", "field": "query"},
                    {"name": "results_count", "label": "Результаты", "field": "results_count"},
                    {"name": "duration_ms", "label": "мс", "field": "duration_ms"},
                    {"name": "error", "label": "Ошибка", "field": "error"},
                ],
                pagination=10,
            ).classes("w-full")
        with ui.expansion("История входов", value=False).classes("rag-group-panel w-full"):
            ui.table(
                rows=auth_events,
                columns=[
                    {"name": "ts", "label": "Время", "field": "ts"},
                    {"name": "username", "label": "Пользователь", "field": "username"},
                    {"name": "event_type", "label": "Событие", "field": "event_type"},
                    {"name": "ok", "label": "OK", "field": "ok"},
                    {"name": "error", "label": "Ошибка", "field": "error"},
                ],
                pagination=10,
            ).classes("w-full")

    def render() -> None:
        header_title.set_text({
            "search": "Поиск",
            "explorer": "Проводник",
            "index": "Индекс",
            "telegram": "Telegram",
            "settings": "Настройки",
            "stats": "Статистика",
        }.get(state.screen, "Поиск"))
        if state.header_breadcrumbs is not None:
            state.header_breadcrumbs.clear()
        if state.header_explorer_actions is not None:
            state.header_explorer_actions.clear()
        update_nav()
        content.clear()
        with content:
            if state.current_user is None:
                try:
                    drawer.set_visibility(False)
                except Exception:
                    pass
                render_login_screen()
                return
            try:
                drawer.set_visibility(True)
            except Exception:
                pass
            if state.screen == "explorer":
                try:
                    drawer.set_visibility(True)
                except Exception:
                    pass
                render_explorer_screen()
            elif state.screen == "index":
                render_index_screen()
            elif state.screen == "telegram":
                render_telegram_screen()
            elif state.screen == "settings":
                render_settings_screen()
            elif state.screen == "stats":
                render_stats_screen()
            else:
                render_search_screen()

    render()


@ui.page("/")
def root_page() -> None:
    ui.navigate.to("/search")


@ui.page("/search")
def search_page() -> None:
    _build_page("search")


@ui.page("/explorer")
def explorer_page() -> None:
    _build_page("explorer")


@ui.page("/index")
def index_page() -> None:
    _build_page("index")


@ui.page("/telegram")
def telegram_page() -> None:
    _build_page("telegram")


@ui.page("/settings")
def settings_page() -> None:
    _build_page("settings")


@ui.page("/stats")
def stats_page() -> None:
    _build_page("stats")


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Запустить NiceGUI-интерфейс RAG Каталога.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--no-show", action="store_true", help="Не открывать браузер автоматически.")
    args = parser.parse_args(argv)
    ui.run(
        title="RAG Каталог",
        host=args.host,
        port=args.port,
        favicon=APP_ICON_PATH if APP_ICON_PATH.exists() else None,
        language="ru",
        reload=False,
        show=not args.no_show,
        dark=False,
        storage_secret="rag-catalog-local-secret",
    )


if __name__ in {"__main__", "__mp_main__"}:
    main()
