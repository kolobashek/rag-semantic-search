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
    explorer_page: int = 0


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

    dirs.sort(key=lambda x: x.name.lower())
    if state.explorer_sort == "По размеру":
        files.sort(key=lambda x: x.stat().st_size if x.exists() else 0, reverse=True)
    elif state.explorer_sort == "По дате":
        files.sort(key=lambda x: x.stat().st_mtime if x.exists() else 0, reverse=True)
    else:
        files.sort(key=lambda x: x.name.lower())
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
        .rag-drawer {
          background: #ffffff;
          border-right: 1px solid var(--rag-border);
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
        .rag-nav-button { justify-content: flex-start; border-radius: 8px; }
        .rag-explorer-grid {
          display: grid;
          grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
          gap: 10px;
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


@ui.page("/")
def index() -> None:
    state = PageState(cfg=load_config())
    state.explorer_path = str(Path(str(state.cfg.get("catalog_path") or "")))
    _install_css()

    with ui.header(fixed=True, elevated=False).classes("rag-header h-16 px-3 md:px-5"):
        ui.button(icon="menu", on_click=lambda: drawer.toggle(), color=None).props("flat round").classes("text-slate-700")
        ui.image("/rag-logo.png").classes("w-9 h-9 rounded") if LOGO_PATH.exists() else ui.icon("manage_search").classes("text-3xl")
        ui.label("RAG Каталог").classes("font-semibold text-lg")
        ui.space()
        status_text = "Qdrant готов" if _ensure_searcher(state) and state.searcher and state.searcher.connected else "Qdrant недоступен"
        ui.label(status_text).classes("hidden sm:block rag-chip")

    with ui.left_drawer(value=True, fixed=True, bordered=True).classes("rag-drawer w-80 p-4") as drawer:
        ui.label("Меню").classes("text-xl font-semibold mb-2")
        nav_area = ui.column().classes("w-full gap-2")
        settings_area = ui.column().classes("w-full gap-3 mt-4")

    with ui.column().classes("rag-page gap-5"):
        content = ui.column().classes("w-full gap-5")

    def set_screen(screen: str) -> None:
        state.screen = screen
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
                ui.button(label, icon=icon, on_click=lambda s=screen: set_screen(s), color=color).props("flat").classes("rag-nav-button w-full")

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
                    ui.button(label, on_click=lambda q=query: choose_query(q), color=None).props("flat dense").classes("rag-nav-button w-full")
            with ui.expansion("Пути", icon="storage").classes("w-full"):
                ui.label(str(state.cfg.get("catalog_path") or "Каталог не задан")).classes("rag-path")
                ui.label(str(state.cfg.get("qdrant_url") or state.cfg.get("qdrant_db_path") or "Qdrant не задан")).classes("rag-path")

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
            )
        except Exception as exc:
            state.search_error = str(exc)
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
                    ui.button(item, icon=icon, on_click=lambda q=item: choose_query(q), color=None).props("flat no-caps").classes("rag-nav-button w-full")

    def render_search_box() -> None:
        with ui.column().classes("rag-search-shell w-full max-w-5xl"):
            suggest_area = ui.column().classes("w-full")
            with ui.row().classes("rag-search-box w-full items-center gap-2 p-2"):
                search_input = ui.input(
                    placeholder="Введите название, номер, контрагента или фразу из документа",
                    value=state.query,
                    autocomplete=_search_suggestions(state),
                ).props("borderless dense clearable input-class=text-base").classes("flex-1")
                ui.button(icon="search", on_click=run_search, color="primary").props("unelevated round")

            def handle_input(_: events.GenericEventArguments | None = None) -> None:
                state.query = str(search_input.value or "")
                render_suggestions(suggest_area, state.query)

            search_input.on("focus", handle_input)
            search_input.on("input", handle_input)
            search_input.on("keydown.enter", run_search)

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
                ui.icon("folder" if kind == "Каталог" else "description").classes("text-3xl text-cyan-800")
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
                                    ui.button(item["name"], icon="folder", on_click=lambda p=item["path"]: go_explorer(p), color=None).props("flat no-caps").classes("rag-nav-button w-full")
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
        for index, result in enumerate(state.results, start=1):
            render_result(result, index)

    def render_explorer_screen() -> None:
        root = Path(str(state.cfg.get("catalog_path") or ""))
        current = _safe_explorer_path(state)
        ui.label("Проводник").classes("rag-title")
        ui.label("Состояние папки сохраняется при переходе между разделами приложения.").classes("rag-subtitle")
        if not root.exists():
            ui.label(f"Каталог не найден: {root}").classes("text-red-700 rag-card p-4")
            return
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

        with ui.row().classes("w-full items-center gap-2"):
            for idx, part in enumerate(parts):
                label = "Корень" if part == root else part.name
                ui.button(label, on_click=lambda p=part: (setattr(state, "explorer_path", str(p)), setattr(state, "explorer_page", 0), render()), color=None).props("flat dense no-caps")
                if idx < len(parts) - 1:
                    ui.icon("chevron_right").classes("text-slate-400")
            ui.space()
            up_button = ui.button("Выше", icon="arrow_upward", on_click=lambda: (setattr(state, "explorer_path", str(current.parent)), setattr(state, "explorer_page", 0), render()), color=None).props("outline")
            if current == root:
                up_button.disable()

        with ui.row().classes("rag-card w-full p-3 gap-3 items-center"):
            filter_input = ui.input(placeholder="Фильтр по имени", value=state.explorer_filter).props("dense outlined clearable").classes("min-w-72 flex-1")
            ui.select(["Все", ".docx", ".xlsx", ".xls", ".pdf"], value=state.explorer_ext, on_change=lambda e: (setattr(state, "explorer_ext", e.value), setattr(state, "explorer_page", 0), render())).props("dense outlined").classes("w-36")
            ui.select(["По имени", "По размеру", "По дате"], value=state.explorer_sort, on_change=lambda e: (setattr(state, "explorer_sort", e.value), render())).props("dense outlined").classes("w-40")

            def apply_filter() -> None:
                state.explorer_filter = str(filter_input.value or "")
                state.explorer_page = 0
                render()

            filter_input.on("keydown.enter", apply_filter)
            ui.button("Фильтр", icon="filter_alt", on_click=apply_filter).props("outline")

        dirs, files, total_files = _file_rows(current, state)
        ui.label(f"{current} · папок {len(dirs)} · файлов {total_files}").classes("rag-path")

        if dirs:
            ui.label("Папки").classes("text-lg font-semibold")
            with ui.element("div").classes("rag-explorer-grid w-full"):
                for d in dirs:
                    with ui.row().classes("rag-card p-3 items-center gap-2"):
                        ui.icon("folder").classes("text-amber-600 text-2xl")
                        ui.button(d.name, on_click=lambda p=d: (setattr(state, "explorer_path", str(p)), setattr(state, "explorer_page", 0), render()), color=None).props("flat no-caps").classes("flex-1 rag-nav-button")

        if not files:
            ui.label("Файлов, соответствующих фильтру, нет.").classes("rag-card p-4 rag-meta")
            return
        state.explorer_page = max(0, min(state.explorer_page, max(0, (len(files) - 1) // PAGE_SIZE)))
        page_files = files[state.explorer_page * PAGE_SIZE : (state.explorer_page + 1) * PAGE_SIZE]
        ui.label("Файлы").classes("text-lg font-semibold")
        with ui.column().classes("w-full gap-2"):
            for file_path in page_files:
                try:
                    stat = file_path.stat()
                    size = _format_file_size(stat.st_size)
                    modified = time.strftime("%d.%m.%Y %H:%M", time.localtime(stat.st_mtime))
                except Exception:
                    size, modified = "", ""
                with ui.row().classes("rag-card w-full p-3 items-center gap-3"):
                    ui.icon("description").classes("text-cyan-800 text-2xl")
                    with ui.column().classes("flex-1 gap-0"):
                        ui.label(file_path.name).classes("font-medium")
                        ui.label(f"{file_path.suffix or 'без расширения'} · {size} · {modified}").classes("rag-meta")
                    url = _file_url(str(file_path))
                    if url:
                        ui.link("Открыть", url, new_tab=True).classes("q-btn q-btn--outline q-btn--rectangle q-btn--no-uppercase")
                    ui.button("Скачать", icon="download", on_click=lambda p=file_path: ui.download(p, filename=p.name)).props("outline")
                    ui.button("ОС", icon="open_in_new", on_click=lambda p=str(file_path.parent): _open_os_path(p)).props("flat")

        if total_files > PAGE_SIZE:
            with ui.row().classes("items-center gap-2"):
                ui.button("Назад", on_click=lambda: (setattr(state, "explorer_page", max(0, state.explorer_page - 1)), render())).props("outline")
                ui.label(f"Страница {state.explorer_page + 1} из {(total_files + PAGE_SIZE - 1) // PAGE_SIZE}").classes("rag-meta")
                ui.button("Вперед", on_click=lambda: (setattr(state, "explorer_page", state.explorer_page + 1), render())).props("outline")

    def render_index_screen() -> None:
        ui.label("Индекс").classes("rag-title")
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

    def render_telegram_screen() -> None:
        ui.label("Telegram").classes("rag-title")
        enabled = bool(state.cfg.get("telegram_enabled"))
        token_set = bool(str(state.cfg.get("telegram_bot_token") or "").strip())
        with ui.column().classes("rag-card w-full p-4 gap-2"):
            ui.label(f"Статус: {'включен' if enabled else 'выключен'}").classes("text-lg font-semibold")
            ui.label(f"Токен: {'задан' if token_set else 'не задан'}").classes("rag-meta")
            bot_link = str(state.cfg.get("telegram_bot_link") or "").strip()
            if bot_link:
                ui.link("Открыть бота", bot_link, new_tab=True)

    def render() -> None:
        update_nav()
        content.clear()
        with content:
            if state.screen == "explorer":
                render_explorer_screen()
            elif state.screen == "index":
                render_index_screen()
            elif state.screen == "telegram":
                render_telegram_screen()
            else:
                render_search_screen()

    render()


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
    )


if __name__ in {"__main__", "__mp_main__"}:
    main()
