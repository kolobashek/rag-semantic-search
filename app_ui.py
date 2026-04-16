"""
app_ui.py — Streamlit веб-интерфейс для RAG Каталога.

Запуск:
    streamlit run app_ui.py
"""

import html
import json
import logging
import time
from collections import Counter
from pathlib import Path, PureWindowsPath
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import streamlit as st

from rag_core import RAGSearcher, load_config, save_config

# ─────────────────────────── logging ───────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────── page config ───────────────────────────────
st.set_page_config(
    page_title="RAG Каталог",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS: поддержка светлой и тёмной тем ──────────────────────────────
st.markdown(
    """
<style>
    .result-card {
        background: var(--secondary-background-color, #f7f7f7);
        border: 1px solid var(--border-color, #e0e0e0);
        border-radius: 6px;
        padding: 1.2rem 1.4rem;
        margin-bottom: 0.8rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.08);
        transition: box-shadow 0.2s, border-color 0.2s;
    }
    .result-card:hover {
        box-shadow: 0 4px 12px rgba(0,0,0,0.12);
        border-color: #1f77b4;
    }
    .badge {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 4px;
        font-size: 0.78rem;
        font-weight: 600;
        margin-right: 6px;
    }
    .badge-score   { background: #d4edda; color: #155724; }
    .badge-ext     { background: #cce5ff; color: #004085; }
    .badge-type    { background: rgba(128,128,128,0.15); color: var(--text-color, #444); }
    .text-preview  {
        background: var(--background-color, #fff);
        border-radius: 4px;
        padding: 0.6rem 0.8rem;
        font-size: 0.88rem;
        color: var(--text-color, #333);
        line-height: 1.55;
        margin-top: 0.6rem;
    }
    .file-link {
        color: #1f77b4;
        text-decoration: none;
        font-size: 0.82rem;
    }
    .file-link:hover {
        text-decoration: underline;
    }
    /* Полоса релевантности */
    .score-bar-bg {
        display: inline-block;
        width: 60px;
        height: 8px;
        background: rgba(128,128,128,0.2);
        border-radius: 4px;
        vertical-align: middle;
        margin-right: 4px;
    }
    .score-bar-fill {
        height: 100%;
        border-radius: 4px;
        background: linear-gradient(90deg, #28a745, #1f77b4);
    }
    /* Лог-виджет */
    .log-container {
        background: #1e1e1e;
        color: #d4d4d4;
        font-family: 'Consolas', 'Monaco', monospace;
        font-size: 0.78rem;
        line-height: 1.5;
        padding: 0.8rem 1rem;
        border-radius: 6px;
        max-height: 420px;
        overflow-y: auto;
        white-space: pre-wrap;
        word-break: break-all;
    }
    .log-line-error   { color: #f48771; }
    .log-line-warning { color: #dcdcaa; }
    .log-line-info    { color: #9cdcfe; }
    .log-line-debug   { color: #6a9955; }
    /* Прогресс-бар этапов */
    .stage-bar {
        height: 18px;
        border-radius: 4px;
        display: inline-block;
        vertical-align: middle;
    }
    /* Проводник */
    .explorer-breadcrumb {
        font-size: 0.88rem;
        padding: 0.4rem 0.6rem;
        background: var(--secondary-background-color, #f0f0f0);
        border-radius: 4px;
        margin-bottom: 0.6rem;
        word-break: break-all;
    }
    .explorer-dir-row {
        padding: 3px 6px;
        border-radius: 4px;
        cursor: pointer;
    }
    .explorer-dir-row:hover { background: rgba(31,119,180,0.1); }
    .ext-icon { font-size: 1rem; margin-right: 4px; }
</style>
""",
    unsafe_allow_html=True,
)

# ─────────────────────────── session state ─────────────────────────────
_STATE_KEYS = {
    "searcher": None,
    "qdrant_connected": False,
    "last_results": [],
    "last_query": "",
    "last_limit": 10,
    "last_file_type": None,
    "last_content_only": False,
    "trigger_search": False,
    "preset_query": "",
    "stats_cache": None,
    "stats_cache_time": 0.0,
    "index_stats_cache": None,
    "index_stats_cache_time": 0.0,
    # Проводник
    "explorer_path": None,        # текущая папка (str или None = корень каталога)
    "explorer_filter": "",        # фильтр по имени
    "explorer_page": 0,           # страница файлов
}
for _k, _v in _STATE_KEYS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ═══════════════════════════ helpers ═══════════════════════════════════

def _init_searcher(cfg: Dict[str, Any]) -> None:
    """Создать RAGSearcher и сохранить в session_state."""
    try:
        searcher = RAGSearcher(cfg)
        st.session_state.searcher = searcher
        st.session_state.qdrant_connected = searcher.connected
        st.session_state.stats_cache = None  # сбросить кэш
    except Exception as exc:
        st.error(f"Ошибка инициализации: {exc}")


def _get_searcher() -> Optional[RAGSearcher]:
    return st.session_state.searcher


def _get_stats(searcher: RAGSearcher) -> Dict[str, Any]:
    """Получить статистику коллекции с кэшем на 10 секунд."""
    now = time.time()
    if st.session_state.stats_cache and now - st.session_state.stats_cache_time < 10:
        return st.session_state.stats_cache
    stats = searcher.get_collection_stats()
    st.session_state.stats_cache = stats
    st.session_state.stats_cache_time = now
    return stats


def _file_url(full_path: str) -> str:
    """
    Конвертирует Windows-путь в file:// URL.
    Пример: 'O:\\Обмен\\Договоры\\file.pdf' → 'file:///O:/Обмен/Договоры/file.pdf'

    PureWindowsPath.parts возвращает корень диска как 'O:\\' — его нельзя
    передавать в quote(), иначе получим 'O%3A%5C'. Обрабатываем диск отдельно:
    берём drive ('O:') и кодируем только компоненты пути без него.
    """
    try:
        p = PureWindowsPath(full_path)
        if not p.parts:
            return ""
        # drive = 'O:' (без слеша), остальные части кодируем через quote
        drive = p.drive  # 'O:'
        rest_parts = list(p.parts[1:])  # ['Обмен', 'Договоры', 'file.pdf']
        encoded = "/".join(quote(part, safe="") for part in rest_parts)
        return "file:///" + drive + "/" + encoded
    except Exception:
        return ""


def _folder_url(full_path: str) -> str:
    """file:// URL папки, содержащей файл."""
    try:
        p = PureWindowsPath(full_path).parent
        if not p.parts:
            return ""
        drive = p.drive
        rest_parts = list(p.parts[1:])
        encoded = "/".join(quote(part, safe="") for part in rest_parts)
        return "file:///" + drive + "/" + encoded
    except Exception:
        return ""


# ═══════════════════════════ indexing helpers ═══════════════════════════

def _get_index_stats(cfg: Dict[str, Any], force: bool = False) -> Dict[str, Any]:
    """
    Читает state.json и возвращает статистику индексирования.
    Кэш на 15 секунд, чтобы не дёргать диск при каждом rerun.
    """
    now = time.time()
    if (
        not force
        and st.session_state.index_stats_cache
        and now - st.session_state.index_stats_cache_time < 15
    ):
        return st.session_state.index_stats_cache

    db_path = Path(cfg.get("qdrant_db_path", ""))
    state_file = db_path / "index_state.json"

    result: Dict[str, Any] = {
        "found": False,
        "state_file": str(state_file),
        "total": 0,
        "by_ext": {},
        "by_stage": {},
        "last_modified": None,
    }

    if not state_file.exists():
        st.session_state.index_stats_cache = result
        st.session_state.index_stats_cache_time = now
        return result

    try:
        with open(state_file, "r", encoding="utf-8") as fh:
            state = json.load(fh)
    except Exception as exc:
        result["error"] = str(exc)
        return result

    files = state.get("files", {})
    by_ext: Counter = Counter()
    by_stage: Counter = Counter()

    for key, meta in files.items():
        ext = Path(key).suffix.lower() or "(без расширения)"
        by_ext[ext] += 1
        stage = meta.get("stage", "content")
        by_stage[stage] += 1

    try:
        mtime = state_file.stat().st_mtime
        result["last_modified"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(mtime))
    except Exception:
        pass

    result.update({
        "found": True,
        "total": len(files),
        "by_ext": dict(by_ext.most_common()),
        "by_stage": dict(by_stage),
    })

    st.session_state.index_stats_cache = result
    st.session_state.index_stats_cache_time = now
    return result


def _read_log_tail(cfg: Dict[str, Any], n_lines: int = 200) -> List[str]:
    """Читает последние n_lines строк лог-файла."""
    log_file = cfg.get("log_file", "")
    if not log_file:
        return ["⚠ Путь к лог-файлу не указан в config.json (поле 'log_file')."]

    try:
        p = Path(log_file)
        if not p.exists():
            return [f"⚠ Лог-файл не найден: {log_file}"]

        with open(p, "r", encoding="utf-8", errors="replace") as fh:
            lines = fh.readlines()

        return [line.rstrip() for line in lines[-n_lines:]]
    except Exception as exc:
        return [f"⚠ Ошибка чтения лога: {exc}"]


def _colorize_log_line(line: str) -> str:
    """Обернуть строку лога в span нужного цвета (HTML)."""
    escaped = html.escape(line)
    upper = line.upper()
    if " - ERROR" in upper or "ERROR" in upper[:20]:
        return f'<span class="log-line-error">{escaped}</span>'
    if " - WARNING" in upper or "WARNING" in upper[:20] or "WARN" in upper[:20]:
        return f'<span class="log-line-warning">{escaped}</span>'
    if " - DEBUG" in upper:
        return f'<span class="log-line-debug">{escaped}</span>'
    return f'<span class="log-line-info">{escaped}</span>'


# ═══════════════════════════ sidebar ═══════════════════════════════════

def render_sidebar(cfg: Dict[str, Any]):
    """Боковая панель: статус, параметры поиска, быстрый поиск, настройки."""
    st.sidebar.title("Настройки")

    # Статус подключения
    searcher = _get_searcher()
    if st.session_state.qdrant_connected and searcher:
        stats = _get_stats(searcher)
        pts = stats.get("points_count", "?")
        label = f"Qdrant подключён  |  Точек: {pts:,}" if isinstance(pts, int) else f"Qdrant подключён  |  Точек: {pts}"
        st.sidebar.success(label)
    else:
        st.sidebar.error("Нет подключения к Qdrant")

    if st.sidebar.button("Переподключить", use_container_width=True):
        _init_searcher(cfg)
        st.rerun()

    st.sidebar.divider()

    # Параметры поиска
    st.sidebar.subheader("Параметры поиска")

    limit = st.sidebar.slider("Количество результатов", 5, 50, 10, step=5)

    file_type = st.sidebar.selectbox(
        "Тип файла",
        options=["Все", ".docx", ".xlsx", ".xls", ".pdf"],
    )
    file_type_val: Optional[str] = None if file_type == "Все" else file_type

    content_only = st.sidebar.checkbox("Только содержимое (без метаданных)")

    st.sidebar.divider()

    # Быстрый поиск
    st.sidebar.subheader("Быстрый поиск")
    presets = [
        ("Договоры",           "договоры"),
        ("Паспорта",            "паспорта"),
        ("Счета",               "счета на оплату"),
        ("Служебные записки",   "служебная записка"),
        ("Финансовые",          "финансовый отчёт"),
        ("Юридические",         "юридический"),
    ]
    for label, query in presets:
        if st.sidebar.button(label, use_container_width=True):
            st.session_state.preset_query = query
            st.session_state.trigger_search = True
            st.rerun()

    st.sidebar.divider()

    # Настройки путей
    with st.sidebar.expander("Пути (config.json)"):
        new_catalog = st.text_input("Папка каталога", value=cfg.get("catalog_path", ""))
        new_qdrant  = st.text_input("База Qdrant",     value=cfg.get("qdrant_db_path", ""))
        new_log     = st.text_input("Лог файл",         value=cfg.get("log_file", ""))

        if st.button("Сохранить и перезапустить", use_container_width=True):
            cfg["catalog_path"]  = new_catalog
            cfg["qdrant_db_path"] = new_qdrant
            cfg["log_file"]       = new_log
            save_config(cfg)
            st.session_state.searcher = None
            _init_searcher(cfg)
            st.success("Настройки сохранены")
            st.rerun()

    return limit, file_type_val, content_only


# ═══════════════════════════ result card ═══════════════════════════════

def render_result_card(result: Dict[str, Any], index: int) -> None:
    """
    Отрисовать карточку одного результата.
    Все пользовательские данные экранируются через html.escape() для защиты от XSS.
    """
    filename  = html.escape(result.get("filename") or "")
    path_str  = html.escape(result.get("path") or "")
    ext       = html.escape(result.get("extension") or "unknown")
    type_str  = html.escape(result.get("type") or "")
    full_path = result.get("full_path") or ""
    text_raw  = result.get("text") or ""
    text_preview = html.escape(text_raw[:400]) + ("…" if len(text_raw) > 400 else "")

    score    = result.get("score", 0)
    size_mb  = result.get("size_mb")
    modified = result.get("modified")

    pct = max(0, min(100, int(score * 100)))
    score_bar = (
        f'<span class="score-bar-bg"><span class="score-bar-fill" '
        f'style="width:{pct}%"></span></span> {score:.2f}'
    )

    meta_parts = []
    if size_mb is not None:
        meta_parts.append(f"<strong>Размер:</strong> {html.escape(str(size_mb))} МБ")
    if modified:
        meta_parts.append(f"<strong>Изменён:</strong> {html.escape(str(modified)[:10])}")
    meta_html = "  &nbsp;|&nbsp;  ".join(meta_parts)

    furl = _file_url(full_path)
    durl = _folder_url(full_path)
    links_html = ""
    if furl:
        links_html = (
            f'<a class="file-link" href="{html.escape(furl)}" target="_blank">'
            f'Открыть файл</a>'
        )
    if durl:
        links_html += (
            f'&nbsp;&nbsp;|&nbsp;&nbsp;'
            f'<a class="file-link" href="{html.escape(durl)}" target="_blank">'
            f'Открыть папку</a>'
        )

    st.markdown(
        f"""
<div class="result-card">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;">
    <div>
      <strong style="color:#1f77b4;font-size:1rem;">[{index}] {filename}</strong>
    </div>
    <div>
      <span class="badge badge-score">{score_bar}</span>
      <span class="badge badge-ext">{ext}</span>
      <span class="badge badge-type">{type_str}</span>
    </div>
  </div>
  <p style="color:#888;font-size:0.83rem;margin:6px 0 2px;">
    <strong>Путь:</strong> {path_str}
    {("&nbsp;&nbsp;|&nbsp;&nbsp;" + links_html) if links_html else ""}
  </p>
  {"<p style='color:#888;font-size:0.82rem;margin:0;'>" + meta_html + "</p>" if meta_parts else ""}
  <div class="text-preview">{text_preview}</div>
</div>
""",
        unsafe_allow_html=True,
    )


# ═══════════════════════════ explorer tab ══════════════════════════════

_EXT_ICON: Dict[str, str] = {
    ".docx": "📄",
    ".doc":  "📄",
    ".xlsx": "📊",
    ".xls":  "📊",
    ".pdf":  "📕",
    ".txt":  "📝",
    ".csv":  "📋",
    ".zip":  "🗜",
    ".rar":  "🗜",
}
_DIR_ICON  = "📁"
_FILE_ICON = "📎"
_PAGE_SIZE = 100  # файлов на страницу


def _ext_icon(ext: str) -> str:
    return _EXT_ICON.get(ext.lower(), _FILE_ICON)


def render_explorer_tab(cfg: Dict[str, Any]) -> None:
    """Вкладка 'Проводник': навигация по каталогу + таблица файлов."""
    catalog_root = Path(cfg.get("catalog_path", ""))

    # Инициализируем текущий путь при первом открытии
    if st.session_state.explorer_path is None:
        st.session_state.explorer_path = str(catalog_root)

    cur_path = Path(st.session_state.explorer_path)

    # Защита: если путь вышел за пределы каталога — сбрасываем в корень
    try:
        cur_path.relative_to(catalog_root)
    except ValueError:
        cur_path = catalog_root
        st.session_state.explorer_path = str(catalog_root)

    if not cur_path.exists():
        st.error(f"Папка каталога не найдена: `{catalog_root}`")
        st.info("Проверьте путь `catalog_path` в настройках (боковая панель).")
        return

    # ── Хлебные крошки ────────────────────────────────────────────────
    parts = []
    p = cur_path
    while True:
        parts.append(p)
        if p == catalog_root or p == p.parent:
            break
        p = p.parent
    parts.reverse()

    crumb_cols = st.columns([8, 2])
    with crumb_cols[0]:
        # Рендерим крошки как кнопки (inline через columns)
        breadcrumb_buttons = st.columns(len(parts))
        for i, part in enumerate(parts):
            label = ("🏠 Корень" if part == catalog_root else part.name)
            with breadcrumb_buttons[i]:
                if st.button(label, key=f"crumb_{i}_{part}", use_container_width=True):
                    st.session_state.explorer_path = str(part)
                    st.session_state.explorer_page = 0
                    st.session_state.explorer_filter = ""
                    st.rerun()

    with crumb_cols[1]:
        if st.button("⬆ На уровень выше", use_container_width=True,
                     disabled=(cur_path == catalog_root)):
            st.session_state.explorer_path = str(cur_path.parent)
            st.session_state.explorer_page = 0
            st.session_state.explorer_filter = ""
            st.rerun()

    # Текущий путь
    st.markdown(
        f'<div class="explorer-breadcrumb">📂 <strong>{html.escape(str(cur_path))}</strong></div>',
        unsafe_allow_html=True,
    )

    # ── Фильтр + расширение ───────────────────────────────────────────
    fc1, fc2, fc3 = st.columns([4, 2, 2])
    with fc1:
        name_filter = st.text_input(
            "Фильтр по имени",
            value=st.session_state.explorer_filter,
            placeholder="часть имени файла или папки…",
            label_visibility="collapsed",
            key="explorer_filter_input",
        )
        if name_filter != st.session_state.explorer_filter:
            st.session_state.explorer_filter = name_filter
            st.session_state.explorer_page = 0
    with fc2:
        ext_filter = st.selectbox(
            "Расширение",
            options=["Все", ".docx", ".xlsx", ".xls", ".pdf"],
            label_visibility="collapsed",
            key="explorer_ext_filter",
        )
    with fc3:
        sort_by = st.selectbox(
            "Сортировка",
            options=["По имени ↑", "По имени ↓", "По размеру ↓", "По дате ↓"],
            label_visibility="collapsed",
            key="explorer_sort",
        )

    # ── Считываем содержимое папки ────────────────────────────────────
    try:
        entries = list(cur_path.iterdir())
    except PermissionError:
        st.error("Нет доступа к этой папке.")
        return
    except Exception as exc:
        st.error(f"Ошибка чтения папки: {exc}")
        return

    # Разделяем на директории и файлы
    dirs  = [e for e in entries if e.is_dir() and not e.name.startswith(".")]
    files = [e for e in entries if e.is_file()
             and not e.name.startswith("~$")   # пропускаем временные Office
             and not e.name.startswith(".")]

    # Фильтрация
    nf_lower = name_filter.strip().lower()
    if nf_lower:
        dirs  = [d for d in dirs  if nf_lower in d.name.lower()]
        files = [f for f in files if nf_lower in f.name.lower()]
    if ext_filter != "Все":
        files = [f for f in files if f.suffix.lower() == ext_filter]

    # Сортировка директорий (всегда по имени)
    dirs.sort(key=lambda d: d.name.lower())

    # Сортировка файлов
    if sort_by == "По имени ↑":
        files.sort(key=lambda f: f.name.lower())
    elif sort_by == "По имени ↓":
        files.sort(key=lambda f: f.name.lower(), reverse=True)
    elif sort_by == "По размеру ↓":
        files.sort(key=lambda f: f.stat().st_size if f.exists() else 0, reverse=True)
    elif sort_by == "По дате ↓":
        files.sort(key=lambda f: f.stat().st_mtime if f.exists() else 0, reverse=True)

    # ── Счётчик ───────────────────────────────────────────────────────
    total_files = len(files)
    st.caption(
        f"Папок: **{len(dirs)}** &nbsp;|&nbsp; Файлов: **{total_files}**"
        + (f" (фильтр: «{html.escape(name_filter)}»)" if name_filter else ""),
    )

    # ── Список директорий ─────────────────────────────────────────────
    if dirs:
        with st.expander(f"📁 Папки ({len(dirs)})", expanded=True):
            # Выводим в 3 колонки
            n_cols = 3
            for row_start in range(0, len(dirs), n_cols):
                row_dirs = dirs[row_start : row_start + n_cols]
                cols = st.columns(n_cols)
                for col, d in zip(cols, row_dirs):
                    with col:
                        if st.button(
                            f"📁 {d.name}",
                            key=f"dir_{d}",
                            use_container_width=True,
                        ):
                            st.session_state.explorer_path = str(d)
                            st.session_state.explorer_page = 0
                            st.session_state.explorer_filter = ""
                            st.rerun()

    st.divider()

    # ── Таблица файлов (с пагинацией) ────────────────────────────────
    if not files:
        st.info("Нет файлов, соответствующих фильтру.")
        return

    # Пагинация
    page = st.session_state.explorer_page
    n_pages = max(1, (total_files + _PAGE_SIZE - 1) // _PAGE_SIZE)
    page = max(0, min(page, n_pages - 1))

    page_files = files[page * _PAGE_SIZE : (page + 1) * _PAGE_SIZE]

    # Строим таблицу вручную (нужны кликабельные ссылки)
    rows_html = ""
    for f in page_files:
        try:
            stat = f.stat()
            size_b = stat.st_size
            size_str = (
                f"{size_b / 1_048_576:.1f} МБ" if size_b >= 1_048_576
                else f"{size_b / 1024:.1f} КБ" if size_b >= 1024
                else f"{size_b} Б"
            )
            mtime_str = time.strftime("%d.%m.%Y %H:%M", time.localtime(stat.st_mtime))
        except Exception:
            size_str = "—"
            mtime_str = "—"

        ext  = f.suffix.lower()
        icon = _ext_icon(ext)
        name_escaped = html.escape(f.name)
        ext_escaped  = html.escape(ext or "—")

        furl = _file_url(str(f))
        durl = _folder_url(str(f))
        link_file = (
            f'<a class="file-link" href="{html.escape(furl)}" target="_blank">📂 открыть</a>'
            if furl else ""
        )
        link_dir = (
            f'<a class="file-link" href="{html.escape(durl)}" target="_blank">📁 папка</a>'
            if durl else ""
        )
        links = "&nbsp;&nbsp;".join(x for x in [link_file, link_dir] if x)

        rows_html += f"""
<tr>
  <td style="padding:4px 8px;">{icon} {name_escaped}</td>
  <td style="padding:4px 8px; color:#888; font-size:0.82rem;">{ext_escaped}</td>
  <td style="padding:4px 8px; color:#888; font-size:0.82rem; text-align:right;">{html.escape(size_str)}</td>
  <td style="padding:4px 8px; color:#888; font-size:0.82rem;">{html.escape(mtime_str)}</td>
  <td style="padding:4px 8px; font-size:0.82rem;">{links}</td>
</tr>"""

    table_html = f"""
<table style="width:100%; border-collapse:collapse;">
  <thead>
    <tr style="border-bottom:2px solid var(--border-color,#ddd); font-size:0.82rem; color:#666;">
      <th style="padding:4px 8px; text-align:left;">Имя файла</th>
      <th style="padding:4px 8px; text-align:left;">Тип</th>
      <th style="padding:4px 8px; text-align:right;">Размер</th>
      <th style="padding:4px 8px; text-align:left;">Изменён</th>
      <th style="padding:4px 8px; text-align:left;">Действия</th>
    </tr>
  </thead>
  <tbody>
    {rows_html}
  </tbody>
</table>
"""
    st.markdown(table_html, unsafe_allow_html=True)

    # Пагинация
    if n_pages > 1:
        pg_c1, pg_c2, pg_c3 = st.columns([2, 4, 2])
        with pg_c1:
            if st.button("◀ Пред.", disabled=(page == 0), key="exp_prev"):
                st.session_state.explorer_page = page - 1
                st.rerun()
        with pg_c2:
            st.markdown(
                f"<p style='text-align:center;color:#888;margin-top:0.4rem;'>"
                f"Страница {page + 1} из {n_pages} "
                f"(файлы {page * _PAGE_SIZE + 1}–{min((page + 1) * _PAGE_SIZE, total_files)})"
                f"</p>",
                unsafe_allow_html=True,
            )
        with pg_c3:
            if st.button("След. ▶", disabled=(page == n_pages - 1), key="exp_next"):
                st.session_state.explorer_page = page + 1
                st.rerun()


# ═══════════════════════════ indexing tab ══════════════════════════════

def render_indexing_tab(cfg: Dict[str, Any]) -> None:
    """Вкладка 'Индексирование': статистика state.json + просмотр логов."""

    # ── Заголовок + кнопка обновления ────────────────────────────────
    col_h, col_btn = st.columns([5, 1])
    with col_h:
        st.subheader("Статистика индексирования")
    with col_btn:
        if st.button("🔄 Обновить", use_container_width=True, key="idx_refresh"):
            st.session_state.index_stats_cache = None
            st.rerun()

    idx = _get_index_stats(cfg)

    if not idx.get("found"):
        if idx.get("error"):
            st.error(f"Ошибка чтения state.json: {idx['error']}")
        else:
            st.warning(
                f"Файл состояния не найден: `{idx['state_file']}`\n\n"
                "Запустите индексирование: `python index_rag.py`"
            )
    else:
        # ── Метрики ───────────────────────────────────────────────────
        total = idx["total"]
        by_stage = idx.get("by_stage", {})
        content_count = by_stage.get("content", 0)
        metadata_count = by_stage.get("metadata", 0)
        other_count = total - content_count - metadata_count

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("Всего файлов в индексе", f"{total:,}")
        with c2:
            st.metric("Полностью проиндексировано", f"{content_count:,}",
                      help="Файлы с этапом 'content' — содержимое полностью проиндексировано")
        with c3:
            st.metric("Только метаданные", f"{metadata_count:,}",
                      help="Файлы с этапом 'metadata' — ожидают индексирования содержимого (этапы small/large)")
        with c4:
            st.metric("Обновлено", idx.get("last_modified", "—"),
                      help="Время последнего обновления state.json")

        # ── Прогресс-бар этапов ───────────────────────────────────────
        if total > 0:
            pct_content  = content_count / total * 100
            pct_metadata = metadata_count / total * 100
            pct_other    = max(0, 100 - pct_content - pct_metadata)

            st.markdown("**Прогресс по этапам:**")
            bar_html = f"""
<div style="display:flex; height:20px; border-radius:6px; overflow:hidden; width:100%; margin-bottom:0.6rem;">
  <div style="width:{pct_content:.1f}%; background:#28a745;" title="content: {content_count:,}"></div>
  <div style="width:{pct_metadata:.1f}%; background:#ffc107;" title="metadata: {metadata_count:,}"></div>
  <div style="width:{pct_other:.1f}%; background:#dee2e6;" title="прочее: {other_count:,}"></div>
</div>
<div style="font-size:0.82rem; color:#888;">
  <span style="color:#28a745">■</span> content ({content_count:,} &nbsp;·&nbsp; {pct_content:.1f}%)
  &nbsp;&nbsp;
  <span style="color:#ffc107">■</span> metadata ({metadata_count:,} &nbsp;·&nbsp; {pct_metadata:.1f}%)
  &nbsp;&nbsp;
  <span style="color:#adb5bd">■</span> прочее ({other_count:,} &nbsp;·&nbsp; {pct_other:.1f}%)
</div>
"""
            st.markdown(bar_html, unsafe_allow_html=True)

        st.divider()

        # ── Разбивка по расширениям ───────────────────────────────────
        by_ext = idx.get("by_ext", {})
        if by_ext:
            st.markdown("**Файлов по типу:**")
            ext_cols = st.columns(min(len(by_ext), 5))
            for i, (ext, count) in enumerate(by_ext.items()):
                with ext_cols[i % len(ext_cols)]:
                    pct = count / total * 100 if total else 0
                    st.metric(ext or "(нет расширения)", f"{count:,}", delta=f"{pct:.1f}%",
                              delta_color="off")

    st.divider()

    # ── Просмотр лог-файла ────────────────────────────────────────────
    log_file = cfg.get("log_file", "")
    col_lh, col_ln = st.columns([4, 2])
    with col_lh:
        st.subheader("Лог индексирования")
        if log_file:
            st.caption(f"Файл: `{log_file}`")
    with col_ln:
        n_lines = st.selectbox(
            "Последних строк",
            options=[50, 100, 200, 500],
            index=1,
            key="log_n_lines",
            label_visibility="collapsed",
        )

    # Фильтр уровня
    log_level_filter = st.radio(
        "Фильтр уровня",
        options=["Все", "INFO+", "WARNING+", "ERROR"],
        index=0,
        horizontal=True,
        key="log_level_filter",
    )

    lines = _read_log_tail(cfg, n_lines=int(n_lines))

    # Применяем фильтр
    def _passes_filter(line: str) -> bool:
        upper = line.upper()
        if log_level_filter == "Все":
            return True
        if log_level_filter == "ERROR":
            return " - ERROR" in upper or upper.startswith("ERROR")
        if log_level_filter == "WARNING+":
            return (
                " - WARNING" in upper or " - ERROR" in upper
                or upper.startswith("WARNING") or upper.startswith("ERROR")
            )
        # INFO+
        return not (" - DEBUG" in upper or upper.startswith("DEBUG"))

    filtered = [l for l in lines if _passes_filter(l)]

    if not filtered:
        st.info("Нет строк, соответствующих фильтру.")
    else:
        colored_lines = "\n".join(_colorize_log_line(l) for l in filtered)
        st.markdown(
            f'<div class="log-container">{colored_lines}</div>',
            unsafe_allow_html=True,
        )
        st.caption(f"Показано {len(filtered)} строк из {len(lines)} последних в файле.")

    # ── Подсказка по командам ─────────────────────────────────────────
    with st.expander("Команды индексирования"):
        st.code(
            "# Быстрый старт: только метаданные (имена файлов, минуты)\n"
            "python index_rag.py --stage metadata\n\n"
            "# Полный контент: docx/xlsx + небольшие PDF\n"
            "python index_rag.py --stage small\n\n"
            "# Крупные и сканированные PDF (долго)\n"
            "python index_rag.py --stage large\n\n"
            "# Все этапы последовательно (по умолчанию)\n"
            "python index_rag.py\n\n"
            "# Удалить удалённые файлы из индекса\n"
            "python index_rag.py --cleanup\n\n"
            "# Пересоздать коллекцию с нуля\n"
            "python index_rag.py --recreate",
            language="bash",
        )


# ═══════════════════════════ main ══════════════════════════════════════

def main() -> None:
    cfg = load_config()

    # Инициализируем searcher один раз за сессию
    if st.session_state.searcher is None:
        _init_searcher(cfg)

    # Заголовок
    col_title, col_status = st.columns([4, 1])
    with col_title:
        st.title("RAG Каталог — Семантический поиск")
    with col_status:
        if st.session_state.qdrant_connected:
            st.success("Подключено")
        else:
            st.error("Не подключено")

    # Боковая панель
    limit, file_type_val, content_only = render_sidebar(cfg)

    # ── Вкладки ──────────────────────────────────────────────────────
    tab_search, tab_explorer, tab_index = st.tabs(["🔍 Поиск", "📁 Проводник", "📊 Индексирование"])

    # ════════════════════ Вкладка: Поиск ═════════════════════════════
    with tab_search:
        st.divider()

        # Поисковая форма
        initial_query = st.session_state.get("preset_query", "")
        if st.session_state.trigger_search and initial_query:
            st.session_state.trigger_search = False

        with st.form("search_form", clear_on_submit=False):
            query = st.text_input(
                "Поисковый запрос",
                value=initial_query,
                placeholder="Введите что ищете: договоры, паспорта, счета…",
                label_visibility="collapsed",
                key="query_input",
            )
            submitted = st.form_submit_button("Найти", use_container_width=False)

        if initial_query:
            st.session_state.preset_query = ""

        should_search = (submitted or st.session_state.trigger_search) and bool(query.strip())
        st.session_state.trigger_search = False

        # Выполнить поиск
        if should_search:
            searcher = _get_searcher()
            if not searcher or not searcher.connected:
                st.error("Нет подключения к Qdrant. Запустите индексирование и обновите страницу.")
            else:
                with st.spinner("Поиск…"):
                    if searcher._embedder is None:
                        st.info("Первый запуск — загружается модель эмбеддинга (~5 сек)…")
                    results = searcher.search(
                        query.strip(),
                        limit=limit,
                        file_type=file_type_val,
                        content_only=content_only,
                    )
                st.session_state.last_results = results
                st.session_state.last_query = query.strip()
                st.session_state.last_limit = limit
                st.session_state.last_file_type = file_type_val
                st.session_state.last_content_only = content_only

        st.divider()

        # Показать результаты
        results = st.session_state.last_results
        if results:
            params_changed = (
                limit != st.session_state.last_limit
                or file_type_val != st.session_state.last_file_type
                or content_only != st.session_state.last_content_only
            )
            if params_changed:
                st.warning("Параметры изменены. Нажмите «Найти» чтобы обновить результаты.")

            st.success(f"Найдено результатов: {len(results)}")
            for i, r in enumerate(results, 1):
                render_result_card(r, i)
        elif should_search:
            st.info("По вашему запросу ничего не найдено.")
        else:
            c1, c2, c3 = st.columns(3)
            with c1:
                st.info("Введите запрос или используйте быстрый поиск в боковой панели")
            with c2:
                st.info("Например: договоры, паспорта, счета, акты")
            with c3:
                st.info("Фильтруйте по типу файла: .docx, .xlsx, .pdf")

        st.divider()

        # Статистика Qdrant
        searcher = _get_searcher()
        if searcher and st.session_state.qdrant_connected:
            st.subheader("Статистика коллекции")
            stats = _get_stats(searcher)
            if stats:
                c1, c2 = st.columns(2)
                with c1:
                    st.metric("Индексировано точек", f"{stats['points_count']:,}")
                with c2:
                    st.metric("Статус", stats["status"])

    # ════════════════════ Вкладка: Проводник ═════════════════════════
    with tab_explorer:
        st.divider()
        render_explorer_tab(cfg)

    # ════════════════════ Вкладка: Индексирование ═════════════════════
    with tab_index:
        st.divider()
        render_indexing_tab(cfg)

    # Footer
    st.markdown(
        """
<div style="text-align:center;color:#aaa;font-size:0.8rem;padding:1rem 0;">
    RAG Semantic Search &nbsp;|&nbsp; DOCX · XLSX · PDF &nbsp;|&nbsp; Qdrant + all-MiniLM embeddings
</div>
""",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
