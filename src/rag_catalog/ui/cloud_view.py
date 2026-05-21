"""cloud_view.py - Cloud Drive screen for NiceGUI app."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from nicegui import ui

from rag_catalog.core.cloud_drive.service import CloudDriveService

from .helpers import _is_admin
from .state import PageState


def render_cloud_drive_screen(
    state: PageState,
    *,
    render_fn: Optional[Callable[[], None]] = None,
    settings_fn: Optional[Callable[[str], None]] = None,
) -> None:
    try:
        service = CloudDriveService.from_config(state.cfg)
        health: Dict[str, Any] = service.storage_health() if service else {}
    except Exception:
        health = {}
        service = None

    with ui.row().classes("w-full items-start justify-between gap-3 flex-wrap"):
        with ui.column().classes("gap-1"):
            ui.label("CLOUD STORAGE").style(
                "font-family:var(--rag-font-mono);text-transform:uppercase;"
                "letter-spacing:0.1em;color:var(--rag-accent);font-size:10px;font-weight:700"
            )
            with ui.row().classes("items-center gap-3"):
                ui.label("Cloud Drive").style(
                    "font-family:var(--rag-font-display);font-weight:800;"
                    "font-size:32px;letter-spacing:-0.03em;margin:0"
                )
                ui.label("NEW").style(
                    "font-family:var(--rag-font-mono);font-size:11px;font-weight:600;"
                    "padding:3px 8px;border-radius:6px;"
                    "background:color-mix(in srgb,#22d3ee 14%,transparent);"
                    "color:#06b6d4;border:1px solid color-mix(in srgb,#22d3ee 30%,transparent);"
                    "text-transform:uppercase;letter-spacing:0.08em"
                )
        with ui.row().classes("gap-2"):
            ui.button(
                "Загрузить",
                icon="upload",
                on_click=lambda: ui.navigate.to("/explorer"),
            ).props("unelevated no-caps")
            if _is_admin(state) and settings_fn is not None:
                ui.button(
                    icon="settings",
                    on_click=lambda: settings_fn("cloud_drive"),
                    color=None,
                ).props("flat round dense").tooltip("Инфраструктура Cloud")

    tab_options = {
        "files": "Файлы",
        "sync": "Синхронизация",
        "settings": "Настройки",
    }

    def set_tab(value: str) -> None:
        if value in tab_options:
            state.cloud_tab = value
        if render_fn is not None:
            render_fn()

    with ui.tabs(value=state.cloud_tab if state.cloud_tab in tab_options else "files").classes("rag-cloud-tabs"):
        for key, label in tab_options.items():
            ui.tab(key, label=label, icon={"files": "folder", "sync": "sync", "settings": "tune"}[key]).on(
                "click", lambda _=None, k=key: set_tab(k)
            )

    active_tab = state.cloud_tab if state.cloud_tab in tab_options else "files"
    if active_tab == "sync":
        _render_sync_tab(state, service, settings_fn=settings_fn)
    elif active_tab == "settings":
        _render_settings_tab(state, service, render_fn=render_fn, settings_fn=settings_fn)
    else:
        _render_files_tab(service, health)


def _render_files_tab(service: Any, health: Dict[str, Any]) -> None:
    has_files = int(health.get("file_count") or 0) > 0
    if has_files and service is not None:
        _render_with_files(service, health)
        return
    _render_empty_state()


def _render_sync_tab(
    state: PageState,
    service: Any,
    *,
    settings_fn: Optional[Callable[[str], None]] = None,
) -> None:
    with ui.column().classes("rag-card w-full p-4 gap-3"):
        with ui.row().classes("w-full items-center gap-2"):
            ui.icon("sync", size="22px")
            ui.label("Синхронизация Cloud").classes("text-xl font-semibold")
            ui.space()
            if settings_fn is not None:
                ui.button(
                    "Пары синхронизации",
                    icon="tune",
                    on_click=lambda: settings_fn("cloud_sync" if _is_admin(state) else "cloud_sync_user"),
                ).props("outline dense no-caps")
        ui.label("Подключённые клиенты, пары папок и конфликты ведутся в общей Cloud-зоне.").classes("rag-meta")
        if service is None:
            ui.label("Cloud Drive не настроен.").classes("rag-meta")
            return
        try:
            clients = service.list_sync_clients(include_offline=True, limit=8)
        except Exception:
            clients = []
        if not clients:
            with ui.element("div").classes("cd-empty-state w-full"):
                ui.icon("devices", size="28px").classes("opacity-30")
                ui.label("Синхронизация пока не подключена.").classes("text-center")
            return
        with ui.column().classes("w-full gap-2"):
            for client in clients:
                with ui.row().classes("rag-explorer-item w-full p-2 items-center gap-3"):
                    ui.icon("computer", size="18px")
                    with ui.column().classes("flex-1 min-w-0 gap-0"):
                        ui.label(str(client.get("device_name") or client.get("device_id") or "Устройство")).classes("font-medium truncate")
                        ui.label(str(client.get("status") or "unknown")).classes("rag-meta text-xs")


def _render_settings_tab(
    state: PageState,
    service: Any,
    *,
    render_fn: Optional[Callable[[], None]] = None,
    settings_fn: Optional[Callable[[str], None]] = None,
) -> None:
    def open_cloud_tab(tab: str) -> None:
        state.cloud_tab = tab
        if render_fn is not None:
            render_fn()

    with ui.column().classes("rag-card w-full p-4 gap-3"):
        with ui.row().classes("w-full items-center gap-2"):
            ui.icon("tune", size="22px")
            ui.label("Настройки Cloud").classes("text-xl font-semibold")
        ui.label("Пользовательские сценарии Cloud доступны здесь. Инфраструктура хранилища остаётся в настройках администратора.").classes("rag-meta")
        with ui.row().classes("gap-2 flex-wrap"):
            ui.button("Открыть файлы", icon="folder", on_click=lambda: open_cloud_tab("files")).props("outline dense no-caps")
            ui.button("Синхронизация", icon="sync_alt", on_click=lambda: open_cloud_tab("sync")).props("outline dense no-caps")
            if settings_fn is not None:
                if _is_admin(state):
                    ui.button("Инфраструктура Cloud", icon="storage", on_click=lambda: settings_fn("cloud_drive")).props("outline dense no-caps")
        if service is not None:
            try:
                health = service.storage_health()
            except Exception:
                health = {}
            if health:
                with ui.row().classes("gap-2 flex-wrap"):
                    ui.label(f"Файлов: {int(health.get('file_count') or 0):,}".replace(",", " ")).classes("rag-chip")
                    ui.label(f"Очередь: {int(health.get('queue_size') or 0)}").classes("rag-chip")


def _render_with_files(service: Any, health: Dict[str, Any]) -> None:
    with ui.element("div").classes("rag-cloud-kpi-grid"):
        _kpi("ФАЙЛОВ В CLOUD",
             f"{int(health.get('file_count') or 0):,}".replace(",", " "),
             f"+{int(health.get('files_added_24h') or 0)} за сутки")
        _kpi("ЗАНЯТО",
             _fmt_size(int(health.get("total_size") or 0)),
             f"из {_fmt_size(int(health.get('quota') or 50 * 1024 ** 3))}")
        _kpi("ИНДЕКС CLOUD",
             "OK" if health.get("index_ok") else "-",
             str(health.get("index_last_run", "-")))
        _kpi("ОЧЕРЕДЬ",
             str(int(health.get("queue_size") or 0)),
             "OCR / reindex")

    _render_queue(service)
    _render_file_grid(service)


def _kpi(label: str, value: str, sub: str) -> None:
    with ui.element("div").classes("rag-cloud-kpi"):
        ui.label(label).classes("rag-cloud-kpi-label")
        ui.label(value).classes("rag-cloud-kpi-value")
        ui.label(sub).classes("rag-cloud-kpi-sub")


def _render_queue(service: Any) -> None:
    try:
        jobs: List[Any] = service.list_bootstrap_jobs(limit=5)
    except Exception:
        return
    if not jobs:
        return
    ui.label("ОЧЕРЕДЬ ОБРАБОТКИ").style(
        "margin:24px 0 12px;font-family:var(--rag-font-mono);"
        "text-transform:uppercase;letter-spacing:0.1em;font-size:10px;font-weight:700;"
        "color:var(--rag-muted)"
    )
    with ui.column().classes("gap-2 w-full"):
        for j in jobs:
            status = str(getattr(j, "status", None) or j.get("status", "pending") if isinstance(j, dict) else j.status)
            label_text = str(getattr(j, "label", None) or (j.get("label") if isinstance(j, dict) else "") or "Задача")
            progress = float(getattr(j, "progress", None) or (j.get("progress") if isinstance(j, dict) else 0) or 0)
            with ui.element("div").classes("rag-cloud-job-row" + (" running" if status == "running" else "")):
                ui.icon("autorenew" if status == "running" else "check_circle_outline", size="20px")\
                    .style("color:var(--rag-accent)" if status == "running" else "color:var(--rag-muted)")
                with ui.column().classes("gap-0 min-w-0"):
                    ui.label(label_text).classes("rag-cloud-job-name")
                    ui.label(f"{status} bootstrap").classes("rag-cloud-job-meta")
                ui.linear_progress(value=progress).props("color=indigo-5" if status == "running" else "")
                ui.label(f"{int(progress * 100)}%")\
                    .style("font-family:var(--rag-font-mono);font-size:11px;text-align:right")


def _render_file_grid(service: Any) -> None:
    try:
        listing = service.list_directory(path="")
        items: List[Dict[str, Any]] = listing.get("items", [])[:24]
    except Exception:
        items = []
    ui.label("ВСЕ ФАЙЛЫ").style(
        "margin:32px 0 12px;font-family:var(--rag-font-mono);"
        "text-transform:uppercase;letter-spacing:0.1em;font-size:10px;font-weight:700;"
        "color:var(--rag-muted)"
    )
    with ui.element("div").style(
        "display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:8px"
    ):
        for f in items:
            name = str(f.get("name") or "")
            ext = (name.rsplit(".", 1)[-1] if "." in name else "txt").lower()
            with ui.element("div").classes("rag-explorer-item p-3")\
                    .style("display:flex;flex-direction:column;gap:8px;cursor:pointer"):
                ui.icon("description", size="32px").style(f"color:{_ext_color(ext)}")
                ui.label(name).style(
                    "font-size:12px;font-weight:500;line-height:1.3;"
                    "overflow:hidden;text-overflow:ellipsis;display:-webkit-box;"
                    "-webkit-line-clamp:2;-webkit-box-orient:vertical"
                )
                with ui.row().classes("justify-between items-center mt-auto"):
                    ui.label(_fmt_size(int(f.get("size") or 0))).classes("rag-meta")


def _render_empty_state() -> None:
    with ui.element("div").classes("rag-cloud-hero"):
        with ui.element("div").classes("rag-cloud-hero-icon"):
            ui.icon("cloud", size="36px")
        ui.label("Cloud Drive пока пуст").classes("rag-cloud-hero-title")
        ui.label(
            "Загрузите документы — они автоматически попадут в семантический "
            "индекс и станут доступны для поиска всей команде (с учётом прав)."
        ).classes("rag-cloud-hero-subtitle")

    _ACTIONS = [
        ("upload", "Загрузить файлы", "Drag-and-drop или диалог выбора. До 500 МБ.", "Выбрать файлы", True),
        ("create_new_folder", "Создать папку", "Структурируйте файлы как удобно.", "Создать", False),
        ("cloud_sync", "Сторонний диск", "Google Drive, OneDrive, Yandex.Disk, S3.", "Подключить", False),
        ("search", "Поиск", "Найдите загруженные файлы в общем поиске.", "Открыть поиск", False),
    ]
    with ui.element("div").classes("rag-cloud-action-grid"):
        for icon, title, desc, cta, featured in _ACTIONS:
            with ui.element("div").classes("rag-cloud-action-card" + (" featured" if featured else "")):
                with ui.element("div").classes("rag-cloud-action-card-icon"):
                    ui.icon(icon, size="24px")
                with ui.column().classes("gap-1"):
                    ui.label(title).classes("rag-cloud-action-card-title")
                    ui.label(desc).classes("rag-cloud-action-card-desc")
                with ui.element("div").classes("rag-cloud-action-card-cta"):
                    ui.label(cta)
                    ui.icon("arrow_forward", size="14px")

    with ui.element("div").classes("rag-cloud-drop-zone"):
        ui.icon("cloud_upload", size="28px").style("color:var(--rag-muted);margin-bottom:8px")
        ui.label("Перетащите файлы в эту область").classes("rag-cloud-drop-zone-title")
        ui.label("docx / xlsx / pdf / jpg / png / txt / до 500 МБ").classes("rag-cloud-drop-zone-meta")


def _fmt_size(b: int) -> str:
    for u in ("Б", "КБ", "МБ", "ГБ", "ТБ"):
        if b < 1024:
            return f"{b} {u}" if u == "Б" else f"{b:.1f} {u}"
        b //= 1024
    return f"{b:.1f} ПБ"


_EXT_COLORS = {
    "pdf": "#dc2626", "doc": "#2563eb", "docx": "#2563eb",
    "xls": "#16a34a", "xlsx": "#16a34a",
    "jpg": "#a855f7", "jpeg": "#a855f7", "png": "#a855f7",
}


def _ext_color(ext: str) -> str:
    return _EXT_COLORS.get(ext, "#64748b")
