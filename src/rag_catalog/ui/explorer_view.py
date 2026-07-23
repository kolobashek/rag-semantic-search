"""
explorer_view.py — Explorer / Cloud Drive browser screen renderer.

Depends on: .state, .helpers, .system, nicegui, rag_catalog.core.
Imported by: nice_app.py.
"""

from __future__ import annotations

import html
import json
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import quote

from nicegui import events, run, ui

from rag_catalog.core.cloud_drive import CloudDriveService

from .helpers import (
    PAGE_SIZE,
    _apply_explorer_filter_input,
    _cd_breadcrumb_chain,
    _cd_file_jobs_map,
    _cd_file_size,
    _cd_get_service,
    _cd_list_children,
    _file_badge_html,
    _file_icon_svg,
    _file_rows,
    _format_file_size,
    _is_admin,
    _is_system_file,
    _open_os_path,
    _safe_explorer_path,
    _save_explorer_settings,
    _save_ui_settings,
    _select_in_os_explorer,
    _viewer_file_url,
)
from .state import (
    PageState,
    _get_auth_db,
    _is_favorite,
    _log_app_event,
    _toggle_favorite,
    _username,
)

_EXPLORER_PAGE_SIZE = 40
_TREE_CHILD_LIMIT = 24
_CLOUD_FILES_DOWNLOAD_URL = "/api/cloud-drive/sync/client-download?format=cloud-files-exe&v=0.4.3"


def _cloud_node_modified_timestamp(node: Any) -> float:
    source_mtime = float(getattr(node, "source_mtime", 0.0) or 0.0)
    if source_mtime > 0:
        return source_mtime
    raw = str(getattr(node, "updated_at", "") or "").strip()
    if not raw:
        return 0.0
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError, OSError):
        return 0.0


def _cloud_node_modified_label(node: Any) -> str:
    timestamp = _cloud_node_modified_timestamp(node)
    if timestamp <= 0:
        return "—"
    return time.strftime("%d.%m.%Y %H:%M", time.localtime(timestamp))


def render_explorer_screen(
    state: PageState,
    *,
    render_fn: Callable,
    go_explorer_fn: Callable,
    open_file_viewer_fn: Callable,
    open_cloud_file_viewer_fn: Callable | None = None,
    choose_query_fn: Callable,
    query_handler: Callable,
) -> None:
    state._explorer_selection_action = None

    def _overflow_title_prop(value: object) -> str:
        return f'data-rag-overflow-title="{html.escape(str(value), quote=True)}"'

    def render_star(path: Path, *, item_type: Optional[str] = None) -> None:
        active = _is_favorite(state, str(path))
        icon = "star" if active else "star_border"
        star = ui.button(icon=icon, color=None).props("flat round dense data-rag-favorite-button")
        star.classes("rag-favorite-star active" if active else "rag-favorite-star")
        star.tooltip("Убрать из избранного" if active else "Добавить в избранное")

        def toggle() -> None:
            _toggle_favorite(state, path, item_type=item_type)
            render_fn()

        star.on("click.stop", toggle)

    def _selection_key(scope: str, path: str) -> str:
        return f"{scope}:{path}"

    def _selection_path(key: str) -> str:
        return key.split(":", 1)[1] if ":" in key else key

    def _selected_set() -> set[str]:
        return set(getattr(state, "explorer_selected_paths", []) or [])

    def _selected_paths(scope: str) -> List[str]:
        prefix = f"{scope}:"
        return [_selection_path(key) for key in getattr(state, "explorer_selected_paths", []) if key.startswith(prefix)]

    def _hidden_key(scope: str, path: str) -> str:
        return _selection_key(scope, str(path or "").strip().replace("\\", "/").strip("/"))

    def _hidden_set() -> set[str]:
        return set(getattr(state, "explorer_hidden_paths", []) or [])

    def _is_hidden(scope: str, path: str) -> bool:
        return _hidden_key(scope, path) in _hidden_set()

    def _set_hidden(scope: str, paths: List[str], hidden: bool) -> None:
        keys = _hidden_set()
        for path in paths:
            key = _hidden_key(scope, path)
            if not key.endswith(":"):
                if hidden:
                    keys.add(key)
                else:
                    keys.discard(key)
        state.explorer_hidden_paths = sorted(keys)
        _save_explorer_settings(state)

    def _set_selected(keys: set[str]) -> None:
        state.explorer_selected_paths = sorted(keys)

    def _toggle_selected(key: str) -> None:
        selected = _selected_set()
        if key in selected:
            selected.remove(key)
        else:
            selected.add(key)
        _set_selected(selected)

    def _set_key_selected(key: str, selected_value: bool) -> None:
        selected = _selected_set()
        if selected_value:
            selected.add(key)
        else:
            selected.discard(key)
        _set_selected(selected)

    def _clear_selection() -> None:
        state.explorer_selected_paths = []

    def _refresh_selection_bar(refs: dict[str, Any]) -> None:
        scope = str(refs.get("scope") or "")
        selected = [key for key in state.explorer_selected_paths if key.startswith(f"{scope}:")]
        visible = set(refs.get("visible_keys") or [])
        selected_set = _selected_set()
        refs["bar"].set_visibility(bool(selected))
        refs["label"].set_text(f"Выбрано: {len(selected)}")
        page_checkbox = refs.get("page_checkbox")
        if page_checkbox is not None:
            all_visible = bool(visible) and visible.issubset(selected_set)
            partial_visible = bool(visible & selected_set) and not all_visible
            refs["syncing"] = True
            try:
                if bool(getattr(page_checkbox, "value", False)) != all_visible:
                    page_checkbox.set_value(all_visible)
            finally:
                refs["syncing"] = False
            page_checkbox.classes(remove="rag-select-empty rag-select-partial rag-select-all")
            page_checkbox.classes(add="rag-select-all" if all_visible else ("rag-select-partial" if partial_visible else "rag-select-empty"))

    def _set_visible_checkboxes(refs: dict[str, Any], value: bool) -> None:
        for key, checkbox in dict(refs.get("checkboxes") or {}).items():
            if key in set(refs.get("visible_keys") or []):
                checkbox.set_value(value)

    def _toggle_visible_selection(refs: dict[str, Any]) -> None:
        visible = set(refs.get("visible_keys") or [])
        should_select = bool(visible) and not visible.issubset(_selected_set())
        _set_selected((_selected_set() | visible) if should_select else (_selected_set() - visible))
        _set_visible_checkboxes(refs, should_select)
        _refresh_selection_bar(refs)

    def _selection_action(scope: str, action: str) -> None:
        handler = getattr(state, "_explorer_selection_action", None)
        if callable(handler):
            handler(scope, action)
            return
        ui.notify("Действие для этого режима пока недоступно.", type="warning")

    def _render_selection_bar(*, scope: str, visible_keys: List[str]) -> dict[str, Any]:
        refs: dict[str, Any] = {
            "scope": scope,
            "visible_keys": list(visible_keys),
            "checkboxes": {},
        }
        refs["bar"] = ui.row().classes("rag-selection-bar w-full items-center gap-1")
        with refs["bar"]:
            ui.icon("checklist", size="18px")
            refs["label"] = ui.label("").classes("font-semibold")
            ui.button(icon="content_copy", on_click=lambda: _selection_action(scope, "copy"), color=None).props("flat round dense").tooltip("Копировать")
            ui.button(icon="content_cut", on_click=lambda: _selection_action(scope, "cut"), color=None).props("flat round dense").tooltip("Вырезать")
            ui.button(icon="delete_outline", on_click=lambda: _selection_action(scope, "delete"), color=None).props("flat round dense").tooltip("Удалить")
            ui.button(icon="ios_share", on_click=lambda: _selection_action(scope, "share"), color=None).props("flat round dense").tooltip("Поделиться")
            ui.button(icon="send", on_click=lambda: _selection_action(scope, "send"), color=None).props("flat round dense").tooltip("Отправить")
            ui.button(icon="archive", on_click=lambda: _selection_action(scope, "archive"), color=None).props("flat round dense").tooltip("Архивировать")
            ui.button(icon="visibility_off", on_click=lambda: _selection_action(scope, "hide"), color=None).props("flat round dense").tooltip("Скрыть из интерфейса")
            ui.button(
                icon="close",
                on_click=lambda: (_clear_selection(), _set_visible_checkboxes(refs, False), _refresh_selection_bar(refs)),
                color=None,
            ).props("flat round dense").tooltip("Снять выделение")
        _refresh_selection_bar(refs)
        return refs

    def _selection_page_checkbox(refs: dict[str, Any]) -> None:
        checkbox = ui.checkbox(value=False).props("dense").classes("rag-select-checkbox rag-select-page-checkbox")

        def _on_page_toggle(_: Any) -> None:
            if refs.get("syncing"):
                return
            _toggle_visible_selection(refs)

        checkbox.on_value_change(_on_page_toggle)
        refs["page_checkbox"] = checkbox
        _refresh_selection_bar(refs)

    def _selection_checkbox(key: str, refs: dict[str, Any]) -> Any:
        checkbox = ui.checkbox(
            value=key in _selected_set(),
            on_change=lambda e, k=key: (_set_key_selected(k, bool(e.value)), _refresh_selection_bar(refs)),
        ).props("dense").classes("rag-select-checkbox")
        refs.setdefault("checkboxes", {})[key] = checkbox
        return checkbox

    def _selection_badge(_path_or_ext: str, _kind: str, key: str, refs: dict[str, Any]) -> None:
        with ui.element("div").classes("rag-file-select-icon"):
            checkbox = _selection_checkbox(key, refs)
            checkbox.classes(add="rag-table-select-checkbox")

    # ── Explorer / Cloud Drive screen ─────────────────────────────────────────

    def _render_cd_explorer(page_state: PageState, svc: "CloudDriveService") -> None:  # noqa: PLR0912,PLR0915
        """Registry-backed Cloud Drive explorer screen."""
        from rag_catalog.core.cloud_drive.models import CloudDriveFile, CloudDriveFolder  # noqa: PLC0415

        if page_state.explorer_view not in {"Таблица", "Список"}:
            page_state.explorer_view = "Таблица"

        try:
            fresh_user = _get_auth_db(page_state).get_user(username=_username(page_state))
            if fresh_user is not None:
                page_state.current_user = fresh_user
        except Exception:
            if page_state.current_user is not None:
                page_state.current_user["group_ids"] = []
                page_state.current_user["groups"] = []

        def _cd_can(path: str, level: str = "viewer") -> bool:
            return svc.user_can_access(
                username=_username(page_state),
                role=str((page_state.current_user or {}).get("role") or ""),
                groups=[str(group_id) for group_id in ((page_state.current_user or {}).get("group_ids") or [])],
                path=path,
                required_level=level,
            )

        def _cd_open_folder(cd_path: str) -> None:
            page_state.explorer_cd_path = cd_path
            page_state.explorer_page = 0
            page_state.explorer_visible_count = _EXPLORER_PAGE_SIZE
            page_state.explorer_selected_paths = []
            _log_app_event(page_state, "cd_explorer", "open_folder", details={"cd_path": cd_path})
            render_fn()

        def _cd_open_tree_dialog() -> None:
            with ui.dialog() as dlg, ui.card().classes("rag-mobile-panel-dialog p-3 gap-2"):
                with ui.row().classes("w-full items-center gap-2"):
                    ui.icon("account_tree", size="20px")
                    ui.label("Дерево").classes("font-semibold flex-1")
                    ui.button(icon="close", on_click=dlg.close, color=None).props("flat round dense")
                if root_folder is None:
                    ui.label("Реестр пуст.").classes("rag-meta")
                else:
                    ancestor_paths = {folder.path for folder in breadcrumbs}
                    open_paths = set(page_state.explorer_tree_open) | ancestor_paths | {root_folder.path}

                    def _dialog_node(folder: CloudDriveFolder, depth: int) -> None:
                        children = svc.registry.list_child_folders(folder.id)
                        is_current = folder.path == cd_path or (not cd_path and folder.is_root)
                        label = "Корень" if folder.is_root else folder.name
                        with ui.row().classes("rag-tree-row" + (" active" if is_current else "")).style(f"padding-left:{depth * 12}px"):
                            ui.button(
                                label,
                                icon="folder_open" if folder.path in open_paths or is_current else "folder",
                                on_click=lambda p=folder.path: (dlg.close(), _cd_open_folder(p)),
                                color=None,
                            ).props(f"flat align=left no-caps dense {_overflow_title_prop(folder.path or 'Корень')}").classes(
                                "rag-nav-button rag-tree-button rag-tree-label"
                            )
                        if folder.path in open_paths:
                            for child in children:
                                _dialog_node(child, depth + 1)

                    with ui.column().classes("w-full gap-0 rag-mobile-panel-body"):
                        _dialog_node(root_folder, 0)
            dlg.open()

        def _cd_open_filters_dialog() -> None:
            with ui.dialog() as dlg, ui.card().classes("rag-mobile-panel-dialog p-3 gap-3"):
                with ui.row().classes("w-full items-center gap-2"):
                    ui.icon("filter_alt", size="20px")
                    ui.label("Фильтры и вид").classes("font-semibold flex-1")
                    ui.button(icon="close", on_click=dlg.close, color=None).props("flat round dense")
                ui.label(f"Папок: {len(child_folders)} · Файлов: {total_files}").classes("rag-meta")
                ui.label(f"Тип: {page_state.explorer_ext}").classes(
                    "rag-chip rag-filter-chip" + (" active" if page_state.explorer_ext != "Все" else "")
                )
                view_select = ui.select(
                    ["Таблица", "Список"],
                    value=page_state.explorer_view if page_state.explorer_view in ("Таблица", "Список") else "Таблица",
                    label="Вид",
                ).props("dense outlined").classes("w-full")
                sort_select = ui.select(
                    ["По имени", "По размеру", "По дате"],
                    value=page_state.explorer_sort,
                    label="Сортировка",
                ).props("dense outlined").classes("w-full")
                sort_direction = ui.select(
                    {"asc": "По возрастанию", "desc": "По убыванию"},
                    value="desc" if page_state.explorer_desc else "asc",
                    label="Направление",
                ).props("dense outlined emit-value map-options").classes("w-full")
                show_hidden_cb = ui.checkbox(
                    "Показать скрытые",
                    value=bool(page_state.explorer_show_hidden),
                ).props("dense")

                def _apply_mobile_filters() -> None:
                    page_state.explorer_view = str(view_select.value or "Таблица")
                    page_state.explorer_sort = str(sort_select.value or "По имени")
                    page_state.explorer_desc = str(sort_direction.value or "asc") == "desc"
                    page_state.explorer_show_hidden = bool(show_hidden_cb.value)
                    page_state.explorer_visible_count = _EXPLORER_PAGE_SIZE
                    _save_explorer_settings(page_state)
                    dlg.close()
                    render_fn()

                ui.button("Применить", icon="check", on_click=_apply_mobile_filters).props("unelevated dense no-caps").classes("w-full")
            dlg.open()

        async def _cd_upload_dialog() -> None:
            """File-picker dialog that uploads files to the current Cloud Drive folder."""
            with ui.dialog() as dlg, ui.card().classes("p-4 gap-3 w-96"):
                ui.label("Загрузить файлы").classes("text-lg font-semibold")
                parent_label = page_state.explorer_cd_path or "/"
                ui.label(f"В папку: {parent_label}").classes("rag-path text-xs")
                upload_results: list[dict] = []

                async def _handle_upload(e: Any) -> None:
                    if not _cd_can(page_state.explorer_cd_path or "", "editor"):
                        ui.notify("Нет прав на загрузку в эту папку.", type="negative")
                        return
                    filename = str(getattr(e, "name", "") or "").strip()
                    content = getattr(e, "content", None)
                    if not filename or content is None:
                        return
                    import tempfile  # noqa: PLC0415
                    suffix = Path(filename).suffix
                    try:
                        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                            tmp.write(content.read())
                            tmp_path = tmp.name
                        await run.io_bound(
                            svc.upload_file,
                            parent_path=page_state.explorer_cd_path or "",
                            filename=filename,
                            source_path=tmp_path,
                            mime_type="",
                        )
                        Path(tmp_path).unlink(missing_ok=True)
                        upload_results.append({"name": filename, "ok": True})
                        _log_app_event(
                            page_state, "cd_explorer", "upload_file",
                            details={"parent": page_state.explorer_cd_path, "name": filename},
                        )
                        ui.notify(f"Файл «{filename}» загружен.", type="positive")
                    except Exception as exc:
                        upload_results.append({"name": filename, "ok": False, "err": str(exc)})
                        ui.notify(f"Ошибка загрузки «{filename}»: {exc}", type="negative")

                ui.upload(
                    multiple=True,
                    on_upload=_handle_upload,
                    auto_upload=True,
                    label="Перетащите файлы сюда или нажмите для выбора",
                ).props("flat bordered").classes("w-full")

                with ui.row().classes("w-full justify-end gap-2 mt-2"):
                    ui.button(
                        "Закрыть", icon="check",
                        on_click=lambda: (dlg.close(), render_fn()),
                    ).props("unelevated dense")
            dlg.open()

        with ui.dialog() as cloud_files_install_dialog, ui.card().classes(
            "p-5 gap-4 w-full max-w-lg"
        ):
            with ui.row().classes("w-full items-center gap-3"):
                ui.icon("cloud_download", size="28px").classes("text-indigo-400")
                with ui.column().classes("flex-1 min-w-0 gap-0"):
                    ui.label("Облако для Windows").classes("text-lg font-semibold")
                    ui.label("Файлы по запросу").classes("rag-meta text-xs")
            ui.label(
                "Приложение показывает в Проводнике Windows все доступные файлы. "
                "Содержимое скачивается только при первом открытии, поэтому весь диск "
                "не занимает место на компьютере."
            ).classes("rag-meta text-sm")
            ui.separator()
            ui.link(
                "Скачать приложение для Windows",
                target=_CLOUD_FILES_DOWNLOAD_URL,
                new_tab=False,
            ).classes("rag-path text-sm font-semibold")
            ui.label(
                "После запуска войдите через браузер под своей корпоративной учётной записью."
            ).classes("rag-meta text-xs")
            with ui.row().classes("w-full justify-end"):
                ui.button("Закрыть", on_click=cloud_files_install_dialog.close).props("flat dense")

        async def _cd_versions_dialog(file: "Any") -> None:
            """Show version history for a Cloud Drive file."""
            with ui.dialog() as dlg, ui.card().classes("p-4 gap-3 w-[480px]"):
                ui.label(f"Версии: {file.name}").classes("text-lg font-semibold")
                ui.label(file.path).classes("rag-path text-xs")
                ui.separator()
                try:
                    result = await run.io_bound(svc.list_versions, file.path)
                    versions = result.get("versions", [])
                    if not versions:
                        with ui.element("div").classes("cd-empty-state w-full"):
                            ui.icon("history", size="24px").classes("opacity-30")
                            ui.label("История версий пуста.").classes("text-center")
                    else:
                        current_id = str(result.get("file", {}).get("current_version_id") or "")
                        for ver in versions:
                            is_cur = str(ver.get("id", "")) == current_id
                            with ui.row().classes("w-full items-center gap-2 py-1"):
                                ui.icon(
                                    "radio_button_checked" if is_cur else "radio_button_unchecked",
                                    size="16px",
                                ).classes("text-indigo-500" if is_cur else "text-slate-400")
                                with ui.column().classes("flex-1 gap-0"):
                                    ts = str(ver.get("created_at") or "")
                                    label = f"Текущая · {ts[:19].replace('T', ' ')}" if is_cur else ts[:19].replace("T", " ")
                                    ui.label(label).classes("text-sm" + (" font-semibold" if is_cur else " rag-meta"))
                                    size = int(ver.get("size_bytes") or 0)
                                    if size:
                                        ui.label(_cd_file_size(size)).classes("rag-meta text-xs")
                except Exception as exc:
                    ui.label(f"Не удалось загрузить версии: {exc}").classes("text-negative text-sm")
                with ui.row().classes("w-full justify-end mt-2"):
                    ui.button("Закрыть", on_click=dlg.close).props("flat dense")
            dlg.open()

        async def _cd_create_dialog(item_type: str) -> None:
            """Show an inline dialog to create a folder or blank Cloud Drive file."""
            type_meta = {
                "folder": {
                    "title": "Новая папка",
                    "input": "Имя папки",
                    "placeholder": "Новая папка",
                    "default": "",
                    "icon": "create_new_folder",
                    "event": "create_folder",
                },
                "word": {
                    "title": "Новый документ Word",
                    "input": "Имя документа",
                    "placeholder": "Новый документ.docx",
                    "default": "Новый документ.docx",
                    "icon": "description",
                    "event": "create_word",
                },
                "excel": {
                    "title": "Новая таблица Excel",
                    "input": "Имя таблицы",
                    "placeholder": "Новая таблица.xlsx",
                    "default": "Новая таблица.xlsx",
                    "icon": "table_chart",
                    "event": "create_excel",
                },
                "text": {
                    "title": "Новый текстовый файл",
                    "input": "Имя файла",
                    "placeholder": "Новый файл.txt",
                    "default": "Новый файл.txt",
                    "icon": "article",
                    "event": "create_text",
                },
            }
            meta = type_meta.get(item_type, type_meta["folder"])
            with ui.dialog() as dlg, ui.card().classes("p-4 gap-3 w-80"):
                ui.label(str(meta["title"])).classes("text-lg font-semibold")
                parent_label = page_state.explorer_cd_path or "/"
                ui.label(f"В: {parent_label}").classes("rag-path text-xs")
                name_input = ui.input(
                    str(meta["input"]),
                    value=str(meta["default"]),
                    placeholder=str(meta["placeholder"]),
                ).props("dense outlined autofocus").classes("w-full")

                async def _do_create() -> None:
                    if not _cd_can(page_state.explorer_cd_path or "", "editor"):
                        ui.notify("Нет прав на создание здесь.", type="negative")
                        return
                    name = str(name_input.value or "").strip()
                    if not name:
                        ui.notify("Введите имя.", type="warning")
                        return
                    try:
                        if item_type == "folder":
                            await run.io_bound(
                                svc.create_folder,
                                parent_path=page_state.explorer_cd_path or "",
                                name=name,
                            )
                        else:
                            await run.io_bound(
                                svc.create_blank_file,
                                parent_path=page_state.explorer_cd_path or "",
                                filename=name,
                                file_type=item_type,
                            )
                        dlg.close()
                        _log_app_event(
                            page_state, "cd_explorer", str(meta["event"]),
                            details={"parent": page_state.explorer_cd_path, "name": name},
                        )
                        ui.notify(f"Создано: «{name}».", type="positive")
                        render_fn()
                    except Exception as exc:
                        ui.notify(f"Не удалось создать: {exc}", type="negative")

                name_input.on("keydown.enter", lambda _: _do_create())
                with ui.row().classes("w-full justify-end gap-2 mt-1"):
                    ui.button("Отмена", on_click=dlg.close).props("flat dense")
                    ui.button("Создать", icon=str(meta["icon"]), on_click=_do_create).props("unelevated dense")
            dlg.open()

        async def _cd_create_picker_dialog() -> None:
            with ui.dialog() as dlg, ui.card().classes("p-2 gap-1 w-64"):
                ui.label("Создать").classes("font-semibold px-2 py-1")

                def _pick_handler(item_type: str) -> Callable[[], Any]:
                    async def _handler() -> None:
                        dlg.close()
                        await _cd_create_dialog(item_type)

                    return _handler

                ui.button("Папку", icon="create_new_folder", on_click=_pick_handler("folder"), color=None).props("flat dense no-caps align=left").classes("w-full")
                ui.button("Документ Word", icon="description", on_click=_pick_handler("word"), color=None).props("flat dense no-caps align=left").classes("w-full")
                ui.button("Таблицу Excel", icon="table_chart", on_click=_pick_handler("excel"), color=None).props("flat dense no-caps align=left").classes("w-full")
                ui.button("Текстовый файл", icon="article", on_click=_pick_handler("text"), color=None).props("flat dense no-caps align=left").classes("w-full")
            dlg.open()

        async def _cd_rename_dialog(node_path: str, node_name: str) -> None:
            """Dialog to rename a file or folder in Cloud Drive."""
            with ui.dialog() as dlg, ui.card().classes("p-4 gap-3 w-80"):
                ui.label("Переименовать").classes("text-lg font-semibold")
                ui.label(node_path).classes("rag-path text-xs")
                name_input = ui.input(
                    "Новое имя",
                    value=node_name,
                ).props("dense outlined autofocus").classes("w-full")

                async def _do_rename() -> None:
                    if not _cd_can(node_path, "editor"):
                        ui.notify("Нет прав на переименование.", type="negative")
                        return
                    new_name = str(name_input.value or "").strip()
                    if not new_name:
                        ui.notify("Введите новое имя.", type="warning")
                        return
                    if new_name == node_name:
                        dlg.close()
                        return
                    try:
                        parent_path = node_path.rsplit("/", 1)[0] if "/" in node_path else ""
                        await run.io_bound(
                            svc.move_node,
                            source_path=node_path,
                            dest_parent_path=parent_path,
                            new_name=new_name,
                        )
                        dlg.close()
                        _log_app_event(
                            page_state, "cd_explorer", "rename",
                            details={"path": node_path, "new_name": new_name},
                        )
                        ui.notify(f"Переименовано в «{new_name}».", type="positive")
                        render_fn()
                    except Exception as exc:
                        ui.notify(f"Ошибка переименования: {exc}", type="negative")

                name_input.on("keydown.enter", lambda _: _do_rename())
                with ui.row().classes("w-full justify-end gap-2 mt-1"):
                    ui.button("Отмена", on_click=dlg.close).props("flat dense")
                    ui.button("Сохранить", icon="drive_file_rename_outline", on_click=_do_rename).props("unelevated dense")
            dlg.open()

        async def _cd_delete_dialog(node_path: str, node_name: str, is_folder: bool = False) -> None:
            """Confirmation dialog before deleting a file or folder."""
            with ui.dialog() as dlg, ui.card().classes("p-4 gap-3 w-96"):
                ui.label("Удалить?").classes("text-lg font-semibold text-red-700")
                kind = "папку" if is_folder else "файл"
                ui.label(f"{'Папка' if is_folder else 'Файл'}: {node_name}").classes("text-sm font-medium")
                ui.label(node_path).classes("rag-path text-xs")
                if is_folder:
                    ui.label(
                        "Все файлы и вложенные папки будут безвозвратно удалены."
                    ).classes("text-xs text-red-600 mt-1")
                else:
                    ui.label("Файл и все его версии будут удалены безвозвратно.").classes("text-xs text-red-600 mt-1")

                async def _do_delete() -> None:
                    if not _cd_can(node_path, "editor"):
                        ui.notify("Нет прав на удаление.", type="negative")
                        return
                    try:
                        await run.io_bound(svc.delete_node, path=node_path)
                        dlg.close()
                        _log_app_event(
                            page_state, "cd_explorer", "delete",
                            details={"path": node_path, "is_folder": is_folder},
                        )
                        ui.notify(f"{'Папка' if is_folder else 'Файл'} «{node_name}» удалён.", type="positive")
                        # If we deleted the current folder, navigate up
                        if is_folder and page_state.explorer_cd_path == node_path:
                            parent = node_path.rsplit("/", 1)[0] if "/" in node_path else ""
                            page_state.explorer_cd_path = parent
                        render_fn()
                    except Exception as exc:
                        ui.notify(f"Ошибка удаления: {exc}", type="negative")

                with ui.row().classes("w-full justify-end gap-2 mt-2"):
                    ui.button("Отмена", on_click=dlg.close).props("flat dense")
                    ui.button(
                        f"Удалить {kind}", icon="delete_forever",
                        on_click=_do_delete,
                        color="negative",
                    ).props("unelevated dense")
            dlg.open()

        async def _cd_move_dialog(node_path: str, node_name: str, is_folder: bool = False) -> None:
            """Dialog to move a file or folder to another folder in Cloud Drive."""
            # Load all available target folders from registry
            try:
                with svc.registry._connect() as _conn:
                    _rows = _conn.execute(
                        "SELECT * FROM cloud_folders ORDER BY path"
                    ).fetchall()
                all_folders = [svc.registry._folder_from_row(r) for r in _rows]
            except Exception:
                all_folders = []

            # Exclude self (and descendants if it's a folder)
            if is_folder:
                candidates = [
                    fo for fo in all_folders
                    if fo.path != node_path and not fo.path.startswith(node_path + "/")
                ]
            else:
                candidates = all_folders
            candidates = [fo for fo in candidates if _cd_can(fo.path, "editor")]

            selected_path: list = [page_state.explorer_cd_path or ""]

            with ui.dialog() as dlg, ui.card().classes("p-4 gap-3 w-96"):
                ui.label("Переместить в папку").classes("text-lg font-semibold")
                ui.label(f"{'Папка' if is_folder else 'Файл'}: {node_name}").classes("text-sm")
                ui.separator()

                if not candidates:
                    ui.label("Нет доступных папок.").classes("rag-meta text-sm")
                else:
                    folder_options = {
                        fo.path: ("Cloud Drive (корень)" if fo.is_root else fo.path)
                        for fo in candidates
                    }
                    sel = ui.select(
                        options=folder_options,
                        value=selected_path[0],
                        label="Целевая папка",
                    ).props("dense outlined emit-value map-options").classes("w-full")
                    sel.on("update:model-value", lambda e: selected_path.__setitem__(0, e.args))

                async def _do_move() -> None:
                    if not _cd_can(node_path, "editor"):
                        ui.notify("Нет прав на перемещение.", type="negative")
                        return
                    dest = str(selected_path[0] or "").strip()
                    if not _cd_can(dest, "editor"):
                        ui.notify("Нет прав на целевую папку.", type="negative")
                        return
                    if dest == (node_path.rsplit("/", 1)[0] if "/" in node_path else ""):
                        ui.notify("Файл уже находится в этой папке.", type="info")
                        dlg.close()
                        return
                    try:
                        await run.io_bound(
                            svc.move_node,
                            source_path=node_path,
                            dest_parent_path=dest,
                            new_name=node_name,
                        )
                        dlg.close()
                        _log_app_event(
                            page_state, "cd_explorer", "move",
                            details={"path": node_path, "dest": dest},
                        )
                        ui.notify(f"«{node_name}» перемещён.", type="positive")
                        render_fn()
                    except Exception as exc:
                        ui.notify(f"Ошибка перемещения: {exc}", type="negative")

                with ui.row().classes("w-full justify-end gap-2 mt-2"):
                    ui.button("Отмена", on_click=dlg.close).props("flat dense")
                    ui.button("Переместить", icon="drive_file_move", on_click=_do_move).props("unelevated dense")
            dlg.open()

        def _cd_open_file(file: CloudDriveFile) -> None:
            if file.storage_key and open_cloud_file_viewer_fn is not None:
                _log_app_event(page_state, "cd_explorer", "open_cloud_preview", details={"path": file.path})
                open_cloud_file_viewer_fn(file)
                return
            src = str(file.source_path or file.path or "")
            if src:
                p = Path(src)
                if p.exists() and p.is_file():
                    _log_app_event(page_state, "cd_explorer", "open_file", details={"path": src})
                    open_file_viewer_fn(p)
                    return
            ui.notify("Исходный файл недоступен на диске.", type="warning")

        async def _cd_reindex_file(file_path: str) -> None:
            if not _cd_can(file_path, "editor"):
                ui.notify("Нет прав на переиндексацию этого файла.", type="negative")
                return
            try:
                job = await run.io_bound(svc.enqueue_reindex, file_path)
                ui.notify(f"Переиндексация запущена (job {str(job.id or '')[:8]})", type="positive")
                _log_app_event(page_state, "cd_explorer", "reindex_file", details={"path": file_path})
            except Exception as exc:
                ui.notify(f"Ошибка: {exc}", type="negative")
            render_fn()

        async def _cd_restore_node(node_path: str) -> None:
            if not _cd_can(node_path, "editor"):
                ui.notify("Нет прав на восстановление.", type="negative")
                return
            try:
                await run.io_bound(svc.restore_node, path=node_path)
                _log_app_event(page_state, "cd_explorer", "restore", details={"path": node_path})
                ui.notify("Объект восстановлен.", type="positive")
                render_fn()
            except Exception as exc:
                ui.notify(f"Ошибка восстановления: {exc}", type="negative")

        def _cd_unique_name(dest_parent_path: str, desired_name: str, *, is_folder: bool) -> str:
            clean_parent = str(dest_parent_path or "").strip().replace("\\", "/").strip("/")
            raw_name = str(desired_name or ("Папка" if is_folder else "Файл")).strip().strip("/\\")
            stem = Path(raw_name).stem if not is_folder else raw_name
            suffix = Path(raw_name).suffix if not is_folder else ""
            candidates = [raw_name, f"{stem} копия{suffix}"]
            candidates.extend(f"{stem} копия {idx}{suffix}" for idx in range(2, 1000))
            for name in candidates:
                target = f"{clean_parent}/{name}" if clean_parent else name
                if svc.registry.get_node_by_path(target) is None:
                    return name
            raise RuntimeError("Не удалось подобрать свободное имя.")

        def _cd_copy_file_to_parent(file_row: CloudDriveFile, dest_parent_path: str, *, preferred_name: str = "") -> CloudDriveFile:
            parent = svc.registry.get_root_folder() if not dest_parent_path else svc.registry.get_folder_by_path(dest_parent_path)
            if parent is None:
                raise RuntimeError(f"Целевая папка не найдена: {dest_parent_path or '/'}")
            name = _cd_unique_name(dest_parent_path, preferred_name or file_row.name, is_folder=False)
            target_path = f"{dest_parent_path.strip('/')}/{name}" if dest_parent_path.strip("/") else name
            copied = svc.registry.upsert_file(
                folder_id=parent.id,
                path=target_path,
                name=name,
                storage_key=file_row.storage_key,
                mime_type=file_row.mime_type,
                size_bytes=file_row.size_bytes,
                checksum=file_row.checksum,
                source_path=file_row.source_path,
                source_mtime=file_row.source_mtime,
            )
            queue = getattr(svc, "_queue_reindex_file", None)
            if callable(queue):
                queue(copied, reason="copy")
            return copied

        def _cd_copy_folder_to_parent(folder: CloudDriveFolder, dest_parent_path: str, *, preferred_name: str = "") -> CloudDriveFolder:
            if folder.is_root:
                raise RuntimeError("Корневую папку копировать нельзя.")
            name = _cd_unique_name(dest_parent_path, preferred_name or folder.name, is_folder=True)
            created = svc.create_folder(parent_path=dest_parent_path, name=name)
            new_path = str(created.get("path") or "").strip("/")
            for child_file in svc.registry.list_files_in_folder(folder.id):
                _cd_copy_file_to_parent(child_file, new_path, preferred_name=child_file.name)
            for child_folder in svc.registry.list_child_folders(folder.id):
                _cd_copy_folder_to_parent(child_folder, new_path, preferred_name=child_folder.name)
            saved = svc.registry.get_folder_by_path(new_path)
            if saved is None:
                raise RuntimeError(f"Созданная папка не найдена: {new_path}")
            return saved

        def _cd_copy_node_to_parent(source_path: str, dest_parent_path: str) -> None:
            node = svc.registry.get_node_by_path(source_path)
            if node is None:
                raise RuntimeError(f"Узел не найден: {source_path}")
            if hasattr(node, "folder_id"):
                _cd_copy_file_to_parent(node, dest_parent_path)
            else:
                _cd_copy_folder_to_parent(node, dest_parent_path)

        def _cd_set_clipboard(paths: List[str], mode: str) -> None:
            clean_paths = [str(path).strip().replace("\\", "/").strip("/") for path in paths if str(path).strip()]
            if not clean_paths:
                ui.notify("Ничего не выбрано.", type="warning")
                return
            state.explorer_clipboard = {"scope": "cd", "mode": mode, "paths": clean_paths}
            ui.notify(("Скопировано" if mode == "copy" else "Вырезано") + f": {len(clean_paths)}", type="positive")
            _log_app_event(page_state, "cd_explorer", f"clipboard_{mode}", details={"count": len(clean_paths)})

        async def _cd_paste_clipboard() -> None:
            clipboard = dict(getattr(state, "explorer_clipboard", {}) or {})
            if clipboard.get("scope") != "cd":
                ui.notify("В буфере нет элементов Cloud Drive.", type="warning")
                return
            paths = [str(path).strip().replace("\\", "/").strip("/") for path in (clipboard.get("paths") or []) if str(path).strip()]
            if not paths:
                ui.notify("Буфер пуст.", type="warning")
                return
            dest = page_state.explorer_cd_path or ""
            mode = str(clipboard.get("mode") or "copy")
            try:
                if mode == "cut":
                    for source_path in paths:
                        node = svc.registry.get_node_by_path(source_path)
                        if node is None:
                            continue
                        name = _cd_unique_name(dest, getattr(node, "name", Path(source_path).name), is_folder=not hasattr(node, "folder_id"))
                        await run.io_bound(svc.move_node, source_path=source_path, dest_parent_path=dest, new_name=name)
                    state.explorer_clipboard = {}
                else:
                    for source_path in paths:
                        await run.io_bound(_cd_copy_node_to_parent, source_path, dest)
                _clear_selection()
                ui.notify(f"Вставлено: {len(paths)}", type="positive")
                _log_app_event(page_state, "cd_explorer", "paste", details={"mode": mode, "count": len(paths), "dest": dest})
                render_fn()
            except Exception as exc:
                ui.notify(f"Ошибка вставки: {exc}", type="negative")

        def _cd_share_paths(paths: List[str], *, verb: str = "Поделиться") -> None:
            clean_paths = [str(path or "").strip().replace("\\", "/").strip("/") for path in paths if str(path or "").strip()]
            if not clean_paths:
                ui.notify("Ничего не выбрано.", type="warning")
                return
            blocked = [path for path in clean_paths if not _cd_can(path, "admin")]
            if blocked:
                ui.notify(f"Нет прав администрирования: {blocked[0]}", type="negative")
                return

            single_path = clean_paths[0] if len(clean_paths) == 1 else ""
            public_links_enabled = bool(page_state.cfg.get("cloud_drive_public_links_enabled"))
            try:
                share_groups = _get_auth_db(page_state).list_groups(include_archived=True)
            except Exception:
                share_groups = []
            share_group_options = {
                str(group.get("id") or ""): str(group.get("name") or group.get("id") or "")
                for group in share_groups
                if str(group.get("id") or "") and str(group.get("status") or "") == "active"
            }
            share_group_labels = {
                str(group.get("id") or ""): (
                    f"{str(group.get('name') or group.get('id') or '')} (архив)"
                    if str(group.get("status") or "") == "archived"
                    else str(group.get("name") or group.get("id") or "")
                )
                for group in share_groups
                if str(group.get("id") or "")
            }

            with ui.dialog() as dlg, ui.card().classes("p-4 gap-3 w-full max-w-2xl max-h-[90vh] overflow-y-auto"):
                ui.label(verb).classes("text-lg font-semibold")
                ui.label(f"Объектов: {len(clean_paths)}").classes("rag-meta")
                with ui.element("div").classes("w-full max-h-32 overflow-y-auto rounded border border-slate-700/30 p-2"):
                    for path in clean_paths[:12]:
                        ui.label(path or "/").classes("rag-path text-xs truncate")
                    if len(clean_paths) > 12:
                        ui.label(f"и ещё {len(clean_paths) - 12}").classes("rag-meta text-xs")

                with ui.expansion("Внутренний доступ", icon="person_add", value=True).classes("w-full"):
                    with ui.row().classes("w-full gap-2 items-center"):
                        share_subject_type = ui.select(
                            {"user": "Пользователь", "group": "Группа", "role": "Роль", "*": "Все"},
                            value="user",
                            label="Кому",
                        ).props("dense outlined").classes("min-w-40")
                        share_subject_id = ui.input("Логин / роль / *", value="").props("dense outlined").classes("flex-1")
                        share_group_id = ui.select(share_group_options, label="Группа").props("dense outlined").classes("flex-1")
                        share_group_id.set_visibility(False)
                    share_access = ui.select(
                        {"viewer": "Просмотр", "editor": "Редактирование", "admin": "Администрирование"},
                        value="viewer",
                        label="Уровень доступа",
                    ).props("dense outlined").classes("w-full max-w-xs")
                    internal_access_box = ui.column().classes("w-full gap-1")

                    def _render_internal_access() -> None:
                        internal_access_box.clear()
                        with internal_access_box:
                            ui.separator()
                            ui.label("Кто имеет доступ").classes("text-sm font-medium")
                            if not single_path:
                                ui.label("Текущие доступы показываются при выборе одного объекта.").classes("rag-meta text-xs")
                                return
                            permissions = svc.list_permissions(path=single_path)
                            if not permissions:
                                ui.label("Явных правил доступа нет.").classes("rag-meta text-xs")
                                return
                            node = svc.registry.get_node_by_path(single_path)
                            direct_ids = {single_path, str(getattr(node, "id", "") or "")}
                            subject_labels = {"user": "Пользователь", "group": "Группа", "role": "Роль", "*": "Все"}
                            access_labels = {"viewer": "Просмотр", "editor": "Редактирование", "admin": "Администрирование"}
                            for permission in permissions:
                                permission_id = str(permission.get("id") or "")
                                subject_type = str(permission.get("subject_type") or "")
                                subject_id = str(permission.get("subject_id") or "*")
                                subject_name = share_group_labels.get(subject_id, subject_id) if subject_type == "group" else subject_id
                                access_level = str(permission.get("access_level") or "viewer")
                                resource_type = str(permission.get("resource_type") or "")
                                resource_id = str(permission.get("resource_id") or "")
                                if resource_type == "global":
                                    scope = "Глобально"
                                elif resource_id in direct_ids:
                                    scope = "Этот объект"
                                else:
                                    scope = "Наследуется"
                                with ui.row().classes("w-full items-center gap-2 py-1"):
                                    ui.icon("person" if subject_type == "user" else "group", size="16px").classes("text-slate-400")
                                    with ui.column().classes("gap-0 min-w-0 flex-1"):
                                        ui.label(f"{subject_labels.get(subject_type, subject_type)}: {subject_name}").classes("text-sm truncate")
                                        ui.label(f"{access_labels.get(access_level, access_level)} · {scope}").classes("rag-meta text-xs")
                                    ui.button(
                                        icon="person_remove",
                                        on_click=_make_permission_revoke_handler(permission_id),
                                    ).props('flat dense round size=sm color=negative aria-label="Отозвать доступ"').tooltip("Отозвать доступ")

                    async def _revoke_internal_share(permission_id: str) -> None:
                        try:
                            ok = await run.io_bound(svc.revoke_permission, permission_id)
                            if not ok:
                                raise RuntimeError("Правило уже удалено или не найдено.")
                            _log_app_event(
                                page_state,
                                "cd_explorer",
                                "share_internal_revoke",
                                details={"path": single_path, "permission_id": permission_id},
                            )
                            _render_internal_access()
                            ui.notify("Доступ отозван.", type="positive")
                        except Exception as exc:
                            ui.notify(f"Не удалось отозвать доступ: {exc}", type="negative")

                    def _make_permission_revoke_handler(permission_id: str) -> Callable[[], Any]:
                        async def _handler() -> None:
                            await _revoke_internal_share(permission_id)

                        return _handler

                    async def _grant_internal_share() -> None:
                        try:
                            subject_type = str(share_subject_type.value or "").strip().lower()
                            subject_id = str(
                                (share_group_id.value if subject_type == "group" else share_subject_id.value) or ""
                            ).strip()
                            if subject_type == "*":
                                subject_id = "*"
                            elif not subject_id:
                                ui.notify("Укажите пользователя, группу или роль.", type="warning")
                                return
                            access_level = str(share_access.value or "viewer").strip().lower()
                            granted: list[dict[str, str]] = []
                            for path in clean_paths:
                                def _grant_path(path: str = path) -> Dict[str, str]:
                                    return svc.grant_path_permission(
                                        subject_type=subject_type,
                                        subject_id=subject_id,
                                        path=path,
                                        access_level=access_level,
                                    )
                                granted.append(await run.io_bound(_grant_path))
                            _log_app_event(
                                page_state,
                                "cd_explorer",
                                "share_internal",
                                details={"count": len(granted), "subject_type": subject_type, "subject_id": subject_id, "access_level": access_level},
                            )
                            _render_internal_access()
                            ui.notify(f"Доступ выдан: {len(granted)}", type="positive")
                        except Exception as exc:
                            ui.notify(f"Не удалось выдать доступ: {exc}", type="negative")

                    def refresh_share_subject_input() -> None:
                        is_group = str(share_subject_type.value or "") == "group"
                        share_subject_id.set_visibility(not is_group)
                        share_group_id.set_visibility(is_group)
                        if str(share_subject_type.value or "") == "*":
                            share_subject_id.set_value("*")

                    share_subject_type.on_value_change(lambda _: refresh_share_subject_input())
                    ui.button("Выдать доступ", icon="lock_open", on_click=_grant_internal_share).props("outline dense")
                    _render_internal_access()

                with ui.expansion("Публичная ссылка", icon="link").classes("w-full"):
                    if len(clean_paths) != 1:
                        ui.label("Публичная ссылка создаётся только для одного объекта.").classes("rag-meta text-xs")
                    else:
                        if not public_links_enabled:
                            with ui.row().classes("w-full items-center gap-2 p-2 rounded bg-amber-950/20"):
                                ui.icon("policy", size="18px").classes("text-amber-500")
                                ui.label("Публичные ссылки отключены политикой Cloud Drive.").classes("text-sm")
                        share_expires = ui.input("Истекает после", value="").props("dense outlined type=datetime-local").classes("w-full")
                        share_expires.tooltip("Пустое значение создаёт ссылку без срока действия.")
                        create_public_button = ui.button("Создать и скопировать ссылку", icon="link").props("outline dense")
                        create_public_button.set_enabled(public_links_enabled)
                        public_links_box = ui.column().classes("w-full gap-1")

                        def _copy_public_link(token: str) -> None:
                            url_path = f"/api/cloud-drive/public/download?token={quote(token, safe='')}"
                            ui.run_javascript(
                                "navigator.clipboard && navigator.clipboard.writeText("
                                f"window.location.origin + {json.dumps(url_path)}"
                                ")"
                            )
                            ui.notify("Публичная ссылка скопирована.", type="positive")

                        def _render_public_links() -> None:
                            public_links_box.clear()
                            with public_links_box:
                                ui.separator()
                                ui.label("Активные публичные ссылки").classes("text-sm font-medium")
                                links = svc.list_share_links(path=single_path)
                                if not links:
                                    ui.label("Активных ссылок нет.").classes("rag-meta text-xs")
                                    return
                                for link in links:
                                    token = str(link.get("token") or "")
                                    expires_at = str(link.get("expires_at") or "")
                                    created_by = str(link.get("created_by") or "") or "-"
                                    with ui.row().classes("w-full items-center gap-2 py-1"):
                                        ui.icon("link", size="16px").classes("text-slate-400")
                                        with ui.column().classes("gap-0 min-w-0 flex-1"):
                                            ui.label(f"Создал: {created_by}").classes("text-sm")
                                            ui.label(
                                                f"До: {expires_at[:16].replace(chr(84), ' ')}" if expires_at else "Без срока"
                                            ).classes("rag-meta text-xs")
                                        ui.button(
                                            icon="content_copy",
                                            on_click=lambda _e=None, t=token: _copy_public_link(t),
                                        ).props('flat dense round size=sm aria-label="Скопировать публичную ссылку"').tooltip("Скопировать ссылку")
                                        ui.button(
                                            icon="link_off",
                                            on_click=_make_link_revoke_handler(token),
                                        ).props('flat dense round size=sm color=negative aria-label="Отозвать публичную ссылку"').tooltip("Отозвать ссылку")

                        async def _revoke_public_link(token: str) -> None:
                            try:
                                ok = await run.io_bound(svc.revoke_share_link, token)
                                if not ok:
                                    raise RuntimeError("Ссылка уже отозвана или не найдена.")
                                _log_app_event(page_state, "cd_explorer", "share_public_link_revoke", details={"path": single_path})
                                _render_public_links()
                                ui.notify("Публичная ссылка отозвана.", type="positive")
                            except Exception as exc:
                                ui.notify(f"Не удалось отозвать ссылку: {exc}", type="negative")

                        def _make_link_revoke_handler(token: str) -> Callable[[], Any]:
                            async def _handler() -> None:
                                await _revoke_public_link(token)

                            return _handler

                        async def _create_public_link() -> None:
                            try:
                                path = clean_paths[0]
                                def _create_link() -> Dict[str, str]:
                                    return svc.create_share_link(
                                        path=path,
                                        created_by=_username(page_state),
                                        expires_at=str(share_expires.value or "").strip(),
                                    )
                                link = await run.io_bound(_create_link)
                                _copy_public_link(str(link.get("token") or ""))
                                _log_app_event(page_state, "cd_explorer", "share_public_link", details={"path": path})
                                _render_public_links()
                                ui.notify("Публичная ссылка скопирована в буфер.", type="positive")
                            except Exception as exc:
                                ui.notify(f"Не удалось создать ссылку: {exc}", type="negative")

                        create_public_button.on_click(_create_public_link)
                        _render_public_links()

                with ui.row().classes("w-full justify-between gap-2 mt-2"):
                    ui.button(
                        "Скопировать пути",
                        icon="content_copy",
                        on_click=lambda: ui.run_javascript(f"navigator.clipboard && navigator.clipboard.writeText({json.dumps(chr(10).join(clean_paths))})"),
                    ).props("flat dense")
                    ui.button("Закрыть", on_click=dlg.close).props("flat dense")
            dlg.open()

        def _cd_archive_paths(paths: List[str]) -> None:
            export_dir = Path("runtime") / "exports"
            export_dir.mkdir(parents=True, exist_ok=True)
            zip_path = export_dir / f"cloud-drive-selection-{int(time.time())}.zip"
            added = 0
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                for node_path in paths:
                    node = svc.registry.get_node_by_path(node_path)
                    files = [node] if node is not None and hasattr(node, "folder_id") else (
                        svc.registry.list_files_under_path(node.path) if node is not None else []
                    )
                    for file_row in files:
                        descriptor = svc.get_download_descriptor(file_row.path)
                        local_path = descriptor.get("file_path")
                        if not local_path:
                            continue
                        archive.write(str(local_path), arcname=file_row.path)
                        added += 1
            if not added:
                zip_path.unlink(missing_ok=True)
                ui.notify("Нет доступных локальных файлов для архива.", type="warning")
                return
            _log_app_event(page_state, "cd_explorer", "archive", details={"count": added, "zip": str(zip_path)})
            ui.download(zip_path, filename=zip_path.name)
            ui.notify(f"Архив создан: {added} файлов.", type="positive")

        def _cd_delete_selected_dialog(paths: List[str]) -> None:
            with ui.dialog() as dlg, ui.card().classes("p-4 gap-3 w-96"):
                ui.label("Удалить выбранное?").classes("text-lg font-semibold text-red-700")
                ui.label(f"Объектов: {len(paths)}").classes("rag-meta")
                ui.label("Удаление переместит элементы в корзину Cloud Drive.").classes("text-xs text-red-600")

                async def _do_delete() -> None:
                    try:
                        for node_path in paths:
                            if not _cd_can(node_path, "editor"):
                                raise RuntimeError(f"Нет прав на удаление: {node_path}")
                            await run.io_bound(svc.delete_node, path=node_path)
                        _clear_selection()
                        dlg.close()
                        ui.notify(f"Удалено: {len(paths)}", type="positive")
                        _log_app_event(page_state, "cd_explorer", "delete_selected", details={"count": len(paths)})
                        render_fn()
                    except Exception as exc:
                        ui.notify(f"Ошибка удаления: {exc}", type="negative")

                with ui.row().classes("w-full justify-end gap-2 mt-2"):
                    ui.button("Отмена", on_click=dlg.close).props("flat dense")
                    ui.button("Удалить", icon="delete_forever", on_click=_do_delete, color="negative").props("unelevated dense")
            dlg.open()

        def _cd_hide_paths(paths: List[str]) -> None:
            _set_hidden("cd", paths, True)
            _clear_selection()
            ui.notify(f"Скрыто из интерфейса: {len(paths)}", type="positive")
            _log_app_event(page_state, "cd_explorer", "hide", details={"count": len(paths)})
            render_fn()

        def _cd_unhide_paths(paths: List[str]) -> None:
            _set_hidden("cd", paths, False)
            _clear_selection()
            ui.notify(f"Вернулось в интерфейс: {len(paths)}", type="positive")
            _log_app_event(page_state, "cd_explorer", "unhide", details={"count": len(paths)})
            render_fn()

        def _cd_selection_action(scope: str, action: str) -> None:
            if scope != "cd":
                ui.notify("Действие доступно только в Cloud Drive.", type="warning")
                return
            paths = _selected_paths("cd")
            if not paths:
                ui.notify("Ничего не выбрано.", type="warning")
                return
            if action == "copy":
                _cd_set_clipboard(paths, "copy")
            elif action == "cut":
                _cd_set_clipboard(paths, "cut")
            elif action == "delete":
                _cd_delete_selected_dialog(paths)
            elif action == "share":
                _cd_share_paths(paths, verb="Поделиться")
            elif action == "send":
                _cd_share_paths(paths, verb="Отправить")
            elif action == "archive":
                _cd_archive_paths(paths)
            elif action == "hide":
                _cd_hide_paths(paths)
            elif action == "unhide":
                _cd_unhide_paths(paths)
            else:
                ui.notify("Неизвестное действие.", type="warning")

        page_state._explorer_selection_action = _cd_selection_action

        requested_share_path = str(page_state.explorer_share_path or "").strip()
        if requested_share_path:
            page_state.explorer_share_path = ""
            ui.timer(
                0.0,
                lambda path=requested_share_path: _cd_share_paths(
                    [path],
                    verb="Управление доступом",
                ),
                once=True,
            )

        cd_path = page_state.explorer_cd_path or ""
        _is_trash_view = cd_path == "__trash__"
        if _is_trash_view:
            cd_path = ""  # don't pass __trash__ to backend helpers
        child_folders, child_files = _cd_list_children(
            svc,
            cd_path,
            cfg=page_state.cfg,
            user=page_state.current_user,
        )
        breadcrumbs = _cd_breadcrumb_chain(svc, cd_path)
        root_folder = svc.registry.get_root_folder()

        def _render_cd_header_breadcrumbs() -> None:
            if page_state.header_breadcrumbs is None:
                return
            page_state.header_breadcrumbs.clear()
            with page_state.header_breadcrumbs:
                _render_cd_breadcrumb_buttons()

        def _render_cd_breadcrumb_buttons() -> None:
            ui.icon("folder", size="16px").classes("text-slate-400")
            for idx, folder in enumerate(breadcrumbs):
                label = "Корень" if folder.is_root else folder.name
                button = ui.button(
                    label,
                    on_click=lambda p=folder.path: _cd_open_folder(p),
                    color=None,
                ).props("flat dense no-caps")
                button.tooltip(folder.path or "Корень")
                if idx < len(breadcrumbs) - 1:
                    ui.icon("chevron_right").classes("text-slate-500")

        def _render_cd_inline_breadcrumbs() -> None:
            with ui.row().classes("rag-explorer-inline-breadcrumbs rag-breadcrumbs gap-1 no-wrap"):
                _render_cd_breadcrumb_buttons()

        _render_cd_header_breadcrumbs()
        if page_state.header_breadcrumbs is not None:
            page_state.header_breadcrumbs.set_visibility(True)
        if page_state.header_explorer_actions is not None:
            page_state.header_explorer_actions.clear()

        def _cd_context_props(node_path: str, *, is_folder: bool, download_url: str = "") -> str:
            attrs = {
                "data-rag-context": "explorer-item",
                "data-rag-scope": "cd",
                "data-rag-type": "folder" if is_folder else "file",
                "data-rag-path": quote(str(node_path or ""), safe=""),
                "data-rag-url": download_url,
                "data-rag-favorite": "false",
                "data-rag-hidden": "true" if _is_hidden("cd", node_path) else "false",
            }
            return " ".join(f'{key}="{html.escape(value, quote=True)}"' for key, value in attrs.items())

        def _cd_single_action(node_path: str, action: str) -> None:
            _set_selected({_selection_key("cd", node_path)})
            _cd_selection_action("cd", action)

        def _cd_hidden_action_buttons(node_path: str, *, is_folder: bool, open_action: Callable[[], None], download_url: str = "") -> None:
            ui.button(on_click=open_action).props("data-rag-open").classes("rag-context-action-hidden")
            if not is_folder and download_url:
                ui.button(on_click=lambda url=download_url: ui.navigate.to(url, new_tab=True)).props("data-rag-download").classes("rag-context-action-hidden")
            ui.button(on_click=lambda p=node_path: _cd_single_action(p, "copy")).props("data-rag-copy").classes("rag-context-action-hidden")
            ui.button(on_click=lambda p=node_path: _cd_single_action(p, "cut")).props("data-rag-cut").classes("rag-context-action-hidden")
            ui.button(on_click=lambda p=node_path: _cd_single_action(p, "delete")).props("data-rag-delete").classes("rag-context-action-hidden")
            ui.button(on_click=lambda p=node_path: _cd_single_action(p, "share")).props("data-rag-share").classes("rag-context-action-hidden")
            ui.button(on_click=lambda p=node_path: _cd_single_action(p, "send")).props("data-rag-send").classes("rag-context-action-hidden")
            ui.button(on_click=lambda p=node_path: _cd_single_action(p, "archive")).props("data-rag-archive").classes("rag-context-action-hidden")
            ui.button(on_click=lambda p=node_path: _cd_single_action(p, "hide")).props("data-rag-hide").classes("rag-context-action-hidden")
            ui.button(on_click=lambda p=node_path: _cd_single_action(p, "unhide")).props("data-rag-unhide").classes("rag-context-action-hidden")

        # Preserve direct-child properties before UI filters are applied.
        direct_folder_count = len(child_folders)
        direct_file_count = len(child_files)
        direct_file_size = sum(int(file.size_bytes or 0) for file in child_files)

        # filter & sort
        name_q = page_state.explorer_filter.strip().lower()
        ext_q = page_state.explorer_ext if page_state.explorer_ext != "Все" else ""
        if name_q:
            child_folders = [f for f in child_folders if name_q in f.name.lower()]
            child_files   = [f for f in child_files   if name_q in f.name.lower()]
        if ext_q:
            child_files = [f for f in child_files if f.name.lower().endswith(ext_q.lower())]
        if not page_state.explorer_show_hidden:
            child_folders = [f for f in child_folders if not _is_hidden("cd", f.path)]
            child_files = [f for f in child_files if not _is_hidden("cd", f.path)]

        all_folder_ids = [str(folder.id) for folder in child_folders]
        current_folder = breadcrumbs[-1] if breadcrumbs else root_folder
        current_folder_id = str(current_folder.id) if current_folder is not None else ""
        cached_folder_sizes = dict(page_state.explorer_folder_sizes)
        child_folder_sizes = {
            folder_id: int(cached_folder_sizes[folder_id])
            for folder_id in all_folder_ids
            if folder_id in cached_folder_sizes
        }

        sort_key = page_state.explorer_sort
        rev = page_state.explorer_desc
        if sort_key == "По имени":
            child_folders.sort(key=lambda x: x.name.lower(), reverse=rev)
            child_files.sort(key=lambda x: x.name.lower(), reverse=rev)
        elif sort_key == "По размеру":
            child_folders.sort(key=lambda x: child_folder_sizes.get(str(x.id), 0), reverse=rev)
            child_files.sort(key=lambda x: x.size_bytes, reverse=rev)
        elif sort_key == "По дате":
            child_folders.sort(key=_cloud_node_modified_timestamp, reverse=rev)
            child_files.sort(key=_cloud_node_modified_timestamp, reverse=rev)

        # Bound each NiceGUI render. Large folders otherwise block Socket.IO
        # heartbeats while hundreds of interactive rows are constructed.
        total_files = len(child_files)
        page_size = _EXPLORER_PAGE_SIZE
        entries: List[tuple[str, Any]] = [
            *(("folder", folder) for folder in child_folders),
            *(("file", file) for file in child_files),
        ]
        total_entries = len(entries)
        page_state.explorer_visible_count = max(
            page_size,
            min(int(page_state.explorer_visible_count or page_size), max(page_size, total_entries)),
        )
        page_entries = entries[:page_state.explorer_visible_count]
        page_folders = [item for item_type, item in page_entries if item_type == "folder"]
        page_files = [item for item_type, item in page_entries if item_type == "file"]

        folder_size_ids = list(dict.fromkeys([*all_folder_ids, current_folder_id]))
        folder_size_ids = [folder_id for folder_id in folder_size_ids if folder_id]
        folder_size_labels: Dict[str, List[Any]] = {}
        now_monotonic = time.monotonic()
        folder_sizes_need_refresh = any(
            folder_id not in cached_folder_sizes
            or now_monotonic - float(page_state.explorer_folder_size_cached_at.get(folder_id, 0.0)) > 60.0
            for folder_id in folder_size_ids
        )

        def _cd_can_show_folder_size(folder: CloudDriveFolder) -> bool:
            if _is_admin(page_state):
                return True
            try:
                if svc.registry.is_user_home_folder_path(folder.path):
                    username = str((page_state.current_user or {}).get("username") or "").strip().lower()
                    return bool(username) and folder.path.strip("/").lower() == username
            except Exception:
                return False
            return True

        def _cd_folder_size_label(folder: CloudDriveFolder) -> str:
            if not _cd_can_show_folder_size(folder):
                return "—"
            if str(folder.id) not in child_folder_sizes:
                return "Считается..."
            return _cd_file_size(int(child_folder_sizes.get(str(folder.id), 0) or 0))

        # Per-file job status map (single DB query for the current page)
        _page_file_ids = [f.id for f in page_files]
        _file_jobs = _cd_file_jobs_map(svc.registry, _page_file_ids)

        _JOB_ICON = {"pending": "hourglass_empty", "running": "sync", "completed": "check_circle", "failed": "error_outline"}
        _JOB_CSS  = {"pending": "cd-status-pending", "running": "cd-status-running", "completed": "cd-status-done", "failed": "cd-status-error"}
        _JOB_TYPE_LABEL = {
            "reindex": {"pending": "В очереди", "running": "Индексируется", "failed": "Ошибка индексации"},
            "cleanup": {"pending": "Очистка", "running": "Очищается", "failed": "Ошибка очистки"},
            "ocr": {"pending": "OCR ожидает", "running": "OCR…", "failed": "Ошибка OCR"},
            "preview": {"pending": "Preview ожидает", "running": "Preview…", "failed": "Ошибка Preview"},
        }
        _JOB_TYPE_TIP = {
            "reindex": {"pending": "Файл ожидает индексации", "running": "Индексируется…", "completed": "Проиндексировано", "failed": "Ошибка индексации"},
            "cleanup": {"pending": "Очистка в очереди", "running": "Удаление из индекса…", "completed": "Очищено", "failed": "Ошибка очистки"},
            "ocr": {"pending": "OCR в очереди", "running": "Распознавание текста…", "completed": "OCR завершён", "failed": "Ошибка OCR"},
            "preview": {"pending": "Preview в очереди", "running": "Создание preview…", "completed": "Preview готов", "failed": "Ошибка preview"},
        }

        def _render_file_status(file_id: str) -> None:
            job = _file_jobs.get(file_id)
            if not job:
                return
            status = job.get("status", "")
            job_type = job.get("job_type", "reindex")
            if status not in _JOB_ICON or status == "completed":
                return
            type_labels = _JOB_TYPE_LABEL.get(job_type, _JOB_TYPE_LABEL["reindex"])
            type_tips = _JOB_TYPE_TIP.get(job_type, _JOB_TYPE_TIP["reindex"])
            label_text = type_labels.get(status, "")
            tip = type_tips.get(status, label_text)
            if status == "failed" and job.get("last_error"):
                tip = f"Ошибка: {job['last_error'][:120]}"
            with ui.element("span").classes(f"cd-status-badge {_JOB_CSS[status]}"):
                ui.icon(_JOB_ICON[status], size="14px")
                if label_text:
                    ui.label(label_text)
                ui.tooltip(tip)

        def _set_cd_sort(value: str, *, toggle_if_active: bool = False) -> None:
            value = value if value in {"По имени", "По размеру", "По дате"} else "По имени"
            if toggle_if_active and page_state.explorer_sort == value:
                page_state.explorer_desc = not bool(page_state.explorer_desc)
            else:
                page_state.explorer_sort = value
                page_state.explorer_desc = False
            page_state.explorer_visible_count = _EXPLORER_PAGE_SIZE
            _save_explorer_settings(page_state)
            render_fn()

        def _toggle_cd_sort_direction() -> None:
            page_state.explorer_desc = not bool(page_state.explorer_desc)
            page_state.explorer_visible_count = _EXPLORER_PAGE_SIZE
            _save_explorer_settings(page_state)
            render_fn()

        def _render_sort_header(label: str, sort_value: str) -> None:
            active = page_state.explorer_sort == sort_value
            icon = "arrow_downward" if page_state.explorer_desc else "arrow_upward"
            button = ui.button(
                label,
                icon=icon if active else None,
                on_click=lambda value=sort_value: _set_cd_sort(value, toggle_if_active=True),
                color=None,
            ).props("flat dense no-caps").classes("rag-sort-header" + (" active" if active else ""))
            direction = "по убыванию" if page_state.explorer_desc else "по возрастанию"
            button.tooltip(f"Сортировать {direction}" if active else f"Сортировать по полю «{label}»")

        # ── Explorer command bar (hi-fi top area) ─────────────────────────
        parent_path = breadcrumbs[-2].path if len(breadcrumbs) >= 2 else ""
        can_go_up = bool(cd_path and root_folder is not None and cd_path != root_folder.path)

        with ui.column().classes("rag-explorer-commandbar w-full gap-2"):
            _render_cd_inline_breadcrumbs()
            with ui.row().classes("rag-explorer-actionline"):
                up_btn = ui.button(
                    icon="arrow_upward",
                    on_click=lambda: _cd_open_folder(parent_path),
                    color=None,
                ).props("flat round dense").classes("rag-explorer-iconbtn")
                if not can_go_up:
                    up_btn.disable()
                ui.button(icon="refresh", on_click=lambda: render_fn(), color=None).props("flat round dense").classes("rag-explorer-iconbtn").tooltip("Обновить")
                folder_search = ui.input(
                    placeholder="семантический поиск в этой папке",
                ).props("dense borderless clearable").classes("rag-explorer-folder-search")

                async def _search_current_folder() -> None:
                    query = str(folder_search.value or "").strip()
                    if not query:
                        return
                    await choose_query_fn(f"{query} path:{cd_path}" if cd_path else query)

                folder_search.on("keydown.enter", _search_current_folder)
                ui.button("Дерево", icon="account_tree", on_click=_cd_open_tree_dialog, color=None).props("outline dense no-caps").classes("rag-explorer-mobile-only")
                ui.button("Фильтры", icon="filter_alt", on_click=_cd_open_filters_dialog, color=None).props("outline dense no-caps").classes("rag-explorer-mobile-only")
                ui.button(
                    "Приложение",
                    icon="cloud_download",
                    on_click=cloud_files_install_dialog.open,
                    color=None,
                ).props("outline dense no-caps").tooltip("Скачать Облако для Windows")
                ui.button("Загрузить", icon="upload", on_click=_cd_upload_dialog, color=None).props("outline dense no-caps")
                ui.button("Создать", icon="add", on_click=_cd_create_picker_dialog, color=None).props("flat dense no-caps")
                if dict(getattr(page_state, "explorer_clipboard", {}) or {}).get("scope") == "cd":
                    ui.button("Вставить", icon="content_paste", on_click=_cd_paste_clipboard, color=None).props("outline dense no-caps")
                ui.separator().props("vertical")
                ui.button(f"тип: {page_state.explorer_ext.lower()}", color=None).props("flat dense no-caps").classes("rag-filter-top-action")
                ui.button("изменён: любой", color=None).props("flat dense no-caps").classes("rag-filter-top-action")
                ui.button("размер: любой", color=None).props("flat dense no-caps").classes("rag-filter-top-action")
                ui.button(
                    "скрытые: да" if page_state.explorer_show_hidden else "скрытые: нет",
                    icon="visibility" if page_state.explorer_show_hidden else "visibility_off",
                    color=None,
                    on_click=lambda: (
                        setattr(page_state, "explorer_show_hidden", not bool(page_state.explorer_show_hidden)),
                        _save_explorer_settings(page_state),
                        render_fn(),
                    ),
                ).props("flat dense no-caps").classes("rag-filter-top-action")
                ui.space()
                ui.label("Сорт:").classes("rag-explorer-sort-label")
                ui.select(
                    ["По имени", "По размеру", "По дате"],
                    value=page_state.explorer_sort,
                    on_change=lambda e: _set_cd_sort(str(e.value or "По имени")),
                ).props("dense outlined").classes("w-36")
                ui.button(
                    icon="arrow_downward" if page_state.explorer_desc else "arrow_upward",
                    on_click=_toggle_cd_sort_direction,
                    color=None,
                ).props("flat round dense").classes("rag-explorer-iconbtn").tooltip(
                    "По убыванию" if page_state.explorer_desc else "По возрастанию"
                )
                ui.select(
                    ["Таблица", "Список"],
                    value=page_state.explorer_view if page_state.explorer_view in ("Таблица", "Список") else "Таблица",
                    on_change=lambda e: (
                        setattr(page_state, "explorer_view", e.value),
                        setattr(page_state, "explorer_visible_count", _EXPLORER_PAGE_SIZE),
                        _save_explorer_settings(page_state),
                        render_fn(),
                    ),
                ).props("dense outlined").classes("w-32")

        # ── Layout skeleton ───────────────────────────────────────────────
        with ui.row().classes("rag-explorer-v2-layout w-full gap-3 items-start"):
            tree_col = ui.column().classes("rag-explorer-tree rag-card p-3 gap-2")
            main_col = ui.column().classes("rag-explorer-files rag-card p-3 gap-3")
            details_col = ui.column().classes("rag-explorer-details rag-card p-3 gap-3")

        # ── Tree column ───────────────────────────────────────────────────
        with tree_col:
            ui.label("ДЕРЕВО").classes("rag-section-label")
            if root_folder is None:
                with ui.element("div").classes("cd-empty-state"):
                    ui.icon("cloud_off", size="24px").classes("opacity-30")
                    ui.label("Реестр пуст. Запустите импорт в настройках Cloud Drive.").classes("text-center text-xs")
            else:
                ancestor_paths = {folder.path for folder in breadcrumbs}
                if root_folder.path not in page_state.explorer_tree_open:
                    page_state.explorer_tree_open.append(root_folder.path)
                open_paths = set(page_state.explorer_tree_open) | ancestor_paths

                def _toggle_tree_path(path: str) -> None:
                    current = set(page_state.explorer_tree_open)
                    if path in current and path not in ancestor_paths:
                        current.remove(path)
                    else:
                        current.add(path)
                    page_state.explorer_tree_open = sorted(current)
                    render_fn()

                def _render_tree_node_cd(folder: CloudDriveFolder, depth: int) -> None:
                    is_current = folder.path == cd_path or (not cd_path and folder.is_root)
                    is_ancestor = folder.path in ancestor_paths and not is_current
                    is_open = folder.path in open_paths
                    children: List[CloudDriveFolder] = []
                    omitted_children = 0
                    if is_open:
                        children, _unused_files = _cd_list_children(
                            svc,
                            folder.path,
                            cfg=page_state.cfg,
                            user=page_state.current_user,
                        )
                        if not page_state.explorer_show_hidden:
                            children = [child for child in children if not _is_hidden("cd", child.path)]
                        required_children = [child for child in children if child.path in ancestor_paths]
                        visible_children = list(children[:_TREE_CHILD_LIMIT])
                        visible_ids = {child.id for child in visible_children}
                        visible_children.extend(child for child in required_children if child.id not in visible_ids)
                        omitted_children = max(0, len(children) - len(visible_children))
                        children = visible_children
                    has_children = bool(children) if is_open else True
                    icon = "folder_open" if is_open or is_current else "folder"
                    label = "Корень" if folder.is_root else folder.name
                    row_classes = (
                        "rag-tree-row"
                        + (" active" if is_current else "")
                        + (" ancestor" if is_ancestor else "")
                        + (" rag-hidden-item" if _is_hidden("cd", folder.path) else "")
                    )
                    with ui.element("div").classes(row_classes).style(f"padding-left: {depth * 12}px"):
                        if has_children:
                            ui.button(
                                icon="expand_more" if is_open else "chevron_right",
                                on_click=lambda p=folder.path: _toggle_tree_path(p),
                                color=None,
                            ).props("flat round dense").classes("rag-tree-toggle").tooltip(
                                "Свернуть" if is_open else "Раскрыть"
                            )
                        else:
                            ui.element("span").classes("rag-tree-toggle")
                        btn = ui.button(
                            label, icon=icon,
                            on_click=lambda p=folder.path: _cd_open_folder(p),
                            color=None,
                        ).props("flat align=left no-caps dense").classes(
                            "rag-nav-button rag-tree-button rag-tree-label"
                            + (" ancestor" if is_ancestor else "")
                        )
                        btn.props(_overflow_title_prop(folder.path or "Корень"))
                    if has_children and is_open:
                        for child in children:
                            _render_tree_node_cd(child, depth + 1)
                        if omitted_children:
                            ui.label(f"Ещё папок: {omitted_children}").classes("rag-meta text-xs ml-8")

                _render_tree_node_cd(root_folder, 0)

            # Корзина (scaffold — soft delete pending backend)
            ui.separator().classes("my-1")
            _is_trash = page_state.explorer_cd_path == "__trash__"
            trash_btn = ui.button(
                "Корзина", icon="delete_outline",
                on_click=lambda: _cd_open_folder("__trash__"),
                color=None,
            ).props("flat align=left no-caps dense").classes(
                "rag-nav-button rag-tree-button w-full" + (" active" if _is_trash else "")
            )
            trash_btn.tooltip("Удалённые файлы и папки. Можно восстановить, пока объект есть в реестре.")

        # ── Details column ────────────────────────────────────────────────
        current_folder_size_labels: List[Any] = []
        with details_col:
            ui.label("Свойства").classes("font-semibold text-sm")
            if breadcrumbs:
                current_node = breadcrumbs[-1]
                ui.label(current_node.name or "Корень").classes("font-semibold truncate")
                ui.label(current_node.path or "/").classes("rag-path text-xs")
                ui.label(f"Изменена: {_cloud_node_modified_label(current_node)}").classes("rag-meta text-xs")
            else:
                ui.label("Корень").classes("font-semibold")
            ui.label(f"На этом уровне: папки {direct_folder_count} · файлы {direct_file_count}").classes(
                "rag-meta text-xs"
            )
            ui.label(
                f"Файлы на этом уровне: {_cd_file_size(direct_file_size)}"
            ).classes("rag-meta text-xs")
            recursive_size = cached_folder_sizes.get(current_folder_id) if current_folder_id else None
            recursive_size_text = (
                _cd_file_size(int(recursive_size or 0)) if recursive_size is not None else "Считается..."
            )
            current_size_label = ui.label(
                f"Общий размер с вложенными папками: {recursive_size_text}"
            ).classes("rag-meta text-xs font-medium")
            current_folder_size_labels.append(current_size_label)
            ui.separator()
            ui.label("Действия").classes("font-semibold text-sm")
            ui.button("Создать", icon="add", on_click=_cd_create_picker_dialog, color=None).props("flat dense no-caps align=left").classes("w-full")
            ui.button(
                "Загрузить файлы", icon="upload_file",
                on_click=_cd_upload_dialog, color=None,
            ).props("flat dense no-caps align=left").classes("w-full")
            if cd_path:
                ui.button(
                    "Найти в этой папке", icon="search",
                    on_click=query_handler(f"path:{cd_path}"), color=None,
                ).props("flat dense no-caps align=left").classes("w-full")
            ui.separator()
            ui.label("Фильтры").classes("font-semibold text-sm")
            with ui.column().classes("w-full gap-1"):
                ui.label(f"Тип: {page_state.explorer_ext}").classes(
                    "rag-chip rag-filter-chip" + (" active" if page_state.explorer_ext != "Все" else "")
                )
                ui.label(f"Вид: {page_state.explorer_view}").classes("rag-chip rag-filter-chip")
                ui.label(f"Сорт.: {page_state.explorer_sort}").classes(
                    "rag-chip rag-filter-chip" + (" active" if page_state.explorer_sort != "По имени" or page_state.explorer_desc else "")
                )
                ui.label("Скрытые: показаны" if page_state.explorer_show_hidden else "Скрытые: скрыты").classes(
                    "rag-chip rag-filter-chip" + (" active" if page_state.explorer_show_hidden else "")
                )

        # ── Main column ───────────────────────────────────────────────────
        with main_col:
            visible_keys = [
                *[_selection_key("cd", folder.path) for folder in page_folders],
                *[_selection_key("cd", f.path) for f in page_files],
            ]
            if _is_trash_view:
                with ui.row().classes("w-full items-center gap-2"):
                    ui.icon("delete_outline", size="22px").classes("text-slate-500")
                    ui.label("Корзина").classes("text-xl font-semibold flex-1")
                    ui.button("В корень", icon="arrow_back", on_click=lambda: _cd_open_folder("")).props("flat dense no-caps")
                    ui.button(icon="refresh", on_click=render_fn, color=None).props("flat round dense").tooltip("Обновить")
                try:
                    trash_result = svc.list_trash(limit=250)
                    trash_items = list(trash_result.get("items") or [])
                except Exception as exc:
                    ui.label(f"Не удалось прочитать корзину: {exc}").classes("text-negative rag-card p-3")
                    return
                if not trash_items:
                    with ui.column().classes("w-full items-center justify-center py-12 gap-3"):
                        ui.icon("delete_outline", size="56px").classes("text-slate-300 dark:text-slate-600")
                        ui.label("Корзина пуста").classes("text-xl font-semibold text-slate-500")
                        ui.label("Удалённые файлы и папки появятся здесь для восстановления.").classes(
                            "rag-meta text-xs text-center max-w-sm"
                        )
                    return
                with ui.column().classes("w-full gap-2"):
                    for item in trash_items:
                        node_type = str(item.get("node_type") or "")
                        is_folder = node_type == "folder"
                        path = str(item.get("path") or "")
                        name = str(item.get("name") or path.rsplit("/", 1)[-1] or "/")
                        deleted_at = str(item.get("deleted_at") or "")
                        with ui.row().classes("rag-explorer-item w-full p-2 items-center gap-3"):
                            ui.icon("folder" if is_folder else "description", size="24px").classes(
                                "text-yellow-500" if is_folder else "text-slate-500"
                            )
                            with ui.column().classes("flex-1 min-w-0 gap-0"):
                                ui.label(name).classes("text-sm font-medium truncate")
                                ui.label(path or "/").classes("rag-path text-xs truncate")
                                ui.label(f"Удалено: {deleted_at[:19].replace('T', ' ') if deleted_at else 'неизвестно'}").classes("rag-meta text-xs")
                            if not is_folder:
                                ui.label(_cd_file_size(int(item.get("size_bytes") or 0))).classes("rag-meta text-xs")
                            ui.button(
                                "Восстановить",
                                icon="restore_from_trash",
                                on_click=lambda p=path: _cd_restore_node(p),
                                color=None,
                            ).props("outline dense no-caps")
                return

            # Entry stats bar
            stats_classes = "rag-cd-entry-stats w-full items-center gap-2 px-1"
            if page_state.explorer_view != "Список":
                stats_classes += " rag-cd-table-stats"
            with ui.row().classes(stats_classes):
                ui.label(f"Папок: {len(child_folders)} · Файлов: {total_files}").classes("rag-path flex-1")
                with ui.element("span").classes("cd-status-badge cd-status-done text-xs"):
                    ui.icon("cloud_done", size="14px")
                    ui.label("Cloud Drive")
            selection_refs = _render_selection_bar(scope="cd", visible_keys=visible_keys)

            # Empty state
            if root_folder is None:
                with ui.element("div").classes("cd-empty-state w-full"):
                    ui.icon("cloud_off", size="32px").classes("opacity-20")
                    ui.label("Реестр пуст — запустите импорт в Настройки → Cloud Drive.").classes("text-center")
            elif not child_folders and not child_files:
                with ui.element("div").classes("cd-empty-state w-full"):
                    ui.icon("folder_open", size="32px").classes("opacity-20")
                    ui.label("Папка пуста или элементы не соответствуют фильтру.").classes("text-center")
                    ui.button(
                        "Загрузить файлы", icon="upload_file",
                        on_click=_cd_upload_dialog,
                        color=None,
                    ).props("outline dense").classes("mt-2")
            else:
                # ── Список view ────────────────────────────────────────────
                if page_state.explorer_view == "Список":
                    if page_folders:
                        with ui.column().classes("rag-explorer-list w-full"):
                            for folder in page_folders:
                                item_key = _selection_key("cd", folder.path)
                                folder_row = ui.row().classes(
                                    "rag-explorer-item w-full p-2 items-center gap-3"
                                    + (" selected" if item_key in _selected_set() else "")
                                    + (" rag-hidden-item" if _is_hidden("cd", folder.path) else "")
                                )
                                folder_row.props(_cd_context_props(folder.path, is_folder=True))
                                with folder_row:
                                    _selection_checkbox(item_key, selection_refs)
                                    ui.html(_file_badge_html(folder.name, "Каталог"), sanitize=False)
                                    with ui.column().classes("flex-1 gap-0"):
                                        ui.button(
                                            folder.name,
                                            on_click=lambda p=folder.path: _cd_open_folder(p),
                                            color=None,
                                        ).props("flat align=left no-caps dense data-rag-open").classes("rag-nav-button w-full")
                                        ui.label(
                                            f"Изменена: {_cloud_node_modified_label(folder)} · "
                                            f"{_cd_folder_size_label(folder)}"
                                        ).classes("rag-meta text-xs")
                                    _cd_hidden_action_buttons(folder.path, is_folder=True, open_action=lambda p=folder.path: _cd_open_folder(p))
                                    render_star(Path(folder.source_path or folder.path), item_type="folder")
                                    if not folder.is_root:
                                        with ui.button(icon="more_vert", color=None).props("flat round dense"):
                                            with ui.menu():
                                                ui.menu_item(
                                                    "Переименовать",
                                                    on_click=lambda fo=folder: _cd_rename_dialog(fo.path, fo.name),
                                                    auto_close=True,
                                                )
                                                ui.menu_item(
                                                    "Переместить в…",
                                                    on_click=lambda fo=folder: _cd_move_dialog(fo.path, fo.name, is_folder=True),
                                                    auto_close=True,
                                                )
                                                ui.separator()
                                                ui.menu_item(
                                                    "Удалить папку…",
                                                    on_click=lambda fo=folder: _cd_delete_dialog(fo.path, fo.name, is_folder=True),
                                                    auto_close=True,
                                                ).classes("text-negative")
                    if page_files:
                        def _cd_download_url(file_path: str) -> str:
                            return f"/api/cloud-drive/download?path={quote(file_path, safe='')}"
                        with ui.column().classes("rag-explorer-list w-full"):
                            for f in page_files:
                                item_key = _selection_key("cd", f.path)
                                download_url = _cd_download_url(f.path)
                                file_row = ui.row().classes(
                                    "rag-explorer-item w-full p-2 items-center gap-3"
                                    + (" selected" if item_key in _selected_set() else "")
                                    + (" rag-hidden-item" if _is_hidden("cd", f.path) else "")
                                )
                                file_row.props(_cd_context_props(f.path, is_folder=False, download_url=download_url))
                                with file_row:
                                    _selection_checkbox(item_key, selection_refs)
                                    ui.html(_file_badge_html(f.name), sanitize=False)
                                    with ui.column().classes("flex-1 gap-0"):
                                        ui.button(
                                            f.name,
                                            on_click=lambda fi=f: _cd_open_file(fi),
                                            color=None,
                                        ).props("flat align=left no-caps dense data-rag-open").classes("rag-nav-button w-full")
                                        ui.label(
                                            f"Изменён: {_cloud_node_modified_label(f)} · {_cd_file_size(f.size_bytes)}"
                                        ).classes("rag-meta text-xs")
                                    _cd_hidden_action_buttons(f.path, is_folder=False, open_action=lambda fi=f: _cd_open_file(fi), download_url=download_url)
                                    ui.button(
                                        icon="history",
                                        on_click=lambda fi=f: _cd_versions_dialog(fi),
                                        color=None,
                                    ).props("flat round dense").tooltip("История версий")
                                    if f.storage_key:
                                        ui.button(
                                            icon="visibility",
                                            on_click=lambda fi=f: _cd_open_file(fi),
                                            color=None,
                                        ).props("flat round dense").tooltip("Просмотреть файл")
                                        ui.button(
                                            icon="download",
                                            on_click=lambda url=download_url: ui.navigate.to(url, new_tab=True),
                                            color=None,
                                        ).props("flat round dense").tooltip("Скачать файл")
                                    _render_file_status(f.id)
                                    with ui.button(icon="more_vert", color=None).props("flat round dense"):
                                        with ui.menu():
                                            ui.menu_item(
                                                "Переименовать",
                                                on_click=lambda fi=f: _cd_rename_dialog(fi.path, fi.name),
                                                auto_close=True,
                                            )
                                            ui.menu_item(
                                                "Переместить в…",
                                                on_click=lambda fi=f: _cd_move_dialog(fi.path, fi.name, is_folder=False),
                                                auto_close=True,
                                            )
                                            if _is_admin(state):
                                                ui.separator()
                                                ui.menu_item(
                                                    "Переиндексировать",
                                                    on_click=lambda fi=f: _cd_reindex_file(fi.path),
                                                    auto_close=True,
                                                )
                                            ui.separator()
                                            ui.menu_item(
                                                "Удалить файл…",
                                                on_click=lambda fi=f: _cd_delete_dialog(fi.path, fi.name, is_folder=False),
                                                auto_close=True,
                                            ).classes("text-negative")
                # ── Таблица view ───────────────────────────────────────────
                else:
                    def _cd_download_url(file_path: str) -> str:
                        return f"/api/cloud-drive/download?path={quote(file_path, safe='')}"
                    with ui.column().classes("w-full gap-0"):
                        with ui.element("div").classes("rag-file-table-header"):
                            with ui.element("div").classes("rag-file-select-icon header"):
                                _selection_page_checkbox(selection_refs)
                            with ui.element("div").classes("rag-file-table-head-name min-w-0"):
                                _render_sort_header("Имя", "По имени")
                                ui.label(f"Папок: {len(child_folders)} · Файлов: {total_files}").classes(
                                    "rag-path rag-cd-mobile-count"
                                )
                            _render_sort_header("Изменён", "По дате")
                            with ui.element("div").classes("rag-file-table-head-size min-w-0"):
                                _render_sort_header("Размер", "По размеру")
                                with ui.element("span").classes("cd-status-badge cd-status-done text-xs rag-cd-mobile-badge"):
                                    ui.icon("cloud_done", size="14px")
                                    ui.label("Cloud Drive")
                            ui.label("Автор").classes("rag-col-header")
                            ui.label("Индекс").classes("rag-col-header")
                            ui.element("div")
                        for folder in page_folders:
                            item_key = _selection_key("cd", folder.path)
                            folder_row_classes = (
                                "rag-file-table-row"
                                + (" selected" if item_key in _selected_set() else "")
                                + (" rag-hidden-item" if _is_hidden("cd", folder.path) else "")
                            )
                            folder_row = ui.element("div").classes(folder_row_classes)
                            folder_row.props(_cd_context_props(folder.path, is_folder=True))
                            with folder_row:
                                _selection_badge(folder.name, "Каталог", item_key, selection_refs)
                                with ui.element("div").classes("rag-file-table-name min-w-0"):
                                    ui.html(_file_badge_html(folder.name, "Каталог"), sanitize=False)
                                    ui.button(
                                        folder.name,
                                        on_click=lambda p=folder.path: _cd_open_folder(p),
                                        color=None,
                                    ).props("flat align=left no-caps dense data-rag-open").classes("rag-nav-button w-full")
                                ui.label(_cloud_node_modified_label(folder)).classes("rag-meta text-xs")
                                size_label = ui.label(_cd_folder_size_label(folder)).classes("rag-meta text-xs")
                                folder_size_labels.setdefault(str(folder.id), []).append(size_label)
                                ui.label("admin").classes("rag-meta text-xs")
                                ui.label("✓").classes("rag-file-table-index-ok")
                                with ui.element("div").classes("rag-file-table-actions"):
                                    render_star(Path(folder.source_path or folder.path), item_type="folder")
                                    if not folder.is_root:
                                        with ui.button(icon="more_vert", color=None).props("flat round dense"):
                                            with ui.menu():
                                                ui.menu_item(
                                                    "Переименовать",
                                                    on_click=lambda fo=folder: _cd_rename_dialog(fo.path, fo.name),
                                                    auto_close=True,
                                                )
                                                ui.menu_item(
                                                    "Переместить в…",
                                                    on_click=lambda fo=folder: _cd_move_dialog(fo.path, fo.name, is_folder=True),
                                                    auto_close=True,
                                                )
                                                ui.separator()
                                                ui.menu_item(
                                                    "Удалить папку…",
                                                    on_click=lambda fo=folder: _cd_delete_dialog(fo.path, fo.name, is_folder=True),
                                                    auto_close=True,
                                                ).classes("text-negative")
                                _cd_hidden_action_buttons(folder.path, is_folder=True, open_action=lambda p=folder.path: _cd_open_folder(p))
                        for f in page_files:
                            item_key = _selection_key("cd", f.path)
                            download_url = _cd_download_url(f.path)
                            file_row_classes = (
                                "rag-file-table-row"
                                + (" selected" if item_key in _selected_set() else "")
                                + (" rag-hidden-item" if _is_hidden("cd", f.path) else "")
                            )
                            file_row = ui.element("div").classes(file_row_classes)
                            file_row.props(_cd_context_props(f.path, is_folder=False, download_url=download_url))
                            with file_row:
                                _selection_badge(f.name, "Файл", item_key, selection_refs)
                                with ui.element("div").classes("rag-file-table-name min-w-0"):
                                    ui.html(_file_badge_html(f.name, "Файл"), sanitize=False)
                                    ui.button(
                                        f.name,
                                        on_click=lambda fi=f: _cd_open_file(fi),
                                        color=None,
                                    ).props("flat align=left no-caps dense data-rag-open").classes("rag-nav-button w-full")
                                ui.label(_cloud_node_modified_label(f)).classes("rag-meta text-xs")
                                ui.label(_cd_file_size(f.size_bytes) if f.size_bytes else "—").classes("rag-meta text-xs")
                                ui.label("admin").classes("rag-meta text-xs")
                                with ui.element("div").classes("rag-file-table-index"):
                                    if _file_jobs.get(f.id):
                                        _render_file_status(f.id)
                                    else:
                                        ui.label("✓").classes("rag-file-table-index-ok")
                                with ui.element("div").classes("rag-file-table-actions"):
                                    src = str(f.source_path or f.path or "")
                                    if src:
                                        ui.button(
                                            icon="open_in_new",
                                            on_click=lambda p=src: _select_in_os_explorer(p),
                                            color=None,
                                        ).props("flat round dense").tooltip("Выделить файл в Проводнике Windows")
                                    ui.button(
                                        icon="history",
                                        on_click=lambda fi=f: _cd_versions_dialog(fi),
                                        color=None,
                                    ).props("flat round dense").tooltip("История версий")
                                    if f.storage_key:
                                        ui.button(
                                            icon="visibility",
                                            on_click=lambda fi=f: _cd_open_file(fi),
                                            color=None,
                                        ).props("flat round dense").tooltip("Просмотреть файл")
                                        ui.button(
                                            icon="download",
                                            on_click=lambda url=download_url: ui.navigate.to(url, new_tab=True),
                                            color=None,
                                        ).props("flat round dense").tooltip("Скачать файл")
                                    render_star(Path(f.source_path or f.path or f.name), item_type="file")
                                    with ui.button(icon="more_vert", color=None).props("flat round dense"):
                                        with ui.menu():
                                            ui.menu_item(
                                                "Переименовать",
                                                on_click=lambda fi=f: _cd_rename_dialog(fi.path, fi.name),
                                                auto_close=True,
                                            )
                                            ui.menu_item(
                                                "Переместить в…",
                                                on_click=lambda fi=f: _cd_move_dialog(fi.path, fi.name, is_folder=False),
                                                auto_close=True,
                                            )
                                            if _is_admin(state):
                                                ui.separator()
                                                ui.menu_item(
                                                    "Переиндексировать",
                                                    on_click=lambda fi=f: _cd_reindex_file(fi.path),
                                                    auto_close=True,
                                                )
                                            ui.separator()
                                            ui.menu_item(
                                                "Удалить файл…",
                                                on_click=lambda fi=f: _cd_delete_dialog(fi.path, fi.name, is_folder=False),
                                                auto_close=True,
                                            ).classes("text-negative")
                                _cd_hidden_action_buttons(f.path, is_folder=False, open_action=lambda fi=f: _cd_open_file(fi), download_url=download_url)

                # Progressive loading keeps the first render bounded without forcing
                # users through expensive full-screen page transitions.
                visible_entry_count = len(page_entries)
                remaining_entries = max(0, total_entries - visible_entry_count)
                if remaining_entries:
                    def _load_more_entries() -> None:
                        page_state.explorer_visible_count = min(
                            total_entries,
                            visible_entry_count + _EXPLORER_PAGE_SIZE,
                        )
                        _log_app_event(
                            page_state,
                            "cd_explorer",
                            "load_more",
                            details={
                                "path": cd_path,
                                "visible": page_state.explorer_visible_count,
                                "total": total_entries,
                            },
                        )
                        render_fn()

                    with ui.column().classes("w-full items-center gap-1 mt-2"):
                        ui.button(
                            f"Загрузить ещё ({min(_EXPLORER_PAGE_SIZE, remaining_entries)})",
                            icon="expand_more",
                            on_click=_load_more_entries,
                            color=None,
                        ).props("flat dense no-caps").classes("rag-explorer-load-more")
                        ui.label(f"Показано {visible_entry_count} из {total_entries}").classes("rag-meta text-xs")

                # ── Drop zone ─────────────────────────────────────────────────
                with ui.element("div").classes("w-full mt-3"):
                    async def _handle_drop_upload(e: "Any") -> None:
                        filename = str(getattr(e, "name", "") or "").strip()
                        content = getattr(e, "content", None)
                        if not filename or content is None:
                            return
                        import tempfile as _tempfile
                        _suffix = Path(filename).suffix
                        with _tempfile.NamedTemporaryFile(delete=False, suffix=_suffix) as _tmp:
                            _tmp.write(content.read())
                            _tmp_path = _tmp.name
                        try:
                            await run.io_bound(
                                svc.upload_file,
                                parent_path=page_state.explorer_cd_path or "",
                                filename=filename,
                                source_path=_tmp_path,
                                mime_type="",
                            )
                            ui.notify(f"Загружен: «{filename}»", type="positive")
                            render_fn()
                        except Exception as _exc:
                            ui.notify(f"Ошибка загрузки «{filename}»: {_exc}", type="negative")
                        finally:
                            Path(_tmp_path).unlink(missing_ok=True)

                    ui.upload(
                        multiple=True,
                        on_upload=_handle_drop_upload,
                        auto_upload=True,
                        label="Перетащите файлы сюда для загрузки",
                    ).props("flat bordered").classes("w-full cd-drop-zone")

        if folder_size_ids and folder_sizes_need_refresh and not page_state.explorer_folder_size_loading:
            requested_path = str(page_state.explorer_cd_path or "")
            requested_ids = list(folder_size_ids)
            page_state.explorer_folder_size_loading = True

            async def _load_folder_sizes() -> None:
                started = time.perf_counter()
                try:
                    sizes = await run.io_bound(svc.registry.folder_size_bytes_map, requested_ids)
                    refreshed_at = time.monotonic()
                    page_state.explorer_folder_sizes.update({str(key): int(value or 0) for key, value in sizes.items()})
                    page_state.explorer_folder_size_cached_at.update({str(key): refreshed_at for key in requested_ids})
                    _log_app_event(
                        page_state,
                        "cd_explorer",
                        "folder_sizes_loaded",
                        details={
                            "path": requested_path,
                            "folders": len(requested_ids),
                            "duration_ms": round((time.perf_counter() - started) * 1000),
                        },
                    )
                    if str(page_state.explorer_cd_path or "") == requested_path:
                        for folder_id, labels in folder_size_labels.items():
                            text = _cd_file_size(int(page_state.explorer_folder_sizes.get(folder_id, 0) or 0))
                            for label in labels:
                                try:
                                    label.set_text(text)
                                except RuntimeError:
                                    pass
                        if current_folder_id:
                            total_text = _cd_file_size(
                                int(page_state.explorer_folder_sizes.get(current_folder_id, 0) or 0)
                            )
                            for label in current_folder_size_labels:
                                try:
                                    label.set_text(f"Общий размер с вложенными папками: {total_text}")
                                except RuntimeError:
                                    pass
                        render_fn()
                except Exception as exc:
                    _log_app_event(
                        page_state,
                        "cd_explorer",
                        "folder_sizes_failed",
                        ok=False,
                        details={
                            "path": requested_path,
                            "folders": len(requested_ids),
                            "duration_ms": round((time.perf_counter() - started) * 1000),
                            "error": str(exc)[:500],
                        },
                    )
                finally:
                    page_state.explorer_folder_size_loading = False
                    if page_state.screen == "explorer" and str(page_state.explorer_cd_path or "") != requested_path:
                        render_fn()

            ui.timer(0.0, _load_folder_sizes, once=True)


    # ── Explorer screen body ─────────────────────────────────────────────
    # ── Cloud Drive registry mode ─────────────────────────────────────
    _cd_svc = _cd_get_service(state.cfg)
    if _cd_svc is not None:
        _render_cd_explorer(state, _cd_svc)
        return

    # ── Legacy os-walk mode ───────────────────────────────────────────
    root = Path(str(state.cfg.get("catalog_path") or ""))
    if not root.exists():
        ui.label(f"Каталог не найден: {root}").classes("text-red-700 rag-card p-4")
        return

    toolbar = ui.column().classes("w-full gap-3")
    with ui.row().classes("rag-explorer-v2-layout w-full gap-3 items-start"):
        tree_area = ui.column().classes("rag-explorer-tree rag-card p-3 gap-2")
        entries_area = ui.column().classes("rag-explorer-files rag-card p-3 gap-3")
        details_area = ui.column().classes("rag-explorer-details rag-card p-3 gap-3")

    def open_folder(path: Path) -> None:
        state.explorer_path = str(path)
        state.explorer_page = 0
        state.explorer_visible_count = _EXPLORER_PAGE_SIZE
        state.explorer_selected_paths = []
        _get_auth_db(state).touch_favorite(username=_username(state), path=str(path))
        _log_app_event(state, "explorer", "open_folder", details={"path": str(path)})
        render_fn()

    def open_file(path: Path) -> None:
        if path.exists() and path.is_file():
            _get_auth_db(state).touch_favorite(username=_username(state), path=str(path))
            _log_app_event(state, "explorer", "open_file", details={"path": str(path)})
            open_file_viewer_fn(path)

    def copy_path(path: Path) -> None:
        ui.run_javascript(f"navigator.clipboard.writeText({json.dumps(str(path))})")
        ui.notify("Путь скопирован.", type="positive")

    _OCR_EXTS = frozenset({".pdf", ".png", ".jpg", ".jpeg", ".bmp", ".gif", ".tiff", ".tif", ".webp"})

    async def open_recognize_dialog(path: Path) -> None:
        from rag_catalog.core.ocr_runtime import recognize_single_file  # noqa: PLC0415

        _text_holder: List[str] = [""]

        with ui.dialog() as dlg, ui.card().classes("w-[min(960px,96vw)] max-h-[90vh] flex flex-col p-4 gap-3"):
            with ui.row().classes("w-full items-center gap-2"):
                with ui.column().classes("flex-1 gap-0 min-w-0"):
                    ui.label("Распознавание текста").classes("text-lg font-semibold")
                    ui.label(path.name).classes("font-medium truncate")
                    ui.label(str(path)).classes("rag-path text-xs")
                ui.button(icon="close", on_click=dlg.close, color=None).props("flat round dense")

            with ui.row().classes("w-full items-center gap-3") as spinner_row:
                ui.spinner("dots", size="1.8em").classes("text-indigo-500")
                ui.label("Распознаю…").classes("rag-meta")

            meta_label = ui.label("").classes("rag-meta")
            meta_label.set_visibility(False)

            with ui.element("div").style(
                "flex:1;min-height:240px;max-height:52vh;overflow-y:auto;"
                "border:1px solid #e5e7eb;border-radius:6px;padding:8px;"
                "font-family:var(--rag-font-mono,monospace);font-size:12px;background:#f9fafb"
            ) as result_box:
                result_html = ui.html("")
            result_box.set_visibility(False)

            with ui.row().classes("w-full justify-end gap-2"):
                copy_btn = ui.button("Копировать", icon="content_copy").props("outline")
                copy_btn.set_visibility(False)
                copy_btn.on("click", lambda: ui.run_javascript(
                    f"navigator.clipboard&&navigator.clipboard.writeText({json.dumps(_text_holder[0])})"
                ))
                ui.button("Закрыть", on_click=dlg.close).props("unelevated")

        dlg.open()

        result = await run.io_bound(recognize_single_file, path, state.cfg)

        spinner_row.set_visibility(False)
        text = str(result.get("text") or "")
        pages = int(result.get("pages") or 0)
        chars = int(result.get("chars") or 0)
        from_cache = bool(result.get("from_cache"))
        r_status = str(result.get("status") or "ok")
        error = str(result.get("error") or "")

        _text_holder[0] = text

        if r_status in ("error", "unsupported"):
            meta_label.set_text(f"Ошибка: {error}" if error else "Не удалось распознать файл")
            meta_label.classes(add="text-red-600")
        elif r_status == "empty" or not text.strip():
            meta_label.set_text("Текст не обнаружен — возможно, изображение не содержит читаемых символов")
        else:
            cache_note = " · из кэша" if from_cache else ""
            meta_label.set_text(f"{chars:,} символов · {pages} стр.{cache_note}".replace(",", " "))
            # Render text with page separators
            html_parts: List[str] = []
            for line in text.split("\n"):
                if line.startswith("Страница:"):
                    html_parts.append(
                        f'<div style="color:#6366f1;font-weight:600;border-top:1px solid #e5e7eb;'
                        f'margin:6px 0 3px;padding-top:4px">{html.escape(line)}</div>'
                    )
                else:
                    html_parts.append(f'<div style="margin-bottom:1px">{html.escape(line) or "&nbsp;"}</div>')
            result_html.set_content("".join(html_parts))
            result_box.set_visibility(True)
            copy_btn.set_visibility(True)

        meta_label.set_visibility(True)

    def explorer_context_props(path: Path, *, is_dir: bool) -> str:
        item_type = "folder" if is_dir else "file"
        item_url = "" if is_dir else _viewer_file_url(str(path))
        favorite = "true" if _is_favorite(state, str(path)) else "false"
        attrs = {
            "data-rag-context": "explorer-item",
            "data-rag-type": item_type,
            "data-rag-path": quote(str(path), safe=""),
            "data-rag-url": item_url,
            "data-rag-favorite": favorite,
        }
        return " ".join(f'{key}="{html.escape(value, quote=True)}"' for key, value in attrs.items())

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
                    ui.button(icon="close", on_click=lambda p=fav_path: (_toggle_favorite(state, p), dialog.close(), render_fn())).props("flat round dense").tooltip("Убрать из избранного")
            ui.button("Закрыть", on_click=dialog.close).props("flat")
        dialog.open()

    def render_tile(path: Path, is_dir: bool, size_class: str, selection_refs: dict[str, Any]) -> None:
        icon = _file_icon_svg(str(path), "Каталог" if is_dir else "Файл")
        click = (lambda p=path: open_folder(p)) if is_dir else (lambda p=path: open_file(p))
        system_class = " system" if not is_dir and _is_system_file(path) else ""
        item_key = _selection_key("fs", str(path))
        tile = ui.column().classes(
            f"rag-explorer-item items-center gap-1 p-2 {size_class}{system_class}"
            + (" selected" if item_key in _selected_set() else "")
        )
        tile.props(explorer_context_props(path, is_dir=is_dir))
        with tile:
            with ui.element("div").classes("rag-tile-select-wrap"):
                _selection_checkbox(item_key, selection_refs)
            with ui.element("div").classes("rag-tile-star-wrap"):
                render_star(path, item_type="folder" if is_dir else "file")
            opener = ui.column().classes("rag-explorer-opener items-center gap-1 cursor-pointer").on("click", click)
            opener.props("data-rag-open")
            with opener:
                ui.html(icon, sanitize=False)
                name_label = ui.label(path.name).classes("rag-explorer-name text-center text-sm")
                name_label.tooltip(str(path.name))
            _os_fn_tile = (lambda p=path: _open_os_path(str(p))) if is_dir else (lambda p=path: _select_in_os_explorer(str(p)))
            os_button = ui.button(on_click=_os_fn_tile).props("data-rag-os")
            os_button.classes("hidden")

    def render_row(path: Path, is_dir: bool, selection_refs: dict[str, Any], compact: bool = False) -> None:
        try:
            stat = path.stat()
            size = "" if is_dir else _format_file_size(stat.st_size)
            modified = time.strftime("%d.%m.%Y %H:%M", time.localtime(stat.st_mtime))
        except Exception:
            size, modified = "", ""
        system_class = " system" if not is_dir and _is_system_file(path) else ""
        item_key = _selection_key("fs", str(path))
        row = ui.row().classes(
            f"rag-explorer-item w-full p-2 items-center gap-3{system_class}"
            + (" selected" if item_key in _selected_set() else "")
        )
        row.props(explorer_context_props(path, is_dir=is_dir))
        with row:
            _selection_checkbox(item_key, selection_refs)
            ui.html(_file_badge_html(str(path), "Каталог" if is_dir else "Файл"), sanitize=False)
            action = (lambda p=path: open_folder(p)) if is_dir else (lambda p=path: open_file(p))
            with ui.column().classes("flex-1 gap-0"):
                open_btn = ui.button(path.name, on_click=action, color=None).props("flat align=left no-caps dense data-rag-open").classes("rag-nav-button w-full")
                open_btn.tooltip(str(path.name))
                if not compact:
                    ui.label(f"{'Папка' if is_dir else path.suffix or 'без расширения'} · {size} · {modified}").classes("rag-meta")
            if not compact:
                if not is_dir:
                    ui.button("Скачать", icon="download", on_click=lambda p=path: (_log_app_event(state, "explorer", "download", details={"path": str(p)}), ui.download(p, filename=p.name))).props("outline dense")
                    if path.suffix.lower() in _OCR_EXTS:
                        ui.button("Распознать", icon="document_scanner", on_click=lambda p=path: open_recognize_dialog(p)).props("outline dense").tooltip("Распознать текст (OCR)")
                _os_fn = (lambda p=path: _open_os_path(str(p))) if is_dir else (lambda p=path: _select_in_os_explorer(str(p)))
                ui.button("ОС", icon="open_in_new", on_click=_os_fn).props("flat dense data-rag-os").tooltip("Открыть в проводнике Windows" if is_dir else "Выделить файл в проводнике Windows")
            else:
                _os_fn2 = (lambda p=path: _open_os_path(str(p))) if is_dir else (lambda p=path: _select_in_os_explorer(str(p)))
                os_button = ui.button(on_click=_os_fn2).props("data-rag-os")
                os_button.classes("hidden")
            render_star(path, item_type="folder" if is_dir else "file")

    def _explorer_path_parts(root_path: Path, current_path: Path) -> List[Path]:
        parts: List[Path] = []
        p = current_path
        while True:
            parts.append(p)
            if p == root_path or p == p.parent:
                break
            p = p.parent
        parts.reverse()
        return parts

    def _child_dirs(path: Path) -> List[Path]:
        try:
            return sorted(
                [p for p in path.iterdir() if p.is_dir() and not p.name.startswith(".")],
                key=lambda p: p.name.lower(),
            )
        except Exception:
            return []

    def render_tree_node(path: Path, depth: int, current_path: Path, ancestors: set[str]) -> None:
        path_key = str(path)
        is_current = path_key == str(current_path)
        is_ancestor = path_key in ancestors and not is_current
        class_bits = ["rag-nav-button", "rag-tree-button", "w-full"]
        if is_current:
            class_bits.append("active")
        elif is_ancestor:
            class_bits.append("ancestor")
        icon = "folder_open" if is_current or is_ancestor else "folder"
        label = "Обмен" if path == root else path.name
        ui.button(
            label,
            icon=icon,
            on_click=lambda p=path: open_folder(p),
            color=None,
        ).props(f"flat align=left no-caps dense {_overflow_title_prop(path)}").classes(" ".join(class_bits)).style(f"padding-left: {depth * 12}px")
        if is_current or is_ancestor:
            for child in _child_dirs(path):
                render_tree_node(child, depth + 1, current_path, ancestors)

    def render_explorer_details() -> None:
        details_area.clear()
        with details_area:
            ui.label("Фильтры").classes("text-lg font-semibold")
            type_active = state.explorer_ext != "Все"
            sort_active = state.explorer_sort != "По имени" or state.explorer_desc
            view_active = state.explorer_view != "Таблица"
            name_active = bool(state.explorer_filter.strip())
            with ui.row().classes("w-full gap-2 flex-wrap"):
                ui.label(f"Тип: {state.explorer_ext}").classes("rag-chip rag-filter-chip" + (" active" if type_active else ""))
                ui.label(f"Вид: {state.explorer_view}").classes("rag-chip rag-filter-chip" + (" active" if view_active else ""))
                ui.label(
                    f"{state.explorer_sort} · {'убывание' if state.explorer_desc else 'возрастание'}"
                ).classes("rag-chip rag-filter-chip" + (" active" if sort_active else ""))
                if name_active:
                    ui.label(f"Имя: {state.explorer_filter}").classes("rag-chip rag-filter-chip active")
            ui.separator()
            ui.label("Свойства").classes("text-lg font-semibold")
            current_details = _safe_explorer_path(state)
            ui.label(current_details.name or str(current_details)).classes("font-semibold truncate")
            ui.label(str(current_details)).classes("rag-path")
            with ui.row().classes("w-full gap-1 mt-1"):
                ui.button(icon="content_copy", on_click=lambda p=current_details: copy_path(p), color=None).props("flat round dense").tooltip("Скопировать путь")
                ui.button(icon="open_in_new", on_click=lambda p=current_details: _open_os_path(str(p)), color=None).props("flat round dense").tooltip("Открыть в Проводнике Windows")

    def render_entries() -> None:
        entries_area.clear()
        current = _safe_explorer_path(state)
        if not current.exists():
            state.explorer_path = str(root)
            current = root

        def _render_fs_header_breadcrumbs() -> None:
            if state.header_breadcrumbs is None:
                return
            state.header_breadcrumbs.clear()
            with state.header_breadcrumbs:
                _render_fs_breadcrumb_buttons()

        def _render_fs_breadcrumb_buttons() -> None:
            ui.icon("folder", size="16px").classes("text-slate-400")
            parts = _explorer_path_parts(root, current)
            for idx, part in enumerate(parts):
                label = "Обмен" if part == root else part.name
                button = ui.button(
                    label,
                    on_click=lambda p=part: (
                        _log_app_event(state, "explorer", "breadcrumb", details={"path": str(p)}),
                        open_folder(p),
                    ),
                    color=None,
                ).props("flat dense no-caps")
                button.tooltip(str(part))
                if idx < len(parts) - 1:
                    ui.icon("chevron_right").classes("text-slate-500")

        def _render_fs_inline_breadcrumbs() -> None:
            with ui.row().classes("rag-explorer-inline-breadcrumbs rag-breadcrumbs gap-1 no-wrap"):
                _render_fs_breadcrumb_buttons()

        _render_fs_header_breadcrumbs()
        if state.header_explorer_actions is not None:
            state.header_explorer_actions.clear()

        dirs, files, total_files = _file_rows(current, state)
        state.explorer_page = max(0, min(state.explorer_page, max(0, (len(files) - 1) // PAGE_SIZE)))
        page_files = files[state.explorer_page * PAGE_SIZE : (state.explorer_page + 1) * PAGE_SIZE]
        visible_keys = [_selection_key("fs", str(path)) for path in [*dirs, *page_files]]

        with entries_area:
            _render_fs_inline_breadcrumbs()
            with ui.row().classes("w-full items-center gap-2"):
                up_button = ui.button(icon="arrow_upward", on_click=lambda: (_log_app_event(state, "explorer", "up", details={"path": str(current.parent)}), open_folder(current.parent)), color=None).props("outline round dense")
                up_button.tooltip("На уровень выше")
                if current == root:
                    up_button.disable()
                ui.label(f"папок {len(dirs)} · файлов {total_files}").classes("rag-path")
            selection_refs = _render_selection_bar(scope="fs", visible_keys=visible_keys)

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
                                remove_button.on("click.stop", lambda p=fav_path: (_toggle_favorite(state, p), render_fn()))
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
                        render_tile(path, path.is_dir(), grid_class, selection_refs)
            elif state.explorer_view == "Список":
                with ui.column().classes("rag-explorer-list w-full"):
                    for path in [*dirs, *page_files]:
                        render_row(path, path.is_dir(), selection_refs, compact=True)
            else:
                with ui.column().classes("w-full gap-2"):
                    for path in [*dirs, *page_files]:
                        render_row(path, path.is_dir(), selection_refs, compact=False)

            if total_files > PAGE_SIZE:
                with ui.row().classes("items-center gap-2"):
                    ui.button("Назад", on_click=lambda: (setattr(state, "explorer_page", max(0, state.explorer_page - 1)), render_entries())).props("outline")
                    ui.label(f"Страница {state.explorer_page + 1} из {(total_files + PAGE_SIZE - 1) // PAGE_SIZE}").classes("rag-meta")
                    ui.button("Вперед", on_click=lambda: (setattr(state, "explorer_page", state.explorer_page + 1), render_entries())).props("outline")

    with tree_area:
        _tree_filter = [""]
        tree_filter_input = ui.input(placeholder="Фильтр по дереву").props("dense outlined clearable").classes("w-full")
        tree_content = ui.column().classes("w-full gap-0")

        def _find_matching_folders(needle: str, max_results: int = 20) -> List[Path]:
            results: List[Path] = []
            queue = [(root, 0)]
            while queue and len(results) < max_results:
                cur, depth = queue.pop(0)
                if depth > 6:
                    continue
                try:
                    children = sorted(
                        [p for p in cur.iterdir() if p.is_dir() and not p.name.startswith(".")],
                        key=lambda p: p.name.lower(),
                    )
                except Exception:
                    children = []
                for child in children:
                    if needle.lower() in child.name.lower():
                        results.append(child)
                    queue.append((child, depth + 1))
            return results

        def _render_tree_content(needle: str = "") -> None:
            tree_content.clear()
            with tree_content:
                ui.label("ИЗБРАННОЕ").classes("rag-section-label")
                if state.favorites:
                    for fav in state.favorites[:5]:
                        fav_path = Path(str(fav.get("path") or ""))
                        ui.button(
                            str(fav.get("title") or fav_path.name or fav_path),
                            icon="folder" if str(fav.get("item_type") or "") == "folder" else "description",
                            on_click=lambda p=fav_path: go_explorer_fn(str(p)),
                            color=None,
                        ).props(f"flat align=left no-caps dense {_overflow_title_prop(fav.get('path') or fav_path)}").classes("rag-nav-button rag-tree-button w-full")
                else:
                    ui.label("Нет закреплённых элементов").classes("rag-meta")
                ui.label("ДЕРЕВО").classes("rag-section-label")
                if needle:
                    matches = _find_matching_folders(needle)
                    if matches:
                        for m in matches:
                            ui.button(
                                m.name,
                                icon="folder",
                                on_click=lambda p=m: open_folder(p),
                                color=None,
                            ).props(f"flat align=left no-caps dense {_overflow_title_prop(m)}").classes("rag-nav-button rag-tree-button w-full")
                    else:
                        ui.label("Совпадений нет").classes("rag-meta text-xs px-2")
                else:
                    current_tree_path = _safe_explorer_path(state)
                    current_ancestors = {str(part) for part in _explorer_path_parts(root, current_tree_path)}
                    render_tree_node(root, 0, current_tree_path, current_ancestors)

        def _on_tree_filter_change(e: events.ValueChangeEventArguments) -> None:
            _tree_filter[0] = str(e.value or "").strip()
            _render_tree_content(_tree_filter[0])

        tree_filter_input.on_value_change(_on_tree_filter_change)
        _render_tree_content()

    render_explorer_details()

    with toolbar:
        current_for_toolbar = _safe_explorer_path(state)
        with ui.row().classes("rag-card w-full p-2 gap-2 items-center"):
            up_btn = ui.button(icon="arrow_upward", on_click=lambda: open_folder(current_for_toolbar.parent), color=None).props("flat round dense").tooltip("На уровень вверх")
            if current_for_toolbar == root:
                up_btn.disable()
            ui.button(icon="refresh", on_click=lambda: render_fn(), color=None).props("flat round dense").tooltip("Обновить")
            render_star(current_for_toolbar, item_type="folder")
        with ui.row().classes("rag-card w-full p-2 gap-2 items-center"):
            ui.icon("search").classes("text-lg")
            _folder_search_input = ui.input(placeholder="Семантический поиск только в этой папке").props("borderless dense").classes("flex-1")

            async def _run_folder_search(_: events.GenericEventArguments | None = None) -> None:
                q = str(_folder_search_input.value or "").strip()
                if q:
                    await choose_query_fn(f"{q} path:{current_for_toolbar}")

            _folder_search_input.on("keyup.enter", _run_folder_search)
            ui.button(icon="search", on_click=_run_folder_search, color=None).props("flat round dense")
            _folder_ai_cb = ui.checkbox("AI", value=bool(state.ai_search_expand)).props("dense").classes("rag-ai-expand")
            _folder_ai_cb.tooltip("AI-дополнение запроса")
            if not bool(state.cfg.get("llm_enabled")):
                _folder_ai_cb.disable()
            _folder_ai_cb.on_value_change(lambda e: (setattr(state, "ai_search_expand", bool(e.value)), _save_ui_settings(state)))
        with ui.row().classes("rag-card w-full p-3 gap-3 items-center"):
            filter_input = ui.input(placeholder="Фильтр по имени", value=state.explorer_filter).props("dense outlined clearable debounce=0").classes("min-w-64 flex-1")

            def update_explorer_setting(attr: str, value: Any) -> None:
                setattr(state, attr, value)
                state.explorer_page = 0
                state.explorer_visible_count = _EXPLORER_PAGE_SIZE
                _save_explorer_settings(state)
                _log_app_event(state, "explorer", "change_setting", details={attr: value})
                render_fn()

            ui.select(["Все", ".docx", ".xlsx", ".xls", ".pdf"], value=state.explorer_ext, on_change=lambda e: update_explorer_setting("explorer_ext", e.value)).props("dense outlined").classes("w-36")
            ui.select(["Крупные значки", "Средние значки", "Мелкие значки", "Список", "Таблица"], value=state.explorer_view, on_change=lambda e: update_explorer_setting("explorer_view", e.value)).props("dense outlined").classes("w-44")
            ui.select(["По имени", "По размеру", "По дате"], value=state.explorer_sort, on_change=lambda e: update_explorer_setting("explorer_sort", e.value)).props("dense outlined").classes("w-40")
            ui.select(["По возрастанию", "По убыванию"], value="По убыванию" if state.explorer_desc else "По возрастанию", on_change=lambda e: update_explorer_setting("explorer_desc", e.value == "По убыванию")).props("dense outlined").classes("w-44")

            def apply_filter(event: events.ValueChangeEventArguments | events.GenericEventArguments | None = None) -> None:
                _apply_explorer_filter_input(state, event, filter_input.value)
                render_fn()

            filter_input.on_value_change(apply_filter)

    render_entries()

# ── Index / indexing management screen ────────────────────────────────────

