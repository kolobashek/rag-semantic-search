"""
explorer_view.py — Explorer / Cloud Drive browser screen renderer.

Depends on: .state, .helpers, .system, nicegui, rag_catalog.core.
Imported by: nice_app.py.
"""

from __future__ import annotations

import html
import json
import time
from pathlib import Path
from typing import Any, Callable, List, Optional
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

    def _selected_set() -> set[str]:
        return set(getattr(state, "explorer_selected_paths", []) or [])

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

    def _render_selection_bar(*, scope: str, visible_keys: List[str]) -> dict[str, Any]:
        refs: dict[str, Any] = {
            "scope": scope,
            "visible_keys": list(visible_keys),
            "checkboxes": {},
        }
        refs["bar"] = ui.row().classes("rag-selection-bar w-full items-center gap-2")
        with refs["bar"]:
            ui.icon("checklist", size="18px")
            refs["label"] = ui.label("").classes("font-semibold")
            ui.button(
                "Снять",
                icon="close",
                on_click=lambda: (_clear_selection(), _set_visible_checkboxes(refs, False), _refresh_selection_bar(refs)),
                color=None,
            ).props("flat dense no-caps")
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

    def _selection_checkbox(key: str, refs: dict[str, Any]) -> None:
        checkbox = ui.checkbox(
            value=key in _selected_set(),
            on_change=lambda e, k=key: (_set_key_selected(k, bool(e.value)), _refresh_selection_bar(refs)),
        ).props("dense").classes("rag-select-checkbox")
        refs.setdefault("checkboxes", {})[key] = checkbox

    # ── Explorer / Cloud Drive screen ─────────────────────────────────────────

    def _render_cd_explorer(page_state: PageState, svc: "CloudDriveService") -> None:  # noqa: PLR0912,PLR0915
        """Registry-backed Cloud Drive explorer screen."""
        from rag_catalog.core.cloud_drive.models import CloudDriveFile, CloudDriveFolder  # noqa: PLC0415

        def _cd_can(path: str, level: str = "viewer") -> bool:
            return svc.user_can_access(
                username=_username(page_state),
                role=str((page_state.current_user or {}).get("role") or ""),
                path=path,
                required_level=level,
            )

        def _cd_open_folder(cd_path: str) -> None:
            page_state.explorer_cd_path = cd_path
            page_state.explorer_page = 0
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
                            ).props("flat align=left no-caps dense").classes(
                                "rag-nav-button rag-tree-button rag-tree-label"
                                + (" active" if is_current else "")
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

                def _apply_mobile_filters() -> None:
                    page_state.explorer_view = str(view_select.value or "Таблица")
                    page_state.explorer_sort = str(sort_select.value or "По имени")
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

        async def _cd_new_folder_dialog() -> None:
            """Show an inline dialog to create a new folder in the current directory."""
            with ui.dialog() as dlg, ui.card().classes("p-4 gap-3 w-80"):
                ui.label("Новая папка").classes("text-lg font-semibold")
                parent_label = page_state.explorer_cd_path or "/"
                ui.label(f"В: {parent_label}").classes("rag-path text-xs")
                name_input = ui.input(
                    "Имя папки",
                    placeholder="Введите имя",
                ).props("dense outlined autofocus").classes("w-full")

                async def _do_create() -> None:
                    if not _cd_can(page_state.explorer_cd_path or "", "editor"):
                        ui.notify("Нет прав на создание папки здесь.", type="negative")
                        return
                    name = str(name_input.value or "").strip()
                    if not name:
                        ui.notify("Введите имя папки.", type="warning")
                        return
                    try:
                        await run.io_bound(
                            svc.create_folder,
                            parent_path=page_state.explorer_cd_path or "",
                            name=name,
                        )
                        dlg.close()
                        _log_app_event(
                            page_state, "cd_explorer", "create_folder",
                            details={"parent": page_state.explorer_cd_path, "name": name},
                        )
                        ui.notify(f"Папка «{name}» создана.", type="positive")
                        render_fn()
                    except Exception as exc:
                        ui.notify(f"Не удалось создать папку: {exc}", type="negative")

                name_input.on("keydown.enter", lambda _: _do_create())
                with ui.row().classes("w-full justify-end gap-2 mt-1"):
                    ui.button("Отмена", on_click=dlg.close).props("flat dense")
                    ui.button("Создать", icon="create_new_folder", on_click=_do_create).props("unelevated dense")
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

        # Keep global header for product navigation only; explorer has local breadcrumbs.
        if page_state.header_breadcrumbs is not None:
            page_state.header_breadcrumbs.clear()
        if page_state.header_explorer_actions is not None:
            page_state.header_explorer_actions.clear()

        # filter & sort
        name_q = page_state.explorer_filter.strip().lower()
        ext_q = page_state.explorer_ext if page_state.explorer_ext != "Все" else ""
        if name_q:
            child_folders = [f for f in child_folders if name_q in f.name.lower()]
            child_files   = [f for f in child_files   if name_q in f.name.lower()]
        if ext_q:
            child_files = [f for f in child_files if f.name.lower().endswith(ext_q.lower())]

        sort_key = page_state.explorer_sort
        rev = page_state.explorer_desc
        if sort_key == "По имени":
            child_folders.sort(key=lambda x: x.name.lower(), reverse=rev)
            child_files.sort(key=lambda x: x.name.lower(), reverse=rev)
        elif sort_key == "По размеру":
            child_files.sort(key=lambda x: x.size_bytes, reverse=rev)
        elif sort_key == "По дате":
            child_files.sort(key=lambda x: x.updated_at, reverse=rev)

        # pagination of files
        total_files = len(child_files)
        page_size = PAGE_SIZE
        page_state.explorer_page = max(0, min(page_state.explorer_page, max(0, (total_files - 1) // page_size)))
        page_files = child_files[page_state.explorer_page * page_size : (page_state.explorer_page + 1) * page_size]

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

        # ── Explorer command bar (hi-fi top area) ─────────────────────────
        parent_path = breadcrumbs[-2].path if len(breadcrumbs) >= 2 else ""
        can_go_up = bool(cd_path and root_folder is not None and cd_path != root_folder.path)

        with ui.column().classes("rag-explorer-commandbar w-full gap-2"):
            with ui.row().classes("rag-explorer-topline"):
                up_btn = ui.button(
                    icon="arrow_upward",
                    on_click=lambda: _cd_open_folder(parent_path),
                    color=None,
                ).props("flat round dense").classes("rag-explorer-iconbtn")
                if not can_go_up:
                    up_btn.disable()
                ui.button(icon="refresh", on_click=lambda: render_fn(), color=None).props("flat round dense").classes("rag-explorer-iconbtn").tooltip("Обновить")
                with ui.row().classes("rag-explorer-pathbar rag-breadcrumbs items-center gap-1 no-wrap"):
                    ui.icon("folder", size="16px").classes("text-slate-400")
                    for idx, folder in enumerate(breadcrumbs):
                        label = "Корень" if folder.is_root else folder.name
                        ui.button(
                            label,
                            on_click=lambda p=folder.path: _cd_open_folder(p),
                            color=None,
                        ).props("flat dense no-caps").tooltip(folder.path or "Корень")
                        if idx < len(breadcrumbs) - 1:
                            ui.icon("chevron_right").classes("text-slate-500")
                folder_search = ui.input(
                    placeholder="семантический поиск в этой папке",
                ).props("dense borderless clearable").classes("rag-explorer-folder-search")

                async def _search_current_folder() -> None:
                    query = str(folder_search.value or "").strip()
                    if not query:
                        return
                    await choose_query_fn(f"{query} path:{cd_path}" if cd_path else query)

                folder_search.on("keydown.enter", _search_current_folder)

            with ui.row().classes("rag-explorer-actionline"):
                ui.button("Дерево", icon="account_tree", on_click=_cd_open_tree_dialog, color=None).props("outline dense no-caps").classes("rag-explorer-mobile-only")
                ui.button("Фильтры", icon="filter_alt", on_click=_cd_open_filters_dialog, color=None).props("outline dense no-caps").classes("rag-explorer-mobile-only")
                ui.button("Загрузить", icon="upload", on_click=_cd_upload_dialog, color=None).props("outline dense no-caps")
                ui.button("Папка", icon="add", on_click=_cd_new_folder_dialog, color=None).props("flat dense no-caps")
                ui.separator().props("vertical")
                ui.button(f"тип: {page_state.explorer_ext.lower()}", color=None).props("flat dense no-caps").classes("rag-filter-top-action")
                ui.button("изменён: любой", color=None).props("flat dense no-caps").classes("rag-filter-top-action")
                ui.button("размер: любой", color=None).props("flat dense no-caps").classes("rag-filter-top-action")
                ui.button("фильтр", icon="add", color=None).props("flat dense no-caps").classes("rag-filter-top-action")
                ui.space()
                ui.label("Сорт:").classes("rag-explorer-sort-label")
                ui.select(
                    ["По имени", "По размеру", "По дате"],
                    value=page_state.explorer_sort,
                    on_change=lambda e: (setattr(page_state, "explorer_sort", e.value), render_fn()),
                ).props("dense outlined").classes("w-36")
                ui.select(
                    ["Таблица", "Список"],
                    value=page_state.explorer_view if page_state.explorer_view in ("Таблица", "Список") else "Таблица",
                    on_change=lambda e: (setattr(page_state, "explorer_view", e.value), render_fn()),
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
                    children = svc.registry.list_child_folders(folder.id)
                    has_children = bool(children)
                    is_open = folder.path in open_paths
                    icon = "folder_open" if is_open or is_current else "folder"
                    label = "Корень" if folder.is_root else folder.name
                    row_classes = (
                        "rag-tree-row"
                        + (" active" if is_current else "")
                        + (" ancestor" if is_ancestor else "")
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
                            + (" active" if is_current else "")
                            + (" ancestor" if is_ancestor else "")
                        )
                        btn.tooltip(folder.path or "Корень")
                    if has_children and is_open:
                        for child in children:
                            _render_tree_node_cd(child, depth + 1)

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
        with details_col:
            ui.label("Свойства").classes("font-semibold text-sm")
            if breadcrumbs:
                current_node = breadcrumbs[-1]
                ui.label(current_node.name or "Корень").classes("font-semibold truncate")
                ui.label(current_node.path or "/").classes("rag-path text-xs")
            else:
                ui.label("Корень").classes("font-semibold")
            total_size = sum(f.size_bytes for f in child_files)
            ui.label(f"Папок: {len(child_folders)}").classes("rag-meta text-xs")
            ui.label(f"Файлов: {total_files}").classes("rag-meta text-xs")
            if total_size:
                ui.label(f"Размер: {_cd_file_size(total_size)}").classes("rag-meta text-xs")
            ui.separator()
            ui.label("Действия").classes("font-semibold text-sm")
            ui.button(
                "Новая папка", icon="create_new_folder",
                on_click=_cd_new_folder_dialog, color=None,
            ).props("flat dense no-caps align=left").classes("w-full")
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

        # ── Main column ───────────────────────────────────────────────────
        with main_col:
            visible_keys = [
                *[_selection_key("cd", folder.path) for folder in child_folders],
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
            with ui.row().classes("w-full items-center gap-2 px-1"):
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
                    if child_folders:
                        with ui.column().classes("rag-explorer-list w-full"):
                            for folder in child_folders:
                                item_key = _selection_key("cd", folder.path)
                                with ui.row().classes(
                                    "rag-explorer-item w-full p-2 items-center gap-3"
                                    + (" selected" if item_key in _selected_set() else "")
                                ):
                                    _selection_checkbox(item_key, selection_refs)
                                    ui.html(_file_badge_html(folder.name, "Каталог"), sanitize=False)
                                    with ui.column().classes("flex-1 gap-0"):
                                        ui.button(
                                            folder.name,
                                            on_click=lambda p=folder.path: _cd_open_folder(p),
                                            color=None,
                                        ).props("flat align=left no-caps dense").classes("rag-nav-button w-full")
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
                                with ui.row().classes(
                                    "rag-explorer-item w-full p-2 items-center gap-3"
                                    + (" selected" if item_key in _selected_set() else "")
                                ):
                                    _selection_checkbox(item_key, selection_refs)
                                    ui.html(_file_badge_html(f.name), sanitize=False)
                                    with ui.column().classes("flex-1 gap-0"):
                                        ui.button(
                                            f.name,
                                            on_click=lambda fi=f: _cd_open_file(fi),
                                            color=None,
                                        ).props("flat align=left no-caps dense").classes("rag-nav-button w-full")
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
                                            on_click=lambda url=_cd_download_url(f.path): ui.navigate.to(url, new_tab=True),
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
                            _selection_page_checkbox(selection_refs)
                            ui.element("div")
                            ui.label("Имя").classes("rag-col-header")
                            ui.label("Изменён").classes("rag-col-header")
                            ui.label("Размер").classes("rag-col-header")
                            ui.label("Автор").classes("rag-col-header")
                            ui.label("Индекс").classes("rag-col-header")
                            ui.element("div")
                        for folder in child_folders:
                            item_key = _selection_key("cd", folder.path)
                            with ui.element("div").classes("rag-file-table-row" + (" selected" if item_key in _selected_set() else "")):
                                _selection_checkbox(item_key, selection_refs)
                                ui.html(_file_badge_html(folder.name, "Каталог"), sanitize=False)
                                with ui.element("div").classes("rag-file-table-name min-w-0"):
                                    ui.button(
                                        folder.name,
                                        on_click=lambda p=folder.path: _cd_open_folder(p),
                                        color=None,
                                    ).props("flat align=left no-caps dense").classes("rag-nav-button w-full")
                                ui.label("—").classes("rag-meta text-xs")
                                ui.label("папка").classes("rag-meta text-xs")
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
                        for f in page_files:
                            item_key = _selection_key("cd", f.path)
                            with ui.element("div").classes("rag-file-table-row" + (" selected" if item_key in _selected_set() else "")):
                                _selection_checkbox(item_key, selection_refs)
                                ui.html(_file_badge_html(f.name), sanitize=False)
                                with ui.element("div").classes("rag-file-table-name min-w-0"):
                                    ui.button(
                                        f.name,
                                        on_click=lambda fi=f: _cd_open_file(fi),
                                        color=None,
                                    ).props("flat align=left no-caps dense").classes("rag-nav-button w-full")
                                ui.label(f.updated_at[:10] if f.updated_at else "—").classes("rag-meta text-xs")
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
                                            on_click=lambda url=_cd_download_url(f.path): ui.navigate.to(url, new_tab=True),
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

                # Pagination
                if total_files > page_size:
                    with ui.row().classes("items-center gap-2 mt-2"):
                        ui.button("Назад", on_click=lambda: (setattr(page_state, "explorer_page", max(0, page_state.explorer_page - 1)), render_fn())).props("outline")
                        ui.label(f"Стр. {page_state.explorer_page + 1} / {(total_files + page_size - 1) // page_size}").classes("rag-meta")
                        ui.button("Вперёд", on_click=lambda: (setattr(page_state, "explorer_page", page_state.explorer_page + 1), render_fn())).props("outline")

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

    def render_breadcrumbs(root_path: Path, current_path: Path) -> None:
        parts = _explorer_path_parts(root_path, current_path)
        with ui.row().classes("rag-breadcrumbs flex-1 min-w-0 items-center gap-1 no-wrap"):
            for idx, part in enumerate(parts):
                label = "Обмен" if part == root_path else part.name
                btn = ui.button(
                    label,
                    on_click=lambda p=part: (_log_app_event(state, "explorer", "breadcrumb", details={"path": str(p)}), open_folder(p)),
                    color=None,
                ).props("flat dense no-caps")
                btn.tooltip(str(part))
                if idx < len(parts) - 1:
                    ui.icon("chevron_right").classes("text-slate-400")

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
        btn = ui.button(
            label,
            icon=icon,
            on_click=lambda p=path: open_folder(p),
            color=None,
        ).props("flat align=left no-caps dense").classes(" ".join(class_bits)).style(f"padding-left: {depth * 12}px")
        btn.tooltip(str(path))
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

        # Keep current folder controls inside the explorer toolbar. Duplicating them
        # in the global header makes the path ambiguous on narrow screens.
        if state.header_breadcrumbs is not None:
            state.header_breadcrumbs.clear()
        if state.header_explorer_actions is not None:
            state.header_explorer_actions.clear()

        dirs, files, total_files = _file_rows(current, state)
        state.explorer_page = max(0, min(state.explorer_page, max(0, (len(files) - 1) // PAGE_SIZE)))
        page_files = files[state.explorer_page * PAGE_SIZE : (state.explorer_page + 1) * PAGE_SIZE]
        visible_keys = [_selection_key("fs", str(path)) for path in [*dirs, *page_files]]

        with entries_area:
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
                        ).props("flat align=left no-caps dense").classes("rag-nav-button rag-tree-button w-full")
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
                            ).props("flat align=left no-caps dense").classes("rag-nav-button rag-tree-button w-full").tooltip(str(m))
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
            render_breadcrumbs(root, current_for_toolbar)
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

