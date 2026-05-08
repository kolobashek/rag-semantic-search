"""
settings_view.py — Settings screen renderer (user profile + admin panels).

Depends on: .state, .helpers, .system, nicegui, rag_catalog.core.
Imported by: nice_app.py.
"""

from __future__ import annotations

import datetime
import json
import re
import threading
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from nicegui import app, run, ui

from rag_catalog.core.cloud_drive import CloudDriveService
from rag_catalog.core.rag_core import load_config, save_config
from rag_catalog.core.user_auth_db import UserAuthDB

from .helpers import (
    _cd_get_service,
    _telegram_deeplink,
)
from .state import (
    CONFIG_PATH_KEYS,
    PageState,
    _get_auth_db,
    _get_telemetry,
    _log_app_event,
    _refresh_current_user,
    _save_config_patch,
    _toggle_favorite,
    _toggle_saved_search,
    _username,
)
from .system import (
    _read_cloud_bootstrap_status,
    _safe_int,
    _stop_managed_timer,
)


def _pick_folder_dialog(input_widget: Any, *, title: str = "Выберите папку") -> None:
    """Open OS native folder-picker dialog (local server only) and populate input_widget."""
    import threading as _threading  # noqa: PLC0415
    def _run() -> None:
        try:
            import tkinter as _tk  # noqa: PLC0415
            from tkinter import filedialog as _fd  # noqa: PLC0415
            root = _tk.Tk()
            root.withdraw()
            root.attributes("-topmost", True)
            chosen = _fd.askdirectory(title=title, parent=root)
            root.destroy()
            if chosen:
                input_widget.set_value(chosen)
                input_widget.run_method("focus")
        except Exception:
            pass
    _threading.Thread(target=_run, daemon=True).start()


def _pick_file_dialog(input_widget: Any, *, title: str = "Выберите файл") -> None:
    """Open OS native file-picker dialog (local server only) and populate input_widget."""
    import threading as _threading  # noqa: PLC0415
    def _run() -> None:
        try:
            import tkinter as _tk  # noqa: PLC0415
            from tkinter import filedialog as _fd  # noqa: PLC0415
            root = _tk.Tk()
            root.withdraw()
            root.attributes("-topmost", True)
            chosen = _fd.askopenfilename(title=title, parent=root)
            root.destroy()
            if chosen:
                input_widget.set_value(chosen)
                input_widget.run_method("focus")
        except Exception:
            pass
    _threading.Thread(target=_run, daemon=True).start()


def render_settings_screen(
    state: PageState,
    *,
    render_fn: Callable,
    query_handler: Callable,
) -> None:

    def render_admin_users(auth_db: UserAuthDB) -> None:
        with ui.column().classes("rag-card w-full p-4 gap-4"):
            ui.label("Админ-панель пользователей").classes("text-xl font-semibold")
            with ui.expansion("Создать пользователя", icon="person_add").classes("w-full"):
                new_username = ui.input("Логин").props("dense outlined").classes("w-full")
                new_display = ui.input("Имя").props("dense outlined").classes("w-full")
                new_telegram = ui.input("Telegram chat id").props("dense outlined").classes("w-full")
                new_telegram_username = ui.input("Telegram username").props("dense outlined prefix=@").classes("w-full")
                new_password = ui.input("Временный пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")
                new_role = ui.select(["user", "admin"], value="user", label="Роль").props("dense outlined").classes("w-full")
                new_status = ui.select(["active", "pending", "blocked"], value="active", label="Статус").props("dense outlined").classes("w-full")
                new_must_change = ui.checkbox("Потребовать смену пароля", value=True)

                def create_user() -> None:
                    ok = auth_db.admin_create_user(
                        username=str(new_username.value or ""),
                        display_name=str(new_display.value or ""),
                        telegram_chat_id=str(new_telegram.value or ""),
                        telegram_username=str(new_telegram_username.value or ""),
                        password=str(new_password.value or ""),
                        role=str(new_role.value or "user"),
                        status=str(new_status.value or "active"),
                        must_change_password=bool(new_must_change.value),
                    )
                    ui.notify("Пользователь создан." if ok else "Не удалось создать пользователя.", type="positive" if ok else "negative")
                    render_fn()

                ui.button("Создать", icon="person_add", on_click=create_user).props("unelevated")

            users = auth_db.list_users()
            for user in users:
                username = str(user.get("username") or "")
                role = str(user.get("role") or "user")
                status = str(user.get("status") or "")
                with ui.expansion(f"{username} · {role} · {status}", icon="person").classes("w-full"):
                    initial_user = {
                        "display_name": str(user.get("display_name") or ""),
                        "telegram_chat_id": str(user.get("telegram_chat_id") or ""),
                        "telegram_username": str(user.get("telegram_username") or ""),
                        "role": role,
                        "status": status or "active",
                        "must_change_password": bool(int(user.get("must_change_password") or 0)),
                    }
                    display_input = ui.input("Имя", value=str(user.get("display_name") or "")).props("dense outlined").classes("w-full")
                    telegram_input = ui.input("Telegram chat id", value=str(user.get("telegram_chat_id") or "")).props("dense outlined").classes("w-full")
                    telegram_username_input = ui.input("Telegram username", value=str(user.get("telegram_username") or "")).props("dense outlined prefix=@").classes("w-full")
                    role_input = ui.select(["user", "admin"], value=role, label="Роль").props("dense outlined").classes("w-full")
                    status_input = ui.select(["active", "pending", "blocked"], value=status or "active", label="Статус").props("dense outlined").classes("w-full")
                    must_input = ui.checkbox("Потребовать смену пароля", value=bool(int(user.get("must_change_password") or 0)))
                    reset_password = ui.input("Новый временный пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")

                    def save_user(
                        username: str = username,
                        display_input: Any = display_input,
                        telegram_input: Any = telegram_input,
                        telegram_username_input: Any = telegram_username_input,
                        role_input: Any = role_input,
                        status_input: Any = status_input,
                        must_input: Any = must_input,
                    ) -> None:
                        ok = auth_db.admin_update_user(
                            username=username,
                            display_name=str(display_input.value or ""),
                            telegram_chat_id=str(telegram_input.value or ""),
                            telegram_username=str(telegram_username_input.value or ""),
                            role=str(role_input.value or "user"),
                            status=str(status_input.value or "active"),
                            must_change_password=bool(must_input.value),
                        )
                        initial_user.update({
                            "display_name": str(display_input.value or ""),
                            "telegram_chat_id": str(telegram_input.value or ""),
                            "telegram_username": str(telegram_username_input.value or ""),
                            "role": str(role_input.value or "user"),
                            "status": str(status_input.value or "active"),
                            "must_change_password": bool(must_input.value),
                        })
                        user_actions.set_visibility(False)
                        ui.notify("Пользователь обновлен." if ok else "Не удалось обновить пользователя.", type="positive" if ok else "negative")
                        _refresh_current_user(state)
                        render_fn()

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
                        render_fn()

                    user_actions = ui.row().classes("rag-dirty-actions")
                    user_actions.set_visibility(False)

                    def current_user_values() -> Dict[str, Any]:
                        return {
                            "display_name": str(display_input.value or ""),
                            "telegram_chat_id": str(telegram_input.value or ""),
                            "telegram_username": str(telegram_username_input.value or ""),
                            "role": str(role_input.value or "user"),
                            "status": str(status_input.value or "active"),
                            "must_change_password": bool(must_input.value),
                        }

                    def refresh_user_dirty() -> None:
                        user_actions.set_visibility(current_user_values() != initial_user)

                    def reset_user_fields() -> None:
                        display_input.set_value(initial_user["display_name"])
                        telegram_input.set_value(initial_user["telegram_chat_id"])
                        telegram_username_input.set_value(initial_user["telegram_username"])
                        role_input.set_value(initial_user["role"])
                        status_input.set_value(initial_user["status"])
                        must_input.set_value(initial_user["must_change_password"])
                        user_actions.set_visibility(False)

                    display_input.on_value_change(lambda _: refresh_user_dirty())
                    telegram_input.on_value_change(lambda _: refresh_user_dirty())
                    telegram_username_input.on_value_change(lambda _: refresh_user_dirty())
                    role_input.on_value_change(lambda _: refresh_user_dirty())
                    status_input.on_value_change(lambda _: refresh_user_dirty())
                    must_input.on_value_change(lambda _: refresh_user_dirty())

                    with ui.row().classes("gap-2"):
                        ui.button("Сбросить пароль", icon="key", on_click=set_password).props("outline")
                        def make_invite(
                            username: str = username,
                            display_input: Any = display_input,
                            telegram_username_input: Any = telegram_username_input,
                        ) -> None:
                            bot_link = str(state.cfg.get("telegram_bot_link") or "").strip()
                            if not bot_link:
                                ui.notify("В config.json не задан telegram_bot_link.", type="warning")
                                return
                            out = auth_db.create_telegram_token(
                                purpose="invite",
                                username=username,
                                display_name=str(display_input.value or ""),
                                telegram_username=str(telegram_username_input.value or ""),
                                created_by=_username(state),
                                ttl_minutes=7 * 24 * 60,
                            )
                            link = _telegram_deeplink(bot_link, "invite", str(out.get("token") or ""))
                            ui.notify(f"Invite-link: {link}", type="positive", timeout=12000)

                        ui.button("Invite Telegram", icon="link", on_click=make_invite).props("outline")
                    with user_actions:
                        with ui.row().classes("rag-dirty-actions-inner"):
                            ui.button("Отменить", icon="close", on_click=reset_user_fields).props("flat dense")
                            ui.button("Сохранить", icon="save", on_click=save_user).props("outline dense")

    def render_admin_telegram_chats(auth_db: UserAuthDB) -> None:
        rows = auth_db.list_telegram_chats()
        with ui.column().classes("rag-card w-full p-4 gap-3"):
            ui.label("Telegram чаты").classes("text-xl font-semibold")
            if not rows:
                ui.label("Привязанных Telegram chat_id пока нет.").classes("rag-meta")
                return
            ui.table(
                rows=rows,
                columns=[
                    {"name": "username", "label": "Пользователь", "field": "username"},
                    {"name": "display_name", "label": "Имя", "field": "display_name"},
                    {"name": "role", "label": "Роль", "field": "role"},
                    {"name": "status", "label": "Статус", "field": "status"},
                    {"name": "telegram_chat_id", "label": "Chat ID", "field": "telegram_chat_id"},
                    {"name": "last_telegram_event_at", "label": "Последнее Telegram-событие", "field": "last_telegram_event_at"},
                    {"name": "last_login_at", "label": "Последний web-вход", "field": "last_login_at"},
                ],
                pagination=10,
            ).classes("w-full")

    def render_admin_registration_requests(auth_db: UserAuthDB) -> None:
        rows = auth_db.list_registration_requests(status="pending", limit=50)
        with ui.column().classes("rag-card w-full p-4 gap-3"):
            ui.label("Заявки на регистрацию").classes("text-xl font-semibold")
            if not rows:
                ui.label("Ожидающих заявок нет.").classes("rag-meta")
                return
            for row in rows:
                req_id = int(row.get("id") or 0)
                title = str(row.get("username") or row.get("display_name") or f"заявка {req_id}")
                tg = str(row.get("telegram_username") or row.get("telegram_chat_id") or "")
                with ui.row().classes("w-full items-center gap-2"):
                    ui.label(f"#{req_id}").classes("rag-chip")
                    ui.label(title).classes("font-medium")
                    ui.label(f"Telegram: {tg or '-'}").classes("rag-meta flex-1")
                    ui.label(str(row.get("source") or "")).classes("rag-meta")

                    def approve(req_id: int = req_id) -> None:
                        out = auth_db.review_registration_request(
                            request_id=req_id,
                            reviewed_by=_username(state),
                            decision="approved",
                        )
                        ui.notify(
                            f"Заявка одобрена: {out.get('username')}" if out.get("ok") else f"Не удалось одобрить: {out.get('reason')}",
                            type="positive" if out.get("ok") else "negative",
                        )
                        render_fn()

                    def reject(req_id: int = req_id) -> None:
                        out = auth_db.review_registration_request(
                            request_id=req_id,
                            reviewed_by=_username(state),
                            decision="rejected",
                        )
                        ui.notify(
                            "Заявка отклонена." if out.get("ok") else f"Не удалось отклонить: {out.get('reason')}",
                            type="positive" if out.get("ok") else "negative",
                        )
                        render_fn()

                    ui.button("Одобрить", icon="check", on_click=approve).props("outline dense")
                    ui.button("Отклонить", icon="close", on_click=reject).props("flat dense")

    def render_admin_security_settings(auth_db: UserAuthDB) -> None:
        current_ttl = auth_db.get_session_ttl_days()
        current_show_system = auth_db.get_show_system_files_for_admin()
        all_users = auth_db.list_users()
        must_change_users = [u for u in all_users if int(u.get("must_change_password") or 0)]
        recent_events = auth_db.list_auth_events(limit=100)
        failed_logins = [e for e in recent_events if not int(e.get("ok") or 0) and str(e.get("event_type") or "") == "login_failed"]

        # Critical: default admin password still in use
        if auth_db.has_default_admin_password():
            with ui.row().classes("items-center gap-3 bg-red-50 dark:bg-red-950 border border-red-300 dark:border-red-700 rounded-lg p-4 w-full"):
                ui.icon("gpp_bad").classes("text-red-500 text-3xl flex-shrink-0")
                with ui.column().classes("flex-1 gap-1"):
                    ui.label("Критическая уязвимость: пароль admin не изменён").classes("font-semibold text-red-700 dark:text-red-300")
                    ui.label(
                        "Пользователь admin использует пароль по умолчанию «admin». "
                        "Смените пароль немедленно — любой может получить права администратора."
                    ).classes("text-red-600 dark:text-red-400 text-sm")
                ui.button(
                    "Сменить пароль", icon="key",
                    on_click=lambda: setattr(state, "settings_section", "profile") or render_fn(),
                ).props("outline dense color=negative")

        if must_change_users:
            with ui.row().classes("items-center gap-2 bg-orange-50 border border-orange-200 rounded p-3 w-full"):
                ui.icon("warning").classes("text-orange-500")
                with ui.column().classes("gap-0"):
                    ui.label(f"{len(must_change_users)} пользователей должны сменить пароль").classes("text-orange-700 text-sm font-medium")
                    ui.label(", ".join(str(u.get("username") or "") for u in must_change_users)).classes("text-orange-600 text-xs")

        with ui.row().classes("w-full gap-3"):
            with ui.column().classes("rag-card flex-1 p-3 gap-1 items-center"):
                ui.icon("group").classes("text-2xl text-primary")
                ui.label(str(len(all_users))).classes("text-xl font-semibold")
                ui.label("Пользователей").classes("rag-meta text-xs")
            with ui.column().classes("rag-card flex-1 p-3 gap-1 items-center"):
                count_color = "text-negative" if must_change_users else "text-positive"
                ui.icon("lock_reset").classes(f"text-2xl {count_color}")
                ui.label(str(len(must_change_users))).classes(f"text-xl font-semibold {count_color}")
                ui.label("Смена пароля").classes("rag-meta text-xs")
            with ui.column().classes("rag-card flex-1 p-3 gap-1 items-center"):
                fail_color = "text-negative" if failed_logins else "text-positive"
                ui.icon("no_accounts").classes(f"text-2xl {fail_color}")
                ui.label(str(len(failed_logins))).classes(f"text-xl font-semibold {fail_color}")
                ui.label("Неудачных входов").classes("rag-meta text-xs")

        with ui.column().classes("rag-card w-full p-4 gap-3"):
            initial_security = {
                "ttl": int(current_ttl),
                "show_system": bool(current_show_system),
            }
            ui.label("Безопасность").classes("text-xl font-semibold")
            ui.label("Максимальная длительность новой сессии пользователя. Допустимый диапазон: 1-7 дней.").classes("rag-meta")
            ttl_input = ui.number(
                "Срок сессии, дней",
                value=current_ttl,
                min=1,
                max=7,
                step=1,
            ).props("dense outlined").classes("w-full max-w-xs")
            show_system_input = ui.checkbox(
                "Показывать служебные файлы администратору",
                value=current_show_system,
            )
            ui.label("Обычные пользователи служебные файлы не видят независимо от этой настройки.").classes("rag-meta")
            action_row = ui.row().classes("rag-dirty-actions")
            action_row.set_visibility(False)

            def current_security() -> Dict[str, Any]:
                return {
                    "ttl": int(ttl_input.value or current_ttl),
                    "show_system": bool(show_system_input.value),
                }

            def refresh_security_dirty() -> None:
                action_row.set_visibility(current_security() != initial_security)

            def reset_security() -> None:
                ttl_input.set_value(initial_security["ttl"])
                show_system_input.set_value(initial_security["show_system"])
                action_row.set_visibility(False)

            def save_session_ttl() -> None:
                saved = auth_db.set_session_ttl_days(int(ttl_input.value or current_ttl))
                show_system = auth_db.set_show_system_files_for_admin(bool(show_system_input.value))
                initial_security.update({"ttl": int(saved), "show_system": bool(show_system)})
                action_row.set_visibility(False)
                _log_app_event(
                    state,
                    "settings",
                    "security",
                    details={"session_ttl_days": saved, "show_system_files_for_admin": show_system},
                )
                ui.notify(f"Сохранено: сессии {saved} дн., служебные файлы {'видны админу' if show_system else 'скрыты'}.", type="positive")
                render_fn()

            ttl_input.on_value_change(lambda _: refresh_security_dirty())
            show_system_input.on_value_change(lambda _: refresh_security_dirty())
            with action_row:
                with ui.row().classes("rag-dirty-actions-inner"):
                    ui.button("Отменить", icon="close", on_click=reset_security).props("flat dense")
                    ui.button("Сохранить настройки безопасности", icon="save", on_click=save_session_ttl).props("outline dense")

        # ── Auth event log ────────────────────────────────────────────────────
        with ui.column().classes("rag-card w-full p-4 gap-3"):
            ui.label("Журнал входов").classes("text-xl font-semibold")
            ui.label("Последние 100 событий авторизации: входы, выходы, смены пароля.").classes("rag-meta")
            if not recent_events:
                with ui.element("div").classes("cd-empty-state py-4"):
                    ui.icon("history", size="28px").classes("opacity-30")
                    ui.label("Событий пока нет.").classes("text-xs")
            else:
                _AUTH_ACTION_LABELS = {
                    "login": "Вход",
                    "login_failed": "Ошибка входа",
                    "logout": "Выход",
                    "session_restore": "Восстановление сессии",
                    "password_change": "Смена пароля",
                    "register": "Регистрация",
                }
                with ui.element("div").classes("w-full overflow-x-auto"):
                    with ui.element("table").classes("w-full text-xs border-collapse"):
                        with ui.element("thead"):
                            with ui.element("tr").classes("border-b rag-section-label"):
                                for col in ("Время", "Пользователь", "Событие", "IP / детали"):
                                    ui.element("th").classes("text-left p-2 font-semibold").text = col
                        with ui.element("tbody"):
                            for ev in recent_events[:100]:
                                ok_ev = bool(ev.get("ok", True))
                                row_cls = "border-b hover:bg-slate-50 dark:hover:bg-slate-800" + (" text-negative" if not ok_ev else "")
                                with ui.element("tr").classes(row_cls):
                                    ts = str(ev.get("ts") or "")[:19].replace("T", " ")
                                    ui.element("td").classes("p-2 font-mono whitespace-nowrap").text = ts
                                    ui.element("td").classes("p-2 font-medium").text = str(ev.get("username") or "—")
                                    action_lbl = _AUTH_ACTION_LABELS.get(str(ev.get("event_type") or ""), str(ev.get("event_type") or "—"))
                                    with ui.element("td").classes("p-2"):
                                        with ui.row().classes("items-center gap-1"):
                                            ui.icon("check_circle" if ok_ev else "cancel", size="14px").classes("text-positive" if ok_ev else "text-negative")
                                            ui.label(action_lbl)
                                    details = ev.get("details") or {}
                                    detail_text = str(details.get("ip") or details.get("error") or details.get("reason") or "")
                                    ui.element("td").classes("p-2 rag-meta truncate max-w-xs").text = detail_text

        # ── Cloud Drive audit log ─────────────────────────────────────────────
        telemetry = _get_telemetry(state)
        cd_audit_events = telemetry.list_app_events(feature="cloud_drive", limit=100)
        with ui.column().classes("rag-card w-full p-4 gap-3"):
            ui.label("Аудит Cloud Drive").classes("text-xl font-semibold")
            ui.label("Последние операции с файлами через API: загрузка, скачивание, перемещение, удаление.").classes("rag-meta")
            if not cd_audit_events:
                with ui.element("div").classes("cd-empty-state py-4"):
                    ui.icon("folder_off", size="28px").classes("opacity-30")
                    ui.label("Операций через API пока не было.").classes("text-xs")
            else:
                _CD_ACTION_LABELS = {
                    "upload": "Загрузка",
                    "download": "Скачивание",
                    "delete": "Удаление",
                    "move": "Перемещение",
                    "rename": "Переименование",
                    "create_folder": "Создание папки",
                    "list_directory": "Просмотр папки",
                    "view_node": "Просмотр",
                    "versions": "Версии",
                    "reindex": "Переиндексация",
                }
                _CD_ACTION_ICON = {
                    "upload": "upload", "download": "download", "delete": "delete",
                    "move": "drive_file_move", "rename": "edit", "create_folder": "create_new_folder",
                    "reindex": "refresh",
                }
                with ui.element("div").classes("w-full overflow-x-auto"):
                    with ui.element("table").classes("w-full text-xs border-collapse"):
                        with ui.element("thead"):
                            with ui.element("tr").classes("border-b rag-section-label"):
                                for col in ("Время", "Пользователь", "Действие", "Путь / детали"):
                                    ui.element("th").classes("text-left p-2 font-semibold").text = col
                        with ui.element("tbody"):
                            for ev in cd_audit_events[:100]:
                                ok_ev = bool(ev.get("ok", True))
                                row_cls = "border-b hover:bg-slate-50 dark:hover:bg-slate-800" + (" text-negative" if not ok_ev else "")
                                with ui.element("tr").classes(row_cls):
                                    ts = str(ev.get("ts") or "")[:19].replace("T", " ")
                                    ui.element("td").classes("p-2 font-mono whitespace-nowrap").text = ts
                                    ui.element("td").classes("p-2 font-medium").text = str(ev.get("username") or "—")
                                    action = str(ev.get("action") or "")
                                    action_lbl = _CD_ACTION_LABELS.get(action, action)
                                    icon_name = _CD_ACTION_ICON.get(action, "storage")
                                    with ui.element("td").classes("p-2"):
                                        with ui.row().classes("items-center gap-1"):
                                            ui.icon(icon_name, size="14px").classes("" if ok_ev else "text-negative")
                                            ui.label(action_lbl).classes("" if ok_ev else "text-negative")
                                    details = ev.get("details") or {}
                                    path = str(details.get("path") or details.get("filename") or details.get("name") or details.get("error") or "")
                                    ui.element("td").classes("p-2 rag-meta font-mono truncate max-w-xs").text = path

    def render_admin_path_settings() -> None:
        def _path_row(label: str, value: str, *, folder: bool = True) -> ui.input:
            with ui.row().classes("w-full items-center gap-1"):
                inp = ui.input(label, value=value).props("dense outlined").classes("flex-1")
                icon = "folder_open" if folder else "description"
                tip = "Выбрать папку" if folder else "Выбрать файл"
                btn = ui.button(icon=icon).props("flat dense round").classes("text-indigo-400 mt-1")
                btn.tooltip(tip)
                if folder:
                    btn.on_click(lambda _inp=inp: _pick_folder_dialog(_inp))
                else:
                    btn.on_click(lambda _inp=inp: _pick_file_dialog(_inp))
            return inp

        with ui.column().classes("rag-card w-full p-4 gap-3"):
            initial_paths = {
                "catalog_path": str(state.cfg.get("catalog_path") or "").strip(),
                "qdrant_url": str(state.cfg.get("qdrant_url") or "").strip(),
                "qdrant_db_path": str(state.cfg.get("qdrant_db_path") or "").strip(),
                "collection_name": str(state.cfg.get("collection_name") or "catalog").strip() or "catalog",
                "telemetry_db_path": str(state.cfg.get("telemetry_db_path") or "").strip(),
                "log_file": str(state.cfg.get("log_file") or "").strip(),
            }
            ui.label("Пути и подключение").classes("text-xl font-semibold")
            ui.label("Эти настройки видны только администратору. После сохранения поиск переподключается к Qdrant с новыми значениями.").classes("rag-meta")
            catalog_input = _path_row("Каталог документов", str(state.cfg.get("catalog_path") or ""), folder=True)
            qdrant_url_input = ui.input("Qdrant URL", value=str(state.cfg.get("qdrant_url") or "")).props("dense outlined").classes("w-full")
            qdrant_db_input = _path_row("Локальный путь Qdrant", str(state.cfg.get("qdrant_db_path") or ""), folder=True)
            collection_input = ui.input("Коллекция", value=str(state.cfg.get("collection_name") or "catalog")).props("dense outlined").classes("w-full")
            telemetry_input = _path_row("БД телеметрии", str(state.cfg.get("telemetry_db_path") or ""), folder=False)
            log_input = _path_row("Лог автоматизации", str(state.cfg.get("log_file") or ""), folder=False)

            with ui.row().classes("w-full gap-2"):
                ui.label(f"Текущий каталог: {state.cfg.get('catalog_path') or '-'}").classes("rag-path")
                ui.label(f"Текущий Qdrant: {state.cfg.get('qdrant_url') or state.cfg.get('qdrant_db_path') or '-'}").classes("rag-path")
            action_row = ui.row().classes("rag-dirty-actions")
            action_row.set_visibility(False)

            def current_paths() -> Dict[str, Any]:
                return {
                    "catalog_path": str(catalog_input.value or "").strip(),
                    "qdrant_url": str(qdrant_url_input.value or "").strip(),
                    "qdrant_db_path": str(qdrant_db_input.value or "").strip(),
                    "collection_name": str(collection_input.value or "catalog").strip() or "catalog",
                    "telemetry_db_path": str(telemetry_input.value or "").strip(),
                    "log_file": str(log_input.value or "").strip(),
                }

            def refresh_paths_dirty() -> None:
                action_row.set_visibility(current_paths() != initial_paths)

            def reset_paths() -> None:
                catalog_input.set_value(initial_paths["catalog_path"])
                qdrant_url_input.set_value(initial_paths["qdrant_url"])
                qdrant_db_input.set_value(initial_paths["qdrant_db_path"])
                collection_input.set_value(initial_paths["collection_name"])
                telemetry_input.set_value(initial_paths["telemetry_db_path"])
                log_input.set_value(initial_paths["log_file"])
                action_row.set_visibility(False)

            def save_paths() -> None:
                values = current_paths()
                new_catalog = values["catalog_path"]
                if new_catalog and not Path(new_catalog).exists():
                    ui.notify("Каталог документов не найден. Проверьте путь.", type="negative")
                    return
                new_qdrant_url = values["qdrant_url"]
                new_qdrant_db = values["qdrant_db_path"]
                if not new_qdrant_url and not new_qdrant_db:
                    ui.notify("Укажите Qdrant URL или локальный путь Qdrant.", type="warning")
                    return
                try:
                    state.cfg = _save_config_patch(values)
                    initial_paths.update(values)
                    action_row.set_visibility(False)
                    state.searcher = None
                    state.searcher_error = ""
                    state.telemetry = None
                    _log_app_event(state, "settings", "save_paths", details={key: state.cfg.get(key) for key in CONFIG_PATH_KEYS})
                    ui.notify("Пути сохранены.", type="positive")
                    render_fn()
                except Exception as exc:
                    ui.notify(f"Не удалось сохранить пути: {exc}", type="negative")

            catalog_input.on_value_change(lambda _: refresh_paths_dirty())
            qdrant_url_input.on_value_change(lambda _: refresh_paths_dirty())
            qdrant_db_input.on_value_change(lambda _: refresh_paths_dirty())
            collection_input.on_value_change(lambda _: refresh_paths_dirty())
            telemetry_input.on_value_change(lambda _: refresh_paths_dirty())
            log_input.on_value_change(lambda _: refresh_paths_dirty())
            with action_row:
                with ui.row().classes("rag-dirty-actions-inner"):
                    ui.button("Отменить", icon="close", on_click=reset_paths).props("flat dense")
                    ui.button("Сохранить пути", icon="save", on_click=save_paths).props("outline dense")

    def render_admin_cloud_drive_settings() -> None:
        default_db_path = str((Path(str(state.cfg.get("qdrant_db_path") or ".")) / "cloud_drive.db").resolve())
        default_storage_root = str((Path(str(state.cfg.get("qdrant_db_path") or ".")) / "cloud_storage").resolve())

        def _path_row_cd(label: str, value: str, *, folder: bool = True) -> ui.input:
            with ui.row().classes("w-full items-center gap-1"):
                inp = ui.input(label, value=value).props("dense outlined").classes("flex-1")
                icon = "folder_open" if folder else "description"
                btn = ui.button(icon=icon).props("flat dense round").classes("text-indigo-400 mt-1")
                btn.tooltip("Выбрать папку" if folder else "Выбрать файл")
                if folder:
                    btn.on_click(lambda _inp=inp: _pick_folder_dialog(_inp))
                else:
                    btn.on_click(lambda _inp=inp: _pick_file_dialog(_inp))
            return inp

        with ui.column().classes("rag-card w-full p-4 gap-3"):
            initial_cloud = {
                "cloud_drive_enabled": bool(state.cfg.get("cloud_drive_enabled")),
                "cloud_drive_db_path": str(state.cfg.get("cloud_drive_db_path") or default_db_path).strip(),
                "cloud_drive_storage": str(state.cfg.get("cloud_drive_storage") or "local").strip() or "local",
                "cloud_drive_storage_root": str(state.cfg.get("cloud_drive_storage_root") or default_storage_root).strip(),
                "cloud_drive_bucket": str(state.cfg.get("cloud_drive_bucket") or "").strip(),
                "cloud_drive_s3_endpoint": str(state.cfg.get("cloud_drive_s3_endpoint") or "").strip(),
                "cloud_drive_s3_region": str(state.cfg.get("cloud_drive_s3_region") or "").strip(),
                "cloud_drive_s3_access_key": str(state.cfg.get("cloud_drive_s3_access_key") or "").strip(),
                "cloud_drive_s3_secret_key": str(state.cfg.get("cloud_drive_s3_secret_key") or "").strip(),
                "catalog_path": str(state.cfg.get("catalog_path") or "").strip(),
                "cloud_drive_autosync_minutes": int(state.cfg.get("cloud_drive_autosync_minutes") or 0),
            }
            stats_ref: Dict[str, Any] = {"value": None}
            autosync_last_run: Dict[str, Any] = {"ts": None}

            ui.label("Cloud Drive").classes("text-xl font-semibold")
            ui.label(
                "Централизованный реестр файлов и папок: дерево каталогов, версии, фоновые задачи. "
                "Поддерживается local storage; импорт — из указанного каталога источника."
            ).classes("rag-meta")

            enabled_input = ui.checkbox("Включить Cloud Drive", value=initial_cloud["cloud_drive_enabled"])
            enabled_input.tooltip("Включает реестр файлов и хранилище Cloud Drive.")

            with ui.row().classes("w-full items-center gap-1"):
                db_input = ui.input("База реестра Cloud Drive", value=initial_cloud["cloud_drive_db_path"]).props("dense outlined").classes("flex-1")
                db_input.tooltip("SQLite-база реестра: хранит структуру папок, метаданные файлов, версии и историю задач.")
                btn_db = ui.button(icon="folder_open").props("flat dense round").classes("text-indigo-400 mt-1")
                btn_db.tooltip("Выбрать файл базы данных")
                btn_db.on_click(lambda: _pick_file_dialog(db_input))

            storage_kind = ui.select(
                {"local": "Local storage", "s3": "S3 / MinIO"},
                value=initial_cloud["cloud_drive_storage"],
                label="Хранилище файлов",
            ).props("dense outlined").classes("w-full")
            storage_kind.tooltip("Место физического хранения содержимого файлов.")

            storage_root_row = ui.row().classes("w-full items-center gap-1")
            with storage_root_row:
                storage_root_input = ui.input("Папка хранения файлов", value=initial_cloud["cloud_drive_storage_root"]).props("dense outlined").classes("flex-1")
                storage_root_input.tooltip("Корневая папка для local storage.")
                btn_root = ui.button(icon="folder_open").props("flat dense round").classes("text-indigo-400 mt-1")
                btn_root.tooltip("Выбрать папку")
                btn_root.on_click(lambda: _pick_folder_dialog(storage_root_input))

            s3_summary_row = ui.row().classes("w-full items-center gap-2")
            with s3_summary_row:
                s3_chip_label = ui.label("").classes("rag-meta text-xs bg-indigo-50 px-2 py-1 rounded")

            with ui.dialog() as s3_dialog, ui.card().classes("w-full max-w-lg gap-3 p-4"):
                ui.label("S3 / MinIO настройки").classes("text-lg font-semibold")
                ui.label(
                    "Параметры относятся к инфраструктуре. Меняйте только если знаете "
                    "что делаете — смена bucket/endpoint делает ранее загруженные файлы недоступными."
                ).classes("rag-meta text-sm")
                with ui.element("div").classes("w-full p-3 rounded border border-orange-200 bg-orange-50"):
                    with ui.row().classes("items-center gap-2"):
                        ui.icon("warning", size="16px").classes("text-orange-600")
                        ui.label("Изменение этих настроек может привести к потере доступа к уже загруженным файлам.").classes("text-sm text-orange-900")
                s3_bucket_input = ui.input("S3 bucket (обязателен)", value=initial_cloud["cloud_drive_bucket"]).props("dense outlined").classes("w-full")
                s3_bucket_input.tooltip("Bucket — контейнер объектов. Без него Cloud Drive не знает куда писать/читать файлы.")
                s3_endpoint_input = ui.input("S3 endpoint (MinIO)", value=initial_cloud["cloud_drive_s3_endpoint"]).props("dense outlined").classes("w-full")
                s3_endpoint_input.tooltip("URL для MinIO (напр. http://127.0.0.1:9000). Для AWS S3 оставьте пустым.")
                s3_region_input = ui.input("S3 region", value=initial_cloud["cloud_drive_s3_region"]).props("dense outlined").classes("w-full")
                s3_region_input.tooltip("Регион. Для MinIO обычно us-east-1.")
                s3_access_input = ui.input("S3 access key", value=initial_cloud["cloud_drive_s3_access_key"]).props("dense outlined").classes("w-full")
                s3_secret_input = ui.input("S3 secret key", value=initial_cloud["cloud_drive_s3_secret_key"], password=True, password_toggle_button=True).props("dense outlined").classes("w-full")
                with ui.row().classes("w-full justify-end gap-2 pt-2"):
                    ui.button("Отмена", on_click=s3_dialog.close).props("flat dense")
                    ui.button("Применить", icon="check", on_click=s3_dialog.close).props("unelevated dense")

            with s3_summary_row:
                btn_s3_configure = ui.button("Настроить S3 / MinIO...", icon="settings", on_click=s3_dialog.open).props("outline dense size=sm")  # noqa: F841

            catalog_input = _path_row_cd("Источник импорта", initial_cloud["catalog_path"], folder=True)
            catalog_input.tooltip("Каталог источника для импорта. Обычно совпадает с основным каталогом документов.")

            bootstrap_limit = ui.number("Лимит импорта файлов (0 = без лимита)", value=0, min=0, step=100).props("dense outlined").classes("w-full")
            bootstrap_limit.tooltip("Ограничивает количество файлов при пробном запуске. 0 — без ограничения.")

            ui.separator()
            with ui.row().classes("w-full items-center gap-2"):
                ui.icon("schedule", size="18px").classes("text-indigo-400")
                ui.label("Автосинхронизация").classes("font-semibold text-sm")
            ui.label(
                "Фоновый таймер автоматически добавляет новые и изменённые файлы в реестр. "
                "Уже существующие файлы пропускаются (upsert). Рекомендуется для каталогов с несколькими пользователями."
            ).classes("rag-meta text-xs")
            autosync_options = {0: "Выключено", 1: "Каждую минуту", 5: "Каждые 5 мин", 15: "Каждые 15 мин", 30: "Каждые 30 мин",
                                60: "Каждый час", 120: "Каждые 2 часа", 240: "Каждые 4 часа"}
            autosync_select = ui.select(
                autosync_options,
                value=int(state.cfg.get("cloud_drive_autosync_minutes") or 0),
                label="Интервал автосинхронизации",
            ).props("dense outlined").classes("w-full max-w-xs")
            autosync_status_label = ui.label("").classes("rag-meta text-xs")

            ui.separator()

            status_box = ui.column().classes("w-full gap-1")
            bootstrap_box = ui.column().classes("w-full gap-1")
            jobs_box = ui.column().classes("w-full gap-2")
            action_row = ui.row().classes("rag-dirty-actions")
            action_row.set_visibility(False)

            def current_cloud_values() -> Dict[str, Any]:
                return {
                    "cloud_drive_enabled": bool(enabled_input.value),
                    "cloud_drive_db_path": str(db_input.value or "").strip(),
                    "cloud_drive_storage": str(storage_kind.value or "local").strip() or "local",
                    "cloud_drive_storage_root": str(storage_root_input.value or "").strip(),
                    "cloud_drive_bucket": str(s3_bucket_input.value or "").strip(),
                    "cloud_drive_s3_endpoint": str(s3_endpoint_input.value or "").strip(),
                    "cloud_drive_s3_region": str(s3_region_input.value or "").strip(),
                    "cloud_drive_s3_access_key": str(s3_access_input.value or "").strip(),
                    "cloud_drive_s3_secret_key": str(s3_secret_input.value or "").strip(),
                    "catalog_path": str(catalog_input.value or "").strip(),
                    "cloud_drive_autosync_minutes": int(autosync_select.value or 0),
                }

            def refresh_cloud_visibility() -> None:
                kind = str(storage_kind.value or "local")
                storage_root_row.set_visibility(kind == "local")
                s3_summary_row.set_visibility(kind == "s3")
                if kind == "s3":
                    bucket = str(s3_bucket_input.value or "").strip()
                    endpoint = str(s3_endpoint_input.value or "").strip()
                    parts = [p for p in [endpoint, bucket] if p]
                    s3_chip_label.set_text(" · ".join(parts) if parts else "Не настроено")

            def refresh_cloud_dirty() -> None:
                action_row.set_visibility(current_cloud_values() != initial_cloud)

            def reset_cloud_settings() -> None:
                enabled_input.set_value(initial_cloud["cloud_drive_enabled"])
                db_input.set_value(initial_cloud["cloud_drive_db_path"])
                storage_kind.set_value(initial_cloud["cloud_drive_storage"])
                storage_root_input.set_value(initial_cloud["cloud_drive_storage_root"])
                s3_bucket_input.set_value(initial_cloud["cloud_drive_bucket"])
                s3_endpoint_input.set_value(initial_cloud["cloud_drive_s3_endpoint"])
                s3_region_input.set_value(initial_cloud["cloud_drive_s3_region"])
                s3_access_input.set_value(initial_cloud["cloud_drive_s3_access_key"])
                s3_secret_input.set_value(initial_cloud["cloud_drive_s3_secret_key"])
                catalog_input.set_value(initial_cloud["catalog_path"])
                autosync_select.set_value(initial_cloud["cloud_drive_autosync_minutes"])
                refresh_cloud_visibility()
                action_row.set_visibility(False)

            def render_cloud_stats(stats_obj: Any, *, title: str) -> None:
                stats_ref["value"] = stats_obj
                status_box.clear()
                with status_box:
                    with ui.row().classes("w-full items-center gap-2"):
                        ui.label(title).classes("font-semibold text-sm")
                        ui.space()
                        ui.button(icon="refresh", on_click=lambda: ui.timer(0.05, refresh_registry_stats, once=True)).props("flat dense round size=sm").classes("text-indigo-400").tooltip("Обновить статистику")
                    if not stats_obj:
                        with ui.element("div").classes("cd-empty-state w-full"):
                            ui.icon("cloud_off", size="28px").classes("opacity-30")
                            ui.label("Реестр ещё не инициализирован — нажмите «Инициализировать реестр».").classes("text-center")
                        return
                    with ui.row().classes("w-full gap-2 flex-wrap"):
                        for icon_name, lbl, val in [
                            ("folder",      "Папок",  f"{int(getattr(stats_obj, 'folders', 0)):,}".replace(",", " ")),
                            ("description", "Файлов", f"{int(getattr(stats_obj, 'files', 0)):,}".replace(",", " ")),
                            ("history",     "Версий", f"{int(getattr(stats_obj, 'versions', 0)):,}".replace(",", " ")),
                            ("pending",     "Jobs",   f"{int(getattr(stats_obj, 'pending_jobs', 0)):,}".replace(",", " ")),
                        ]:
                            with ui.column().classes("rag-card p-2 gap-0 items-center min-w-20 flex-1"):
                                ui.icon(icon_name, size="18px").classes("text-indigo-400")
                                ui.label(val).classes("text-base font-semibold leading-tight")
                                ui.label(lbl).classes("rag-meta text-xs")
                    root_path = str(getattr(stats_obj, "root_path", "") or "")
                    if root_path:
                        ui.label(f"Корень: {root_path}").classes("rag-path text-xs")
                    try:
                        _savings = CloudDriveService.from_config(build_cloud_config()).registry.get_storage_savings()
                        _saved = int(_savings.get("saved_bytes") or 0)
                        _dups = max(0, int(_savings.get("total_files") or 0) - int(_savings.get("unique_storage_keys") or 0))
                        if _saved > 0 or _dups > 0:
                            _mb = _saved / 1_048_576
                            _lbl = f"Дедупликация: сэкономлено {_mb:.1f} МБ"
                            if _dups > 0:
                                _lbl += f" · {_dups} дублей"
                            ui.label(_lbl).classes("rag-meta text-xs text-green-700")
                    except Exception:
                        pass

            _CD_STATUS_META = {
                "pending":   ("schedule",     "cd-status-pending",   "Ожидание"),
                "running":   ("sync",         "cd-status-running",   "Выполняется"),
                "done":      ("check_circle", "cd-status-done",      "Завершён"),
                "error":     ("error",        "cd-status-error",     "Ошибка"),
                "stale":     ("warning",      "cd-status-error",     "Устарело"),
                "cancelled": ("cancel",       "cd-status-cancelled", "Отменён"),
            }

            def _cd_status_badge(status: str) -> None:
                icon_name, css_cls, label_ru = _CD_STATUS_META.get(status, ("help", "cd-status-cancelled", status))
                with ui.element("span").classes(f"cd-status-badge {css_cls}"):
                    ui.icon(icon_name, size="14px")
                    ui.label(label_ru)

            _JOB_TYPE_LABELS: Dict[str, str] = {
                "bootstrap": "Импорт реестра",
                "reindex": "Реиндексация",
                "scan": "Сканирование структуры",
                "cleanup": "Очистка",
            }

            def render_bootstrap_status() -> None:
                bootstrap_state = _read_cloud_bootstrap_status(build_cloud_config())
                bootstrap_box.clear()
                with bootstrap_box:
                    ui.label("Статус импорта").classes("font-semibold text-sm")
                    raw_status = str(bootstrap_state.get("status") or bootstrap_state.get("job_status") or "idle")
                    status = {"pending": "pending", "running": "running", "completed": "done", "failed": "error", "cancelled": "cancelled"}.get(raw_status, raw_status)
                    if status == "idle":
                        with ui.element("div").classes("cd-empty-state w-full"):
                            ui.icon("cloud_upload", size="24px").classes("opacity-30")
                            ui.label("Импорт не запускался. Нажмите «Добавить новые файлы» ниже.").classes("text-center")
                        return
                    _cd_status_badge(status)
                    imported_files = _safe_int(bootstrap_state.get("imported_files"), 0)
                    total_files = _safe_int(bootstrap_state.get("total_files"), 0)
                    if total_files > 0:
                        ratio = max(0.0, min(1.0, imported_files / total_files))
                        ui.linear_progress(value=ratio).classes("w-full")
                        ui.label(f"Файлы: {imported_files:,} / {total_files:,} ({round(ratio * 100)}%)".replace(",", " ")).classes("rag-meta")
                    elif imported_files:
                        ui.label(f"Файлы: {imported_files:,}".replace(",", " ")).classes("rag-meta")
                    imported_folders = _safe_int(bootstrap_state.get("imported_folders"), 0)
                    if imported_folders:
                        ui.label(f"Папки: {imported_folders:,}".replace(",", " ")).classes("rag-meta")
                    current_path = str(bootstrap_state.get("current_path") or "").strip()
                    if current_path:
                        ui.label(f"Текущий путь: {current_path}").classes("rag-path")
                    error_text = str(bootstrap_state.get("error") or "").strip()
                    if error_text:
                        ui.label(error_text).classes("text-negative text-sm")
                    started_at = str(bootstrap_state.get("started_at") or "").strip()
                    finished_at = str(bootstrap_state.get("finished_at") or "").strip()
                    if started_at:
                        ui.label(f"Старт: {started_at[:19].replace(chr(84), chr(32))}").classes("rag-meta")
                    if finished_at:
                        ui.label(f"Финиш: {finished_at[:19].replace(chr(84), chr(32))}").classes("rag-meta")

            def render_bootstrap_jobs() -> None:
                jobs_box.clear()
                cfg_now = build_cloud_config()
                if not str(cfg_now.get("cloud_drive_db_path") or "").strip():
                    with jobs_box:
                        ui.label("Последние задачи").classes("font-semibold text-sm")
                        with ui.element("div").classes("cd-empty-state w-full"):
                            ui.icon("settings", size="24px").classes("opacity-30")
                            ui.label("Сохраните настройки Cloud Drive, чтобы видеть историю задач.").classes("text-center")
                    return
                try:
                    service = CloudDriveService.from_config(cfg_now)
                    jobs = service.list_bootstrap_jobs(limit=8)
                except Exception as exc:
                    with jobs_box:
                        ui.label("Последние задачи").classes("font-semibold text-sm")
                        with ui.element("div").classes("cd-empty-state w-full"):
                            ui.icon("error_outline", size="24px").classes("text-red-400 opacity-70")
                            ui.label(f"Не удалось прочитать задачи: {exc}").classes("text-center text-red-600 text-xs")
                    return
                with jobs_box:
                    ui.label("Последние задачи").classes("font-semibold text-sm")
                    if not jobs:
                        with ui.element("div").classes("cd-empty-state w-full"):
                            ui.icon("history", size="24px").classes("opacity-30")
                            ui.label("История задач пуста. Запустите импорт, чтобы начать.").classes("text-center")
                        return
                    for job in jobs:
                        progress = dict(job.progress or {})
                        raw_status = str(job.status or progress.get("status") or "")
                        norm_status = {"pending": "pending", "running": "running", "completed": "done", "failed": "error", "cancelled": "cancelled"}.get(raw_status, raw_status)
                        imported_files = _safe_int(progress.get("imported_files"), 0)
                        total_files = _safe_int(progress.get("total_files"), 0)
                        catalog_src = str(progress.get("catalog") or progress.get("current_path") or "").strip()
                        error_text = str(job.last_error or progress.get("error") or "").strip()
                        import_files_flag = bool(progress.get("import_files", True))
                        job_type_raw = str(getattr(job, "job_type", "") or "bootstrap")
                        job_label = _JOB_TYPE_LABELS.get(job_type_raw, job_type_raw)
                        if job_type_raw == "bootstrap":
                            job_label = "Импорт файлов" if import_files_flag else "Сканирование структуры"
                        started_at = str(progress.get("started_at") or "").strip()
                        finished_at = str(progress.get("finished_at") or "").strip()
                        with ui.element("div").classes("cd-jobs-card w-full"):
                            with ui.row().classes("w-full items-center gap-2"):
                                _cd_status_badge(norm_status)
                                ui.label(job_label).classes("text-xs font-medium")
                                ui.space()
                                ui.label(job.id[:8]).classes("font-mono text-xs rag-meta")
                            if catalog_src:
                                ui.label(catalog_src).classes("rag-path text-xs truncate")
                            if total_files > 0:
                                ratio = max(0.0, min(1.0, imported_files / total_files))
                                ui.linear_progress(value=ratio, size="4px", show_value=False).classes("w-full my-1").props("color=indigo")
                                ui.label(f"{imported_files:,} / {total_files:,} файлов ({round(ratio * 100)}%)".replace(",", " ")).classes("rag-meta text-xs")
                            elif imported_files:
                                ui.label(f"{imported_files:,} файлов".replace(",", " ")).classes("rag-meta text-xs")
                            if started_at or finished_at:
                                time_parts = []
                                if started_at:
                                    time_parts.append(f"Старт: {started_at[:19].replace(chr(84), chr(32))}")
                                if finished_at:
                                    time_parts.append(f"Финиш: {finished_at[:19].replace(chr(84), chr(32))}")
                                ui.label(" · ".join(time_parts)).classes("rag-meta text-xs")
                            if error_text and norm_status in ("error", "cancelled"):
                                ui.label(error_text).classes("text-red-600 text-xs mt-1 truncate")
                            with ui.row().classes("gap-1 mt-1"):
                                if raw_status in {"running", "pending"}:
                                    ui.button(icon="close", on_click=lambda _e=None, jid=job.id: cancel_bootstrap_job(jid)).props("flat dense round size=sm color=negative").tooltip("Отменить задачу")
                                if raw_status in {"failed", "cancelled", "completed"}:
                                    ui.button(icon="replay", on_click=lambda _e=None, jid=job.id: retry_bootstrap_job(jid)).props("flat dense round size=sm").tooltip("Повторить задачу")

            def build_cloud_config() -> Dict[str, Any]:
                values = current_cloud_values()
                cfg = dict(state.cfg)
                cfg.update(values)
                return cfg

            def persist_cloud_values(values: Dict[str, Any]) -> Dict[str, Any]:
                cfg = load_config()
                cfg.update(values)
                save_config(cfg)
                state.cfg = cfg
                initial_cloud.update(values)
                return cfg

            def save_cloud_settings() -> None:
                values = current_cloud_values()
                if not values["cloud_drive_db_path"]:
                    ui.notify("Укажите путь к базе данных реестра.", type="warning")
                    return
                if values["cloud_drive_storage"] == "local" and not values["cloud_drive_storage_root"]:
                    ui.notify("Укажите папку хранения файлов для локального хранилища.", type="warning")
                    return
                if values["cloud_drive_storage"] == "s3":
                    if not values["cloud_drive_bucket"]:
                        ui.notify("Для S3/MinIO требуется cloud_drive_bucket.", type="warning")
                        return
                    if not values["cloud_drive_s3_access_key"] or not values["cloud_drive_s3_secret_key"]:
                        ui.notify("Для S3/MinIO укажите access key и secret key.", type="warning")
                        return
                try:
                    persist_cloud_values(values)
                    refresh_cloud_visibility()
                    action_row.set_visibility(False)
                    _log_app_event(state, "settings", "save_cloud_drive", details=values)
                    ui.notify("Настройки Cloud Drive сохранены.", type="positive")
                except Exception as exc:
                    ui.notify(f"Не удалось сохранить настройки: {exc}", type="negative")

            async def refresh_registry_stats() -> None:
                try:
                    cfg = build_cloud_config()
                    service = await run.io_bound(CloudDriveService.from_config, cfg)
                    stats_obj = await run.io_bound(service.registry.stats)
                    render_cloud_stats(stats_obj, title="Статистика реестра")
                    render_bootstrap_status()
                    render_bootstrap_jobs()
                except Exception:
                    render_cloud_stats(None, title="Статистика реестра")

            with ui.dialog() as init_confirm_dialog, ui.card().classes("p-4 gap-3 max-w-sm"):
                ui.label("Инициализировать реестр?").classes("text-base font-semibold")
                ui.label(
                    "Создаёт схему базы данных Cloud Drive. Если реестр уже существует, данные не удаляются. "
                    "Нажмите Создать только если настраиваете Cloud Drive впервые или после удаления базы."
                ).classes("rag-meta text-sm")
                with ui.row().classes("w-full justify-end gap-2 pt-1"):
                    ui.button("Отмена", on_click=init_confirm_dialog.close).props("flat dense")
                    ui.button("Создать", icon="database", on_click=lambda: (init_confirm_dialog.close(), ui.timer(0.05, init_registry, once=True))).props("unelevated dense")

            async def init_registry() -> None:
                try:
                    cfg = persist_cloud_values(current_cloud_values())
                    service = await run.io_bound(CloudDriveService.from_config, cfg)
                    stats_obj = await run.io_bound(service.registry.stats)
                    render_cloud_stats(stats_obj, title="Реестр инициализирован")
                    _log_app_event(state, "cloud_drive", "init_registry", details=current_cloud_values())
                    ui.notify("Реестр Cloud Drive инициализирован.", type="positive")
                except Exception as exc:
                    ui.notify(f"Не удалось инициализировать реестр: {exc}", type="negative")

            def _run_bootstrap_background(cfg: Dict[str, Any], *, job_id: str) -> None:
                try:
                    service = CloudDriveService.from_config(cfg)
                    service.run_bootstrap_job(job_id)
                except Exception:
                    pass

            async def bootstrap_registry(*, import_files: bool = True) -> None:
                catalog = str(catalog_input.value or "").strip()
                if not catalog:
                    ui.notify("Укажите источник импорта.", type="warning")
                    return
                if not Path(catalog).exists():
                    ui.notify("Источник импорта не найден.", type="negative")
                    return
                limit_value = int(bootstrap_limit.value or 0)
                try:
                    cfg = persist_cloud_values(current_cloud_values())
                    current_state = _read_cloud_bootstrap_status(cfg)
                    if str(current_state.get("job_status") or current_state.get("status") or "") in {"running", "pending"}:
                        ui.notify("Импорт уже выполняется.", type="warning")
                        render_bootstrap_status()
                        return
                    service = CloudDriveService.from_config(cfg)
                    job = service.create_bootstrap_job(
                        catalog_root=catalog,
                        max_files=None if limit_value <= 0 else limit_value,
                        import_files=import_files,
                    )
                    threading.Thread(
                        target=_run_bootstrap_background,
                        kwargs={"cfg": cfg, "job_id": job.id},
                        name="cloud-drive-bootstrap",
                        daemon=True,
                    ).start()
                    autosync_last_run["ts"] = datetime.datetime.now()
                    render_bootstrap_status()
                    render_bootstrap_jobs()
                    _log_app_event(state, "cloud_drive", "bootstrap", details={"catalog": catalog, "import_files": import_files, "limit": limit_value})
                    ui.notify("Импорт запущен в фоне.", type="positive")
                except Exception as exc:
                    ui.notify(f"Не удалось запустить импорт: {exc}", type="negative")

            async def cancel_bootstrap_job(job_id: str) -> None:
                try:
                    cfg = persist_cloud_values(current_cloud_values())
                    service = await run.io_bound(CloudDriveService.from_config, cfg)
                    await run.io_bound(service.cancel_job, job_id)
                    render_bootstrap_status()
                    render_bootstrap_jobs()
                    ui.notify("Отмена задачи запрошена.", type="warning")
                except Exception as exc:
                    ui.notify(f"Не удалось отменить задачу: {exc}", type="negative")

            async def retry_bootstrap_job(job_id: str) -> None:
                try:
                    cfg = persist_cloud_values(current_cloud_values())
                    current_state = _read_cloud_bootstrap_status(cfg)
                    if str(current_state.get("job_status") or current_state.get("status") or "") in {"running", "pending"}:
                        ui.notify("Сейчас уже выполняется другой импорт.", type="warning")
                        return
                    service = await run.io_bound(CloudDriveService.from_config, cfg)
                    job = await run.io_bound(service.retry_bootstrap_job, job_id)
                    threading.Thread(
                        target=_run_bootstrap_background,
                        kwargs={"cfg": cfg, "job_id": job.id},
                        name="cloud-drive-bootstrap",
                        daemon=True,
                    ).start()
                    autosync_last_run["ts"] = datetime.datetime.now()
                    render_bootstrap_status()
                    render_bootstrap_jobs()
                    ui.notify("Импорт перезапущен.", type="positive")
                except Exception as exc:
                    ui.notify(f"Не удалось повторить импорт: {exc}", type="negative")

            def _autosync_tick() -> None:
                interval_min = int(autosync_select.value or 0)
                if interval_min <= 0:
                    autosync_status_label.set_text("")
                    return
                last = autosync_last_run.get("ts")
                now = datetime.datetime.now()
                if last is None:
                    autosync_last_run["ts"] = now
                    autosync_status_label.set_text(f"Автосинхронизация включена, первый запуск через {interval_min} мин")
                    return
                elapsed = (now - last).total_seconds() / 60
                remaining = interval_min - elapsed
                if remaining <= 0:
                    cfg_now = build_cloud_config()
                    catalog = str(cfg_now.get("catalog_path") or "").strip()
                    if catalog and Path(catalog).exists():
                        current_state = _read_cloud_bootstrap_status(cfg_now)
                        if str(current_state.get("job_status") or current_state.get("status") or "") not in {"running", "pending"}:
                            try:
                                service = CloudDriveService.from_config(cfg_now)
                                job = service.create_bootstrap_job(catalog_root=catalog, import_files=True)
                                threading.Thread(
                                    target=_run_bootstrap_background,
                                    kwargs={"cfg": cfg_now, "job_id": job.id},
                                    name="cloud-drive-autosync",
                                    daemon=True,
                                ).start()
                                autosync_last_run["ts"] = now
                                autosync_status_label.set_text(f"Автосинхронизация: запущена в {now.strftime(chr(37) + chr(72) + chr(58) + chr(37) + chr(77) + chr(58) + chr(37) + chr(83))}")
                                render_bootstrap_status()
                                render_bootstrap_jobs()
                                return
                            except Exception:
                                pass
                    autosync_last_run["ts"] = now
                else:
                    next_in = int(remaining) + 1
                    last_str = last.strftime(chr(37) + chr(72) + chr(58) + chr(37) + chr(77))
                    autosync_status_label.set_text(f"Автосинхронизация: последний запуск в {last_str}, следующий через ~{next_in} мин")

            refresh_cloud_visibility()
            render_cloud_stats(None, title="Статистика реестра")
            render_bootstrap_status()
            render_bootstrap_jobs()
            ui.timer(0.2, refresh_registry_stats, once=True)

            enabled_input.on_value_change(lambda _: refresh_cloud_dirty())
            db_input.on_value_change(lambda _: refresh_cloud_dirty())
            storage_kind.on_value_change(lambda _: (refresh_cloud_visibility(), refresh_cloud_dirty()))
            storage_root_input.on_value_change(lambda _: refresh_cloud_dirty())
            s3_bucket_input.on_value_change(lambda _: (refresh_cloud_visibility(), refresh_cloud_dirty()))
            s3_endpoint_input.on_value_change(lambda _: (refresh_cloud_visibility(), refresh_cloud_dirty()))
            s3_region_input.on_value_change(lambda _: refresh_cloud_dirty())
            s3_access_input.on_value_change(lambda _: refresh_cloud_dirty())
            s3_secret_input.on_value_change(lambda _: refresh_cloud_dirty())
            catalog_input.on_value_change(lambda _: refresh_cloud_dirty())
            autosync_select.on_value_change(lambda _: refresh_cloud_dirty())

            with action_row:
                with ui.row().classes("rag-dirty-actions-inner"):
                    ui.button("Отменить", icon="close", on_click=reset_cloud_settings).props("flat dense")
                    ui.button("Сохранить настройки", icon="save", on_click=save_cloud_settings).props("outline dense")

            ui.separator()
            with ui.row().classes("w-full gap-2 flex-wrap items-center"):
                ui.button("Инициализировать реестр", icon="database", on_click=init_confirm_dialog.open).props("outline")
                ui.button("Добавить новые файлы", icon="cloud_upload", on_click=lambda: bootstrap_registry(import_files=True)).props("unelevated")

            _stop_managed_timer(state.cloud_drive_timer)
            state.cloud_drive_timer = ui.timer(3.0, lambda: (render_bootstrap_status(), render_bootstrap_jobs(), _autosync_tick()))

    def render_admin_cloud_sync_settings() -> None:  # noqa: PLR0912,PLR0915
        """Sprint 4: Sync client admin settings — folder pairs, policies, connected clients."""
        cd_enabled = bool(state.cfg.get("cloud_drive_enabled"))

        with ui.column().classes("rag-card w-full p-4 gap-3"):
            ui.label("Sync клиент").classes("text-xl font-semibold")
            ui.label(
                "Управление desktop sync-клиентами: отслеживание подключённых устройств, "
                "настройка пар папок и политики разрешения конфликтов."
            ).classes("rag-meta")

            if not cd_enabled:
                with ui.element("div").classes("cd-empty-state w-full py-6"):
                    ui.icon("cloud_off", size="32px").classes("opacity-30")
                    ui.label("Cloud Drive не включён — активируйте его в настройках Cloud Drive.").classes("text-center")
                    ui.button(
                        "Перейти в Cloud Drive",
                        icon="cloud",
                        on_click=lambda: (setattr(state, "settings_section", "cloud_drive"), render_fn()),
                        color=None,
                    ).props("outline dense")
                return

            ui.separator()

            # ── Client download ──────────────────────────────────────────
            with ui.dialog() as _install_dlg, ui.card().classes("p-5 gap-4 w-full max-w-lg"):
                ui.label("Установка sync-клиента").classes("text-base font-semibold")
                ui.label(
                    "Скачайте скрипт на компьютер пользователя, установите зависимости "
                    "и запустите с параметрами сервера и токена."
                ).classes("rag-meta text-sm")
                ui.separator()

                ui.label("Шаг 1 — скачать").classes("font-semibold text-sm")
                with ui.row().classes("gap-3 items-center flex-wrap"):
                    _dl_msi_link = ui.link("Windows MSI", target="#", new_tab=True).classes("rag-path text-sm")
                    ui.label("·").classes("rag-meta")
                    _dl_win_link = ui.link("Windows EXE (установщик)", target="#", new_tab=True).classes("rag-path text-sm")
                    ui.label("·").classes("rag-meta")
                    _dl_py_link = ui.link("Python .py", target="#", new_tab=True).classes("rag-meta text-sm")
                ui.label(
                    "MSI: тихая установка, поддержка групповых политик. "
                    "EXE: мастер настройки. Python: Linux/macOS."
                ).classes("rag-meta text-xs")

                ui.label("Шаг 2 — установить зависимости").classes("font-semibold text-sm mt-1")
                with ui.row().classes("w-full gap-1 items-center"):
                    _pip_cmd = ui.input(value="pip install requests watchdog").props(
                        'readonly dense outlined'
                    ).classes("flex-1 font-mono text-xs")
                    ui.button(icon="content_copy", on_click=lambda: ui.run_javascript(
                        f"navigator.clipboard.writeText({repr(_pip_cmd.value)})"
                    )).props("flat dense round").tooltip("Копировать")

                ui.label("Шаг 3 — запустить").classes("font-semibold text-sm mt-1")
                _run_cmd_input = ui.input(value="python rag_sync_client.py --server … --token …").props(
                    'readonly dense outlined'
                ).classes("w-full font-mono text-xs")
                with ui.row().classes("w-full justify-end gap-2"):
                    ui.button(icon="content_copy", on_click=lambda: ui.run_javascript(
                        f"navigator.clipboard.writeText({repr(_run_cmd_input.value)})"
                    )).props("flat dense round").tooltip("Копировать команду")
                    ui.button("Закрыть", on_click=_install_dlg.close).props("flat dense")

                ui.label(
                    "Токен — сессионный токен любого активного пользователя. "
                    "Клиент сохранит его в ~/.rag_sync/config.json после первого запуска."
                ).classes("rag-meta text-xs mt-1")

            async def open_install_dialog() -> None:
                try:
                    origin = await ui.run_javascript("window.location.origin")
                except Exception:
                    origin = "http://localhost:8080"
                tok = str(app.storage.user.get("auth_token") or "…").strip()
                _base = f"{origin}/api/cloud-drive/sync/client-download?auth_token={tok}"
                _dl_msi_link.target = f"{_base}&format=msi"
                _dl_win_link.target = f"{_base}&format=exe"
                _dl_py_link.target = f"{_base}&format=py"
                _run_cmd_input.set_value(
                    f"python rag_sync_client.py --server {origin} --token {tok}"
                )
                _install_dlg.open()

            with ui.row().classes("w-full justify-end"):
                ui.button(
                    "Скачать клиент", icon="download", on_click=open_install_dialog
                ).props("outline dense").classes("text-indigo-400")

            ui.separator()

            svc = _cd_get_service(state.cfg)
            if svc is None:
                with ui.element("div").classes("cd-empty-state w-full py-4"):
                    ui.icon("cloud_off", size="28px").classes("opacity-30")
                    ui.label("Cloud Drive не инициализирован. Сначала создайте реестр.").classes("text-center")
                return

            policy_labels = {
                "ask": "Спрашивать",
                "cloud_wins": "Cloud Drive приоритетнее",
                "local_wins": "Локальная версия приоритетнее",
                "newest_wins": "Более новая версия",
            }
            conflict_resolution_labels = {
                "cloud_wins": "Оставить Cloud Drive",
                "local_wins": "Оставить локальную",
                "newest_wins": "Оставить более новую",
                "keep_both": "Оставить обе",
                "ignore": "Игнорировать",
            }

            def _load_clients() -> list[dict]:
                try:
                    return svc.list_sync_clients(limit=100)
                except Exception as exc:
                    ui.notify(f"Не удалось прочитать sync-клиенты: {exc}", type="negative")
                    return []

            def _load_pairs(client_id: str = "") -> list[dict]:
                try:
                    return svc.list_sync_pairs(client_id=client_id)
                except Exception as exc:
                    ui.notify(f"Не удалось прочитать пары синхронизации: {exc}", type="negative")
                    return []

            def _register_manual_client() -> None:
                import platform as _platform  # noqa: PLC0415
                import socket as _socket  # noqa: PLC0415
                host = _socket.gethostname() or "local"
                username = _username(state) or "admin"
                try:
                    svc.register_sync_client(
                        username=username,
                        device_id=f"manual-{username}-{host}".lower(),
                        display_name=f"{host} (ручная настройка)",
                        platform=_platform.system() or "manual",
                        status="offline",
                        metadata={"source": "web-settings"},
                    )
                    ui.notify("Sync-клиент добавлен в реестр.", type="positive")
                    render_fn()
                except Exception as exc:
                    ui.notify(f"Не удалось добавить клиента: {exc}", type="negative")

            clients = _load_clients()
            selected_client = str(clients[0].get("id") or "") if clients else ""

            # ── Подключённые клиенты ──────────────────────────────────────
            with ui.expansion("Подключённые клиенты", icon="computer", value=True).classes("w-full"):
                if not clients:
                    with ui.element("div").classes("cd-empty-state w-full py-4"):
                        ui.icon("sync_disabled", size="28px").classes("opacity-30")
                        ui.label("Нет подключённых sync-клиентов.").classes("text-center")
                        ui.label("До установки desktop-агента можно создать ручную запись клиента для настройки пар.").classes("text-center rag-meta text-xs")
                        ui.button("Добавить текущий компьютер", icon="add", on_click=_register_manual_client).props("outline dense")
                else:
                    with ui.column().classes("w-full gap-2"):
                        for client in clients:
                            status = str(client.get("status") or "offline")
                            with ui.row().classes("rag-explorer-item w-full p-2 items-center gap-3"):
                                ui.icon("computer", size="20px").classes("text-indigo-400")
                                with ui.column().classes("flex-1 min-w-0 gap-0"):
                                    ui.label(str(client.get("display_name") or client.get("device_id") or "Sync client")).classes("text-sm font-medium truncate")
                                    ui.label(
                                        f"{client.get('username') or ''} · {client.get('platform') or 'unknown'} · last seen: {str(client.get('last_seen_at') or '')[:19].replace('T', ' ')}"
                                    ).classes("rag-meta text-xs truncate")
                                color = "positive" if status == "online" else "warning" if status in {"paused", "error"} else "grey-4"
                                ui.badge(status, color=color).classes("text-xs")
                        ui.button("Добавить текущий компьютер", icon="add", on_click=_register_manual_client).props("outline dense")

            ui.separator()

            # ── Пары папок ────────────────────────────────────────────────
            with ui.expansion("Пары папок", icon="folder_copy", value=True).classes("w-full"):
                pairs = _load_pairs()
                if not pairs:
                    with ui.element("div").classes("cd-empty-state w-full py-3"):
                        ui.icon("folder_copy", size="24px").classes("opacity-30")
                        ui.label("Нет настроенных пар для синхронизации.").classes("text-center")
                else:
                    with ui.column().classes("w-full gap-2"):
                        for pair in pairs:
                            with ui.row().classes("rag-explorer-item w-full p-2 items-center gap-3"):
                                ui.icon("folder_copy", size="20px").classes("text-indigo-400")
                                with ui.column().classes("flex-1 min-w-0 gap-0"):
                                    ui.label(str(pair.get("local_path") or "(не задано)")).classes("text-sm font-medium truncate")
                                    cloud_path = str(pair.get("cloud_path") or "")
                                    ui.label(f"→ Cloud Drive: {cloud_path or 'Корень'}").classes("rag-meta text-xs truncate")
                                policy = str(pair.get("conflict_policy") or "ask")
                                ui.badge(policy_labels.get(policy, policy), color="grey-4").classes("text-xs")
                                if not bool(pair.get("enabled", True)):
                                    ui.badge("выключено", color="warning").classes("text-xs")
                                ui.button(
                                    icon="delete",
                                    color=None,
                                    on_click=lambda pair_id=str(pair.get("id") or ""), client_id=str(pair.get("client_id") or ""): (
                                        svc.delete_sync_pair(pair_id, client_id=client_id), ui.notify("Пара удалена.", type="positive"), render_fn()
                                    ),
                                ).props("flat round dense").tooltip("Удалить пару").classes("text-negative")

                async def _add_pair_dialog() -> None:
                    if not clients:
                        ui.notify("Сначала добавьте sync-клиент.", type="warning")
                        return
                    with ui.dialog() as dlg, ui.card().classes("p-4 gap-3 w-[520px]"):
                        ui.label("Добавить пару синхронизации").classes("text-lg font-semibold")
                        client_options = {
                            str(c.get("id") or ""): f"{c.get('display_name') or c.get('device_id')} · {c.get('username')}"
                            for c in clients
                            if c.get("id")
                        }
                        client_sel = ui.select(
                            options=client_options,
                            value=selected_client,
                            label="Sync-клиент",
                        ).props("dense outlined emit-value map-options").classes("w-full")
                        local_input = ui.input(
                            "Локальная папка",
                            placeholder="C:\\Users\\Иван\\Documents\\Рабочие",
                        ).props("dense outlined").classes("w-full")
                        try:
                            with svc.registry._connect() as _c:
                                _frows = _c.execute("SELECT * FROM cloud_folders WHERE deleted_at='' ORDER BY path").fetchall()
                            folder_options = {
                                fo.path: (fo.path or "Корень")
                                for fo in [svc.registry._folder_from_row(r) for r in _frows]
                            }
                        except Exception:
                            folder_options = {"": "Корень"}
                        if "" not in folder_options:
                            folder_options = {"": "Корень", **folder_options}
                        cloud_sel = ui.select(
                            options=folder_options,
                            value="",
                            label="Папка в Cloud Drive",
                        ).props("dense outlined emit-value map-options").classes("w-full")
                        policy_sel = ui.select(
                            options=policy_labels,
                            value="ask",
                            label="Политика конфликтов",
                        ).props("dense outlined emit-value map-options").classes("w-full")

                        def _do_add() -> None:
                            client_id = str(client_sel.value or "").strip()
                            local_path = str(local_input.value or "").strip()
                            if not client_id:
                                ui.notify("Выберите sync-клиент.", type="warning")
                                return
                            if not local_path:
                                ui.notify("Укажите локальную папку.", type="warning")
                                return
                            try:
                                svc.upsert_sync_pair(
                                    client_id=client_id,
                                    local_path=local_path,
                                    cloud_path=str(cloud_sel.value or ""),
                                    conflict_policy=str(policy_sel.value or "ask"),
                                    enabled=True,
                                )
                                dlg.close()
                                ui.notify("Пара синхронизации сохранена.", type="positive")
                                render_fn()
                            except Exception as exc:
                                ui.notify(f"Не удалось сохранить пару: {exc}", type="negative")

                        with ui.row().classes("w-full justify-end gap-2 mt-2"):
                            ui.button("Отмена", on_click=dlg.close).props("flat dense")
                            ui.button("Добавить", icon="add", on_click=_do_add).props("unelevated dense")
                    dlg.open()

                ui.button("Добавить пару", icon="add_link", on_click=_add_pair_dialog).props("outline dense")

            ui.separator()

            # ── Выборочная синхронизация ──────────────────────────────────
            with ui.expansion("Выборочная синхронизация", icon="checklist", value=False).classes("w-full"):
                if not clients:
                    ui.label("Нет sync-клиентов для настройки selective sync.").classes("rag-meta text-sm")
                else:
                    client_options = {
                        str(c.get("id") or ""): f"{c.get('display_name') or c.get('device_id')} · {c.get('username')}"
                        for c in clients
                        if c.get("id")
                    }
                    client_sel = ui.select(
                        options=client_options,
                        value=selected_client,
                        label="Sync-клиент",
                    ).props("dense outlined emit-value map-options").classes("w-full max-w-xl")
                    try:
                        with svc.registry._connect() as _c2:
                            _frows2 = _c2.execute(
                                "SELECT * FROM cloud_folders WHERE deleted_at='' AND depth <= 2 AND is_root=0 ORDER BY path"
                            ).fetchall()
                        top_folders = [svc.registry._folder_from_row(r) for r in _frows2]
                    except Exception:
                        top_folders = []
                    if not top_folders:
                        with ui.element("div").classes("cd-empty-state w-full py-3"):
                            ui.icon("folder_off", size="24px").classes("opacity-30")
                            ui.label("Нет папок в реестре. Запустите импорт в Cloud Drive.").classes("text-center")
                    else:
                        mode_sel = ui.select(
                            options={"exclude": "Исключить выбранные", "include": "Синхронизировать только выбранные"},
                            value="exclude",
                            label="Режим",
                        ).props("dense outlined emit-value map-options").classes("w-full max-w-sm")
                        existing = svc.list_selective_sync_paths(client_id=selected_client).get("paths", []) if selected_client else []
                        existing_paths = {str(item.get("cloud_path") or "") for item in existing}
                        checkboxes: Dict[str, Any] = {}
                        with ui.column().classes("w-full gap-1"):
                            for folder in top_folders:
                                cb = ui.checkbox(folder.path, value=(folder.path in existing_paths))
                                checkboxes[folder.path] = cb

                        def _save_selective_sync() -> None:
                            client_id = str(client_sel.value or "").strip()
                            if not client_id:
                                ui.notify("Выберите sync-клиент.", type="warning")
                                return
                            paths = [p for p, cb in checkboxes.items() if bool(cb.value)]
                            try:
                                svc.set_selective_sync_paths(
                                    client_id=client_id,
                                    paths=paths,
                                    mode=str(mode_sel.value or "exclude"),
                                    replace=True,
                                )
                                ui.notify("Выборочная синхронизация сохранена.", type="positive")
                                render_fn()
                            except Exception as exc:
                                ui.notify(f"Ошибка: {exc}", type="negative")

                        ui.button("Сохранить", icon="save", on_click=_save_selective_sync).props("outline dense")

            ui.separator()

            # ── Журнал конфликтов ────────────────────────────────────────
            with ui.expansion("Журнал конфликтов", icon="history_toggle_off", value=True).classes("w-full"):
                try:
                    open_conflicts = svc.list_sync_conflicts(status="open", limit=100)
                    resolved_conflicts = svc.list_sync_conflicts(status="resolved", limit=20)
                except Exception as exc:
                    ui.label(f"Не удалось прочитать конфликты: {exc}").classes("text-negative text-sm")
                    open_conflicts = []
                    resolved_conflicts = []
                if not open_conflicts:
                    with ui.element("div").classes("cd-empty-state w-full py-3"):
                        ui.icon("check_circle", size="24px").classes("opacity-30")
                        ui.label("Открытых конфликтов нет.").classes("text-center")
                else:
                    with ui.column().classes("w-full gap-2"):
                        for conflict in open_conflicts:
                            with ui.column().classes("rag-explorer-item w-full p-3 gap-2"):
                                with ui.row().classes("w-full items-center gap-2"):
                                    ui.icon("merge", size="20px").classes("text-orange-500")
                                    with ui.column().classes("flex-1 min-w-0 gap-0"):
                                        ui.label(str(conflict.get("path") or conflict.get("cloud_path") or conflict.get("local_path") or "Конфликт")).classes("text-sm font-medium truncate")
                                        ui.label(
                                            f"{conflict.get('conflict_type') or 'unknown'} · {str(conflict.get('created_at') or '')[:19].replace('T', ' ')}"
                                        ).classes("rag-meta text-xs")
                                    ui.badge("open", color="warning").classes("text-xs")
                                with ui.row().classes("w-full gap-2 flex-wrap"):
                                    for resolution, label in conflict_resolution_labels.items():
                                        ui.button(
                                            label,
                                            on_click=lambda cid=str(conflict.get("id") or ""), res=resolution: (
                                                svc.resolve_sync_conflict(cid, resolution=res, resolved_by=_username(state)),
                                                ui.notify("Конфликт закрыт.", type="positive"),
                                                render_fn(),
                                            ),
                                        ).props("outline dense no-caps")
                if resolved_conflicts:
                    ui.separator()
                    ui.label("Последние закрытые").classes("font-semibold text-sm")
                    with ui.column().classes("w-full gap-1"):
                        for conflict in resolved_conflicts:
                            with ui.row().classes("rag-explorer-item w-full p-2 items-center gap-2"):
                                ui.icon("done", size="18px").classes("text-green-500")
                                ui.label(str(conflict.get("path") or "")).classes("text-sm flex-1 truncate")
                                ui.badge(str(conflict.get("resolution") or "resolved"), color="positive").classes("text-xs")

    def render_admin_llm_settings() -> None:
        def _fetch_ollama_models(ollama_url: str) -> List[str]:
            """Запросить список моделей из Ollama /api/tags. Возвращает [] при ошибке."""
            try:
                import json as _json  # noqa: PLC0415
                import urllib.request as _ur  # noqa: PLC0415
                req = _ur.Request(f"{ollama_url.rstrip('/')}/api/tags", method="GET")
                with _ur.urlopen(req, timeout=4) as resp:
                    data = _json.loads(resp.read().decode())
                return sorted(m["name"] for m in (data.get("models") or []) if m.get("name"))
            except Exception:
                return []

        current_url = str(state.cfg.get("ollama_url") or "http://localhost:11434")
        current_expand = str(state.cfg.get("llm_expand_model") or "phi3:mini")
        current_rag = str(state.cfg.get("llm_rag_model") or "qwen3:8b")

        # Подтягиваем модели сразу при рендере
        available_models = _fetch_ollama_models(current_url)
        # Гарантируем, что текущие значения есть в списке даже если Ollama недоступен
        for m in [current_expand, current_rag]:
            if m and m not in available_models:
                available_models.insert(0, m)
        if not available_models:
            available_models = [current_expand, current_rag]

        with ui.column().classes("rag-card w-full p-4 gap-3"):
            initial_llm = {
                "llm_enabled": bool(state.cfg.get("llm_enabled")),
                "ollama_url": current_url.strip(),
                "llm_expand_model": current_expand.strip(),
                "llm_rag_model": current_rag.strip(),
            }
            ui.label("Нейросеть (LLM)").classes("text-xl font-semibold")
            ui.label(
                "Используется Ollama, запущенный локально. "
                "Включите, чтобы получать ответ ИИ по документам и автоматически расширять запросы."
            ).classes("rag-meta")

            llm_toggle = ui.switch(
                "Включить ИИ-ответ и расширение запроса",
                value=bool(state.cfg.get("llm_enabled")),
            )
            ollama_url_input = ui.input(
                "Ollama URL",
                value=current_url,
            ).props("dense outlined").classes("w-full")

            status_label = ui.label(
                f"Найдено моделей: {len(available_models)}" if available_models else "Ollama недоступен — список пуст"
            ).classes("rag-meta text-sm")

            expand_select = ui.select(
                label="Модель расширения запроса (быстрая, лёгкая)",
                options=available_models,
                value=current_expand,
                with_input=True,
            ).props("dense outlined").classes("w-full")

            rag_select = ui.select(
                label="Модель RAG Q&A (умная, для анализа документов)",
                options=available_models,
                value=current_rag,
                with_input=True,
            ).props("dense outlined").classes("w-full")

            async def refresh_models() -> None:
                url = str(ollama_url_input.value or "http://localhost:11434").strip()
                models = await run.io_bound(_fetch_ollama_models, url)
                for m in [str(expand_select.value or ""), str(rag_select.value or "")]:
                    if m and m not in models:
                        models.insert(0, m)
                if not models:
                    status_label.set_text("Ollama недоступен или нет установленных моделей")
                    ui.notify("Ollama не отвечает по адресу: " + url, type="warning")
                    return
                expand_select.options = models
                rag_select.options = models
                expand_select.update()
                rag_select.update()
                status_label.set_text(f"Найдено моделей: {len(models)}")
                ui.notify(f"Обновлено: {len(models)} моделей", type="positive")

            ui.button("Обновить список моделей", icon="refresh", on_click=refresh_models).props("flat dense")

            action_row = ui.row().classes("rag-dirty-actions")
            action_row.set_visibility(False)

            def current_llm_settings() -> Dict[str, Any]:
                return {
                    "llm_enabled": bool(llm_toggle.value),
                    "ollama_url": str(ollama_url_input.value or "http://localhost:11434").strip(),
                    "llm_expand_model": str(expand_select.value or "phi3:mini").strip(),
                    "llm_rag_model": str(rag_select.value or "qwen3:8b").strip(),
                }

            def refresh_llm_dirty() -> None:
                action_row.set_visibility(current_llm_settings() != initial_llm)

            def reset_llm_settings() -> None:
                llm_toggle.set_value(initial_llm["llm_enabled"])
                ollama_url_input.set_value(initial_llm["ollama_url"])
                expand_select.set_value(initial_llm["llm_expand_model"])
                rag_select.set_value(initial_llm["llm_rag_model"])
                action_row.set_visibility(False)

            def save_llm_settings() -> None:
                try:
                    values = current_llm_settings()
                    cfg = load_config()
                    cfg["llm_enabled"] = values["llm_enabled"]
                    cfg["ollama_url"] = values["ollama_url"]
                    cfg["llm_expand_model"] = values["llm_expand_model"]
                    cfg["llm_rag_model"] = values["llm_rag_model"]
                    save_config(cfg)
                    state.cfg = cfg
                    initial_llm.update(values)
                    action_row.set_visibility(False)
                    _log_app_event(state, "settings", "save_llm", details={
                        "llm_enabled": cfg["llm_enabled"],
                        "ollama_url": cfg["ollama_url"],
                    })
                    ui.notify("Настройки нейросети сохранены.", type="positive")
                except Exception as exc:
                    ui.notify(f"Не удалось сохранить: {exc}", type="negative")

            llm_toggle.on_value_change(lambda _: refresh_llm_dirty())
            ollama_url_input.on_value_change(lambda _: refresh_llm_dirty())
            expand_select.on_value_change(lambda _: refresh_llm_dirty())
            rag_select.on_value_change(lambda _: refresh_llm_dirty())
            with action_row:
                with ui.row().classes("rag-dirty-actions-inner"):
                    ui.button("Отменить", icon="close", on_click=reset_llm_settings).props("flat dense")
                    ui.button("Сохранить настройки нейросети", icon="save", on_click=save_llm_settings).props("outline dense")

    def render_admin_search_aliases() -> None:
        telemetry = _get_telemetry(state)
        with ui.column().classes("rag-card w-full p-4 gap-3"):
            ui.label("Синонимы поиска").classes("text-xl font-semibold")
            ui.label(
                "Группы расширяют запросы без переиндексации: например, «реквизиты» ищет карточки предприятия и расчетные счета."
            ).classes("rag-meta")

            groups = telemetry.list_search_alias_groups() if hasattr(telemetry, "list_search_alias_groups") else []
            with ui.expansion("Добавить группу", icon="add", value=False).classes("w-full"):
                new_key = ui.input("Ключ группы", placeholder="company_card").props("dense outlined").classes("w-full")
                new_label = ui.input("Название", placeholder="Карточка предприятия").props("dense outlined").classes("w-full")
                new_aliases = ui.textarea("Синонимы, по одному на строку").props("dense outlined autogrow").classes("w-full")
                new_negative = ui.textarea("Исключения, по одному на строку").props("dense outlined autogrow").classes("w-full")

                def add_group() -> None:
                    label = str(new_label.value or "").strip()
                    key = str(new_key.value or label).strip()
                    aliases = [x.strip() for x in str(new_aliases.value or "").splitlines() if x.strip()]
                    negatives = [x.strip() for x in str(new_negative.value or "").splitlines() if x.strip()]
                    try:
                        telemetry.save_search_alias_group(key=key, label=label or key, aliases=aliases, negative_aliases=negatives)
                        _log_app_event(state, "settings", "search_alias_add", details={"key": key, "label": label})
                        ui.notify("Группа синонимов добавлена.", type="positive")
                        render_fn()
                    except Exception as exc:
                        ui.notify(f"Не удалось сохранить: {exc}", type="negative")

                ui.button("Добавить группу", icon="save", on_click=add_group).props("outline")

            for group in groups:
                group_key = str(group.get("key") or "")
                alias_text = "\n".join(str(a.get("alias") or "") for a in group.get("aliases") or [])
                negative_text = "\n".join(str(x) for x in group.get("negative_aliases") or [])
                with ui.expansion(str(group.get("label") or group_key), icon="travel_explore", value=False).classes("w-full"):
                    initial_group = {
                        "label": str(group.get("label") or ""),
                        "aliases": alias_text,
                        "negative": negative_text,
                    }
                    label_input = ui.input("Название", value=str(group.get("label") or "")).props("dense outlined").classes("w-full")
                    aliases_input = ui.textarea("Синонимы", value=alias_text).props("dense outlined autogrow").classes("w-full")
                    negative_input = ui.textarea("Исключения", value=negative_text).props("dense outlined autogrow").classes("w-full")
                    ui.label(f"Ключ: {group_key} · обновлено: {group.get('updated_at') or '-'}").classes("rag-meta")

                    def save_group(
                        key: str = group_key,
                        label_ref: Any = label_input,
                        aliases_ref: Any = aliases_input,
                        negative_ref: Any = negative_input,
                    ) -> None:
                        aliases = [x.strip() for x in str(aliases_ref.value or "").splitlines() if x.strip()]
                        negatives = [x.strip() for x in str(negative_ref.value or "").splitlines() if x.strip()]
                        telemetry.save_search_alias_group(
                            key=key,
                            label=str(label_ref.value or key),
                            aliases=aliases,
                            negative_aliases=negatives,
                        )
                        initial_group.update({
                            "label": str(label_ref.value or key),
                            "aliases": str(aliases_ref.value or ""),
                            "negative": str(negative_ref.value or ""),
                        })
                        group_actions.set_visibility(False)
                        _log_app_event(state, "settings", "search_alias_save", details={"key": key})
                        ui.notify("Синонимы сохранены.", type="positive")
                        render_fn()

                    def delete_group(key: str = group_key) -> None:
                        telemetry.delete_search_alias_group(key=key)
                        _log_app_event(state, "settings", "search_alias_delete", details={"key": key})
                        ui.notify("Группа удалена.", type="positive")
                        render_fn()

                    group_actions = ui.row().classes("rag-dirty-actions")
                    group_actions.set_visibility(False)

                    def current_group_values() -> Dict[str, Any]:
                        return {
                            "label": str(label_input.value or group_key),
                            "aliases": str(aliases_input.value or ""),
                            "negative": str(negative_input.value or ""),
                        }

                    def refresh_group_dirty() -> None:
                        group_actions.set_visibility(current_group_values() != initial_group)

                    def reset_group_fields() -> None:
                        label_input.set_value(initial_group["label"])
                        aliases_input.set_value(initial_group["aliases"])
                        negative_input.set_value(initial_group["negative"])
                        group_actions.set_visibility(False)

                    label_input.on_value_change(lambda _: refresh_group_dirty())
                    aliases_input.on_value_change(lambda _: refresh_group_dirty())
                    negative_input.on_value_change(lambda _: refresh_group_dirty())

                    with ui.row().classes("gap-2"):
                        ui.button("Удалить", icon="delete", on_click=delete_group).props("flat dense")
                    with group_actions:
                        with ui.row().classes("rag-dirty-actions-inner"):
                            ui.button("Отменить", icon="close", on_click=reset_group_fields).props("flat dense")
                            ui.button("Сохранить", icon="save", on_click=save_group).props("outline dense")

            candidates = telemetry.suggest_search_alias_candidates(limit=12) if hasattr(telemetry, "suggest_search_alias_candidates") else []
            with ui.expansion("Кандидаты из истории поиска", icon="psychology", value=False).classes("w-full"):
                if not candidates:
                    ui.label("Пока нет кандидатов. Они появятся после положительных реакций на результаты поиска.").classes("rag-meta")

                def _quick_add_alias(cq: str, cp: str) -> None:
                    import re as _re
                    _key = _re.sub(r"[^a-z0-9]+", "_", cq.lower()).strip("_") or "alias"
                    try:
                        telemetry.save_search_alias_group(
                            key=_key, label=cq, aliases=[cq, cp], source="analytics"
                        )
                        _log_app_event(state, "settings", "search_alias_add", details={"key": _key, "from": "admin_candidate"})
                        ui.notify(f"Синоним добавлен: «{cq}» = «{cp}»", type="positive")
                        render_fn()
                    except Exception as exc:
                        ui.notify(f"Не удалось добавить: {exc}", type="negative")

                for item in candidates:
                    cand_q = str(item.get("query") or "")
                    cand_p = str(item.get("candidate") or "")
                    with ui.row().classes("w-full items-center gap-2"):
                        ui.label(cand_p).classes("font-medium")
                        ui.label(f"запрос: {cand_q}").classes("rag-meta")
                        ui.label(str(item.get("title") or item.get("path") or "")).classes("rag-path flex-1")
                        ui.button("Добавить", icon="add", on_click=lambda cq=cand_q, cp=cand_p: _quick_add_alias(cq, cp)).props("flat dense no-caps").classes("text-xs")


    # ── Settings screen body ──────────────────────────────────────────────
    auth_db = _get_auth_db(state)

    # ── Форма входа (без боковой панели) ────────────────────────────
    if state.current_user is None:
        ui.label("Настройки").classes("text-2xl font-semibold")
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
                render_fn()

            username_input.on("keyup.enter", lambda _: ui.run_javascript(
                "const ins=document.querySelectorAll('.q-field__native,input[type=password]');"
                "const i=Array.from(ins).findIndex(el=>el===document.activeElement);"
                "if(i>=0&&ins[i+1])ins[i+1].focus();"
            ))
            password_input.on("keyup.enter", lambda _: login())
            ui.button("Войти", icon="login", on_click=login).props("unelevated")
        return

    user = state.current_user
    is_admin = str(user.get("role") or "user") == "admin"

    # ── Реестр секций: (key, icon, label, keywords) ─────────────────
    user_sections: List[tuple] = [
        ("profile",         "person",         "Профиль",                  ["имя", "аккаунт", "профиль"]),
        ("telegram_sync",   "sync",           "Синхронизация Telegram",   ["telegram", "бот", "синхронизация"]),
        ("cloud_sync_user", "sync_alt",       "Cloud Sync",               ["sync", "синхронизация", "папка", "desktop"]),
        ("explorer",        "folder_open",    "Проводник",                ["файлы", "вид", "сортировка"]),
        ("favorites",       "star_border",    "Избранное",                ["закладки"]),
        ("saved_searches",  "bookmark",       "Сохранённые запросы",      ["запросы", "поиск", "сохранено"]),
        ("password",        "key",            "Пароль и выход",           ["смена", "выход", "logout"]),
    ]
    admin_sections: List[tuple] = [
        ("paths",         "storage",        "Пути и Qdrant",          ["каталог", "база", "url", "коллекция"]),
        ("cloud_drive",   "cloud",          "Cloud Drive",            ["cloud", "registry", "bootstrap", "storage", "s3"]),
        ("cloud_sync",    "sync_alt",       "Sync клиент",            ["sync", "синхронизация", "клиент", "desktop", "папка"]),
        ("llm",           "smart_toy",      "Нейросеть",              ["ollama", "модель", "ai", "llm", "rag"]),
        ("aliases",       "travel_explore", "Синонимы поиска",        ["группы", "расширение", "запросы"]),
        ("indexing",      "build",          "Индексация",             ["индекс", "статус", "прогресс"]),
        ("security",      "security",       "Сессии и безопасность",  ["сессии", "системные файлы"]),
        ("users",         "group",          "Пользователи",           ["роль", "статус", "логин"]),
        ("registrations", "person_add",     "Регистрации",            ["заявки", "одобрить"]),
        ("telegram_bot",   "send",           "Telegram бот",           ["бот", "chat id", "telegram"]),
    ]

    active = [state.settings_section]  # сохраняем между ре-рендерами
    q_ref  = [""]

    # ── IDE-лейаут ───────────────────────────────────────────────────
    with ui.row().classes("w-full gap-0 items-start"):

        # Левая боковая панель
        with ui.column().classes("flex-none gap-1").style(
            "width:220px; min-width:220px; border-right:1px solid #e5e7eb; padding-right:12px; margin-right:16px"
        ):
            ui.label("Настройки").classes("text-xl font-semibold mb-2")
            search_box = ui.input(
                placeholder="Поиск настроек…",
                on_change=lambda e: (q_ref.__setitem__(0, str(e.value or "").lower()), render_nav()),
            ).props("dense outlined clearable").classes("w-full")

            nav_col = ui.column().classes("w-full gap-0")

        # Правая область контента
        content_col = ui.column().classes("flex-1 gap-3 min-w-0")

    # ── Навигация ────────────────────────────────────────────────────
    def _visible(entry: tuple) -> bool:
        q = q_ref[0]
        if not q:
            return True
        key, icon, label, kws = entry
        return q in label.lower() or any(q in kw.lower() for kw in kws)

    def render_nav() -> None:
        nav_col.clear()
        with nav_col:
            groups: List[tuple] = [("", user_sections)]
            if is_admin:
                groups.append(("Администратор", admin_sections))
            for group_label, sections in groups:
                filtered = [s for s in sections if _visible(s)]
                if not filtered:
                    continue
                if group_label:
                    ui.label(group_label.upper()).classes(
                        "text-xs text-gray-400 font-semibold mt-3 mb-1 px-2"
                    )
                for key, icon, label, _ in filtered:
                    is_active = active[0] == key
                    bg = "background:#eef2ff;" if is_active else ""
                    with ui.row().classes("w-full items-center gap-2 px-2 py-1 rounded cursor-pointer").style(
                        bg + "user-select:none"
                    ).on("click", lambda k=key: navigate(k)):
                        ui.icon(icon, size="16px").classes(
                            "text-indigo-600" if is_active else "text-gray-400"
                        )
                        ui.label(label).classes(
                            "text-sm font-medium text-indigo-700" if is_active else "text-sm text-gray-700"
                        )

    # ── Контент секции ───────────────────────────────────────────────
    def render_section() -> None:
        content_col.clear()
        with content_col:
            sec = active[0]

            if sec == "profile":
                with ui.column().classes("rag-card w-full p-4 gap-3"):
                    initial_profile = {
                        "display_name": str(user.get("display_name") or ""),
                    }
                    ui.label("Профиль").classes("text-xl font-semibold")
                    ui.label(
                        f"Логин: {user.get('username')} · роль: {user.get('role')} · статус: {user.get('status')}"
                    ).classes("rag-meta")
                    disp_in = ui.input("Имя", value=str(user.get("display_name") or "")).props("dense outlined").classes("w-full")
                    linked_tg_id = str(user.get("telegram_chat_id") or "").strip()
                    linked_tg_un = str(user.get("telegram_username") or "").strip()

                    def save_profile() -> None:
                        auth_db.update_profile(
                            username=str(user.get("username") or ""),
                            display_name=str(disp_in.value or ""),
                            telegram_chat_id=linked_tg_id,
                            telegram_username=linked_tg_un,
                        )
                        initial_profile["display_name"] = str(disp_in.value or "")
                        _refresh_current_user(state)
                        profile_actions.set_visibility(False)
                        ui.notify("Профиль сохранён.", type="positive")

                    profile_actions = ui.row().classes("rag-dirty-actions")
                    profile_actions.set_visibility(False)

                    def refresh_profile_dirty() -> None:
                        profile_actions.set_visibility(str(disp_in.value or "") != initial_profile["display_name"])

                    def reset_profile() -> None:
                        disp_in.set_value(initial_profile["display_name"])
                        profile_actions.set_visibility(False)

                    disp_in.on_value_change(lambda _: refresh_profile_dirty())
                    with profile_actions:
                        with ui.row().classes("rag-dirty-actions-inner"):
                            ui.button("Отменить", icon="close", on_click=reset_profile).props("flat dense")
                            ui.button("Сохранить профиль", icon="save", on_click=save_profile).props("outline dense")

            elif sec == "telegram_sync":
                with ui.column().classes("rag-card w-full p-4 gap-3"):
                    linked_tg_id = str(user.get("telegram_chat_id") or "").strip()
                    linked_tg_un = str(user.get("telegram_username") or "").strip()
                    linked_label = f"@{linked_tg_un}" if linked_tg_un else linked_tg_id
                    ui.label("Синхронизация Telegram").classes("text-xl font-semibold")
                    ui.label("Связь нужна для входа через Telegram и команд бота от вашего имени.").classes("rag-meta")
                    with ui.row().classes("w-full items-center gap-2"):
                        ui.icon("check_circle" if linked_tg_id else "radio_button_unchecked").classes(
                            "text-green-600" if linked_tg_id else "text-gray-400"
                        )
                        ui.label(f"Привязан: {linked_label}" if linked_tg_id else "Telegram не привязан").classes("font-medium")

                    def bind_tg() -> None:
                        bot_link = str(state.cfg.get("telegram_bot_link") or "").strip()
                        if not bot_link:
                            ui.notify("В config.json не задан telegram_bot_link.", type="warning")
                            return
                        out = auth_db.create_telegram_link_token(username=str(user.get("username") or ""))
                        if not out.get("ok"):
                            ui.notify(f"Ошибка: {out.get('reason')}", type="negative")
                            return
                        link = _telegram_deeplink(bot_link, "link", str(out.get("token") or ""))
                        if not link:
                            ui.notify("Не удалось создать ссылку привязки.", type="negative")
                            return
                        ui.run_javascript(
                            "(() => {"
                            f"const url = {json.dumps(link)};"
                            "const w = window.open(url, '_blank', 'noopener,noreferrer');"
                            "if (!w) { window.location.href = url; }"
                            "})();"
                        )
                        ui.notify("Откройте Telegram и подтвердите привязку.", type="positive")

                    def unlink_tg() -> None:
                        if not linked_tg_id:
                            return
                        auth_db.unlink_telegram_chat_id(linked_tg_id)
                        _refresh_current_user(state)
                        ui.notify("Telegram отвязан.", type="warning")
                        render_section()

                    with ui.row().classes("gap-2"):
                        ui.button("Синхронизировать", icon="link", on_click=bind_tg).props("outline")
                        if linked_tg_id:
                            ui.button("Отвязать", icon="link_off", on_click=unlink_tg).props("flat color=negative")

            elif sec == "cloud_sync_user":
                with ui.column().classes("rag-card w-full p-4 gap-3"):
                    ui.label("Cloud Sync").classes("text-xl font-semibold")
                    ui.label(
                        "Desktop sync-клиент синхронизирует выбранные папки вашего компьютера "
                        "с Cloud Drive. Настройте пары папок и политику конфликтов."
                    ).classes("rag-meta")
                    cd_enabled2 = bool(state.cfg.get("cloud_drive_enabled"))
                    if not cd_enabled2:
                        with ui.element("div").classes("cd-empty-state w-full py-4"):
                            ui.icon("cloud_off", size="28px").classes("opacity-30")
                            ui.label("Cloud Drive не включён — обратитесь к администратору.").classes("text-center")
                    else:
                        svc = _cd_get_service(state.cfg)
                        username = str(user.get("username") or "").strip().lower()
                        clients = svc.list_sync_clients(username=username, limit=20) if svc is not None else []
                        pairs = svc.list_sync_pairs(username=username) if svc is not None else []
                        conflicts = svc.list_sync_conflicts(username=username, status="open", limit=20) if svc is not None else []
                        connected = any(str(c.get("status") or "") == "online" for c in clients)
                        with ui.row().classes("w-full items-center gap-3 p-3 rag-explorer-item"):
                            ui.icon("sync" if connected else "sync_disabled", size="24px").classes(
                                "text-green-500" if connected else "text-slate-400"
                            )
                            with ui.column().classes("flex-1 gap-0"):
                                ui.label("Sync-клиент подключён" if connected else "Sync-клиент не подключён").classes("font-medium")
                                ui.label(
                                    f"Клиентов: {len(clients)} · папок: {len(pairs)} · открытых конфликтов: {len(conflicts)}"
                                ).classes("rag-meta text-xs")
                            ui.badge("online" if connected else "offline", color="positive" if connected else "grey-4").classes("text-xs")

                        with ui.row().classes("w-full justify-end mt-1"):
                            async def _open_user_install_dlg() -> None:
                                try:
                                    _origin = await ui.run_javascript("window.location.origin")
                                except Exception:
                                    _origin = "http://localhost:8080"
                                _tok = str(app.storage.user.get("auth_token") or "…").strip()
                                _cmd = f"python rag_sync_client.py --server {_origin} --token {_tok}"
                                with ui.dialog() as _udlg, ui.card().classes("p-5 gap-4 w-full max-w-lg"):
                                    ui.label("Установка sync-клиента").classes("text-base font-semibold")
                                    ui.label(
                                        "Скачайте установщик и запустите на своём компьютере. "
                                        "Сервер и токен уже вписаны в установщик."
                                    ).classes("rag-meta text-sm")
                                    ui.separator()
                                    ui.label("Шаг 1 — скачать").classes("font-semibold text-sm")
                                    _base_url = f"{_origin}/api/cloud-drive/sync/client-download?auth_token={_tok}"
                                    with ui.row().classes("gap-3 items-center flex-wrap"):
                                        ui.link("Windows MSI", target=f"{_base_url}&format=msi", new_tab=True).classes("rag-path text-sm")
                                        ui.label("·").classes("rag-meta")
                                        ui.link("Windows EXE (установщик)", target=f"{_base_url}&format=exe", new_tab=True).classes("rag-path text-sm")
                                        ui.label("·").classes("rag-meta")
                                        ui.link("Python .py (Linux/Mac)", target=f"{_base_url}&format=py", new_tab=True).classes("rag-meta text-sm")
                                    ui.label(
                                        "MSI: тихая установка, поддержка групповых политик. "
                                        "EXE: мастер настройки с полями сервера и токена."
                                    ).classes("rag-meta text-xs")
                                    ui.separator()
                                    ui.label("Для Python-скрипта: запустить").classes("font-semibold text-sm")
                                    with ui.row().classes("w-full gap-1 items-center"):
                                        _run2 = ui.input(value=_cmd).props("readonly dense outlined").classes("flex-1 font-mono text-xs")
                                        ui.button(icon="content_copy", on_click=lambda: ui.run_javascript(f"navigator.clipboard.writeText({repr(_run2.value)})" )).props("flat dense round").tooltip("Копировать")
                                    ui.button("Закрыть", on_click=_udlg.close).props("flat dense")
                                _udlg.open()
                            ui.button("Скачать клиент", icon="download", on_click=_open_user_install_dlg).props("outline dense").classes("text-indigo-400")

                        ui.separator()
                        ui.label("Мои папки синхронизации").classes("font-semibold")

                        if not pairs:
                            with ui.element("div").classes("cd-empty-state w-full py-3"):
                                ui.icon("folder_copy", size="24px").classes("opacity-30")
                                ui.label("Нет настроенных папок для синхронизации.").classes("text-center")
                                ui.label("Добавьте пары папок в Настройках → Sync клиент (доступно администраторам).").classes("text-center rag-meta text-xs")
                        else:
                            with ui.column().classes("w-full gap-1"):
                                for _pair in pairs:
                                    with ui.row().classes("rag-explorer-item w-full p-2 items-center gap-3"):
                                        ui.icon("folder_copy", size="20px").classes("text-indigo-400")
                                        with ui.column().classes("flex-1 gap-0"):
                                            ui.label(str(_pair.get("local_path") or "(не задано)")).classes("text-sm font-medium")
                                            ui.label(f"→ Cloud Drive: {_pair.get('cloud_path') or 'Корень'}").classes("rag-meta text-xs")
                                        _pol = str(_pair.get("conflict_policy") or "ask")
                                        _pol_lbl = {"ask": "Спрашивать", "cloud_wins": "Cloud Drive", "local_wins": "Локальная", "newest_wins": "Новая"}.get(_pol, _pol)
                                        ui.badge(_pol_lbl, color="grey-4").classes("text-xs")
                        if conflicts:
                            ui.separator()
                            ui.label("Открытые конфликты").classes("font-semibold")
                            for conflict in conflicts:
                                with ui.row().classes("rag-explorer-item w-full p-2 items-center gap-2"):
                                    ui.icon("merge", size="18px").classes("text-orange-500")
                                    ui.label(str(conflict.get("path") or conflict.get("cloud_path") or "")).classes("text-sm flex-1 truncate")
                                    ui.badge(str(conflict.get("conflict_type") or "conflict"), color="warning").classes("text-xs")

            elif sec == "explorer":
                with ui.column().classes("rag-card w-full p-4 gap-3"):
                    ui.label("Проводник").classes("text-xl font-semibold")
                    ui.label(
                        f"Вид: {state.explorer_view} · сортировка: {state.explorer_sort} · "
                        f"{'убывание' if state.explorer_desc else 'возрастание'} · тип: {state.explorer_ext}"
                    ).classes("rag-meta")

                    def reset_explorer() -> None:
                        auth_db.reset_user_settings(username=str(user.get("username") or ""))
                        state.explorer_view = "Таблица"
                        state.explorer_sort = "По имени"
                        state.explorer_desc = False
                        state.explorer_ext = "Все"
                        _log_app_event(state, "settings", "reset_explorer")
                        ui.notify("Настройки проводника сброшены.", type="positive")
                        render_section()

                    ui.button("Сбросить настройки проводника", icon="restart_alt", on_click=reset_explorer).props("outline")

            elif sec == "favorites":
                with ui.column().classes("rag-card w-full p-4 gap-3"):
                    ui.label("Избранное").classes("text-xl font-semibold")
                    if not state.favorites:
                        ui.label("Закладок пока нет. Добавьте файл или папку звёздочкой в проводнике.").classes("rag-meta")
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
                                ui.button("Открыть", on_click=lambda p=fav_path: open_file_viewer(p)).props("outline dense")
                            ui.button(icon="delete", on_click=lambda p=fav_path: (
                                _toggle_favorite(state, p), render_section()
                            )).props("flat round dense")

            elif sec == "saved_searches":
                with ui.column().classes("rag-card w-full p-4 gap-3"):
                    ui.label("Сохранённые запросы").classes("text-xl font-semibold")
                    if not state.saved_searches:
                        ui.label("Нет сохранённых запросов. Нажмите на закладку рядом с результатами поиска, чтобы сохранить запрос.").classes("rag-meta")
                    for ss in state.saved_searches:
                        ss_q = str(ss.get("query") or "")
                        ss_label = str(ss.get("label") or ss_q)
                        with ui.row().classes("w-full items-center gap-2"):
                            ui.icon("bookmark", size="16px").classes("text-amber-500 shrink-0")
                            with ui.column().classes("flex-1 min-w-0 gap-0"):
                                ui.label(ss_label).classes("text-sm font-medium truncate")
                                if ss_label != ss_q:
                                    ui.label(ss_q).classes("rag-path text-xs truncate")
                            ui.button(icon="search", on_click=query_handler(ss_q), color=None).props("flat round dense").tooltip("Выполнить этот запрос")
                            ui.button(icon="delete", on_click=lambda q=ss_q: (
                                _toggle_saved_search(state, q), render_section()
                            ), color=None).props("flat round dense")

            elif sec == "password":
                with ui.column().classes("rag-card w-full p-4 gap-3"):
                    ui.label("Смена пароля").classes("text-xl font-semibold")
                    old_pw = ui.input("Текущий пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")
                    new_pw = ui.input("Новый пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")
                    new_pw2 = ui.input("Повторите пароль", password=True, password_toggle_button=True).props("dense outlined").classes("w-full")
                    _focus_next_js = (
                        "const ins=document.querySelectorAll('.q-field__native,input[type=password]');"
                        "const i=Array.from(ins).findIndex(el=>el===document.activeElement);"
                        "if(i>=0&&ins[i+1])ins[i+1].focus();"
                    )
                    old_pw.on("keyup.enter", lambda _: ui.run_javascript(_focus_next_js))
                    new_pw.on("keyup.enter", lambda _: ui.run_javascript(_focus_next_js))

                    def change_pw() -> None:
                        if str(new_pw.value or "") != str(new_pw2.value or ""):
                            ui.notify("Пароли не совпадают.", type="warning")
                            return
                        ok = auth_db.change_password(
                            username=str(user.get("username") or ""),
                            old_password=str(old_pw.value or ""),
                            new_password=str(new_pw.value or ""),
                        )
                        if ok:
                            _refresh_current_user(state)
                        ui.notify("Пароль изменён." if ok else "Не удалось изменить пароль.",
                                  type="positive" if ok else "negative")

                    new_pw2.on("keyup.enter", lambda _: change_pw())
                    with ui.row().classes("gap-2"):
                        ui.button("Сменить пароль", icon="key", on_click=change_pw).props("outline")
                        ui.button("Выйти", icon="logout", on_click=do_logout).props("flat")

            elif sec == "paths":
                render_admin_path_settings()
            elif sec == "cloud_drive":
                render_admin_cloud_drive_settings()
            elif sec == "cloud_sync":
                render_admin_cloud_sync_settings()
            elif sec == "llm":
                render_admin_llm_settings()
            elif sec == "aliases":
                render_admin_search_aliases()
            elif sec == "indexing":
                render_index_dashboard()
            elif sec == "security":
                render_admin_security_settings(auth_db)
            elif sec == "users":
                render_admin_users(auth_db)
            elif sec == "registrations":
                render_admin_registration_requests(auth_db)
            elif sec == "telegram_bot":
                with ui.column().classes("rag-card w-full p-4 gap-3"):
                    enabled = bool(state.cfg.get("telegram_enabled"))
                    token_set = bool(str(state.cfg.get("telegram_bot_token") or "").strip())
                    bot_link = str(state.cfg.get("telegram_bot_link") or "").strip()
                    ui.label("Управление Telegram ботом").classes("text-xl font-semibold")
                    with ui.row().classes("gap-2 flex-wrap"):
                        ui.label(f"Статус: {'включен' if enabled else 'выключен'}").classes("rag-chip")
                        ui.label(f"Токен: {'задан' if token_set else 'не задан'}").classes("rag-chip")
                        ui.label(f"Ссылка: {'задана' if bot_link else 'не задана'}").classes("rag-chip")
                    if bot_link:
                        ui.link("Открыть бота", bot_link, new_tab=True).classes("rag-link")
                render_admin_telegram_chats(auth_db)

    def navigate(key: str) -> None:
        active[0] = key
        state.settings_section = key
        render_nav()
        render_section()

    render_nav()
    render_section()

def render_telegram_screen() -> None:
    enabled = bool(state.cfg.get("telegram_enabled"))
    token_set = bool(str(state.cfg.get("telegram_bot_token") or "").strip())
    with ui.column().classes("rag-card w-full p-4 gap-2"):
        ui.label(f"Статус: {'включен' if enabled else 'выключен'}").classes("text-lg font-semibold")
        ui.label(f"Токен: {'задан' if token_set else 'не задан'}").classes("rag-meta")
        bot_link = str(state.cfg.get("telegram_bot_link") or "").strip()
        if bot_link:
            ui.link("Открыть бота", bot_link, new_tab=True)

# ── Analytics / stats screen ───────────────────────────────────────────
