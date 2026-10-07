"""Authenticated work shift journal using the existing application shell."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

from nicegui import run, ui

from rag_catalog.core.roles import can_manage_shifts
from rag_catalog.core.work_shifts import STATUS_LABELS, WorkShiftJournal

from .state import PageState, _get_auth_db
from .work_shifts_admin import show_shift_administration, show_shift_history

logger = logging.getLogger(__name__)


def render_work_shifts(state: PageState) -> None:
    journal = None
    users = {}
    offset = 0
    total = 0
    busy = False
    action_buttons = {}
    admin = can_manage_shifts(state.current_user)

    def update_actions():
        status = table.selected[0]["status"] if table.selected else None
        allowed = {
            "edit": status is not None, "submitted": admin and status == "draft", "delete": admin and status is not None,
            "approved": admin and status in {"draft", "submitted"}, "history": status is not None,
            "draft": admin and status in {"submitted", "approved"},
        }
        for key, button in action_buttons.items():
            button.set_enabled(allowed[key])

    async def call(method, *args, **kwargs):
        return await run.io_bound(method, state.auth_token, *args, **kwargs)

    def error(exc):
        if isinstance(exc, (ValueError, PermissionError)):
            message = str(exc)
        else:
            logger.exception("Work shift operation failed")
            message = "Не удалось выполнить операцию. Повторите позже."
        ui.notify(message, type="negative")

    def filters():
        return dict(date_from=start.value or "", date_to=end.value or "",
                    employee=employee_filter.value or "", status=status_filter.value or "")

    async def refresh(reset=False):
        nonlocal offset, total, busy
        if journal is None or busy:
            return
        busy = True
        progress.set_visibility(True)
        if reset:
            offset = 0
        try:
            result = await call(journal.list, **filters(), offset=offset)
            total = result["count"]
            if offset and offset >= total:
                offset = max(0, ((total - 1) // 50) * 50)
                result = await call(journal.list, **filters(), offset=offset)
            table.rows = [dict(row, hours=round(row["work_minutes"] / 60, 2),
                               breaks=round(row["break_minutes"] / 60, 2),
                               status_label=STATUS_LABELS[row["status"]]) for row in result["rows"]]
            table.selected = []
            table.update()
            summary.set_text(f"Смен: {total} · Отработано: {result['work_minutes'] / 60:g} ч · "
                             f"Перерывы: {result['break_minutes'] / 60:g} ч")
            page_label.set_text(f"{offset + 1 if total else 0}–{min(offset + 50, total)} из {total}")
            previous.set_enabled(offset > 0)
            following.set_enabled(offset + 50 < total)
        except Exception as exc:
            table.rows = []
            table.selected = []
            table.update()
            summary.set_text("Журнал недоступен")
            error(exc)
        finally:
            busy = False
            progress.set_visibility(False)
            update_actions()

    def selected():
        if not table.selected:
            ui.notify("Выберите смену", type="warning")
            return None
        return dict(table.selected[0])

    async def edit(new=False):
        if journal is None:
            return
        row = {} if new else selected()
        if row is None:
            return
        try:
            available_users = await call(journal.users)
            if row.get("employee") and row["employee"] not in available_users:
                available_users[row["employee"]] = row["employee_name"] + " (архив)"
            references = await call(journal.references)
        except Exception as exc:
            error(exc)
            return
        with ui.dialog() as dialog, ui.card().classes("w-full max-w-2xl").style("background: var(--rag-surface-strong)"):
            ui.label("Новая смена" if new else f"Смена №{row['id']}").classes("text-lg font-semibold")
            with ui.element("div").classes("grid grid-cols-1 sm:grid-cols-2 gap-3 w-full").style("max-height: 45vh; overflow-y: auto"):
                inputs = {}
                inputs["employee"] = ui.select(available_users, label="Сотрудник",
                    value=row.get("employee") or (state.current_user or {}).get("username")).classes("w-full")
                inputs["employee"].set_enabled(admin)
                inputs["work_date"] = ui.input("Дата", value=row.get("work_date", date.today().isoformat())).props("type=date")
                inputs["shift_number"] = ui.select({1: "1", 2: "2", 3: "3"}, label="Номер смены", value=row.get("shift_number", 1))
                for key, label in (("organization", "Организация"), ("equipment", "Техника / госномер"),
                                   ("workplace", "Объект"), ("partner", "Контрагент"),
                                   ("waybill_number", "Номер путевого листа")):
                    options = sorted({ref["name"] for ref in references if ref["kind"] == key} | ({row[key]} if row.get(key) else set()))
                    if key != "waybill_number":
                        inputs[key] = ui.select(options, label=label, value=row.get(key) or None,
                                                with_input=True, new_value_mode="add-unique" if admin else None).classes("w-full")
                    else:
                        inputs[key] = ui.input(label, value=row.get(key, "")).props("maxlength=200")
                inputs["hours"] = ui.number("Отработано без перерывов, ч", value=row.get("work_minutes", 0) / 60,
                                            min=0, max=24, step=0.25)
                inputs["breaks"] = ui.number("Перерывы, ч", value=row.get("break_minutes", 0) / 60,
                                             min=0, max=24, step=0.25)
            inputs["comment"] = ui.textarea("Комментарий", value=row.get("comment", "")).classes("w-full").props("maxlength=2000 rows=2")

            async def save():
                save_button.disable()
                try:
                    await call(journal.save, {key: field.value for key, field in inputs.items()},
                               shift_id=row.get("id"), revision=row.get("revision"))
                    dialog.close()
                    await refresh()
                    ui.notify("Смена сохранена", type="positive")
                except Exception as exc:
                    error(exc)
                finally:
                    save_button.enable()

            with ui.row().classes("w-full justify-end"):
                ui.button("Отмена", on_click=dialog.close).props("flat")
                save_button = ui.button("Сохранить" if admin else "Сохранить и отправить", icon="save", on_click=save)
        dialog.open()

    async def change(action):
        row = selected()
        if row is None or journal is None:
            return
        with ui.dialog() as dialog, ui.card().classes("w-full max-w-lg").style("background: var(--rag-surface-strong)"):
            title = {"delete": "Удалить смену?", "draft": "Вернуть в черновик?",
                     "submitted": "Отправить на проверку?", "approved": "Подтвердить смену?"}[action]
            ui.label(title).classes("text-lg font-semibold")
            ui.label(f"{row['work_date']} · {row['employee_name']} · {row['equipment']}").classes("break-words")
            reason = ui.textarea("Причина исправления").classes("w-full").props("maxlength=2000")
            reason.set_visibility(row["status"] == "approved" and action != "delete")

            async def execute():
                confirm.disable()
                try:
                    if action == "delete":
                        await call(journal.delete, row["id"], row["revision"])
                    else:
                        await call(journal.transition, row["id"], row["revision"], action, reason.value or "")
                    dialog.close()
                    await refresh()
                except Exception as exc:
                    error(exc)
                finally:
                    confirm.enable()

            with ui.row():
                ui.button("Отмена", on_click=dialog.close).props("flat")
                confirm = ui.button("Подтвердить", on_click=execute)
        dialog.open()

    async def history():
        row = selected()
        if row is None or journal is None:
            return
        try:
            events = await call(journal.history, row["id"])
            with ui.dialog() as dialog, ui.card().classes("w-full max-w-2xl").style("background: var(--rag-surface-strong)"):
                ui.label(f"История смены №{row['id']}").classes("text-lg font-semibold")
                labels = dict(STATUS_LABELS, create="Создана", edit="Изменена", delete="Удалена")
                with ui.column().classes("w-full max-h-96 overflow-auto"):
                    for event in events:
                        with ui.expansion(f"{event['created_at'][:19]} UTC · {event['actor']} · {labels[event['action']]}").classes("w-full"):
                            ui.label(event["reason"] or "Без комментария")
                            show_shift_history(event)
                ui.button("Закрыть", on_click=dialog.close).props("flat")
            dialog.open()
        except Exception as exc:
            error(exc)

    async def export():
        if journal is None:
            return
        try:
            content = await call(journal.export_csv, **filters())
            ui.download(content, f"shifts-{start.value}-{end.value}.csv", media_type="text/csv")
        except Exception as exc:
            error(exc)

    async def paginate(delta):
        nonlocal offset
        if busy:
            return
        offset = max(0, offset + delta * 50)
        await refresh()

    with ui.column().classes("w-full min-w-0 gap-4"):
        with ui.row().classes("w-full items-center justify-between"):
            ui.label("Смены и путевые листы").classes("text-xl font-semibold")
            with ui.row():
                new_button = ui.button("Новая смена", icon="add", on_click=lambda: edit(new=True))
                new_button.disable()
                ui.button(icon="download", on_click=export).props('flat aria-label="Выгрузить CSV"').tooltip("Выгрузить CSV")
                if admin:
                    ui.button("Управление", icon="manage_accounts", on_click=lambda: show_shift_administration(state, journal))
        with ui.row().classes("w-full items-end gap-3"):
            start = ui.input("С", value=date.today().replace(day=1).isoformat()).props("type=date")
            end = ui.input("По", value=date.today().isoformat()).props("type=date")
            employee_filter = ui.select({"": "Все сотрудники"}, value="", label="Сотрудник").classes("w-60")
            employee_filter.set_visibility(admin)
            status_filter = ui.select({"": "Все статусы", **STATUS_LABELS}, value="", label="Статус").classes("w-48")
            ui.button(icon="refresh", on_click=lambda: refresh(True)).props('flat aria-label="Применить фильтры"').tooltip("Применить фильтры")
        summary = ui.label("Загрузка журнала...").classes("text-sm")
        progress = ui.linear_progress().props("indeterminate").classes("w-full")
        columns = [dict(name=key, field=key, label=label, align="left") for key, label in (
            ("work_date", "Дата"), ("status_label", "Статус"), ("shift_number", "Смена"), ("employee_name", "Сотрудник"),
            ("organization", "Организация"), ("equipment", "Техника"), ("workplace", "Объект"),
            ("waybill_number", "Путевой лист"), ("hours", "Часы"), ("breaks", "Перерывы"))]
        table = ui.table(columns=columns, rows=[], row_key="id", selection="single", on_select=update_actions).classes("w-full max-w-full").props("flat bordered wrap-cells")
        table.add_slot('body-cell-status_label', '''
            <q-td :props="props"><q-badge :color="props.row.status === 'approved' ? 'positive' : 'negative'"
            :label="props.row.status_label" /></q-td>''')
        with ui.row().classes("w-full flex-wrap items-center"):
            action_buttons["edit"] = ui.button(icon="edit", on_click=lambda: edit()).props('flat aria-label="Изменить смену"').tooltip("Изменить смену")
            if admin:
                action_buttons["submitted"] = ui.button("На проверку", icon="send", on_click=lambda: change("submitted")).props("flat")
                action_buttons["approved"] = ui.button("Подтвердить", icon="check", on_click=lambda: change("approved")).props("flat")
                action_buttons["draft"] = ui.button("В черновик", icon="undo", on_click=lambda: change("draft")).props("flat")
            action_buttons["history"] = ui.button(icon="history", on_click=history).props('flat aria-label="История изменений"').tooltip("История изменений")
            if admin:
                action_buttons["delete"] = ui.button(icon="delete", on_click=lambda: change("delete")).props('flat color=negative aria-label="Удалить смену"').tooltip("Удалить смену")
            update_actions()
        with ui.row().classes("w-full items-center justify-end"):
            previous = ui.button(icon="chevron_left", on_click=lambda: paginate(-1)).props("flat")
            previous.tooltip("Предыдущая страница").disable()
            page_label = ui.label("0 из 0")
            following = ui.button(icon="chevron_right", on_click=lambda: paginate(1)).props("flat")
            following.tooltip("Следующая страница").disable()

    async def initialize():
        nonlocal journal, users
        try:
            root = Path(__file__).resolve().parents[3]
            path = Path(state.cfg.get("work_shifts_db_path") or root / "data" / "work_shifts.db")
            if not path.is_absolute():
                path = root / path
            auth = await run.io_bound(_get_auth_db, state)
            journal = await run.io_bound(WorkShiftJournal, path, auth)
            users = await call(journal.users)
            employee_filter.set_options({"": "Все сотрудники", **users})
            new_button.enable()
            await refresh()
        except Exception as exc:
            progress.set_visibility(False)
            summary.set_text("Журнал недоступен")
            error(exc)

    ui.timer(0, initialize, once=True)
