"""Dispatcher directories and immutable waybill history in the existing UI shell."""

from nicegui import run, ui

from rag_catalog.core.work_shifts import REFERENCE_LABELS, STATUS_LABELS

FIELD_LABELS = dict(employee="Логин", employee_name="Сотрудник", work_date="Дата", shift_number="Смена",
                    organization="Организация", equipment="Техника", workplace="Объект", partner="Контрагент",
                    waybill_number="Путевой лист", comment="Комментарий", work_minutes="Работа, мин",
                    break_minutes="Перерывы, мин", status="Статус", deleted="Удалена")


def show_shift_history(event):
    for key, (before, after) in event["changes"].items():
        if key == "status":
            before, after = STATUS_LABELS.get(before, before), STATUS_LABELS.get(after, after)
        ui.label(f"{FIELD_LABELS.get(key, key)}: {before if before is not None else '—'} → {after}").classes("break-words w-full")


async def show_shift_administration(state, journal):
    if journal is None:
        return
    offset = 0

    async def call(method, *args, **kwargs):
        return await run.io_bound(method, state.auth_token, *args, **kwargs)

    def report(exc):
        ui.notify(str(exc) if isinstance(exc, (ValueError, PermissionError)) else "Не удалось выполнить операцию", type="negative")

    async def reference_editor(row=None):
        row = row or {}
        with ui.dialog() as editor, ui.card().classes("w-full max-w-lg").style("background: var(--rag-surface-strong)"):
            ui.label("Запись справочника").classes("text-lg")
            kind = ui.select(REFERENCE_LABELS, label="Справочник", value=row.get("kind", "organization")).classes("w-full")
            kind.set_enabled(not row)
            name = ui.input("Название", value=row.get("name", "")).props("maxlength=200").classes("w-full")
            archived = ui.checkbox("В архиве", value=bool(row.get("archived")))
            archived.set_visibility(bool(row))

            async def save():
                button.disable()
                try:
                    await call(journal.save_reference, kind.value, name.value, record_id=row.get("id"),
                               revision=row.get("revision"), archived=archived.value)
                    editor.close()
                    await refresh_references()
                except Exception as exc:
                    report(exc)
                finally:
                    button.enable()

            with ui.row():
                ui.button("Отмена", on_click=editor.close).props("flat")
                button = ui.button("Сохранить", icon="save", on_click=save)
        editor.open()

    async def employee_editor(row=None):
        row = row or {}
        if row and not row["editable"]:
            ui.notify("Дополнительные роли меняет администратор в настройках", type="warning")
            return
        with ui.dialog() as editor, ui.card().classes("w-full max-w-lg").style("background: var(--rag-surface-strong)"):
            ui.label("Водитель").classes("text-lg")
            username = ui.input("Логин", value=row.get("username", "")).classes("w-full")
            username.set_enabled(not row)
            name = ui.input("ФИО", value=row.get("display_name", "")).classes("w-full")
            password = ui.input("Временный пароль", password=True, password_toggle_button=True).classes("w-full")
            archived = ui.checkbox("В архиве", value=row.get("status") == "blocked")
            archived.set_visibility(bool(row))

            async def save():
                button.disable()
                try:
                    await call(journal.save_employee, username=username.value or "", display_name=name.value or "",
                               password=password.value or "", create=not row, archived=archived.value)
                    editor.close()
                    await refresh_employees()
                except Exception as exc:
                    report(exc)
                finally:
                    button.enable()

            with ui.row():
                ui.button("Отмена", on_click=editor.close).props("flat")
                button = ui.button("Сохранить", icon="save", on_click=save)
        editor.open()

    async def refresh_references():
        try:
            refs.rows = [dict(r, kind_label=REFERENCE_LABELS[r["kind"]],
                             state="Архив" if r["archived"] else "Активна")
                         for r in await call(journal.references, archived=True)]
            refs.selected = []
            refs.update()
        except Exception as exc:
            report(exc)

    async def refresh_employees():
        try:
            staff.rows = [dict(r, state="Активен" if r["status"] == "active" else "Отключён")
                          for r in await call(journal.employees)]
            staff.selected = []
            staff.update()
        except Exception as exc:
            report(exc)

    async def refresh_history(delta=0):
        nonlocal offset
        offset = max(0, offset + delta * 50)
        try:
            result = await call(journal.audit, offset=offset)
            events.clear()
            with events:
                for event in result["rows"]:
                    action = {**STATUS_LABELS, "create": "Создание", "edit": "Изменение", "delete": "Удаление"}[event["action"]]
                    with ui.expansion(f"№{event['shift_id']} · {event['created_at'][:19]} UTC · {event['actor']} · {action}").classes("w-full"):
                        if event["reason"]:
                            ui.label(event["reason"]).classes("break-words")
                        show_shift_history(event)
            page.set_text(f"{offset + 1 if result['count'] else 0}–{min(offset + 50, result['count'])} из {result['count']}")
            previous.set_enabled(offset > 0)
            following.set_enabled(offset + 50 < result["count"])
        except Exception as exc:
            report(exc)

    def columns(fields):
        return [dict(name=key, field=key, label=label, align="left") for key, label in fields]

    with ui.dialog() as dialog, ui.card().classes("w-full max-w-5xl").style("background: var(--rag-surface-strong)"):
        ui.label("Управление сменами").classes("text-xl")
        with ui.tabs().classes("w-full") as tabs:
            references_tab = ui.tab("Справочники")
            employees_tab = ui.tab("Сотрудники")
            history_tab = ui.tab("История всех путевых")
        with ui.tab_panels(tabs, value=references_tab).classes("w-full").style("max-height: 65vh; overflow-y: auto"):
            with ui.tab_panel(references_tab):
                with ui.row():
                    ui.button("Добавить запись", icon="add", on_click=lambda: reference_editor())
                    ui.button(icon="edit", on_click=lambda: reference_editor(refs.selected[0]) if refs.selected else None).props("flat").tooltip("Изменить или архивировать запись")
                    ui.button(icon="refresh", on_click=refresh_references).props("flat").tooltip("Обновить справочники")
                refs = ui.table(columns=columns((("kind_label", "Справочник"), ("name", "Название"), ("state", "Статус"))),
                                rows=[], row_key="id", selection="single", pagination=15).classes("w-full").props("flat wrap-cells")
            with ui.tab_panel(employees_tab):
                with ui.row():
                    ui.button("Добавить водителя", icon="person_add", on_click=lambda: employee_editor())
                    ui.button(icon="edit", on_click=lambda: employee_editor(staff.selected[0]) if staff.selected else None).props("flat").tooltip("Изменить или архивировать сотрудника")
                    ui.button(icon="refresh", on_click=refresh_employees).props("flat").tooltip("Обновить сотрудников")
                staff = ui.table(columns=columns((("username", "Логин"), ("display_name", "ФИО"), ("state", "Статус"))),
                                 rows=[], row_key="username", selection="single", pagination=15).classes("w-full").props("flat wrap-cells")
            with ui.tab_panel(history_tab):
                ui.button(icon="refresh", on_click=lambda: refresh_history()).props("flat").tooltip("Обновить историю")
                events = ui.column().classes("w-full")
                with ui.row().classes("items-center"):
                    previous = ui.button(icon="chevron_left", on_click=lambda: refresh_history(-1)).props("flat").tooltip("Предыдущая страница")
                    page = ui.label()
                    following = ui.button(icon="chevron_right", on_click=lambda: refresh_history(1)).props("flat").tooltip("Следующая страница")
        ui.button("Закрыть", on_click=dialog.close).props("flat")
    dialog.open()
    await refresh_references()
    await refresh_employees()
    await refresh_history()
