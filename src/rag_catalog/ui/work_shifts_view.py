"""Authenticated work shift journal using the existing application shell."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import date
from pathlib import Path

from nicegui import run, ui

from rag_catalog.core.roles import can_manage_shifts, can_use_shifts
from rag_catalog.core.shift_attachments import ShiftAttachments
from rag_catalog.core.shift_operations import CATEGORIES, printable_shift
from rag_catalog.core.work_shifts import STATUS_LABELS, WorkShiftJournal

from .state import PageState, _get_auth_db, _log_app_event
from .work_shifts_admin import open_directory_editor, show_month_sheet, show_shift_administration, show_shift_history
from .work_shifts_pay import shift_work_editor
from .shift_payroll_view import open_payroll

logger = logging.getLogger(__name__)


def render_work_shifts(state: PageState) -> None:
    journal = None
    users = {}
    offset = 0
    total = 0
    busy = False
    action_buttons = {}
    admin = can_manage_shifts(state.current_user)
    managed_orgs = {}

    def update_actions():
        row = table.selected[0] if table.selected else {}
        status = row.get("status")
        manager = row.get("organization_id") in managed_orgs
        allowed = {
            "edit": status is not None, "submitted": manager and status == "draft", "delete": manager and status is not None,
            "approved": manager and status in {"draft", "submitted"}, "history": status is not None,
            "attachments": status is not None,
            "print": status is not None,
            "draft": manager and status in {"submitted", "approved"},
        }
        for key, button in action_buttons.items():
            button.set_enabled(allowed[key])

    async def call(method, *args, **kwargs):
        started = time.perf_counter()
        ok = False
        try:
            result = await run.io_bound(method, state.auth_token, *args, **kwargs)
            ok = True
            return result
        finally:
            _log_app_event(state, "shifts", method.__name__, ok=ok,
                           details={"duration_ms": round((time.perf_counter() - started) * 1000)})

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
            if not new:
                row = await call(journal.get, row["id"])
            organizations = await call(journal.organizations)
            initial_org = row.get("organization_id") or (next(iter(organizations)) if len(organizations) == 1 else None)
            references = await call(journal.references)
        except Exception as exc:
            error(exc)
            return
        with ui.dialog() as dialog, ui.card().classes("w-full max-w-2xl").style("background: var(--rag-surface-strong)") as editor_card:
            ui.label("Новая смена" if new else f"Смена №{row['id']}").classes("text-lg font-semibold")
            with ui.element("div").classes("grid grid-cols-1 sm:grid-cols-2 gap-3 w-full").style("max-height: 45vh; overflow-y: auto"):
                inputs = {}
                inputs["entry_type"] = ui.select({"vehicle": "Смена с техникой", "attendance": "Явка без техники"},
                    label="Вид записи", value=row.get("entry_type","vehicle")).classes("w-full")
                org = ui.select(organizations, label="Организация журнала", value=initial_org).classes("w-full")
                org.set_enabled(new and len(organizations) > 1)
                org.set_visibility(len(organizations) > 1)
                add_actions = {}
                people_options, permits = {}, {}
                pairing = False

                def picker(key, label, action):
                    field = ui.select({}, label=label, with_input=True,clearable=True).classes("w-full min-w-0")
                    inputs[key] = field

                    def create():
                        scope = org.value
                        if scope not in managed_orgs:
                            return
                        field.run_method("hidePopup")

                        async def added(saved_org, value):
                            if org.value != saved_org:
                                return
                            if key == "employee":
                                people = await call(journal.users, organization_id=saved_org)
                                day = inputs["work_date"].value
                                marks = await call(journal.qualifications,saved_org,work_date=day)
                                if org.value == saved_org:
                                    people_options.update(people)
                                    permits.update(marks)
                                    category.value = None
                                    field.set_options(people, value=value)
                                    users.update(people)
                                    employee_filter.set_options({"": "Все сотрудники", **users})
                                    inputs["assigned_employee"].set_options(people,value=inputs["assigned_employee"].value)
                                    pair("employee")
                            else:
                                refs = await call(journal.references)
                                if org.value == saved_org:
                                    references[:] = refs
                                    set_reference(key, value)
                                    pair(key)

                        with editor_card:
                            open_directory_editor(journal, call, {scope: organizations[scope]}, key, added, error)

                    buttons = []
                    with field.add_slot("append"):
                        buttons.append(ui.button(icon="add", on_click=create).props(
                            f'flat dense round aria-label="{action}"').on("click.stop", js_handler="() => {}").tooltip(action))
                    for slot in ("after-options", "no-option"):
                        with field.add_slot(slot):
                            buttons.append(ui.button(action, icon="add", on_click=create).props("flat no-caps").classes("w-full"))
                    add_actions[key] = buttons
                    return field

                category = ui.select(CATEGORIES, label="Категория сотрудника", clearable=True).classes("w-full")
                picker("employee", "Сотрудник", "Добавить сотрудника")
                inputs["work_date"] = ui.input("Дата", value=row.get("work_date", date.today().isoformat())).props("type=date")
                inputs["shift_number"] = ui.select({1: "1", 2: "2", 3: "3"}, label="Номер смены", value=row.get("shift_number", 1))
                for key, label, action in (("organization", "Организация в путевом листе", "Добавить организацию"),
                                           ("equipment", "Техника / госномер", "Добавить технику"),
                                           ("workplace", "Объект", "Добавить объект"), ("partner", "Контрагент", "Добавить контрагента")):
                    picker(key, label, action)
                inputs["waybill_number"] = ui.input("Номер путевого листа", value=row.get("waybill_number", "")).props("maxlength=200")
                inputs["hours"] = ui.number("Отработано без перерывов, ч", value=row.get("work_minutes", 0) / 60,
                                            min=0, max=24, step=0.25)
                inputs["breaks"] = ui.number("Перерывы, ч", value=row.get("break_minutes", 0) / 60,
                                             min=0, max=24, step=0.25)
                for k,label in (("time_from","Начало смены"),("time_to","Окончание смены")):
                    inputs[k] = ui.input(label, value=row.get(k,"")).props("type=time")
            with ui.expansion("Замена по назначению", icon="swap_horiz",
                              value=bool(row.get("assigned_equipment") or row.get("assigned_employee"))).classes("w-full") as substitution:
                inputs["assigned_employee"] = ui.select({}, label="Первоначально назначен",with_input=True,clearable=True).classes("w-full")
                inputs["assigned_equipment"] = ui.select([], label="Первоначально назначенная техника",with_input=True,clearable=True).classes("w-full")
            refresh_rates, work_lines = shift_work_editor(journal, call, org, inputs, row, error)
            warning = ui.label().classes("text-negative font-medium").props("role=status")
            warning.set_visibility(False)
            no_match = ui.label().classes("text-warning text-sm").props("role=status")
            no_match.set_visibility(False)
            checks = 0

            async def check_overlap():
                nonlocal checks
                checks += 1
                version = checks
                warning.set_visibility(False)
                await asyncio.sleep(.2)
                if version != checks or not dialog.value or org.value not in managed_orgs:
                    return
                keys = ("employee","assigned_employee","equipment","assigned_equipment","work_date","shift_number","time_from","time_to")
                values = {key:inputs[key].value for key in keys}
                if not values["work_date"] or bool(values["time_from"]) != bool(values["time_to"]):
                    return
                try:
                    clashes = await call(journal.conflict_preview,organization_id=org.value,shift_id=row.get("id"),**values)
                    if version == checks and dialog.value:
                        warning.set_text("Пересечение смен: " + clashes)
                        warning.set_visibility(bool(clashes))
                except ValueError:
                    pass  # The save action validates incomplete dates and times.

            for key in ("employee","assigned_employee","equipment","assigned_equipment","work_date","shift_number","time_from","time_to"):
                inputs[key].on_value_change(check_overlap)
            inputs["comment"] = ui.textarea("Комментарий", value=row.get("comment", "")).classes("w-full").props("maxlength=2000 rows=2")

            async def save():
                save_button.disable()
                try:
                    if not org.value:
                        raise ValueError("Выберите организацию")
                    if not inputs["employee"].value:
                        raise ValueError("Выберите фактического исполнителя")
                    values = {key: field.value for key, field in inputs.items()}
                    await call(journal.save, dict(values, organization_id=org.value, work_lines=work_lines()),
                               shift_id=row.get("id"), revision=row.get("revision"),require_equipment_category=True)
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
            def set_reference(key, value):
                field = inputs[key]
                options = {r["name"] for r in references if r["organization_id"] == org.value and r["kind"] == key}
                if key == "organization" and org.value:
                    options.add(organizations[org.value])
                    value = value or organizations[org.value]
                if value:
                    options.add(value)
                field.set_options(sorted(options), value=value)
                field.set_enabled(bool(org.value))
                if key == "organization":
                    field.set_visibility(org.value == "default")
                if key == "equipment":
                    assigned = inputs["assigned_equipment"]
                    current = assigned.value or row.get("assigned_equipment") or None
                    assigned.set_options(sorted(options | ({current} if current else set())), value=current)

            def set_entry_type():
                vehicle = inputs["entry_type"].value == "vehicle"
                for key in ("equipment","assigned_equipment","waybill_number"):
                    inputs[key].set_visibility(vehicle)
                    if not vehicle:
                        inputs[key].set_value("" if key=="waybill_number" else None)

            def pair(prefer="employee"):
                nonlocal pairing
                if pairing:
                    return
                pairing = True
                try:
                    driver, machine = inputs["employee"],inputs["equipment"]
                    who,which = driver.value,machine.value
                    opts = {k:v for k,v in people_options.items() if not category.value or category.value in permits.get(k,())}
                    vehicle = inputs["entry_type"].value=="vehicle"
                    eqs = {r["name"]:r for r in references if r["kind"]=="equipment" and r["organization_id"]==org.value
                           and r["equipment_category"] and r["licence_kind"] and r["licence_mark"]}
                    historic = not new and inputs["work_date"].value==row["work_date"]
                    if historic and row["equipment"]:
                        eqs.setdefault(row["equipment"],{})
                    def allowed(person,name):
                        if historic and (person,name)==(row["employee"],row["equipment"]):
                            return True
                        r = eqs.get(name)
                        rights = permits.get(person,())
                        return bool(r and r["equipment_category"] in rights and r["licence_kind"]+":"+r["licence_mark"] in rights)
                    who = who if who in opts else None
                    which = which if vehicle and which in eqs else None
                    if who and which and not allowed(who,which):
                        if prefer=="equipment" and org.value in managed_orgs:
                            who=None
                        else:
                            which=None
                    possible_people = {k:v for k,v in opts.items() if not which or allowed(k,which)}
                    possible_eqs = sorted(k for k in eqs if not who or allowed(who,k))
                    if driver.options!=possible_people or driver.value!=who:
                        driver.set_options(possible_people,value=who)
                    if machine.options!=possible_eqs or machine.value!=which:
                        machine.set_options(possible_eqs,value=which)
                    msg = ("Нет сотрудников с подходящими допусками на дату смены." if which and not possible_people else
                           "Нет подходящей техники. Проверьте допуски сотрудника и требования в карточке техники." if who and not possible_eqs else "")
                    no_match.set_text(msg)
                    no_match.set_visibility(vehicle and bool(msg))
                finally:
                    pairing = False

            async def date_changed():
                scope,day = org.value,inputs["work_date"].value
                if not scope or not day:
                    return
                marks = await call(journal.qualifications,scope,work_date=day)
                if (scope,day)==(org.value,inputs["work_date"].value):
                    permits.clear()
                    permits.update(marks)
                    pair()

            category.on_value_change(lambda:pair())
            inputs["employee"].on_value_change(lambda:pair("employee"))
            inputs["equipment"].on_value_change(lambda:pair("equipment"))
            inputs["work_date"].on_value_change(date_changed)
            def entry_changed():
                set_entry_type()
                pair()
            inputs["entry_type"].on_value_change(entry_changed)

            async def choose_organization():
                nonlocal pairing
                selected_org = org.value
                manager = selected_org in managed_orgs
                save_button.disable()
                for buttons in add_actions.values():
                    for button in buttons:
                        button.set_visibility(manager)
                people = await call(journal.users, organization_id=selected_org) if selected_org else {}
                marks = await call(journal.qualifications,selected_org,work_date=inputs["work_date"].value) if selected_org else {}
                if org.value != selected_org:
                    return
                if row.get("employee") and row["employee"] not in people:
                    people[row["employee"]] = row["employee_name"] + " (архив)"
                people_options.clear()
                people_options.update(people)
                permits.clear()
                permits.update(marks)
                pairing=True
                current = row.get("employee") or (None if manager else (state.current_user or {}).get("username"))
                inputs["employee"].set_options(people, value=current if current in people else None)
                inputs["employee"].set_enabled(manager)
                category.set_visibility(manager)
                category.value = None
                substitution.set_visibility(manager or bool(row.get("assigned_employee") or row.get("assigned_equipment")))
                assigned = row.get("assigned_employee") or None
                assigned_people = dict(people)
                if assigned:
                    assigned_people.setdefault(assigned,row["assigned_employee_name"]+" (архив)")
                inputs["assigned_employee"].set_options(assigned_people,value=assigned)
                inputs["assigned_equipment"].value = None
                for key in ("assigned_employee","assigned_equipment"):
                    inputs[key].set_enabled(manager)
                for key in ("organization", "equipment", "workplace", "partner"):
                    set_reference(key, row.get(key) or None)
                set_entry_type()
                pairing=False
                pair()
                await refresh_rates()
                save_button.set_text("Сохранить" if manager else "Сохранить и отправить")
                save_button.set_enabled(bool(selected_org))

            org.on_value_change(choose_organization)
            await choose_organization()
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

    async def attachments():
        row = selected()
        if row is None or journal is None:
            return
        try:
            store = await run.io_bound(ShiftAttachments, journal)
            items = await call(store.list, row["id"])
            with ui.dialog() as dialog, ui.card().classes("w-full max-w-xl"):
                ui.label(f"Путевые листы · смена №{row['id']}").classes("text-lg font-semibold")
                if not items:
                    ui.label("К этой смене ещё не приложили путевой лист.")

                def download_action(item):
                    async def download():
                        try:
                            file = await call(store.read, row["id"], item["id"])
                            ui.download(file["content"], file["filename"], media_type="application/octet-stream")
                        except Exception as exc:
                            error(exc)
                    return download

                for item in items:
                    ui.button(item["filename"], icon="download", on_click=download_action(item)).props("flat no-caps")
                ui.button("Закрыть", on_click=dialog.close).props("flat")
            dialog.open()
        except Exception as exc:
            error(exc)

    async def print_card():
        row = selected()
        if not row:
            return
        try:
            item = await call(journal.get, row["id"])
            ui.download(printable_shift(item,STATUS_LABELS[item["status"]]), f'shift-{item["id"]}.html', media_type="text/html")
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

    async def load_journal():
        root = Path(__file__).resolve().parents[3]
        path = root / (state.cfg.get("work_shifts_db_path") or "data/work_shifts.db")
        auth = await run.io_bound(_get_auth_db, state)
        return await run.io_bound(WorkShiftJournal, path, auth)

    financial = bool(state.current_user and state.current_user.get("is_payroll_owner"))
    if not can_use_shifts(state.current_user):
        if financial:
            async def payroll_only():
                nonlocal journal
                try:
                    if journal is None:
                        journal = await load_journal()
                    await open_payroll(journal, call, error)
                except Exception as exc:
                    error(exc)

            ui.label("Зарплата").classes("text-xl font-semibold")
            ui.button("Расчёт за месяц", icon="payments", on_click=payroll_only)
        return

    with ui.column().classes("w-full min-w-0 gap-4"):
        with ui.row().classes("w-full items-center justify-between"):
            ui.label("Смены и путевые листы").classes("text-xl font-semibold")
            with ui.row():
                new_button = ui.button("Новая смена", icon="add", on_click=lambda: edit(new=True)).props("data-rag-usage=shifts.create")
                new_button.disable()
                ui.button(icon="download", on_click=export).props('flat aria-label="Выгрузить CSV"').tooltip("Выгрузить CSV")
                if admin:
                    ui.button("Управление", icon="manage_accounts", on_click=lambda: show_shift_administration(state, journal))
                    ui.button("Табель",icon="calendar_month", on_click=lambda: show_month_sheet(journal,call,error))
                if financial:
                    ui.button("Расчёт за месяц",icon="payments",on_click=lambda:open_payroll(journal,call,error))
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
            ("waybill_number", "Путевой лист"), ("hours", "Часы"), ("breaks", "Перерывы"),("conflict","Пересечения"))]
        table = ui.table(columns=columns, rows=[], row_key="id", selection="single", on_select=update_actions).classes("w-full max-w-full").props("flat bordered wrap-cells")
        table.add_slot('body-cell-status_label', '''
            <q-td :props="props"><q-badge :color="props.row.status === 'approved' ? 'positive' : 'negative'"
            :label="props.row.status_label" /></q-td>''')
        table.add_slot("body-cell-conflict", """<q-td :props="props"><q-badge v-if="props.row.conflict"
            color="negative" :label="props.row.conflict" /></q-td>""")
        with ui.row().classes("w-full flex-wrap items-center"):
            action_buttons["edit"] = ui.button(icon="edit", on_click=lambda: edit()).props('flat aria-label="Изменить смену"').tooltip("Изменить смену")
            if admin:
                action_buttons["submitted"] = ui.button("На проверку", icon="send", on_click=lambda: change("submitted")).props("flat")
                action_buttons["approved"] = ui.button("Подтвердить", icon="check", on_click=lambda: change("approved")).props("flat")
                action_buttons["draft"] = ui.button("В черновик", icon="undo", on_click=lambda: change("draft")).props("flat")
            action_buttons["history"] = ui.button(icon="history", on_click=history).props('flat aria-label="История изменений"').tooltip("История изменений")
            action_buttons["attachments"] = ui.button("Путевые листы", icon="attach_file", on_click=attachments).props("flat")
            action_buttons["print"] = ui.button(icon="print",on_click=print_card).props('flat aria-label="Версия для печати"').tooltip("Скачать карточку для печати")
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
        nonlocal journal, users, managed_orgs
        try:
            journal = await load_journal()
            users = await call(journal.users)
            managed_orgs = await call(journal.organizations, manage=True)
            employee_filter.set_options({"": "Все сотрудники", **users})
            new_button.enable()
            await refresh()
        except Exception as exc:
            progress.set_visibility(False)
            summary.set_text("Журнал недоступен")
            error(exc)

    ui.timer(0, initialize, once=True)
