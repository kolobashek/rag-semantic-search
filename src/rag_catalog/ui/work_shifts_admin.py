"""Dispatcher directories and immutable waybill history in the existing UI shell."""

from datetime import date

from nicegui import run, ui

from rag_catalog.core.work_shifts import REFERENCE_LABELS, STATUS_LABELS
from rag_catalog.core.shift_operations import CATEGORIES, VEHICLE_CATEGORIES, LICENCES, MARKS

FIELD_LABELS = dict(employee="Логин", employee_name="Сотрудник", work_date="Дата", shift_number="Смена",
                    assigned_employee="Назначенный сотрудник", assigned_employee_name="ФИО по назначению",
                    assigned_equipment="Назначенная техника", registration_number="Фактический госномер",
                    assigned_registration_number="Госномер по назначению", time_from="Начало", time_to="Окончание",
                    work_lines="Выполненные работы",
                    organization="Организация", equipment="Техника", workplace="Объект", partner="Контрагент",
                    waybill_number="Путевой лист", comment="Комментарий", work_minutes="Работа, мин",
                    break_minutes="Перерывы, мин", status="Статус", deleted="Удалена")


def show_shift_history(event):
    for key, (before, after) in event["changes"].items():
        if key == "status":
            before, after = STATUS_LABELS.get(before, before), STATUS_LABELS.get(after, after)
        ui.label(f"{FIELD_LABELS.get(key, key)}: {before if before is not None else '—'} → {after}").classes("break-words w-full")


def licence_fields():
    fields = {}
    for kind,label in LICENCES.items():
        with ui.expansion(label,icon="badge").classes("w-full"):
            marks = ui.select(MARKS[kind],multiple=True,value=[],label=f"Открытые категории: {label}").props("use-chips").classes("w-full")
            with ui.row().classes("w-full gap-3"):
                since = ui.input("Дата выдачи").props("type=date")
                until = ui.input("Действительно до").props("type=date")
            fields[kind] = marks,since,until
    def read():
        return {k:dict(marks=m.value,since=s.value,until=e.value) for k,(m,s,e) in fields.items() if m.value}
    def load(docs):
        for k,(m,s,e) in fields.items():
            doc = docs.get(k,{})
            m.value,s.value,e.value = doc.get("marks",[]),doc.get("since",""),doc.get("until","")
    return read,load


def open_directory_editor(journal, call, organizations, kind, on_saved, report, *, row=None, choose_kind=False):
    row = row or {}
    employee = kind == "employee"
    if employee and row and not row["editable"]:
        ui.notify("Дополнительные роли меняет администратор в настройках", type="warning")
        return
    default_org = next(iter(organizations)) if len(organizations) == 1 else None
    existing_org = row["organization_ids"][0] if employee and row else row.get("organization_id")
    with ui.dialog() as editor, ui.card().classes("w-full max-w-lg").style("background: var(--rag-surface-strong)"):
        titles = {"employee": "Сотрудник", "organization": "Организация в путевом листе", "equipment": "Техника",
                  "partner": "Контрагент", "workplace": "Объект"}
        ui.label("Запись справочника" if choose_kind else titles[kind]).classes("text-lg")
        org = ui.select(organizations, label="Организация сотрудника" if employee else "Организация журнала",
                        value=existing_org or default_org).classes("w-full")
        org.set_enabled(not row)
        org.set_visibility(len(organizations) > 1)
        if employee:
            with_login = ui.checkbox("Создать учётную запись для входа", value=bool(row and not row.get("offline")))
            with_login.set_visibility(not row)
            username = ui.input("Логин", value=row.get("username", "")).classes("w-full")
            username.set_enabled(not row)
            name = ui.input("ФИО", value=row.get("display_name", "")).classes("w-full")
            categories = ui.select(CATEGORIES, value=row.get("categories", []), multiple=True,
                                   label="Категории сотрудника").props("use-chips").classes("w-full")
            read_docs,load_docs = licence_fields()
            load_docs(row.get("licences",{}))
            password = ui.input("Временный пароль", password=True, password_toggle_button=True).props(
                'hint="Не менее 12 символов" autocomplete=new-password').classes("w-full")
            username.bind_visibility_from(with_login, "value")
            password.bind_visibility_from(with_login, "value")
        else:
            kind_field = ui.select(REFERENCE_LABELS, label="Справочник", value=kind).classes("w-full")
            kind_field.set_enabled(not row)
            kind_field.set_visibility(choose_kind)
            name = ui.input("Название", value=row.get("equipment_name") or row.get("name", "")).props("maxlength=200").classes("w-full")
            with ui.row().classes("w-full gap-3") as numbers:
                registration = ui.input("Госномер", value=row.get("registration_number", "")).props("maxlength=32").classes("flex-1 min-w-0")
                fleet = ui.input("Бортовой номер", value=row.get("fleet_number", "")).props("maxlength=32").classes("flex-1 min-w-0")
            eq_category = ui.select(VEHICLE_CATEGORIES, label="Категория техники",
                                    value=row.get("equipment_category") or None).classes("w-full")
            eq_category.bind_visibility_from(kind_field,"value",value="equipment")
            with ui.row().classes("w-full gap-3") as requirements:
                doc_kind = ui.select(LICENCES,label="Требуемое удостоверение",value=row.get("licence_kind") or None).classes("w-full")
                doc_mark = ui.select(MARKS.get(doc_kind.value,[]),label="Требуемая категория прав",value=row.get("licence_mark") or None)
                doc_kind.on_value_change(lambda:doc_mark.set_options(MARKS.get(doc_kind.value,[]),value=None))
            requirements.bind_visibility_from(kind_field,"value",value="equipment")
            numbers.bind_visibility_from(kind_field, "value", value="equipment")
        archived = ui.checkbox("В архиве", value=row.get("status") == "blocked" if employee else bool(row.get("archived")))
        archived.set_visibility(bool(row))

        async def save():
            button.disable()
            try:
                if not org.value:
                    raise ValueError("Выберите организацию")
                if employee:
                    value = (username.value or "").strip().lower()
                    value = await call(journal.save_employee, offline=not with_login.value, categories=categories.value,licences=read_docs(),
                                       username=value, display_name=name.value or "",
                               password=password.value or "", create=not row, archived=archived.value, organization_id=org.value)
                    password.set_value("")
                else:
                    if kind_field.value=="equipment" and not archived.value and not (eq_category.value and doc_mark.value and doc_kind.value):
                        raise ValueError("Выберите категорию техники, удостоверение и категорию прав")
                    value = await call(journal.save_reference, kind_field.value, name.value or "",
                                       record_id=row.get("id"), revision=row.get("revision"), archived=archived.value,
                                       organization_id=org.value,
                                       registration_number=registration.value if kind_field.value == "equipment" else "",
                                       fleet_number=fleet.value if kind_field.value == "equipment" else "",
                                       equipment_category=(eq_category.value or "") if kind_field.value=="equipment" else "",
                                       licence_kind=(doc_kind.value or "") if kind_field.value=="equipment" else "",
                                       licence_mark=(doc_mark.value or "") if kind_field.value=="equipment" else "")
                editor.close()
                await on_saved(org.value, value)
            except Exception as exc:
                report(exc)
            finally:
                button.enable()

        with ui.row():
            ui.button("Отмена", on_click=editor.close).props("flat")
            button = ui.button("Сохранить", icon="save", on_click=save)
    editor.on("hide", editor.delete)
    editor.open()


async def show_shift_administration(state, journal):
    if journal is None:
        return
    offset = 0

    async def call(method, *args, **kwargs):
        return await run.io_bound(method, state.auth_token, *args, **kwargs)

    organizations = await call(journal.organizations, manage=True)

    def report(exc):
        ui.notify(str(exc) if isinstance(exc, (ValueError, PermissionError)) else "Не удалось выполнить операцию", type="negative")

    def reference_editor(row=None):
        open_directory_editor(journal, call, organizations, (row or {}).get("kind", "organization"),
                              lambda org, value: refresh_references(), report, row=row, choose_kind=True)

    async def employee_editor(row=None):
        if row and row["editable"]:
            row = dict(row,licences=await call(journal.staff_licences,row["organization_ids"][0],row["username"]))
        open_directory_editor(journal, call, organizations, "employee", lambda org, value: refresh_employees(), report, row=row)

    async def edit_categories():
        if not staff.selected:
            return
        person = staff.selected[0]
        scopes = {k:organizations[k] for k in person["organization_ids"] if k in organizations}
        with ui.dialog() as child, ui.card().classes("w-full max-w-md"):
            ui.label("Допуски: " + person["display_name"]).classes("text-lg")
            org = ui.select(scopes,value=next(iter(scopes)),label="Организация журнала")
            org.set_visibility(len(scopes)>1)
            categories = ui.select(CATEGORIES,multiple=True,value=[],label="Категории сотрудника").props("use-chips").classes("w-full")
            read_docs,load_docs = licence_fields()
            async def load():
                scope = org.value
                submit.disable()
                try:
                    current = await call(journal.qualifications,scope)
                    docs = await call(journal.staff_licences,scope,person["username"])
                    if org.value==scope:
                        categories.value = current.get(person["username"],[])
                        load_docs(docs)
                        submit.enable()
                except Exception as exc:
                    report(exc)
            async def save():
                submit.disable()
                try:
                    await call(journal.set_categories,org.value,person["username"],categories.value,licences=read_docs())
                    child.close()
                    await refresh_employees()
                except Exception as exc:
                    report(exc)
                finally:
                    submit.enable()
            org.on_value_change(load)
            with ui.row():
                ui.button("Отмена",on_click=child.close).props("flat")
                submit=ui.button("Сохранить",icon="save",on_click=save)
        child.open()
        await load()

    async def refresh_references():
        try:
            refs.rows = [dict(r, kind_label=REFERENCE_LABELS[r["kind"]], org_name=organizations.get(r["organization_id"], ""),
                             state="Архив" if r["archived"] else "Активна")
                         for r in await call(journal.references, archived=True)]
            refs.selected = []
            refs.update()
        except Exception as exc:
            report(exc)

    async def refresh_employees():
        try:
            staff.rows = [dict(r, login="" if r.get("offline") else r["username"],
                               state=("Без входа" if r.get("offline") else "Активен") if r["status"] == "active" else "Отключён")
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
                refs = ui.table(columns=columns((("org_name", "Организация"), ("kind_label", "Справочник"), ("name", "Название"), ("state", "Статус"))),
                                rows=[], row_key="id", selection="single", pagination=15).classes("w-full").props("flat wrap-cells")
            with ui.tab_panel(employees_tab):
                with ui.row():
                    ui.button("Добавить сотрудника", icon="person_add", on_click=lambda: employee_editor())
                    ui.button("Допуски",icon="badge",on_click=edit_categories).props("flat")
                    ui.button(icon="edit", on_click=lambda: employee_editor(staff.selected[0]) if staff.selected else None).props("flat").tooltip("Изменить или архивировать сотрудника")
                    ui.button(icon="refresh", on_click=refresh_employees).props("flat").tooltip("Обновить сотрудников")
                staff = ui.table(columns=columns((("login", "Логин"), ("display_name", "ФИО"), ("state", "Статус"))),
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


async def show_month_sheet(journal, call, report):
    if journal is None:
        return
    with ui.dialog() as dialog, ui.card().classes("w-full max-w-5xl"):
        ui.label("Табель фактической работы").classes("text-lg")
        with ui.row().classes("items-center"):
            month = ui.input("Месяц",value=date.today().strftime("%Y-%m")).props("type=month")
            attendance = ui.checkbox("Только явки без техники")
        cols = [(k,k) for k in ("Сотрудник","Дни и часы","Всего, ч","Подтверждено, ч")]
        table = ui.table(columns=[dict(name=str(i),label=label,field=str(i),align="left") for i,(_,label) in enumerate(cols)],
                         rows=[],row_key="employee",pagination=20).props("flat wrap-cells").classes("w-full")
        async def refresh():
            request = month.value, attendance.value
            try:
                people = await call(journal.month_sheet,request[0],attendance_only=request[1])
                if request != (month.value,attendance.value):
                    return
                table.rows = [{"employee":key,"0":p["name"],"1":", ".join(f'{d}: {m/60:g} ч' for d,m in p["days"].items()),
                               "2":f'{p["minutes"]/60:g}',"3":f'{p["approved"]/60:g}'}
                              for key,p in people.items()]
                table.update()
            except Exception as exc:
                table.rows = []
                table.update()
                report(exc)
        month.on_value_change(refresh)
        attendance.on_value_change(refresh)
        ui.button("Закрыть",on_click=dialog.close).props("flat")
    dialog.open()
    await refresh()
