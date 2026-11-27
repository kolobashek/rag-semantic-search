"""Excel forms and monthly reconciliation registers in the shift journal."""

import base64
import json
from datetime import date
from pathlib import Path

from nicegui import run, ui

from rag_catalog.core.waybill_documents import WAYBILL_SHIFT_FIELDS, WaybillDocuments, print_pdf
from rag_catalog.core.waybill_registers import WaybillRegisters

XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


async def _deliver(workbook, name, preview):
    if not preview:
        ui.download(workbook, name + ".xlsx", media_type=XLSX)
        return
    preparing = ui.notification("Готовим версию для печати…", spinner=True, timeout=None)
    try:
        pdf = await run.io_bound(print_pdf, workbook)
        encoded = json.dumps(base64.b64encode(pdf).decode("ascii"))
        url = await ui.run_javascript(
            f"const bytes = Uint8Array.from(atob({encoded}), c => c.charCodeAt(0));"
            "return URL.createObjectURL(new Blob([bytes], {type: 'application/pdf'}))",
            timeout=15,
        )
    finally:
        preparing.dismiss()
    with ui.dialog() as dialog, ui.card().classes("w-full max-w-5xl"):
        ui.label(name).classes("text-lg font-semibold")
        frame = ui.element("iframe").classes("w-full").style("height: 62vh")
        frame.props.update({"src": url, "title": name})
        with ui.row():
            print_button = ui.button("Печать", icon="print")
            print_button.on("click", js_handler=f"() => document.getElementById('c{frame.id}').contentWindow.print()")
            ui.button("Скачать PDF", icon="download",
                      on_click=lambda: ui.download(pdf, name + ".pdf", media_type="application/pdf")).props("flat")
            ui.button("Закрыть", on_click=dialog.close).props("flat")
    dialog.on("hide", js_handler=f"() => URL.revokeObjectURL({json.dumps(url)})")
    dialog.on("hide", dialog.delete)
    dialog.open()


def _organization_selector(organizations):
    initial = next(iter(organizations)) if len(organizations) == 1 else None
    return ui.select(organizations, label="Организация журнала", value=initial).classes("w-full")


def _import_template(store, call, organization, added, report):
    content = b""

    async def upload(event):
        nonlocal content
        content = await event.file.read()
        if not name.value:
            name.set_value(Path(event.file.name).stem)
        save_button.enable()

    async def save():
        save_button.disable()
        try:
            template_id = await call(store.add_template, organization, name.value or "", content)
            dialog.close()
            await added(template_id)
            ui.notify("Бланк добавлен", type="positive")
        except Exception as exc:
            report(exc)
        finally:
            save_button.enable()

    with ui.dialog() as dialog, ui.card().classes("w-full max-w-lg"):
        ui.label("Импорт бланка Excel").classes("text-lg font-semibold")
        name = ui.input("Название бланка").classes("w-full")
        ui.label("Загрузите XLSX с одним листом и настройте в Excel область печати. "
                 "В ячейках можно использовать маркеры. Например, {{маршрут}} добавит поле для ручного ввода.")
        with ui.expansion("Поля из карточки смены").classes("w-full"):
            for key, label in WAYBILL_SHIFT_FIELDS.items():
                ui.label(f"{label} — {{{{{key}}}}}")
        ui.upload(label="Выбрать книгу Excel", auto_upload=True, max_file_size=4 * 1024 * 1024,
                  on_rejected=lambda: ui.notify("Выберите файл .xlsx до 4 МБ", type="warning"),
                  on_upload=upload).props('accept=".xlsx"').classes("w-full")
        with ui.row():
            save_button = ui.button("Добавить бланк", on_click=save)
            save_button.disable()
            ui.button("Отмена", on_click=dialog.close).props("flat")
    dialog.on("hide", dialog.delete)
    dialog.open()


def _create_template(store, call, organization, added, report):
    async def save():
        submit.disable()
        try:
            extra = [label.strip() for label in (manual.value or "").splitlines() if label.strip()]
            template_id = await call(store.create_template, organization, name.value or "", fields.value or [], extra)
            dialog.close()
            await added(template_id)
            ui.notify("Бланк создан", type="positive")
        except Exception as exc:
            report(exc)
        finally:
            submit.enable()

    with ui.dialog() as dialog, ui.card().classes("w-full max-w-lg"):
        ui.label("Создание бланка").classes("text-lg font-semibold")
        name = ui.input("Название бланка").classes("w-full")
        fields = ui.select(WAYBILL_SHIFT_FIELDS, label="Поля из смены", multiple=True,
                           value=[k for k in ("waybill_number", "work_date", "employee_name", "equipment")
                                  if k in WAYBILL_SHIFT_FIELDS]).classes("w-full").props("use-chips")
        manual = ui.textarea("Поля для ручного ввода").classes("w-full").props("rows=3")
        ui.label("Введите каждое название на отдельной строке.").classes("text-sm")
        with ui.row():
            submit = ui.button("Создать бланк", icon="add", on_click=save)
            ui.button("Отмена", on_click=dialog.close).props("flat")
    dialog.on("hide", dialog.delete)
    dialog.open()


async def show_waybill_library(journal, call, managed_orgs, report):
    try:
        organizations = await call(journal.organizations)
        store = await run.io_bound(WaybillDocuments, journal)
    except Exception as exc:
        report(exc)
        return

    async def refresh(preferred=None):
        scope = organization.value
        template.set_options({}, value=None)
        manage.set_visibility(scope in managed_orgs)
        book.set_enabled(bool(scope))
        try:
            options = await call(store.templates, scope) if scope else {}
            if organization.value == scope:
                template.set_options(options, value=preferred if preferred in options else next(iter(options), None))
        except Exception as exc:
            report(exc)

    async def download_book():
        try:
            workbook = await call(store.book, organization.value)
            await _deliver(workbook, "Бланки путевых листов", False)
        except Exception as exc:
            report(exc)

    async def blank(preview):
        scope, choice = organization.value, template.value
        if choice is None:
            return
        label = template.options[choice]
        try:
            workbook = await call(store.blank, scope, choice)
            await _deliver(workbook, label, preview)
        except Exception as exc:
            report(exc)

    with ui.dialog() as dialog, ui.card().classes("w-full max-w-xl"):
        ui.label("Бланки путевых листов").classes("text-lg font-semibold")
        organization = _organization_selector(organizations)
        template = ui.select({}, label="Бланк").classes("w-full")
        with ui.row():
            for label, icon, preview in (("Скачать чистый бланк", "download", False), ("Печать бланка", "print", True)):
                button = ui.button(label, icon=icon, on_click=lambda p=preview: blank(p))
                button.bind_enabled_from(template, "value", backward=lambda v: v is not None)
        with ui.row() as manage:
            ui.button("Создать бланк", icon="add",
                      on_click=lambda: _create_template(store, call, organization.value, refresh, report)).props("flat")
            ui.button("Импорт из Excel", icon="upload_file",
                      on_click=lambda: _import_template(store, call, organization.value, refresh, report)).props("flat")
        with ui.row():
            book = ui.button("Книга всех бланков", icon="library_books", on_click=download_book).props("flat")
            ui.button("Закрыть", on_click=dialog.close).props("flat")
        organization.on_value_change(lambda: refresh())
    dialog.open()
    await refresh()


async def show_shift_waybill(journal, call, shift, refresh_journal, report):
    scope = shift["organization_id"]
    try:
        store = await run.io_bound(WaybillDocuments, journal)
        options = await call(store.templates, scope)
        issued = await call(store.issued_templates, shift["id"])
    except Exception as exc:
        report(exc)
        return

    def mark_saved(keys):
        return {key: f"{label} (сохранён)" if key in keys else label for key, label in options.items()}

    async def after_save():
        await refresh_journal()
        saved = await call(store.issued_templates, shift["id"])
        template.set_options(mark_saved(saved), value=template.value)

    async def open_editor():
        key = template.value
        if key is None:
            return
        try:
            labels = await call(store.fields, scope, key)
            draft = await call(store.draft, shift["id"], key)
            _waybill_editor(store, call, shift["id"], key, options[key], labels, draft, after_save, report)
        except Exception as exc:
            report(exc)

    with ui.dialog() as dialog, ui.card().classes("w-full max-w-lg"):
        ui.label(f"Путевой лист · смена №{shift['id']}").classes("text-lg font-semibold")
        recent = next((key for key in issued if key in options), next(iter(options), None))
        template = ui.select(mark_saved(issued), label="Бланк", value=recent).classes("w-full")
        if not options:
            ui.label("Добавьте форму в разделе «Бланки путевых листов».")
        with ui.row():
            ui.button("Открыть бланк", on_click=open_editor).set_enabled(bool(options))
            ui.button("Закрыть", on_click=dialog.close).props("flat")
    dialog.open()


def _waybill_editor(store, call, shift_id, key, title, labels, draft, refresh_journal, report):
    manual = {}
    busy = False

    async def act(output=None):
        nonlocal draft, busy
        if busy:
            return
        busy = True
        try:
            values = {k: field.value or "" for k, field in manual.items()}
            if not draft["revision"] or any(value != draft["values"].get(k, "") for k, value in values.items()):
                draft = await call(store.save, shift_id, key, values, draft["revision"], draft["shift_revision"])
                await refresh_journal()
            if output:
                workbook = await call(store.export, shift_id, key, draft["revision"], draft["shift_revision"])
                await _deliver(workbook, f"{title} — смена {shift_id}", output == "print")
            else:
                ui.notify("Путевой лист сохранён", type="positive")
        except Exception as exc:
            report(exc)
        finally:
            busy = False

    with ui.dialog() as dialog, ui.card().classes("w-full max-w-2xl"):
        ui.label(f"{title} · смена №{shift_id}").classes("text-lg font-semibold")
        if any(k in WAYBILL_SHIFT_FIELDS for k in labels):
            ui.label("Для исправления данных из журнала откройте карточку смены.")
        with ui.element("div").classes("grid grid-cols-1 sm:grid-cols-2 gap-2 w-full overflow-auto").style("max-height: 52vh"):
            for k, label in labels.items():
                field = ui.input(label, value=draft["values"].get(k, "")).classes("w-full")
                if k in WAYBILL_SHIFT_FIELDS:
                    field.props("readonly")
                else:
                    manual[k] = field
        with ui.row():
            ui.button("Сохранить", icon="save", on_click=lambda: act())
            ui.button("Скачать Excel", icon="download", on_click=lambda: act("excel")).props("flat")
            ui.button("Версия для печати", icon="print", on_click=lambda: act("print")).props("flat")
            ui.button("Закрыть", on_click=dialog.close).props("flat")
    dialog.open()


REGISTER_FIELDS = {
    "work_date": "Дата", "waybill_number": "Путевой лист", "shift_number": "Смена",
    "equipment": "Техника", "registration_number": "Госномер", "employee_name": "Сотрудник",
    "hours": "Отработано, ч", "workplace": "Объект", "work": "Описание работ",
    "duty_hours": "Техдежурство, ч", "trips": "Рейсы", "fuel_used": "Расход топлива, л",
    "fuel_filled": "Заправка, л", "rate_with_vat": "Договорный тариф за час с НДС, ₽",
    "duty_rate_with_vat": "Тариф техдежурства за час с НДС, ₽",
}


async def show_waybill_registers(journal, call, managed_orgs, report):
    try:
        store = await run.io_bound(WaybillRegisters, journal)
    except Exception as exc:
        report(exc)
        return

    async def refresh():
        scope, period = organization.value, month.value
        partner.set_options([], value=None)
        month_book.disable()
        status.set_text("")
        if not scope or not period:
            return
        try:
            options = await call(store.partners, scope, period)
            if (scope, period) != (organization.value, month.value):
                return
            partner.set_options(options, value=options[0] if len(options) == 1 else None)
            month_book.set_enabled(bool(options))
            if not options:
                status.set_text("За этот месяц нет реестров и подтверждённых смен с контрагентом.")
        except Exception as exc:
            report(exc)

    async def download_month():
        scope, period = organization.value, month.value
        try:
            content = await call(store.month_book, scope, period)
            await _deliver(content, f"Реестры путевых листов — {managed_orgs[scope]} — {period}", False)
        except Exception as exc:
            report(exc)

    async def open_register():
        args = (organization.value, month.value, partner.value)
        if not all(args):
            return
        try:
            document = await call(store.open, *args)
            _register_editor(store, call, args, document, report)
        except Exception as exc:
            report(exc)

    with ui.dialog() as dialog, ui.card().classes("w-full max-w-lg"):
        ui.label("Реестры путевых листов").classes("text-lg font-semibold")
        organization = _organization_selector(managed_orgs)
        month = ui.input("Месяц", value=date.today().strftime("%Y-%m")).props("type=month")
        partner = ui.select([], label="Контрагент", with_input=True).classes("w-full")
        status = ui.label().props("role=status")
        with ui.row():
            button = ui.button("Открыть реестр", on_click=open_register)
            button.bind_enabled_from(partner, "value", backward=bool)
            month_book = ui.button("Книга реестров за месяц", icon="library_books",
                                   on_click=download_month).props("flat").tooltip("Книга по всем контрагентам организации")
            ui.button("Закрыть", on_click=dialog.close).props("flat")
        organization.on_value_change(refresh)
        month.on_value_change(refresh)
    dialog.open()
    await refresh()


def _register_editor(store, call, args, document, report):
    rows = [dict(row) for row in document["rows"]]
    busy = False

    def edit_row():
        if not table.selected or busy:
            return
        selected = next(r for r in rows if r["shift_id"] == table.selected[0]["shift_id"])

        def apply_changes():
            for key, field in inputs.items():
                previous = selected.get(key)
                if field.value != ("" if previous is None else str(previous)):
                    selected[key] = field.value
            table.rows = rows
            table.selected = []
            table.update()
            change.disable()
            editor.close()

        with ui.dialog() as editor, ui.card().classes("w-full max-w-lg"):
            ui.label("Строка реестра").classes("text-lg font-semibold")
            inputs = {}
            with ui.element("div").classes("grid grid-cols-1 sm:grid-cols-2 gap-2 w-full overflow-auto").style("max-height: 56vh"):
                for key, label in REGISTER_FIELDS.items():
                    value = selected.get(key)
                    field = ui.input(label, value="" if value is None else str(value)).classes("w-full")
                    if key == "work_date":
                        field.props("type=date")
                    if key in {"hours", "duty_hours", "fuel_used", "fuel_filled", "rate_with_vat", "duty_rate_with_vat"}:
                        field.props("inputmode=decimal")
                    if key in {"shift_number", "trips"}:
                        field.props("inputmode=numeric")
                    inputs[key] = field
            with ui.row():
                ui.button("Применить", on_click=apply_changes)
                ui.button("Отмена", on_click=editor.close).props("flat")
        editor.open()

    async def act(output=None):
        nonlocal document, busy
        if busy:
            return
        busy = True
        try:
            if not document["revision"] or rows != document["rows"]:
                document = await call(store.save, *args, document["revision"], rows)
            if output == "refresh":
                document = await call(store.refresh, *args, document["revision"])
            if rows != document["rows"]:
                rows[:] = [dict(r) for r in document["rows"]]
                table.rows = rows
                table.selected = []
                table.update()
                change.disable()
            if output in {"excel", "print"}:
                workbook = await call(store.xlsx, *args, revision=document["revision"])
                await _deliver(workbook, f"Реестр путевых листов — {args[2]} — {args[1]}", output == "print")
            else:
                ui.notify("Список смен обновлён" if output == "refresh" else "Реестр сохранён", type="positive")
        except Exception as exc:
            report(exc)
        finally:
            busy = False

    with ui.dialog() as dialog, ui.card().classes("w-full max-w-6xl"):
        ui.label(f"Реестр путевых листов · {document['month']}").classes("text-lg font-semibold")
        ui.label(f"{document['organization']} · {document['partner']}")
        columns = [dict(name=k, field=k, label=REGISTER_FIELDS[k], align="left")
                   for k in ("work_date", "waybill_number", "equipment", "registration_number", "employee_name", "hours")]
        table = ui.table(columns=columns, rows=rows, row_key="shift_id", selection="single", pagination=15,
                         on_select=lambda: change.set_enabled(bool(table.selected))).classes("w-full").props("flat bordered wrap-cells")
        with ui.row():
            change = ui.button("Изменить строку", icon="edit", on_click=edit_row)
            change.disable()
            ui.button("Сохранить реестр", icon="save", on_click=lambda: act())
            ui.button("Добавить подтверждённые смены", icon="playlist_add", on_click=lambda: act("refresh")).props("flat")
            ui.button("Скачать Excel", icon="download", on_click=lambda: act("excel")).props("flat")
            ui.button("Версия для печати", icon="print", on_click=lambda: act("print")).props("flat")
            ui.button("Закрыть", on_click=dialog.close).props("flat")
    dialog.open()
