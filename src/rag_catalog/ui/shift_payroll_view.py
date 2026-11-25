"""Monthly estimate and its explicit management operations."""

from datetime import date
from decimal import Decimal, InvalidOperation

from nicegui import ui

from rag_catalog.core.shift_payroll import GROUPS, MonthlyPayroll
from rag_catalog.core.shift_operations import UNITS


def rub(cents):
    return "—" if cents is None else f"{Decimal(cents) / 100:.2f}"


def columns(labels):
    return [dict(name=key, field=key, label=label, align="left") for key, label in labels]


async def open_payroll(journal, call, report):
    if journal is None:
        return
    service = MonthlyPayroll(journal)
    organizations = await call(service.organizations)
    data = {}
    version = 0

    async def refresh():
        nonlocal data, version
        version += 1
        request = version
        data = {}
        totals.rows = []
        ledger.rows = []
        tariffs.rows = []
        tariffs.update()
        ledger.selected = []
        totals.update()
        ledger.update()
        remove.disable()
        for b in actions:
            b.disable()
        norm_text.set_text("")
        if not org.value or not month.value:
            return
        try:
            result = await call(service.sheet, org.value, month.value)
            if request != version:
                return
            data = result
            tariffs.rows = [dict(id=i, work=r["work"], unit=UNITS[r["unit"]], price=rub(r["kopecks"]),
                                 since=r["since"], equipment=r["equipment"], workplace=r["workplace"])
                            for i, r in enumerate(data["rates"])]
            tariffs.update()
            n = sum(data["days"])
            norm_text.set_text(
                f"Норма: {sum(bool(v) for v in data['days'])} раб. дн. · {n / 60:g} ч"
                if data["norm_id"]
                else "Календарь не подтверждён. Расчёт по окладу недоступен."
            )
            totals.rows = [
                dict(
                    employee=k,
                    name=p["name"],
                    approved=f"{p['minutes'] / 60:g}",
                    pending=f"{p['pending'] / 60:g}",
                    salary=rub(p["salary"]),
                    base=rub(p["base"]),
                    credit=rub(p["credit"]),
                    debit=rub(p["debit"]),
                    voluntary=rub(p["voluntary"]),
                    estimate=rub(p["estimate"]),
                    delta=f"{(p['minutes'] - n) / 60:+g}" if data["norm_id"] and p["salary"] else "—",
                    extra=rub(p["extra_cents"]),
                )
                for k, p in sorted(data["people"].items(), key=lambda item: item[1]["name"].casefold())
            ]
            ledger.rows = [
                dict(
                    id=r["id"],
                    name=r["name"],
                    label=r["label"],
                    kind=GROUPS[r["kind"]],
                    amount=rub(r["calculated"]),
                    rate=f"{r['basis_points'] / 100:g}%" if r["basis_points"] else "—",
                    earnings=rub(r["earnings"]),
                    period=r["since"]
                    if r["until_month"] == r["since"]
                    else f"{r['since']} → {r['until_month'] or 'далее'}",
                    basis=r["basis"],
                    actor=r["actor"],
                    state="Отменена" if r["stopped"] else "Действует",
                    stopped=r["stopped"],
                )
                for r in data["items"]
            ]
            totals.update()
            ledger.update()
            for b in actions:
                b.enable()
        except Exception as exc:
            report(exc)

    def save_buttons(modal, action, label="Сохранить"):
        async def submit():
            save.disable()
            try:
                await action()
                modal.close()
                await refresh()
            except Exception as exc:
                report(exc)
            finally:
                save.enable()

        with ui.row():
            ui.button("Отмена", on_click=modal.close).props("flat")
            save = ui.button(label, icon="save", on_click=submit)
        modal.on("hide", modal.delete)

    def calendar_editor():
        scope, when, revision = org.value, month.value, data["norm_id"]
        with ui.dialog() as modal, ui.card().classes("w-full max-w-lg"):
            ui.label(f"Норма рабочего времени · {when}").classes("text-lg")
            ui.label("Плановые часы по дням. Выходные и праздники: 0.").classes("text-sm")
            with ui.element("div").classes("grid grid-cols-4 sm:grid-cols-7 gap-2 w-full"):
                fields = [
                    ui.number(str(i), value=m / 60, min=0, max=24, step=0.25).classes("w-full")
                    for i, m in enumerate(data["days"], 1)
                ]

            async def apply():
                try:
                    minutes = [Decimal(str(f.value)) * 60 for f in fields]
                    if any(not x.is_finite() or x != x.to_integral_value() for x in minutes):
                        raise ValueError
                except (InvalidOperation, ValueError):
                    raise ValueError("Укажите часы по каждому дню с точностью до минуты") from None
                await call(service.set_norm, scope, when, [int(m) for m in minutes], revision=revision)

            save_buttons(modal, apply, "Подтвердить норму")
        modal.open()

    def type_editor():
        scope = org.value
        with ui.dialog() as modal, ui.card().classes("w-full max-w-sm"):
            ui.label("Новый вид операции").classes("text-lg")
            title = ui.input("Название вида").props("maxlength=100").classes("w-full")
            group = ui.select(GROUPS, label="Группа", value="credit").classes("w-full")
            save_buttons(modal, lambda: call(service.add_type, scope, title.value, group.value))
        modal.open()

    async def rate_editor():
        scope = org.value
        refs = await call(service.rate_options, scope)
        if scope != org.value:
            return
        with ui.dialog() as modal, ui.card().classes("w-full max-w-md"):
            ui.label("Новый тариф").classes("text-lg")
            work = ui.input("Вид работы").props("maxlength=200")
            unit = ui.select(UNITS, value="hour", label="Единица расчёта")
            price = ui.number("Ставка, ₽ за единицу", min=0, step=0.01)
            since = ui.input("Действует с", value=date.today().isoformat()).props("type=date")
            eq = ui.select([r["name"] for r in refs if r["kind"] == "equipment"], label="Только для техники", with_input=True).props("clearable")
            wp = ui.select([r["name"] for r in refs if r["kind"] == "workplace"], label="Только для объекта", with_input=True).props("clearable")
            save_buttons(modal, lambda: call(journal.add_rate, organization_id=scope, work=work.value, unit=unit.value,
                         price=price.value, since=since.value, equipment=eq.value or "", workplace=wp.value or ""), "Сохранить тариф")
        modal.open()

    async def entry_editor(salary=False):
        scope, when = org.value, month.value
        people = await call(service.employees, scope)
        if (org.value, month.value) != (scope, when) or not data:
            return
        people.update({k: p["name"] for k, p in data["people"].items() if k not in people})
        types = data["types"]
        with ui.dialog() as modal, ui.card().classes("w-full max-w-md"):
            ui.label(f"{'Оклад с месяца' if salary else 'Операция за месяц'} · {when}").classes("text-lg")
            employee = ui.select(people, label="Сотрудник", with_input=True).classes("w-full")
            if not salary:
                kind = ui.select(
                    {k: f"{label} · {GROUPS[g]}" for k, (label, g) in types.items()},
                    label="Вид операции",
                    with_input=True,
                ).classes("w-full")
            amount = ui.input("Оклад, ₽" if salary else "Сумма, ₽").props("inputmode=decimal").classes("w-full")
            if not salary:
                basis = ui.input("Основание / документ").props("maxlength=300").classes("w-full")
                monthly = ui.checkbox("Повторять ежемесячно")
                end = (
                    ui.input("Последний месяц (необязательно)")
                    .props("type=month")
                    .bind_visibility_from(monthly, "value")
                )
                consent = ui.checkbox("Есть заявление сотрудника на взнос").bind_visibility_from(
                    kind, "value", backward=lambda k: k in types and types[k][1] == "voluntary"
                )
                as_percent = ui.checkbox("Процент от начислений до удержаний").bind_visibility_from(
                    kind, "value", backward=lambda k: k in types and types[k][1] in ("debit", "voluntary")
                )
                percent = (
                    ui.input("Ставка, %", value="1")
                    .props("inputmode=decimal")
                    .bind_visibility_from(as_percent, "value")
                )
                amount.bind_visibility_from(as_percent, "value", backward=lambda v: not v)

                def type_changed():
                    as_percent.value = kind.value == "union"
                    consent.value = False
                    if kind.value == "union":
                        monthly.value = True

                kind.on_value_change(type_changed)

            async def apply():
                if not employee.value:
                    raise ValueError("Выберите сотрудника")
                if salary:
                    await call(service.set_salary, scope, when, employee.value, amount.value)
                else:
                    await call(
                        service.add_item,
                        scope,
                        when,
                        employee.value,
                        kind.value,
                        amount.value,
                        basis.value,
                        monthly=monthly.value,
                        until=end.value or "",
                        consent=consent.value,
                        percent=percent.value if as_percent.value else None,
                    )

            save_buttons(modal, apply)
        modal.open()

    def stop():
        if not ledger.selected:
            return
        selected = ledger.selected[0]
        scope, when = org.value, month.value
        with ui.dialog() as modal, ui.card().classes("w-full max-w-sm"):
            ui.label(f"Отменить «{selected['label']}» для {selected['name']} с {when}?")
            save_buttons(
                modal, lambda: call(service.stop_item, scope, when, selected["id"]), "Отменить с месяца"
            )
        modal.open()

    with ui.dialog() as dialog, ui.card().classes("w-full max-w-6xl"):
        ui.label("Расчёт за месяц").classes("text-lg")
        with ui.row().classes("items-center"):
            org = ui.select(
                organizations,
                label="Организация журнала",
                value=next(iter(organizations)) if len(organizations) == 1 else None,
            )
            org.set_visibility(len(organizations) > 1)
            month = ui.input("Расчётный месяц", value=date.today().strftime("%Y-%m")).props("type=month")
            norm_text = ui.label().classes("text-sm")
        with ui.row():
            actions = [
                ui.button(label, icon=icon, on_click=action).props("flat dense")
                for label, icon, action in (
                    ("Норма месяца", "calendar_month", calendar_editor),
                    ("Оклад", "payments", lambda: entry_editor(True)),
                    ("Добавить операцию", "add", lambda: entry_editor(False)),
                    ("Новый вид", "playlist_add", type_editor),
                    ("Новый тариф", "add_card", rate_editor),
                )
            ]
        with ui.tabs() as tabs:
            result_tab = ui.tab("Предварительный расчёт")
            items_tab = ui.tab("Начисления и удержания")
            rates_tab = ui.tab("Тарифы")
        with ui.tab_panels(tabs, value=result_tab).classes("w-full"):
            with ui.tab_panel(rates_tab):
                tariffs = ui.table(columns=columns((("work","Вид работы"),("unit","Единица"),("price","Ставка, ₽"),
                    ("equipment","Техника"),("workplace","Объект"),("since","Действует с"))),rows=[],row_key="id",
                    pagination=15).props("flat").classes("w-full")
            with ui.tab_panel(result_tab):
                totals = (
                    ui.table(
                        columns=columns(
                            (
                                ("name", "Сотрудник"),
                                ("approved", "Утверждено, ч"),
                                ("pending", "На проверке, ч"),
                                ("delta", "К норме, ч"),
                                ("salary", "Оклад, ₽"),
                                ("base", "По часам / работам, ₽"),
                                ("extra", "В т.ч. сверх нормы, ₽"),
                                ("credit", "Начисления, ₽"),
                                ("debit", "Удержания, ₽"),
                                ("voluntary", "Взносы, ₽"),
                                ("estimate", "Итого, ₽"),
                            )
                        ),
                        rows=[],
                        row_key="employee",
                        pagination=15,
                    )
                    .props("flat")
                    .classes("w-full")
                )
            with ui.tab_panel(items_tab):
                ledger = (
                    ui.table(
                        columns=columns(
                            (
                                ("name", "Сотрудник"),
                                ("label", "Вид"),
                                ("kind", "Группа"),
                                ("amount", "Сумма, ₽"),
                                ("rate", "Ставка"),
                                ("earnings", "База, ₽"),
                                ("period", "Период"),
                                ("basis", "Основание"),
                                ("actor", "Внёс"),
                                ("state", "Статус"),
                            )
                        ),
                        rows=[],
                        row_key="id",
                        selection="single",
                        pagination=15,
                        on_select=lambda: remove.set_enabled(
                            bool(ledger.selected and not ledger.selected[0]["stopped"])
                        ),
                    )
                    .props("flat wrap-cells")
                    .classes("w-full")
                )
                remove = ui.button("Отменить с месяца", icon="event_busy", on_click=stop).props("flat")
        org.on_value_change(refresh)
        month.on_value_change(refresh)
        ui.button("Закрыть", on_click=dialog.close).props("flat")
    dialog.on("hide", dialog.delete)
    dialog.open()
    await refresh()
