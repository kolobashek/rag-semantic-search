"""Work lines in a shift draft; the journal verifies the price again on save."""

import json
from decimal import Decimal, InvalidOperation

from nicegui import ui

from rag_catalog.core.shift_operations import UNITS


def shift_work_editor(journal, call, org, fields, row, managed_orgs, report):
    original = json.loads(row.get("work_lines", "[]"))
    entries = []
    available = {}
    generation = 0

    def context():
        return (org.value, fields["work_date"].value, fields["equipment"].value or "", fields["workplace"].value or "")

    def label(rate):
        return f'{rate["work"]} · {rate["kopecks"] / 100:.2f} ₽/{UNITS[rate["unit"]]}'

    def totals():
        try:
            total = sum((Decimal(str(q.value or 0)) * available[r.value]["kopecks"]).quantize(
                Decimal("1"), rounding="ROUND_HALF_UP") for r, q, _ in entries if r.value in available)
            amount.set_text(f"Начисление фактическому исполнителю: {total / 100:.2f} ₽")
        except (InvalidOperation, ValueError):
            amount.set_text("Проверьте объём работ")

    def add_line(rate_id=None, qty=None):
        with lines, ui.row().classes("w-full items-end gap-2 flex-nowrap") as block:
            options = {k: label(v) for k,v in available.items()}
            rate = ui.select(options, label="Вид работы и тариф", with_input=True,
                             value=rate_id if rate_id in options else None, on_change=totals).classes("min-w-0 flex-1")
            quantity = ui.number("Объём", value=qty, min=0, on_change=totals).classes("w-20")
            def remove():
                entries.remove((rate, quantity, block))
                block.delete()
                totals()
            ui.button(icon="delete_outline", on_click=remove).props('flat dense round aria-label="Убрать строку работы"')
            entries.append((rate, quantity, block))
        totals()

    async def refresh():
        nonlocal available, generation
        generation += 1
        version = generation
        scope, day, equipment, workplace = ctx = context()
        create.set_visibility(scope in managed_orgs)
        try:
            found = await call(journal.rates, organization_id=scope, work_date=day, equipment=equipment,
                               workplace=workplace) if scope and day else []
        except Exception as exc:
            if generation == version:
                report(exc)
            return
        if version != generation or ctx != context():
            return
        available = {r["id"]: r for r in found}
        if ctx == tuple(row.get(k) for k in ("organization_id","work_date","equipment","workplace")):
            available.update({r["rate_id"]: r for r in original})
        options = {k: label(v) for k,v in available.items()}
        for rate, _, _ in entries:
            rate.set_options(options, value=rate.value if rate.value in options else None)
        add.set_enabled(bool(available))
        totals()

    def new_rate():
        scope, day, equipment, workplace = context()
        if scope not in managed_orgs:
            return
        with ui.dialog() as modal, ui.card().classes("w-full max-w-md"):
            ui.label("Новый тариф").classes("text-lg")
            work = ui.input("Вид работы").props("maxlength=200").classes("w-full")
            unit = ui.select(UNITS, value="hour", label="Единица расчёта").classes("w-full")
            price = ui.number("Ставка, ₽ за единицу", min=0, step=0.01)
            since = ui.input("Действует с", value=day).props("type=date")
            eq = ui.checkbox(f"Только техника: {equipment}", value=bool(equipment))
            eq.set_visibility(bool(equipment))
            wp = ui.checkbox(f"Только объект: {workplace}", value=bool(workplace))
            wp.set_visibility(bool(workplace))
            async def save():
                button.disable()
                try:
                    key = await call(journal.add_rate, organization_id=scope, work=work.value, unit=unit.value,
                                     price=price.value, since=since.value, equipment=equipment if eq.value else "",
                                     workplace=workplace if wp.value else "")
                    modal.close()
                    await refresh()
                    if key in available:
                        add_line(key, 1)
                except Exception as exc:
                    report(exc)
                finally:
                    button.enable()
            with ui.row():
                ui.button("Отмена", on_click=modal.close).props("flat")
                button = ui.button("Сохранить тариф", icon="save", on_click=save)
        modal.on("hide", modal.delete)
        modal.open()

    with ui.expansion("Работы и начисление", icon="calculate", value=bool(original)).classes("w-full"):
        lines = ui.column().classes("w-full gap-1")
        with ui.row():
            add = ui.button("Строка работы", icon="add", on_click=lambda: add_line()).props("flat dense")
            add.disable()
            create = ui.button("Новый тариф", icon="add_card", on_click=new_rate).props("flat dense")
            create.set_visibility(False)
        amount = ui.label().classes("text-sm")
        available = {r["rate_id"]: r for r in original}
        for item in original:
            add_line(item["rate_id"], float(item["quantity"]))
        totals()
    for key in ("work_date","equipment","workplace"):
        fields[key].on_value_change(refresh)
    return refresh, lambda: [dict(rate_id=r.value, quantity=q.value) for r,q,_ in entries]
