"""Operational work quantities. Pricing is confined to the payroll screen."""

import json

from nicegui import ui

from rag_catalog.core.shift_operations import UNITS


def shift_work_editor(journal, call, org, fields, row, report):
    original = json.loads(row.get("work_lines", "[]"))
    entries = []
    generation = 0
    available = {r["rate_id"]: r for r in original}

    def context():
        return (org.value, fields["work_date"].value, fields["equipment"].value or "", fields["workplace"].value or "")

    def options():
        return {k: f'{r["work"]} · {UNITS[r["unit"]]}' for k, r in available.items()}

    def add_line(rate_id=None, qty=None):
        with lines, ui.row().classes("w-full items-end gap-2 flex-nowrap") as block:
            rate = ui.select(options(), label="Вид работы", with_input=True, value=rate_id).classes("min-w-0 flex-1")
            quantity = ui.number("Объём", value=qty, min=0).classes("w-20")

            def remove():
                entries.remove((rate, quantity))
                block.delete()

            ui.button(icon="delete_outline", on_click=remove).props('flat dense round aria-label="Убрать строку работы"')
            entries.append((rate, quantity))

    async def refresh():
        nonlocal available, generation
        generation += 1
        version = generation
        scope, day, equipment, workplace = ctx = context()
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
        for rate, _ in entries:
            rate.set_options(options(), value=rate.value if rate.value in available else None)
        add.set_enabled(bool(available))

    with ui.expansion("Выполненные работы", icon="list", value=bool(original)).classes("w-full"):
        lines = ui.column().classes("w-full gap-1")
        add = ui.button("Строка работы", icon="add", on_click=lambda: add_line()).props("flat dense")
        add.disable()
        for item in original:
            add_line(item["rate_id"], float(item["quantity"]))
    for key in ("work_date","equipment","workplace"):
        fields[key].on_value_change(refresh)
    return refresh, lambda: [dict(rate_id=r.value, quantity=q.value) for r,q in entries]
