from io import BytesIO

import pytest
from openpyxl import load_workbook
from test_waybill_documents import service as service

from rag_catalog.core import waybill_forms as forms
from rag_catalog.core.waybill_documents import _current_template, _original_template
from rag_catalog.core.waybill_templates import workbook_bytes


def test_saved_waybill_keeps_previous_layout_after_builder_edit(service):
    store, shift = service
    old = store.create_template("dispatcher", "one", "Рейсы", ["employee_name"], ["Груз"], metric_fields=["trips"])
    draft = store.draft("driver", shift, old)
    saved = store.save("driver", shift, old, {"field_1": "Щебень", "trips": "3"}, 0, draft["shift_revision"])
    spec = store.template_editor("dispatcher", "one", old)
    assert spec["builder"] and spec["fields"]["field_1"] == "Груз"
    preview = store.preview_builder("dispatcher", "one", "Рейсы", ["employee_name", "partner"],
                                    ["Новый груз"], metric_fields=["tonnes"], replace=old)
    assert "Новый груз" in {c.value for c in load_workbook(BytesIO(preview)).active["A"]}
    assert old in store.templates("driver", "one")
    revised = store.create_template("dispatcher", "one", "Рейсы", ["employee_name", "partner"],
                                    ["Новый груз"], metric_fields=["tonnes"], replace=old)
    assert old not in store.templates("driver", "one")
    assert store.templates("driver", "one", shift)[old] == "Рейсы (прежний макет)"
    assert store.fields("driver", "one", revised)["field_1"] == "Новый груз"
    assert "tonnes" in store.fields("driver", "one", revised)
    previous = load_workbook(BytesIO(store.export("driver", shift, old, saved["revision"], saved["shift_revision"])))
    assert "Щебень" in {c.value for c in previous.active["B"]}
    assert "Груз" in {c.value for c in previous.active["A"]}
    assert load_workbook(BytesIO(store.book("driver", "one"))).sheetnames.count("Рейсы") == 1
    with pytest.raises(ValueError, match="изменился"):
        store.create_template("dispatcher", "one", "Рейсы", ["employee_name"], [], replace=old)
    assert _latest(store, old) == revised
    with pytest.raises(PermissionError):
        store.template_editor("driver", "one", revised)
    with pytest.raises(ValueError):
        store.template_editor("second", "two", revised)


def _latest(store, key):
    with store.journal._connection() as conn:
        return _current_template(conn, "one", key)


def test_built_in_layout_can_be_edited_and_reimported_with_split_dates_and_both_pages(service):
    store, shift = service
    first = "truck_4s"
    original = store.template_editor("dispatcher", "one", first)
    assert not original["builder"] and original["cells"]["BG5"] == "{{work_date.day}}"
    change = {"Q6": "Перевозчик {{organization}}", "DP35": "Груз {{cargo}}"}
    preview = load_workbook(BytesIO(store.preview_cells("dispatcher", "one", first, change)))
    assert preview.active["Q6"].value == "Перевозчик "
    assert first in store.templates("dispatcher", "one")
    v2 = store.revise_cells("dispatcher", "one", first, change)
    assert _latest(store, first) == v2
    assert first not in store.templates("driver", "one")
    assert first in store.templates("second", "two")
    fields = store.fields("driver", "one", v2)
    assert fields.keys() == forms.form_fields(first).keys()
    assert fields["cargo"] == forms.form_fields(first)["cargo"]
    source = load_workbook(BytesIO(store.editable_template("dispatcher", "one", v2)))
    assert source.active["BG5"].value == "{{work_date.day}}"
    source.active["DP35"] = "Материал {{cargo}}"
    third = store.add_template("dispatcher", "one", original["name"], workbook_bytes(source), replace=v2)
    assert _latest(store, first) == third
    with pytest.raises(ValueError, match="обновлён"):
        store.draft("driver", shift, v2)
    draft = store.draft("driver", shift, third)
    state = store.save("driver", shift, third, {"cargo": "=123"}, 0, draft["shift_revision"])
    filled = load_workbook(BytesIO(store.export("driver", shift, third, state["revision"], state["shift_revision"]))).active
    assert [filled[c].value for c in ("BG5", "BP5", "CL5", "Q6", "DP35")] == [
        "14", "11", "2026", "Перевозчик Первая", "Материал =123"]
    assert len(filled._print_area.ranges) == 2


def test_legacy_constructor_recognition_and_isolation(service):
    store, _ = service
    key = store.create_template("dispatcher", "one", "Смена", ["partner"], ["Плечо"])
    with store.journal._connection() as conn:
        conn.execute("UPDATE waybill_templates SET layout_type='legacy'")
    assert store.template_editor("dispatcher", "one", key)["builder"]
    with pytest.raises(PermissionError):
        store.revise_cells("driver", "one", key, {"A1": "Новый"})
    with pytest.raises(ValueError):
        store.revise_cells("second", "two", key, {"A1": "Новый"})


def test_operator_can_keep_a_distinct_copy_for_each_customer(service):
    store, _ = service
    original = "truck_4s"
    fork = store.revise_cells("dispatcher", "one", original, {}, copy_name="Сдельный заказчика")
    v2 = store.revise_cells("dispatcher", "one", fork, {"DP35": "Материал {{cargo}}"})
    available = store.templates("dispatcher", "one")
    assert original in available and v2 in available and fork not in available
    assert v2 not in store.templates("second", "two")
    with store.journal._connection() as conn:
        assert _original_template(conn, "one", v2) == original
        assert _current_template(conn, "one", original) == original
        assert _current_template(conn, "one", fork) == v2
    own = store.create_template("dispatcher", "one", "Карьер", ["partner"], [], metric_fields=["trips"])
    variant = store.create_template("dispatcher", "one", "Площадка", ["partner"], [],
                                    metric_fields=["distance"], copy_from=own)
    assert {own, variant} <= store.templates("dispatcher", "one").keys()
