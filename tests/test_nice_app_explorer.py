from __future__ import annotations

from dataclasses import dataclass

from rag_catalog.ui.nice_app import PageState, _apply_explorer_filter_input, _file_rows


@dataclass
class _Event:
    value: str


def test_explorer_filter_uses_current_input_event_value(tmp_path) -> None:
    (tmp_path / "Invoices").mkdir()
    (tmp_path / "Passports").mkdir()
    (tmp_path / "invoice.pdf").write_text("x", encoding="utf-8")
    (tmp_path / "passport.pdf").write_text("x", encoding="utf-8")

    state = PageState(cfg={})
    state.explorer_filter = "old stale value"
    state.explorer_page = 3

    _apply_explorer_filter_input(state, _Event("pass"), fallback="old stale value")

    dirs, files, total_files = _file_rows(tmp_path, state)
    assert state.explorer_filter == "pass"
    assert state.explorer_page == 0
    assert [path.name for path in dirs] == ["Passports"]
    assert [path.name for path in files] == ["passport.pdf"]
    assert total_files == 1
