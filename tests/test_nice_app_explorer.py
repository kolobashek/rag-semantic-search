from __future__ import annotations

from dataclasses import dataclass

from rag_catalog.ui.nice_app import (
    PageState,
    _apply_explorer_filter_input,
    _file_rows,
    _file_icon_svg,
    _is_system_file,
    _read_index_stats,
    _read_index_telemetry,
    _save_config_patch,
    _normalize_search_results,
    _run_catalog_search,
)
import rag_catalog.ui.nice_app as nice_app
from rag_catalog.core.telemetry_db import TelemetryDB


@dataclass
class _Event:
    value: str


class _AuthDB:
    def __init__(self, show_system: bool) -> None:
        self.show_system = show_system

    def get_show_system_files_for_admin(self) -> bool:
        return self.show_system


class _SearchBackend:
    def __init__(self, search_result: object, lexical_result: list[dict[str, object]]) -> None:
        self.search_result = search_result
        self.lexical_result = lexical_result

    def search(self, *args: object, **kwargs: object) -> object:
        return self.search_result

    def _lexical_catalog_search(self, **kwargs: object) -> list[dict[str, object]]:
        return self.lexical_result


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


def test_search_results_are_normalized_when_backend_returns_none() -> None:
    assert _normalize_search_results(None) == []
    assert _normalize_search_results({"filename": "passport.pdf"}) == []
    assert _normalize_search_results([{"filename": "passport.pdf"}, None, "bad"]) == [{"filename": "passport.pdf"}]


def test_catalog_search_uses_lexical_fallback_when_backend_returns_none() -> None:
    backend = _SearchBackend(
        search_result=None,
        lexical_result=[{"filename": "passport.pdf"}, {"filename": "passport folder"}],
    )

    assert _run_catalog_search(  # type: ignore[arg-type]
        backend,
        query="паспорт",
        limit=10,
        file_type=None,
        content_only=False,
    ) == [{"filename": "passport.pdf"}, {"filename": "passport folder"}]


def test_system_files_are_hidden_for_regular_users(tmp_path) -> None:
    (tmp_path / "manual.pdf").write_text("x", encoding="utf-8")
    (tmp_path / "driver.dll").write_text("x", encoding="utf-8")
    (tmp_path / "setup.dl_").write_text("x", encoding="utf-8")

    state = PageState(cfg={})
    state.current_user = {"username": "user", "role": "user"}

    _dirs, files, total_files = _file_rows(tmp_path, state)
    assert [path.name for path in files] == ["manual.pdf"]
    assert total_files == 1


def test_admin_can_show_system_files(tmp_path) -> None:
    (tmp_path / "manual.pdf").write_text("x", encoding="utf-8")
    (tmp_path / "driver.dll").write_text("x", encoding="utf-8")
    (tmp_path / "setup.dl_").write_text("x", encoding="utf-8")

    state = PageState(cfg={})
    state.current_user = {"username": "admin", "role": "admin"}
    state.auth_db = _AuthDB(show_system=True)  # type: ignore[assignment]

    _dirs, files, total_files = _file_rows(tmp_path, state)
    assert [path.name for path in files] == ["driver.dll", "manual.pdf", "setup.dl_"]
    assert total_files == 3


def test_system_file_icons_are_muted() -> None:
    assert _is_system_file("driver.dll")
    assert _is_system_file("setup.dl_")
    assert 'class="rag-file-icon system"' in _file_icon_svg("driver.dll")
    assert 'class="rag-file-icon"' in _file_icon_svg("manual.pdf")


def test_index_stats_include_total_size(tmp_path) -> None:
    qdrant_dir = tmp_path / "qdrant"
    qdrant_dir.mkdir()
    catalog = tmp_path / "catalog"
    catalog.mkdir()
    pdf = catalog / "a.pdf"
    pdf.write_bytes(b"12345")
    docx = catalog / "b.docx"
    docx.write_bytes(b"123")
    (qdrant_dir / "index_state.json").write_text(
        '{"files": {' + f'{pdf.as_posix()!r}: {{}}, {docx.as_posix()!r}: {{}}'.replace("'", '"') + "}}",
        encoding="utf-8",
    )

    stats = _read_index_stats({"qdrant_db_path": str(qdrant_dir)})

    assert stats["found"] is True
    assert stats["total"] == 2
    assert stats["total_size_bytes"] == 8
    assert stats["by_ext"][".pdf"] == 1


def test_index_telemetry_reads_stage_and_ocr_progress(tmp_path) -> None:
    db_path = tmp_path / "telemetry.db"
    db = TelemetryDB(str(db_path))
    run_id = db.start_index_run(catalog_path="O:\\Обмен", collection_name="catalog", recreate=False)
    db.start_stage(run_id=run_id, stage="metadata", total_files=10)
    db.update_stage(
        run_id=run_id,
        stage="metadata",
        processed_files=4,
        added_files=2,
        updated_files=1,
        skipped_files=1,
        error_files=0,
        points_added=12,
    )
    ocr_id = db.start_ocr_run(collection_name="catalog", found_scanned=5)
    db.update_ocr_progress(ocr_run_id=ocr_id, processed_pdfs=2)

    telemetry = _read_index_telemetry({"telemetry_db_path": str(db_path)})

    assert telemetry["active_runs"][0]["run_id"] == run_id
    assert telemetry["active_stages"][0]["processed_files"] == 4
    assert telemetry["active_ocr"]["processed_pdfs"] == 2


def test_save_config_patch_only_updates_allowed_path_keys(monkeypatch) -> None:
    saved = {}

    monkeypatch.setattr(
        nice_app,
        "load_config",
        lambda: {
            "catalog_path": "old",
            "qdrant_url": "http://old",
            "telegram_bot_token": "secret",
        },
    )
    monkeypatch.setattr(nice_app, "save_config", lambda cfg: saved.update(cfg))

    result = _save_config_patch(
        {
            "catalog_path": "new",
            "qdrant_url": "",
            "telegram_bot_token": "leak",
            "unknown": "ignored",
        }
    )

    assert result["catalog_path"] == "new"
    assert result["qdrant_url"] == ""
    assert result["telegram_bot_token"] == "secret"
    assert "unknown" not in result
    assert saved == result
