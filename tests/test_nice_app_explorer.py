from __future__ import annotations

import io
from dataclasses import dataclass
from tempfile import SpooledTemporaryFile

import pytest
from fastapi import HTTPException

from rag_catalog.ui.state import (
    PageState,
    _save_config_patch,
)
from rag_catalog.ui.helpers import (
    _apply_explorer_filter_input,
    _file_rows,
    _file_icon_svg,
    _is_system_file,
    _read_index_stats,
    _read_index_telemetry,
    _normalize_search_results,
    _run_catalog_search,
)
from rag_catalog.ui.system import (
    _recover_background_tasks,
    _resolve_index_recovery_stage,
)
from rag_catalog.ui.api import (
    api_cloud_drive_bootstrap_jobs,
    api_cloud_drive_bootstrap_status,
    api_cloud_drive_create_folder,
    api_cloud_drive_download,
    api_cloud_drive_delete,
    api_cloud_drive_job,
    api_cloud_drive_file_statuses,
    api_cloud_drive_job_latest,
    api_cloud_drive_job_run,
    api_cloud_drive_job_retry,
    api_cloud_drive_jobs,
    api_cloud_drive_move,
    api_cloud_drive_reindex,
    api_cloud_drive_rename,
    api_cloud_drive_upload,
    api_cloud_drive_list,
    api_cloud_drive_node,
    api_cloud_drive_versions,
    api_cloud_drive_storage_health,
)
import rag_catalog.ui.api as cloud_api
import rag_catalog.ui.state as ui_state
import rag_catalog.ui.system as ui_system
from rag_catalog.core.index_state_db import IndexStateDB
from rag_catalog.core.telemetry_db import TelemetryDB
from rag_catalog.core.cloud_drive.service import CloudDriveService
from rag_catalog.core.user_auth_db import UserAuthDB
from starlette.datastructures import UploadFile


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
        self.search_kwargs: dict[str, object] = {}
        self.lexical_kwargs: dict[str, object] = {}

    def search(self, *args: object, **kwargs: object) -> object:
        self.search_kwargs = dict(kwargs)
        return self.search_result

    def _lexical_catalog_search(self, **kwargs: object) -> list[dict[str, object]]:
        self.lexical_kwargs = dict(kwargs)
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
        query_original="паспорт",
        query_used="паспорт",
        limit=10,
        file_type=None,
        content_only=False,
        title_only=False,
    ) == [{"filename": "passport.pdf"}, {"filename": "passport folder"}]


def test_catalog_search_passes_title_only_and_original_query_to_backend() -> None:
    backend = _SearchBackend(
        search_result=[{"filename": "meta.docx", "type": "file_metadata"}],
        lexical_result=[],
    )

    out = _run_catalog_search(  # type: ignore[arg-type]
        backend,
        query="expanded",
        query_original="typed",
        query_used="expanded",
        limit=5,
        file_type=None,
        content_only=False,
        title_only=True,
    )

    assert out == [{"filename": "meta.docx", "type": "file_metadata"}]
    assert backend.search_kwargs["query_original"] == "typed"
    assert backend.search_kwargs["title_only"] is True


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
    state_db = IndexStateDB(str(qdrant_dir / "index_state.db"))
    state_db.upsert_many(
        [
            {
                "full_path": str(tmp_path / "catalog" / "a.pdf"),
                "fingerprint": "5_1",
                "mtime": 1.0,
                "stage": "content",
                "size_bytes": 5,
                "extension": ".pdf",
            },
            {
                "full_path": str(tmp_path / "catalog" / "b.docx"),
                "fingerprint": "3_1",
                "mtime": 1.0,
                "stage": "content",
                "size_bytes": 3,
                "extension": ".docx",
            },
        ]
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


def test_resolve_index_recovery_stage_prefers_running_stage_over_note(tmp_path) -> None:
    db = TelemetryDB(str(tmp_path / "telemetry.db"))
    run_id = db.start_index_run(
        catalog_path="O:\\Обмен",
        collection_name="catalog",
        recreate=False,
        note="stage=large",
        worker_pid=4242,
    )
    db.start_stage(run_id=run_id, stage="metadata", total_files=10)

    stage = _resolve_index_recovery_stage(db, {"run_id": run_id, "note": "stage=large"})

    assert stage == "metadata"


def test_recover_background_tasks_restarts_dead_index_and_ocr(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "telemetry.db"
    db = TelemetryDB(str(db_path))
    index_run_id = db.start_index_run(
        catalog_path="O:\\Обмен",
        collection_name="catalog",
        recreate=False,
        note="stage=small",
        worker_pid=10101,
    )
    db.start_stage(run_id=index_run_id, stage="small", total_files=100)
    ocr_run_id = db.start_ocr_run(
        collection_name="catalog",
        found_scanned=4,
        note="min_text_len=50",
        worker_pid=20202,
    )
    calls: dict[str, object] = {}

    monkeypatch.setattr(ui_system, "_is_process_alive", lambda pid: False)

    def _fake_launch_indexer(cfg, **kwargs):
        calls["index"] = {"cfg": dict(cfg), "kwargs": dict(kwargs)}
        return 33333

    def _fake_launch_ocr(cfg, **kwargs):
        calls["ocr"] = {"cfg": dict(cfg), "kwargs": dict(kwargs)}
        return 44444

    monkeypatch.setattr(ui_system, "_launch_indexer", _fake_launch_indexer)
    monkeypatch.setattr(ui_system, "_launch_ocr", _fake_launch_ocr)

    _recover_background_tasks(
        {
            "telemetry_db_path": str(db_path),
            "qdrant_db_path": str(tmp_path / "qdrant"),
            "index_read_workers": 3,
            "index_max_chunks": 777,
        }
    )

    index_row = db.fetch_dicts("SELECT status, note FROM index_runs WHERE run_id=?", [index_run_id])[0]
    ocr_row = db.fetch_dicts("SELECT status, note FROM ocr_runs WHERE ocr_run_id=?", [ocr_run_id])[0]
    assert index_row["status"] == "cancelled"
    assert "server_restart_recovery" in str(index_row["note"] or "")
    assert ocr_row["status"] == "cancelled"
    assert "server_restart_recovery" in str(ocr_row["note"] or "")
    assert calls["index"]["kwargs"]["stage"] == "small"  # type: ignore[index]
    assert "ocr" not in calls


def test_recover_background_tasks_does_not_restart_when_process_is_alive(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "telemetry.db"
    db = TelemetryDB(str(db_path))
    db.start_index_run(
        catalog_path="O:\\Обмен",
        collection_name="catalog",
        recreate=False,
        note="stage=all",
        worker_pid=30303,
    )
    db.start_ocr_run(
        collection_name="catalog",
        found_scanned=5,
        note="min_text_len=60",
        worker_pid=40404,
    )
    calls = {"index": 0, "ocr": 0}

    monkeypatch.setattr(ui_system, "_is_process_alive", lambda pid: True)
    monkeypatch.setattr(ui_system, "_launch_indexer", lambda cfg, **kwargs: calls.__setitem__("index", calls["index"] + 1) or 1)
    monkeypatch.setattr(ui_system, "_launch_ocr", lambda cfg, **kwargs: calls.__setitem__("ocr", calls["ocr"] + 1) or 1)

    _recover_background_tasks({"telemetry_db_path": str(db_path), "qdrant_db_path": str(tmp_path / "qdrant")})

    assert calls["index"] == 0
    assert calls["ocr"] == 0
    active_index = db.get_active_index_run()
    active_ocr = db.get_active_ocr_run()
    assert active_index is not None and active_index["status"] == "running"
    assert active_ocr is not None and active_ocr["status"] == "running"


def test_save_config_patch_only_updates_allowed_path_keys(monkeypatch) -> None:
    saved = {}

    monkeypatch.setattr(
        ui_state,
        "load_config",
        lambda: {
            "catalog_path": "old",
            "qdrant_url": "http://old",
            "telegram_bot_token": "secret",
        },
    )
    monkeypatch.setattr(ui_state, "save_config", lambda cfg: saved.update(cfg))

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


def test_cloud_drive_bootstrap_status_api_reads_current_status(monkeypatch) -> None:
    cfg = {"cloud_drive_db_path": "D:/cloud_drive.db"}
    expected = {"status": "running", "files_imported": 12}

    monkeypatch.setattr(cloud_api, "load_config", lambda: dict(cfg))
    monkeypatch.setattr(cloud_api, "_require_cloud_drive_api_user", lambda *_args, **_kwargs: {"username": "admin", "role": "admin", "status": "active"})
    monkeypatch.setattr(cloud_api, "_read_cloud_bootstrap_status", lambda current_cfg: expected if current_cfg == cfg else {})

    assert api_cloud_drive_bootstrap_status() == expected


def test_cloud_drive_bootstrap_status_returns_idle_without_jobs(monkeypatch, tmp_path) -> None:
    cfg = {
        "cloud_drive_db_path": str(tmp_path / "cloud_drive.db"),
        "cloud_drive_storage": "local",
        "cloud_drive_storage_root": str(tmp_path / "storage"),
    }
    monkeypatch.setattr(cloud_api, "load_config", lambda: dict(cfg))
    monkeypatch.setattr(cloud_api, "_require_cloud_drive_api_user", lambda *_args, **_kwargs: {"username": "admin", "role": "admin", "status": "active"})

    status = api_cloud_drive_bootstrap_status()

    assert status["status"] == "idle"
    assert status["job_status"] == "idle"


def test_cloud_drive_bootstrap_jobs_api_returns_serialized_jobs(monkeypatch, tmp_path) -> None:
    cfg = {
        "cloud_drive_db_path": str(tmp_path / "cloud_drive.db"),
        "cloud_drive_storage": "local",
        "cloud_drive_storage_root": str(tmp_path / "storage"),
    }
    service = CloudDriveService.from_config(cfg)
    job = service.create_bootstrap_job(catalog_root="O:/Обмен", max_files=25, import_files=True)
    service.registry.update_job(
        job.id,
        status="running",
        payload={
            **job.payload,
            "progress": {
                "status": "running",
                "files_imported": 7,
                "total_files": 25,
            },
        },
        attempts=2,
    )

    monkeypatch.setattr(
        cloud_api,
        "load_config",
        lambda: dict(cfg),
    )
    monkeypatch.setattr(cloud_api, "_require_cloud_drive_api_user", lambda *_args, **_kwargs: {"username": "admin", "role": "admin", "status": "active"})

    jobs = api_cloud_drive_bootstrap_jobs(limit=1)

    assert len(jobs) == 1
    assert jobs[0]["id"] == job.id
    assert jobs[0]["job_type"] == "bootstrap"
    assert jobs[0]["status"] == "running"
    assert jobs[0]["attempts"] == 2
    assert jobs[0]["progress"]["files_imported"] == 7


def test_cloud_drive_storage_health_api(monkeypatch, tmp_path) -> None:
    cfg = {
        "cloud_drive_db_path": str(tmp_path / "cloud_drive.db"),
        "cloud_drive_storage": "local",
        "cloud_drive_storage_root": str(tmp_path / "storage"),
    }
    monkeypatch.setattr(cloud_api, "load_config", lambda: dict(cfg))
    monkeypatch.setattr(cloud_api, "_require_cloud_drive_api_user", lambda *_args, **_kwargs: {"username": "admin", "role": "admin", "status": "active"})

    health = api_cloud_drive_storage_health()

    assert health["backend"] == "local"
    assert health["ok"] is True
    assert health["writable"] is True


def test_cloud_drive_jobs_api_returns_serialized_jobs(monkeypatch, tmp_path) -> None:
    cfg = {
        "cloud_drive_db_path": str(tmp_path / "cloud_drive.db"),
        "cloud_drive_storage": "local",
        "cloud_drive_storage_root": str(tmp_path / "storage"),
    }
    service = CloudDriveService.from_config(cfg)
    bootstrap = service.registry.queue_job(job_type="bootstrap", status="running", payload={"progress": {"step": 1}})
    reindex = service.registry.queue_job(job_type="reindex", status="pending", payload={"scope": "file"})
    monkeypatch.setattr(cloud_api, "load_config", lambda: dict(cfg))
    monkeypatch.setattr(cloud_api, "_require_cloud_drive_api_user", lambda *_args, **_kwargs: {"username": "admin", "role": "admin", "status": "active"})

    jobs = api_cloud_drive_jobs(limit=10)
    latest = api_cloud_drive_job_latest(job_type="bootstrap")
    fetched = api_cloud_drive_job(job_id=reindex.id)

    assert len(jobs) == 2
    assert latest["id"] == bootstrap.id
    assert latest["started_at"] != ""
    assert fetched["id"] == reindex.id
    assert fetched["job_type"] == "reindex"


def test_cloud_drive_file_statuses_api_returns_latest_job_by_file(monkeypatch, tmp_path) -> None:
    cfg = {
        "cloud_drive_db_path": str(tmp_path / "cloud_drive.db"),
        "cloud_drive_storage": "local",
        "cloud_drive_storage_root": str(tmp_path / "storage"),
        "telemetry_db_path": str(tmp_path / "telemetry.db"),
    }
    service = CloudDriveService.from_config(cfg)
    root = service.registry.ensure_root_folder(root_name="Обмен", source_path="")
    folder = service.registry.upsert_folder(path="Folder A", name="Folder A", parent_id=root.id, depth=1, source_path="")
    file_row = service.registry.upsert_file(
        folder_id=folder.id,
        path="Folder A/hello.txt",
        name="hello.txt",
        storage_key="Folder A/hello.txt",
        mime_type="text/plain",
        size_bytes=5,
        checksum="abc",
        source_path="",
    )
    service.registry.queue_job(job_type="reindex", status="pending", file_id=file_row.id, payload={"progress": {"status": "pending"}})
    latest = service.registry.queue_job(job_type="reindex", status="running", file_id=file_row.id, payload={"progress": {"status": "running"}})
    monkeypatch.setattr(cloud_api, "load_config", lambda: dict(cfg))
    monkeypatch.setattr(cloud_api, "_require_cloud_drive_api_user", lambda *_args, **_kwargs: {"username": "user", "role": "user", "status": "active"})

    statuses = api_cloud_drive_file_statuses(paths="Folder A/hello.txt")

    assert statuses[file_row.id]["id"] == latest.id
    assert statuses[file_row.id]["status"] == "running"


def test_cloud_drive_node_and_list_api(monkeypatch, tmp_path) -> None:
    cfg = {
        "cloud_drive_db_path": str(tmp_path / "cloud_drive.db"),
        "cloud_drive_storage": "local",
        "cloud_drive_storage_root": str(tmp_path / "storage"),
    }
    service = CloudDriveService.from_config(cfg)
    root = service.registry.ensure_root_folder(root_name="Обмен", source_path="O:/Обмен")
    child = service.registry.upsert_folder(
        path="Folder A",
        name="Folder A",
        parent_id=root.id,
        depth=1,
        source_path="O:/Обмен/Folder A",
    )
    service.registry.upsert_file(
        folder_id=child.id,
        path="Folder A/hello.txt",
        name="hello.txt",
        storage_key="Folder A/hello.txt",
        mime_type="text/plain",
        size_bytes=5,
        checksum="abc",
        source_path="O:/Обмен/Folder A/hello.txt",
    )
    monkeypatch.setattr(cloud_api, "load_config", lambda: dict(cfg))
    monkeypatch.setattr(cloud_api, "_require_cloud_drive_api_user", lambda *_args, **_kwargs: {"username": "user", "role": "user", "status": "active"})

    root_node = api_cloud_drive_node()
    folder_node = api_cloud_drive_node("Folder A")
    listing = api_cloud_drive_list("Folder A")

    assert root_node["node_type"] == "folder"
    assert root_node["is_root"] is True
    assert folder_node["name"] == "Folder A"
    assert listing["folder"]["path"] == "Folder A"
    assert [item["name"] for item in listing["files"]] == ["hello.txt"]


def test_cloud_drive_create_folder_api(monkeypatch, tmp_path) -> None:
    cfg = {
        "cloud_drive_db_path": str(tmp_path / "cloud_drive.db"),
        "cloud_drive_storage": "local",
        "cloud_drive_storage_root": str(tmp_path / "storage"),
    }
    service = CloudDriveService.from_config(cfg)
    root = service.registry.ensure_root_folder(root_name="Обмен", source_path="O:/Обмен")
    service.registry.upsert_folder(
        path="Folder A",
        name="Folder A",
        parent_id=root.id,
        depth=1,
        source_path="O:/Обмен/Folder A",
    )
    monkeypatch.setattr(cloud_api, "load_config", lambda: dict(cfg))
    monkeypatch.setattr(cloud_api, "_require_cloud_drive_api_user", lambda *_args, **_kwargs: {"username": "user", "role": "user", "status": "active"})

    created = api_cloud_drive_create_folder(parent_path="Folder A", name="Nested")

    assert created["node_type"] == "folder"
    assert created["path"] == "Folder A/Nested"
    assert service.registry.get_folder_by_path("Folder A/Nested") is not None


def test_cloud_drive_download_api(monkeypatch, tmp_path) -> None:
    cfg = {
        "cloud_drive_db_path": str(tmp_path / "cloud_drive.db"),
        "cloud_drive_storage": "local",
        "cloud_drive_storage_root": str(tmp_path / "storage"),
    }
    service = CloudDriveService.from_config(cfg)
    root = service.registry.ensure_root_folder(root_name="Обмен", source_path="O:/Обмен")
    folder = service.registry.upsert_folder(
        path="Folder A",
        name="Folder A",
        parent_id=root.id,
        depth=1,
        source_path="O:/Обмен/Folder A",
    )
    source_file = tmp_path / "hello.txt"
    source_file.write_text("hello", encoding="utf-8")
    service.storage.put_file(source_file, "Folder A/hello.txt")
    service.registry.upsert_file(
        folder_id=folder.id,
        path="Folder A/hello.txt",
        name="hello.txt",
        storage_key="Folder A/hello.txt",
        mime_type="text/plain",
        size_bytes=5,
        checksum="abc",
        source_path="O:/Обмен/Folder A/hello.txt",
    )
    monkeypatch.setattr(cloud_api, "load_config", lambda: dict(cfg))
    monkeypatch.setattr(cloud_api, "_require_cloud_drive_api_user", lambda *_args, **_kwargs: {"username": "user", "role": "user", "status": "active"})

    response = api_cloud_drive_download("Folder A/hello.txt")

    assert response.path.endswith("hello.txt")
    assert response.filename == "hello.txt"


def test_cloud_drive_upload_api(monkeypatch, tmp_path) -> None:
    cfg = {
        "cloud_drive_db_path": str(tmp_path / "cloud_drive.db"),
        "cloud_drive_storage": "local",
        "cloud_drive_storage_root": str(tmp_path / "storage"),
    }
    service = CloudDriveService.from_config(cfg)
    root = service.registry.ensure_root_folder(root_name="Обмен", source_path="O:/Обмен")
    service.registry.upsert_folder(
        path="Folder A",
        name="Folder A",
        parent_id=root.id,
        depth=1,
        source_path="O:/Обмен/Folder A",
    )
    monkeypatch.setattr(cloud_api, "load_config", lambda: dict(cfg))

    buffer = SpooledTemporaryFile()
    buffer.write(b"hello")
    buffer.seek(0)
    upload = UploadFile(file=buffer, filename="hello.txt", headers={"content-type": "text/plain"})

    import asyncio
    monkeypatch.setattr(cloud_api, "_require_cloud_drive_api_user", lambda *_args, **_kwargs: {"username": "user", "role": "user", "status": "active"})
    result = asyncio.run(api_cloud_drive_upload(parent_path="Folder A", file=upload))

    assert result["node_type"] == "file"
    assert result["path"] == "Folder A/hello.txt"
    assert service.registry.get_file_by_path("Folder A/hello.txt") is not None


def test_cloud_drive_versions_api(monkeypatch, tmp_path) -> None:
    cfg = {
        "cloud_drive_db_path": str(tmp_path / "cloud_drive.db"),
        "cloud_drive_storage": "local",
        "cloud_drive_storage_root": str(tmp_path / "storage"),
    }
    service = CloudDriveService.from_config(cfg)
    root = service.registry.ensure_root_folder(root_name="Обмен", source_path="O:/Обмен")
    service.registry.upsert_folder(
        path="Folder A",
        name="Folder A",
        parent_id=root.id,
        depth=1,
        source_path="O:/Обмен/Folder A",
    )
    source_file = tmp_path / "hello.txt"
    source_file.write_text("hello", encoding="utf-8")
    source_file2 = tmp_path / "hello2.txt"
    source_file2.write_text("hello-2", encoding="utf-8")
    service.upload_file(parent_path="Folder A", filename="hello.txt", source_path=str(source_file), mime_type="text/plain")
    service.upload_file(parent_path="Folder A", filename="hello.txt", source_path=str(source_file2), mime_type="text/plain")
    monkeypatch.setattr(cloud_api, "load_config", lambda: dict(cfg))
    monkeypatch.setattr(cloud_api, "_require_cloud_drive_api_user", lambda *_args, **_kwargs: {"username": "user", "role": "user", "status": "active"})

    versions = api_cloud_drive_versions("Folder A/hello.txt")

    assert versions["file"]["path"] == "Folder A/hello.txt"
    assert len(versions["versions"]) == 2
    assert versions["versions"][0]["is_current"] is True


def test_cloud_drive_move_rename_delete_api(monkeypatch, tmp_path) -> None:
    cfg = {
        "cloud_drive_db_path": str(tmp_path / "cloud_drive.db"),
        "cloud_drive_storage": "local",
        "cloud_drive_storage_root": str(tmp_path / "storage"),
        "telemetry_db_path": str(tmp_path / "telemetry.db"),
    }
    service = CloudDriveService.from_config(cfg)
    root = service.registry.ensure_root_folder(root_name="Обмен", source_path="O:/Обмен")
    folder = service.registry.upsert_folder(
        path="Folder A",
        name="Folder A",
        parent_id=root.id,
        depth=1,
        source_path="O:/Обмен/Folder A",
    )
    source_file = tmp_path / "hello.txt"
    source_file.write_text("hello", encoding="utf-8")
    service.storage.put_file(source_file, "Folder A/hello.txt")
    service.registry.upsert_file(
        folder_id=folder.id,
        path="Folder A/hello.txt",
        name="hello.txt",
        storage_key="Folder A/hello.txt",
        mime_type="text/plain",
        size_bytes=5,
        checksum="abc",
        source_path="O:/Обмен/Folder A/hello.txt",
    )
    monkeypatch.setattr(cloud_api, "load_config", lambda: dict(cfg))
    monkeypatch.setattr(cloud_api, "_require_cloud_drive_api_user", lambda *_args, **_kwargs: {"username": "user", "role": "user", "status": "active"})

    renamed = api_cloud_drive_rename(path="Folder A/hello.txt", new_name="renamed.txt")
    moved = api_cloud_drive_move(source_path="Folder A", dest_parent_path="", new_name="Archive")
    deleted = api_cloud_drive_delete(path="Archive")

    assert renamed["path"] == "Folder A/renamed.txt"
    assert moved["path"] == "Archive"
    assert deleted["node_type"] == "folder"
    assert service.registry.get_folder_by_path("Archive") is None
    events = TelemetryDB(cfg["telemetry_db_path"]).fetch_dicts(
        "SELECT username, feature, action, ok FROM app_events ORDER BY id"
    )
    assert [(row["username"], row["feature"], row["action"], row["ok"]) for row in events] == [
        ("user", "cloud_drive", "rename", 1),
        ("user", "cloud_drive", "move", 1),
        ("user", "cloud_drive", "delete", 1),
    ]


def test_cloud_drive_reindex_api_queues_job(monkeypatch, tmp_path) -> None:
    cfg = {
        "cloud_drive_db_path": str(tmp_path / "cloud_drive.db"),
        "cloud_drive_storage": "local",
        "cloud_drive_storage_root": str(tmp_path / "storage"),
    }
    service = CloudDriveService.from_config(cfg)
    root = service.registry.ensure_root_folder(root_name="Обмен", source_path="O:/Обмен")
    folder = service.registry.upsert_folder(
        path="Folder A",
        name="Folder A",
        parent_id=root.id,
        depth=1,
        source_path="O:/Обмен/Folder A",
    )
    source_file = tmp_path / "hello.txt"
    source_file.write_text("hello", encoding="utf-8")
    created = service.upload_file(parent_path="Folder A", filename="hello.txt", source_path=str(source_file), mime_type="text/plain")
    monkeypatch.setattr(cloud_api, "load_config", lambda: dict(cfg))
    monkeypatch.setattr(cloud_api, "_require_cloud_drive_api_user", lambda *_args, **_kwargs: {"username": "user", "role": "user", "status": "active"})

    job = api_cloud_drive_reindex(path="Folder A/hello.txt")

    assert job["job_type"] == "reindex"
    assert job["status"] == "pending"
    assert job["file_id"] == created["id"]


def test_cloud_drive_job_run_api_processes_reindex_job(monkeypatch, tmp_path) -> None:
    cfg = {
        "cloud_drive_db_path": str(tmp_path / "cloud_drive.db"),
        "cloud_drive_storage": "local",
        "cloud_drive_storage_root": str(tmp_path / "storage"),
        "catalog_path": str(tmp_path / "catalog"),
    }
    service = CloudDriveService.from_config(cfg)
    root = service.registry.ensure_root_folder(root_name="Обмен", source_path="")
    service.registry.upsert_folder(
        path="Folder A",
        name="Folder A",
        parent_id=root.id,
        depth=1,
        source_path="",
    )
    source_file = tmp_path / "hello.txt"
    source_file.write_text("hello", encoding="utf-8")
    service.upload_file(parent_path="Folder A", filename="hello.txt", source_path=str(source_file), mime_type="text/plain")
    job = service.registry.get_latest_job(job_type="reindex")
    assert job is not None
    monkeypatch.setattr(cloud_api, "load_config", lambda: dict(cfg))
    monkeypatch.setattr(cloud_api, "_require_cloud_drive_api_user", lambda *_args, **_kwargs: {"username": "admin", "role": "admin", "status": "active"})

    result = api_cloud_drive_job_run(job_id=job.id)

    assert result["status"] == "completed"
    assert result["progress"]["status"] == "done"
    assert result["progress"]["indexed"] is False


def test_cloud_drive_job_retry_api_requeues_failed_reindex(monkeypatch, tmp_path) -> None:
    cfg = {
        "cloud_drive_db_path": str(tmp_path / "cloud_drive.db"),
        "cloud_drive_storage": "local",
        "cloud_drive_storage_root": str(tmp_path / "storage"),
    }
    service = CloudDriveService.from_config(cfg)
    root = service.registry.ensure_root_folder(root_name="Обмен", source_path="")
    folder = service.registry.upsert_folder(path="Folder A", name="Folder A", parent_id=root.id, depth=1, source_path="")
    file_row = service.registry.upsert_file(
        folder_id=folder.id,
        path="Folder A/hello.txt",
        name="hello.txt",
        storage_key="Folder A/hello.txt",
        mime_type="text/plain",
        size_bytes=5,
        checksum="abc",
        source_path="",
    )
    failed = service.registry.queue_job(
        job_type="reindex",
        status="failed",
        file_id=file_row.id,
        version_id=file_row.current_version_id,
        payload={"path": file_row.path, "progress": {"status": "failed"}},
    )
    monkeypatch.setattr(cloud_api, "load_config", lambda: dict(cfg))
    monkeypatch.setattr(cloud_api, "_require_cloud_drive_api_user", lambda *_args, **_kwargs: {"username": "admin", "role": "admin", "status": "active"})

    retried = api_cloud_drive_job_retry(job_id=failed.id)

    assert retried["job_type"] == "reindex"
    assert retried["status"] == "pending"
    assert retried["file_id"] == file_row.id
    assert retried["payload"]["retried_from_job_id"] == failed.id


def test_cloud_drive_api_requires_auth(monkeypatch, tmp_path) -> None:
    cfg = {
        "cloud_drive_db_path": str(tmp_path / "cloud_drive.db"),
        "cloud_drive_storage": "local",
        "cloud_drive_storage_root": str(tmp_path / "storage"),
        "users_db_path": str(tmp_path / "users.db"),
    }
    auth_db = UserAuthDB(cfg["users_db_path"])
    assert auth_db.admin_create_user(username="user", password="8215", role="user", status="active")
    token = auth_db.create_session(username="user")
    monkeypatch.setattr(cloud_api, "load_config", lambda: dict(cfg))

    user = cloud_api._require_cloud_drive_api_user(cfg, auth_token=token)

    assert user["username"] == "user"


def test_cloud_drive_api_admin_guard_rejects_non_admin(tmp_path) -> None:
    cfg = {
        "cloud_drive_db_path": str(tmp_path / "cloud_drive.db"),
        "cloud_drive_storage": "local",
        "cloud_drive_storage_root": str(tmp_path / "storage"),
        "users_db_path": str(tmp_path / "users.db"),
    }
    auth_db = UserAuthDB(cfg["users_db_path"])
    assert auth_db.admin_create_user(username="user", password="8215", role="user", status="active")
    token = auth_db.create_session(username="user")

    with pytest.raises(HTTPException) as exc:
        cloud_api._require_cloud_drive_api_user(cfg, auth_token=token, admin_only=True)

    assert exc.value.status_code == 403
