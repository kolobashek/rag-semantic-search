from __future__ import annotations

import types
from pathlib import Path

from rag_catalog.core.cloud_drive.registry import CloudDriveRegistryDB
from rag_catalog.core.cloud_drive.service import CloudDriveService
from rag_catalog.core.cloud_drive.storage import (
    LocalStorageAdapter,
    S3StorageAdapter,
    compute_file_checksum,
    guess_mime_type,
    normalize_s3_credential,
)
from rag_catalog.core.index_state_db import IndexStateDB


def test_local_storage_adapter_put_and_delete(tmp_path: Path) -> None:
    source = tmp_path / 'source.txt'
    source.write_text('payload', encoding='utf-8')
    storage = LocalStorageAdapter(str(tmp_path / 'storage'))

    storage.put_file(source, 'docs/source.txt')

    target = tmp_path / 'storage' / 'docs' / 'source.txt'
    assert target.exists()
    assert storage.exists('docs/source.txt') is True
    assert storage.resolve_path('docs/source.txt') == str(target)

    storage.delete('docs/source.txt')
    assert target.exists() is False


def test_storage_helpers(tmp_path: Path) -> None:
    file_path = tmp_path / 'report.pdf'
    file_path.write_bytes(b'abc123')

    assert compute_file_checksum(file_path)
    assert guess_mime_type(file_path) == 'application/pdf'


def test_normalize_s3_credential_accepts_minio_console_labels() -> None:
    assert normalize_s3_credential("RootUser: minioadmin") == "minioadmin"
    assert normalize_s3_credential("RootPass: minioadmin123") == "minioadmin123"
    assert normalize_s3_credential(" plain-key ") == "plain-key"


def test_local_storage_healthcheck(tmp_path: Path) -> None:
    storage = LocalStorageAdapter(str(tmp_path / 'storage'))

    health = storage.healthcheck()

    assert health["backend"] == "local"
    assert health["ok"] is True
    assert health["writable"] is True
    assert str(tmp_path / 'storage') == health["target"]


def test_cloud_drive_service_storage_health(tmp_path: Path) -> None:
    service = CloudDriveService(
        registry=CloudDriveRegistryDB(str(tmp_path / 'registry.db')),
        storage=LocalStorageAdapter(str(tmp_path / 'storage')),
    )

    health = service.get_storage_health()

    assert health.backend == "local"
    assert health.ok is True
    assert health.writable is True


def test_bootstrap_import_files_backfills_existing_registry_objects(tmp_path: Path) -> None:
    catalog = tmp_path / "catalog"
    catalog.mkdir()
    source = catalog / "report.txt"
    source.write_text("payload", encoding="utf-8")

    storage_root = tmp_path / "storage"
    service = CloudDriveService(
        registry=CloudDriveRegistryDB(str(tmp_path / "registry.db")),
        storage=LocalStorageAdapter(str(storage_root)),
    )

    service.bootstrap_from_catalog(str(catalog), import_files=False)
    assert not [path for path in storage_root.rglob("*") if path.is_file()]

    service.bootstrap_from_catalog(str(catalog), import_files=True)

    stored_files = [path for path in storage_root.rglob("*") if path.is_file()]
    assert len(stored_files) == 1
    assert stored_files[0].read_text(encoding="utf-8") == "payload"


def test_s3_storage_ensure_container_creates_missing_bucket(monkeypatch) -> None:
    calls: list[tuple[str, dict]] = []

    class _MissingBucket(Exception):
        response = {"Error": {"Code": "NoSuchBucket"}, "ResponseMetadata": {"HTTPStatusCode": 404}}

    class _FakeS3Client:
        def head_bucket(self, **kwargs):
            calls.append(("head_bucket", kwargs))
            raise _MissingBucket()

        def create_bucket(self, **kwargs):
            calls.append(("create_bucket", kwargs))

        def put_object(self, **kwargs):
            calls.append(("put_object", kwargs))

        def delete_object(self, **kwargs):
            calls.append(("delete_object", kwargs))

    fake_boto3 = types.SimpleNamespace(client=lambda *_args, **_kwargs: _FakeS3Client())
    monkeypatch.setitem(__import__("sys").modules, "boto3", fake_boto3)

    storage = S3StorageAdapter(bucket="rag", endpoint_url="http://127.0.0.1:9000", region="us-east-1", access_key="ak", secret_key="sk")
    result = storage.ensure_container()

    assert result["backend"] == "s3"
    assert result["ok"] is True
    assert result["created"] is True
    assert ("create_bucket", {"Bucket": "rag"}) in calls


def test_cloud_drive_upload_uses_content_addressed_dedup_storage(tmp_path: Path) -> None:
    storage_root = tmp_path / "storage"
    service = CloudDriveService(
        registry=CloudDriveRegistryDB(str(tmp_path / "registry.db")),
        storage=LocalStorageAdapter(str(storage_root)),
    )
    root = service.registry.ensure_root_folder(root_name="Обмен", source_path="")
    service.registry.upsert_folder(path="A", name="A", parent_id=root.id, depth=1, source_path="")
    service.registry.upsert_folder(path="B", name="B", parent_id=root.id, depth=1, source_path="")
    source = tmp_path / "payload.txt"
    source.write_text("same-content", encoding="utf-8")

    first = service.upload_file(parent_path="A", filename="first.txt", source_path=str(source), mime_type="text/plain")
    second = service.upload_file(parent_path="B", filename="second.txt", source_path=str(source), mime_type="text/plain")

    assert first["storage_key"] == second["storage_key"]
    assert first["storage_key"].startswith("objects/sha256/")
    stored_files = [path for path in storage_root.rglob("*") if path.is_file()]
    assert len(stored_files) == 1


class _FakePresignedStorage:
    def __init__(self) -> None:
        self.keys: set[str] = set()

    def put_file(self, source_path: Path, storage_key: str) -> None:
        self.keys.add(storage_key)

    def exists(self, storage_key: str) -> bool:
        return storage_key in self.keys

    def move(self, old_storage_key: str, new_storage_key: str) -> None:
        self.keys.remove(old_storage_key)
        self.keys.add(new_storage_key)

    def delete(self, storage_key: str) -> None:
        self.keys.discard(storage_key)

    def resolve_path(self, storage_key: str) -> str:
        return f"s3://bucket/{storage_key}"

    def presigned_download_url(self, storage_key: str, *, expires_in: int = 3600) -> str:
        return f"https://storage.example/{storage_key}?expires={expires_in}"

    def healthcheck(self) -> dict:
        return {"backend": "s3", "ok": True, "writable": True, "target": "s3://bucket", "error": ""}


def test_cloud_drive_download_descriptor_supports_presigned_storage(tmp_path: Path) -> None:
    storage = _FakePresignedStorage()
    service = CloudDriveService(
        registry=CloudDriveRegistryDB(str(tmp_path / "registry.db")),
        storage=storage,
    )
    service.registry.ensure_root_folder(root_name="Обмен", source_path="")
    source = tmp_path / "report.pdf"
    source.write_bytes(b"pdf")

    service.upload_file(parent_path="", filename="report.pdf", source_path=str(source), mime_type="application/pdf")
    descriptor = service.get_download_descriptor("report.pdf")

    assert descriptor["mode"] == "redirect_url"
    assert descriptor["url"].startswith("https://storage.example/objects/sha256/")
    assert descriptor["filename"] == "report.pdf"


def test_cloud_drive_storage_coverage_reports_missing_registry_objects(tmp_path: Path) -> None:
    service = CloudDriveService(
        registry=CloudDriveRegistryDB(str(tmp_path / "registry.db")),
        storage=_FakePresignedStorage(),
    )
    service.registry.ensure_root_folder(root_name="Обмен", source_path="")
    source = tmp_path / "report.pdf"
    source.write_bytes(b"pdf")
    service.upload_file(parent_path="", filename="report.pdf", source_path=str(source), mime_type="application/pdf")

    replacement_storage = _FakePresignedStorage()
    service = CloudDriveService(registry=service.registry, storage=replacement_storage)

    coverage = service.get_storage_coverage(sample_limit=10)

    assert coverage["ok"] is False
    assert coverage["needs_backfill"] is True
    assert coverage["checked"] == 1
    assert coverage["missing"] == 1
    assert coverage["missing_examples"][0]["path"] == "report.pdf"


def test_cloud_drive_index_coverage_reports_missing_stale_and_errors(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / "registry.db"))
    service = CloudDriveService(registry=registry, storage=LocalStorageAdapter(str(tmp_path / "storage")))
    root = registry.ensure_root_folder(root_name="Обмен", source_path="")
    current = registry.upsert_file(
        folder_id=root.id,
        path="current.txt",
        name="current.txt",
        storage_key="objects/sha256/aa/aa/current.txt",
        mime_type="text/plain",
        size_bytes=1,
        checksum="aaaa",
        source_path="",
    )
    stale = registry.upsert_file(
        folder_id=root.id,
        path="stale.txt",
        name="stale.txt",
        storage_key="objects/sha256/bb/bb/stale.txt",
        mime_type="text/plain",
        size_bytes=1,
        checksum="bbbb",
        source_path="",
    )
    failed = registry.upsert_file(
        folder_id=root.id,
        path="failed.txt",
        name="failed.txt",
        storage_key="objects/sha256/cc/cc/failed.txt",
        mime_type="text/plain",
        size_bytes=1,
        checksum="cccc",
        source_path="",
    )
    registry.upsert_file(
        folder_id=root.id,
        path="missing.txt",
        name="missing.txt",
        storage_key="objects/sha256/dd/dd/missing.txt",
        mime_type="text/plain",
        size_bytes=1,
        checksum="dddd",
        source_path="",
    )
    registry.upsert_file(
        folder_id=root.id,
        path="ignored.dll",
        name="ignored.dll",
        storage_key="objects/sha256/ee/ee/ignored.dll",
        mime_type="application/octet-stream",
        size_bytes=1,
        checksum="eeee",
        source_path="",
    )
    registry.upsert_file(
        folder_id=root.id,
        path="~$draft.xlsx",
        name="~$draft.xlsx",
        storage_key="objects/sha256/ff/ff/draft.xlsx",
        mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        size_bytes=1,
        checksum="ffff",
        source_path="",
    )
    state_db_path = tmp_path / "index_state.db"
    state_db = IndexStateDB(str(state_db_path))
    state_db.upsert_many(
        [
            {
                "full_path": "cloud:current",
                "stage": "content",
                "cloud_file_id": current.id,
                "cloud_version_id": current.current_version_id,
                "cloud_path": current.path,
                "status": "ok",
            },
            {
                "full_path": "cloud:stale",
                "stage": "content",
                "cloud_file_id": stale.id,
                "cloud_version_id": "old-version",
                "cloud_path": stale.path,
                "status": "ok",
            },
            {
                "full_path": "cloud:failed",
                "stage": "content",
                "cloud_file_id": failed.id,
                "cloud_version_id": failed.current_version_id,
                "cloud_path": failed.path,
                "status": "error",
                "last_error": "boom",
            },
        ]
    )

    coverage = service.get_index_coverage(index_state_db_path=str(state_db_path), sample_limit=10)

    assert coverage["ok"] is False
    assert coverage["registry_files"] == 6
    assert coverage["indexed_current"] == 1
    assert coverage["indexable_registry_files"] == 4
    assert coverage["indexable_indexed_current"] == 1
    assert coverage["indexable_missing"] == 1
    assert coverage["indexable_stale"] == 1
    assert coverage["indexable_errored"] == 1
    assert coverage["unsupported_missing"] == 2
    assert coverage["stale"] == 1
    assert coverage["errored"] == 1
    assert coverage["missing"] == 3
    assert coverage["indexable_missing_examples"][0]["path"] == "missing.txt"
    assert coverage["unsupported_missing_examples"][0]["path"] == "ignored.dll"
    assert coverage["stale_examples"][0]["path"] == "stale.txt"
    assert coverage["error_examples"][0]["last_error"] == "boom"


def test_cloud_drive_index_coverage_accepts_legacy_source_path_entries(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / "registry.db"))
    service = CloudDriveService(registry=registry, storage=LocalStorageAdapter(str(tmp_path / "storage")))
    root = registry.ensure_root_folder(root_name="Обмен", source_path=str(tmp_path / "catalog"))
    current = registry.upsert_file(
        folder_id=root.id,
        path="Folder/report.txt",
        name="report.txt",
        storage_key="objects/sha256/aa/aa/report.txt",
        mime_type="text/plain",
        size_bytes=1,
        checksum="aaaa",
        source_path=str(tmp_path / "catalog" / "Folder" / "report.txt"),
    )
    state_db_path = tmp_path / "index_state.db"
    state_db = IndexStateDB(str(state_db_path))
    state_db.upsert_many(
        [
            {
                "full_path": current.source_path,
                "stage": "content",
                "status": "ok",
            },
        ]
    )

    coverage = service.get_index_coverage(index_state_db_path=str(state_db_path), sample_limit=10)

    assert coverage["ok"] is True
    assert coverage["registry_files"] == 1
    assert coverage["indexed_current"] == 1
    assert coverage["indexable_registry_files"] == 1
    assert coverage["indexable_indexed_current"] == 1
    assert coverage["missing"] == 0


def test_cloud_drive_index_coverage_repair_queues_indexable_gaps(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / "registry.db"))
    service = CloudDriveService(registry=registry, storage=LocalStorageAdapter(str(tmp_path / "storage")))
    root = registry.ensure_root_folder(root_name="Обмен", source_path="")
    stale = registry.upsert_file(
        folder_id=root.id,
        path="stale.txt",
        name="stale.txt",
        storage_key="objects/sha256/aa/aa/stale.txt",
        mime_type="text/plain",
        size_bytes=1,
        checksum="aaaa",
        source_path="",
    )
    failed = registry.upsert_file(
        folder_id=root.id,
        path="failed.txt",
        name="failed.txt",
        storage_key="objects/sha256/bb/bb/failed.txt",
        mime_type="text/plain",
        size_bytes=1,
        checksum="bbbb",
        source_path="",
    )
    registry.upsert_file(
        folder_id=root.id,
        path="missing.txt",
        name="missing.txt",
        storage_key="objects/sha256/cc/cc/missing.txt",
        mime_type="text/plain",
        size_bytes=1,
        checksum="cccc",
        source_path="",
    )
    registry.upsert_file(
        folder_id=root.id,
        path="ignored.dll",
        name="ignored.dll",
        storage_key="objects/sha256/dd/dd/ignored.dll",
        mime_type="application/octet-stream",
        size_bytes=1,
        checksum="dddd",
        source_path="",
    )
    registry.queue_job(
        job_type="reindex",
        status="pending",
        file_id=stale.id,
        version_id=stale.current_version_id,
        payload={"path": stale.path, "reason": "existing"},
    )
    state_db_path = tmp_path / "index_state.db"
    state_db = IndexStateDB(str(state_db_path))
    state_db.upsert_many(
        [
            {
                "full_path": "cloud:stale",
                "stage": "content",
                "cloud_file_id": stale.id,
                "cloud_version_id": "old-version",
                "cloud_path": stale.path,
                "status": "ok",
            },
            {
                "full_path": "cloud:failed",
                "stage": "content",
                "cloud_file_id": failed.id,
                "cloud_version_id": failed.current_version_id,
                "cloud_path": failed.path,
                "status": "error",
                "last_error": "boom",
            },
        ]
    )

    result = service.enqueue_index_coverage_repair(
        index_state_db_path=str(state_db_path),
        scopes="missing,stale,error",
        limit=10,
    )

    assert result["candidates"] == 3
    assert result["queued"] == 2
    assert result["skipped_existing"] == 1
    jobs = registry.list_pending_jobs(job_types=["reindex"], limit=10)
    queued_paths = {str(job.payload.get("path") or "") for job in jobs}
    assert {"missing.txt", "failed.txt", "stale.txt"} == queued_paths
    repair_reasons = {
        str(job.payload.get("reason") or "")
        for job in jobs
        if str(job.payload.get("path") or "") in {"missing.txt", "failed.txt"}
    }
    assert repair_reasons == {"coverage_missing", "coverage_error"}


def test_cloud_drive_index_coverage_repair_batches_past_existing_jobs(tmp_path: Path) -> None:
    registry = CloudDriveRegistryDB(str(tmp_path / "registry.db"))
    service = CloudDriveService(registry=registry, storage=LocalStorageAdapter(str(tmp_path / "storage")))
    root = registry.ensure_root_folder(root_name="Обмен", source_path="")
    for idx in range(3):
        registry.upsert_file(
            folder_id=root.id,
            path=f"missing-{idx}.txt",
            name=f"missing-{idx}.txt",
            storage_key=f"objects/sha256/{idx}{idx}/{idx}{idx}/missing-{idx}.txt",
            mime_type="text/plain",
            size_bytes=1,
            checksum=str(idx) * 4,
            source_path="",
        )
    state_db_path = tmp_path / "index_state.db"
    IndexStateDB(str(state_db_path))

    first = service.enqueue_index_coverage_repair(index_state_db_path=str(state_db_path), scopes="missing", limit=1)
    second = service.enqueue_index_coverage_repair(index_state_db_path=str(state_db_path), scopes="missing", limit=1)

    assert first["queued"] == 1
    assert second["queued"] == 1
    jobs = registry.list_pending_jobs(job_types=["reindex"], limit=10)
    queued_paths = {str(job.payload.get("path") or "") for job in jobs}
    assert len(queued_paths) == 2
