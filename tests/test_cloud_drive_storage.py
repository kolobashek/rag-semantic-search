from __future__ import annotations

from pathlib import Path

from rag_catalog.core.cloud_drive.storage import LocalStorageAdapter, compute_file_checksum, guess_mime_type
from rag_catalog.core.cloud_drive.service import CloudDriveService
from rag_catalog.core.cloud_drive.registry import CloudDriveRegistryDB


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
