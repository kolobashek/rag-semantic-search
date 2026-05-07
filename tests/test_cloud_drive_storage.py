from __future__ import annotations

from pathlib import Path

from rag_catalog.core.cloud_drive.storage import LocalStorageAdapter, compute_file_checksum, guess_mime_type


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
