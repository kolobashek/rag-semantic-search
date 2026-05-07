from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

from .models import CloudDriveStats
from .registry import CloudDriveRegistryDB
from .storage import StorageAdapter, compute_file_checksum, guess_mime_type, resolve_storage_adapter


class CloudDriveService:
    def __init__(self, *, registry: CloudDriveRegistryDB, storage: StorageAdapter) -> None:
        self.registry = registry
        self.storage = storage

    @classmethod
    def from_config(cls, config: Dict[str, object]) -> 'CloudDriveService':
        db_path = str(config.get('cloud_drive_db_path') or '').strip()
        if not db_path:
            raise RuntimeError('Не задан cloud_drive_db_path.')
        return cls(
            registry=CloudDriveRegistryDB(db_path),
            storage=resolve_storage_adapter(config),
        )

    def bootstrap_from_catalog(self, catalog_root: str, *, max_files: Optional[int] = None, import_files: bool = False) -> CloudDriveStats:
        root = Path(catalog_root)
        if not root.exists() or not root.is_dir():
            raise RuntimeError(f'Каталог не найден: {root}')
        root_folder = self.registry.ensure_root_folder(root_name=root.name or 'root', source_path=str(root))
        imported = 0
        folder_cache: Dict[Path, str] = {root: root_folder.id}

        for dirpath, dirnames, filenames in __import__('os').walk(root):
            base = Path(dirpath)
            base_id = folder_cache.get(base)
            if base_id is None:
                rel_base = base.relative_to(root)
                parent = base.parent if base.parent in folder_cache else root
                parent_id = folder_cache[parent]
                base_folder = self.registry.upsert_folder(
                    path=str(rel_base).replace('\\', '/'),
                    name=base.name,
                    parent_id=parent_id,
                    depth=len(rel_base.parts),
                    source_path=str(base),
                )
                base_id = base_folder.id
                folder_cache[base] = base_id

            for dirname in sorted(dirnames):
                child = base / dirname
                rel_child = child.relative_to(root)
                folder = self.registry.upsert_folder(
                    path=str(rel_child).replace('\\', '/'),
                    name=dirname,
                    parent_id=base_id,
                    depth=len(rel_child.parts),
                    source_path=str(child),
                )
                folder_cache[child] = folder.id

            for filename in sorted(filenames):
                file_path = base / filename
                rel_file = file_path.relative_to(root)
                storage_key = str(rel_file).replace('\\', '/')
                if import_files and not self.storage.exists(storage_key):
                    self.storage.put_file(file_path, storage_key)
                checksum = compute_file_checksum(file_path)
                self.registry.upsert_file(
                    folder_id=base_id,
                    path=storage_key,
                    name=filename,
                    storage_key=storage_key,
                    mime_type=guess_mime_type(file_path),
                    size_bytes=file_path.stat().st_size,
                    checksum=checksum,
                    source_path=str(file_path),
                )
                imported += 1
                if max_files is not None and imported >= max_files:
                    return self.registry.stats()
        return self.registry.stats()

    def enqueue_reindex(self, path: str) -> None:
        file_row = self.registry.get_file_by_path(path)
        if file_row is None:
            raise RuntimeError(f'Файл не найден в registry: {path}')
        self.registry.queue_job(
            job_type='reindex',
            file_id=file_row.id,
            version_id=file_row.current_version_id,
            payload={'path': file_row.path},
        )
