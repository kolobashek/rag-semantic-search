from __future__ import annotations

import hashlib
import mimetypes
import shutil
import uuid
from pathlib import Path
from typing import Protocol


class StorageAdapter(Protocol):
    def put_file(self, source_path: Path, storage_key: str) -> None: ...
    def exists(self, storage_key: str) -> bool: ...
    def delete(self, storage_key: str) -> None: ...
    def resolve_path(self, storage_key: str) -> str: ...
    def healthcheck(self) -> dict: ...


class LocalStorageAdapter:
    def __init__(self, root: str) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _target(self, storage_key: str) -> Path:
        safe_key = storage_key.replace('..', '').replace('\\', '/').lstrip('/')
        return self.root / Path(safe_key)

    def put_file(self, source_path: Path, storage_key: str) -> None:
        target = self._target(storage_key)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, target)

    def exists(self, storage_key: str) -> bool:
        return self._target(storage_key).exists()

    def delete(self, storage_key: str) -> None:
        target = self._target(storage_key)
        if target.exists():
            target.unlink()

    def resolve_path(self, storage_key: str) -> str:
        return str(self._target(storage_key))

    def healthcheck(self) -> dict:
        probe = self.root / ".healthcheck"
        try:
            self.root.mkdir(parents=True, exist_ok=True)
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
            return {
                "backend": "local",
                "ok": True,
                "writable": True,
                "target": str(self.root),
                "error": "",
            }
        except Exception as exc:
            return {
                "backend": "local",
                "ok": False,
                "writable": False,
                "target": str(self.root),
                "error": str(exc),
            }


class S3StorageAdapter:
    def __init__(self, *, bucket: str, endpoint_url: str = '', region: str = '', access_key: str = '', secret_key: str = '') -> None:
        try:
            import boto3  # type: ignore
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError('Для S3 storage нужен boto3.') from exc
        self.bucket = bucket
        self.endpoint_url = endpoint_url or None
        self.region = region or None
        self._client = boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            region_name=self.region,
            aws_access_key_id=access_key or None,
            aws_secret_access_key=secret_key or None,
        )

    def put_file(self, source_path: Path, storage_key: str) -> None:
        self._client.upload_file(str(source_path), self.bucket, storage_key)

    def exists(self, storage_key: str) -> bool:
        try:
            self._client.head_object(Bucket=self.bucket, Key=storage_key)
            return True
        except Exception:
            return False

    def delete(self, storage_key: str) -> None:
        self._client.delete_object(Bucket=self.bucket, Key=storage_key)

    def resolve_path(self, storage_key: str) -> str:
        prefix = self.endpoint_url.rstrip('/') if self.endpoint_url else 's3://'
        if prefix == 's3://':
            return f's3://{self.bucket}/{storage_key}'
        return f'{prefix}/{self.bucket}/{storage_key}'

    def healthcheck(self) -> dict:
        probe_key = f".healthcheck/{uuid.uuid4().hex}"
        try:
            self._client.put_object(Bucket=self.bucket, Key=probe_key, Body=b"ok")
            self._client.delete_object(Bucket=self.bucket, Key=probe_key)
            return {
                "backend": "s3",
                "ok": True,
                "writable": True,
                "target": self.resolve_path(""),
                "error": "",
            }
        except Exception as exc:
            return {
                "backend": "s3",
                "ok": False,
                "writable": False,
                "target": self.resolve_path(""),
                "error": str(exc),
            }


def resolve_storage_adapter(config: dict) -> StorageAdapter:
    kind = str(config.get('cloud_drive_storage') or 'local').strip().lower()
    if kind == 'local':
        root = str(config.get('cloud_drive_storage_root') or '').strip()
        if not root:
            raise RuntimeError('Не задан cloud_drive_storage_root для local storage.')
        return LocalStorageAdapter(root)
    if kind == 's3':
        bucket = str(config.get('cloud_drive_bucket') or '').strip()
        if not bucket:
            raise RuntimeError('Не задан cloud_drive_bucket для S3 storage.')
        return S3StorageAdapter(
            bucket=bucket,
            endpoint_url=str(config.get('cloud_drive_s3_endpoint') or '').strip(),
            region=str(config.get('cloud_drive_s3_region') or '').strip(),
            access_key=str(config.get('cloud_drive_s3_access_key') or '').strip(),
            secret_key=str(config.get('cloud_drive_s3_secret_key') or '').strip(),
        )
    raise RuntimeError(f'Неизвестный cloud_drive_storage: {kind}')


def compute_file_checksum(path: Path, *, algorithm: str = 'sha256', chunk_size: int = 1024 * 1024) -> str:
    hasher = hashlib.new(algorithm)
    with path.open('rb') as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def guess_mime_type(path: Path) -> str:
    mime, _enc = mimetypes.guess_type(str(path))
    return mime or 'application/octet-stream'
