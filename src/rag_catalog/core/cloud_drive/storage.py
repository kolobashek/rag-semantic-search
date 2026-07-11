from __future__ import annotations

import hashlib
import mimetypes
import shutil
import uuid
from pathlib import Path
from typing import Protocol


class StorageAdapter(Protocol):
    def put_file(self, source_path: Path, storage_key: str) -> None: ...
    def download_file(self, storage_key: str, target_path: Path) -> None: ...
    def exists(self, storage_key: str) -> bool: ...
    def list_keys(self) -> set[str]: ...
    def move(self, old_storage_key: str, new_storage_key: str) -> None: ...
    def delete(self, storage_key: str) -> None: ...
    def resolve_path(self, storage_key: str) -> str: ...
    def healthcheck(self) -> dict: ...


def normalize_s3_credential(value: str) -> str:
    """Accept plain keys and common MinIO console snippets like `RootUser: minioadmin`."""
    text = str(value or "").strip()
    lowered = text.lower()
    for prefix in ("rootuser:", "rootpass:", "accesskey:", "secretkey:", "access key:", "secret key:"):
        if lowered.startswith(prefix):
            return text[len(prefix):].strip()
    return text


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

    def download_file(self, storage_key: str, target_path: Path) -> None:
        source = self._target(storage_key)
        if not source.exists():
            raise RuntimeError(f'Storage object not found: {storage_key}')
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target_path)

    def exists(self, storage_key: str) -> bool:
        return self._target(storage_key).exists()

    def list_keys(self) -> set[str]:
        result: set[str] = set()
        if not self.root.exists():
            return result
        for p in self.root.rglob("*"):
            if p.is_file() and not p.name.startswith("."):
                result.add(str(p.relative_to(self.root)).replace("\\", "/"))
        return result

    def move(self, old_storage_key: str, new_storage_key: str) -> None:
        source = self._target(old_storage_key)
        if not source.exists():
            raise RuntimeError(f'Storage object not found: {old_storage_key}')
        target = self._target(new_storage_key)
        target.parent.mkdir(parents=True, exist_ok=True)
        source.replace(target)

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

    def ensure_container(self) -> dict:
        return self.healthcheck()


class S3StorageAdapter:
    def __init__(self, *, bucket: str, endpoint_url: str = '', region: str = '', access_key: str = '', secret_key: str = '') -> None:
        try:
            import boto3  # type: ignore
            from botocore.config import Config  # type: ignore
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
        self._health_client = boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            region_name=self.region,
            aws_access_key_id=access_key or None,
            aws_secret_access_key=secret_key or None,
            config=Config(connect_timeout=1, read_timeout=1, retries={'max_attempts': 0}),
        )

    def put_file(self, source_path: Path, storage_key: str) -> None:
        self._client.upload_file(str(source_path), self.bucket, storage_key)

    def download_file(self, storage_key: str, target_path: Path) -> None:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        self._client.download_file(self.bucket, storage_key, str(target_path))

    def exists(self, storage_key: str) -> bool:
        try:
            self._client.head_object(Bucket=self.bucket, Key=storage_key)
            return True
        except Exception:
            return False

    def list_keys(self) -> set[str]:
        result: set[str] = set()
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket):
            for obj in page.get("Contents", []):
                key = str(obj.get("Key") or "")
                if key and not key.startswith("."):
                    result.add(key)
        return result

    def move(self, old_storage_key: str, new_storage_key: str) -> None:
        self._client.copy_object(
            Bucket=self.bucket,
            CopySource={"Bucket": self.bucket, "Key": old_storage_key},
            Key=new_storage_key,
        )
        self._client.delete_object(Bucket=self.bucket, Key=old_storage_key)

    def delete(self, storage_key: str) -> None:
        self._client.delete_object(Bucket=self.bucket, Key=storage_key)

    def resolve_path(self, storage_key: str) -> str:
        prefix = self.endpoint_url.rstrip('/') if self.endpoint_url else 's3://'
        if prefix == 's3://':
            return f's3://{self.bucket}/{storage_key}'
        return f'{prefix}/{self.bucket}/{storage_key}'

    def presigned_download_url(self, storage_key: str, *, expires_in: int = 3600) -> str:
        return str(
            self._client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": storage_key},
                ExpiresIn=max(60, int(expires_in or 3600)),
            )
        )

    def healthcheck(self) -> dict:
        probe_key = f".healthcheck/{uuid.uuid4().hex}"
        try:
            self._health_client.put_object(Bucket=self.bucket, Key=probe_key, Body=b"ok")
            self._health_client.delete_object(Bucket=self.bucket, Key=probe_key)
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

    def ensure_container(self) -> dict:
        created = False
        try:
            self._client.head_bucket(Bucket=self.bucket)
        except Exception as exc:
            response = getattr(exc, "response", {}) or {}
            error = response.get("Error", {}) if isinstance(response, dict) else {}
            code = str(error.get("Code") or "")
            status = int((response.get("ResponseMetadata", {}) or {}).get("HTTPStatusCode") or 0) if isinstance(response, dict) else 0
            if code not in {"404", "NoSuchBucket", "NotFound"} and status != 404:
                raise RuntimeError(f"Не удалось проверить S3 bucket {self.bucket}: {exc}") from exc
            kwargs = {"Bucket": self.bucket}
            if self.region and self.region != "us-east-1":
                kwargs["CreateBucketConfiguration"] = {"LocationConstraint": self.region}
            self._client.create_bucket(**kwargs)
            created = True
        health = self.healthcheck()
        health["bucket"] = self.bucket
        health["created"] = created
        return health


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
            access_key=normalize_s3_credential(str(config.get('cloud_drive_s3_access_key') or '')),
            secret_key=normalize_s3_credential(str(config.get('cloud_drive_s3_secret_key') or '')),
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
