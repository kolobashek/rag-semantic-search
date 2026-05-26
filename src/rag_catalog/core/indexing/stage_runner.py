from __future__ import annotations

import logging
import os
import shutil
import subprocess
import tarfile
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple
from zipfile import ZipFile

from qdrant_client.models import PointStruct
from tqdm import tqdm

from ..exact_tokens import add_numeric_tokens, repair_zip_member_name
from ..extractors import ExtractedDocument, extract_doc_meta
from .qdrant_writer import upsert_points

_TAR_ARCHIVE_SUFFIXES = (".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz", ".tbz2", ".tar.xz", ".txz")
_COMMAND_ARCHIVE_SUFFIXES = (".rar",)
_NESTED_ARCHIVE_SUFFIXES = (*_TAR_ARCHIVE_SUFFIXES, ".zip", ".7z", *_COMMAND_ARCHIVE_SUFFIXES)
_COMMAND_ARCHIVE_TOOLS = ("bsdtar", "7z", "7zz", "7za")
_WINDOWS_7Z_PATHS = (
    Path("C:/Program Files/7-Zip/7z.exe"),
    Path("C:/Program Files (x86)/7-Zip/7z.exe"),
)


def _hidden_subprocess_kwargs() -> Dict[str, Any]:
    if os.name != "nt":
        return {}
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = subprocess.SW_HIDE
    return {
        "creationflags": int(getattr(subprocess, "CREATE_NO_WINDOW", 0) or 0),
        "startupinfo": startupinfo,
    }


def _archive_type_for_path(path: Path | str) -> str:
    name = str(path or "").lower()
    if name.endswith(".zip"):
        return "zip"
    if name.endswith(".7z"):
        return "7z"
    if name.endswith(_COMMAND_ARCHIVE_SUFFIXES):
        return "command"
    if name.endswith(_TAR_ARCHIVE_SUFFIXES):
        return "tar"
    return ""


def _is_nested_archive_member(name: str) -> bool:
    return str(name or "").lower().endswith(_NESTED_ARCHIVE_SUFFIXES)


def _command_archive_tool() -> Tuple[str, str] | None:
    for name in _COMMAND_ARCHIVE_TOOLS:
        path = shutil.which(name)
        if path:
            return name, path
    if os.name == "nt":
        for path in _WINDOWS_7Z_PATHS:
            if path.exists():
                return "7z", str(path)
    return None


def _parse_7z_list_output(stdout: str) -> List[Dict[str, Any]]:
    members: List[Dict[str, Any]] = []
    in_entries = False
    current: Dict[str, Any] = {}

    def _flush() -> None:
        nonlocal current
        name = str(current.get("name") or "").strip()
        attrs = str(current.get("attributes") or "")
        is_folder = str(current.get("folder") or "").strip() == "+"
        if name and not name.endswith("/") and "D" not in attrs and not is_folder:
            members.append(dict(current))
        current = {}

    for line in str(stdout or "").splitlines():
        raw = line.strip()
        if raw.startswith("----------"):
            in_entries = True
            current = {}
            continue
        if not in_entries:
            continue
        if not raw:
            if current:
                _flush()
            continue
        key, sep, value = raw.partition(" = ")
        if not sep:
            continue
        key = key.strip().lower()
        value = value.strip()
        if key == "path":
            if current:
                _flush()
            current["name"] = value
        elif key == "size":
            try:
                current["size_bytes"] = int(value or 0)
            except ValueError:
                current["size_bytes"] = 0
        elif key == "modified":
            current["mtime_text"] = value
        elif key == "crc":
            current["crc"] = value
        elif key == "attributes":
            current["attributes"] = value
        elif key == "folder":
            current["folder"] = value
    if current:
        _flush()
    return members


def _list_command_archive_members(filepath: Path, *, timeout: int = 120) -> Tuple[str, List[Dict[str, Any]]]:
    tool = _command_archive_tool()
    if not tool:
        raise RuntimeError("недоступны bsdtar/7z/7zz/7za")
    tool_name, tool_path = tool
    if tool_name == "bsdtar":
        proc = subprocess.run(
            [tool_path, "-tf", str(filepath)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            check=False,
            **_hidden_subprocess_kwargs(),
        )
        if proc.returncode != 0:
            raise RuntimeError((proc.stderr or "").strip() or f"{tool_name} exit code {proc.returncode}")
        members = [
            {"name": raw_name.strip(), "size_bytes": 0}
            for raw_name in proc.stdout.splitlines()
            if raw_name.strip() and not raw_name.strip().endswith("/")
        ]
        return tool_name, members

    proc = subprocess.run(
        [tool_path, "l", "-slt", "-sccUTF-8", str(filepath)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
        **_hidden_subprocess_kwargs(),
    )
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or "").strip() or f"{tool_name} exit code {proc.returncode}")
    return tool_name, _parse_7z_list_output(proc.stdout)


def _extract_command_archive_member(archive_path: Path, member_name: str, *, timeout: int = 300) -> bytes:
    tool = _command_archive_tool()
    if not tool:
        raise RuntimeError("недоступны bsdtar/7z/7zz/7za")
    tool_name, tool_path = tool
    if tool_name == "bsdtar":
        proc = subprocess.run(
            [tool_path, "-xOf", str(archive_path), member_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
            **_hidden_subprocess_kwargs(),
        )
    else:
        proc = subprocess.run(
            [tool_path, "x", "-so", "-y", "-sccUTF-8", str(archive_path), member_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
            **_hidden_subprocess_kwargs(),
        )
    if proc.returncode != 0:
        err = proc.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(err or f"{tool_name} exit code {proc.returncode}")
    return bytes(proc.stdout)


def _normalize_only_path_key(value: Any) -> str:
    return str(value or "").strip().replace("/", "\\").lower()


def _task_matches_only_paths(item: Dict[str, Any], allowed: set[str]) -> bool:
    if not allowed:
        return True
    keys = {
        item.get("state_key"),
        item.get("filepath"),
        item.get("source_path"),
        item.get("relative_path"),
    }
    return any(_normalize_only_path_key(key) in allowed for key in keys if key)


class IndexStageRunner:
    """Runs one index stage while delegating indexer-specific operations to RAGIndexer.

    This keeps the public RAGIndexer contract stable and moves the pipeline
    orchestration out of the already-large indexer class.
    """

    def __init__(
        self,
        indexer: Any,
        *,
        stages: Tuple[str, ...],
        supported_extensions: set[str],
        image_extensions: set[str],
        file_category: Callable[[Path, float, float], str],
        generate_tags: Callable[..., List[str]],
        logger: logging.Logger,
    ) -> None:
        self._indexer = indexer
        self._stages = tuple(stages)
        self._supported_extensions = set(supported_extensions)
        self._image_extensions = set(image_extensions)
        self._file_category = file_category
        self._generate_tags = generate_tags
        self._logger = logger

    def run(self, stage: str = "content") -> Dict[str, int]:
        """
        Pipeline-индексирование на указанном этапе.

          stage="metadata" — только имя/путь/размер/mtime (не читает файлы);
          stage="small"    — быстрый проход по всем файлам, ограниченный max_chunks;
          stage="large"    — полный проход/догрузка оставшихся чанков;
          stage="content"  — legacy: полное содержимое для всех файлов за один проход.

        Pipeline:
          - ThreadPoolExecutor читает файлы параллельно (I/O-bound)
          - Главный поток batch-кодирует накопленные чанки и пишет в Qdrant

        Прирост производительности:
          - Batch encode: 5-10x быстрее чем поштучно
          - Pipeline: чтение следующих файлов идёт пока GPU/CPU кодирует предыдущие
        """
        if stage not in (*self._stages, "content"):
            raise ValueError(f"Неизвестный stage: {stage!r}. Допустимо: {self._stages} или 'content'")
        indexer = self._indexer
        indexer.current_stage = stage

        ENCODE_BATCH = 256  # сколько чанков накапливать перед одним вызовом encode()
                            # 256 оптимально для CPU (OpenBLAS матричные операции)

        # Семафор ограничивает число одновременно «зависших» daemon-потоков.
        # При массовых SMB-таймаутах без ограничения они накапливаются до OOM.
        # Лимит = 2 * read_workers: за один проход воркеров может зависнуть
        # не более read_workers файлов, запас ×2 на перекрытие таймаутов.
        _reader_sem = threading.Semaphore(indexer.read_workers * 2)

        self._logger.info(
            "════════ Этап '%s' (pipeline, workers=%d): %s ════════",
            stage, indexer.read_workers, indexer.catalog_path,
        )

        all_files = [
            f
            for f in indexer.catalog_path.rglob("*")
            if f.is_file()
            and f.suffix.lower() in self._supported_extensions
            and not f.name.startswith("~$")  # пропускать временные файлы Office
            and not indexer._is_excluded_path(f)
        ]
        self._logger.info("Найдено файлов на диске: %d (поддерживаемые расширения)", len(all_files))

        def _normal_task(filepath: Path) -> Dict[str, Any]:
            fingerprint, mtime = indexer._get_file_fingerprint(filepath)
            return {
                "filepath": filepath,
                "source_path": filepath,
                "relative_path": filepath.relative_to(indexer.catalog_path),
                "state_key": str(filepath),
                "fingerprint": fingerprint,
                "mtime": mtime,
                "size_bytes": int(filepath.stat().st_size),
                "sort_mtime": float(mtime),
                "archive_path": None,
                "archive_member": "",
            }

        def _archive_member_category(ext: str, size_bytes: int) -> str:
            if ext in (".txt", ".csv", ".rtf", ".pptx"):
                return "small"
            if ext in (".docx", ".xlsx", ".xls") and size_bytes < indexer.small_office_mb * 1_048_576:
                return "small"
            if ext == ".pdf" and size_bytes < indexer.small_pdf_mb * 1_048_576:
                return "small"
            return "large"

        archive_member_keys: Dict[str, set[str]] = {}

        def _archive_member_task(
            *,
            filepath: Path,
            archive_fingerprint: str,
            archive_mtime: float,
            archive_rel: str,
            archive_type: str,
            archive_member_raw: str,
            archive_member_display: str,
            size_bytes: int,
            member_mtime: float,
            fingerprint_extra: str,
        ) -> Dict[str, Any] | None:
            member = repair_zip_member_name(archive_member_display.replace("\\", "/").lstrip("/"))
            ext = Path(member).suffix.lower()
            if ext not in self._supported_extensions or _is_nested_archive_member(member):
                return None
            logical_path = Path(f"{archive_rel}/{member}")
            if indexer._is_excluded_path(logical_path):
                return None
            return {
                "filepath": filepath,
                "source_path": filepath,
                "relative_path": logical_path,
                "state_key": f"{filepath}::{member}",
                "fingerprint": f"{archive_fingerprint}:{archive_type}:{fingerprint_extra}",
                "mtime": float(member_mtime),
                "size_bytes": int(size_bytes),
                "sort_mtime": float(archive_mtime),
                "archive_path": filepath,
                "archive_type": archive_type,
                "archive_member": archive_member_raw,
                "archive_member_display": member,
                "archive_category": _archive_member_category(ext, int(size_bytes)),
            }

        def _zip_tasks(filepath: Path) -> List[Dict[str, Any]]:
            tasks: List[Dict[str, Any]] = []
            archive_fingerprint, archive_mtime = indexer._get_file_fingerprint(filepath)
            archive_rel = filepath.relative_to(indexer.catalog_path).as_posix()
            try:
                with ZipFile(filepath, "r") as zf:
                    for info in zf.infolist():
                        if info.is_dir():
                            continue
                        archive_member_raw = info.filename
                        archive_member_display = archive_member_raw.replace("\\", "/").lstrip("/")
                        try:
                            member_dt = datetime(*info.date_time).timestamp()
                        except Exception:
                            member_dt = archive_mtime
                        task = _archive_member_task(
                            filepath=filepath,
                            archive_fingerprint=archive_fingerprint,
                            archive_mtime=archive_mtime,
                            archive_rel=archive_rel,
                            archive_type="zip",
                            archive_member_raw=archive_member_raw,
                            archive_member_display=archive_member_display,
                            size_bytes=int(info.file_size),
                            member_mtime=float(member_dt),
                            fingerprint_extra=f"{info.CRC}:{info.file_size}:{info.date_time}",
                        )
                        if task is not None:
                            tasks.append(task)
                archive_member_keys[str(filepath)] = {str(task["state_key"]) for task in tasks}
            except Exception as exc:
                self._logger.warning("ZIP %s не прочитан: %s", filepath, exc)
            return tasks

        def _tar_tasks(filepath: Path) -> List[Dict[str, Any]]:
            tasks: List[Dict[str, Any]] = []
            archive_fingerprint, archive_mtime = indexer._get_file_fingerprint(filepath)
            archive_rel = filepath.relative_to(indexer.catalog_path).as_posix()
            try:
                with tarfile.open(filepath, "r:*") as tf:
                    for info in tf.getmembers():
                        if not info.isfile():
                            continue
                        raw_name = str(info.name or "")
                        task = _archive_member_task(
                            filepath=filepath,
                            archive_fingerprint=archive_fingerprint,
                            archive_mtime=archive_mtime,
                            archive_rel=archive_rel,
                            archive_type="tar",
                            archive_member_raw=raw_name,
                            archive_member_display=raw_name,
                            size_bytes=int(info.size or 0),
                            member_mtime=float(info.mtime or archive_mtime),
                            fingerprint_extra=f"{raw_name}:{info.size}:{info.mtime}:{info.chksum}",
                        )
                        if task is not None:
                            tasks.append(task)
                archive_member_keys[str(filepath)] = {str(task["state_key"]) for task in tasks}
            except Exception as exc:
                self._logger.warning("TAR %s не прочитан: %s", filepath, exc)
            return tasks

        def _seven_zip_tasks(filepath: Path) -> List[Dict[str, Any]]:
            tasks: List[Dict[str, Any]] = []
            try:
                import py7zr  # type: ignore[import-not-found]
            except Exception as exc:
                self._logger.warning("7Z %s не прочитан: py7zr недоступен (%s)", filepath, exc)
                return tasks
            archive_fingerprint, archive_mtime = indexer._get_file_fingerprint(filepath)
            archive_rel = filepath.relative_to(indexer.catalog_path).as_posix()
            try:
                with py7zr.SevenZipFile(filepath, "r") as zf:
                    if zf.needs_password():
                        self._logger.warning("7Z %s пропущен: архив защищён паролем", filepath)
                        return tasks
                    for info in zf.list():
                        if getattr(info, "is_directory", False) or not getattr(info, "is_file", True):
                            continue
                        raw_name = str(getattr(info, "filename", "") or "")
                        created = getattr(info, "creationtime", None)
                        try:
                            member_dt = float(created.timestamp()) if created is not None else archive_mtime
                        except Exception:
                            member_dt = archive_mtime
                        size_bytes = int(getattr(info, "uncompressed", 0) or 0)
                        crc = str(getattr(info, "crc32", "") or "")
                        task = _archive_member_task(
                            filepath=filepath,
                            archive_fingerprint=archive_fingerprint,
                            archive_mtime=archive_mtime,
                            archive_rel=archive_rel,
                            archive_type="7z",
                            archive_member_raw=raw_name,
                            archive_member_display=raw_name,
                            size_bytes=size_bytes,
                            member_mtime=member_dt,
                            fingerprint_extra=f"{raw_name}:{size_bytes}:{member_dt}:{crc}",
                        )
                        if task is not None:
                            tasks.append(task)
                archive_member_keys[str(filepath)] = {str(task["state_key"]) for task in tasks}
            except Exception as exc:
                self._logger.warning("7Z %s не прочитан: %s", filepath, exc)
            return tasks

        def _command_archive_tasks(filepath: Path) -> List[Dict[str, Any]]:
            tasks: List[Dict[str, Any]] = []
            archive_fingerprint, archive_mtime = indexer._get_file_fingerprint(filepath)
            archive_rel = filepath.relative_to(indexer.catalog_path).as_posix()
            try:
                tool_name, members = _list_command_archive_members(filepath)
                for member_info in members:
                    raw_name = str(member_info.get("name") or "").strip()
                    if not raw_name or raw_name.endswith("/"):
                        continue
                    size_bytes = int(member_info.get("size_bytes") or 0)
                    member_mtime = archive_mtime
                    task = _archive_member_task(
                        filepath=filepath,
                        archive_fingerprint=archive_fingerprint,
                        archive_mtime=archive_mtime,
                        archive_rel=archive_rel,
                        archive_type="command",
                        archive_member_raw=raw_name,
                        archive_member_display=raw_name,
                        size_bytes=size_bytes,
                        member_mtime=member_mtime,
                        fingerprint_extra=f"{tool_name}:{raw_name}:{size_bytes}:{member_info.get('crc') or ''}:{archive_fingerprint}",
                    )
                    if task is not None:
                        tasks.append(task)
                archive_member_keys[str(filepath)] = {str(task["state_key"]) for task in tasks}
            except Exception as exc:
                self._logger.warning("Архив %s не прочитан: %s", filepath, exc)
            return tasks

        all_tasks: List[Dict[str, Any]] = []
        for filepath in all_files:
            archive_type = _archive_type_for_path(filepath)
            if archive_type == "zip":
                all_tasks.extend(_zip_tasks(filepath))
            elif archive_type == "tar":
                all_tasks.extend(_tar_tasks(filepath))
            elif archive_type == "7z":
                all_tasks.extend(_seven_zip_tasks(filepath))
            elif archive_type == "command":
                all_tasks.extend(_command_archive_tasks(filepath))
            else:
                all_tasks.append(_normal_task(filepath))

        if hasattr(indexer, "state_db"):
            for archive_path, current_keys in archive_member_keys.items():
                prefix = f"{archive_path}::"
                stale_keys = sorted(set(indexer.state_db.list_entries_by_prefix(prefix)) - set(current_keys))
                if not stale_keys:
                    continue
                self._logger.info("ZIP cleanup: %s — удаляю %d устаревших entries", archive_path, len(stale_keys))
                for key in stale_keys:
                    indexer._delete_file_vectors(Path(key))
                indexer.state_db.delete_entries(stale_keys)

        only_paths = {
            _normalize_only_path_key(path)
            for path in (getattr(indexer, "only_paths", None) or set())
            if str(path or "").strip()
        }
        if only_paths:
            before = len(all_tasks)
            all_tasks = [item for item in all_tasks if _task_matches_only_paths(item, only_paths)]
            self._logger.info("Ограничение списка файлов: %d → %d по --only-paths-file", before, len(all_tasks))

        # metadata/small/large видят все файлы. Разница не в типе файла, а в глубине:
        # small пишет первые N чанков, large догружает хвост и закрывает content.
        if stage == "small":
            scope_files = all_tasks
            self._logger.info(
                "Этап 'small': быстрый проход по всем файлам, лимит %d чанков/файл",
                int(getattr(indexer, "max_chunks_per_file", 0) or 0),
            )
        elif stage == "large":
            scope_files = all_tasks
            self._logger.info("Этап 'large': полный проход по всем файлам / догрузка оставшихся чанков")
        else:
            # metadata или legacy "content" — работаем со всем
            scope_files = all_tasks
        scope_files = sorted(
            scope_files,
            key=lambda item: float(item.get("sort_mtime") or 0.0),
            reverse=True,
        )

        stage_stats: Dict[str, int] = {
            "total_files": len(scope_files),
            "processed_files": 0,
            "added_files": 0,
            "updated_files": 0,
            "skipped_files": 0,
            "error_files": 0,
            "points_added": 0,
        }
        if bool(getattr(indexer, "dry_run", False)):
            planned: List[Dict[str, str]] = []
            skipped = 0
            for item in scope_files:
                file_key = str(item["state_key"])
                fingerprint = str(item["fingerprint"])
                existing = indexer._get_state_entry(file_key)
                reason = ""
                if not existing:
                    reason = "new"
                elif str(existing.get("fingerprint") or "") != fingerprint:
                    reason = "changed"
                else:
                    existing_stage = str(existing.get("stage") or "content")
                    existing_status = str(existing.get("status") or ("error" if existing_stage == "error" else "ok"))
                    existing_ext = str(existing.get("extension") or Path(file_key).suffix or "").lower()
                    if existing_status == "error":
                        if hasattr(indexer, "state_db") and not indexer.state_db.is_failed_retry_due(file_key):
                            skipped += 1
                            continue
                        reason = "retry_error"
                    elif (
                        stage in ("small", "large")
                        and bool(getattr(indexer, "skip_ocr", False))
                        and existing_ext in {".pdf", *self._image_extensions}
                        and existing_status in {"deferred_ocr", "empty"}
                        and existing_stage in {"metadata", "empty"}
                        and str(existing.get("indexed_stage") or "") in {"small", "large"}
                    ):
                        skipped += 1
                        continue
                    elif stage == "metadata":
                        skipped += 1
                        continue
                    elif stage == "small" and existing_stage in ("content", "partial", "small"):
                        skipped += 1
                        continue
                    elif stage == "large" and existing_stage == "content":
                        indexed_stage = str(existing.get("indexed_stage") or "")
                        try:
                            indexed_chunks = int(existing.get("indexed_chunks") or 0)
                            total_chunks = int(existing.get("total_chunks") or 0)
                        except (TypeError, ValueError):
                            indexed_chunks = 0
                            total_chunks = 0
                        if indexed_stage != "small" or (indexed_chunks > 0 and (total_chunks <= 0 or indexed_chunks >= total_chunks)):
                            skipped += 1
                            continue
                    elif existing_stage in ("content", stage):
                        skipped += 1
                        continue
                    else:
                        reason = f"stage_upgrade:{existing_stage}->{stage}"
                planned.append({"path": Path(item["relative_path"]).as_posix(), "reason": reason})

            stage_stats["processed_files"] = len(planned)
            stage_stats["skipped_files"] = skipped
            stage_stats["dry_run_files"] = len(planned)
            self._logger.info(
                "--dry-run stage=%s: к обработке %d, пропуск %d",
                stage,
                len(planned),
                skipped,
            )
            for row in planned:
                self._logger.info("--dry-run: %s | %s", row["reason"], row["path"])
            return stage_stats

        last_telemetry_push = time.monotonic()
        telemetry_push_interval_sec = 1.0
        telemetry_push_every_n = 25
        if indexer.run_id:
            indexer.telemetry.start_stage(
                run_id=indexer.run_id,
                stage=stage,
                total_files=stage_stats["total_files"],
            )

        # ── буферы для batch-encode ──────────────────────────────────
        pending_texts: List[str] = []
        pending_payloads: List[Dict[str, Any]] = []
        pending_states: List[Dict[str, Any]] = []
        seen_content_hashes: Dict[str, str] = {}

        def _point_id(payload: Dict[str, Any]) -> str:
            doc_id = str(payload.get("doc_id") or payload.get("full_path") or "")
            if str(payload.get("type") or "") == "file_metadata":
                key = f"{doc_id}:metadata"
            else:
                key = f"{doc_id}:chunk:{int(payload.get('chunk_index') or 0)}"
            return str(uuid.uuid5(uuid.NAMESPACE_URL, key))

        def flush() -> None:
            """
            Batch-encode накопленных текстов и запись в Qdrant.
            Разбивает большой список на куски по ENCODE_BATCH,
            чтобы один вызов encode() не блокировал главный поток надолго.
            """
            if not pending_texts:
                if pending_states:
                    if hasattr(indexer, "state_db"):
                        indexer.state_db.upsert_many(pending_states)
                    else:
                        for row in pending_states:
                            indexer._upsert_state_entry(row)
                    pending_states.clear()
                return
            # Нарезаем на мини-батчи — encode каждого занимает < ~1 сек
            for i in range(0, len(pending_texts), ENCODE_BATCH):
                chunk_texts    = pending_texts[i : i + ENCODE_BATCH]
                chunk_payloads = pending_payloads[i : i + ENCODE_BATCH]
                vectors = indexer.embedder.encode(
                    chunk_texts, normalize_embeddings=True,
                    batch_size=256, show_progress_bar=False,
                )
                points = [
                    PointStruct(id=_point_id(p), vector=v.tolist(), payload=p)
                    for v, p in zip(vectors, chunk_payloads)
                ]
                written = upsert_points(
                    indexer.qdrant,
                    collection_name=indexer.collection_name,
                    points=points,
                    timeout_sec=int(getattr(indexer, "qdrant_timeout_sec", 60) or 60),
                )
                indexer.point_count += written
                stage_stats["points_added"] += written
            self._logger.info(
                "Записан батч: %d точек (итого %d)", len(pending_texts), indexer.point_count
            )
            if hasattr(indexer, "state_db"):
                indexer.state_db.upsert_many(pending_states)
            else:
                for row in pending_states:
                    indexer._upsert_state_entry(row)
            pending_texts.clear()
            pending_payloads.clear()
            pending_states.clear()

        # ── I/O-worker: читает один файл, возвращает тексты+payload ─
        def extract_one(item: Dict[str, Any]):
            """
            Выполняется в потоке-воркере.
            Не кодирует векторы (encode — в главном потоке).
            Возвращает None если файл не изменился.
            """
            source_path = Path(item["source_path"])
            relative_path = Path(item["relative_path"])
            file_key = str(item["state_key"])
            fingerprint = str(item["fingerprint"])
            mtime = float(item["mtime"])
            size_bytes = int(item.get("size_bytes") or 0)

            existing_entry = indexer._get_state_entry(file_key)
            if (
                existing_entry
                and str(existing_entry.get("status") or existing_entry.get("stage") or "") == "error"
                and hasattr(indexer, "state_db")
                and not indexer.state_db.is_failed_retry_due(file_key)
            ):
                return {"skipped": True}

            # Stage-aware skip:
            #  - на этапе metadata пропускаем любой уже проиндексированный файл;
            #  - на этапах small/large пропускаем только те, что уже дошли до "content".
            if indexer._should_skip_for_stage(file_key, fingerprint):
                return {"skipped": True}

            ext = relative_path.suffix.lower()
            size_mb = round(size_bytes / 1_048_576, 1)

            # Таймаут на извлечение. Для large-этапа PDFs включает OCR (tesseract),
            # который может занимать несколько минут на многостраничных документах.
            FILE_TIMEOUT = 600 if stage == "large" else 45  # секунд

            t_start = time.monotonic()
            full_text = ""
            extracted_doc: ExtractedDocument | None = None
            file_type = ""
            failure_error = ""

            _buf: list = [None, None]  # [result_text, exception]
            _doc_fn = None

            def _archive_member_bytes() -> bytes:
                archive_path = Path(str(item["archive_path"]))
                member_name = str(item["archive_member"])
                archive_type = str(item.get("archive_type") or "zip")
                if archive_type == "zip":
                    with ZipFile(archive_path, "r") as zf:
                        return zf.read(member_name)
                if archive_type == "tar":
                    with tarfile.open(archive_path, "r:*") as tf:
                        member = tf.extractfile(member_name)
                        if member is None:
                            raise KeyError(f"There is no file member named {member_name!r} in the archive")
                        return member.read()
                if archive_type == "7z":
                    import py7zr  # type: ignore[import-not-found]
                    from py7zr.io import BytesIOFactory  # type: ignore[import-not-found]

                    limit = max(1, int(item.get("size_bytes") or 0) + 1)
                    factory = BytesIOFactory(limit)
                    with py7zr.SevenZipFile(archive_path, "r") as zf:
                        zf.extract(targets=[member_name], factory=factory)
                    product = factory.get(member_name)
                    product.seek(0)
                    return product.read()
                if archive_type == "command":
                    return _extract_command_archive_member(archive_path, member_name)
                raise ValueError(f"Неподдерживаемый тип архива: {archive_type}")

            # Режим «только метадата»: либо текущий этап = metadata,
            # либо расширение явно в metadata_only_extensions (legacy флаг).
            # Содержимое не читается — файл попадает в индекс по имени/пути/размеру.
            if indexer.current_stage == "metadata" or ext in indexer.metadata_only_extensions:
                file_type = ext.lstrip(".") or "file"
                _fn = None
            elif ext == ".docx":
                file_type = "docx"
                _fn = indexer._extract_docx
            elif ext == ".doc":
                file_type = "doc"
                _fn = indexer._extract_doc
            elif ext in (".xlsx", ".xlsm", ".xls"):
                file_type = "xlsx"
                _fn = None
                _doc_fn = indexer._extract_spreadsheet_document
            elif ext == ".rtf":
                file_type = "rtf"
                _fn = indexer._extract_rtf
            elif ext == ".pptx":
                file_type = "pptx"
                _fn = None
                _doc_fn = indexer._extract_pptx_document
            elif ext == ".txt":
                file_type = "txt"
                _fn = indexer._extract_text
            elif ext == ".csv":
                file_type = "csv"
                _fn = indexer._extract_csv
            elif ext in (".html", ".htm"):
                file_type = "html"
                _fn = indexer._extract_html
            elif ext == ".pdf":
                file_type = "pdf"
                _fn = None
                _doc_fn = indexer._extract_pdf_document
            elif ext in self._image_extensions:
                if indexer.skip_ocr:
                    file_type = "image"
                    _fn = indexer._cached_ocr_text
                else:
                    file_type = "image"
                    _fn = indexer._extract_image
            else:
                _fn = None

            if _fn is not None or _doc_fn is not None:
                # Логируем тяжёлые файлы заранее — только для тех, что реально читаем
                if size_mb >= 5:
                    self._logger.info("Читаю крупный файл (%.1f МБ): %s", size_mb, relative_path.name)

                import threading as _th

                # Проверяем семафор без блокировки: если лимит зависших потоков
                # исчерпан — пропускаем файл, не создавая новый поток.
                if not _reader_sem.acquire(blocking=False):
                    self._logger.warning(
                        "Лимит daemon-потоков исчерпан (%d): пропускаю %s",
                        indexer.read_workers * 2, relative_path.name,
                    )
                    full_text = ""
                    failure_error = "reader_limit_exhausted"
                else:
                    def _reader():
                        try:
                            reader_fn = _doc_fn or _fn
                            if item.get("archive_path") and item.get("archive_member"):
                                with tempfile.TemporaryDirectory(prefix="rag_zip_") as tmp:
                                    temp_path = Path(tmp) / Path(str(item.get("archive_member_display") or item["archive_member"])).name
                                    temp_path.write_bytes(_archive_member_bytes())
                                    _buf[0] = reader_fn(temp_path)
                            else:
                                _buf[0] = reader_fn(source_path)
                        except Exception as _e:
                            _buf[1] = _e

                    _t = _th.Thread(target=_reader, daemon=True)
                    _t.start()
                    _t.join(timeout=FILE_TIMEOUT)
                    if _t.is_alive():
                        # Поток завис (SMB stall) — освобождаем семафор только
                        # после его завершения через отдельный cleanup-поток.
                        self._logger.warning(
                            "ТАЙМАУТ SMB (>%dс): пропускаю %s — воркер остался в фоне",
                            FILE_TIMEOUT, relative_path.name,
                        )
                        def _cleanup(_t=_t, _sem=_reader_sem):
                            _t.join()        # ждём в фоне сколько потребуется
                            _sem.release()   # освобождаем слот
                        _th.Thread(target=_cleanup, daemon=True).start()
                        full_text = ""
                        failure_error = f"timeout>{FILE_TIMEOUT}s"
                    else:
                        _reader_sem.release()   # поток завершился штатно
                        if _buf[1] is not None:
                            self._logger.warning("Ошибка чтения %s: %s", relative_path.name, _buf[1])
                            full_text = ""
                            failure_error = str(_buf[1])
                        else:
                            if isinstance(_buf[0], ExtractedDocument):
                                extracted_doc = _buf[0]
                                full_text = extracted_doc.text
                            else:
                                full_text = _buf[0] or ""

            elapsed = time.monotonic() - t_start
            if elapsed >= 30:
                self._logger.warning(
                    "Долгое извлечение (%.0fс, %.1f МБ): %s",
                    elapsed, size_mb, relative_path.name,
                )

            deferred_ocr = (
                stage in ("small", "large")
                and bool(getattr(indexer, "skip_ocr", False))
                and (ext == ".pdf" or ext in self._image_extensions)
                and not failure_error
                and not full_text.strip()
            )

            chunk_source = extracted_doc if extracted_doc is not None else full_text
            chunk_items = indexer._chunk_text_with_provenance(chunk_source) if full_text.strip() else []
            chunks = [str(item.get("text") or "") for item in chunk_items]
            total_chunks = len(chunks)
            stage_chunk_limit = int(getattr(indexer, "max_chunks_per_file", 0) or 0) if stage == "small" else 0
            append_from_chunk = 0
            append_only = False
            if (
                stage == "large"
                and existing_entry
                and str(existing_entry.get("fingerprint") or "") == fingerprint
                and str(existing_entry.get("stage") or "") in {"partial", "small"}
            ):
                try:
                    append_from_chunk = max(0, int(existing_entry.get("indexed_chunks") or 0))
                except (TypeError, ValueError):
                    append_from_chunk = 0
                if append_from_chunk > 0:
                    append_only = True
            content_hash = indexer._content_hash(full_text)
            duplicate_of = ""
            if content_hash and hasattr(indexer, "state_db"):
                duplicate = indexer.state_db.find_by_content_hash(content_hash, exclude_path=file_key)
                duplicate_of = str((duplicate or {}).get("full_path") or "")
            if stage_chunk_limit and len(chunks) >= stage_chunk_limit:
                self._logger.debug(
                    "Файл %s: %d чанков → обрезано до %d",
                    relative_path.name, len(chunks), stage_chunk_limit,
                )
                chunk_items = chunk_items[:stage_chunk_limit]
                chunks = [str(item.get("text") or "") for item in chunk_items]
                # При быстром проходе экстрактор мог остановиться по лимиту символов,
                # поэтому точное число чанков может быть неизвестно. Важно отметить,
                # что файл требует full-прохода.
                total_chunks = max(total_chunks, len(chunks) + 1)
            elif append_only and append_from_chunk < len(chunk_items):
                chunk_items = chunk_items[append_from_chunk:]
                chunks = [str(item.get("text") or "") for item in chunk_items]
            elif append_only:
                chunk_items = []
                chunks = []

            # Генерируем теги для файла (по пути, содержимому, синонимам)
            tags = self._generate_tags(source_path, relative_path, full_text, getattr(indexer, "synonym_map", {}) or {})

            stat = source_path.stat()
            logical_path_text = relative_path.as_posix()
            if item.get("archive_path") and item.get("archive_member"):
                with tempfile.TemporaryDirectory(prefix="rag_zip_meta_") as tmp:
                    temp_path = Path(tmp) / Path(str(item.get("archive_member_display") or item["archive_member"])).name
                    temp_path.write_bytes(_archive_member_bytes())
                    doc_meta = extract_doc_meta(temp_path)
            else:
                doc_meta = extract_doc_meta(source_path)
            meta_text = (
                f"Файл: {relative_path.name} | Путь: {logical_path_text}"
                f" | Расширение: {ext}"
            )
            if doc_meta.get("doc_author"):
                meta_text += f" | Автор: {doc_meta['doc_author']}"
            if doc_meta.get("doc_last_editor"):
                meta_text += f" | Редактор: {doc_meta['doc_last_editor']}"
            if tags:
                meta_text += f" | Теги: {', '.join(tags[:30])}"
            meta_payload: Dict[str, Any] = {
                "type": "file_metadata",
                "payload_schema_version": int(getattr(indexer, "payload_schema_version", 1) or 1),
                "text": meta_text,
                "filename": relative_path.name,
                "extension": ext,
                "size_mb": round(size_bytes / (1024 * 1024), 2),
                "modified": datetime.fromtimestamp(mtime).isoformat(),
                "created": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                "path": logical_path_text,
                "full_path": file_key,
                "tags": tags,
                "content_hash": content_hash,
                "is_duplicate": bool(duplicate_of),
                "duplicate_of": duplicate_of,
                **doc_meta,
            }
            if item.get("archive_path") and item.get("archive_member"):
                meta_payload.update(
                    {
                        "archive_path": str(item["archive_path"]),
                        "archive_member": str(item["archive_member"]),
                        "archive_member_display": str(item.get("archive_member_display") or item["archive_member"]),
                    }
                )
            add_numeric_tokens(meta_payload, meta_text, relative_path.name, logical_path_text)
            base_provenance = indexer._base_provenance(
                filepath=Path(file_key),
                relative_path=relative_path,
                state_key=file_key,
                payload_extra=None,
            )
            meta_payload.update(base_provenance)
            doc_id = str(base_provenance["doc_id"])
            content_payloads = []
            chunk_index_offset = append_from_chunk if append_only else 0
            for offset, item in enumerate(chunk_items):
                idx = chunk_index_offset + offset
                chunk = str(item.get("text") or "")
                clean_chunk = indexer._strip_provenance_markers(chunk) or chunk
                chunk_payload = {
                    "type": f"{file_type}_content",
                    "payload_schema_version": int(getattr(indexer, "payload_schema_version", 1) or 1),
                    "text": clean_chunk,
                    "filename": relative_path.name,
                    "extension": ext,
                    "path": logical_path_text,
                    "full_path": file_key,
                    "chunk_index": idx,
                    "tags": tags,
                    "content_hash": content_hash,
                    "is_duplicate": bool(duplicate_of),
                    "duplicate_of": duplicate_of,
                    **doc_meta,
                    **base_provenance,
                    **indexer._chunk_provenance(
                        chunk=chunk,
                        chunk_index=idx,
                        doc_id=doc_id,
                        block=item.get("block"),
                    ),
                }
                add_numeric_tokens(chunk_payload, clean_chunk, relative_path.name, logical_path_text)
                content_payloads.append(chunk_payload)
            return {
                "filepath": source_path,
                "source_path": source_path,
                "file_key": file_key,
                "fingerprint": fingerprint,
                "mtime": mtime,
                "size_bytes": size_bytes,
                "was_indexed": indexer._get_state_entry(file_key) is not None,
                "meta_text": meta_text,
                "meta_payload": meta_payload,
                "chunks": chunks,
                "content_payloads": content_payloads,
                "has_content": bool(chunks),
                "append_only": append_only,
                "indexed_chunks": (append_from_chunk + len(chunks)) if append_only else len(chunks),
                "total_chunks": total_chunks,
                "content_hash": content_hash,
                "error": failure_error,
                "deferred_ocr": deferred_ocr,
                "skipped": False,
            }

        # ── основной pipeline ────────────────────────────────────────
        with ThreadPoolExecutor(max_workers=indexer.read_workers) as pool:
            futures = {pool.submit(extract_one, item): item for item in scope_files}
            for future in tqdm(as_completed(futures), total=len(scope_files),
                                desc=f"Этап {stage}"):
                try:
                    result = future.result()
                except Exception as exc:
                    fp = futures[future].get("relative_path") or futures[future].get("filepath")
                    self._logger.error("Ошибка обработки %s: %s", fp, exc, exc_info=True)
                    stage_stats["error_files"] += 1
                    stage_stats["processed_files"] += 1
                    indexer._check_indexer_control(stage=stage, stage_stats=stage_stats)
                    continue

                if result is None:
                    indexer._check_indexer_control(stage=stage, stage_stats=stage_stats)
                    continue  # файл не изменился
                if result.get("skipped"):
                    stage_stats["skipped_files"] += 1
                    stage_stats["processed_files"] += 1
                    if indexer.run_id and (
                        stage_stats["processed_files"] % telemetry_push_every_n == 0
                        or (time.monotonic() - last_telemetry_push) >= telemetry_push_interval_sec
                    ):
                        indexer.telemetry.update_stage(
                            run_id=indexer.run_id,
                            stage=stage,
                            processed_files=stage_stats["processed_files"],
                            added_files=stage_stats["added_files"],
                            updated_files=stage_stats["updated_files"],
                            skipped_files=stage_stats["skipped_files"],
                            error_files=stage_stats["error_files"],
                            points_added=stage_stats["points_added"],
                        )
                        last_telemetry_push = time.monotonic()
                    indexer._check_indexer_control(stage=stage, stage_stats=stage_stats)
                    continue

                stage_stats["processed_files"] += 1
                indexer._check_indexer_control(stage=stage, stage_stats=stage_stats)

                content_hash = str(result.get("content_hash") or "")
                if content_hash:
                    duplicate_of = seen_content_hashes.get(content_hash, "")
                    if not duplicate_of and hasattr(indexer, "state_db"):
                        duplicate = indexer.state_db.find_by_content_hash(content_hash, exclude_path=str(result["file_key"]))
                        duplicate_of = str((duplicate or {}).get("full_path") or "")
                    if duplicate_of:
                        result["meta_payload"]["is_duplicate"] = True
                        result["meta_payload"]["duplicate_of"] = duplicate_of
                        for cpayload in result["content_payloads"]:
                            cpayload["is_duplicate"] = True
                            cpayload["duplicate_of"] = duplicate_of
                    else:
                        seen_content_hashes[content_hash] = str(result["file_key"])

                # При full-проходе после quick не трогаем уже записанные первые чанки,
                # а добавляем только хвост. В остальных случаях старые векторы заменяются.
                if result["was_indexed"] and not result.get("append_only"):
                    indexer._delete_file_vectors(Path(result["file_key"]))
                    stage_stats["updated_files"] += 1
                elif result["was_indexed"]:
                    stage_stats["updated_files"] += 1
                else:
                    stage_stats["added_files"] += 1

                # Добавить метаданные и контентные чанки в буфер
                if not result.get("append_only"):
                    pending_texts.append(result["meta_text"])
                    pending_payloads.append(result["meta_payload"])
                for cpayload in result["content_payloads"]:
                    pending_texts.append(str(cpayload.get("text") or ""))
                    pending_payloads.append(cpayload)
                if result.get("error"):
                    file_stage = "error"
                    status = "error"
                    last_error = str(result.get("error") or "")
                    next_retry_at = 0.0
                    stage_stats["error_files"] += 1
                    if hasattr(indexer, "state_db"):
                        failed_row = indexer.state_db.record_failed_path(
                            str(result["file_key"]),
                            fingerprint=str(result["fingerprint"]),
                            error=last_error,
                        )
                        try:
                            next_retry_at = float(failed_row.get("next_retry_at") or 0.0)
                        except (TypeError, ValueError):
                            next_retry_at = 0.0
                else:
                    if hasattr(indexer, "state_db"):
                        indexer.state_db.clear_failed_path(str(result["file_key"]))
                    indexed_chunks = int(result.get("indexed_chunks") or 0)
                    total_chunks = int(result.get("total_chunks") or 0)
                    if stage == "metadata":
                        file_stage = "metadata"
                    elif result.get("deferred_ocr"):
                        file_stage = "metadata"
                    elif result.get("has_content") or result.get("append_only"):
                        file_stage = (
                            "partial"
                            if stage == "small" and total_chunks > 0 and indexed_chunks < total_chunks
                            else "content"
                        )
                    else:
                        file_stage = "empty"
                    status = (
                        "deferred_ocr"
                        if result.get("deferred_ocr")
                        else "empty" if file_stage == "empty" else "ok"
                    )
                    last_error = "deferred_ocr" if result.get("deferred_ocr") else ""
                    next_retry_at = 0.0
                if stage in ("small", "large") and not result.get("has_content") and not result.get("deferred_ocr"):
                    self._logger.warning(
                        "Этап %s: файл %s без контента, сохраняю stage=%s (будет повторная попытка)",
                        stage,
                        Path(str(result["file_key"])).name,
                        file_stage,
                    )
                pending_states.append(
                    {
                        "full_path": result["file_key"],
                        "fingerprint": result["fingerprint"],
                        "mtime": result["mtime"],
                        "stage": file_stage,
                        "indexed_stage": stage,
                        "status": status,
                        "last_error": last_error,
                        "next_retry_at": next_retry_at,
                        "size_bytes": int(result.get("size_bytes") or 0),
                        "extension": str(result["meta_payload"].get("extension") or ""),
                        "content_hash": str(result.get("content_hash") or ""),
                        "indexed_chunks": int(result.get("indexed_chunks") or 0),
                        "total_chunks": int(result.get("total_chunks") or 0),
                    }
                )

                # Достигли порога — кодируем и пишем в Qdrant
                if len(pending_texts) >= ENCODE_BATCH:
                    flush()

                if indexer.run_id and (
                    stage_stats["processed_files"] % telemetry_push_every_n == 0
                    or (time.monotonic() - last_telemetry_push) >= telemetry_push_interval_sec
                ):
                    indexer.telemetry.update_stage(
                        run_id=indexer.run_id,
                        stage=stage,
                        processed_files=stage_stats["processed_files"],
                        added_files=stage_stats["added_files"],
                        updated_files=stage_stats["updated_files"],
                        skipped_files=stage_stats["skipped_files"],
                        error_files=stage_stats["error_files"],
                        points_added=stage_stats["points_added"],
                    )
                    last_telemetry_push = time.monotonic()

        flush()  # финальный батч (остаток)

        self._logger.info("Этап '%s' завершён. Добавлено точек за сессию: %d",
                    stage, indexer.point_count)

        # Чистим «фантомы» только когда имеем полный список всех файлов на диске
        # (т.е. на этапах metadata и content). На small/large мы видим только
        # часть файлов и не должны по этому основанию удалять других.
        if stage in ("metadata", "content"):
            indexer._run_deleted_files += indexer._cleanup_deleted_files([str(item["state_key"]) for item in all_tasks])

        info = indexer.qdrant.get_collection(indexer.collection_name)
        self._logger.info("Коллекция '%s': %d точек", indexer.collection_name, info.points_count)
        if indexer.run_id:
            indexer.telemetry.finish_stage(
                run_id=indexer.run_id,
                stage=stage,
                status="completed",
                processed_files=stage_stats["processed_files"],
                added_files=stage_stats["added_files"],
                updated_files=stage_stats["updated_files"],
                skipped_files=stage_stats["skipped_files"],
                error_files=stage_stats["error_files"],
                points_added=stage_stats["points_added"],
            )
        return stage_stats
