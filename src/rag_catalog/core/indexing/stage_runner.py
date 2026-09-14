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
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Tuple
from zipfile import ZipFile

from qdrant_client.models import PointStruct
from tqdm import tqdm

from ..exact_tokens import add_numeric_tokens, repair_zip_member_name
from ..extractors import ExtractedDocument, extract_doc_meta, is_unreadable_source_error
from ..indexer_control import read_indexer_control
from ..retrieval import prepare_passage_texts
from .heartbeat import STATUS_FAILED as HEARTBEAT_FAILED
from .heartbeat import STATUS_FINISHED as HEARTBEAT_FINISHED
from .heartbeat import STATUS_RUNNING as HEARTBEAT_RUNNING
from .heartbeat import write_heartbeat
from .ocr_deferral import document_has_deferred_embedded_ocr, document_ocr_error, is_deferred_ocr_candidate
from .qdrant_writer import upsert_points

_TAR_ARCHIVE_SUFFIXES = (".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz", ".tbz2", ".tar.xz", ".txz")
_COMMAND_ARCHIVE_SUFFIXES = (".rar",)
_NESTED_ARCHIVE_SUFFIXES = (*_TAR_ARCHIVE_SUFFIXES, ".zip", ".7z", *_COMMAND_ARCHIVE_SUFFIXES)
_COMMAND_ARCHIVE_TOOLS = ("bsdtar", "7z", "7zz", "7za")
_WINDOWS_7Z_PATHS = (
    Path("C:/Program Files/7-Zip/7z.exe"),
    Path("C:/Program Files (x86)/7-Zip/7z.exe"),
)
# Пустое извлечение (не PDF/картинка): повторная попытка через 24 ч, не на каждом прогоне.
EMPTY_RETRY_DELAY_SEC = 86_400
EMPTY_RETRY_MAX_DELAY_SEC = 7 * 86_400
# Если не прочитана большая доля inventory — cleanup «фантомов» слишком опасен.
FAILED_INVENTORY_CLEANUP_SKIP_RATIO = 0.10
# Столько батчей подряд не удалось записать в Qdrant (после всех ретраев) —
# Qdrant недоступен, продолжать прогон бессмысленно: файлы уже помечены error.
QDRANT_CONSECUTIVE_FLUSH_FAILURES_ABORT = 3


def _encode_with_transient_retry(
    embedder: Any,
    texts: List[str],
    *,
    initial_batch_size: int,
    logger: logging.Logger,
) -> Any:
    """Retry malformed ONNX/DirectML errors with a smaller inference batch."""
    batch_sizes = tuple(
        dict.fromkeys(
            (
                max(1, int(initial_batch_size)),
                max(1, int(initial_batch_size) // 2),
                max(1, int(initial_batch_size) // 4),
            )
        )
    )
    for attempt, batch_size in enumerate(batch_sizes, start=1):
        try:
            return embedder.encode(
                texts,
                normalize_embeddings=True,
                batch_size=batch_size,
                show_progress_bar=False,
            )
        except UnicodeDecodeError:
            if attempt >= len(batch_sizes):
                raise
            next_batch_size = batch_sizes[attempt]
            logger.warning(
                "ONNX/DirectML вернул повреждённое сообщение об ошибке; "
                "повторяю embedding-батч %d с batch_size=%d (попытка %d/%d)",
                len(texts),
                next_batch_size,
                attempt + 1,
                len(batch_sizes),
            )
            time.sleep(0.5 * attempt)


def _wait_for_reader_thread(thread: Any, *, timeout_sec: float) -> str:
    """Wait in short intervals so cancel is not hidden by a long extraction."""
    deadline = time.monotonic() + max(0.1, float(timeout_sec))
    while thread.is_alive():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return "timeout"
        thread.join(timeout=min(1.0, remaining))
        if str(read_indexer_control().get("command") or "running").lower() == "cancel":
            return "cancel"
    return "completed"


def _bounded_executor_results(
    pool: ThreadPoolExecutor,
    worker: Callable[[Dict[str, Any]], Any],
    items: Iterable[Dict[str, Any]],
    *,
    max_in_flight: int,
) -> Iterator[Tuple[Future[Any], Dict[str, Any]]]:
    """Yield completed work without retaining results for the whole corpus."""
    source_iter = iter(items)
    futures: Dict[Future[Any], Dict[str, Any]] = {}

    def submit_next() -> bool:
        try:
            item = next(source_iter)
        except StopIteration:
            return False
        futures[pool.submit(worker, item)] = item
        return True

    for _ in range(max(1, int(max_in_flight))):
        if not submit_next():
            break

    while futures:
        completed, _ = wait(tuple(futures), return_when=FIRST_COMPLETED)
        for future in completed:
            source_item = futures.pop(future)
            submit_next()
            yield future, source_item


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


def _task_only_path_keys(item: Dict[str, Any]) -> set[str]:
    return {
        _normalize_only_path_key(key)
        for key in (
            item.get("state_key"),
            item.get("filepath"),
            item.get("source_path"),
            item.get("relative_path"),
        )
        if key
    }


def _task_matches_only_paths(item: Dict[str, Any], allowed: set[str]) -> bool:
    if not allowed:
        return True
    return bool(_task_only_path_keys(item) & allowed)


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
        """Запустить этап; при падении записать heartbeat status=failed и пробросить."""
        self._stage_progress: Dict[str, int] = {}
        try:
            return self._run_stage(stage)
        except BaseException:
            heartbeat_path = str(getattr(self._indexer, "heartbeat_path", "") or "")
            if heartbeat_path:
                progress = self._stage_progress or {}
                write_heartbeat(
                    heartbeat_path,
                    stage=stage,
                    processed=int(progress.get("processed_files", 0)),
                    total=int(progress.get("total_files", 0)),
                    run_id=str(getattr(self._indexer, "run_id", "") or ""),
                    status=HEARTBEAT_FAILED,
                )
            raise

    def _run_stage(self, stage: str) -> Dict[str, int]:
        """
        Pipeline-индексирование на указанном этапе.

          stage="metadata" — только имя/путь/размер/mtime (не читает файлы);
          stage="small"    — быстрый проход по небольшим файлам, ограниченный max_chunks;
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

        # ONNX Runtime резервирует arena под максимальный inference batch и не
        # всегда возвращает её ОС. Batch 256 занимал почти 10 ГБ на 32-ГБ хосте;
        # 64 сохраняет векторизацию пакетами без вытеснения web/bot из памяти.
        ENCODE_BATCH = 64
        WRITE_BATCH = max(ENCODE_BATCH, min(2048, int(getattr(indexer, "batch_size", 1000) or 1000)))

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

        # Файлы/архивы, которые не удалось прочитать при построении inventory.
        # Их записи в state НЕЛЬЗЯ считать «удалёнными с диска» — см. cleanup ниже.
        # Для архива корень = "<archive_path>::", для обычного файла = сам путь.
        failed_inventory_roots: set[str] = set()

        def _normal_task(filepath: Path) -> Dict[str, Any] | None:
            try:
                fingerprint, mtime = indexer._get_file_fingerprint(filepath)
                size_bytes = int(filepath.stat().st_size)
            except OSError:
                self._logger.debug("Файл исчез во время сканирования, пропуск: %s", filepath)
                failed_inventory_roots.add(str(filepath))
                return None
            return {
                "filepath": filepath,
                "source_path": filepath,
                "relative_path": filepath.relative_to(indexer.catalog_path),
                "state_key": str(filepath),
                "fingerprint": fingerprint,
                "mtime": mtime,
                "size_bytes": size_bytes,
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

        def _archive_read_failed(filepath: Path, label: str, exc: Exception) -> None:
            failed_inventory_roots.add(f"{filepath}::")
            self._logger.warning("%s %s не прочитан: %s", label, filepath, exc)

        def _zip_tasks(filepath: Path) -> List[Dict[str, Any]]:
            tasks: List[Dict[str, Any]] = []
            try:
                archive_fingerprint, archive_mtime = indexer._get_file_fingerprint(filepath)
                archive_rel = filepath.relative_to(indexer.catalog_path).as_posix()
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
                _archive_read_failed(filepath, "ZIP", exc)
                return []
            return tasks

        def _tar_tasks(filepath: Path) -> List[Dict[str, Any]]:
            tasks: List[Dict[str, Any]] = []
            try:
                archive_fingerprint, archive_mtime = indexer._get_file_fingerprint(filepath)
                archive_rel = filepath.relative_to(indexer.catalog_path).as_posix()
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
                _archive_read_failed(filepath, "TAR", exc)
                return []
            return tasks

        def _seven_zip_tasks(filepath: Path) -> List[Dict[str, Any]]:
            tasks: List[Dict[str, Any]] = []
            try:
                import py7zr  # type: ignore[import-not-found]
            except Exception as exc:
                _archive_read_failed(filepath, "7Z (py7zr недоступен)", exc)
                return tasks
            try:
                archive_fingerprint, archive_mtime = indexer._get_file_fingerprint(filepath)
                archive_rel = filepath.relative_to(indexer.catalog_path).as_posix()
                with py7zr.SevenZipFile(filepath, "r") as zf:
                    if zf.needs_password():
                        self._logger.warning("7Z %s пропущен: архив защищён паролем", filepath)
                        failed_inventory_roots.add(f"{filepath}::")
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
                _archive_read_failed(filepath, "7Z", exc)
                return []
            return tasks

        def _command_archive_tasks(filepath: Path) -> List[Dict[str, Any]]:
            tasks: List[Dict[str, Any]] = []
            try:
                archive_fingerprint, archive_mtime = indexer._get_file_fingerprint(filepath)
                archive_rel = filepath.relative_to(indexer.catalog_path).as_posix()
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
                _archive_read_failed(filepath, "Архив", exc)
                return []
            return tasks

        only_paths = {
            _normalize_only_path_key(path)
            for path in (getattr(indexer, "only_paths", None) or set())
            if str(path or "").strip()
        }
        all_tasks: List[Dict[str, Any]] = []
        for filepath in all_files:
            if only_paths:
                keys = {str(filepath), str(filepath.relative_to(indexer.catalog_path))}
                normalized = {_normalize_only_path_key(key) for key in keys}
                selected = bool(normalized & only_paths) or any(
                    requested.startswith(key + "::") or requested.startswith(key + "\\")
                    for key in normalized for requested in only_paths
                )
                if not selected:
                    continue
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
                task = _normal_task(filepath)
                if task is not None:
                    all_tasks.append(task)

        if hasattr(indexer, "state_db") and not only_paths and not getattr(indexer, "dry_run", False):
            for archive_path, current_keys in archive_member_keys.items():
                prefix = f"{archive_path}::"
                stale_keys = sorted(set(indexer.state_db.list_entries_by_prefix(prefix)) - set(current_keys))
                if not stale_keys:
                    continue
                self._logger.info("ZIP cleanup: %s — удаляю %d устаревших entries", archive_path, len(stale_keys))
                removed_keys: List[str] = []
                for key in stale_keys:
                    try:
                        indexer._delete_file_vectors(Path(key))
                    except Exception:
                        # Векторы не удалены — state оставляем, чтобы повторить позже.
                        continue
                    removed_keys.append(key)
                if removed_keys:
                    indexer.state_db.delete_entries(removed_keys)

        if only_paths:
            before = len(all_tasks)
            all_tasks = [item for item in all_tasks if _task_matches_only_paths(item, only_paths)]
            self._logger.info("Ограничение списка файлов: %d → %d по --only-paths-file", before, len(all_tasks))
            matched_only_paths = {
                key
                for item in all_tasks
                for key in _task_only_path_keys(item)
                if key in only_paths
            }
            unmatched_only_paths = sorted(only_paths - matched_only_paths)
            if unmatched_only_paths:
                self._logger.warning(
                    "--only-paths-file: %d путей не сопоставлены с текущим inventory; первые %d: %s",
                    len(unmatched_only_paths),
                    min(10, len(unmatched_only_paths)),
                    unmatched_only_paths[:10],
                )

        # small не читает тяжёлые файлы дважды: они сразу переходят в large.
        # large видит весь корпус, пропускает уже полные small-файлы и догружает partial.
        if stage == "small":
            scope_files = [
                item
                for item in all_tasks
                if (
                    str(item.get("archive_category") or "") == "small"
                    if item.get("archive_path")
                    else self._file_category(
                        Path(item["source_path"]),
                        indexer.small_office_mb,
                        indexer.small_pdf_mb,
                    ) == "small"
                )
            ]
            self._logger.info(
                "Этап 'small': %d небольших файлов из %d, лимит %d чанков/файл",
                len(scope_files),
                len(all_tasks),
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
        state_snapshot = (
            indexer.state_db.entries_snapshot()
            if hasattr(indexer, "state_db") and hasattr(indexer.state_db, "entries_snapshot")
            else {}
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
        self._stage_progress = stage_stats
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
                    if existing_status == "reindexing":
                        reason = "reindexing_interrupted"
                    elif existing_status == "error":
                        if hasattr(indexer, "state_db") and not indexer.state_db.is_failed_retry_due(file_key):
                            skipped += 1
                            continue
                        reason = "retry_error"
                    elif (
                        existing_status == "empty"
                        and stage in ("small", "large")
                        and hasattr(indexer, "state_db")
                        and not indexer.state_db.is_failed_retry_due(file_key)
                    ):
                        # Пустое извлечение с отложенным повтором (см. empty backoff)
                        skipped += 1
                        continue
                    elif (
                        stage in ("small", "large")
                        and bool(getattr(indexer, "skip_ocr", False))
                        and existing_stage in {"metadata", "empty"}
                        and str(existing.get("indexed_stage") or "") in {"small", "large"}
                        and is_deferred_ocr_candidate(existing_ext, existing_status)
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
        heartbeat_every_n = 25
        heartbeat_path = str(getattr(indexer, "heartbeat_path", "") or "")

        def _write_stage_heartbeat(status: str = HEARTBEAT_RUNNING) -> None:
            if not heartbeat_path:
                return
            write_heartbeat(
                heartbeat_path,
                stage=stage,
                processed=stage_stats["processed_files"],
                total=stage_stats["total_files"],
                run_id=str(getattr(indexer, "run_id", "") or ""),
                status=status,
                extra={
                    "error_files": stage_stats["error_files"],
                    "points_added": stage_stats["points_added"],
                },
            )

        def _maybe_heartbeat() -> None:
            if heartbeat_path and stage_stats["processed_files"] % heartbeat_every_n == 0:
                _write_stage_heartbeat()

        _write_stage_heartbeat()
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

        consecutive_flush_failures = [0]

        def _persist_pending_states() -> None:
            if hasattr(indexer, "state_db"):
                indexer.state_db.upsert_many(pending_states)
            else:
                for row in pending_states:
                    indexer._upsert_state_entry(row)

        def _mark_batch_failed(exc: Exception, points_count: int) -> None:
            """Qdrant не принял батч после всех ретраев: файлы батча → error с backoff.

            Прогон продолжается (обрыв соединения не должен ронять ночную индексацию);
            но если Qdrant не отвечает несколько батчей подряд — останавливаемся.
            """
            error_text = f"qdrant_upsert_failed: {exc}"
            failed_files = 0
            for row in pending_states:
                if str(row.get("status") or "") == "error":
                    continue  # уже учтён (например, qdrant_delete_failed)
                key = str(row.get("full_path") or "")
                next_retry_at = 0.0
                if key and hasattr(indexer, "state_db"):
                    try:
                        failed_row = indexer.state_db.record_failed_path(
                            key, fingerprint=str(row.get("fingerprint") or ""), error=error_text
                        )
                        next_retry_at = float((failed_row or {}).get("next_retry_at") or 0.0)
                    except Exception:
                        next_retry_at = 0.0
                row.update(
                    {
                        "stage": "error",
                        "status": "error",
                        "last_error": error_text,
                        "next_retry_at": next_retry_at,
                        "content_hash": "",
                        "indexed_chunks": 0,
                        "total_chunks": 0,
                    }
                )
                failed_files += 1
            stage_stats["error_files"] += failed_files
            consecutive_flush_failures[0] += 1
            self._logger.error(
                "Qdrant: батч из %d точек не записан после ретраев (%s) — %d файлов помечены "
                "error с повторной попыткой, прогон продолжается (%d/%d сбоев подряд)",
                points_count,
                exc,
                failed_files,
                consecutive_flush_failures[0],
                QDRANT_CONSECUTIVE_FLUSH_FAILURES_ABORT,
            )
            _persist_pending_states()
            pending_texts.clear()
            pending_payloads.clear()
            pending_states.clear()
            if consecutive_flush_failures[0] >= QDRANT_CONSECUTIVE_FLUSH_FAILURES_ABORT:
                raise RuntimeError(
                    f"Qdrant недоступен: {consecutive_flush_failures[0]} батчей подряд не записаны "
                    f"({exc}); файлы помечены error и будут повторены на следующем прогоне"
                ) from exc

        def flush() -> None:
            """
            Batch-encode накопленных текстов и запись в Qdrant.
            Разбивает большой список на куски по ENCODE_BATCH,
            чтобы один вызов encode() не блокировал главный поток надолго.
            """
            if not pending_texts:
                if pending_states:
                    _persist_pending_states()
                    pending_states.clear()
                return
            encoded_points: List[PointStruct] = []
            # Нарезаем encode на мини-батчи, затем пишем весь накопленный блок
            # одним запросом, чтобы не платить сетевой/fsync overhead каждые 256 точек.
            for i in range(0, len(pending_texts), ENCODE_BATCH):
                chunk_texts    = pending_texts[i : i + ENCODE_BATCH]
                chunk_payloads = pending_payloads[i : i + ENCODE_BATCH]
                vectors = _encode_with_transient_retry(
                    indexer.embedder,
                    prepare_passage_texts(
                        str(getattr(indexer, "embedding_model", "") or ""),
                        chunk_texts,
                    ),
                    initial_batch_size=ENCODE_BATCH,
                    logger=self._logger,
                )
                indexer._check_indexer_control(stage=stage, stage_stats=stage_stats)
                encoded_points.extend(
                    PointStruct(id=_point_id(p), vector=v.tolist(), payload=p)
                    for v, p in zip(vectors, chunk_payloads)
                )
            try:
                written = upsert_points(
                    indexer.qdrant,
                    collection_name=indexer.collection_name,
                    points=encoded_points,
                    timeout_sec=int(getattr(indexer, "qdrant_timeout_sec", 60) or 60),
                )
            except Exception as exc:
                _mark_batch_failed(exc, len(encoded_points))
                return
            consecutive_flush_failures[0] = 0
            indexer.point_count += written
            stage_stats["points_added"] += written
            self._logger.info(
                "Записан батч: %d точек (итого %d)", len(pending_texts), indexer.point_count
            )
            _persist_pending_states()
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

            existing_entry = state_snapshot.get(file_key)
            existing_status = str((existing_entry or {}).get("status") or (existing_entry or {}).get("stage") or "")
            if (
                existing_entry
                and (
                    existing_status == "error"
                    or (existing_status == "empty" and stage in ("small", "large"))
                )
                and hasattr(indexer, "state_db")
                and not indexer.state_db.is_failed_retry_due(file_key)
            ):
                # error — backoff после ошибки; empty — отложенный повтор пустого
                # извлечения (через 24 ч), а не на каждом прогоне.
                return {"skipped": True}

            # Stage-aware skip:
            #  - на этапе metadata пропускаем любой уже проиндексированный файл;
            #  - на этапах small/large пропускаем только те, что уже дошли до "content".
            if indexer._should_skip_for_stage(
                file_key,
                fingerprint,
                existing_entry=existing_entry,
                entry_loaded=True,
            ):
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
            unreadable_source = False

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
                _fn = None
                _doc_fn = indexer._extract_docx_document
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
                        ocr_context = getattr(indexer, "_ocr_context", None)
                        try:
                            if ocr_context is not None:
                                ocr_context.logical_path = file_key
                                ocr_context.logical_mtime = mtime
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
                        finally:
                            if ocr_context is not None:
                                for attr in ("logical_path", "logical_mtime"):
                                    try:
                                        delattr(ocr_context, attr)
                                    except AttributeError:
                                        pass

                    _t = _th.Thread(target=_reader, daemon=True)
                    _t.start()
                    reader_status = _wait_for_reader_thread(_t, timeout_sec=FILE_TIMEOUT)
                    if reader_status in {"timeout", "cancel"}:
                        # Поток завис (SMB stall) — освобождаем семафор только
                        # после его завершения через отдельный cleanup-поток.
                        if reader_status == "timeout":
                            self._logger.warning(
                                "ТАЙМАУТ SMB (>%dс): пропускаю %s — воркер остался в фоне",
                                FILE_TIMEOUT, relative_path.name,
                            )
                        def _cleanup(_t=_t, _sem=_reader_sem):
                            _t.join()        # ждём в фоне сколько потребуется
                            _sem.release()   # освобождаем слот
                        _th.Thread(target=_cleanup, daemon=True).start()
                        if reader_status == "cancel":
                            return {"skipped": True}
                        full_text = ""
                        failure_error = f"timeout>{FILE_TIMEOUT}s"
                    else:
                        _reader_sem.release()   # поток завершился штатно
                        if _buf[1] is not None:
                            self._logger.warning("Ошибка чтения %s: %s", relative_path.name, _buf[1])
                            full_text = ""
                            failure_error = str(_buf[1])
                            unreadable_source = is_unreadable_source_error(_buf[1])
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

            if document_ocr_error(extracted_doc):
                failure_error = f"embedded_ocr_failed: {document_ocr_error(extracted_doc)}"
            skip_ocr_active = stage in ("small", "large") and bool(getattr(indexer, "skip_ocr", False))
            deferred_ocr = (
                skip_ocr_active
                and (ext == ".pdf" or ext in self._image_extensions)
                and not failure_error
                and not full_text.strip()
            )
            # Office-контейнер с картинками, OCR которых пропущен (--no-ocr): текст
            # (если есть) индексируем, но файл помечаем deferred_ocr — как скан PDF,
            # чтобы OCR-прогон его дочитал.
            embedded_ocr_deferred = bool(
                skip_ocr_active
                and not failure_error
                and document_has_deferred_embedded_ocr(extracted_doc)
            )
            deferred_ocr = deferred_ocr or embedded_ocr_deferred
            preserve_existing_partial = bool(
                stage == "large"
                and existing_entry
                and str(existing_entry.get("stage") or "") == "partial"
                and int(existing_entry.get("indexed_chunks") or 0) > 0
                and not full_text.strip()
                and not deferred_ocr
            )
            if preserve_existing_partial and not failure_error:
                failure_error = "empty_full_extraction_after_partial"

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
            if stage == "metadata":
                doc_meta = {}
            elif item.get("archive_path") and item.get("archive_member"):
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
                    "modified": meta_payload["modified"],
                    "created": meta_payload["created"],
                    "size_mb": meta_payload["size_mb"],
                    "path": logical_path_text,
                    "full_path": file_key,
                    "chunk_index": idx,
                    "tags": tags,
                    "content_hash": content_hash,
                    "is_duplicate": bool(duplicate_of),
                    "duplicate_of": duplicate_of,
                    **doc_meta,
                    **base_provenance,
                    **indexer._spreadsheet_payload_fields(file_type),
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
                "was_indexed": existing_entry is not None,
                "existing_stage": str((existing_entry or {}).get("stage") or ""),
                "same_fingerprint": bool(
                    existing_entry
                    and str(existing_entry.get("fingerprint") or "") == fingerprint
                ),
                "had_failure": bool(
                    existing_entry
                    and str(existing_entry.get("status") or existing_entry.get("stage") or "") == "error"
                ),
                "meta_text": meta_text,
                "meta_payload": meta_payload,
                "chunks": chunks,
                "content_payloads": content_payloads,
                "has_content": bool(chunks),
                "source_has_content": bool(full_text.strip()),
                "append_only": append_only,
                "preserve_existing_partial": preserve_existing_partial,
                "existing_indexed_chunks": int((existing_entry or {}).get("indexed_chunks") or 0),
                "existing_total_chunks": int((existing_entry or {}).get("total_chunks") or 0),
                "indexed_chunks": (append_from_chunk + len(chunks)) if append_only else len(chunks),
                "total_chunks": total_chunks,
                "content_hash": content_hash,
                "error": failure_error,
                "unreadable_source": unreadable_source,
                "deferred_ocr": deferred_ocr,
                "skipped": False,
            }

        # ── основной pipeline ────────────────────────────────────────
        with ThreadPoolExecutor(max_workers=indexer.read_workers) as pool:
            # Не отправляем весь корпус в executor сразу: завершённый Future
            # удерживает результат extract_one вместе с полным текстом файла.
            # На большом каталоге это превращалось в многогигабайтную очередь,
            # пока главный поток был занят batch-encode.
            max_in_flight = max(1, int(indexer.read_workers) * 2)
            for future, source_item in tqdm(
                _bounded_executor_results(
                    pool,
                    extract_one,
                    scope_files,
                    max_in_flight=max_in_flight,
                ),
                total=len(scope_files),
                desc=f"Этап {stage}",
            ):
                try:
                    result = future.result()
                except Exception as exc:
                    fp = source_item.get("relative_path") or source_item.get("filepath")
                    self._logger.error("Ошибка обработки %s: %s", fp, exc, exc_info=True)
                    stage_stats["error_files"] += 1
                    stage_stats["processed_files"] += 1
                    _maybe_heartbeat()
                    indexer._check_indexer_control(stage=stage, stage_stats=stage_stats)
                    continue

                if result is None:
                    indexer._check_indexer_control(stage=stage, stage_stats=stage_stats)
                    continue  # файл не изменился
                if result.get("skipped"):
                    stage_stats["skipped_files"] += 1
                    stage_stats["processed_files"] += 1
                    _maybe_heartbeat()
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
                _maybe_heartbeat()
                indexer._check_indexer_control(stage=stage, stage_stats=stage_stats)

                if str(result.get("error") or "").startswith("embedded_ocr_failed:"):
                    stage_stats["error_files"] += 1
                    failed = indexer.state_db.record_failed_path(
                        str(result["file_key"]), fingerprint=str(result["fingerprint"]), error=result["error"],
                    )
                    pending_states.append({
                        **(state_snapshot.get(str(result["file_key"])) or {}),
                        "full_path": result["file_key"], "fingerprint": result["fingerprint"],
                        "mtime": result["mtime"], "stage": "error", "status": "error",
                        "last_error": result["error"], "next_retry_at": (failed or {}).get("next_retry_at", 0),
                    })
                    continue

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
                force_replace = bool(
                    str(result["meta_payload"].get("extension") or "").lower()
                    in set(getattr(indexer, "force_replace_extensions", set()) or set())
                )
                metadata_only_upgrade = bool(
                    result.get("same_fingerprint")
                    and str(result.get("existing_stage") or "") in {"metadata", "empty"}
                    and stage in {"small", "large"}
                    and not force_replace
                )
                if (
                    (result["was_indexed"] or force_replace)
                    and not result.get("append_only")
                    and not metadata_only_upgrade
                ):
                    # Окно между удалением старых векторов и flush() новых: синхронно
                    # помечаем файл как reindexing, чтобы при обрыве прогона он не
                    # считался готовым и был переиндексирован.
                    existing_row = state_snapshot.get(str(result["file_key"]))
                    if existing_row and hasattr(indexer, "state_db"):
                        indexer.state_db.upsert_many(
                            [{**existing_row, "status": "reindexing", "last_error": "", "next_retry_at": 0.0}]
                        )
                    try:
                        indexer._delete_file_vectors(Path(result["file_key"]))
                    except Exception as exc:
                        # Старые векторы не удалены: новые точки НЕ пишем (иначе смесь
                        # старых и новых чанков), файл помечаем как error с backoff.
                        delete_error = f"qdrant_delete_failed: {exc}"
                        self._logger.error(
                            "Файл %s: не удалось удалить старые векторы, пропускаю запись: %s",
                            Path(str(result["file_key"])).name,
                            exc,
                        )
                        stage_stats["error_files"] += 1
                        next_retry_at = 0.0
                        if hasattr(indexer, "state_db"):
                            failed_row = indexer.state_db.record_failed_path(
                                str(result["file_key"]),
                                fingerprint=str(result["fingerprint"]),
                                error=delete_error,
                            )
                            try:
                                next_retry_at = float(failed_row.get("next_retry_at") or 0.0)
                            except (TypeError, ValueError):
                                next_retry_at = 0.0
                        pending_states.append(
                            {
                                "full_path": result["file_key"],
                                "fingerprint": result["fingerprint"],
                                "mtime": result["mtime"],
                                "stage": "error",
                                "indexed_stage": stage,
                                "status": "error",
                                "last_error": delete_error,
                                "next_retry_at": next_retry_at,
                                "size_bytes": int(result.get("size_bytes") or 0),
                                "extension": str(result["meta_payload"].get("extension") or ""),
                                "content_hash": "",
                                "indexed_chunks": 0,
                                "total_chunks": 0,
                            }
                        )
                        continue
                if result["was_indexed"]:
                    stage_stats["updated_files"] += 1
                else:
                    stage_stats["added_files"] += 1

                # Добавить метаданные и контентные чанки в буфер
                if not result.get("append_only") and not metadata_only_upgrade:
                    pending_texts.append(result["meta_text"])
                    pending_payloads.append(result["meta_payload"])
                for cpayload in result["content_payloads"]:
                    pending_texts.append(str(cpayload.get("text") or ""))
                    pending_payloads.append(cpayload)
                indexed_chunks = int(result.get("indexed_chunks") or 0)
                total_chunks = int(result.get("total_chunks") or 0)
                if result.get("error"):
                    if result.get("unreadable_source"):
                        file_stage = "empty"
                        status = "unreadable"
                        last_error = str(result.get("error") or "unreadable_source")
                        next_retry_at = 0.0
                        if hasattr(indexer, "state_db"):
                            indexer.state_db.clear_failed_path(str(result["file_key"]))
                    elif result.get("preserve_existing_partial"):
                        file_stage = "partial"
                        indexed_chunks = int(result.get("existing_indexed_chunks") or 0)
                        total_chunks = max(
                            indexed_chunks + 1,
                            int(result.get("existing_total_chunks") or 0),
                        )
                    else:
                        file_stage = "error"
                    if not result.get("unreadable_source"):
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
                    result_ext = str(result["meta_payload"].get("extension") or "").lower()
                    if (
                        status == "empty"
                        and stage in ("small", "large")
                        and int(result.get("size_bytes") or 0) > 0
                        and result_ext in self._supported_extensions
                        and result_ext != ".pdf"
                        and result_ext not in self._image_extensions
                        and hasattr(indexer, "state_db")
                    ):
                        # Пустой ≠ сломанный: экстракторы возвращают "" и для пустого
                        # документа, и при проглоченном исключении. Не решаем навсегда —
                        # повторная попытка через 24 ч (далее backoff x2), а не на каждом прогоне.
                        last_error = "empty_extraction"
                        failed_row = indexer.state_db.record_failed_path(
                            str(result["file_key"]),
                            fingerprint=str(result["fingerprint"]),
                            error=last_error,
                            base_delay_seconds=EMPTY_RETRY_DELAY_SEC,
                            max_delay_seconds=EMPTY_RETRY_MAX_DELAY_SEC,
                        )
                        try:
                            next_retry_at = float(failed_row.get("next_retry_at") or 0.0)
                        except (TypeError, ValueError):
                            next_retry_at = 0.0
                if (
                    stage in ("small", "large")
                    and not result.get("source_has_content")
                    and not result.get("deferred_ocr")
                ):
                    if result.get("unreadable_source"):
                        self._logger.warning(
                            "Этап %s: файл %s поврежден или нечитаем, сохраняю status=unreadable "
                            "без автоматического повтора",
                            stage,
                            Path(str(result["file_key"])).name,
                        )
                    else:
                        self._logger.warning(
                            "Этап %s: файл %s без контента, сохраняю stage=%s "
                            "(будет повторная попытка)",
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
                        "indexed_chunks": indexed_chunks,
                        "total_chunks": total_chunks,
                    }
                )

                # Достигли порога — кодируем и пишем в Qdrant
                if len(pending_texts) >= WRITE_BATCH:
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
        #
        # --only-paths-file усекает all_tasks до явно перечисленных путей, поэтому
        # такой inventory тоже неполон: передать его в cleanup означало бы объявить
        # «удалёнными» все остальные записи state и стереть индекс целиком.
        if stage in ("metadata", "content"):
            if only_paths:
                self._logger.info(
                    "Cleanup «фантомов» пропущен на этапе '%s': список файлов ограничен "
                    "--only-paths-file (%d путей), inventory неполон",
                    stage,
                    len(only_paths),
                )
            else:
                inventory_keys = [str(item["state_key"]) for item in all_tasks]
                protected_keys = self._protected_inventory_keys(failed_inventory_roots)
                failed_ratio = len(failed_inventory_roots) / max(1, len(all_files))
                skip_ratio = self._cleanup_skip_failed_ratio()
                if failed_inventory_roots and failed_ratio > skip_ratio:
                    self._logger.error(
                        "Cleanup «фантомов» ПРОПУЩЕН на этапе '%s': не прочитано %d из %d файлов/архивов "
                        "(%.1f%% > %.0f%%, index_cleanup_skip_failed_ratio) — inventory ненадёжен, "
                        "удаление из индекса отменено",
                        stage,
                        len(failed_inventory_roots),
                        len(all_files),
                        failed_ratio * 100,
                        skip_ratio * 100,
                    )
                else:
                    if protected_keys:
                        self._logger.warning(
                            "Cleanup: %d записей state защищены от удаления — %d файлов/архивов "
                            "не удалось прочитать при сканировании",
                            len(protected_keys),
                            len(failed_inventory_roots),
                        )
                    indexer._run_deleted_files += indexer._cleanup_deleted_files(
                        inventory_keys + sorted(protected_keys)
                    )

        try:
            info = indexer.qdrant.get_collection(indexer.collection_name)
            self._logger.info("Коллекция '%s': %d точек", indexer.collection_name, info.points_count)
        except Exception as exc:
            # Информационный запрос: обрыв соединения здесь не должен ронять завершённый этап.
            self._logger.warning("Коллекция '%s': не удалось получить статистику: %s", indexer.collection_name, exc)
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
        _write_stage_heartbeat(HEARTBEAT_FINISHED)
        return stage_stats

    def _cleanup_skip_failed_ratio(self) -> float:
        """Порог доли нечитаемых файлов/архивов inventory (config index_cleanup_skip_failed_ratio)."""
        raw = getattr(self._indexer, "cleanup_skip_failed_ratio", FAILED_INVENTORY_CLEANUP_SKIP_RATIO)
        try:
            value = float(FAILED_INVENTORY_CLEANUP_SKIP_RATIO if raw is None else raw)
        except (TypeError, ValueError):
            return FAILED_INVENTORY_CLEANUP_SKIP_RATIO
        return max(0.0, min(1.0, value))

    def _protected_inventory_keys(self, failed_roots: set[str]) -> set[str]:
        """Ключи state, которые нельзя удалять: их источник не удалось прочитать."""
        indexer = self._indexer
        protected: set[str] = set()
        if not failed_roots or not hasattr(indexer, "state_db"):
            return protected
        for root in failed_roots:
            if root.endswith("::"):
                protected.update(indexer.state_db.list_entries_by_prefix(root))
            else:
                protected.add(root)
                protected.update(indexer.state_db.list_entries_by_prefix(f"{root}::"))
        return protected
