from __future__ import annotations

import logging
import threading
import time
import uuid
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple
from zipfile import ZipFile

from qdrant_client.models import PointStruct
from tqdm import tqdm

from ..extractors import extract_doc_meta
from .qdrant_writer import upsert_points


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
          stage="small"    — полное содержимое docx/xlsx/xls и мелких PDF;
          stage="large"    — полное содержимое крупных и сканированных PDF;
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

        def _zip_member_category(ext: str, size_bytes: int) -> str:
            if ext in (".txt", ".csv", ".rtf", ".pptx"):
                return "small"
            if ext in (".docx", ".xlsx", ".xls") and size_bytes < indexer.small_office_mb * 1_048_576:
                return "small"
            if ext == ".pdf" and size_bytes < indexer.small_pdf_mb * 1_048_576:
                return "small"
            return "large"

        def _zip_tasks(filepath: Path) -> List[Dict[str, Any]]:
            tasks: List[Dict[str, Any]] = []
            archive_fingerprint, archive_mtime = indexer._get_file_fingerprint(filepath)
            archive_rel = filepath.relative_to(indexer.catalog_path).as_posix()
            try:
                with ZipFile(filepath, "r") as zf:
                    for info in zf.infolist():
                        if info.is_dir():
                            continue
                        member = info.filename.replace("\\", "/").lstrip("/")
                        ext = Path(member).suffix.lower()
                        if ext not in self._supported_extensions or ext == ".zip":
                            continue
                        logical_path = Path(f"{archive_rel}/{member}")
                        if indexer._is_excluded_path(logical_path):
                            continue
                        try:
                            member_dt = datetime(*info.date_time).timestamp()
                        except Exception:
                            member_dt = archive_mtime
                        tasks.append(
                            {
                                "filepath": filepath,
                                "source_path": filepath,
                                "relative_path": logical_path,
                                "state_key": f"{filepath}::{member}",
                                "fingerprint": f"{archive_fingerprint}:{info.CRC}:{info.file_size}:{info.date_time}",
                                "mtime": float(member_dt),
                                "size_bytes": int(info.file_size),
                                "sort_mtime": float(archive_mtime),
                                "archive_path": filepath,
                                "archive_member": member,
                                "archive_category": _zip_member_category(ext, int(info.file_size)),
                            }
                        )
            except Exception as exc:
                self._logger.warning("ZIP %s не прочитан: %s", filepath, exc)
            return tasks

        all_tasks: List[Dict[str, Any]] = []
        for filepath in all_files:
            if filepath.suffix.lower() == ".zip":
                all_tasks.extend(_zip_tasks(filepath))
            else:
                all_tasks.append(_normal_task(filepath))

        # Партиция по этапам: metadata берёт всё, small/large — свою категорию.
        if stage == "small":
            scope_files = [
                item for item in all_tasks
                if (
                    item.get("archive_category")
                    or self._file_category(item["filepath"], indexer.small_office_mb, indexer.small_pdf_mb)
                ) == "small"
            ]
            self._logger.info("Отфильтровано для этапа 'small': %d файлов (docx/xlsx/xls + PDF < %g МБ)",
                        len(scope_files), indexer.small_pdf_mb)
        elif stage == "large":
            scope_files = [
                item for item in all_tasks
                if (
                    item.get("archive_category")
                    or self._file_category(item["filepath"], indexer.small_office_mb, indexer.small_pdf_mb)
                ) == "large"
            ]
            self._logger.info("Отфильтровано для этапа 'large': %d файлов (крупные Office + большие/сканированные PDF)",
                        len(scope_files))
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
        # (file_key, fingerprint, mtime, stage, size_bytes, extension)
        pending_states: List[Tuple[str, str, float, str, int, str]] = []

        def flush() -> None:
            """
            Batch-encode накопленных текстов и запись в Qdrant.
            Разбивает большой список на куски по ENCODE_BATCH,
            чтобы один вызов encode() не блокировал главный поток надолго.
            """
            if not pending_texts:
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
                    PointStruct(id=str(uuid.uuid4()), vector=v.tolist(), payload=p)
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
                indexer.state_db.upsert_many(
                    [
                        {
                            "full_path": file_key,
                            "fingerprint": fingerprint,
                            "mtime": mtime,
                            "stage": file_stage,
                            "size_bytes": size_bytes,
                            "extension": extension,
                        }
                        for file_key, fingerprint, mtime, file_stage, size_bytes, extension in pending_states
                    ]
                )
            else:
                for file_key, fingerprint, mtime, file_stage, size_bytes, extension in pending_states:
                    indexer._upsert_state_entry(
                        {
                            "full_path": file_key,
                            "fingerprint": fingerprint,
                            "mtime": mtime,
                            "stage": file_stage,
                            "size_bytes": size_bytes,
                            "extension": extension,
                        }
                    )
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
            file_type = ""

            _buf: list = [None, None]  # [result_text, exception]

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
            elif ext in (".xlsx", ".xls"):
                file_type = "xlsx"
                _fn = indexer._extract_spreadsheet
            elif ext == ".rtf":
                file_type = "rtf"
                _fn = indexer._extract_rtf
            elif ext == ".pptx":
                file_type = "pptx"
                _fn = indexer._extract_pptx
            elif ext == ".txt":
                file_type = "txt"
                _fn = indexer._extract_text
            elif ext == ".csv":
                file_type = "csv"
                _fn = indexer._extract_csv
            elif ext == ".pdf":
                file_type = "pdf"
                _fn = indexer._extract_pdf
            elif ext in self._image_extensions:
                if indexer.skip_ocr:
                    file_type = "image"
                    _fn = None  # OCR отключён — только метаданные
                else:
                    file_type = "image"
                    _fn = indexer._extract_image
            else:
                _fn = None

            if _fn is not None:
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
                else:
                    def _reader():
                        try:
                            if item.get("archive_path") and item.get("archive_member"):
                                with tempfile.TemporaryDirectory(prefix="rag_zip_") as tmp:
                                    temp_path = Path(tmp) / Path(str(item["archive_member"])).name
                                    with ZipFile(Path(item["archive_path"]), "r") as zf:
                                        temp_path.write_bytes(zf.read(str(item["archive_member"])))
                                    _buf[0] = _fn(temp_path)
                            else:
                                _buf[0] = _fn(source_path)
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
                    else:
                        _reader_sem.release()   # поток завершился штатно
                        if _buf[1] is not None:
                            self._logger.warning("Ошибка чтения %s: %s", relative_path.name, _buf[1])
                            full_text = ""
                        else:
                            full_text = _buf[0] or ""

            elapsed = time.monotonic() - t_start
            if elapsed >= 30:
                self._logger.warning(
                    "Долгое извлечение (%.0fс, %.1f МБ): %s",
                    elapsed, size_mb, relative_path.name,
                )

            chunks = indexer._chunk_text(full_text) if full_text.strip() else []
            if indexer.max_chunks_per_file and len(chunks) > indexer.max_chunks_per_file:
                self._logger.warning(
                    "Файл %s: %d чанков → обрезано до %d",
                    relative_path.name, len(chunks), indexer.max_chunks_per_file,
                )
                chunks = chunks[: indexer.max_chunks_per_file]

            # Генерируем теги для файла (по пути, содержимому, синонимам)
            tags = self._generate_tags(source_path, relative_path, full_text, getattr(indexer, "synonym_map", {}) or {})

            stat = source_path.stat()
            logical_path_text = relative_path.as_posix()
            if item.get("archive_path") and item.get("archive_member"):
                with tempfile.TemporaryDirectory(prefix="rag_zip_meta_") as tmp:
                    temp_path = Path(tmp) / Path(str(item["archive_member"])).name
                    with ZipFile(Path(item["archive_path"]), "r") as zf:
                        temp_path.write_bytes(zf.read(str(item["archive_member"])))
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
                "text": meta_text,
                "filename": relative_path.name,
                "extension": ext,
                "size_mb": round(size_bytes / (1024 * 1024), 2),
                "modified": datetime.fromtimestamp(mtime).isoformat(),
                "created": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                "path": logical_path_text,
                "full_path": file_key,
                "tags": tags,
                **doc_meta,
            }
            if item.get("archive_path") and item.get("archive_member"):
                meta_payload.update(
                    {
                        "archive_path": str(item["archive_path"]),
                        "archive_member": str(item["archive_member"]),
                    }
                )
            base_provenance = indexer._base_provenance(
                filepath=Path(file_key),
                relative_path=relative_path,
                state_key=file_key,
                payload_extra=None,
            )
            meta_payload.update(base_provenance)
            doc_id = str(base_provenance["doc_id"])
            content_payloads = [
                {
                    "type": f"{file_type}_content",
                    "text": chunk,
                    "filename": relative_path.name,
                    "extension": ext,
                    "path": logical_path_text,
                    "full_path": file_key,
                    "chunk_index": idx,
                    "tags": tags,
                    **doc_meta,
                    **base_provenance,
                    **indexer._chunk_provenance(chunk=chunk, chunk_index=idx, doc_id=doc_id),
                }
                for idx, chunk in enumerate(chunks)
            ]
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
                    continue

                if result is None:
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
                    continue

                stage_stats["processed_files"] += 1

                # Если файл уже был в индексе — удалить старые векторы
                # (нужно и при изменении файла, и при апгрейде metadata→content)
                if result["was_indexed"]:
                    indexer._delete_file_vectors(Path(result["file_key"]))
                    stage_stats["updated_files"] += 1
                else:
                    stage_stats["added_files"] += 1

                # Добавить метаданные и контентные чанки в буфер
                pending_texts.append(result["meta_text"])
                pending_payloads.append(result["meta_payload"])
                for chunk, cpayload in zip(result["chunks"], result["content_payloads"]):
                    pending_texts.append(chunk)
                    pending_payloads.append(cpayload)
                file_stage = "metadata" if stage == "metadata" else (
                    "content" if result.get("has_content") else "empty"
                )
                if stage in ("small", "large") and not result.get("has_content"):
                    self._logger.warning(
                        "Этап %s: файл %s без контента, сохраняю stage=empty (будет повторная попытка)",
                        stage,
                        Path(str(result["file_key"])).name,
                    )
                pending_states.append(
                    (
                        result["file_key"],
                        result["fingerprint"],
                        result["mtime"],
                        file_stage,
                        int(result.get("size_bytes") or 0),
                        str(result["meta_payload"].get("extension") or ""),
                    )
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
