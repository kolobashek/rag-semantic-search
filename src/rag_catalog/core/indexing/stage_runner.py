from __future__ import annotations

import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

from qdrant_client.models import PointStruct
from tqdm import tqdm

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
        object.__setattr__(self, "_indexer", indexer)
        object.__setattr__(self, "_stages", tuple(stages))
        object.__setattr__(self, "_supported_extensions", set(supported_extensions))
        object.__setattr__(self, "_image_extensions", set(image_extensions))
        object.__setattr__(self, "_file_category", file_category)
        object.__setattr__(self, "_generate_tags", generate_tags)
        object.__setattr__(self, "_logger", logger)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._indexer, name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            object.__setattr__(self, name, value)
        else:
            setattr(self._indexer, name, value)

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
        self.current_stage = stage

        ENCODE_BATCH = 256  # сколько чанков накапливать перед одним вызовом encode()
                            # 256 оптимально для CPU (OpenBLAS матричные операции)

        # Семафор ограничивает число одновременно «зависших» daemon-потоков.
        # При массовых SMB-таймаутах без ограничения они накапливаются до OOM.
        # Лимит = 2 * read_workers: за один проход воркеров может зависнуть
        # не более read_workers файлов, запас ×2 на перекрытие таймаутов.
        _reader_sem = threading.Semaphore(self.read_workers * 2)

        self._logger.info(
            "════════ Этап '%s' (pipeline, workers=%d): %s ════════",
            stage, self.read_workers, self.catalog_path,
        )

        all_files = [
            f
            for f in self.catalog_path.rglob("*")
            if f.is_file()
            and f.suffix.lower() in self._supported_extensions
            and not f.name.startswith("~$")  # пропускать временные файлы Office
        ]
        self._logger.info("Найдено файлов на диске: %d (DOCX/XLSX/XLS/PDF)", len(all_files))

        # Партиция по этапам: metadata берёт всё, small/large — свою категорию.
        if stage == "small":
            scope_files = [
                f for f in all_files
                if self._file_category(f, self.small_office_mb, self.small_pdf_mb) == "small"
            ]
            self._logger.info("Отфильтровано для этапа 'small': %d файлов (docx/xlsx/xls + PDF < %g МБ)",
                        len(scope_files), self.small_pdf_mb)
        elif stage == "large":
            scope_files = [
                f for f in all_files
                if self._file_category(f, self.small_office_mb, self.small_pdf_mb) == "large"
            ]
            self._logger.info("Отфильтровано для этапа 'large': %d файлов (крупные Office + большие/сканированные PDF)",
                        len(scope_files))
        else:
            # metadata или legacy "content" — работаем со всем
            scope_files = all_files

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
        if self.run_id:
            self.telemetry.start_stage(
                run_id=self.run_id,
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
                vectors = self.embedder.encode(
                    chunk_texts, normalize_embeddings=True,
                    batch_size=256, show_progress_bar=False,
                )
                points = [
                    PointStruct(id=str(uuid.uuid4()), vector=v.tolist(), payload=p)
                    for v, p in zip(vectors, chunk_payloads)
                ]
                written = upsert_points(self.qdrant, collection_name=self.collection_name, points=points)
                self.point_count += written
                stage_stats["points_added"] += written
            self._logger.info(
                "Записан батч: %d точек (итого %d)", len(pending_texts), self.point_count
            )
            if hasattr(self, "state_db"):
                self.state_db.upsert_many(
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
                    self._upsert_state_entry(
                        {
                            "full_path": file_key,
                            "fingerprint": fingerprint,
                            "mtime": mtime,
                            "stage": file_stage,
                            "size_bytes": size_bytes,
                            "extension": extension,
                        }
                    )
            self._save_state()
            pending_texts.clear()
            pending_payloads.clear()
            pending_states.clear()

        # ── I/O-worker: читает один файл, возвращает тексты+payload ─
        def extract_one(filepath: Path):
            """
            Выполняется в потоке-воркере.
            Не кодирует векторы (encode — в главном потоке).
            Возвращает None если файл не изменился.
            """
            relative_path = filepath.relative_to(self.catalog_path)
            fingerprint, mtime = self._get_file_fingerprint(filepath)
            file_key = str(filepath)

            # Stage-aware skip:
            #  - на этапе metadata пропускаем любой уже проиндексированный файл;
            #  - на этапах small/large пропускаем только те, что уже дошли до "content".
            if self._should_skip_for_stage(file_key, fingerprint):
                return {"skipped": True}

            ext = filepath.suffix.lower()
            size_mb = round(filepath.stat().st_size / 1_048_576, 1)

            # Таймаут на извлечение: если воркер завис на сетевом I/O (SMB stall) —
            # пропускаем файл через 5 минут и продолжаем индексирование.
            # Используем daemon-поток: он умрёт сам когда процесс завершится,
            # даже если заблокирован в ядре на SMB read().
            FILE_TIMEOUT = 45  # секунд (быстро бросаем зависшие SMB-файлы)

            t_start = time.monotonic()
            full_text = ""
            file_type = ""

            _buf: list = [None, None]  # [result_text, exception]

            # Режим «только метадата»: либо текущий этап = metadata,
            # либо расширение явно в metadata_only_extensions (legacy флаг).
            # Содержимое не читается — файл попадает в индекс по имени/пути/размеру.
            if self.current_stage == "metadata" or ext in self.metadata_only_extensions:
                file_type = ext.lstrip(".") or "file"
                _fn = None
            elif ext == ".docx":
                file_type = "docx"
                _fn = self._extract_docx
            elif ext in (".xlsx", ".xls"):
                file_type = "xlsx"
                _fn = self._extract_spreadsheet
            elif ext == ".pdf":
                file_type = "pdf"
                _fn = self._extract_pdf
            elif ext in self._image_extensions:
                if self.skip_ocr:
                    file_type = "image"
                    _fn = None  # OCR отключён — только метаданные
                else:
                    file_type = "image"
                    _fn = self._extract_image
            else:
                _fn = None

            if _fn is not None:
                # Логируем тяжёлые файлы заранее — только для тех, что реально читаем
                if size_mb >= 5:
                    self._logger.info("Читаю крупный файл (%.1f МБ): %s", size_mb, filepath.name)

                import threading as _th

                # Проверяем семафор без блокировки: если лимит зависших потоков
                # исчерпан — пропускаем файл, не создавая новый поток.
                if not _reader_sem.acquire(blocking=False):
                    self._logger.warning(
                        "Лимит daemon-потоков исчерпан (%d): пропускаю %s",
                        self.read_workers * 2, filepath.name,
                    )
                    full_text = ""
                else:
                    def _reader():
                        try:
                            _buf[0] = _fn(filepath)
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
                            FILE_TIMEOUT, filepath.name,
                        )
                        def _cleanup(_t=_t, _sem=_reader_sem):
                            _t.join()        # ждём в фоне сколько потребуется
                            _sem.release()   # освобождаем слот
                        _th.Thread(target=_cleanup, daemon=True).start()
                        full_text = ""
                    else:
                        _reader_sem.release()   # поток завершился штатно
                        if _buf[1] is not None:
                            self._logger.warning("Ошибка чтения %s: %s", filepath.name, _buf[1])
                            full_text = ""
                        else:
                            full_text = _buf[0] or ""

            elapsed = time.monotonic() - t_start
            if elapsed >= 30:
                self._logger.warning(
                    "Долгое извлечение (%.0fс, %.1f МБ): %s",
                    elapsed, size_mb, filepath.name,
                )

            chunks = self._chunk_text(full_text) if full_text.strip() else []
            if self.max_chunks_per_file and len(chunks) > self.max_chunks_per_file:
                self._logger.warning(
                    "Файл %s: %d чанков → обрезано до %d",
                    filepath.name, len(chunks), self.max_chunks_per_file,
                )
                chunks = chunks[: self.max_chunks_per_file]

            # Генерируем теги для файла (по пути, содержимому, синонимам)
            tags = self._generate_tags(filepath, relative_path, full_text, getattr(self, "synonym_map", {}) or {})

            stat = filepath.stat()
            meta_text = (
                f"Файл: {filepath.name} | Путь: {relative_path}"
                f" | Расширение: {filepath.suffix}"
            )
            if tags:
                meta_text += f" | Теги: {', '.join(tags[:30])}"
            meta_payload: Dict[str, Any] = {
                "type": "file_metadata",
                "text": meta_text,
                "filename": filepath.name,
                "extension": ext,
                "size_mb": round(stat.st_size / (1024 * 1024), 2),
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "path": str(relative_path),
                "full_path": str(filepath),
                "tags": tags,
            }
            base_provenance = self._base_provenance(
                filepath=filepath,
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
                    "filename": filepath.name,
                    "extension": ext,
                    "path": str(relative_path),
                    "full_path": str(filepath),
                    "chunk_index": idx,
                    "tags": tags,
                    **base_provenance,
                    **self._chunk_provenance(chunk=chunk, chunk_index=idx, doc_id=doc_id),
                }
                for idx, chunk in enumerate(chunks)
            ]
            return {
                "filepath": filepath,
                "file_key": file_key,
                "fingerprint": fingerprint,
                "mtime": mtime,
                "size_bytes": int(stat.st_size),
                "was_indexed": self._get_state_entry(file_key) is not None,
                "meta_text": meta_text,
                "meta_payload": meta_payload,
                "chunks": chunks,
                "content_payloads": content_payloads,
                "has_content": bool(chunks),
                "skipped": False,
            }

        # ── основной pipeline ────────────────────────────────────────
        with ThreadPoolExecutor(max_workers=self.read_workers) as pool:
            futures = {pool.submit(extract_one, f): f for f in scope_files}
            for future in tqdm(as_completed(futures), total=len(scope_files),
                                desc=f"Этап {stage}"):
                try:
                    result = future.result()
                except Exception as exc:
                    fp = futures[future]
                    self._logger.error("Ошибка обработки %s: %s", fp, exc, exc_info=True)
                    stage_stats["error_files"] += 1
                    stage_stats["processed_files"] += 1
                    continue

                if result is None:
                    continue  # файл не изменился
                if result.get("skipped"):
                    stage_stats["skipped_files"] += 1
                    stage_stats["processed_files"] += 1
                    if self.run_id and (
                        stage_stats["processed_files"] % telemetry_push_every_n == 0
                        or (time.monotonic() - last_telemetry_push) >= telemetry_push_interval_sec
                    ):
                        self.telemetry.update_stage(
                            run_id=self.run_id,
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
                    self._delete_file_vectors(result["filepath"])
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
                    "content" if result.get("has_content") else "metadata"
                )
                if stage in ("small", "large") and file_stage == "metadata":
                    self._logger.warning(
                        "Этап %s: файл %s без контента, оставляю stage=metadata",
                        stage,
                        result["filepath"].name,
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

                if self.run_id and (
                    stage_stats["processed_files"] % telemetry_push_every_n == 0
                    or (time.monotonic() - last_telemetry_push) >= telemetry_push_interval_sec
                ):
                    self.telemetry.update_stage(
                        run_id=self.run_id,
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
                    stage, self.point_count)

        # Чистим «фантомы» только когда имеем полный список всех файлов на диске
        # (т.е. на этапах metadata и content). На small/large мы видим только
        # часть файлов и не должны по этому основанию удалять других.
        if stage in ("metadata", "content"):
            self._run_deleted_files += self._cleanup_deleted_files(all_files)

        info = self.qdrant.get_collection(self.collection_name)
        self._logger.info("Коллекция '%s': %d точек", self.collection_name, info.points_count)
        if self.run_id:
            self.telemetry.finish_stage(
                run_id=self.run_id,
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

