"""
ocr_pdfs.py — OCR-проход по сканированным PDF в RAG-каталоге.

Алгоритм:
  1. Подключается к Qdrant и находит PDF-файлы с пустым/коротким текстом
     (индексированные ранее с флагом --no-ocr).
  2. Удаляет найденные файлы из index_state.db (сбрасывает кэш),
     чтобы index_rag.py переиндексировал их заново.
  3. Запускает index_rag.py без --no-ocr — OCR применяется автоматически
     для файлов с пустым текстовым слоем.

Требования:
    pip install pytesseract pdf2image pymupdf
    Tesseract OCR: https://github.com/UB-Mannheim/tesseract/wiki
    Poppler (для pdf2image): https://github.com/oschwartz10612/poppler-windows

Вариант без системного PATH:
    tools/tesseract/tesseract.exe
    tools/poppler/Library/bin

Запуск:
    python ocr_pdfs.py
    python ocr_pdfs.py --url http://localhost:6333 --min-text-len 100
    python ocr_pdfs.py --dry-run   # показать файлы без запуска OCR
"""

import argparse
import logging
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from ._platform_compat import apply_windows_platform_workarounds

apply_windows_platform_workarounds()

from .embedding_collections import resolve_embedding_collection_name
from .index_state_db import IndexStateDB
from .log_history import install_env_log_handler
from .rag_core import load_config
from .telemetry_db import TelemetryDB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
install_env_log_handler()
logger = logging.getLogger(__name__)

# ──────────────────────────── константы ─────────────────────────────────────

# PDF с текстом короче этого порога считается сканом (нет полезного текста)
DEFAULT_MIN_TEXT_LEN = 50


def _windows_detached_creationflags() -> int:
    flags = 0
    for name in ("CREATE_NO_WINDOW", "DETACHED_PROCESS", "CREATE_NEW_PROCESS_GROUP", "CREATE_BREAKAWAY_FROM_JOB"):
        flags |= int(getattr(subprocess, name, 0) or 0)
    return flags


def _is_process_alive(pid: int) -> bool:
    if int(pid or 0) <= 0:
        return False
    try:
        os.kill(int(pid), 0)
    except PermissionError:
        return True
    except ProcessLookupError:
        return False
    except OSError:
        return False
    return True


def _effective_workers(requested: int) -> int:
    """workers<=0 трактуем как auto-подбор."""
    if int(requested or 0) > 0:
        return max(1, min(32, int(requested)))
    cpu = max(1, int(os.cpu_count() or 1))
    return max(1, min(4, max(1, cpu // 2)))


# ─────────────────────────── Qdrant helpers ──────────────────────────────────

def find_state_db_ocr_candidates(state_dir: Path, *, small_pdf_mb: float = 2.0) -> List[str]:
    """Return large PDF paths that state DB still has without indexed content."""
    db_path = Path(state_dir) / "index_state.db"
    if not db_path.exists():
        return []
    min_size = max(0, int(float(small_pdf_mb or 2.0) * 1_048_576))
    try:
        with sqlite3.connect(str(db_path), timeout=30.0) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT full_path
                FROM state_entries
                WHERE lower(extension)=?
                  AND COALESCE(size_bytes, 0) >= ?
                  AND (
                        stage != 'content'
                     OR status IN ('empty', 'error')
                     OR indexed_stage IN ('', 'metadata', 'small')
                  )
                ORDER BY size_bytes DESC, updated_at DESC, full_path
                """,
                (".pdf", min_size),
            ).fetchall()
    except sqlite3.Error as exc:
        logger.warning("Не удалось прочитать OCR-кандидатов из state DB: %s", exc)
        return []
    paths: List[str] = []
    seen: set[str] = set()
    for row in rows:
        path = str(row["full_path"] or "").strip()
        if path and path not in seen:
            seen.add(path)
            paths.append(path)
    return paths


def _payload_schema_field_names(collection_info: Any) -> set[str]:
    schema = getattr(collection_info, "payload_schema", None) or {}
    if isinstance(schema, dict):
        return {str(key) for key in schema.keys()}
    try:
        return {str(key) for key in schema}
    except Exception:
        return set()


def ensure_ocr_payload_indexes(client: Any, collection: str, *, collection_info: Any = None, timeout_sec: int = 300) -> None:
    """Ensure Qdrant can filter OCR candidate scans without full collection walks."""
    try:
        from qdrant_client.models import PayloadSchemaType
    except ImportError:
        return

    try:
        info = collection_info if collection_info is not None else client.get_collection(collection)
        indexed_fields = _payload_schema_field_names(info)
    except Exception as exc:
        logger.warning("Не удалось проверить payload-index Qdrant для OCR: %s", exc)
        return

    queued_fields: set[str] = set()
    for field_name in ("type", "extension"):
        if field_name in indexed_fields:
            continue
        try:
            logger.info("Ставлю в очередь payload-index Qdrant для OCR: %s=keyword", field_name)
            client.create_payload_index(
                collection_name=collection,
                field_name=field_name,
                field_schema=PayloadSchemaType.KEYWORD,
                wait=False,
                timeout=min(60, max(5, int(timeout_sec or 300))),
            )
            queued_fields.add(field_name)
        except Exception as exc:
            logger.warning(
                "Не удалось создать payload-index Qdrant для поля %s: %s. OCR-поиск продолжится медленным scan.",
                field_name,
                exc,
            )

    if not queued_fields:
        return

    deadline = time.monotonic() + max(300, int(timeout_sec or 300), 1800)
    pending = set(queued_fields)
    while pending and time.monotonic() < deadline:
        time.sleep(5)
        try:
            indexed_fields = _payload_schema_field_names(client.get_collection(collection))
        except Exception as exc:
            logger.debug("Ожидание payload-index Qdrant для OCR: %s", exc)
            continue
        pending = {field for field in queued_fields if field not in indexed_fields}

    if pending:
        logger.warning(
            "Payload-index Qdrant для OCR ещё не готов: %s. OCR-поиск продолжится, но может быть медленным.",
            ", ".join(sorted(pending)),
        )
    else:
        logger.info("Payload-index Qdrant для OCR готов: %s", ", ".join(sorted(queued_fields)))


def find_scanned_pdfs(
    qdrant_url: str,
    collection: str,
    min_text_len: int,
    scroll_limit: int = 1000,
    qdrant_timeout_sec: int = 60,
    scroll_retries: int = 4,
) -> List[str]:
    """
    Найти пути PDF-файлов без полезного текстового содержимого в Qdrant.

    Алгоритм:
      1. Собираем все full_path из записей file_metadata с extension=.pdf.
      2. Собираем все full_path из записей pdf_content (файлы с реальным текстом).
         Дополнительно проверяем длину текста в контентных чанках — если суммарно
         мало символов, файл также считается сканом.
      3. PDF без ни одного контентного чанка (или с суммарным текстом < min_text_len)
         — кандидаты на OCR.

    ВАЖНО: поле "text" в записях file_metadata содержит строку вида
    "Файл: X | Путь: Y | Расширение: .pdf" — оно всегда короткое и НЕ отражает
    наличие текстового слоя. Для определения скана используем только pdf_content.

    Возвращает список full_path файлов для OCR.
    """
    try:
        from qdrant_client import QdrantClient
        from qdrant_client.models import FieldCondition, Filter, MatchValue
    except ImportError:
        logger.error("qdrant-client не установлен. pip install qdrant-client")
        return []

    try:
        if qdrant_url:
            client = QdrantClient(url=qdrant_url, timeout=max(5, int(qdrant_timeout_sec or 60)))
        else:
            from .rag_core import load_config as _lc
            cfg = _lc()
            client = QdrantClient(
                path=str(cfg["qdrant_db_path"]),
                timeout=max(5, int(qdrant_timeout_sec or 60)),
            )
        collection_info = client.get_collection(collection)
        ensure_ocr_payload_indexes(
            client,
            collection,
            collection_info=collection_info,
            timeout_sec=max(5, int(qdrant_timeout_sec or 60)),
        )
    except Exception as exc:
        logger.error("Не удалось подключиться к Qdrant: %s", exc)
        return []

    def _scroll_all(flt: "Filter") -> List[Dict[str, Any]]:
        """Прокрутить всю коллекцию с заданным фильтром."""
        results: List[Dict[str, Any]] = []
        offset: Optional[Any] = None
        while True:
            for attempt in range(max(1, int(scroll_retries or 1))):
                try:
                    records, offset = client.scroll(
                        collection_name=collection,
                        scroll_filter=flt,
                        limit=scroll_limit,
                        offset=offset,
                        with_payload=True,
                        with_vectors=False,
                        timeout=max(5, int(qdrant_timeout_sec or 60)),
                    )
                    break
                except Exception as exc:
                    if attempt >= max(1, int(scroll_retries or 1)) - 1:
                        logger.error("Ошибка прокрутки коллекции: %s", exc)
                        return results
                    backoff_s = min(8, 2 ** attempt)
                    logger.warning(
                        "Прокрутка коллекции временно не удалась (%s). Повтор через %ss (%d/%d)…",
                        exc,
                        backoff_s,
                        attempt + 1,
                        max(1, int(scroll_retries or 1)),
                    )
                    time.sleep(backoff_s)
            results.extend(records)
            if not offset:
                break
        return results

    # ── Шаг 1: все PDF файлы по метаданным ─────────────────────────────
    logger.info("Шаг 1/2: Сбор PDF-метаданных из коллекции '%s'…", collection)
    meta_records = _scroll_all(Filter(must=[
        FieldCondition(key="type",      match=MatchValue(value="file_metadata")),
        FieldCondition(key="extension", match=MatchValue(value=".pdf")),
    ]))
    all_pdf_paths: Dict[str, str] = {}  # full_path → full_path (дедупликация)
    for rec in meta_records:
        p = rec.payload or {}
        fp = p.get("full_path") or p.get("path") or ""
        if fp:
            all_pdf_paths[fp] = fp

    logger.info("  Всего PDF в индексе: %d", len(all_pdf_paths))

    if not all_pdf_paths:
        return []

    # ── Шаг 2: PDF с реальным текстовым содержимым ──────────────────────
    logger.info("Шаг 2/2: Сбор текстовых чанков pdf_content…")
    content_records = _scroll_all(Filter(must=[
        FieldCondition(key="type", match=MatchValue(value="pdf_content")),
    ]))

    # Суммируем длину текста по каждому full_path
    content_text_len: Dict[str, int] = {}
    for rec in content_records:
        p = rec.payload or {}
        fp = p.get("full_path") or p.get("path") or ""
        if not fp:
            continue
        text = p.get("text", "") or ""
        content_text_len[fp] = content_text_len.get(fp, 0) + len(text.strip())

    # ── Шаг 3: PDF без контента или с слишком коротким текстом ──────────
    scanned: List[str] = []
    for fp in all_pdf_paths:
        total_len = content_text_len.get(fp, 0)
        if total_len < min_text_len:
            scanned.append(fp)

    logger.info(
        "Найдено сканированных PDF (суммарный текст < %d симв.): %d",
        min_text_len, len(scanned),
    )
    return scanned


# ─────────────────────────── state helpers ───────────────────────────────────

def remove_from_state_db(state_db_path: Path, file_paths: List[str]) -> int:
    """Удалить записи указанных файлов из SQLite state БД."""
    if not state_db_path.exists():
        raise FileNotFoundError(
            f"SQLite state БД не найдена: {state_db_path}. "
            "Сначала запустите index_rag для инициализации и миграции state."
        )
    state_db = IndexStateDB(str(state_db_path))
    removed = state_db.delete_entries(file_paths)
    logger.info("Удалено записей из state БД: %d", removed)
    return removed


# ─────────────────────────── main ────────────────────────────────────────────

def main() -> int:
    cfg = load_config()

    parser = argparse.ArgumentParser(
        description="OCR-проход по сканированным PDF в RAG-каталоге",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Примеры:\n"
            "  python ocr_pdfs.py\n"
            "  python ocr_pdfs.py --url http://localhost:6333\n"
            "  python ocr_pdfs.py --min-text-len 200 --dry-run\n"
        ),
    )
    parser.add_argument(
        "--url",
        default=cfg.get("qdrant_url", ""),
        dest="qdrant_url",
        help="URL Qdrant-сервера (по умолчанию из config.json)",
    )
    parser.add_argument(
        "--collection",
        default=cfg["collection_name"],
        help="Имя коллекции Qdrant",
    )
    parser.add_argument(
        "--state",
        default=cfg["qdrant_db_path"],
        dest="state_dir",
        help="Папка с index_state.db",
    )
    parser.add_argument(
        "--min-text-len",
        type=int,
        default=DEFAULT_MIN_TEXT_LEN,
        help=f"Минимальная длина текста (по умолчанию {DEFAULT_MIN_TEXT_LEN}); "
             "PDF с текстом короче считается сканом",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Показать список файлов для OCR, не запускать индексатор",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=int(cfg.get("index_read_workers", 4)),
        help="Количество рабочих потоков для индексатора (по умолчанию index_read_workers из config.json)",
    )
    parser.add_argument(
        "--scroll-limit",
        type=int,
        default=int(cfg.get("qdrant_scroll_limit", 256) or 256),
        help="Размер страницы при scroll в Qdrant (по умолчанию qdrant_scroll_limit из config.json)",
    )
    parser.add_argument(
        "--qdrant-timeout",
        type=int,
        default=int(cfg.get("qdrant_timeout_sec", 60) or 60),
        help="Таймаут запросов к Qdrant в секундах (по умолчанию qdrant_timeout_sec из config.json)",
    )
    parser.add_argument(
        "--scroll-retries",
        type=int,
        default=4,
        help="Число повторов scroll-запросов к Qdrant при временных ошибках/таймаутах",
    )
    parser.add_argument(
        "--ocr-engine",
        default=str(cfg.get("ocr_engine") or "tesseract"),
        dest="ocr_engine",
        choices=("tesseract", "rapidocr"),
        help="OCR движок: tesseract (CPU, по умолчанию) или rapidocr (GPU/DirectML)",
    )
    args = parser.parse_args()
    args.collection = resolve_embedding_collection_name(
        args.collection,
        str(cfg.get("embedding_model") or ""),
        enabled=bool(cfg.get("embedding_collection_versioning", False)),
        suffix=str(cfg.get("embedding_collection_suffix") or ""),
    )
    workers_effective = _effective_workers(int(args.workers or 0))

    # ── Инициализация телеметрии ─────────────────────────────────────────────
    telemetry_path = (cfg.get("telemetry_db_path") or "").strip()
    if not telemetry_path:
        telemetry_path = str(Path(args.state_dir) / "rag_telemetry.db")
    telemetry = TelemetryDB(telemetry_path)

    # Не запускаем OCR-проход поверх уже активной индексации, т.к. OCR внутри
    # сам запускает index_rag (stage=large).
    active_index = telemetry.get_active_index_run() if hasattr(telemetry, "get_active_index_run") else None
    if active_index:
        live_pid = int(active_index.get("worker_pid") or 0)
        if _is_process_alive(live_pid):
            logger.warning(
                "Найдена активная индексация (PID %s). OCR-проход пропущен, чтобы не запускать параллельный index_rag.",
                live_pid,
            )
            return 2

    # ── Создать запись OCR-прохода до долгого scan Qdrant ────────────────────
    ocr_start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    ocr_run_id = telemetry.start_ocr_run(
        collection_name=args.collection,
        found_scanned=0,
        note=f"searching_scanned_pdfs min_text_len={args.min_text_len}",
        worker_pid=os.getpid(),
    )
    logger.info("OCR run_id: %s", ocr_run_id)

    # ── 1. Найти кандидаты на OCR ────────────────────────────────────────────
    scanned = find_state_db_ocr_candidates(
        Path(args.state_dir),
        small_pdf_mb=float(cfg.get("small_pdf_mb") or 2.0),
    )
    if scanned:
        logger.info(
            "Быстрый список OCR-кандидатов из state DB (PDF >= %g МБ, без content): %d",
            float(cfg.get("small_pdf_mb") or 2.0),
            len(scanned),
        )
    else:
        scanned = find_scanned_pdfs(
            qdrant_url=args.qdrant_url,
            collection=args.collection,
            min_text_len=args.min_text_len,
            scroll_limit=max(50, int(args.scroll_limit or 256)),
            qdrant_timeout_sec=max(5, int(args.qdrant_timeout or 60)),
            scroll_retries=max(1, int(args.scroll_retries or 1)),
        )

    if not scanned:
        logger.info("Сканированные PDF не найдены — OCR не требуется.")
        telemetry.finish_ocr_run(
            ocr_run_id=ocr_run_id,
            status="completed",
            processed_pdfs=0,
            note="no_scanned_pdfs",
        )
        return 0

    telemetry.update_ocr_progress(
        ocr_run_id=ocr_run_id,
        found_scanned=len(scanned),
        note=f"min_text_len={args.min_text_len}",
    )

    logger.info("")
    logger.info("Файлы для OCR (%d):", len(scanned))
    for fp in scanned[:20]:
        logger.info("  %s", fp)
    if len(scanned) > 20:
        logger.info("  … и ещё %d файлов", len(scanned) - 20)

    if args.dry_run:
        logger.info("--dry-run: индексатор не запущен.")
        telemetry.finish_ocr_run(
            ocr_run_id=ocr_run_id,
            status="completed",
            processed_pdfs=0,
            note=f"dry_run found_scanned={len(scanned)}",
        )
        return 0

    # ── 2. Убрать их из state БД, чтобы индексатор переобработал ─────────────
    state_db_path = Path(args.state_dir) / "index_state.db"
    try:
        removed = remove_from_state_db(state_db_path, scanned)
    except FileNotFoundError as exc:
        logger.error("%s", exc)
        telemetry.finish_ocr_run(
            ocr_run_id=ocr_run_id,
            status="failed",
            note=str(exc),
        )
        return 1

    telemetry.update_ocr_progress(
        ocr_run_id=ocr_run_id,
        note=f"state_entries_removed={removed}",
    )

    # ── 3. Запустить index_rag как модуль (устойчиво к относительным импортам) ──
    cmd = [sys.executable, "-u", "-m", "rag_catalog.core.index_rag"]

    if cfg.get("catalog_path"):
        cmd += ["--catalog", cfg["catalog_path"]]
    if args.qdrant_url:
        cmd += ["--url", args.qdrant_url]
    else:
        cmd += ["--db", cfg["qdrant_db_path"]]
    cmd += [
        "--collection", args.collection,
        "--workers",    str(workers_effective),
        "--stage",      "large",  # сканированные PDF — этап large
        "--force-ocr",
        # НЕТ --no-ocr: OCR включён
    ]
    if str(args.ocr_engine or "tesseract").strip().lower() == "rapidocr":
        cmd += ["--ocr-engine", "rapidocr"]

    logger.info("")
    logger.info("=" * 60)
    logger.info("Запуск OCR-прохода…")
    logger.info("Команда: %s", " ".join(cmd))
    logger.info("(Это может занять несколько часов — ~5–30 сек на страницу OCR)")
    logger.info("=" * 60)
    logger.info("")

    def _actual_processed_count() -> int:
        """Read the real processed_files count from index_stage_progress (large stage)."""
        try:
            rows = telemetry.fetch_dicts(
                """SELECT processed_files FROM index_stage_progress
                   WHERE stage='large' AND ts_started >= ?
                   ORDER BY ts_started DESC LIMIT 1""",
                (ocr_start_time,),
            )
            if rows:
                return int(rows[0].get("processed_files") or 0)
        except Exception:
            pass
        return 0

    try:
        run_kwargs: Dict[str, Any] = {"check": False}
        if os.name == "nt":
            # CREATE_NO_WINDOW suppresses console popup; no DETACHED_PROCESS so that
            # stdout/stderr are inherited from this process (→ ocr.log).
            run_kwargs["creationflags"] = int(getattr(subprocess, "CREATE_NO_WINDOW", 0) or 0)
        result = subprocess.run(cmd, **run_kwargs)
        exit_code = result.returncode
        status = "completed" if exit_code == 0 else "failed"
        telemetry.finish_ocr_run(
            ocr_run_id=ocr_run_id,
            status=status,
            processed_pdfs=_actual_processed_count(),
            note=f"exit_code={exit_code}",
        )
        return exit_code
    except KeyboardInterrupt:
        logger.info("OCR прерван пользователем (Ctrl+C)")
        telemetry.finish_ocr_run(
            ocr_run_id=ocr_run_id,
            status="cancelled",
            processed_pdfs=_actual_processed_count(),
            note="interrupted by user",
        )
        return 0
    except Exception as exc:
        logger.error("Ошибка запуска индексатора: %s", exc)
        telemetry.finish_ocr_run(
            ocr_run_id=ocr_run_id,
            status="failed",
            processed_pdfs=_actual_processed_count(),
            note=str(exc),
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
