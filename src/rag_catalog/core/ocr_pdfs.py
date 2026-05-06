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
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from ._platform_compat import apply_windows_platform_workarounds
apply_windows_platform_workarounds()

from .index_state_db import IndexStateDB
from .rag_core import load_config
from .telemetry_db import TelemetryDB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
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
        client.get_collection(collection)
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
    args = parser.parse_args()
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

    # ── 1. Найти сканы в Qdrant ──────────────────────────────────────────────
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
        return 0

    logger.info("")
    logger.info("Файлы для OCR (%d):", len(scanned))
    for fp in scanned[:20]:
        logger.info("  %s", fp)
    if len(scanned) > 20:
        logger.info("  … и ещё %d файлов", len(scanned) - 20)

    if args.dry_run:
        logger.info("--dry-run: индексатор не запущен.")
        return 0

    # ── Создать запись OCR-прохода в телеметрии ──────────────────────────────
    ocr_run_id = telemetry.start_ocr_run(
        collection_name=args.collection,
        found_scanned=len(scanned),
        note=f"min_text_len={args.min_text_len}",
        worker_pid=os.getpid(),
    )
    logger.info("OCR run_id: %s", ocr_run_id)

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
    cmd += [
        "--db",         cfg["qdrant_db_path"],
        "--collection", args.collection,
        "--workers",    str(workers_effective),
        "--stage",      "large",  # сканированные PDF — этап large
        # НЕТ --no-ocr: OCR включён
    ]

    logger.info("")
    logger.info("=" * 60)
    logger.info("Запуск OCR-прохода…")
    logger.info("Команда: %s", " ".join(cmd))
    logger.info("(Это может занять несколько часов — ~5–30 сек на страницу OCR)")
    logger.info("=" * 60)
    logger.info("")

    try:
        run_kwargs: Dict[str, Any] = {"check": False}
        if os.name == "nt":
            run_kwargs["creationflags"] = _windows_detached_creationflags()
        result = subprocess.run(cmd, **run_kwargs)
        exit_code = result.returncode
        status = "completed" if exit_code == 0 else "failed"
        telemetry.finish_ocr_run(
            ocr_run_id=ocr_run_id,
            status=status,
            processed_pdfs=len(scanned),
            note=f"exit_code={exit_code}",
        )
        return exit_code
    except KeyboardInterrupt:
        logger.info("OCR прерван пользователем (Ctrl+C)")
        telemetry.finish_ocr_run(
            ocr_run_id=ocr_run_id,
            status="cancelled",
            note="interrupted by user",
        )
        return 0
    except Exception as exc:
        logger.error("Ошибка запуска индексатора: %s", exc)
        telemetry.finish_ocr_run(
            ocr_run_id=ocr_run_id,
            status="failed",
            note=str(exc),
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
