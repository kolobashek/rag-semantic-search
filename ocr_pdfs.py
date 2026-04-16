"""
ocr_pdfs.py — OCR-проход по сканированным PDF в RAG-каталоге.

Алгоритм:
  1. Подключается к Qdrant и находит PDF-файлы с пустым/коротким текстом
     (индексированные ранее с флагом --no-ocr).
  2. Удаляет найденные файлы из index_state.json (сбрасывает кэш),
     чтобы index_rag.py переиндексировал их заново.
  3. Запускает index_rag.py без --no-ocr — OCR применяется автоматически
     для файлов с пустым текстовым слоем.

Требования:
    pip install pytesseract pdf2image pymupdf
    Tesseract OCR: https://github.com/UB-Mannheim/tesseract/wiki
    Poppler (для pdf2image): https://github.com/oschwartz10612/poppler-windows

Запуск:
    python ocr_pdfs.py
    python ocr_pdfs.py --url http://localhost:6333 --min-text-len 100
    python ocr_pdfs.py --dry-run   # показать файлы без запуска OCR
"""

import argparse
import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# ── WMI-патч (Python 3.14) — должен быть до любого импорта torch/transformers ──
import platform as _p
if hasattr(_p, '_wmi_query'):
    _p._wmi_query = lambda *a, **kw: ('10.0.19041', '1', 'Multiprocessor Free', '0', '0')
# ─────────────────────────────────────────────────────────────────────────────

from rag_core import load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ──────────────────────────── константы ─────────────────────────────────────

# PDF с текстом короче этого порога считается сканом (нет полезного текста)
DEFAULT_MIN_TEXT_LEN = 50


# ─────────────────────────── Qdrant helpers ──────────────────────────────────

def find_scanned_pdfs(
    qdrant_url: str,
    collection: str,
    min_text_len: int,
    scroll_limit: int = 1000,
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
            client = QdrantClient(url=qdrant_url)
        else:
            from rag_core import load_config as _lc
            cfg = _lc()
            client = QdrantClient(path=str(cfg["qdrant_db_path"]))
        client.get_collection(collection)
    except Exception as exc:
        logger.error("Не удалось подключиться к Qdrant: %s", exc)
        return []

    def _scroll_all(flt: "Filter") -> List[Dict[str, Any]]:
        """Прокрутить всю коллекцию с заданным фильтром."""
        results: List[Dict[str, Any]] = []
        offset: Optional[Any] = None
        while True:
            try:
                records, offset = client.scroll(
                    collection_name=collection,
                    scroll_filter=flt,
                    limit=scroll_limit,
                    offset=offset,
                    with_payload=True,
                    with_vectors=False,
                )
            except Exception as exc:
                logger.error("Ошибка прокрутки коллекции: %s", exc)
                break
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

def remove_from_state(state_path: Path, file_paths: List[str]) -> int:
    """
    Удалить записи указанных файлов из index_state.json.

    Возвращает количество удалённых записей.
    """
    if not state_path.exists():
        logger.warning("Файл состояния не найден: %s", state_path)
        return 0

    try:
        with open(state_path, "r", encoding="utf-8") as fh:
            state: Dict[str, Any] = json.load(fh)
    except Exception as exc:
        logger.error("Не удалось прочитать state-файл: %s", exc)
        return 0

    files_dict = state.get("files", {})
    removed = 0
    path_set = set(file_paths)

    for fp in list(files_dict.keys()):
        if fp in path_set:
            del files_dict[fp]
            removed += 1

    state["files"] = files_dict

    try:
        with open(state_path, "w", encoding="utf-8") as fh:
            json.dump(state, fh, indent=2, ensure_ascii=False)
        logger.info("Удалено записей из state: %d", removed)
    except Exception as exc:
        logger.error("Не удалось записать state-файл: %s", exc)
        return 0

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
        help="Папка с index_state.json",
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
        default=1,
        help="Количество рабочих потоков для индексатора (по умолчанию 1 — OCR CPU-intensive)",
    )
    args = parser.parse_args()

    # ── 1. Найти сканы в Qdrant ──────────────────────────────────────────────
    scanned = find_scanned_pdfs(
        qdrant_url=args.qdrant_url,
        collection=args.collection,
        min_text_len=args.min_text_len,
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

    # ── 2. Убрать их из state.json, чтобы индексатор переобработал ──────────
    state_path = Path(args.state_dir) / "index_state.json"
    removed = remove_from_state(state_path, scanned)
    if removed == 0 and state_path.exists():
        logger.warning("Записи не найдены в state.json — продолжаю всё равно")

    # ── 3. Запустить index_rag.py без --no-ocr ───────────────────────────────
    index_script = Path(__file__).parent / "index_rag.py"
    if not index_script.exists():
        logger.error("index_rag.py не найден в %s", index_script.parent)
        return 1

    cmd = [sys.executable, "-u", str(index_script)]

    if cfg.get("catalog_path"):
        cmd += ["--catalog", cfg["catalog_path"]]
    if args.qdrant_url:
        cmd += ["--url", args.qdrant_url]
    cmd += [
        "--db",         cfg["qdrant_db_path"],
        "--collection", args.collection,
        "--workers",    str(args.workers),
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
        result = subprocess.run(cmd, check=False)
        return result.returncode
    except KeyboardInterrupt:
        logger.info("OCR прерван пользователем (Ctrl+C)")
        return 0
    except Exception as exc:
        logger.error("Ошибка запуска индексатора: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
