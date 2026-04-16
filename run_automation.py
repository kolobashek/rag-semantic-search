"""
run_automation.py — Автоматический запуск индексирования + OCR для RAG-системы.

Запускает шаги последовательно без участия пользователя.
Читает пути из config.json.

Запуск:
    python run_automation.py
"""

import logging
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from rag_core import load_config

# ─────────────────────────── config ────────────────────────────────────
cfg = load_config()

CATALOG_PATH = cfg["catalog_path"]
QDRANT_DB_PATH = cfg["qdrant_db_path"]
COLLECTION_NAME = cfg["collection_name"]
LOG_FILE = cfg["log_file"]

# ─────────────────────────── logging ───────────────────────────────────
# FileHandler с UTF-8 — корректная запись кириллицы в файл.
# StreamHandler с явным указанием stdout и попыткой переключить на UTF-8,
# чтобы кириллица не искажалась в консоли Windows (cp866/cp1251).
_file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
_stream_handler = logging.StreamHandler(sys.stdout)
try:
    _stream_handler.stream.reconfigure(encoding="utf-8")  # Python 3.7+
except AttributeError:
    pass

_fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
_file_handler.setFormatter(_fmt)
_stream_handler.setFormatter(_fmt)

logging.basicConfig(
    level=logging.INFO,
    handlers=[_file_handler, _stream_handler],
)
logger = logging.getLogger(__name__)


# ─────────────────────────── helpers ───────────────────────────────────

def run_command(args: list, description: str) -> bool:
    """
    Запустить команду (список аргументов) и дождаться завершения.
    Использует список аргументов вместо shell=True для безопасности.
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("ЗАПУСК: %s", description)
    logger.info("=" * 60)
    logger.info("Команда: %s", " ".join(args))

    start = time.time()
    try:
        process = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )

        for line in process.stdout:
            line = line.rstrip()
            print(line)
            logger.info(line)

        rc = process.wait()
        elapsed = time.time() - start

        if rc == 0:
            logger.info("✓ %s — ЗАВЕРШЕНО успешно (%.1f мин)", description, elapsed / 60)
            return True
        else:
            logger.error("✗ %s — ОШИБКА, код возврата: %d", description, rc)
            return False

    except FileNotFoundError:
        logger.error("✗ python не найден или файл скрипта недоступен: %s", args)
        return False
    except Exception as exc:
        logger.error("✗ Ошибка при выполнении %s: %s", description, exc)
        return False


def check_prerequisites() -> bool:
    """Проверить наличие необходимых файлов и директорий."""
    logger.info("Проверка предварительных условий…")
    ok = True

    script = Path(__file__).parent / "index_rag.py"
    if not script.exists():
        logger.error("ОШИБКА: index_rag.py не найден в %s", script.parent)
        ok = False

    if not Path(CATALOG_PATH).exists():
        logger.error("ОШИБКА: Папка каталога не найдена: %s", CATALOG_PATH)
        ok = False

    if ok:
        logger.info("✓ Все условия выполнены")
    return ok


# ─────────────────────────── main workflow ─────────────────────────────

def main() -> bool:
    import argparse

    parser = argparse.ArgumentParser(
        description="RAG Automation — запуск индексирования (+ опционально OCR)"
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help=(
            "Пересоздать коллекцию с нуля (медленно, 2–3 часа). "
            "Без флага — инкрементальное обновление (быстро, 5–10 мин)."
        ),
    )
    args = parser.parse_args()

    mode = "ПОЛНАЯ ПЕРЕИНДЕКСАЦИЯ (--recreate)" if args.recreate else "ИНКРЕМЕНТАЛЬНОЕ ОБНОВЛЕНИЕ"

    logger.info("")
    logger.info("=" * 60)
    logger.info("RAG AUTOMATION — Индексирование + OCR")
    logger.info("Режим: %s", mode)
    logger.info("=" * 60)
    logger.info("Начало: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("Каталог: %s", CATALOG_PATH)
    logger.info("База Qdrant: %s", QDRANT_DB_PATH)
    logger.info("Лог: %s", LOG_FILE)

    if not check_prerequisites():
        logger.error("\nАВАРИЙНЫЙ ОСТАНОВ: условия не выполнены")
        return False

    # ── Шаг 1: Индексирование ──────────────────────────────────────
    logger.info("")
    logger.info("=" * 60)
    if args.recreate:
        logger.info("ШАГ 1: ПОЛНАЯ ПЕРЕИНДЕКСАЦИЯ ДОКУМЕНТОВ")
        logger.info("Это может занять 2–3 часа…")
    else:
        logger.info("ШАГ 1: ИНКРЕМЕНТАЛЬНОЕ ОБНОВЛЕНИЕ ИНДЕКСА")
        logger.info("Обрабатываются только новые/изменённые файлы (~5–10 мин)")
    logger.info("=" * 60)

    index_script = str(Path(__file__).parent / "index_rag.py")
    index_args = [
        sys.executable,
        index_script,
        "--catalog", CATALOG_PATH,
        "--db", QDRANT_DB_PATH,
        "--collection", COLLECTION_NAME,
    ]
    if args.recreate:
        index_args.append("--recreate")

    if not run_command(index_args, "ИНДЕКСИРОВАНИЕ"):
        logger.error("\nАВАРИЙНЫЙ ОСТАНОВ: индексирование завершилось с ошибкой")
        return False

    # Верификация индексирования
    # Используем тот же режим подключения, что и индексатор:
    # если задан qdrant_url — сервер, иначе — локальный SQLite.
    try:
        from qdrant_client import QdrantClient
        qdrant_url = cfg.get("qdrant_url", "")
        if qdrant_url:
            qd = QdrantClient(url=qdrant_url)
            logger.info("Верификация: подключение к Qdrant-серверу %s", qdrant_url)
        else:
            qd = QdrantClient(path=QDRANT_DB_PATH)
        info = qd.get_collection(COLLECTION_NAME)
        logger.info("✓ Верификация: %d точек проиндексировано", info.points_count)
    except Exception as exc:
        logger.error("✗ Верификация не удалась: %s", exc)
        return False

    # ── Шаг 2: OCR (опционально) ───────────────────────────────────
    logger.info("")
    logger.info("=" * 60)
    logger.info("ШАГ 2: OCR ДЛЯ СКАНИРОВАННЫХ PDF (ОПЦИОНАЛЬНО)")
    logger.info("Это может занять 12–18 часов…")
    logger.info("Ctrl+C — пропустить OCR и завершить")
    logger.info("=" * 60)

    ocr_script = Path(__file__).parent / "ocr_pdfs.py"

    try:
        for i in range(10, 0, -1):
            print(f"\rЗапуск OCR через {i} сек… (Ctrl+C чтобы пропустить)  ", end="", flush=True)
            time.sleep(1)
        print()

        if not ocr_script.exists():
            logger.warning("ocr_pdfs.py не найден — OCR пропущен.")
            logger.warning("Примечание: PDF-файлы уже обрабатываются автоматически в index_rag.py.")
        else:
            run_command([sys.executable, str(ocr_script)], "OCR")

    except KeyboardInterrupt:
        logger.info("\n\n✓ OCR пропущен пользователем")

    # ── Финальная сводка ───────────────────────────────────────────
    logger.info("")
    logger.info("=" * 60)
    logger.info("АВТОМАТИЗАЦИЯ ЗАВЕРШЕНА УСПЕШНО!")
    logger.info("=" * 60)
    logger.info("Окончание: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("")
    logger.info("Запуск интерфейсов:")
    logger.info("  Веб UI:        streamlit run app_ui.py")
    logger.info("  Windows-приложение: python windows_app.py")
    logger.info("  CLI:           python rag_search_fixed.py --query \"запрос\"")
    logger.info("Лог файл: %s", LOG_FILE)
    logger.info("=" * 60)

    return True


# ─────────────────────────── entry point ───────────────────────────────

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\n\nПРЕРВАНО пользователем")
        sys.exit(1)
    except Exception as exc:
        logger.error("\n\nКРИТИЧЕСКАЯ ОШИБКА: %s", exc)
        sys.exit(1)
