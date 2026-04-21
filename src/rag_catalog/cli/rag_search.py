"""
rag_search.py — CLI-инструмент семантического поиска по RAG-каталогу.

Использует общий RAGSearcher из rag_core.py.

Режимы:
  Интерактивный:  python rag_search.py
  CLI:            python rag_search.py --query "договоры газпром" --limit 5
  Фильтр:         python rag_search.py --query "счета" --type .xlsx
  Только текст:   python rag_search.py --query "паспорта" --content-only
  Docker Qdrant:  python rag_search.py --url http://localhost:6333 --query "..."
"""

import argparse
import json
import logging
import sys
from typing import Optional

from rag_catalog.core._platform_compat import apply_windows_platform_workarounds
apply_windows_platform_workarounds()

from rag_catalog.core.rag_core import RAGSearcher, load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ─────────────────────────── interactive session ────────────────────────

def interactive_search(searcher: RAGSearcher) -> None:
    """Запустить интерактивную поисковую сессию."""
    cfg = searcher.config
    print("\n=== RAG Семантический Поиск ===")
    print(f"Коллекция: {cfg['collection_name']}")
    print("Введите 'exit' для выхода, 'help' для справки.\n")

    while True:
        try:
            raw = input("Запрос: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nВыход.")
            break

        if not raw:
            continue
        if raw.lower() == "exit":
            break
        if raw.lower() == "help":
            _print_help()
            continue

        # Разбор встроенных флагов: /type .docx, /content
        file_type: Optional[str] = None
        content_only = False
        clean_parts = []
        tokens = raw.split()
        i = 0
        while i < len(tokens):
            if tokens[i] == "/type" and i + 1 < len(tokens):
                file_type = tokens[i + 1]
                i += 2
            elif tokens[i] == "/content":
                content_only = True
                i += 1
            else:
                clean_parts.append(tokens[i])
                i += 1

        query = " ".join(clean_parts) if clean_parts else raw

        try:
            results = searcher.search(
                query, limit=10, file_type=file_type, content_only=content_only, source="cli_interactive"
            )
        except Exception as exc:
            logger.error("Ошибка поиска: %s", exc)
            print(f"Ошибка: {exc}\n")
            continue

        if not results:
            print("Ничего не найдено.\n")
            continue

        print(f"\nНайдено результатов: {len(results)}\n")
        for idx, r in enumerate(results, 1):
            print(f"[{idx}] Score: {r['score']}  |  Type: {r['type']}")
            print(f"     Файл:  {r['filename']}")
            print(f"     Путь:  {r['path']}")
            details = []
            if r.get("extension"):
                details.append(f"Ext: {r['extension']}")
            if r.get("size_mb") is not None:
                details.append(f"Размер: {r['size_mb']} МБ")
            if r.get("modified"):
                details.append(f"Изменён: {r['modified'][:10]}")
            if details:
                print("     " + "  |  ".join(details))
            preview = r["text"][:300].replace("\n", " ")
            if len(r["text"]) > 300:
                preview += "…"
            print(f"     Текст: {preview}\n")


def _print_help() -> None:
    print(
        "\n=== СПРАВКА ===\n"
        "Флаги (добавляйте в начало запроса):\n"
        "  /type .docx    — фильтр по типу файла (.docx, .xlsx, .pdf)\n"
        "  /content       — только содержимое (без метаданных файлов)\n"
        "  exit           — выход\n\n"
        "Примеры:\n"
        "  договоры газпром\n"
        "  /type .xlsx финансовый отчёт\n"
        "  /content служебная записка\n"
        "  /type .docx /content паспорта\n"
    )


# ─────────────────────────── CLI (JSON output) ─────────────────────────

def cli_search(
    searcher: RAGSearcher,
    query: str,
    limit: int,
    file_type: Optional[str],
    content_only: bool,
) -> int:
    """Выполнить поиск и вывести результат как JSON."""
    try:
        results = searcher.search(query, limit, file_type, content_only, source="cli_json")
        print(json.dumps(results, indent=2, ensure_ascii=False))
        return 0
    except Exception as exc:
        logger.error("Ошибка поиска: %s", exc)
        print(json.dumps({"error": str(exc)}, ensure_ascii=False))
        return 1


# ─────────────────────────── entry point ───────────────────────────────

def main() -> int:
    cfg = load_config()

    parser = argparse.ArgumentParser(
        description="RAG Семантический поиск по каталогу",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Примеры:\n"
            "  python rag_search.py\n"
            "  python rag_search.py --query \"договоры газпром\" --limit 5\n"
            "  python rag_search.py --query \"счета\" --type .xlsx\n"
            "  python rag_search.py --url http://localhost:6333 --query \"паспорта\"\n"
        ),
    )
    parser.add_argument("--url", default=cfg.get("qdrant_url", ""), dest="qdrant_url",
                        help="URL Qdrant-сервера (по умолчанию: http://localhost:6333)")
    parser.add_argument("--db", default=cfg["qdrant_db_path"],
                        help="Путь к локальной базе Qdrant (если --url не задан)")
    parser.add_argument("--collection", default=cfg["collection_name"], help="Имя коллекции")
    parser.add_argument("--model", default=cfg["embedding_model"], help="Модель эмбеддинга")
    parser.add_argument("--query", help="Поисковый запрос (без — интерактивный режим)")
    parser.add_argument("--limit", type=int, default=10, help="Количество результатов")
    parser.add_argument("--type", dest="file_type", help="Фильтр по расширению (.docx, .xlsx, .pdf)")
    parser.add_argument("--content-only", action="store_true", help="Только содержимое, без метаданных")
    args = parser.parse_args()

    cfg["qdrant_url"] = args.qdrant_url
    cfg["qdrant_db_path"] = args.db
    cfg["collection_name"] = args.collection
    cfg["embedding_model"] = args.model

    try:
        searcher = RAGSearcher(cfg)
        if not searcher.connected:
            logger.error("Не удалось подключиться к Qdrant. Убедитесь что Docker запущен.")
            return 1
    except Exception as exc:
        logger.error("Ошибка инициализации: %s", exc)
        return 1

    if args.query:
        return cli_search(searcher, args.query, args.limit, args.file_type, args.content_only)

    interactive_search(searcher)
    return 0


if __name__ == "__main__":
    sys.exit(main())
