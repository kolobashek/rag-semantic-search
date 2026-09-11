"""Валидация config.json перед стартом индексатора.

validate_config(cfg) -> list[Issue]. Уровни: error (стартовать нельзя),
warning (стартуем, но пишем в лог), info (пояснение).
"""

from __future__ import annotations

import json
import sqlite3
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .embedding_collections import resolve_embedding_collection_name
from .rag_core import DEFAULT_CONFIG

LEVEL_ERROR = "error"
LEVEL_WARNING = "warning"
LEVEL_INFO = "info"

# Ключи, которые читаются кодом, но отсутствуют в DEFAULT_CONFIG
# (собрано grep-ом cfg.get(...) / config.get(...) по src/rag_catalog).
# Ключи индексатора index_embedded_media / index_cleanup_skip_failed_ratio /
# indexer_heartbeat_path объявлены в DEFAULT_CONFIG и известны через него.
EXTRA_KNOWN_KEYS: frozenset[str] = frozenset(
    {
        # индексатор
        "ocr_engine",
        "ocr_rapid_files_per_process",
        "ocr_rapid_input_mb_per_process",
        "ocr_rapid_pages_per_process",
        "synonym_map",
        "model_path",
        "qdrant_collection",
        "qdrant_finalize_timeout_sec",
        "qdrant_indexing_threshold",
        "qdrant_max_unindexed_vectors",
        "qdrant_payload_audit_sample_size",
        "qdrant_spreadsheet_audit_sample_size",
        # cloud drive / ui / llm / telegram / pilot
        "cloud_drive_acl",
        "cloud_drive_autosync_minutes",
        "cloud_drive_backup_dir",
        "cloud_drive_backup_max_age_hours",
        "cloud_drive_queue_lag_warn_sec",
        "filesystem_search_max_items",
        "launcher_web_start_timeout_sec",
        "llm_expand_timeout_sec",
        "llm_rag_timeout_sec",
        "telegram_upload_path",
        "ui_quick_search_timeout_sec",
        "ui_reconnect_timeout_sec",
        "ui_search_timeout_sec",
        "pilot_ground_truth_coverage_min",
        "pilot_no_answer_accuracy_min",
        "pilot_retrieval_max_age_hours",
        "pilot_retrieval_recall_min",
        "pilot_search_p95_ms",
    }
)

# Ключи-заглушки: объявлены в DEFAULT_CONFIG, но нигде в коде не читаются.
# Проверяется по исходникам пакета: как только ключ «оживят», предупреждение исчезнет.
STUB_KEY_CANDIDATES: tuple[str, ...] = ("retrieval_lexical_top_k", "retrieval_final_top_k")

EMBEDDING_MISMATCH_KEY = "embedding_config"


@dataclass(frozen=True)
class Issue:
    level: str
    key: str
    message: str

    def __str__(self) -> str:
        return f"[{self.level}] {self.key}: {self.message}"


def known_config_keys() -> frozenset[str]:
    return frozenset(DEFAULT_CONFIG.keys()) | EXTRA_KNOWN_KEYS


def _package_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _key_referenced_in_sources(key: str, root: Optional[Path] = None) -> bool:
    """True если ключ читается где-то в коде пакета (кроме объявления в rag_core)."""
    base = root or _package_root()
    needle = f'"{key}"'
    for path in base.rglob("*.py"):
        if path.name == "rag_core.py" and root is None:
            continue
        try:
            if needle in path.read_text(encoding="utf-8", errors="ignore"):
                return True
        except OSError:
            continue
    return False


def check_qdrant_reachable(url: str, *, timeout_sec: float = 3.0) -> Optional[str]:
    """None если Qdrant отвечает на GET /collections, иначе текст ошибки."""
    clean = str(url or "").strip().rstrip("/")
    if not clean:
        return None
    try:
        with urllib.request.urlopen(f"{clean}/collections", timeout=timeout_sec) as resp:  # noqa: S310
            if int(getattr(resp, "status", 200) or 200) >= 400:
                return f"HTTP {resp.status}"
            body = resp.read(65536)
            try:
                json.loads(body.decode("utf-8", errors="replace"))
            except ValueError:
                return "ответ не JSON"
            return None
    except urllib.error.HTTPError as exc:
        return f"HTTP {exc.code}"
    except Exception as exc:  # URLError, timeout, ConnectionRefused
        return str(exc)


def read_state_index_config(state_db_path: Path) -> Dict[str, str]:
    """Прочитать index_config из state-БД без её инициализации/миграции."""
    if not state_db_path.exists():
        return {}
    uri = f"file:{state_db_path.as_posix()}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=5.0)
    except sqlite3.Error:
        return {}
    try:
        rows = conn.execute("SELECT key, value FROM index_config").fetchall()
        return {str(k): str(v) for k, v in rows}
    except sqlite3.Error:
        return {}
    finally:
        conn.close()


def validate_config(
    cfg: Dict[str, Any],
    *,
    check_qdrant: bool = True,
    check_state_db: bool = True,
    state_db_path: Optional[Path] = None,
    collection_name: Optional[str] = None,
    embedding_model: Optional[str] = None,
    qdrant_timeout_sec: float = 3.0,
    source_root: Optional[Path] = None,
) -> List[Issue]:
    issues: List[Issue] = []
    known = known_config_keys()

    # 1. Неизвестные ключи
    for key in sorted(str(k) for k in cfg.keys()):
        if key not in known:
            issues.append(Issue(LEVEL_WARNING, key, "неизвестный ключ конфига — нигде не читается (опечатка?)"))

    # 2. Ключи-заглушки
    for key in STUB_KEY_CANDIDATES:
        if key in cfg and not _key_referenced_in_sources(key, source_root):
            issues.append(Issue(LEVEL_WARNING, key, "unused: ключ объявлен, но кодом поиска не используется"))

    # 3. index_skip_ocr
    skip_ocr = bool(cfg.get("index_skip_ocr", False))
    issues.append(
        Issue(
            LEVEL_INFO,
            "index_skip_ocr",
            (
                "true — OCR сканов/картинок ВЫКЛЮЧЕН по умолчанию (PDF без текстового слоя "
                "получат status=deferred_ocr); включить: index_skip_ocr=false или --force-ocr"
                if skip_ocr
                else "false — OCR включён по умолчанию; выключить разово: --no-ocr"
            ),
        )
    )

    # 3b. index_cleanup_skip_failed_ratio — доля в [0, 1]
    if "index_cleanup_skip_failed_ratio" in cfg:
        raw_ratio = cfg.get("index_cleanup_skip_failed_ratio")
        try:
            ratio = float(raw_ratio)
            ratio_ok = 0.0 <= ratio <= 1.0
        except (TypeError, ValueError):
            ratio_ok = False
        if not ratio_ok:
            issues.append(
                Issue(
                    LEVEL_WARNING,
                    "index_cleanup_skip_failed_ratio",
                    f"ожидается число от 0 до 1 (доля нечитаемых файлов), получено {raw_ratio!r}; будет 0.1",
                )
            )

    # 4. OCR-бинарники
    for key, kind in (("ocr_tesseract_cmd", "file"), ("ocr_poppler_bin", "dir")):
        value = str(cfg.get(key) or "").strip()
        if not value:
            continue
        path = Path(value)
        ok = path.is_file() if kind == "file" else path.is_dir()
        if not ok:
            level = LEVEL_WARNING if skip_ocr else LEVEL_ERROR
            issues.append(Issue(level, key, f"путь не найден: {value}"))

    # 5. Доступность Qdrant (только warning)
    qdrant_url = str(cfg.get("qdrant_url") or "").strip()
    if check_qdrant and qdrant_url:
        error = check_qdrant_reachable(qdrant_url, timeout_sec=qdrant_timeout_sec)
        if error:
            issues.append(Issue(LEVEL_WARNING, "qdrant_url", f"Qdrant недоступен ({qdrant_url}/collections): {error}"))

    # 6. embedding_model / collection_name / vector_size ↔ index_config в state-БД
    if check_state_db:
        db_path = state_db_path
        if db_path is None:
            base = str(cfg.get("qdrant_db_path") or "").strip()
            db_path = Path(base) / "index_state.db" if base else None
        if db_path is not None:
            stored = read_state_index_config(Path(db_path))
            if stored:
                model = str(embedding_model if embedding_model is not None else cfg.get("embedding_model") or "")
                collection = str(
                    collection_name
                    if collection_name is not None
                    else resolve_embedding_collection_name(
                        str(cfg.get("collection_name") or ""),
                        model,
                        enabled=bool(cfg.get("embedding_collection_versioning", False)),
                        suffix=str(cfg.get("embedding_collection_suffix") or ""),
                    )
                )
                desired = {
                    "embedding_model": model,
                    "collection_name": collection,
                    "vector_size": str(int(cfg.get("vector_size") or 0)),
                }
                mismatches = [
                    f"{key}: state={stored.get(key)!r}, config={value!r}"
                    for key, value in desired.items()
                    if stored.get(key) and stored.get(key) != value
                ]
                if mismatches:
                    issues.append(
                        Issue(
                            LEVEL_ERROR,
                            EMBEDDING_MISMATCH_KEY,
                            "индекс создан с другой embedding-конфигурацией: "
                            + "; ".join(mismatches)
                            + ". Запустите с --recreate (индекс будет перестроен) "
                            "или верните прежние значения / отдельную collection.",
                        )
                    )
    return issues


def issues_by_level(issues: Iterable[Issue], level: str) -> List[Issue]:
    return [issue for issue in issues if issue.level == level]
