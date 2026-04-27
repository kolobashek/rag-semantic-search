"""
telemetry_db.py — SQLite-телеметрия для индексации и поисковых запросов.
"""

import json
import re
import sqlite3
import threading
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


DEFAULT_SEARCH_ALIAS_GROUPS: List[Dict[str, Any]] = [
    {
        "key": "company_card",
        "label": "Карточка предприятия",
        "aliases": [
            "карточка предприятия",
            "карточка организации",
            "карточка контрагента",
            "реквизиты",
            "банковские реквизиты",
            "расчетный счет",
            "расчётный счёт",
            "р/с",
            "рс",
        ],
        "negative_aliases": ["карточка учета", "инвентарная карточка", "карточка учета шин"],
    },
    {
        "key": "machine_passport",
        "label": "Паспорт техники",
        "aliases": [
            "паспорт техники",
            "псм",
            "птс",
            "стс",
            "техпаспорт",
            "технический паспорт",
            "паспорт самоходной машины",
            "выписка из электронного паспорта",
        ],
        "negative_aliases": ["паспорт гражданина", "личный паспорт"],
    },
    {
        "key": "contract",
        "label": "Договор",
        "aliases": ["договор", "договора", "контракт", "соглашение", "доп соглашение", "дс"],
        "negative_aliases": [],
    },
    {
        "key": "invoice",
        "label": "Счет на оплату",
        "aliases": ["счет", "счёт", "счет на оплату", "счёт на оплату", "оплата", "платеж", "платёж"],
        "negative_aliases": ["счет 41", "счет 60", "счет 62"],
    },
    {
        "key": "waybill",
        "label": "Путевой лист",
        "aliases": ["путевой лист", "путевка", "путёвка", "п/л", "маршрутный лист"],
        "negative_aliases": [],
    },
    {
        "key": "act",
        "label": "Акт",
        "aliases": ["акт", "акты", "акт выполненных работ", "акт приема", "акт приёма", "акт сдачи"],
        "negative_aliases": [],
    },
    {
        "key": "reconciliation",
        "label": "Акт сверки",
        "aliases": ["акт сверки", "сверка", "взаиморасчеты", "взаиморасчёты"],
        "negative_aliases": [],
    },
    {
        "key": "power_of_attorney",
        "label": "Доверенность",
        "aliases": ["доверенность", "доверка", "представитель", "полномочия"],
        "negative_aliases": [],
    },
    {
        "key": "registry",
        "label": "Реестр",
        "aliases": ["реестр", "ведомость", "табель", "журнал", "список"],
        "negative_aliases": [],
    },
    {
        "key": "tax_request",
        "label": "Налоговое требование",
        "aliases": ["требование", "фнс", "налоговая", "выездная проверка", "впн", "представление документов"],
        "negative_aliases": [],
    },
]


def _norm_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower().replace("ё", "е"))


class TelemetryDB:
    """Простой потокобезопасный слой записи/чтения телеметрии."""

    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS search_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts TEXT NOT NULL,
                        source TEXT NOT NULL,
                        query TEXT NOT NULL,
                        query_original TEXT NOT NULL DEFAULT '',
                        query_used TEXT NOT NULL DEFAULT '',
                        limit_value INTEGER,
                        file_type TEXT,
                        content_only INTEGER NOT NULL DEFAULT 0,
                        results_count INTEGER NOT NULL DEFAULT 0,
                        duration_ms INTEGER NOT NULL DEFAULT 0,
                        ok INTEGER NOT NULL DEFAULT 1,
                        error TEXT,
                        username TEXT NOT NULL DEFAULT ''
                    );

                    CREATE TABLE IF NOT EXISTS fact_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts TEXT NOT NULL,
                        source TEXT NOT NULL,
                        question TEXT NOT NULL,
                        ok INTEGER NOT NULL DEFAULT 0,
                        answer TEXT,
                        source_type TEXT,
                        value_kg INTEGER,
                        duration_ms INTEGER NOT NULL DEFAULT 0,
                        error TEXT
                    );

                    CREATE TABLE IF NOT EXISTS index_runs (
                        run_id TEXT PRIMARY KEY,
                        ts_started TEXT NOT NULL,
                        ts_finished TEXT,
                        status TEXT NOT NULL,
                        worker_pid INTEGER NOT NULL DEFAULT 0,
                        catalog_path TEXT,
                        collection_name TEXT,
                        recreate INTEGER NOT NULL DEFAULT 0,
                        total_files INTEGER NOT NULL DEFAULT 0,
                        added_files INTEGER NOT NULL DEFAULT 0,
                        updated_files INTEGER NOT NULL DEFAULT 0,
                        skipped_files INTEGER NOT NULL DEFAULT 0,
                        deleted_files INTEGER NOT NULL DEFAULT 0,
                        error_files INTEGER NOT NULL DEFAULT 0,
                        points_added INTEGER NOT NULL DEFAULT 0,
                        note TEXT
                    );

                    CREATE TABLE IF NOT EXISTS index_stage_progress (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        run_id TEXT NOT NULL,
                        stage TEXT NOT NULL,
                        ts_started TEXT NOT NULL,
                        ts_updated TEXT NOT NULL,
                        ts_finished TEXT,
                        status TEXT NOT NULL,
                        total_files INTEGER NOT NULL DEFAULT 0,
                        processed_files INTEGER NOT NULL DEFAULT 0,
                        added_files INTEGER NOT NULL DEFAULT 0,
                        updated_files INTEGER NOT NULL DEFAULT 0,
                        skipped_files INTEGER NOT NULL DEFAULT 0,
                        error_files INTEGER NOT NULL DEFAULT 0,
                        points_added INTEGER NOT NULL DEFAULT 0,
                        UNIQUE(run_id, stage),
                        FOREIGN KEY(run_id) REFERENCES index_runs(run_id)
                    );

                    CREATE TABLE IF NOT EXISTS index_settings (
                        key TEXT PRIMARY KEY,
                        value_json TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS app_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts TEXT NOT NULL,
                        username TEXT NOT NULL DEFAULT '',
                        screen TEXT NOT NULL DEFAULT '',
                        feature TEXT NOT NULL DEFAULT '',
                        action TEXT NOT NULL DEFAULT '',
                        ok INTEGER NOT NULL DEFAULT 1,
                        details_json TEXT NOT NULL DEFAULT '{}'
                    );

                    CREATE TABLE IF NOT EXISTS search_feedback (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts TEXT NOT NULL,
                        username TEXT NOT NULL DEFAULT '',
                        source TEXT NOT NULL DEFAULT '',
                        query TEXT NOT NULL DEFAULT '',
                        result_path TEXT NOT NULL DEFAULT '',
                        result_title TEXT NOT NULL DEFAULT '',
                        feedback INTEGER NOT NULL,
                        result_rank INTEGER,
                        result_score REAL,
                        details_json TEXT NOT NULL DEFAULT '{}'
                    );

                    CREATE TABLE IF NOT EXISTS search_alias_groups (
                        key TEXT PRIMARY KEY,
                        label TEXT NOT NULL,
                        negative_aliases_json TEXT NOT NULL DEFAULT '[]',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS search_aliases (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        group_key TEXT NOT NULL,
                        alias TEXT NOT NULL,
                        alias_norm TEXT NOT NULL,
                        weight REAL NOT NULL DEFAULT 0.7,
                        source TEXT NOT NULL DEFAULT 'manual',
                        status TEXT NOT NULL DEFAULT 'active',
                        updated_at TEXT NOT NULL,
                        UNIQUE(group_key, alias_norm),
                        FOREIGN KEY(group_key) REFERENCES search_alias_groups(key) ON DELETE CASCADE
                    );

                    CREATE TABLE IF NOT EXISTS ocr_runs (
                        ocr_run_id TEXT PRIMARY KEY,
                        ts_started TEXT NOT NULL,
                        ts_updated TEXT NOT NULL,
                        ts_finished TEXT,
                        status TEXT NOT NULL,
                        worker_pid INTEGER NOT NULL DEFAULT 0,
                        collection_name TEXT NOT NULL DEFAULT '',
                        found_scanned INTEGER NOT NULL DEFAULT 0,
                        processed_pdfs INTEGER NOT NULL DEFAULT 0,
                        index_run_id TEXT,
                        note TEXT NOT NULL DEFAULT ''
                    );

                    CREATE TABLE IF NOT EXISTS index_schedules (
                        id TEXT PRIMARY KEY,
                        label TEXT NOT NULL DEFAULT '',
                        enabled INTEGER NOT NULL DEFAULT 1,
                        cadence TEXT NOT NULL DEFAULT 'daily',
                        time TEXT NOT NULL DEFAULT '03:00',
                        days_json TEXT NOT NULL DEFAULT '["Mon","Tue","Wed","Thu","Fri"]',
                        stage TEXT NOT NULL DEFAULT 'all',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        last_run_at TEXT
                    );
                    """
                )
                self._migrate_schema(conn)
                self._ensure_default_search_aliases(conn)
                conn.executescript(
                    """
                    CREATE INDEX IF NOT EXISTS idx_search_logs_ts
                      ON search_logs(ts);
                    CREATE INDEX IF NOT EXISTS idx_search_logs_username
                      ON search_logs(username, ts);
                    CREATE INDEX IF NOT EXISTS idx_fact_logs_ts
                      ON fact_logs(ts);
                    CREATE INDEX IF NOT EXISTS idx_index_runs_started
                      ON index_runs(ts_started);
                    CREATE INDEX IF NOT EXISTS idx_stage_run
                      ON index_stage_progress(run_id, stage);
                    CREATE INDEX IF NOT EXISTS idx_stage_status
                      ON index_stage_progress(status, ts_started);
                    CREATE INDEX IF NOT EXISTS idx_app_events_ts
                      ON app_events(ts);
                    CREATE INDEX IF NOT EXISTS idx_app_events_feature
                      ON app_events(feature, action, ts);
                    CREATE INDEX IF NOT EXISTS idx_search_feedback_query_path
                      ON search_feedback(query, result_path, ts);
                    CREATE INDEX IF NOT EXISTS idx_search_feedback_path
                      ON search_feedback(result_path, ts);
                    CREATE INDEX IF NOT EXISTS idx_search_aliases_norm
                      ON search_aliases(alias_norm, status);
                    CREATE INDEX IF NOT EXISTS idx_search_aliases_group
                      ON search_aliases(group_key, status);
                    CREATE INDEX IF NOT EXISTS idx_ocr_runs_ts
                      ON ocr_runs(ts_started);
                    CREATE INDEX IF NOT EXISTS idx_ocr_runs_status
                      ON ocr_runs(status);
                    CREATE INDEX IF NOT EXISTS idx_index_schedules_enabled
                      ON index_schedules(enabled);
                    """
                )

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        """Добавить отсутствующие столбцы для обратной совместимости."""
        search_cols = {row["name"] for row in conn.execute("PRAGMA table_info(search_logs)").fetchall()}
        if "username" not in search_cols:
            conn.execute("ALTER TABLE search_logs ADD COLUMN username TEXT NOT NULL DEFAULT ''")
            search_cols.add("username")
        if "query_original" not in search_cols:
            conn.execute("ALTER TABLE search_logs ADD COLUMN query_original TEXT NOT NULL DEFAULT ''")
            search_cols.add("query_original")
        if "query_used" not in search_cols:
            conn.execute("ALTER TABLE search_logs ADD COLUMN query_used TEXT NOT NULL DEFAULT ''")
            search_cols.add("query_used")
        conn.execute(
            """
            UPDATE search_logs
            SET query_original = query
            WHERE COALESCE(query_original, '') = ''
              AND COALESCE(query, '') <> ''
            """
        )
        conn.execute(
            """
            UPDATE search_logs
            SET query_used = query
            WHERE COALESCE(query_used, '') = ''
              AND COALESCE(query, '') <> ''
            """
        )
        index_cols = {row["name"] for row in conn.execute("PRAGMA table_info(index_runs)").fetchall()}
        if "worker_pid" not in index_cols:
            conn.execute("ALTER TABLE index_runs ADD COLUMN worker_pid INTEGER NOT NULL DEFAULT 0")
        ocr_cols = {row["name"] for row in conn.execute("PRAGMA table_info(ocr_runs)").fetchall()}
        if "worker_pid" not in ocr_cols:
            conn.execute("ALTER TABLE ocr_runs ADD COLUMN worker_pid INTEGER NOT NULL DEFAULT 0")

    def _ensure_default_search_aliases(self, conn: sqlite3.Connection) -> None:
        now = _utc_now()
        for group in DEFAULT_SEARCH_ALIAS_GROUPS:
            key = str(group["key"])
            label = str(group["label"])
            negative_aliases = group.get("negative_aliases") or []
            conn.execute(
                """
                INSERT OR IGNORE INTO search_alias_groups (
                    key, label, negative_aliases_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (key, label, json.dumps(negative_aliases, ensure_ascii=False), now, now),
            )
            for alias in [label, *(group.get("aliases") or [])]:
                alias_norm = _norm_text(str(alias))
                if not alias_norm:
                    continue
                conn.execute(
                    """
                    INSERT OR IGNORE INTO search_aliases (
                        group_key, alias, alias_norm, weight, source, status, updated_at
                    )
                    VALUES (?, ?, ?, ?, 'seed', 'active', ?)
                    """,
                    (key, str(alias).strip(), alias_norm, 0.85, now),
                )

    # ── search ────────────────────────────────────────────────────────

    def log_search(
        self,
        *,
        source: str,
        query: str,
        limit_value: int,
        file_type: Optional[str],
        content_only: bool,
        results_count: int,
        duration_ms: int,
        ok: bool,
        error: str = "",
        username: str = "",
        query_original: str = "",
        query_used: str = "",
    ) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO search_logs (
                        ts, source, query, query_original, query_used, limit_value, file_type, content_only,
                        results_count, duration_ms, ok, error, username
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        _utc_now(),
                        source or "unknown",
                        query or "",
                        query_original or query or "",
                        query_used or query or "",
                        int(limit_value),
                        file_type or "",
                        1 if content_only else 0,
                        int(results_count),
                        int(duration_ms),
                        1 if ok else 0,
                        error or "",
                        (username or "").strip().lower(),
                    ),
                )

    def log_search_feedback(
        self,
        *,
        username: str,
        source: str,
        query: str,
        result_path: str,
        result_title: str = "",
        feedback: int,
        result_rank: Optional[int] = None,
        result_score: Optional[float] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        raw_value = int(feedback)
        value = max(-3, min(3, raw_value))
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO search_feedback (
                        ts, username, source, query, result_path, result_title,
                        feedback, result_rank, result_score, details_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        _utc_now(),
                        (username or "").strip().lower(),
                        source or "",
                        query or "",
                        result_path or "",
                        result_title or "",
                        value,
                        int(result_rank) if result_rank is not None else None,
                        float(result_score) if result_score is not None else None,
                        json.dumps(details or {}, ensure_ascii=False, sort_keys=True),
                    ),
                )

    def get_search_feedback_scores(self, *, query: str, paths: List[str]) -> Dict[str, int]:
        clean_paths = [str(path or "").strip() for path in paths if str(path or "").strip()]
        if not clean_paths:
            return {}
        q = (query or "").strip().lower()
        placeholders = ",".join("?" for _ in clean_paths)
        params: List[Any] = [q, *clean_paths]
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    f"""
                    SELECT result_path, SUM(feedback) AS score
                    FROM search_feedback
                    WHERE lower(query)=? AND result_path IN ({placeholders})
                    GROUP BY result_path
                    """,
                    params,
                ).fetchall()
        return {str(row["result_path"]): int(row["score"] or 0) for row in rows}

    # ── search aliases ────────────────────────────────────────────────

    def list_search_alias_groups(self) -> List[Dict[str, Any]]:
        with self._lock:
            with self._connect() as conn:
                groups = conn.execute(
                    """
                    SELECT key, label, negative_aliases_json, updated_at
                    FROM search_alias_groups
                    ORDER BY label COLLATE NOCASE
                    """
                ).fetchall()
                aliases = conn.execute(
                    """
                    SELECT group_key, alias, weight, source, status
                    FROM search_aliases
                    WHERE status='active'
                    ORDER BY group_key, weight DESC, alias COLLATE NOCASE
                    """
                ).fetchall()
        by_group: Dict[str, List[Dict[str, Any]]] = {}
        for row in aliases:
            by_group.setdefault(str(row["group_key"]), []).append(
                {
                    "alias": str(row["alias"]),
                    "weight": float(row["weight"] or 0.7),
                    "source": str(row["source"] or ""),
                    "status": str(row["status"] or "active"),
                }
            )
        out: List[Dict[str, Any]] = []
        for row in groups:
            try:
                negative_aliases = json.loads(str(row["negative_aliases_json"] or "[]"))
            except json.JSONDecodeError:
                negative_aliases = []
            out.append(
                {
                    "key": str(row["key"]),
                    "label": str(row["label"]),
                    "aliases": by_group.get(str(row["key"]), []),
                    "negative_aliases": [str(x) for x in negative_aliases if str(x).strip()],
                    "updated_at": str(row["updated_at"] or ""),
                }
            )
        return out

    def save_search_alias_group(
        self,
        *,
        key: str,
        label: str,
        aliases: List[str],
        negative_aliases: Optional[List[str]] = None,
        source: str = "manual",
    ) -> Dict[str, Any]:
        group_key = re.sub(r"[^a-z0-9_]+", "_", _norm_text(key)).strip("_")
        if not group_key:
            group_key = re.sub(r"[^a-z0-9_]+", "_", _norm_text(label)).strip("_")
        if not group_key:
            raise ValueError("key or label is required")
        clean_label = str(label or key).strip()
        clean_aliases = []
        seen = set()
        for alias in [clean_label, *(aliases or [])]:
            value = re.sub(r"\s+", " ", str(alias or "").strip())
            norm = _norm_text(value)
            if not value or norm in seen:
                continue
            seen.add(norm)
            clean_aliases.append(value)
        clean_negative = [
            re.sub(r"\s+", " ", str(alias or "").strip())
            for alias in (negative_aliases or [])
            if str(alias or "").strip()
        ]
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO search_alias_groups (
                        key, label, negative_aliases_json, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        label=excluded.label,
                        negative_aliases_json=excluded.negative_aliases_json,
                        updated_at=excluded.updated_at
                    """,
                    (group_key, clean_label, json.dumps(clean_negative, ensure_ascii=False), now, now),
                )
                conn.execute("DELETE FROM search_aliases WHERE group_key=?", (group_key,))
                for alias in clean_aliases:
                    conn.execute(
                        """
                        INSERT INTO search_aliases (
                            group_key, alias, alias_norm, weight, source, status, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, 'active', ?)
                        """,
                        (group_key, alias, _norm_text(alias), 0.9 if alias == clean_label else 0.7, source or "manual", now),
                    )
        return {"key": group_key, "label": clean_label, "aliases": clean_aliases, "negative_aliases": clean_negative}

    def delete_search_alias_group(self, *, key: str) -> bool:
        group_key = str(key or "").strip()
        if not group_key:
            return False
        with self._lock:
            with self._connect() as conn:
                conn.execute("DELETE FROM search_aliases WHERE group_key=?", (group_key,))
                cur = conn.execute("DELETE FROM search_alias_groups WHERE key=?", (group_key,))
                return cur.rowcount > 0

    def expand_search_query(self, query: str, *, max_aliases: int = 24) -> Dict[str, Any]:
        query_norm = _norm_text(query)
        query_terms = set(re.findall(r"[a-zа-я0-9\-]{2,}", query_norm, flags=re.IGNORECASE))
        if not query_terms and not query_norm:
            return {"expanded_query": "", "aliases": [], "groups": []}
        groups = self.list_search_alias_groups()
        matched_groups: List[Dict[str, Any]] = []
        expanded_aliases: List[str] = []
        seen_aliases = set()
        for group in groups:
            aliases = [str(a.get("alias") or "") for a in group.get("aliases") or []]
            negative_norms = [_norm_text(x) for x in group.get("negative_aliases") or []]
            if any(neg and neg in query_norm for neg in negative_norms):
                continue
            hit = False
            for alias in aliases:
                alias_norm = _norm_text(alias)
                alias_terms = set(re.findall(r"[a-zа-я0-9\-]{2,}", alias_norm, flags=re.IGNORECASE))
                if not alias_norm or not alias_terms:
                    continue
                if alias_norm in query_norm or alias_terms.issubset(query_terms):
                    hit = True
                    break
            if not hit:
                continue
            matched_groups.append({"key": group["key"], "label": group["label"]})
            for alias in aliases:
                alias_norm = _norm_text(alias)
                if not alias_norm or alias_norm in seen_aliases or alias_norm in query_norm:
                    continue
                seen_aliases.add(alias_norm)
                expanded_aliases.append(alias)
                if len(expanded_aliases) >= max_aliases:
                    break
            if len(expanded_aliases) >= max_aliases:
                break
        expanded_query = " ".join([str(query or "").strip(), *expanded_aliases]).strip()
        return {"expanded_query": expanded_query, "aliases": expanded_aliases, "groups": matched_groups}

    def suggest_search_alias_candidates(self, *, limit: int = 30) -> List[Dict[str, Any]]:
        rows = self.fetch_dicts(
            """
            SELECT query, result_title, result_path, SUM(feedback) AS score, COUNT(*) AS hits
            FROM search_feedback
            WHERE feedback > 0 AND query <> '' AND result_path <> ''
            GROUP BY lower(query), result_path
            ORDER BY score DESC, hits DESC, MAX(ts) DESC
            LIMIT 200
            """
        )
        known_aliases = set()
        for group in self.list_search_alias_groups():
            for alias in group.get("aliases") or []:
                known_aliases.add(_norm_text(str(alias.get("alias") or "")))
        counter: Counter[tuple[str, str, str]] = Counter()
        samples: Dict[tuple[str, str, str], str] = {}
        for row in rows:
            query = _norm_text(str(row.get("query") or ""))
            path = str(row.get("result_path") or "")
            title = str(row.get("result_title") or "") or Path(path).stem
            bag = _norm_text(" ".join([title, path]))
            query_terms = set(re.findall(r"[a-zа-я0-9\-]{3,}", query, flags=re.IGNORECASE))
            tokens = [t for t in re.findall(r"[a-zа-я0-9\-]{3,}", bag, flags=re.IGNORECASE) if t not in query_terms]
            phrases = set(tokens[:16])
            words = re.findall(r"[a-zа-я0-9\-]{3,}", _norm_text(title), flags=re.IGNORECASE)
            for size in (2, 3):
                for idx in range(0, max(0, len(words) - size + 1)):
                    phrases.add(" ".join(words[idx : idx + size]))
            for phrase in phrases:
                if phrase in known_aliases or len(phrase) < 3:
                    continue
                key = (query, phrase, path)
                counter[key] += int(row.get("score") or 1)
                samples[key] = title
        out = []
        for (query, phrase, path), score in counter.most_common(max(1, int(limit))):
            out.append({"query": query, "candidate": phrase, "path": path, "title": samples.get((query, phrase, path), ""), "score": score})
        return out

    # ── fact ──────────────────────────────────────────────────────────

    def log_fact(
        self,
        *,
        source: str,
        question: str,
        ok: bool,
        answer: str,
        source_type: str,
        value_kg: Optional[int],
        duration_ms: int,
        error: str = "",
    ) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO fact_logs (
                        ts, source, question, ok, answer, source_type, value_kg, duration_ms, error
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        _utc_now(),
                        source or "unknown",
                        question or "",
                        1 if ok else 0,
                        answer or "",
                        source_type or "",
                        int(value_kg) if value_kg is not None else None,
                        int(duration_ms),
                        error or "",
                    ),
                )

    # ── app events ────────────────────────────────────────────────────

    def log_app_event(
        self,
        *,
        username: str,
        screen: str,
        feature: str,
        action: str,
        ok: bool = True,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO app_events (ts, username, screen, feature, action, ok, details_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        _utc_now(),
                        (username or "").strip().lower(),
                        screen or "",
                        feature or "",
                        action or "",
                        1 if ok else 0,
                        json.dumps(details or {}, ensure_ascii=False, sort_keys=True),
                    ),
                )

    # ── index runs ────────────────────────────────────────────────────

    def get_index_settings(self) -> Dict[str, Any]:
        defaults: Dict[str, Any] = {
            "schedule_enabled": False,
            "cadence": "daily",
            "time": "03:00",
            "days": ["Mon", "Tue", "Wed", "Thu", "Fri"],
            "stage": "all",
            "recreate": False,
            "workers": 4,
            "max_chunks": 2000,
            "skip_inline_ocr": False,
            "ocr_enabled": False,
            "ocr_min_text_len": 50,
        }
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT value_json FROM index_settings WHERE key='index_schedule'"
                ).fetchone()
        if row is None:
            return defaults
        try:
            saved = json.loads(str(row["value_json"] or "{}"))
        except json.JSONDecodeError:
            saved = {}
        if isinstance(saved, dict):
            defaults.update(saved)
        return defaults

    def save_index_settings(self, settings: Dict[str, Any]) -> Dict[str, Any]:
        current = self.get_index_settings()
        current.update(settings or {})
        stage = str(current.get("stage") or "all").strip().lower()
        if stage not in {"all", "metadata", "small", "large", "content"}:
            stage = "all"
        cadence = str(current.get("cadence") or "daily").strip().lower()
        if cadence not in {"manual", "hourly", "daily", "weekly"}:
            cadence = "daily"
        current["stage"] = stage
        current["cadence"] = cadence
        current["schedule_enabled"] = bool(current.get("schedule_enabled"))
        current["recreate"] = bool(current.get("recreate"))
        current["skip_inline_ocr"] = bool(current.get("skip_inline_ocr"))
        current["ocr_enabled"] = bool(current.get("ocr_enabled"))
        current["workers"] = max(1, min(32, int(current.get("workers") if current.get("workers") is not None else 4)))
        current["max_chunks"] = max(0, min(100_000, int(current.get("max_chunks") if current.get("max_chunks") is not None else 0)))
        current["ocr_min_text_len"] = max(1, min(100_000, int(current.get("ocr_min_text_len") if current.get("ocr_min_text_len") is not None else 50)))
        days = current.get("days")
        if not isinstance(days, list):
            days = []
        current["days"] = [str(day) for day in days if str(day).strip()]
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO index_settings (key, value_json, updated_at)
                    VALUES ('index_schedule', ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        value_json=excluded.value_json,
                        updated_at=excluded.updated_at
                    """,
                    (json.dumps(current, ensure_ascii=False, sort_keys=True), _utc_now()),
                )
        return current

    def start_index_run(
        self,
        *,
        catalog_path: str,
        collection_name: str,
        recreate: bool,
        note: str = "",
        worker_pid: int = 0,
    ) -> str:
        run_id = str(uuid.uuid4())
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO index_runs (
                        run_id, ts_started, status, worker_pid, catalog_path, collection_name, recreate, note
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        _utc_now(),
                        "running",
                        int(worker_pid),
                        catalog_path,
                        collection_name,
                        1 if recreate else 0,
                        note or "",
                    ),
                )
        return run_id

    def get_active_index_run(self) -> Optional[Dict[str, Any]]:
        """Вернуть активный индексный запуск (status='running') или None."""
        rows = self.fetch_dicts(
            "SELECT * FROM index_runs WHERE status='running' ORDER BY ts_started DESC LIMIT 1"
        )
        return rows[0] if rows else None

    def finalize_running_index_runs(
        self,
        *,
        status: str = "cancelled",
        note: str = "",
    ) -> int:
        """Завершить все зависшие index_runs со status='running'."""
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                run_rows = conn.execute(
                    "SELECT run_id FROM index_runs WHERE status='running'"
                ).fetchall()
                run_ids = [str(row["run_id"]) for row in run_rows if row["run_id"]]
                if not run_ids:
                    return 0
                placeholders = ",".join("?" for _ in run_ids)
                conn.execute(
                    f"""
                    UPDATE index_stage_progress
                    SET
                        ts_updated=?,
                        ts_finished=COALESCE(ts_finished, ?),
                        status=?
                    WHERE run_id IN ({placeholders}) AND status='running'
                    """,
                    (now, now, status, *run_ids),
                )
                conn.execute(
                    f"""
                    UPDATE index_runs
                    SET
                        ts_finished=COALESCE(ts_finished, ?),
                        status=?,
                        note=CASE
                            WHEN ? = '' THEN note
                            WHEN note IS NULL OR note = '' THEN ?
                            ELSE note || ' | ' || ?
                        END
                    WHERE run_id IN ({placeholders}) AND status='running'
                    """,
                    (now, status, note or "", note or "", note or "", *run_ids),
                )
                return len(run_ids)

    def start_stage(self, *, run_id: str, stage: str, total_files: int) -> None:
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO index_stage_progress (
                        run_id, stage, ts_started, ts_updated, status, total_files
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, stage) DO UPDATE SET
                        ts_updated=excluded.ts_updated,
                        status=excluded.status,
                        total_files=excluded.total_files
                    """,
                    (run_id, stage, now, now, "running", int(total_files)),
                )

    def update_stage(
        self,
        *,
        run_id: str,
        stage: str,
        processed_files: int,
        added_files: int,
        updated_files: int,
        skipped_files: int,
        error_files: int,
        points_added: int,
    ) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE index_stage_progress
                    SET
                        ts_updated=?,
                        processed_files=?,
                        added_files=?,
                        updated_files=?,
                        skipped_files=?,
                        error_files=?,
                        points_added=?
                    WHERE run_id=? AND stage=?
                    """,
                    (
                        _utc_now(),
                        int(processed_files),
                        int(added_files),
                        int(updated_files),
                        int(skipped_files),
                        int(error_files),
                        int(points_added),
                        run_id,
                        stage,
                    ),
                )

    def finish_stage(
        self,
        *,
        run_id: str,
        stage: str,
        status: str,
        processed_files: int,
        added_files: int,
        updated_files: int,
        skipped_files: int,
        error_files: int,
        points_added: int,
    ) -> None:
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE index_stage_progress
                    SET
                        ts_updated=?,
                        ts_finished=?,
                        status=?,
                        processed_files=?,
                        added_files=?,
                        updated_files=?,
                        skipped_files=?,
                        error_files=?,
                        points_added=?
                    WHERE run_id=? AND stage=?
                    """,
                    (
                        now,
                        now,
                        status,
                        int(processed_files),
                        int(added_files),
                        int(updated_files),
                        int(skipped_files),
                        int(error_files),
                        int(points_added),
                        run_id,
                        stage,
                    ),
                )

    def finish_index_run(
        self,
        *,
        run_id: str,
        status: str,
        total_files: int,
        added_files: int,
        updated_files: int,
        skipped_files: int,
        deleted_files: int,
        error_files: int,
        points_added: int,
        note: str = "",
    ) -> None:
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE index_runs
                    SET
                        ts_finished=?,
                        status=?,
                        total_files=?,
                        added_files=?,
                        updated_files=?,
                        skipped_files=?,
                        deleted_files=?,
                        error_files=?,
                        points_added=?,
                        note=CASE WHEN ? = '' THEN note ELSE ? END
                    WHERE run_id=?
                    """,
                    (
                        now,
                        status,
                        int(total_files),
                        int(added_files),
                        int(updated_files),
                        int(skipped_files),
                        int(deleted_files),
                        int(error_files),
                        int(points_added),
                        note or "",
                        note or "",
                        run_id,
                    ),
                )
                if status != "completed":
                    conn.execute(
                        """
                        UPDATE index_stage_progress
                        SET
                            ts_updated=?,
                            ts_finished=COALESCE(ts_finished, ?),
                            status=?
                        WHERE run_id=? AND status='running'
                        """,
                        (now, now, status, run_id),
                    )

    # ── OCR runs ──────────────────────────────────────────────────────

    def start_ocr_run(
        self,
        *,
        collection_name: str = "",
        found_scanned: int = 0,
        note: str = "",
        worker_pid: int = 0,
    ) -> str:
        """Создать запись о начале OCR-прохода. Возвращает ocr_run_id."""
        ocr_run_id = str(uuid.uuid4())
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO ocr_runs (
                        ocr_run_id, ts_started, ts_updated, status,
                        worker_pid, collection_name, found_scanned, processed_pdfs, note
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ocr_run_id, now, now, "running",
                        int(worker_pid),
                        collection_name or "",
                        int(found_scanned),
                        0,
                        note or "",
                    ),
                )
        return ocr_run_id

    def update_ocr_progress(
        self,
        *,
        ocr_run_id: str,
        found_scanned: Optional[int] = None,
        processed_pdfs: Optional[int] = None,
        index_run_id: Optional[str] = None,
        note: Optional[str] = None,
    ) -> None:
        """Обновить прогресс OCR-прохода."""
        sets = ["ts_updated=?"]
        params: List[Any] = [_utc_now()]
        if found_scanned is not None:
            sets.append("found_scanned=?")
            params.append(int(found_scanned))
        if processed_pdfs is not None:
            sets.append("processed_pdfs=?")
            params.append(int(processed_pdfs))
        if index_run_id is not None:
            sets.append("index_run_id=?")
            params.append(index_run_id)
        if note is not None:
            sets.append("note=?")
            params.append(note)
        params.append(ocr_run_id)
        with self._lock:
            with self._connect() as conn:
                # f-строка безопасна: `sets` содержит только хардкоженные имена
                # столбцов (константы выше), все значения идут через параметры (?).
                # Не добавляйте сюда user-controlled строки!
                conn.execute(
                    f"UPDATE ocr_runs SET {', '.join(sets)} WHERE ocr_run_id=?",
                    params,
                )

    def finish_ocr_run(
        self,
        *,
        ocr_run_id: str,
        status: str,
        processed_pdfs: int = 0,
        note: str = "",
    ) -> None:
        """Завершить OCR-проход (status: 'completed', 'failed', 'cancelled')."""
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE ocr_runs
                    SET ts_updated=?, ts_finished=?, status=?, processed_pdfs=?,
                        note=CASE WHEN ? = '' THEN note ELSE ? END
                    WHERE ocr_run_id=?
                    """,
                    (now, now, status, int(processed_pdfs), note or "", note or "", ocr_run_id),
                )

    def finalize_running_ocr_runs(
        self,
        *,
        status: str = "cancelled",
        note: str = "",
    ) -> int:
        """Завершить все зависшие ocr_runs со status='running'."""
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                run_rows = conn.execute(
                    "SELECT ocr_run_id FROM ocr_runs WHERE status='running'"
                ).fetchall()
                run_ids = [str(row["ocr_run_id"]) for row in run_rows if row["ocr_run_id"]]
                if not run_ids:
                    return 0
                placeholders = ",".join("?" for _ in run_ids)
                conn.execute(
                    f"""
                    UPDATE ocr_runs
                    SET
                        ts_updated=?,
                        ts_finished=COALESCE(ts_finished, ?),
                        status=?,
                        note=CASE
                            WHEN ? = '' THEN note
                            WHEN note IS NULL OR note = '' THEN ?
                            ELSE note || ' | ' || ?
                        END
                    WHERE ocr_run_id IN ({placeholders}) AND status='running'
                    """,
                    (now, now, status, note or "", note or "", note or "", *run_ids),
                )
                return len(run_ids)

    def get_active_ocr_run(self) -> Optional[Dict[str, Any]]:
        """Вернуть активный OCR-проход (status='running') или None."""
        rows = self.fetch_dicts(
            "SELECT * FROM ocr_runs WHERE status='running' ORDER BY ts_started DESC LIMIT 1"
        )
        return rows[0] if rows else None

    def get_last_ocr_runs(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Вернуть последние N OCR-проходов."""
        return self.fetch_dicts(
            "SELECT * FROM ocr_runs ORDER BY ts_started DESC LIMIT ?",
            [int(limit)],
        )

    # ── index schedules ───────────────────────────────────────────────

    def list_index_schedules(self) -> List[Dict[str, Any]]:
        """Вернуть все расписания индексации, отсортированные по времени создания."""
        rows = self.fetch_dicts(
            "SELECT * FROM index_schedules ORDER BY created_at"
        )
        for row in rows:
            try:
                row["days"] = json.loads(str(row.get("days_json") or "[]"))
            except json.JSONDecodeError:
                row["days"] = []
        return rows

    def save_index_schedule(
        self,
        *,
        id: Optional[str] = None,
        label: str = "",
        enabled: bool = True,
        cadence: str = "daily",
        time: str = "03:00",
        days: Optional[List[str]] = None,
        stage: str = "all",
    ) -> Dict[str, Any]:
        """Создать или обновить расписание. Возвращает сохранённую запись."""
        sched_id = str(id or uuid.uuid4())
        clean_cadence = cadence if cadence in {"hourly", "daily", "weekly"} else "daily"
        clean_stage = stage if stage in {"all", "metadata", "small", "large", "content"} else "all"
        clean_days = [str(d) for d in (days or []) if str(d).strip()]
        now = _utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO index_schedules (id, label, enabled, cadence, time, days_json, stage, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        label=excluded.label,
                        enabled=excluded.enabled,
                        cadence=excluded.cadence,
                        time=excluded.time,
                        days_json=excluded.days_json,
                        stage=excluded.stage,
                        updated_at=excluded.updated_at
                    """,
                    (
                        sched_id,
                        str(label or "").strip(),
                        1 if enabled else 0,
                        clean_cadence,
                        str(time or "03:00").strip(),
                        json.dumps(clean_days, ensure_ascii=False),
                        clean_stage,
                        now,
                        now,
                    ),
                )
        return {
            "id": sched_id, "label": label, "enabled": enabled,
            "cadence": clean_cadence, "time": time, "days": clean_days,
            "stage": clean_stage,
        }

    def delete_index_schedule(self, *, id: str) -> bool:
        """Удалить расписание по id. Возвращает True если запись была найдена."""
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute("DELETE FROM index_schedules WHERE id=?", (str(id),))
                return cur.rowcount > 0

    def touch_index_schedule(self, *, id: str) -> None:
        """Обновить last_run_at расписания на текущее UTC-время."""
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "UPDATE index_schedules SET last_run_at=? WHERE id=?",
                    (_utc_now(), str(id)),
                )

    def get_daily_index_stats(self, *, days: int = 30) -> List[Dict[str, Any]]:
        """Статистика индексации по дням за последние N дней."""
        return self.fetch_dicts(
            """
            SELECT
                date(ts_started) AS day,
                stage,
                SUM(processed_files) AS files,
                SUM(added_files) AS added,
                SUM(points_added) AS points,
                COUNT(*) AS runs,
                CAST(SUM(
                    CAST((julianday(COALESCE(ts_finished, ts_updated)) - julianday(ts_started)) * 86400 AS INTEGER)
                ) AS INTEGER) AS total_sec
            FROM index_stage_progress
            WHERE ts_started >= date('now', ?)
              AND ts_finished IS NOT NULL
            GROUP BY date(ts_started), stage
            ORDER BY day, stage
            """,
            [f"-{int(days)} days"],
        )

    # ── generic ───────────────────────────────────────────────────────

    def fetch_dicts(self, query: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(query, params or [])
                return [dict(r) for r in cur.fetchall()]
