"""Bounded, first-party product analytics. No document or form contents are stored."""

from __future__ import annotations

import atexit
import logging
import math
import queue
import re
import sqlite3
import threading
import time
import uuid
from collections import defaultdict
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path

SCREENS = {"search", "explorer", "shifts", "jobs", "index", "settings", "stats"}
KINDS = {"click", "page_view", "timing", "error", "operation"}
CONTROL_ACTIONS = {
    "search.submit", "files.open_file", "files.open_folder", "files.download", "files.share",
    "files.copy", "files.cut", "files.delete", "files.archive", "files.hide", "files.unhide",
    "files.favorite", "files.upload", "files.versions", "files.rename", "files.move",
    "control.refresh", "control.more", "control.close", "control.save", "control.cancel",
    "control.create", "control.edit", "control.filter", "control.select", "control.help",
    "control.theme", "control.logout", "control.tab", "control.expand", "control.button",
    "control.checkbox", "control.menu", "control.previous", "control.next", "control.approve",
    "control.reject", "client.download",
} | {f"navigation.{screen}" for screen in SCREENS}
CONTROL_ACTIONS |= {f"shifts.{action}" for action in (
    "create", "edit", "save", "approve", "submit", "revert", "delete", "history", "export", "manage")}
BROWSER_ACTIONS = {
    "click": CONTROL_ACTIONS,
    "page_view": {"page.open"},
    "timing": {"page.ready", "page.response", "page.transition", "interaction.response", "files.preview_response"},
    "error": {"browser.javascript", "browser.promise", "connection.lost"},
}
MAX_BATCH = 50
MAX_BODY = 32768
RETENTION_DAYS = 90
MAX_ROWS = 200_000
_NAME = re.compile(r"^[a-z][a-z0-9_.]{0,79}$")
_ID = re.compile(r"^[a-zA-Z0-9-]{8,64}$")
logger = logging.getLogger(__name__)


def analytics_path(cfg: dict) -> str:
    base = cfg.get("usage_analytics_db_path")
    if base:
        return str(base)
    telemetry = cfg.get("telemetry_db_path")
    parent = Path(telemetry).parent if telemetry else Path(cfg.get("qdrant_db_path") or "data")
    return str(parent / "usage_analytics.sqlite3")


def clean_event(event: dict, *, browser: bool = False) -> dict:
    """Only copy typed, bounded fields; discard arbitrary details and client identity."""
    if not isinstance(event, dict):
        raise ValueError("Event must be an object")
    kind, action, screen = (event.get(key) for key in ("kind", "action", "screen"))
    if not all(isinstance(value, str) for value in (kind, action, screen)):
        raise ValueError("Invalid event")
    if kind not in KINDS or screen not in SCREENS or not _NAME.fullmatch(action):
        raise ValueError("Invalid event")
    if browser and action not in BROWSER_ACTIONS.get(kind, set()):
        raise ValueError("Unsupported browser action")
    duration = event.get("duration_ms")
    if duration is not None:
        if isinstance(duration, bool) or not isinstance(duration, (int, float)):
            raise ValueError("Invalid duration")
        if not math.isfinite(duration) or not 0 <= duration <= 3_600_000:
            raise ValueError("Invalid duration")
        duration = round(duration, 1)
    if kind == "timing" and duration is None:
        raise ValueError("Timing needs a duration")
    if kind not in {"timing", "operation"}:
        duration = None
    event_id = event.get("event_id", uuid.uuid4().hex)
    session = event.get("session_id", "")
    if not isinstance(event_id, str) or not _ID.fullmatch(event_id):
        raise ValueError("Invalid event id")
    if not isinstance(session, str) or (session and not _ID.fullmatch(session)):
        raise ValueError("Invalid session id")
    age = event.get("age_ms", 0) if browser else 0
    if isinstance(age, bool) or not isinstance(age, (int, float)) or not math.isfinite(age) or not 0 <= age <= 300_000:
        raise ValueError("Invalid event age")
    return dict(event_id=event_id, session_id=session, screen=screen, kind=kind, action=action,
                duration_ms=duration, ok=kind != "error" and event.get("ok") is not False,
                source="browser" if browser else "server", ts=time.time() - age / 1000)


class UsageAnalyticsDB:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS usage_events (
                    id INTEGER PRIMARY KEY, event_id TEXT NOT NULL, ts REAL NOT NULL,
                    username TEXT NOT NULL, session_id TEXT NOT NULL, source TEXT NOT NULL,
                    screen TEXT NOT NULL, kind TEXT NOT NULL, action TEXT NOT NULL,
                    duration_ms REAL, ok INTEGER NOT NULL,
                    UNIQUE(username, source, event_id)
                );
                CREATE INDEX IF NOT EXISTS usage_ts ON usage_events(ts);
                CREATE INDEX IF NOT EXISTS usage_user_ts ON usage_events(username, ts);
            """)

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.path, timeout=0.25)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA max_page_count=32768")
        conn.execute("PRAGMA wal_autocheckpoint=256")
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def append(self, username: str, events: list[dict]) -> int:
        username = str(username).strip().lower()[:128]
        if not username:
            return 0
        now = time.time()
        with self.connect() as conn:
            before = conn.total_changes
            conn.executemany("""INSERT OR IGNORE INTO usage_events
                (event_id, ts, username, session_id, source, screen, kind, action, duration_ms, ok)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", [
                (e["event_id"], e["ts"], username, e["session_id"], e["source"], e["screen"],
                 e["kind"], e["action"], e["duration_ms"], int(e["ok"])) for e in events
            ])
            stored = conn.total_changes - before
            self._prune(conn, now)
        return stored

    @staticmethod
    def _prune(conn, now: float):
        conn.execute("DELETE FROM usage_events WHERE ts < ?", (now - RETENTION_DAYS * 86400,))
        conn.execute("DELETE FROM usage_events WHERE id <= "
                     "(SELECT id FROM usage_events ORDER BY id DESC LIMIT 1 OFFSET ?)", (MAX_ROWS,))
        conn.execute("PRAGMA incremental_vacuum(64)")

    def prune(self):
        with self.connect() as conn:
            self._prune(conn, time.time())

    def report(self, *, days: int = 7, username: str = "", screen: str = "") -> dict:
        if not 1 <= days <= RETENTION_DAYS or (screen and screen not in SCREENS):
            raise ValueError("Invalid report filters")
        self.prune()
        where = "ts >= ? AND ts <= ?"
        now = time.time()
        args: list = [now - days * 86400, now]
        for column, value in (("username", username.strip().lower()), ("screen", screen)):
            if value:
                where += f" AND {column} = ?"
                args.append(value)
        with self.connect() as conn:
            def rows(sql):
                return [dict(row) for row in conn.execute(sql, args)]

            overview = rows(f"""SELECT COUNT(*) AS events, COUNT(DISTINCT username) AS users,
                COALESCE(SUM(kind='click'), 0) AS clicks,
                COALESCE(SUM(kind='page_view'), 0) AS views,
                COALESCE(SUM(ok=0), 0) AS errors,
                MIN(ts) AS first_event FROM usage_events WHERE {where}""")[0]
            users = rows(f"""SELECT username, SUM(kind='click') AS clicks,
                SUM(kind='page_view') AS views, SUM(kind='operation') AS operations,
                SUM(ok=0) AS errors, MAX(ts) AS last_seen
                FROM usage_events WHERE {where} GROUP BY username ORDER BY clicks DESC, views DESC""")
            actions = rows(f"""SELECT screen, action, kind, COUNT(*) AS count,
                COUNT(DISTINCT username) AS users, SUM(ok=0) AS errors
                FROM usage_events WHERE {where} AND kind IN ('click','operation')
                GROUP BY screen, action, kind ORDER BY count DESC LIMIT 50""")
            timings = rows(f"""WITH ranked AS (
                SELECT source, screen, action, duration_ms,
                  ROW_NUMBER() OVER (PARTITION BY source, screen, action ORDER BY duration_ms) AS rn,
                  COUNT(*) OVER (PARTITION BY source, screen, action) AS n
                FROM usage_events WHERE {where} AND duration_ms IS NOT NULL AND ok=1
            ) SELECT source, screen, action, COUNT(*) AS count, ROUND(AVG(duration_ms), 1) AS avg_ms,
                ROUND(AVG(CASE WHEN rn IN ((n+1)/2, (n+2)/2) THEN duration_ms END), 1) AS p50_ms,
                MIN(CASE WHEN rn >= (n * 95 + 99) / 100 THEN duration_ms END) AS p95_ms,
                MAX(duration_ms) AS max_ms
                FROM ranked GROUP BY source, screen, action ORDER BY p95_ms DESC LIMIT 50""")
            daily = rows(f"""SELECT strftime('%Y-%m-%d', ts, 'unixepoch') AS day,
                SUM(kind='click') AS clicks, SUM(kind='page_view') AS views, SUM(ok=0) AS errors
                FROM usage_events WHERE {where} GROUP BY day ORDER BY day""")
            recent = rows(f"""SELECT id, ts, username, screen, kind, action, source, duration_ms, ok
                FROM usage_events WHERE {where} ORDER BY ts DESC, id DESC LIMIT 200""")
            choices = [row[0] for row in conn.execute("SELECT DISTINCT username FROM usage_events ORDER BY username")]
        return dict(overview=overview, users=users, actions=actions, timings=timings, daily=daily,
                    recent=recent, usernames=choices, days=days, retention_days=RETENTION_DAYS, max_rows=MAX_ROWS)


class UsageCollector:
    """One bounded queue per database; web callbacks never wait for SQLite."""
    def __init__(self, path: str):
        self.path = path
        self.pending: queue.Queue = queue.Queue(maxsize=2048)
        self.dropped = 0
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, name="usage-analytics", daemon=True)
        self.thread.start()
        atexit.register(self.close)

    def submit(self, username: str, events: list[dict]) -> int:
        accepted = 0
        for event in events:
            try:
                self.pending.put_nowait((username, event))
                accepted += 1
            except queue.Full:
                self.dropped += 1
        return accepted

    def _run(self):
        db = None
        last_prune = 0.0
        while not self.stop.is_set() or not self.pending.empty():
            group = defaultdict(list)
            try:
                user, event = self.pending.get(timeout=1)
                group[user].append(event)
            except queue.Empty:
                pass
            while sum(map(len, group.values())) < 100:
                try:
                    user, event = self.pending.get_nowait()
                    group[user].append(event)
                except queue.Empty:
                    break
            try:
                db = db or UsageAnalyticsDB(self.path)
                for user, events in group.items():
                    db.append(user, events)
                if time.monotonic() - last_prune >= 3600:
                    db.prune()
                    last_prune = time.monotonic()
            except Exception:
                self.dropped += sum(map(len, group.values()))
                logger.warning("Usage analytics storage unavailable", exc_info=True)
                self.stop.wait(5)
            finally:
                for _ in range(sum(map(len, group.values()))):
                    self.pending.task_done()

    def close(self):
        self.stop.set()
        self.thread.join(timeout=2)


@lru_cache(maxsize=8)
def collector(path: str) -> UsageCollector:
    return UsageCollector(path)


def record_app_event(cfg: dict, username: str, screen: str, feature: str, action: str,
                     ok: bool, details: dict) -> None:
    screen = {"search": "search", "cd_explorer": "explorer", "explorer": "explorer", "shifts": "shifts"}.get(feature, screen)
    if not username or cfg.get("usage_analytics_enabled", True) is False or screen not in SCREENS:
        return
    if feature == "diagnostics" and action == "render_done":
        kind = "timing"
    elif feature == "search":
        if action not in {"run_render_final", "run_exact_name_complete", "run_quick", "run_quick_timeout",
                          "run_full", "export_csv", "open_result", "toggle_ai_expand"}:
            return
        kind = "timing" if action in {"run_quick", "run_full", "run_quick_timeout"} else "operation"
        if action in {"run_render_final", "run_exact_name_complete"} or (action == "run_quick" and details.get("numeric_exact")):
            action, kind = "complete", "operation"
        ok = ok and not details.get("has_error", False)
    elif feature in {"cd_explorer", "explorer", "favorites", "saved_search", "ui", "shifts"}:
        background = (feature == "shifts" and action in {"list", "users", "references"}) or action.startswith("folder_sizes_")
        kind = "timing" if background else "operation"
    else:
        return
    try:
        event = clean_event(dict(kind=kind, screen=screen, action=f"{feature}.{action}",
                                duration_ms=details.get("duration_ms"), ok=ok))
        collector(analytics_path(cfg)).submit(username, [event])
    except Exception:
        logger.debug("Usage event skipped", exc_info=True)
