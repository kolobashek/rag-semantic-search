"""Waybill scans kept in the shift journal with the same row-level access checks."""

import ntpath
import re
from datetime import datetime, timezone
from pathlib import PureWindowsPath

from .work_shifts import WorkShiftJournal

MAX_FILE_BYTES = 16 * 1024 * 1024
_COLUMNS = "id, shift_id, filename, length(content) AS size_bytes, created_at, uploaded_by"


def safe_filename(value: str) -> str:
    name = PureWindowsPath(str(value)).name
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f\x7f]', "_", name).strip(" .")
    name = name.encode("utf-8", errors="replace")[:200].decode("utf-8", errors="ignore").rstrip(" .")
    reserved = ntpath.isreserved(name) if hasattr(ntpath, "isreserved") else PureWindowsPath(name).is_reserved()
    if not name or reserved:
        return "document" + PureWindowsPath(name).suffix
    return name


class ShiftAttachments:
    def __init__(self, journal: WorkShiftJournal):
        self.journal = journal
        with journal._connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS shift_attachments (
                    id INTEGER PRIMARY KEY,
                    shift_id INTEGER NOT NULL REFERENCES shifts(id),
                    filename TEXT NOT NULL,
                    content BLOB NOT NULL,
                    uploaded_by TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE (uploaded_by, event_id)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS shift_attachments_shift ON shift_attachments(shift_id)")

    def list(self, token: str, shift_id: int) -> list[dict]:
        user = self.journal._user(token)
        with self.journal._connection() as conn:
            conn.execute("BEGIN")
            self.journal._row(conn, user, shift_id)
            return [dict(row) for row in conn.execute(
                f"SELECT {_COLUMNS} FROM shift_attachments WHERE shift_id=? ORDER BY id", (shift_id,)
            )]

    def read(self, token: str, shift_id: int, attachment_id: int) -> dict:
        user = self.journal._user(token)
        with self.journal._connection() as conn:
            conn.execute("BEGIN")
            self.journal._row(conn, user, shift_id)
            row = conn.execute(
                f"SELECT {_COLUMNS}, content FROM shift_attachments WHERE shift_id=? AND id=?",
                (shift_id, attachment_id),
            ).fetchone()
            if not row:
                raise PermissionError("Вложение недоступно")
            return dict(row)

    def add(self, token: str, shift_id: int, event_id: str, filename: str, content: bytes) -> int:
        user = self.journal._user(token)
        if not isinstance(content, bytes) or not 0 < len(content) <= MAX_FILE_BYTES:
            raise ValueError("Путевой лист должен быть непустым файлом размером до 16 МБ")
        if not event_id or len(event_id) > 256:
            raise ValueError("Отсутствует идентификатор вложения")
        key = (user["username"], event_id)
        with self.journal._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self.journal._row(conn, user, shift_id)
            row = conn.execute(
                "SELECT id, shift_id FROM shift_attachments WHERE uploaded_by=? AND event_id=?", key
            ).fetchone()
            if row:
                if row["shift_id"] != shift_id:
                    raise ValueError("Этот файл уже приложен к другой смене")
                return row["id"]
            return conn.execute("""
                INSERT INTO shift_attachments (shift_id, filename, content, uploaded_by, event_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (shift_id, safe_filename(filename), content, *key, datetime.now(timezone.utc).isoformat())).lastrowid
