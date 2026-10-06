"""Latest bounded client log snapshots and durable administrator requests."""
from __future__ import annotations

import re
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

from .client_logs import redact_secrets

MAX_LOG_BYTES = 256 * 1024
RETENTION_SECONDS = 14 * 86400


def clean_log(text: str) -> str:
    value = redact_secrets(text)
    value = re.sub(r'(?i)([?&](?:device_code|code|x-amz-[a-z-]+)=)[^&\s]+', r'\1<redacted>', value)
    value = re.sub(r'(?i)(\bcode\s+)[A-Z0-9]{4}-[A-Z0-9]{4}', r'\1<redacted>', value)
    return value.encode('utf-8')[-MAX_LOG_BYTES:].decode('utf-8', errors='ignore')


class ClientDiagnosticsDB:
    def __init__(self, path: str):
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.execute('PRAGMA auto_vacuum=INCREMENTAL')
            db.execute('''CREATE TABLE IF NOT EXISTS client_diagnostics (
                client_id TEXT PRIMARY KEY, request_id TEXT NOT NULL DEFAULT '',
                requested_by TEXT NOT NULL DEFAULT '', requested_at REAL NOT NULL DEFAULT 0,
                uploaded_at REAL NOT NULL DEFAULT 0, app_version TEXT NOT NULL DEFAULT '',
                log_text TEXT NOT NULL DEFAULT '')''')

    @classmethod
    def from_config(cls, cfg):
        return cls(str(cfg['cloud_drive_db_path']) + '.diagnostics.sqlite3')

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def _prune(db):
        db.execute('DELETE FROM client_diagnostics WHERE MAX(requested_at, uploaded_at) < ?',
                   (time.time() - RETENTION_SECONDS,))
        db.execute('PRAGMA incremental_vacuum(64)')

    def read(self, client_id: str) -> dict:
        with self._connect() as db:
            self._prune(db)
            row = db.execute('SELECT * FROM client_diagnostics WHERE client_id=?', (client_id,)).fetchone()
        return dict(row) if row else {'client_id': client_id, 'request_id': '', 'requested_at': 0,
                                    'uploaded_at': 0, 'app_version': '', 'log_text': ''}

    def request(self, client_id: str, requested_by: str) -> dict:
        with self._connect() as db:
            self._prune(db)
            db.execute('INSERT OR IGNORE INTO client_diagnostics(client_id) VALUES (?)', (client_id,))
            # Repeated button clicks reuse the pending request, so an upload cannot be starved.
            db.execute('''UPDATE client_diagnostics SET request_id=?, requested_by=?, requested_at=?
                          WHERE client_id=? AND request_id='' ''',
                       (uuid.uuid4().hex, requested_by, time.time(), client_id))
        return self.read(client_id)

    def submit(self, client_id: str, text: str, *, request_id: str = '', app_version: str = '') -> None:
        text = clean_log(text)
        with self._connect() as db:
            self._prune(db)
            db.execute('INSERT OR IGNORE INTO client_diagnostics(client_id) VALUES (?)', (client_id,))
            db.execute('''UPDATE client_diagnostics SET log_text=?, uploaded_at=?, app_version=?,
                          request_id=CASE WHEN request_id=? THEN '' ELSE request_id END
                          WHERE client_id=?''',
                       (text, time.time(), app_version[:40], request_id, client_id))
