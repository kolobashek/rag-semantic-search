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
HEALTH_FRESH_SECONDS = 90


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
            db.execute('''CREATE TABLE IF NOT EXISTS client_updates (
                client_id TEXT PRIMARY KEY, target_version TEXT NOT NULL,
                requested_by TEXT NOT NULL, requested_at REAL NOT NULL,
                reported_version TEXT NOT NULL DEFAULT '', checked_at REAL NOT NULL DEFAULT 0,
                completed_at REAL NOT NULL DEFAULT 0)''')
            db.execute('''CREATE TABLE IF NOT EXISTS client_health (
                client_id TEXT PRIMARY KEY, last_seen_at REAL NOT NULL,
                app_version TEXT NOT NULL, phase TEXT NOT NULL,
                state TEXT NOT NULL, last_error TEXT NOT NULL)''')
            db.execute('''CREATE TABLE IF NOT EXISTS client_recoveries (
                client_id TEXT PRIMARY KEY, request_id TEXT NOT NULL,
                requested_by TEXT NOT NULL, requested_at REAL NOT NULL,
                switched_at REAL NOT NULL DEFAULT 0)''')

    def request_recovery(self, client_id: str, requested_by: str) -> dict:
        with self._connect() as db:
            db.execute('''INSERT INTO client_recoveries VALUES (?,?,?,?,0)
                          ON CONFLICT(client_id) DO UPDATE SET request_id=excluded.request_id,
                          requested_by=excluded.requested_by, requested_at=excluded.requested_at, switched_at=0
                          WHERE client_recoveries.switched_at>0 OR client_recoveries.requested_at<?''',
                       (client_id, uuid.uuid4().hex, requested_by, time.time(), time.time() - 86400))
        return self.read_recovery(client_id)

    def read_recovery(self, client_id: str) -> dict:
        with self._connect() as db:
            db.execute('DELETE FROM client_recoveries WHERE MAX(requested_at, switched_at) < ?',
                       (time.time() - RETENTION_SECONDS,))
            row = db.execute('SELECT * FROM client_recoveries WHERE client_id=?', (client_id,)).fetchone()
        return dict(row) if row else {}

    def poll_recovery(self, client_id: str, root_key: str) -> str:
        with self._connect() as db:
            db.execute('''UPDATE client_recoveries SET switched_at=?
                          WHERE client_id=? AND request_id=? AND switched_at=0''',
                       (time.time(), client_id, root_key))
        row = self.read_recovery(client_id)
        return row['request_id'] if row and not row['switched_at'] and row['requested_at'] >= time.time() - 86400 else ''

    def heartbeat(self, client_id: str, *, app_version: str, phase: str, state: str, last_error: str) -> None:
        with self._connect() as db:
            self._prune(db)
            db.execute('''INSERT INTO client_health VALUES (?,?,?,?,?,?)
                          ON CONFLICT(client_id) DO UPDATE SET
                          last_seen_at=excluded.last_seen_at, app_version=excluded.app_version,
                          phase=excluded.phase, state=excluded.state, last_error=excluded.last_error''',
                       (client_id, time.time(), app_version[:40], phase[:40], state[:40],
                        clean_log(last_error)[:1000]))

    def read_health(self, client_id: str) -> dict:
        with self._connect() as db:
            self._prune(db)
            row = db.execute('SELECT * FROM client_health WHERE client_id=?', (client_id,)).fetchone()
        health = dict(row) if row else {'last_seen_at': 0, 'app_version': '', 'phase': '',
                                       'state': '', 'last_error': ''}
        age = time.time() - health['last_seen_at']
        health['fresh'] = bool(health['last_seen_at'] and 0 <= age <= HEALTH_FRESH_SECONDS)
        return health

    def request_update(self, client_id: str, requested_by: str, version: str) -> dict:
        with self._connect() as db:
            db.execute('''INSERT INTO client_updates(client_id,target_version,requested_by,requested_at)
                          VALUES (?,?,?,?) ON CONFLICT(client_id) DO UPDATE SET
                          target_version=excluded.target_version, requested_by=excluded.requested_by,
                          requested_at=excluded.requested_at, completed_at=0''',
                       (client_id, version, requested_by, time.time()))
        return self.read_update(client_id)

    def read_update(self, client_id: str) -> dict:
        with self._connect() as db:
            row = db.execute('SELECT * FROM client_updates WHERE client_id=?', (client_id,)).fetchone()
        return dict(row) if row else {}

    def poll_update(self, client_id: str, version: str) -> dict:
        with self._connect() as db:
            row = db.execute('SELECT * FROM client_updates WHERE client_id=?', (client_id,)).fetchone()
            if row:
                def parts(value):
                    return tuple(int(p) for p in value.split('.')) if re.fullmatch(r'\d+\.\d+\.\d+', value) else ()
                done = bool(parts(version) and parts(version) >= parts(row['target_version']))
                db.execute('''UPDATE client_updates SET reported_version=?, checked_at=?,
                              completed_at=CASE WHEN ? THEN ? ELSE completed_at END WHERE client_id=?''',
                           (version[:40], time.time(), done, time.time(), client_id))
        return self.read_update(client_id)

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
        db.execute('DELETE FROM client_health WHERE last_seen_at < ?', (time.time() - RETENTION_SECONDS,))
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
