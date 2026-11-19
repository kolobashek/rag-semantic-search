"""Select one shift for the next uncaptioned attachment in this MAX binding."""

import hashlib
import time

from rag_catalog.core.work_shifts import WorkShiftJournal


class MaxWaybillRequests:
    def __init__(self, journal: WorkShiftJournal):
        self.journal = journal
        with journal._connection() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS max_waybill_requests (
                    prompt_id TEXT PRIMARY KEY, binding TEXT NOT NULL,
                    shift_id INTEGER NOT NULL REFERENCES shifts(id), expires REAL NOT NULL, file_event TEXT,
                    UNIQUE(binding, file_event)
                );
                CREATE INDEX IF NOT EXISTS max_waybill_pending ON max_waybill_requests(binding, expires)
                    WHERE file_event IS NULL;
                CREATE INDEX IF NOT EXISTS max_waybill_expiry ON max_waybill_requests(expires);
            """)

    def select(self, binding: str, shift_id: int, event_id: str) -> tuple[str, bool]:
        user = self.journal._user(binding)
        prompt = hashlib.sha256(event_id.encode()).hexdigest()
        now = time.time()
        with self.journal._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            self.journal._row(db, user, shift_id)
            db.execute("DELETE FROM max_waybill_requests WHERE expires<?", (now - 7 * 86400,))
            old = db.execute("SELECT * FROM max_waybill_requests WHERE prompt_id=?", (prompt,)).fetchone()
            if old:
                return prompt, old["binding"] == binding and old["file_event"] is None and old["expires"] > now
            db.execute("UPDATE max_waybill_requests SET expires=? WHERE binding=? AND file_event IS NULL AND expires>?",
                       (now, binding, now))
            db.execute("INSERT INTO max_waybill_requests(prompt_id,binding,shift_id,expires) VALUES(?,?,?,?)",
                       (prompt, binding, shift_id, now + 600))
        return prompt, True

    def claim(self, binding: str, file_event: str) -> int | None:
        user = self.journal._user(binding)
        with self.journal._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            claimed = db.execute("SELECT shift_id FROM max_waybill_requests WHERE binding=? AND file_event=?",
                                 (binding, file_event)).fetchone()
            if claimed:
                self.journal._row(db, user, claimed["shift_id"])
                return claimed["shift_id"]
            pending = db.execute("""SELECT prompt_id,shift_id FROM max_waybill_requests
                WHERE binding=? AND file_event IS NULL AND expires>?""", (binding, time.time())).fetchone()
            if not pending:
                return None
            self.journal._row(db, user, pending["shift_id"])
            db.execute("UPDATE max_waybill_requests SET file_event=? WHERE prompt_id=?",
                       (file_event, pending["prompt_id"]))
            return pending["shift_id"]

    def cancel(self, binding: str, prompt: str):
        with self.journal._connection() as db:
            db.execute("UPDATE max_waybill_requests SET expires=? WHERE binding=? AND prompt_id=? AND file_event IS NULL",
                       (time.time(), binding, prompt))
