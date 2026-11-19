"""Authenticated webhook ingress and a replayable local inbox for the employee bot."""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import sqlite3
import time
from dataclasses import asdict
from pathlib import Path
from typing import Callable

from fastapi import HTTPException, Request
from filelock import FileLock, Timeout

from rag_catalog.core.sqlite_runtime import ClosingConnection, cached_database

from .max_api import MaxAPIError, MaxClient, MaxEvent, MaxFile, normalize_update

logger = logging.getLogger(__name__)
_MAX_BODY = 1024 * 1024
_RETENTION = 7 * 86400
_ATTEMPT_LIMIT = 16


class MaxInbox:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS max_inbox (
                    seq INTEGER PRIMARY KEY,
                    bot TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    payload TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    available REAL NOT NULL DEFAULT 0,
                    finished REAL,
                    UNIQUE(bot, event_id)
                );
                CREATE INDEX IF NOT EXISTS max_inbox_pending
                    ON max_inbox(bot, user_id, seq) WHERE finished IS NULL;
                CREATE INDEX IF NOT EXISTS max_inbox_finished
                    ON max_inbox(finished) WHERE finished IS NOT NULL;
            """)

    def _connect(self):
        return sqlite3.connect(self.path, factory=ClosingConnection, timeout=5)

    def enqueue(self, bot: str, event: MaxEvent) -> None:
        with self._connect() as db:
            db.execute("DELETE FROM max_inbox WHERE finished < ?", (time.time() - _RETENTION,))
            db.execute("""INSERT INTO max_inbox(bot, event_id, user_id, payload)
                          VALUES (?, ?, ?, ?) ON CONFLICT(bot, event_id) DO NOTHING""",
                       (bot, event.event_id, event.user_id, json.dumps(asdict(event), ensure_ascii=False)))

    def deliver_one(self, bot: str, handle: Callable[[MaxEvent], None]) -> bool:
        # The OS releases this lock on process death. Neither HTTP requests nor
        # business operations hold a SQLite transaction while the worker runs.
        lock = FileLock(str(self.path) + ".worker.lock")
        try:
            lock.acquire(timeout=0)
        except Timeout:
            return False
        try:
            with self._connect() as db:
                row = db.execute("""
                    SELECT seq, payload, attempts FROM max_inbox AS e
                    WHERE bot=? AND finished IS NULL AND available<=? AND NOT EXISTS (
                        SELECT 1 FROM max_inbox AS earlier WHERE earlier.bot=e.bot
                        AND earlier.user_id=e.user_id AND earlier.seq<e.seq AND earlier.finished IS NULL
                    ) ORDER BY seq LIMIT 1
                """, (bot, time.time())).fetchone()
            if not row:
                return False
            seq, payload, attempts = row
            try:
                fields = json.loads(payload)
                fields["files"] = tuple(MaxFile(**item) for item in fields["files"])
                handle(MaxEvent(**fields))
            except Exception as exc:
                attempts += 1
                # Do not log any event fields, signed links, token or raw exceptions.
                logger.warning("max_delivery_failed seq=%s attempt=%s error=%s", seq, attempts, type(exc).__name__)
                if attempts < _ATTEMPT_LIMIT:
                    with self._connect() as db:
                        db.execute("UPDATE max_inbox SET attempts=?, available=? WHERE seq=?",
                                   (attempts, time.time() + min(1800, 2 ** attempts), seq))
                    return True
                logger.error("max_delivery_exhausted seq=%s", seq)
            with self._connect() as db:
                db.execute("UPDATE max_inbox SET payload=NULL, finished=?, attempts=? WHERE seq=?",
                           (time.time(), attempts, seq))
            return True
        finally:
            lock.release()


def _configuration(load_config):
    from .max_settings import read_settings

    cfg = load_config()
    return cfg, read_settings(cfg)


def _handle(cfg, settings, get_auth_db, event):
    from .max_workflows import MaxWorkflows

    client = MaxClient(settings.token)
    if event.callback_id:
        try:
            client.answer_callback(event.callback_id)
        except MaxAPIError:
            # The notification is transient and can expire on a replay.
            pass
    workflows = MaxWorkflows({**cfg, "max_office_url": settings.office_url}, get_auth_db(cfg), client)
    workflows.handle(event)


def register_max_webhook(app, load_config, get_auth_db):
    """Register before the app starts. Settings may be enabled after startup."""
    task = None
    wake = asyncio.Event()

    def dispatch_one():
        cfg, settings = _configuration(load_config)
        if settings is None:
            return None
        inbox = cached_database(MaxInbox, settings.inbox_path)
        return inbox.deliver_one(settings.bot_link, lambda event: _handle(cfg, settings, get_auth_db, event))

    async def run():
        while True:
            wake.clear()
            try:
                worked = await asyncio.to_thread(dispatch_one)
            except Exception as exc:
                logger.error("max_inbox_unavailable error=%s", type(exc).__name__)
                worked = False
            if worked:
                continue
            try:
                await asyncio.wait_for(wake.wait(), timeout=None if worked is None else 2)
            except asyncio.TimeoutError:
                pass

    async def start():
        nonlocal task, wake
        if task is None or task.done():
            wake = asyncio.Event()
            task = asyncio.create_task(run(), name="max-employee-inbox")

    async def stop():
        nonlocal task
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            task = None

    if hasattr(app, "on_startup"):  # NiceGUI has its own lifespan.
        app.on_startup(start)
        app.on_shutdown(stop)
    else:
        app.router.add_event_handler("startup", start)
        app.router.add_event_handler("shutdown", stop)

    @app.post("/api/max/webhook", include_in_schema=False)
    async def max_webhook(request: Request):
        _, settings = await asyncio.to_thread(_configuration, load_config)
        if settings is None or not settings.secret:
            raise HTTPException(404)
        supplied = request.headers.getlist("X-Max-Bot-Api-Secret")
        if len(supplied) != 1 or not secrets.compare_digest(supplied[0].encode(), settings.secret.encode()):
            raise HTTPException(403)
        data = bytearray()
        async for chunk in request.stream():
            if len(data) + len(chunk) > _MAX_BODY:
                raise HTTPException(413)
            data.extend(chunk)
        try:
            update = json.loads(data)
        except (ValueError, UnicodeError, RecursionError):
            raise HTTPException(400) from None
        if not isinstance(update, dict) or not isinstance(update.get("update_type"), str):
            raise HTTPException(400)
        event = normalize_update(update)
        if event:
            def enqueue():
                cached_database(MaxInbox, settings.inbox_path).enqueue(settings.bot_link, event)

            await asyncio.to_thread(enqueue)
            await start()
            wake.set()
        return {"ok": True}
