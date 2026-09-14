"""Notify administrators once per failed/stale indexing run, without starting jobs."""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

from .heartbeat import describe_heartbeat, is_stale, read_heartbeat

logger = logging.getLogger(__name__)


class IndexHeartbeatMonitor:
    def __init__(self, cfg, auth_db, send):
        self.cfg = cfg
        self.auth_db = auth_db
        self.send = send
        root = Path(str(cfg.get("qdrant_db_path") or "data"))
        self.heartbeat_path = Path(str(cfg.get("indexer_heartbeat_path") or root / "indexer_heartbeat.json"))
        if not self.heartbeat_path.is_absolute():
            self.heartbeat_path = Path(__file__).resolve().parents[4] / self.heartbeat_path
        self.receipts_path = root / "indexer_alert_receipts.json"
        self.next_check = 0.0
        try:
            receipts = json.loads(self.receipts_path.read_text(encoding="utf-8"))
            self.receipts = dict(receipts) if isinstance(receipts, dict) else {}
        except (OSError, ValueError):
            self.receipts = {}

    def poll(self):
        if not self.cfg.get("index_alerts_enabled", True) or time.monotonic() < self.next_check:
            return
        self.next_check = time.monotonic() + 60
        heartbeat = read_heartbeat(self.heartbeat_path)
        if not heartbeat:
            return
        status = str(heartbeat.get("status") or "running")
        if status == "running" and is_stale(heartbeat):
            status = "stale"
        if status not in {"failed", "stale"}:
            return
        identity = f"{heartbeat.get('run_id') or heartbeat.get('pid')}:{heartbeat.get('stage')}:{status}"
        restriction = str(self.cfg.get("telegram_allowed_chat_id") or "").strip()
        for user in self.auth_db.list_users():
            chat_id = str(user.get("telegram_chat_id") or "").strip()
            if (user.get("role") != "admin" or user.get("status") != "active"
                    or not chat_id or (restriction and chat_id != restriction)):
                continue
            key = f"{identity}:{chat_id}"
            if key in self.receipts:
                continue
            try:
                self.send(chat_id, "Индексация требует внимания.\n" + describe_heartbeat(heartbeat))
            except Exception:
                logger.warning("Index alert delivery failed; will retry", exc_info=True)
                continue
            self.receipts[key] = time.time()
            self.receipts = dict(sorted(self.receipts.items(), key=lambda pair: pair[1])[-200:])
            try:
                self.receipts_path.parent.mkdir(parents=True, exist_ok=True)
                tmp = self.receipts_path.with_suffix(f".{os.getpid()}.tmp")
                tmp.write_text(json.dumps(self.receipts), encoding="utf-8")
                os.replace(tmp, self.receipts_path)
            except OSError:
                logger.warning("Could not persist index alert receipts", exc_info=True)
