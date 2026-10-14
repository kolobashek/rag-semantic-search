"""Authenticated ingestion and admin-only usage reports."""

from __future__ import annotations

import json
import threading
import time
from collections import OrderedDict
from urllib.parse import urlsplit

from fastapi import Header, HTTPException, Request
from nicegui import app, run

from rag_catalog.core.rag_core import load_config
from rag_catalog.core.usage_analytics import (
    MAX_BATCH,
    MAX_BODY,
    UsageAnalyticsDB,
    analytics_path,
    clean_event,
    collector,
)
from rag_catalog.core.user_auth_db import UserAuthDB

from .state import _users_db_path

_buckets: OrderedDict = OrderedDict()
_bucket_lock = threading.Lock()


def session_token(authorization: str = "") -> str:
    token = authorization.removeprefix("Bearer ").strip()
    if not token:
        try:
            token = str(app.storage.user.get("auth_token") or "")
        except RuntimeError:
            token = ""
    return token


def require_user(cfg: dict, authorization: str = "", *, admin: bool = False) -> dict:
    token = session_token(authorization)
    user = UserAuthDB(str(_users_db_path(cfg))).get_user_by_session(token) if token else None
    if not user:
        raise HTTPException(401, "Требуется авторизация.")
    if user.get("status") != "active" or (admin and user.get("role") != "admin"):
        raise HTTPException(403, "Недостаточно прав.")
    return user


def _rate_limit(username: str, count: int):
    now = time.monotonic()
    with _bucket_lock:
        since, used = _buckets.get(username, (now, 0))
        if now - since >= 60:
            since, used = now, 0
        if used + count > 240:
            raise HTTPException(429, "Слишком много событий.", headers={"Retry-After": "60"})
        _buckets[username] = (since, used + count)
        _buckets.move_to_end(username)
        while len(_buckets) > 512:
            _buckets.popitem(last=False)


@app.post("/api/usage/events")
async def usage_events(request: Request, authorization: str = Header(default="")) -> dict:
    cfg = load_config()
    user = await run.io_bound(require_user, cfg, session_token(authorization))
    origin = request.headers.get("origin", "")
    if (request.headers.get("sec-fetch-site") == "cross-site"
            or (origin and urlsplit(origin).netloc.lower() != request.headers.get("host", "").lower())):
        raise HTTPException(403, "Недопустимый источник.")
    if cfg.get("usage_analytics_enabled", True) is False:
        return {"accepted": 0, "enabled": False}
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > MAX_BODY:
            raise HTTPException(413, "Слишком большой пакет.")
        body.extend(chunk)
    try:
        payload = json.loads(body)
        events = payload["events"]
        if not isinstance(events, list) or not 1 <= len(events) <= MAX_BATCH:
            raise ValueError("Invalid batch")
        cleaned = [clean_event(event, browser=True) for event in events]
    except (ValueError, KeyError, TypeError):
        raise HTTPException(400, "Некорректные события.") from None
    _rate_limit(user["username"], len(cleaned))
    accepted = collector(analytics_path(cfg)).submit(user["username"], cleaned)
    return {"accepted": accepted, "enabled": True}


@app.get("/api/usage/report")
async def usage_report(days: int = 7, username: str = "", screen: str = "",
                       authorization: str = Header(default="")) -> dict:
    cfg = load_config()
    await run.io_bound(require_user, cfg, session_token(authorization), admin=True)
    try:
        report = await run.io_bound(lambda: UsageAnalyticsDB(analytics_path(cfg)).report(
            days=days, username=username, screen=screen))
    except ValueError:
        raise HTTPException(400, "Некорректный фильтр.") from None
    report["enabled"] = cfg.get("usage_analytics_enabled", True) is not False
    report["dropped"] = collector(analytics_path(cfg)).dropped
    return report


def start_usage_collection():
    cfg = load_config()
    if cfg.get("usage_analytics_enabled", True) is not False:
        collector(analytics_path(cfg))


app.on_startup(start_usage_collection)
