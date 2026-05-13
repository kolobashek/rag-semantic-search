"""Device Authorization Grant (RFC 8628-inspired) — in-memory code store."""
from __future__ import annotations

import secrets
import threading
import time
from typing import Any, Dict

_store: Dict[str, Dict[str, Any]] = {}  # device_code → record
_lock = threading.Lock()

_CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # excludes O, I, 0, 1
EXPIRE_SECONDS = 300
POLL_INTERVAL = 5


def _user_code() -> str:
    def half() -> str:
        return "".join(secrets.choice(_CODE_CHARS) for _ in range(4))

    return f"{half()}-{half()}"


def _cleanup() -> None:
    now = time.monotonic()
    dead = [k for k, v in _store.items() if v["expires_at"] < now]
    for k in dead:
        del _store[k]


def create_code(server_base: str) -> Dict[str, Any]:
    _cleanup()
    device_code = secrets.token_urlsafe(24)
    base = server_base.rstrip("/")
    with _lock:
        existing_ucs = {v["user_code"] for v in _store.values()}
        uc = _user_code()
        while uc in existing_ucs:
            uc = _user_code()
        _store[device_code] = {
            "user_code": uc,
            "status": "pending",   # pending | approved | denied | expired
            "token": None,
            "username": None,
            "server_base": base,
            "expires_at": time.monotonic() + EXPIRE_SECONDS,
        }
    verify_url = f"{base}/auth/device"
    return {
        "device_code": device_code,
        "user_code": uc,
        "verification_uri": verify_url,
        "verification_uri_complete": f"{verify_url}?code={uc}",
        "expires_in": EXPIRE_SECONDS,
        "interval": POLL_INTERVAL,
    }


def approve_code(user_code: str, token: str, username: str) -> bool:
    clean = str(user_code or "").strip().upper().replace(" ", "")
    with _lock:
        for rec in _store.values():
            if rec["user_code"].upper().replace("-", "") == clean.replace("-", ""):
                if rec["status"] != "pending" or rec["expires_at"] < time.monotonic():
                    rec["status"] = "expired"
                    return False
                rec.update(status="approved", token=token, username=username)
                return True
    return False


def poll_token(device_code: str) -> Dict[str, Any]:
    with _lock:
        rec = _store.get(str(device_code or ""))
        if rec is None:
            return {"status": "not_found"}
        if rec["status"] == "pending" and rec["expires_at"] < time.monotonic():
            rec["status"] = "expired"
        approved = rec["status"] == "approved"
        return {
            "status": rec["status"],
            "token": rec["token"] if approved else None,
            "username": rec["username"] if approved else None,
            "server": rec.get("server_base") if approved else None,
        }
