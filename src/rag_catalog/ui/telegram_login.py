"""Finish Telegram web sign-in in the initiating browser, independently of its UI socket."""

import secrets
from datetime import datetime, timezone
from urllib.parse import urlsplit

from fastapi import Request
from nicegui import app
from starlette.concurrency import run_in_threadpool
from starlette.responses import Response

WAITS = "telegram_login_waits"


def remember_telegram_login(challenge: dict) -> str:
    # Only the short-lived receipt reaches sessionStorage. The independent
    # verifier stays in the initiating browser's server-side cookie storage.
    receipt = secrets.token_urlsafe(24)
    storage = app.storage.user
    now = datetime.now(timezone.utc).isoformat()
    waits = {key: value for key, value in storage.get(WAITS, {}).items() if value["expires_at"] > now}
    waits[receipt] = {"verifier": challenge["verifier"], "expires_at": challenge["expires_at"]}
    storage[WAITS] = waits
    return receipt


def register_telegram_login_route(application, auth_db):
    @application.api_route("/api/auth/telegram/check", methods=["POST", "DELETE"], include_in_schema=False)
    async def check_telegram_login(request: Request):
        def reply(status):
            return Response(status_code=status, headers={"Cache-Control": "no-store"})

        origin = request.headers.get("origin", "")
        try:
            source = urlsplit(origin)
            valid_origin = (source.scheme == request.url.scheme and source.netloc == request.headers.get("host")
                            and request.headers.get("sec-fetch-site", "same-origin") == "same-origin")
        except ValueError:
            valid_origin = False
        if not valid_origin:
            return reply(403)
        receipt = request.headers.get("x-rag-login-request", "")
        storage = app.storage.user
        wait = storage.get(WAITS, {}).get(receipt)
        if not wait or wait["expires_at"] <= datetime.now(timezone.utc).isoformat():
            return reply(410)
        if request.method == "DELETE":
            storage[WAITS] = {key: value for key, value in storage[WAITS].items() if key != receipt}
            return reply(204)
        if wait.get("done"):
            return reply(200 if storage.get("auth_token") else 410)

        def check():
            return auth_db().consume_confirmed_telegram_login(verifier=wait["verifier"], browser_session=True)

        result = await run_in_threadpool(check)
        token = result.get("session_token")
        if receipt not in storage.get(WAITS, {}) or (token and storage.get("auth_token") not in (None, "", token)):
            if token and storage.get("auth_token") != token:
                await run_in_threadpool(lambda: auth_db().revoke_session(token))
            return reply(410)
        if result.get("reason") == "pending":
            return reply(202)
        if not token:
            return reply(410)
        storage["auth_token"] = token
        # Other pending tabs must not overwrite the newly authenticated session.
        storage[WAITS] = {receipt: {"expires_at": wait["expires_at"], "done": True}}
        return reply(200)
