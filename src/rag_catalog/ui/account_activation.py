"""One-time account setup, without exposing invitation secrets in URLs sent to the server."""

from __future__ import annotations

import json
import secrets
from collections.abc import Callable
from urllib.parse import urlsplit

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.concurrency import run_in_threadpool

from rag_catalog.core.user_auth_db import UserAuthDB

_PAGE = """<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Активация аккаунта | RAG Cloud</title>
<style nonce="__NONCE__">
*{box-sizing:border-box}body{margin:0;background:#f5f7fa;color:#18212e;font:16px system-ui,sans-serif}
main{max-width:440px;margin:64px auto;padding:24px}h1{font-size:24px}label{display:block;margin:20px 0 8px}
input,button{width:100%;font:inherit;padding:12px;border:1px solid #8795a5;border-radius:6px}
button{margin-top:24px;background:#1269b0;color:white;cursor:pointer}button:disabled{opacity:.6}
#message{line-height:1.5;overflow-wrap:anywhere}a{color:#1269b0}header{font-weight:700;color:#1269b0}
</style></head><body><main><header>RAG Cloud</header><h1>Установите пароль</h1>
<form id="activation">
<label for="password">Новый пароль (8–128 символов)</label>
<input id="password" type="password" autocomplete="new-password" minlength="8" maxlength="128" required>
<label for="confirmation">Повторите пароль</label>
<input id="confirmation" type="password" autocomplete="new-password" minlength="8" maxlength="128" required>
<button id="submit" type="submit">Сохранить пароль</button></form>
<p id="message" role="status" aria-live="polite"></p><a id="login" href="/" hidden>Перейти ко входу</a>
</main><script nonce="__NONCE__">
(() => {
  let token = '';
  const form = document.getElementById('activation');
  const button = document.getElementById('submit');
  const message = document.getElementById('message');
  const password = document.getElementById('password');
  const confirmation = document.getElementById('confirmation');
  function loadInvitation() {
    token = new URLSearchParams(location.hash.slice(1)).get('token') || '';
    history.replaceState(null, '', location.pathname);
    form.hidden = !/^[A-Za-z0-9_-]{43}$/.test(token);
    password.value = ''; confirmation.value = '';
    document.getElementById('login').hidden = true;
    message.textContent = form.hidden ? 'Откройте полную ссылку активации, полученную от администратора.' : '';
  }
  loadInvitation();
  window.addEventListener('hashchange', loadInvitation);
  form.addEventListener('submit', async event => {
    event.preventDefault();
    if (password.value !== confirmation.value) {
      message.textContent = 'Пароли не совпадают.';
      return;
    }
    button.disabled = true;
    message.textContent = '';
    try {
      const response = await fetch('/api/auth/activate', {
        method: 'POST', credentials: 'omit', cache: 'no-store',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({token, new_password: password.value})
      });
      const result = await response.json();
      if (!response.ok) {
        message.textContent = result.detail || 'Не удалось активировать аккаунт.';
        return;
      }
      token = ''; password.value = ''; confirmation.value = ''; form.hidden = true;
      message.textContent = 'Пароль установлен. Ваш логин: ' + result.username;
      document.getElementById('login').hidden = false;
    } catch {
      message.textContent = 'Нет ответа сервера. Если пароль уже сохранён, попробуйте войти с ним.';
      document.getElementById('login').hidden = false;
    } finally { button.disabled = false; }
  });
})();
</script></body></html>"""

_HEADERS = {"Cache-Control": "no-store", "Referrer-Policy": "no-referrer", "X-Content-Type-Options": "nosniff"}


def register_activation_routes(app: FastAPI, auth_db: Callable[[], UserAuthDB]) -> None:
    @app.get("/auth/activate", include_in_schema=False)
    def activation_page() -> HTMLResponse:
        nonce = secrets.token_urlsafe(24)
        return HTMLResponse(_PAGE.replace("__NONCE__", nonce), headers={
            **_HEADERS,
            "Content-Security-Policy": (
                f"default-src 'none'; script-src 'nonce-{nonce}'; style-src 'nonce-{nonce}'; "
                "connect-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'"
            ),
        })

    @app.post("/api/auth/activate", include_in_schema=False)
    async def activate(request: Request) -> JSONResponse:
        def error(detail: str, status: int = 400) -> JSONResponse:
            return JSONResponse({"detail": detail}, status_code=status, headers=_HEADERS)

        origin = request.headers.get("origin")
        if origin:
            try:
                allowed_origin = urlsplit(origin).netloc == request.headers.get("host")
            except ValueError:
                allowed_origin = False
            if not allowed_origin:
                return error("Недопустимый источник запроса.", 403)
        if request.headers.get("content-type", "").split(";")[0].strip() != "application/json":
            return error("Ожидается JSON.", 415)
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > 8192:
                return error("Запрос слишком большой.", 413)
        try:
            data = json.loads(body)
        except (ValueError, UnicodeError):
            return error("Некорректный запрос.")
        if not isinstance(data, dict):
            return error("Некорректный запрос.")

        def complete() -> dict:
            return auth_db().activate_account(token=data.get("token"), new_password=data.get("new_password"))

        result = await run_in_threadpool(complete)
        if not result["ok"]:
            if result["reason"] == "invalid_password":
                return error("Пароль должен содержать от 8 до 128 символов и не состоять из пробелов.")
            return error("Ссылка недействительна, уже использована или истекла. Обратитесь к администратору.")
        return JSONResponse(result, headers=_HEADERS)
