"""Accept a MAX credential once from a browser on the same workstation."""

import hmac
import secrets
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs

from .max_api import MaxAPIError
from .max_settings import configure


def serve(cfg, office_url, bot_link, *, ready=print, lifetime=1800):
    path = "/" + secrets.token_urlsafe(32)
    csrf = secrets.token_urlsafe(32)
    page = f"""<!doctype html><html lang="ru"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Подключение бота MAX</title><main><h1>Подключение бота MAX</h1>
<p>Скопируйте токен из карточки бота «Кабинет сотрудника» в MAX для бизнеса.
Вставьте его в поле ниже на этом компьютере.</p>
<form method="post" autocomplete="off"><input type="hidden" name="csrf" value="{csrf}">
<label>Токен бота <input type="password" name="token" required autocomplete="off"></label>
<button>Подключить бота</button></form></main></html>"""
    connected = False

    class Form(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass  # The local one-time URL and submitted credential are private.

        def reply(self, status, body):
            data = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("Content-Security-Policy", "default-src 'none'; form-action 'self'; frame-ancestors 'none'")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def expected(self):
            return self.headers.get("Host") == host and self.path == path

        def do_GET(self):
            self.reply(200, page) if self.expected() else self.reply(404, "")

        def do_POST(self):
            nonlocal connected
            if not self.expected() or self.headers.get("Origin") != "http://" + host:
                return self.reply(403, "")
            length = self.headers.get("Content-Length", "")
            if (self.headers.get_content_type() != "application/x-www-form-urlencoded"
                    or not length.isdecimal() or int(length) > 8192):
                return self.reply(400, "")
            form = parse_qs(self.rfile.read(int(length)).decode("ascii", "replace"))
            if not hmac.compare_digest(form.get("csrf", [""])[0].encode(), csrf.encode()):
                return self.reply(403, "")
            try:
                configure(cfg, office_url, bot_link, form.get("token", [""])[0].strip())
            except (OSError, ValueError, MaxAPIError):
                return self.reply(400, page.replace("<form ", "<p>Проверьте токен выбранного бота и подключение к MAX.</p><form "))
            connected = True
            self.reply(200, "Бот подключён. Вернитесь в чат.")

    with HTTPServer(("127.0.0.1", 0), Form) as server:
        host = "127.0.0.1:" + str(server.server_port)
        ready("http://" + host + path)
        deadline = time.monotonic() + lifetime
        server.timeout = 1
        while not connected and time.monotonic() < deadline:
            server.handle_request()
    return connected
