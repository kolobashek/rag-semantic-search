"""Transport for the MAX employee bot. API credentials are confined to the API host."""

from __future__ import annotations

import os
import re
import ssl
import time
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path, PureWindowsPath
from tempfile import NamedTemporaryFile
from typing import Any, BinaryIO
from urllib.parse import urljoin, urlsplit
from uuid import uuid4

import requests
from urllib3.fields import RequestField

from .employee_onboarding import OnboardingContact

API_BASE = "https://platform-api2.max.ru"
UPDATE_TYPES = ("bot_started", "message_created", "message_callback")
_TEXT_LIMIT = 4000
_BLOCK_SIZE = 256 * 1024


class MaxAPIError(RuntimeError):
    """Safe to log without leaking signed URLs, message contents or tokens."""

    def __init__(self, message: str, *, status: int = 0, code: str = ""):
        super().__init__(message)
        self.status = status
        self.code = code


@dataclass(frozen=True)
class MaxFile:
    name: str
    url: str = field(repr=False)
    size: int | None = None
    kind: str = "file"


@dataclass(frozen=True)
class MaxEvent:
    user_id: int
    event_id: str
    text: str = field(default="", repr=False)
    files: tuple[MaxFile, ...] = ()
    callback_id: str | None = None
    display_name: str = ""
    contact: OnboardingContact | None = field(default=None, repr=False)


def _object(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _string(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _message_files(body: dict) -> tuple[MaxFile, ...]:
    result = []
    attachments = body.get("attachments")
    if not isinstance(attachments, list):
        return ()
    for raw in attachments:
        attachment = _object(raw)
        kind = attachment.get("type")
        url = _string(_object(attachment.get("payload")).get("url"))
        if kind not in ("image", "file") or not url:
            continue
        filename = PureWindowsPath(_string(attachment.get("filename"))).name
        name = filename if filename not in ("", ".", "..") else ("photo.jpg" if kind == "image" else "document")
        size = attachment.get("size")
        result.append(MaxFile(name, url, size if type(size) is int and size >= 0 else None, kind))
    return tuple(result)


def _message_contact(body: dict) -> OnboardingContact | None:
    attachments = body.get("attachments")
    if not isinstance(attachments, list):
        return None
    for item in attachments:
        if _object(item).get("type") != "contact":
            continue
        payload = _object(item.get("payload"))
        info = _object(payload.get("max_info"))
        uid = info.get("user_id")
        name = " ".join(filter(None, (_string(info.get("first_name")), _string(info.get("last_name")))))
        phone = ""
        vcard = re.sub(r"\r?\n[ \t]", "", _string(payload.get("vcf_info"))[:8192])
        for line in vcard.splitlines():
            label, _, value = line.partition(":")
            prop = label.split(";")[0].rsplit(".", 1)[-1].upper()
            if prop == "FN" and not name:
                name = value.replace(r"\,", ",").replace(r"\;", ";")
            elif prop == "TEL" and not phone:
                phone = value.removeprefix("tel:")[:64]
        return OnboardingContact(name=name[:150], sender_id=str(uid) if type(uid) is int and 0 < uid < 2**63 else "",
                                 phone=phone)
    return None


def normalize_update(update: dict) -> MaxEvent | None:
    """Only direct contact with a human may reach the employee workflows."""
    kind = _object(update).get("update_type")
    if kind not in UPDATE_TYPES:
        return None
    files = ()
    contact = None
    callback_id = None
    if kind == "bot_started":
        user = _object(update.get("user"))
        timestamp = update.get("timestamp")
        if type(timestamp) is not int:
            return None
        identifier = str(timestamp)
        text = ("/start " + _string(update.get("payload"))).rstrip()
    else:
        message = _object(update.get("message"))
        if _object(message.get("recipient")).get("chat_type") != "dialog":
            return None
        if kind == "message_callback":
            callback = _object(update.get("callback"))
            user = _object(callback.get("user"))
            identifier = callback_id = _string(callback.get("callback_id"))
            text = _string(callback.get("payload"))
        else:
            user = _object(message.get("sender"))
            body = _object(message.get("body"))
            identifier = _string(body.get("mid"))
            text = _string(body.get("text"))
            files = _message_files(body)
            contact = _message_contact(body)
            link = _object(message.get("link"))
            if contact is None and link.get("type") == "forward":
                forwarded = _object(link.get("message"))
                contact = _message_contact(forwarded)
                # MAX can omit the wrapper body when the message is only a forward.
                # Keep the actor from message.sender; the forwarded sender is unrelated.
                if not identifier and contact and type(message.get("timestamp")) is int and forwarded.get("mid"):
                    identifier = f"forward:{message['timestamp']}:{_string(forwarded['mid'])}"
    uid = user.get("user_id")
    if not identifier or type(uid) is not int or not 0 < uid < 2**63 or user.get("is_bot"):
        return None
    name = " ".join(filter(None, (_string(user.get("first_name")), _string(user.get("last_name")))))
    return MaxEvent(uid, f"{kind}:{uid}:{identifier}", text, files, callback_id, name, contact)


def _media_url(url: str) -> str:
    try:
        parts = urlsplit(url)
        host = parts.hostname or ""
        valid = (
            parts.scheme == "https" and parts.port in (None, 443)
            and parts.username is None and parts.password is None
            and host.endswith((".oneme.ru", ".okcdn.ru", ".max.ru"))
            and "\\" not in url and not any(ord(char) < 32 for char in url)
        )
    except ValueError:
        valid = False
    if not valid:
        raise MaxAPIError("Недопустимый адрес медиафайла MAX", code="media.invalid")
    return url


@lru_cache(maxsize=1)
def _system_tls():
    return ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)


class _MaxTLSAdapter(requests.adapters.HTTPAdapter):
    def build_connection_pool_key_attributes(self, request, verify, cert=None):
        host, options = super().build_connection_pool_key_attributes(request, verify, cert)
        if verify is True:
            # The Windows trust store can contain the issuer used by the MAX API.
            options["ssl_context"] = _system_tls()
        return host, options


def _request(method: str, url: str, *, operation: str, **kwargs) -> requests.Response:
    # Explicit auth prevents requests from silently using unrelated .netrc credentials.
    try:
        with requests.Session() as session:
            session.mount("https://", _MaxTLSAdapter())
            return session.request(method, url, allow_redirects=False, auth=lambda req: req, **kwargs)
    except requests.RequestException:
        raise MaxAPIError(f"Сетевая ошибка MAX при операции {operation}") from None


def _check_status(response: requests.Response, operation: str) -> None:
    if 200 <= response.status_code < 300:
        return
    try:
        code = _string(_object(response.json()).get("code"))
    except ValueError:
        code = ""
    raise MaxAPIError(f"MAX {operation}: HTTP {response.status_code}", status=response.status_code, code=code)


def _json(response: requests.Response, operation: str) -> dict:
    _check_status(response, operation)
    try:
        data = response.json()
    except ValueError:
        data = None
    if not isinstance(data, dict) or data.get("success") is False:
        raise MaxAPIError(f"MAX отклонил операцию {operation} или вернул некорректный ответ")
    return data


class _MultipartFile:
    """Stream one multipart field with Content-Length, including for large documents."""

    def __init__(self, stream: BinaryIO, filename: str):
        self.stream = stream
        self.boundary = uuid4().hex
        part = RequestField(name="data", data=None, filename=filename)
        part.make_multipart(content_type="application/octet-stream")
        self.header = f"--{self.boundary}\r\n{part.render_headers()}".encode()
        self.footer = f"\r\n--{self.boundary}--\r\n".encode()

    def __len__(self):
        return len(self.header) + os.fstat(self.stream.fileno()).st_size + len(self.footer)

    def __iter__(self):
        yield self.header
        while block := self.stream.read(_BLOCK_SIZE):
            yield block
        yield self.footer


class MaxClient:
    def __init__(self, token: str):
        if not token or not token.strip():
            raise ValueError("Не задан токен MAX-бота")
        self._token = token.strip()

    def _api(self, method: str, endpoint: str, *, timeout: int = 30, **kwargs) -> dict:
        for attempt in range(3):
            with _request(method, API_BASE + endpoint, operation=endpoint,
                          headers={"Authorization": self._token}, timeout=(10, timeout), **kwargs) as response:
                if response.status_code != 429 or attempt == 2:
                    return _json(response, endpoint)
                delay = response.headers.get("Retry-After", "")
                if not delay.isascii() or not delay.isdecimal() or len(delay) > 2:
                    delay = 2 ** attempt
                if int(delay) > 10:
                    _check_status(response, endpoint)
            time.sleep(max(1, int(delay)))
        raise AssertionError("unreachable")

    def get_me(self) -> dict:
        return self._api("GET", "/me")

    def subscriptions(self) -> list[dict]:
        entries = self._api("GET", "/subscriptions").get("subscriptions")
        if not isinstance(entries, list) or not all(isinstance(entry, dict) for entry in entries):
            raise MaxAPIError("Некорректный список подписок MAX")
        return [{key: value for key, value in entry.items() if key in ("url", "time", "update_types")}
                for entry in entries]

    def subscribe(self, url: str, secret: str) -> None:
        parsed = urlsplit(url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.port is not None or parsed.username is not None:
            raise ValueError("Webhook MAX требует HTTPS-адрес без явного порта")
        if not re.fullmatch(r"[A-Za-z0-9_-]{5,256}", secret):
            raise ValueError("Недопустимый формат секрета webhook MAX")
        self._api("POST", "/subscriptions", json={"url": url, "secret": secret, "update_types": list(UPDATE_TYPES)})

    def get_updates(self, marker: int | None = None, timeout: int = 25) -> tuple[list[dict], int | None]:
        params = {"timeout": timeout}
        if marker is not None:
            params["marker"] = marker
        result = self._api("GET", "/updates", params=params, timeout=timeout + 10)
        updates = result.get("updates")
        next_marker = result.get("marker")
        if not isinstance(updates, list) or (next_marker is not None and type(next_marker) is not int):
            raise MaxAPIError("Некорректный ответ MAX при чтении обновлений")
        return updates, next_marker

    def _send(self, user_id: int, text: str, attachments: list[dict]) -> str:
        data = self._api("POST", "/messages", params={"user_id": user_id, "disable_link_preview": "true"},
                         json={"text": text or None, "attachments": attachments})
        mid = _string(_object(_object(data.get("message")).get("body")).get("mid"))
        if not mid:
            raise MaxAPIError("MAX не вернул идентификатор отправленного сообщения")
        return mid

    def send_message(self, user_id: int, text: str, *, buttons: list[list[dict]] | None = None) -> str:
        keyboard = [{"type": "inline_keyboard", "payload": {"buttons": buttons}}] if buttons else []
        while len(text) > _TEXT_LIMIT:
            self._send(user_id, text[:_TEXT_LIMIT], [])
            text = text[_TEXT_LIMIT:]
        return self._send(user_id, text, keyboard)

    def answer_callback(self, callback_id: str, text: str = "Принято") -> None:
        self._api("POST", "/answers", params={"callback_id": callback_id}, json={"notification": text})

    def send_document(self, user_id: int, path: str | Path, caption: str = "") -> str:
        path = Path(path)
        with path.open("rb") as source:
            location = self._api("POST", "/uploads", params={"type": "file"})
            url = _media_url(_string(location.get("url")))
            body = _MultipartFile(source, path.name)
            headers = {"Content-Type": f"multipart/form-data; boundary={body.boundary}"}
            with _request("POST", url, operation="загрузка", data=body, headers=headers, timeout=(10, 120)) as resp:
                token = _string(_json(resp, "загрузка").get("token"))
        if not token:
            raise MaxAPIError("MAX не вернул токен загруженного файла")
        attachments = [{"type": "file", "payload": {"token": token}}]
        for attempt in range(5):
            try:
                return self._send(user_id, caption[:_TEXT_LIMIT], attachments)
            except MaxAPIError as exc:
                if exc.code != "attachment.not.ready" or attempt == 4:
                    raise
                time.sleep(2 ** attempt)
        raise AssertionError("unreachable")

    def download_file(self, attachment: MaxFile, destination: Path, *, max_bytes: int) -> int:
        if max_bytes < 0 or (attachment.size is not None and attachment.size > max_bytes):
            raise MaxAPIError("Вложение превышает ограничение размера", code="media.too_large")
        url = _media_url(attachment.url)
        destination = Path(destination)
        for _ in range(5):
            with _request("GET", url, operation="скачивание", stream=True, timeout=(10, 60)) as response:
                if response.is_redirect:
                    url = _media_url(urljoin(url, response.headers["Location"]))
                    continue
                if response.status_code != 200:
                    code = "media.expired" if response.status_code in (403, 404, 410) else ""
                    raise MaxAPIError(f"MAX скачивание: HTTP {response.status_code}",
                                      status=response.status_code, code=code)
                length = response.headers.get("Content-Length", "")
                if length.isascii() and length.isdigit() and (len(length) > 20 or int(length) > max_bytes):
                    raise MaxAPIError("Вложение превышает ограничение размера", code="media.too_large")
                temp = NamedTemporaryFile(dir=destination.parent, delete=False)
                try:
                    written = 0
                    with temp:
                        for chunk in response.iter_content(_BLOCK_SIZE):
                            written += len(chunk)
                            if written > max_bytes:
                                raise MaxAPIError("Вложение превышает ограничение размера", code="media.too_large")
                            temp.write(chunk)
                    if attachment.size is not None and written != attachment.size:
                        raise MaxAPIError("Размер скачанного вложения отличается от заявленного", code="media.size_mismatch")
                    os.replace(temp.name, destination)
                    return written
                except requests.RequestException:
                    raise MaxAPIError("MAX прервал скачивание файла") from None
                finally:
                    Path(temp.name).unlink(missing_ok=True)
        raise MaxAPIError("Слишком много перенаправлений при скачивании файла MAX", code="media.invalid")
