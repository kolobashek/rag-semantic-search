"""Private, local installation settings for the employee bot."""

from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import tempfile
from dataclasses import dataclass, field
from getpass import getpass
from pathlib import Path
from urllib.parse import urlsplit

from filelock import FileLock

from .max_api import MaxAPIError, MaxClient

PROJECT_ROOT = Path(__file__).resolve().parents[3]
WEBHOOK_PATH = "/api/max/webhook"


@dataclass(frozen=True)
class MaxSettings:
    token: str = field(repr=False)
    secret: str = field(repr=False)
    office_url: str
    bot_link: str
    inbox_path: Path
    subscribed: bool = False


def _directory(cfg: dict) -> Path:
    configured = str(cfg.get("max_state_dir") or "runtime/max")
    return PROJECT_ROOT / configured


def _office_origin(url: str) -> str:
    parts = urlsplit(url)
    if (parts.scheme != "https" or not parts.hostname or parts.netloc != parts.hostname
            or parts.path not in ("", "/") or parts.query or parts.fragment):
        raise ValueError("Для кабинета укажите HTTPS-адрес без пути и порта.")
    return f"https://{parts.hostname}"


def _bot_link(url: str) -> str:
    if not re.fullmatch(r"https://max\.ru/[A-Za-z0-9_]+", url):
        raise ValueError("Укажите публичную ссылку нового бота в формате https://max.ru/имя_бота.")
    return url


def read_settings(cfg: dict) -> MaxSettings | None:
    directory = _directory(cfg)
    try:
        raw = json.loads((directory / "bot.json").read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    if not isinstance(raw, dict) or not raw.get("token") or not re.fullmatch(r"[\w-]{32,256}", raw.get("secret", ""), re.ASCII):
        raise ValueError("Настройки MAX повреждены. Проверьте локальный файл bot.json.")
    return MaxSettings(raw["token"], raw["secret"], _office_origin(raw["office_url"]),
                       _bot_link(raw["bot_link"]), directory / "inbox.sqlite", raw.get("subscribed") is True)


def public_bot_link(cfg: dict) -> str:
    try:
        settings = read_settings(cfg)
    except (OSError, ValueError, KeyError, TypeError):
        return ""
    return settings.bot_link if settings and settings.subscribed else ""


def _save(directory: Path, settings: MaxSettings) -> None:
    fd, name = tempfile.mkstemp(dir=directory, prefix=".bot-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump({"token": settings.token, "secret": settings.secret, "office_url": settings.office_url,
                       "bot_link": settings.bot_link, "subscribed": settings.subscribed}, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, directory / "bot.json")
    finally:
        Path(name).unlink(missing_ok=True)


def configure(cfg: dict, office_url: str, bot_link: str, token: str) -> MaxSettings:
    office_url, bot_link = _office_origin(office_url), _bot_link(bot_link)
    client = MaxClient(token)
    username = str(client.get_me().get("username") or "").lstrip("@")
    if bot_link.rsplit("/", 1)[1].casefold() != username.casefold():
        raise ValueError("Токен принадлежит другому боту. Проверьте карточку нового бота в MAX.")
    directory = _directory(cfg)
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    with FileLock(str(directory / "setup.lock"), timeout=10):
        previous = read_settings(cfg)
        if previous and (previous.bot_link != bot_link or previous.office_url != office_url):
            raise ValueError("Этот каталог уже подключён к другому боту или домену.")
        secret = previous.secret if previous else secrets.token_urlsafe(36)
        inbox_path = directory / "inbox.sqlite"
        settings = MaxSettings(token, secret, office_url, bot_link, inbox_path,
                               bool(previous and previous.token == token and previous.subscribed))
        # The endpoint must know the same secret even for the first delivery.
        _save(directory, settings)
        client.subscribe(office_url + WEBHOOK_PATH, settings.secret)
        settings = MaxSettings(token, secret, office_url, bot_link, inbox_path, subscribed=True)
        _save(directory, settings)
        return settings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Подключение MAX-бота для сотрудников")
    sub = parser.add_subparsers(dest="action", required=True)
    setup = sub.add_parser("configure", help="Проверить токен и подключить webhook")
    setup.add_argument("--office-url", required=True)
    setup.add_argument("--bot-link", required=True)
    setup.add_argument("--local-form", action="store_true", help="Ввести токен в браузере на этом компьютере")
    sub.add_parser("status", help="Проверить настройки и подписку в MAX")
    args = parser.parse_args(argv)
    from rag_catalog.core.rag_core import load_config

    try:
        cfg = load_config()
        if args.action == "configure":
            if args.local_form:
                from .max_local_setup import serve
                return 0 if serve(cfg, args.office_url, args.bot_link) else 1
            token = os.environ.get("RAG_MAX_SETUP_TOKEN") or getpass("Токен нового бота MAX: ")
            settings = configure(cfg, args.office_url, args.bot_link, token.strip())
            print(f"Подключён {settings.bot_link}")
        else:
            settings = read_settings(cfg)
            if not settings:
                print("MAX ещё не подключён.")
                return 1
            client = MaxClient(settings.token)
            subscriptions = client.subscriptions()
            if not settings.subscribed or not any(item.get("url") == settings.office_url + WEBHOOK_PATH for item in subscriptions):
                print("В MAX нет подписки на адрес этого кабинета. Повторите configure.")
                return 1
            print(f"Подписка MAX активна: {settings.bot_link} → {settings.office_url}")
    except (EOFError, OSError, ValueError, KeyError, MaxAPIError) as exc:
        # Settings, signed URLs and credentials must never be interpolated into diagnostics.
        print("Не удалось подключить MAX." if isinstance(exc, (OSError, KeyError)) else str(exc))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
