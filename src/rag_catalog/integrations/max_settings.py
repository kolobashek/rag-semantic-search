"""Private, local installation settings for the employee bot."""

from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import tempfile
from dataclasses import dataclass, field, replace
from getpass import getpass
from pathlib import Path
from urllib.parse import urlsplit

from filelock import FileLock

from .max_api import UPDATE_TYPES, MaxAPIError, MaxClient

PROJECT_ROOT = Path(__file__).resolve().parents[3]
WEBHOOK_PATH = "/api/max/webhook"
COMMAND_HINTS = {
    "menu": "Открыть главное меню",
    "request": "Подать заявку на доступ",
    "status": "Проверить статус заявки",
    "start": "Начать работу с ботом",
}
COMMANDS_RETRY = "Для синхронизации меню выполните python -m rag_catalog.integrations.max_settings commands."


class MenuSetupPending(MaxAPIError):
    """The webhook has been installed and only the command menu needs attention."""


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


def _bot_profile(client: MaxClient, bot_link: str) -> dict:
    profile = client.get_me()
    username = str(profile.get("username") or "").lstrip("@")
    if bot_link.rsplit("/", 1)[1].casefold() != username.casefold():
        raise ValueError("Токен принадлежит другому боту. Проверьте карточку нового бота в MAX.")
    return profile


def _current_commands(profile: dict) -> list[dict]:
    commands = profile.get("commands")
    if commands is None:
        return []
    if not isinstance(commands, list) or not all(
        isinstance(cmd, dict) and isinstance(cmd.get("name"), str) and 1 <= len(cmd["name"]) <= 64
        and (cmd.get("description") is None or isinstance(cmd["description"], str) and 1 <= len(cmd["description"]) <= 128)
        for cmd in commands
    ):
        raise MaxAPIError("MAX вернул некорректный список команд.")
    return commands


def _sync_commands(client: MaxClient, bot_link: str, profile: dict) -> None:
    current = _current_commands(profile)
    names = {cmd["name"] for cmd in current}
    desired = [
        {"name": cmd["name"], "description": COMMAND_HINTS[cmd["name"]]} if cmd["name"] in COMMAND_HINTS else cmd
        for cmd in current
    ]
    desired.extend({"name": name, "description": desc} for name, desc in COMMAND_HINTS.items() if name not in names)
    if current == desired:
        return
    if len(desired) > 32:
        raise MaxAPIError("В меню MAX нет места для всех четырёх команд. Освободите место в списке из 32 команд.")
    client.set_commands(desired)
    observed = _current_commands(_bot_profile(client, bot_link))
    if not all(cmd in observed for cmd in desired):
        raise MaxAPIError("MAX ещё не подтвердил сохранение полного списка команд.")


def _has_webhook(client: MaxClient, office_url: str) -> bool:
    return any(
        item.get("url") == office_url + WEBHOOK_PATH
        and (item.get("update_types") is None or isinstance(item["update_types"], list)
             and all(event in item["update_types"] for event in UPDATE_TYPES))
        for item in client.subscriptions()
    )


def _connect_bot(directory: Path, settings: MaxSettings, client: MaxClient) -> MaxSettings:
    if not settings.subscribed or not _has_webhook(client, settings.office_url):
        client.subscribe(settings.office_url + WEBHOOK_PATH, settings.secret)
    if not settings.subscribed:
        settings = replace(settings, subscribed=True)
        _save(directory, settings)
    try:
        _sync_commands(client, settings.bot_link, _bot_profile(client, settings.bot_link))
    except (MaxAPIError, ValueError) as exc:
        raise MenuSetupPending(f"Вебхук подключён. Меню команд требует проверки. {exc} {COMMANDS_RETRY}") from None
    return settings


def configure(cfg: dict, office_url: str, bot_link: str, token: str) -> MaxSettings:
    office_url, bot_link = _office_origin(office_url), _bot_link(bot_link)
    client = MaxClient(token)
    _bot_profile(client, bot_link)
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
        if previous != settings:
            _save(directory, settings)
        return _connect_bot(directory, settings, client)


def _installed_settings(cfg: dict) -> MaxSettings:
    settings = read_settings(cfg)
    if not settings:
        raise ValueError("MAX ещё не подключён. Выполните configure.")
    return settings


def reconcile_installation(cfg: dict, *, commands_only: bool) -> None:
    directory = _directory(cfg)
    if not directory.is_dir():
        raise ValueError("MAX ещё не подключён. Выполните configure.")
    with FileLock(str(directory / "setup.lock"), timeout=10):
        settings = _installed_settings(cfg)
        client = MaxClient(settings.token)
        profile = _bot_profile(client, settings.bot_link)
        if commands_only:
            _sync_commands(client, settings.bot_link, profile)
        else:
            _connect_bot(directory, settings, client)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Подключение MAX-бота для сотрудников")
    sub = parser.add_subparsers(dest="action", required=True)
    setup = sub.add_parser("configure", help="Проверить токен и подключить webhook")
    setup.add_argument("--office-url", required=True)
    setup.add_argument("--bot-link", required=True)
    setup.add_argument("--local-form", action="store_true", help="Ввести токен в браузере на этом компьютере")
    sub.add_parser("status", help="Проверить подписку и четыре команды в MAX")
    sub.add_parser("commands", help="Восстановить подсказки команд в MAX")
    sub.add_parser("recover", help="Восстановить подписку и подсказки по сохранённым настройкам")
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
            print(f"Подключён {settings.bot_link}. Четыре команды подтверждены в MAX.")
        elif args.action == "commands":
            reconcile_installation(cfg, commands_only=True)
            print("Четыре команды подтверждены в MAX.")
        elif args.action == "recover":
            reconcile_installation(cfg, commands_only=False)
            print("Подписка и четыре команды готовы.")
        else:
            settings = _installed_settings(cfg)
            client = MaxClient(settings.token)
            if not settings.subscribed or not _has_webhook(client, settings.office_url):
                print("Подписка MAX требует восстановления. Выполните recover.")
                return 1
            print(f"Подписка MAX активна: {settings.bot_link} → {settings.office_url}")
            try:
                commands = _current_commands(_bot_profile(client, settings.bot_link))
            except (MaxAPIError, ValueError) as exc:
                raise MenuSetupPending(f"Список команд MAX не подтверждён. {exc} {COMMANDS_RETRY}") from None
            if not all({"name": name, "description": desc} in commands for name, desc in COMMAND_HINTS.items()):
                print("Список команд MAX отличается от настроенного. " + COMMANDS_RETRY)
                return 2
            print("В MAX подтверждены команды /menu, /request, /status, /start.")
    except MenuSetupPending as exc:
        print(str(exc))
        return 2
    except (EOFError, OSError, ValueError, KeyError, MaxAPIError) as exc:
        # Settings, signed URLs and credentials must never be interpolated into diagnostics.
        print("Не удалось подключить MAX." if isinstance(exc, (OSError, KeyError)) else str(exc))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
