"""The shared employee portal URL and private delivery of onboarding decisions."""
import requests


def employee_office_url(cfg: dict) -> str:
    from .max_settings import _office_origin, read_settings
    origin = cfg.get("employee_office_url") or cfg.get("max_office_url")
    try:
        if origin:
            return _office_origin(str(origin).strip())
        settings = read_settings(cfg)
        return settings.office_url if settings else ""
    except (OSError, ValueError, KeyError, TypeError):
        return ""


def existing_account_instructions(provider):
    name = "MAX" if provider == "max" else "Telegram"
    return (f"Администратор одобрил привязку к существующему кабинету. Войдите в него привычным способом. "
            f"В верхней панели нажмите «{name}», затем «Открыть бота {name}». "
            "Откройте бота из аккаунта, с которого подали заявку.")


def reviewer_notice(auth, org_id, request_id, *, preferred_channel="telegram"):
    targets = []
    for user in auth.list_users():
        if user.get("status") != "active" or user.get("must_change_password"):
            continue
        if not (user.get("is_platform_admin") or "admin" in user.get("organization_roles", {}).get(org_id, [])):
            continue
        channels = {"telegram": user.get("telegram_chat_id"), "max": user.get("max_user_id")}
        options = [preferred_channel, *[ch for ch in channels if ch != preferred_channel]]
        contacts = [(ch, channels[ch]) for ch in options if channels.get(ch)]
        if contacts:
            targets.append(contacts)
    return {"reviewers": targets, "text": f"Новая заявка на доступ №{request_id}. Откройте «Заявки»."}


def dispatch_notice(cfg: dict, notice: dict) -> bool:
    """Deliver only to the private sender recorded on the reviewed request."""
    if "reviewers" in notice:
        delivered = False
        for contacts in notice["reviewers"]:
            for provider, sender in contacts:
                if dispatch_notice(cfg, {"provider": provider, "sender_id": sender, "text": notice["text"]}):
                    delivered = True
                    break
        return delivered
    provider = notice.get("provider")
    sender = str(notice.get("sender_id") or "")
    message = notice.get("text") or ""
    if not sender.isascii() or not sender.isdecimal() or int(sender) <= 0 or not message:
        return False
    office = employee_office_url(cfg) if notice.get("open_office") else ""
    try:
        if provider == "telegram" and cfg.get("telegram_enabled") and cfg.get("telegram_bot_token"):
            from .telegram_bot import send_message

            if office:
                send_message(cfg["telegram_bot_token"], sender, message,
                             {"inline_keyboard": [[{"text": "Открыть кабинет", "url": office}]]})
            else:
                send_message(cfg["telegram_bot_token"], sender, message)
            return True
        if provider == "max":
            from .max_api import MaxClient
            from .max_settings import read_settings

            settings = read_settings(cfg)
            if settings and settings.subscribed:
                buttons = [[{"type": "link", "text": "Открыть кабинет", "url": office}]] if office else None
                MaxClient(settings.token).send_message(int(sender), message, buttons=buttons)
                return True
    except (OSError, ValueError, RuntimeError, requests.RequestException, KeyError):
        # An approved employee can obtain the decision again in the verified chat.
        return False
    return False
