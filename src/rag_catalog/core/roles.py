"""Application capabilities; legacy role remains a compatibility projection."""

import json

ROLE_LABELS = {"user": "Пользователь облака", "driver": "Водитель", "dispatcher": "Диспетчер", "admin": "Администратор"}


def normalize_roles(roles):
    if not isinstance(roles, (list, tuple, set)) or not roles or any(r not in ROLE_LABELS for r in roles):
        raise ValueError("Выберите хотя бы одну допустимую роль")
    return sorted(set(roles))


def user_roles(user):
    if not user:
        return set()
    if "roles" in user:
        values = user["roles"]
    elif user.get("roles_json"):
        try:
            values = json.loads(user["roles_json"])
        except (TypeError, ValueError):
            return set()
    else:
        values = [user.get("role")]
    if not isinstance(values, (list, tuple, set)):
        return set()
    return {r for r in values if isinstance(r, str) and r in ROLE_LABELS}


def primary_role(roles):
    return next(role for role in ("admin", "user", "dispatcher", "driver") if role in roles)


def can_manage_shifts(user):
    return bool(user_roles(user) & {"admin", "dispatcher"})


def can_use_shifts(user):
    return bool(user_roles(user) & {"admin", "dispatcher", "driver"})


def can_use_catalog(user):
    return bool(user_roles(user) & {"admin", "user"})


def can_open_screen(user, screen):
    if screen == "shifts":
        return can_use_shifts(user)
    if screen in {"jobs", "index", "stats"}:
        return "admin" in user_roles(user)
    return can_use_catalog(user)
