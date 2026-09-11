"""Аутентификация и ACL для десктопного клиента (PyQt).

Зачем отдельный модуль: нативное окно раньше открывалось без входа вообще и
ходило в поиск напрямую, минуя фильтрацию Cloud Drive, — то есть все права,
которые веб тщательно проверяет, в .exe обходились. Логика вынесена из Qt в
чистые функции, чтобы её можно было тестировать без установленного PyQt6.

Важно: события ``login_failed`` пишет вызывающая сторона — именно ими питается
троттлинг перебора паролей (`login_throttle_status`). Веб делает это в
`nice_app._do_login`; здесь повторяется тот же контракт.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from rag_catalog.core.user_auth_db import UserAuthDB

from .helpers import _filter_cloud_drive_search_results, _users_db_path

__all__ = [
    "AuthResult",
    "authenticate",
    "build_result_filter",
    "open_auth_db",
]


class AuthResult:
    """Итог попытки входа: пользователь либо причина отказа и текст для UI."""

    __slots__ = ("user", "reason", "message", "retry_after")

    def __init__(
        self,
        user: Optional[Dict[str, Any]],
        reason: str,
        message: str,
        retry_after: int = 0,
    ) -> None:
        self.user = user
        self.reason = reason
        self.message = message
        self.retry_after = retry_after

    @property
    def ok(self) -> bool:
        return self.user is not None

    def __repr__(self) -> str:  # pragma: no cover - отладочный вывод
        return f"AuthResult(reason={self.reason!r}, ok={self.ok})"


def open_auth_db(cfg: Dict[str, Any]) -> UserAuthDB:
    """UserAuthDB по тому же пути, что использует веб."""
    return UserAuthDB(str(_users_db_path(cfg)))


_MESSAGES = {
    "pending": "Заявка ещё не активирована администратором.",
    "blocked": "Аккаунт заблокирован. Обратитесь к администратору.",
    "invalid_credentials": "Неверный логин или пароль.",
    "not_found": "Неверный логин или пароль.",
}


def authenticate(
    cfg: Dict[str, Any],
    username: str,
    password: str,
    *,
    auth_db: Optional[UserAuthDB] = None,
) -> AuthResult:
    """Проверяет пару логин/пароль и логирует неудачи для троттлинга.

    Возвращает AuthResult; при ``ok`` в ``user`` лежит запись пользователя,
    пригодная для :func:`build_result_filter`.
    """
    login = (username or "").strip()
    db = auth_db if auth_db is not None else open_auth_db(cfg)

    if not login or not password:
        return AuthResult(None, "invalid_credentials", _MESSAGES["invalid_credentials"])

    outcome = db.login_with_reason(username=login, password=password)
    reason = str(outcome.get("reason") or "")
    user = outcome.get("user")

    if reason == "rate_limited":
        retry_after = int(outcome.get("retry_after_seconds") or 0)
        minutes = max(1, (retry_after + 59) // 60)
        return AuthResult(
            None,
            reason,
            f"Слишком много попыток входа. Повторите через {minutes} мин.",
            retry_after,
        )

    if user is None:
        error = reason if reason in {"pending", "blocked"} else "bad_credentials"
        try:
            db.log_auth_event(
                username=login,
                event_type="login_failed",
                ok=False,
                error=error,
            )
        except Exception:  # pragma: no cover - журнал не должен ломать вход
            pass
        return AuthResult(None, reason or "invalid_credentials", _MESSAGES.get(reason, _MESSAGES["invalid_credentials"]))

    try:
        db.log_auth_event(username=login, event_type="login", ok=True, error="")
    except Exception:  # pragma: no cover
        pass
    return AuthResult(dict(user), "ok", "")


def build_result_filter(
    cfg: Dict[str, Any],
    user: Optional[Dict[str, Any]],
) -> Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]:
    """Тот же ACL-фильтр выдачи, что применяет веб.

    Fail-closed: если фильтрация упала, выдача пустая — лучше ничего не
    показать, чем показать чужое.
    """

    def _filter(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not results:
            return results
        try:
            return _filter_cloud_drive_search_results(cfg, user, results)
        except Exception:
            return []

    return _filter
