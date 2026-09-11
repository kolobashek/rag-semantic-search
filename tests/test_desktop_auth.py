"""Аутентификация и ACL нативного клиента.

Логика намеренно вынесена из Qt, поэтому тесты идут без установленного PyQt6.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from rag_catalog.ui import desktop_auth


class _FakeAuthDB:
    """Минимальный UserAuthDB: отдаёт заданный исход и пишет журнал событий."""

    def __init__(self, outcome: Dict[str, Any]) -> None:
        self.outcome = outcome
        self.events: List[Dict[str, Any]] = []

    def login_with_reason(self, *, username: str, password: str = "") -> Dict[str, Any]:
        self.last_credentials = (username, password)
        return self.outcome

    def log_auth_event(self, *, username: str, event_type: str, ok: bool, error: str = "") -> None:
        self.events.append(
            {"username": username, "event_type": event_type, "ok": ok, "error": error}
        )


def test_successful_login_returns_user_and_logs_login() -> None:
    db = _FakeAuthDB({"user": {"username": "dmitry", "role": "user"}, "reason": "ok"})

    result = desktop_auth.authenticate({}, " Dmitry ", "secret", auth_db=db)

    assert result.ok
    assert result.user is not None and result.user["username"] == "dmitry"
    assert db.last_credentials == ("Dmitry", "secret")
    assert [e["event_type"] for e in db.events] == ["login"]


@pytest.mark.parametrize(
    ("reason", "expected_error"),
    [
        ("invalid_credentials", "bad_credentials"),
        ("not_found", "bad_credentials"),
        ("pending", "pending"),
        ("blocked", "blocked"),
    ],
)
def test_failed_login_is_logged_so_throttling_can_see_it(reason: str, expected_error: str) -> None:
    """login_failed пишет вызывающая сторона — на этих событиях стоит троттлинг."""
    db = _FakeAuthDB({"user": None, "reason": reason})

    result = desktop_auth.authenticate({}, "dmitry", "wrong", auth_db=db)

    assert not result.ok
    assert result.message
    assert db.events == [
        {"username": "dmitry", "event_type": "login_failed", "ok": False, "error": expected_error}
    ]


def test_rate_limited_reports_minutes_and_does_not_log_extra_failure() -> None:
    db = _FakeAuthDB({"user": None, "reason": "rate_limited", "retry_after_seconds": 125})

    result = desktop_auth.authenticate({}, "dmitry", "wrong", auth_db=db)

    assert not result.ok
    assert result.reason == "rate_limited"
    assert "3 мин" in result.message
    assert db.events == []


def test_empty_credentials_do_not_reach_the_database() -> None:
    db = _FakeAuthDB({"user": {"username": "dmitry"}, "reason": "ok"})

    assert not desktop_auth.authenticate({}, "", "secret", auth_db=db).ok
    assert not desktop_auth.authenticate({}, "dmitry", "", auth_db=db).ok
    assert db.events == []


def test_result_filter_delegates_to_web_acl(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: Dict[str, Any] = {}

    def _fake_filter(cfg, user, results, *, service=None):
        seen["cfg"] = cfg
        seen["user"] = user
        return [r for r in results if r["full_path"].startswith("O:/open")]

    monkeypatch.setattr(desktop_auth, "_filter_cloud_drive_search_results", _fake_filter)
    result_filter = desktop_auth.build_result_filter({"k": 1}, {"username": "dmitry"})

    filtered = result_filter(
        [{"full_path": "O:/open/a.pdf"}, {"full_path": "O:/private/b.pdf"}]
    )

    assert filtered == [{"full_path": "O:/open/a.pdf"}]
    assert seen["user"] == {"username": "dmitry"}


def test_result_filter_is_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Сбой проверки прав не должен превращаться в показ чужих документов."""

    def _boom(*args: Any, **kwargs: Any):
        raise RuntimeError("registry is down")

    monkeypatch.setattr(desktop_auth, "_filter_cloud_drive_search_results", _boom)
    result_filter = desktop_auth.build_result_filter({}, {"username": "dmitry"})

    assert result_filter([{"full_path": "O:/private/b.pdf"}]) == []


def test_result_filter_keeps_empty_input_cheap(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(*args: Any, **kwargs: Any):  # pragma: no cover - не должен вызваться
        raise AssertionError("фильтр не нужен для пустой выдачи")

    monkeypatch.setattr(desktop_auth, "_filter_cloud_drive_search_results", _boom)
    assert desktop_auth.build_result_filter({}, None)([]) == []
