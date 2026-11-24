import time
import traceback

import pytest
import requests

import telegram_bot
from telegram_bot import (
    OPEN_FILE_FEEDBACK,
    _clean_tg_text,
    _file_uri,
    _main_menu,
    _message_file_info,
    _safe_filename,
    chat_action,
    format_fact_answer,
    process_message,
    process_query,
    send_chat_action,
    send_search_results,
    set_bot_commands,
)


def test_opening_file_is_neutral_feedback() -> None:
    assert OPEN_FILE_FEEDBACK == 0


def test_tg_call_does_not_expose_token_in_transport_errors(monkeypatch) -> None:
    token = "123456789:ABC_def-ghi"

    def fail(*_args, **_kwargs):
        raise requests.ConnectTimeout(f"https://api.telegram.org/bot{token}/getUpdates")

    monkeypatch.setattr(telegram_bot.requests, "post", fail)

    with pytest.raises(RuntimeError, match="Telegram API getUpdates unavailable: ConnectTimeout") as exc_info:
        telegram_bot.tg_call(token, "getUpdates", {})

    assert token not in str(exc_info.value)


@pytest.mark.parametrize("operation", ["call", "upload", "download"])
def test_telegram_http_error_never_leaks_token_in_traceback(monkeypatch, tmp_path, operation):
    token = "123456789:private-test-token"
    response = requests.Response()
    response.status_code = 502
    response.url = f"https://api.telegram.org/bot{token}/getUpdates"
    response._content = b"Bad Gateway"
    response._content_consumed = True
    monkeypatch.setattr(telegram_bot.requests, "post", lambda *a, **kw: response)
    monkeypatch.setattr(telegram_bot.requests, "get", lambda *a, **kw: response)
    monkeypatch.setattr(telegram_bot, "send_chat_action", lambda *a, **kw: None)
    document = tmp_path / "document.txt"
    document.write_text("fixture")
    with pytest.raises(telegram_bot.TelegramAPIError, match="HTTP 502") as caught:
        if operation == "call":
            telegram_bot.tg_call(token, "getUpdates", {})
        elif operation == "upload":
            telegram_bot.send_document(token, "123", str(document))
        else:
            telegram_bot.download_file(token, "document.txt", tmp_path / "download.txt")
    assert token not in "".join(traceback.format_exception(caught.value))


@pytest.mark.parametrize("body", [b'not json', b'{"ok":false,"description":"private-test-token"}', b'[]'])
def test_telegram_invalid_responses_do_not_log_response_body(monkeypatch, body):
    token = "private-test-token"
    response = requests.Response()
    response.status_code = 200
    response._content = body
    monkeypatch.setattr(telegram_bot.requests, "post", lambda *a, **kw: response)
    with pytest.raises(telegram_bot.TelegramAPIError) as caught:
        telegram_bot.tg_call(token, "getUpdates", {})
    assert "private-test-token" not in "".join(traceback.format_exception(caught.value))


class _FakeSearcher:
    def __init__(self, fact_result=None, fact_exc=None, search_result=None, search_exc=None, answer_result=None):
        self._fact_result = fact_result
        self._fact_exc = fact_exc
        self._search_result = search_result if search_result is not None else []
        self._search_exc = search_exc
        self._answer_result = answer_result
        self.last_search_kwargs = {}
        self.fact_result_filter = None
        self.answer_result_filter = None

    def answer_fact_question(self, _q, limit=30, *, result_filter=None):
        self.fact_result_filter = result_filter
        if self._fact_exc:
            raise self._fact_exc
        return self._fact_result if self._fact_result is not None else {"ok": False}

    def search(self, *_args, **_kwargs):
        if self._search_exc:
            raise self._search_exc
        self.last_search_kwargs = dict(_kwargs)
        return self._search_result

    def answer_documents(self, *_args, **_kwargs):
        self.answer_result_filter = _kwargs.get("result_filter")
        return self._answer_result if self._answer_result is not None else {"ok": False}


class _FakeAuthDB:
    def __init__(self, out=None, user=None):
        self.out = out
        self.user = {"role": "user", **user} if user else user
        self.events = []
        self.unlinked = False
        self.registration_requests = []

    def confirm_verification(self, *, telegram_chat_id: str, code: str):
        return self.out(telegram_chat_id, code) if callable(self.out) else self.out

    def confirm_password_reset(self, *, telegram_chat_id: str, code: str):
        return self.out(telegram_chat_id, code) if callable(self.out) else self.out

    def get_user_by_telegram_chat_id(self, telegram_chat_id: str):
        return self.user

    def unlink_telegram_chat_id(self, telegram_chat_id: str):
        self.unlinked = True
        if not self.user:
            return None
        username = self.user.get("username")
        self.user = None
        return username

    def log_auth_event(self, **kwargs):
        self.events.append(kwargs)

    def consume_telegram_start_token(self, **kwargs):
        return self.out(**kwargs) if callable(self.out) else self.out


def test_process_query_reports_infra_error_from_fact() -> None:
    s = _FakeSearcher(fact_exc=ConnectionError("qdrant down"))
    out = process_query(s, "сколько весит pc300")
    assert "Ошибка инфраструктуры поиска" in out


def test_process_query_returns_fact_answer() -> None:
    s = _FakeSearcher(
        fact_result={
            "ok": True,
            "answer": "3400 кг согласно ПСМ",
            "source": {
                "filename": "ПСМ PC300.pdf",
                "full_path": r"O:\Обмен\ПСМ\PC300.pdf",
                "text_excerpt": "Масса: 3400 кг",
            },
        }
    )
    out = process_query(s, "сколько весит pc300")
    assert "3400 кг согласно ПСМ" in out
    assert "ПСМ PC300.pdf" in out


def test_process_query_fallback_results() -> None:
    s = _FakeSearcher(
        fact_result={"ok": False},
        search_result=[{"filename": "паспорт.docx", "score": 0.8, "full_path": r"O:\Обмен\паспорт.docx"}],
    )
    out = process_query(s, "паспорт")
    assert "Точный факт не извлечён" in out
    assert "паспорт.docx" in out


def test_process_query_returns_rag_answer_for_question() -> None:
    s = _FakeSearcher(
        fact_result={"ok": False},
        answer_result={
            "ok": True,
            "answer": "В документах указано, что договор действует до 2026 года.",
            "sources": [{"filename": "договор.docx", "full_path": r"O:\Обмен\договор.docx", "citation": "[S1] договор.docx · chunk 2"}],
        },
    )
    out = process_query(s, "когда действует договор?", username="ivan")
    assert "Ответ по документам:" in out
    assert "действует до 2026" in out
    assert "Источники:" in out
    assert "[S1] договор.docx" in out


def test_process_query_no_results() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[])
    out = process_query(s, "нечто редкое")
    assert out == "Ничего не найдено."


def test_interactive_search_sends_result_and_nav_buttons(monkeypatch) -> None:
    s = _FakeSearcher(
        search_result=[
            {"filename": "паспорт.pdf", "full_path": r"O:\Обмен\паспорт.pdf", "extension": ".pdf", "score": 0.9},
            {"filename": "договор.docx", "full_path": r"O:\Обмен\договор.docx", "extension": ".docx", "score": 0.8},
        ]
    )
    sent = []

    def fake_send_message(token, chat_id, text, reply_markup=None):
        sent.append({"token": token, "chat_id": chat_id, "text": text, "reply_markup": reply_markup})
        return len(sent)

    monkeypatch.setattr(telegram_bot, "send_message", fake_send_message)

    sid = send_search_results("token", s, chat_id="777", query="паспорт", username="ivan", user={"username": "ivan", "role": "user"})

    assert sid
    assert len(sent) == 3
    assert "паспорт.pdf" in sent[0]["text"]
    assert "score=" not in sent[0]["text"]
    first_keyboard = sent[0]["reply_markup"]["inline_keyboard"]
    assert first_keyboard[0][0]["text"] == "📥 Получить"
    assert first_keyboard[0][1]["text"] == "👍"
    assert first_keyboard[0][2]["text"] == "👎"
    nav_keyboard = sent[-1]["reply_markup"]["inline_keyboard"]
    assert any(button["text"] == "Ещё ↓" for row in nav_keyboard for button in row)
    assert any(button["text"] == "PDF" for row in nav_keyboard for button in row)
    assert any(button["text"] == "Уточнить запрос" for row in nav_keyboard for button in row)


def test_send_chat_action_calls_telegram_api(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(telegram_bot, "tg_call", lambda token, method, payload: calls.append((token, method, payload)) or {"ok": True})

    send_chat_action("token", "777", "typing")

    assert calls == [("token", "sendChatAction", {"chat_id": "777", "action": "typing"})]


def test_chat_action_sends_initial_action_and_stops(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(telegram_bot, "tg_call", lambda token, method, payload: calls.append((method, payload)) or {"ok": True})

    with chat_action("token", "777", "typing"):
        pass

    assert calls[0] == ("sendChatAction", {"chat_id": "777", "action": "typing"})


def test_main_menu_contains_admin_buttons() -> None:
    menu = _main_menu({"username": "admin", "role": "admin"})
    labels = [button["text"] for row in menu["keyboard"] for button in row]
    assert "Заявка" not in labels
    assert "Добавить пользователя" in labels
    assert "Заявки" in labels


def test_main_menu_hides_admin_and_request_buttons_for_regular_user() -> None:
    menu = _main_menu({"username": "ivan", "role": "user"})
    labels = [button["text"] for row in menu["keyboard"] for button in row]
    assert "Заявка" not in labels
    assert "Добавить пользователь" not in labels
    assert "Добавить пользователя" not in labels
    assert "Заявки" not in labels
    assert "Поиск" in labels


def test_main_menu_shows_request_for_unknown_user() -> None:
    menu = _main_menu(None)
    labels = [button["text"] for row in menu["keyboard"] for button in row]
    assert "Заявка" in labels
    assert "Добавить файл" not in labels


def test_set_bot_commands_hides_admin_commands_by_default(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(telegram_bot, "tg_call", lambda token, method, payload: calls.append(payload) or {"ok": True})

    set_bot_commands("token")

    commands = [item["command"] for item in calls[0]["commands"]]
    assert "requests" not in commands
    assert "add_user" not in commands


def test_safe_filename_removes_path_and_forbidden_chars() -> None:
    assert _safe_filename(r"..\secret:bad?.pdf") == "secret_bad_.pdf"
    assert _safe_filename("   ") == "telegram_file"


def test_message_file_info_document() -> None:
    info = _message_file_info(
        {
            "document": {
                "file_id": "abc",
                "file_name": r"..\invoice?.pdf",
                "file_size": 123,
            }
        }
    )
    assert info["file_id"] == "abc"
    assert info["file_name"] == "invoice_.pdf"
    assert info["kind"] == "document"


def test_message_file_info_picks_largest_photo() -> None:
    info = _message_file_info(
        {
            "photo": [
                {"file_id": "small", "file_size": 10},
                {"file_id": "large", "file_size": 20},
            ]
        }
    )
    assert info["file_id"] == "large"
    assert info["kind"] == "photo"


def test_process_message_verify_success() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[])
    auth = _FakeAuthDB({"ok": True, "username": "ivan"})
    out = process_message(
        searcher=s,
        auth_db=auth,
        text="/verify 123456",
        chat_id="777",
        allowed_chat_id="",
    )
    assert "подтверждён" in out
    assert "ivan" in out
    assert auth.events[-1]["event_type"] == "telegram_verify_success"


def test_process_message_verify_not_found() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[])
    auth = _FakeAuthDB({"ok": False, "reason": "not_found"})
    out = process_message(
        searcher=s,
        auth_db=auth,
        text="/verify 000000",
        chat_id="777",
        allowed_chat_id="123",
    )
    assert "Доступ запрещён" in out


def test_process_message_recover_success() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[])
    auth = _FakeAuthDB({"ok": True, "username": "ivan"})
    out = process_message(
        searcher=s,
        auth_db=auth,
        text="/recover 123456",
        chat_id="777",
        allowed_chat_id="",
    )
    assert "Код восстановления подтверждён" in out


def test_process_message_denies_search_for_unauthorized_chat() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[{"filename": "secret.pdf"}])
    auth = _FakeAuthDB(user=None)
    out = process_message(
        searcher=s,
        auth_db=auth,
        text="паспорт",
        chat_id="777",
        allowed_chat_id="",
    )
    assert "Доступ запрещён" in out
    assert s.last_search_kwargs == {}
    assert auth.events[-1]["event_type"] == "telegram_search_denied"


def test_process_message_allows_search_for_verified_user() -> None:
    s = _FakeSearcher(
        fact_result={"ok": False},
        search_result=[{"filename": "паспорт.docx", "score": 0.8, "full_path": r"O:\Обмен\паспорт.docx"}],
    )
    auth = _FakeAuthDB(user={"username": "ivan", "telegram_chat_id": "777", "status": "active"})
    out = process_message(
        searcher=s,
        auth_db=auth,
        text="паспорт",
        chat_id="777",
        allowed_chat_id="",
    )
    assert "паспорт.docx" in out
    assert s.last_search_kwargs["username"] == "ivan"
    assert auth.events[-1]["event_type"] == "telegram_search"


def test_help_hides_admin_commands_for_regular_user() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[])
    auth = _FakeAuthDB(user={"username": "ivan", "telegram_chat_id": "777", "role": "user", "status": "active"})
    out = process_message(
        searcher=s,
        auth_db=auth,
        text="/help",
        chat_id="777",
        allowed_chat_id="",
    )
    assert "Команды администратора" not in out
    assert "/add_user" not in out
    assert "/requests" not in out


def test_help_shows_admin_commands_for_admin() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[])
    auth = _FakeAuthDB(user={"username": "admin", "telegram_chat_id": "777", "role": "admin", "status": "active"})
    out = process_message(
        searcher=s,
        auth_db=auth,
        text="/help",
        chat_id="777",
        allowed_chat_id="",
    )
    assert "Команды администратора" in out
    assert "/add_user" in out
    assert "/requests" in out


def test_process_message_whoami_for_verified_user() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[])
    auth = _FakeAuthDB(user={"username": "ivan", "telegram_chat_id": "777", "role": "admin", "status": "active"})
    out = process_message(
        searcher=s,
        auth_db=auth,
        text="/whoami",
        chat_id="777",
        allowed_chat_id="",
    )
    assert "ivan" in out
    assert "Администратор" in out
    assert auth.events[-1]["event_type"] == "telegram_whoami"


def test_process_message_logout_unlinks_telegram() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[])
    auth = _FakeAuthDB(user={"username": "ivan", "telegram_chat_id": "777", "role": "user", "status": "active"})
    out = process_message(
        searcher=s,
        auth_db=auth,
        text="/logout",
        chat_id="777",
        allowed_chat_id="",
    )
    assert "отвязан" in out
    assert auth.unlinked is True
    assert auth.user is None
    assert auth.events[-1]["event_type"] == "telegram_logout"


def test_process_message_help_is_logged() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[])
    auth = _FakeAuthDB(user={"username": "ivan", "telegram_chat_id": "777", "role": "user", "status": "active"})
    out = process_message(
        searcher=s,
        auth_db=auth,
        text="/help",
        chat_id="777",
        allowed_chat_id="",
    )
    assert "Для поиска напишите" in out
    assert auth.events[-1]["event_type"] == "telegram_help"


def test_process_message_start_for_unauthorized_contains_chat_id() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[])
    auth = _FakeAuthDB(user=None)
    out = process_message(
        searcher=s,
        auth_db=auth,
        text="/start",
        chat_id="777",
        allowed_chat_id="",
    )
    assert "Ваш Telegram ID: 777" in out


def test_process_message_start_verify_payload_confirms_user() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[])
    auth = _FakeAuthDB(out={"ok": True, "username": "ivan"}, user={"username": "ivan", "telegram_chat_id": "777", "status": "active"})
    out = process_message(
        searcher=s,
        auth_db=auth,
        text="/start verify_123456",
        chat_id="777",
        allowed_chat_id="",
    )
    assert "Авторизация прошла успешно" in out
    assert "Для поиска напишите" in out


def test_process_message_start_link_payload_uses_token_flow() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[])
    auth = _FakeAuthDB(
        out={"ok": True, "purpose": "link", "username": "ivan"},
        user={"username": "ivan", "telegram_chat_id": "777", "role": "user", "status": "active"},
    )
    out = process_message(
        searcher=s,
        auth_db=auth,
        text="/start link_secret",
        chat_id="777",
        allowed_chat_id="",
        telegram_username="ivan_tg",
    )
    assert "Авторизация прошла успешно" in out


@pytest.fixture
def employee_chat(tmp_path, monkeypatch):
    from rag_catalog.core.user_auth_db import UserAuthDB

    db = UserAuthDB(str(tmp_path / "employees.db"))
    responses, notices = [], []
    cfg = {"employee_office_url": "https://office.example.com", "telegram_bot_link": "https://t.me/unit_test_bot"}
    monkeypatch.setattr(telegram_bot, "send_message",
                        lambda token, chat, text, reply_markup=None: responses.append((chat, text, reply_markup)))
    monkeypatch.setattr(telegram_bot, "answer_callback_query", lambda *args: None)
    monkeypatch.setattr(telegram_bot, "dispatch_notice", lambda cfg, notice: notices.append(notice) or True)

    def update(actor, *, text="", callback="", chat=None, kind="private", **fields):
        actor = int(actor)
        message = {"chat": {"id": chat if chat is not None else actor, "type": kind}, **fields}
        sender = {"id": actor, "first_name": "Сотрудник"}
        payload = ({"callback_query": {"id": "test", "from": sender, "message": message, "data": callback}}
                   if callback else {"message": {**message, "from": sender, "text": text}})
        start = len(responses)
        telegram_bot.handle_update("unit", db, cfg, {"update_id": start + 1, **payload})
        return responses[-1] if len(responses) > start else None

    return db, update, responses, notices


def _choose(reply, label):
    return next(b["callback_data"] for row in reply[2]["inline_keyboard"] for b in row if b["text"] == label)


def _create_staff(db, username, sender, roles):
    assert db.admin_create_user(username=username, password="local_fixture_password", telegram_chat_id=str(sender),
                                roles=roles, must_change_password=False)


def test_driver_and_dispatcher_have_shift_features_without_catalog(employee_chat, monkeypatch):
    import rag_catalog.ui.helpers as helpers

    db, update, _, _ = employee_chat
    _create_staff(db, "driver1", 71, ["driver"])
    _create_staff(db, "dispatcher1", 72, ["dispatcher"])
    monkeypatch.setattr(helpers, "_cached_searcher_if_ready", lambda *a, **kw: pytest.fail("search must stay closed"))
    for actor, role, heading in [(71, "Водитель", "Мои смены"), (72, "Диспетчер", "Журнал смен сотрудников")]:
        answer = update(actor, text="/whoami")
        assert role in answer[1]
        labels = [b["text"] for row in answer[2]["keyboard"] for b in row]
        assert "Смены" in labels and "Каталог" not in labels and "Заявка" not in labels
        journal = update(actor, text="Смены")[1]
        assert heading in journal and "https://office.example.com/shifts" in journal
        assert "https://office.example.com/shifts" in update(actor, text="Путевой лист")[1]
        for command in ["Каталог", "/upload", "/search", "название документа"]:
            assert "закрыт" in update(actor, text=command)[1]
        assert "закрыт" in update(actor, callback="more:old")[1]
        assert "https://office.example.com/shifts" in update(actor, document={"file_id": "bad"})[1]


def test_private_login_requires_explicit_button_and_ignores_foreign_chat(employee_chat):
    db, update, _, _ = employee_chat
    _create_staff(db, "driver1", 71, ["driver"])
    challenge = db.create_telegram_login_challenge()
    private = challenge["token"]
    verifier = challenge["verifier"]
    assert update(71, text="/start login_" + private, kind="group") is None
    assert update(71, text="/start login_" + private, chat=72) is None
    reply = update(71, text="/start login_" + private)
    button = _choose(reply, "Подтвердить вход в браузере")
    assert verifier not in repr(reply)
    assert db.consume_confirmed_telegram_login(verifier=verifier)["reason"] == "pending"
    assert not update(72, chat=71, callback=button)
    assert not update(71, kind="supergroup", callback=button)
    assert db.consume_confirmed_telegram_login(verifier=verifier)["reason"] == "pending"
    assert "подтверждён" in update(71, callback=button)[1]
    assert not db.consume_confirmed_telegram_login(verifier=private)["ok"]
    assert db.consume_confirmed_telegram_login(verifier=verifier)["username"] == "driver1"
    assert not db.consume_confirmed_telegram_login(verifier=verifier)["ok"]
    assert "Подтвердить" not in update(99, text="/start login_" + private)[1]
    assert private not in repr(db.list_auth_events()) and verifier not in repr(db.list_auth_events())


def test_telegram_user_picker_creates_pending_bound_invite_only_after_confirmation(employee_chat):
    db, update, responses, notices = employee_chat
    _create_staff(db, "admin1", 11, ["admin"])
    _create_staff(db, "office1", 12, ["user"])
    contact = {"user_id": 44, "first_name": "Иван", "last_name": "Петров"}
    update(12, contact=contact)
    assert not db.get_user_by_telegram_chat_id("44")
    update(11, text="/add_user")
    pick = responses[-2][2]["keyboard"][0][0]["request_users"]
    assert pick["user_is_bot"] is False
    reply = update(11, users_shared={"request_id": pick["request_id"], "users": [contact]})
    assert "Иван Петров" in reply[1]
    assert any("✓ Водитель" == b["text"] for row in reply[2]["inline_keyboard"] for b in row)
    reply = update(11, callback=_choose(reply, "Далее"))
    assert "Водитель" in reply[1]
    reply = update(11, callback=_choose(reply, "Создать приглашение"))
    link = reply[1].split("?start=")[1]
    assert not any(r[0] == "44" for r in responses)
    assert not notices and "Временный пароль" not in repr(responses)
    assert "недоступно" in update(45, text="/start " + link)[1]
    activation = update(44, text="/start " + link)
    assert activation[2]["inline_keyboard"][0][0]["url"].startswith("https://office.example.com/auth/activate#token=")
    assert not db.get_user_by_telegram_chat_id("44")
    assert link not in repr(db.list_auth_events())


@pytest.mark.parametrize("decision,confirm_label,notice", [
    ("/approve", "Одобрить заявку", "одобрена"), ("/reject", "Да, отклонить", "отклонена"),
])
def test_telegram_approval_and_rejection_notify_requester(employee_chat, decision, confirm_label, notice):
    db, update, _, notices = employee_chat
    _create_staff(db, "admin1", 11, ["admin"])
    update(44, text="/register")
    reply = update(44, text="Иван Петров")
    reply = update(44, callback=_choose(reply, "Отправить заявку"))
    assert "ожидает" in reply[1]
    pending = db.list_employee_requests(admin_username="admin1")
    assert len(pending) == 1 and pending[0]["sender_id"] == "44"
    request_id = pending[0]["id"]
    assert "администратору" in update(45, text=f"{decision} {request_id}")[1]
    reply = update(11, text=f"{decision} {request_id}")
    if decision == "/approve":
        reply = update(11, callback=_choose(reply, "Далее"))
    reply = update(11, callback=_choose(reply, confirm_label))
    assert notice in reply[1]
    assert notices[0]["reviewers"] == [[("telegram", "11")]]
    assert notices[1:] == [{"provider": "telegram", "sender_id": "44",
                        "text": f"Ваша заявка {notice}. Откройте /status для подробностей."}]
    reply = update(44, text="/status")
    if decision == "/approve":
        assert "#token=" in reply[2]["inline_keyboard"][0][0]["url"]
    else:
        assert "отклонил" in reply[1]


@pytest.mark.parametrize("button", ["more:acl", "open:acl:0", "cat:acl:pg:0"])
def test_telegram_callbacks_recheck_current_groups(employee_chat, monkeypatch, button):
    db, update, _, _ = employee_chat
    _create_staff(db, "office1", 12, ["user"])
    user = {"username": "office1", "role": "user", "group_ids": ["revoked"]}

    def restrict(cfg, current, results):
        return results if "revoked" in current["group_ids"] else []

    monkeypatch.setattr(telegram_bot, "acl_filter_results", restrict)
    monkeypatch.setattr(telegram_bot, "send_document", lambda *a, **kw: pytest.fail("revoked file"))
    sessions = telegram_bot.CATALOG_SESSIONS if button.startswith("cat:") else telegram_bot.SEARCH_SESSIONS
    monkeypatch.setitem(sessions, "acl", {"user": user, "username": "office1", "chat_id": "12", "created_at": time.time(),
                                         "cfg": {"cloud_drive_db_path": "dummy"}, "all_paths": ["closed.pdf"],
                                         "results": [{"full_path": "closed.pdf"}]})
    assert "изменился" in update(12, callback=button)[1]


# ── sanitization helpers ──────────────────────────────────────────────────

def test_clean_tg_text_strips_control_chars() -> None:
    assert _clean_tg_text("hello\x00world\x1f!") == "helloworld!"


def test_clean_tg_text_normalizes_crlf() -> None:
    assert "\r" not in _clean_tg_text("line1\r\nline2\rline3")


def test_clean_tg_text_truncates() -> None:
    long = "а" * 2000
    result = _clean_tg_text(long, max_len=100)
    assert len(result) <= 101  # 100 chars + "…"
    assert result.endswith("…")


def test_file_uri_windows_cyrillic() -> None:
    uri = _file_uri(r"O:\Обмен\Договоры\файл.pdf")
    assert uri.startswith("file:///O:/")
    assert "%D0" in uri  # кириллица закодирована


def test_file_uri_windows_spaces() -> None:
    uri = _file_uri(r"C:\My Documents\file name.pdf")
    assert "%20" in uri or "My%20Documents" in uri


def test_file_uri_empty_returns_empty() -> None:
    assert _file_uri("") == ""
    assert _file_uri(None) == ""  # type: ignore[arg-type]


def test_format_fact_answer_no_newlines_in_path() -> None:
    result = format_fact_answer({
        "answer": "3400 кг",
        "source": {
            "filename": "file\nwith\nnewlines.pdf",
            "full_path": r"O:\Обмен\file.pdf",
            "text_excerpt": "Масса:\n3400 кг",
        },
    })
    # Перевод строки допустим между полями, но не внутри имени файла
    lines = result.split("\n")
    file_line = next(l for l in lines if l.startswith("Файл:"))
    assert "\n" not in file_line


# ─── Cloud Drive ACL в боте ───────────────────────────────────────────────────

_ACL_CFG = {"cloud_drive_db_path": "cloud_drive.db", "catalog_path": r"O:\Обмен"}


def _deny_all(monkeypatch) -> None:
    import rag_catalog.ui.helpers as ui_helpers

    monkeypatch.setattr(ui_helpers, "_filter_cloud_drive_search_results", lambda cfg, user, results: [])


def _allow_only(monkeypatch, allowed_paths: set) -> None:
    import rag_catalog.ui.helpers as ui_helpers

    def _filter(cfg, user, results):
        return [item for item in results if str(item.get("full_path") or "") in allowed_paths]

    monkeypatch.setattr(ui_helpers, "_filter_cloud_drive_search_results", _filter)


def test_bot_search_results_are_filtered_by_cloud_drive_acl(monkeypatch) -> None:
    _deny_all(monkeypatch)
    s = _FakeSearcher(
        fact_result={"ok": False},
        search_result=[{"filename": "зарплаты.xlsx", "score": 0.9, "full_path": r"O:\Обмен\Кадры\зарплаты.xlsx"}],
    )

    out = process_query(s, "зарплаты", cfg=_ACL_CFG, user={"username": "ivan", "role": "user"})

    assert out == "Ничего не найдено."
    assert "зарплаты.xlsx" not in out


def test_bot_search_without_cloud_drive_keeps_results() -> None:
    s = _FakeSearcher(
        fact_result={"ok": False},
        search_result=[{"filename": "паспорт.docx", "score": 0.8, "full_path": r"O:\Обмен\паспорт.docx"}],
    )

    out = process_query(s, "паспорт", cfg={}, user={"username": "ivan", "role": "user"})

    assert "паспорт.docx" in out


def test_bot_rag_answer_is_suppressed_when_source_is_restricted(monkeypatch) -> None:
    _deny_all(monkeypatch)
    s = _FakeSearcher(
        fact_result={"ok": False},
        answer_result={
            "ok": True,
            "answer": "Оклад директора — 500 000 ₽.",
            "sources": [{"filename": "зарплаты.xlsx", "full_path": r"O:\Обмен\Кадры\зарплаты.xlsx"}],
        },
        search_result=[],
    )

    out = process_query(s, "какой оклад у директора?", cfg=_ACL_CFG, user={"username": "ivan", "role": "user"})

    assert "500 000" not in out
    assert out == "Ничего не найдено."


def test_bot_fact_answer_is_suppressed_when_source_is_restricted(monkeypatch) -> None:
    _deny_all(monkeypatch)
    s = _FakeSearcher(
        fact_result={
            "ok": True,
            "answer": "3400 кг согласно ПСМ",
            "source": {"filename": "PC300.pdf", "full_path": r"O:\Обмен\Закрытая\PC300.pdf"},
        },
        search_result=[],
    )

    out = process_query(s, "сколько весит pc300", cfg=_ACL_CFG, user={"username": "ivan", "role": "user"})

    assert "3400 кг" not in out


def test_bot_catalog_hides_documents_without_access(monkeypatch, tmp_path) -> None:
    _deny_all(monkeypatch)
    root = tmp_path / "catalog"
    (root / "Кадры").mkdir(parents=True)
    secret = root / "Кадры" / "зарплаты.xlsx"
    secret.write_text("secret", encoding="utf-8")

    cfg = {"cloud_drive_db_path": "cloud_drive.db", "catalog_path": str(root)}
    monkeypatch.setattr(telegram_bot, "_catalog_load_paths", lambda _cfg: [str(secret)])
    sent = []
    monkeypatch.setattr(
        telegram_bot,
        "send_message",
        lambda token, chat_id, text, reply_markup=None: sent.append(text) or len(sent),
    )

    telegram_bot.open_catalog("token", cfg, "777", user={"username": "ivan", "role": "user"})

    assert sent == ["Нет доступных вам документов."]
    assert telegram_bot.CATALOG_SESSIONS == {}


def test_bot_catalog_keeps_permitted_documents(monkeypatch, tmp_path) -> None:
    root = tmp_path / "catalog"
    root.mkdir(parents=True)
    allowed = root / "план.docx"
    allowed.write_text("plan", encoding="utf-8")
    _allow_only(monkeypatch, {str(allowed)})

    cfg = {"cloud_drive_db_path": "cloud_drive.db", "catalog_path": str(root)}
    monkeypatch.setattr(telegram_bot, "_catalog_load_paths", lambda _cfg: [str(allowed)])
    monkeypatch.setattr(
        telegram_bot,
        "send_message",
        lambda token, chat_id, text, reply_markup=None: 1,
    )

    telegram_bot.CATALOG_SESSIONS.clear()
    telegram_bot.open_catalog("token", cfg, "777", user={"username": "ivan", "role": "user"})

    sessions = list(telegram_bot.CATALOG_SESSIONS.values())
    assert len(sessions) == 1
    assert sessions[0]["all_paths"] == [str(allowed)]
    telegram_bot.CATALOG_SESSIONS.clear()


def test_acl_filter_fails_closed_when_helper_raises(monkeypatch) -> None:
    import rag_catalog.ui.helpers as ui_helpers

    def _boom(cfg, user, results):
        raise RuntimeError("registry down")

    monkeypatch.setattr(ui_helpers, "_filter_cloud_drive_search_results", _boom)

    assert telegram_bot.acl_filter_paths(_ACL_CFG, {"username": "ivan"}, [r"O:\Обмен\a.docx"]) == []


def test_catalog_sessions_expire_by_ttl(monkeypatch) -> None:
    telegram_bot.CATALOG_SESSIONS.clear()
    telegram_bot.CATALOG_SESSIONS["old"] = {
        "created_at": time.time() - telegram_bot.SEARCH_SESSION_TTL_SEC - 10,
        "all_paths": ["x"],
    }
    telegram_bot.CATALOG_SESSIONS["fresh"] = {"created_at": time.time(), "all_paths": ["y"]}

    telegram_bot._cleanup_search_sessions()

    assert set(telegram_bot.CATALOG_SESSIONS) == {"fresh"}
    telegram_bot.CATALOG_SESSIONS.clear()
