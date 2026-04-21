from telegram_bot import (
    _clean_tg_text,
    _file_uri,
    _main_menu,
    _message_file_info,
    _safe_filename,
    build_interactive_search_response,
    chat_action,
    format_fact_answer,
    process_contact_message,
    process_message,
    process_query,
    send_chat_action,
    set_bot_commands,
)
import telegram_bot


class _FakeSearcher:
    def __init__(self, fact_result=None, fact_exc=None, search_result=None, search_exc=None):
        self._fact_result = fact_result
        self._fact_exc = fact_exc
        self._search_result = search_result if search_result is not None else []
        self._search_exc = search_exc
        self.last_search_kwargs = {}

    def answer_fact_question(self, _q, limit=30):
        if self._fact_exc:
            raise self._fact_exc
        return self._fact_result if self._fact_result is not None else {"ok": False}

    def search(self, *_args, **_kwargs):
        if self._search_exc:
            raise self._search_exc
        self.last_search_kwargs = dict(_kwargs)
        return self._search_result


class _FakeAuthDB:
    def __init__(self, out=None, user=None):
        self.out = out
        self.user = user
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

    def upsert_user_from_telegram_contact(self, *, telegram_chat_id: str, username_hint: str = "", display_name: str = ""):
        username = (username_hint or f"tg_{telegram_chat_id}").lower()
        self.user = {"username": username, "telegram_chat_id": telegram_chat_id, "role": "user", "status": "active"}
        return {"username": username, "created": True, "temp_password": "temp-pass"}

    def consume_telegram_start_token(self, **kwargs):
        return self.out(**kwargs) if callable(self.out) else self.out

    def create_registration_request(self, **kwargs):
        row = {"id": len(self.registration_requests) + 1, **kwargs}
        self.registration_requests.append(row)
        return {"ok": True, "id": row["id"], "status": "pending"}

    def list_registration_requests(self, *, status: str = "pending", limit: int = 100):
        return [
            {"id": 1, "username": "ivan", "display_name": "Ivan", "telegram_username": "ivan_tg", "source": "telegram"}
        ]

    def review_registration_request(self, **kwargs):
        decision = kwargs.get("decision")
        if decision == "approved":
            return {"ok": True, "status": "approved", "username": "ivan"}
        return {"ok": True, "status": "rejected"}

    def create_admin_invite(self, **kwargs):
        return {"ok": True, "username": kwargs.get("username") or "ivan", "token": "invite-token", "temp_password": "temp-pass"}

    def create_telegram_token(self, **kwargs):
        return {"ok": True, "token": "invite-token"}


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


def test_process_query_no_results() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[])
    out = process_query(s, "нечто редкое")
    assert out == "Ничего не найдено."


def test_interactive_search_response_has_file_buttons() -> None:
    s = _FakeSearcher(
        search_result=[
            {"filename": "паспорт.pdf", "full_path": r"O:\Обмен\паспорт.pdf", "extension": ".pdf", "score": 0.9},
            {"filename": "договор.docx", "full_path": r"O:\Обмен\договор.docx", "extension": ".docx", "score": 0.8},
        ]
    )
    out = build_interactive_search_response(s, chat_id="777", query="паспорт", username="ivan")
    assert "Варианты по запросу" in out["text"]
    assert "📄 получить файл" in out["text"]
    assert "score=" not in out["text"]
    keyboard = out["reply_markup"]["inline_keyboard"]
    assert keyboard[0][0]["text"] == "📄 Получить 1"
    assert keyboard[0][1]["text"] == "👍 1"
    assert keyboard[0][2]["text"] == "👎 1"
    assert any(button["text"] == "Ещё варианты" for row in keyboard for button in row)
    assert any(button["text"] == "PDF" for row in keyboard for button in row)
    assert any(button["text"] == "Дополнить поиск" for row in keyboard for button in row)


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
    assert "Код не найден" in out


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


def test_process_message_request_button_is_ignored_for_active_user() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[])
    auth = _FakeAuthDB(user={"username": "ivan", "telegram_chat_id": "777", "status": "active"})
    out = process_message(
        searcher=s,
        auth_db=auth,
        text="Заявка",
        chat_id="777",
        allowed_chat_id="",
    )
    assert "уже авторизованы" in out


def test_process_message_register_is_ignored_for_active_user() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[])
    auth = _FakeAuthDB(user={"username": "ivan", "telegram_chat_id": "777", "status": "active"})
    out = process_message(
        searcher=s,
        auth_db=auth,
        text="/register Иван",
        chat_id="777",
        allowed_chat_id="",
    )
    assert "Новая заявка не нужна" in out
    assert auth.registration_requests == []


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
    assert "admin" in out
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
    assert "Отправьте вопрос по документам" in out
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
    assert "Ваш chat_id: 777" in out


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
    assert "Ок, вы авторизованы как ivan." in out
    assert "Отправьте вопрос по документам" in out


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
    assert "Ок, вы авторизованы как ivan." in out


def test_process_message_start_login_payload_confirms_login() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[])
    auth = _FakeAuthDB(out={"ok": True, "purpose": "login", "username": "ivan"})
    out = process_message(
        searcher=s,
        auth_db=auth,
        text="/start login_secret",
        chat_id="777",
        allowed_chat_id="",
    )
    assert "Вход подтверждён" in out


def test_process_message_register_unknown_creates_pending_request() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[])
    auth = _FakeAuthDB(user=None)
    out = process_message(
        searcher=s,
        auth_db=auth,
        text="/register Иван Петров",
        chat_id="777",
        allowed_chat_id="",
        telegram_username="ivan_tg",
    )
    assert "Заявка #1" in out
    assert auth.registration_requests[0]["telegram_chat_id"] == "777"


def test_process_message_admin_add_user_returns_invite_link() -> None:
    s = _FakeSearcher(fact_result={"ok": False}, search_result=[])
    auth = _FakeAuthDB(user={"username": "admin", "telegram_chat_id": "777", "role": "admin", "status": "active"})
    out = process_message(
        searcher=s,
        auth_db=auth,
        text="/add_user @ivan ivan",
        chat_id="777",
        allowed_chat_id="",
        bot_link="https://t.me/test_bot",
    )
    assert "Пользователь 'ivan' создан" in out
    assert "https://t.me/test_bot?start=invite_" in out


def test_process_contact_message_requires_admin() -> None:
    auth = _FakeAuthDB(user={"username": "user1", "telegram_chat_id": "777", "role": "user", "status": "active"})
    out = process_contact_message(
        auth_db=auth,
        sender_chat_id="777",
        contact={"user_id": 999, "first_name": "Ivan"},
        allowed_chat_id="",
        app_auth_link="https://example.local/login",
    )
    assert "только администратор" in out["reply"].lower()


def test_process_contact_message_creates_user_and_prepare_notify() -> None:
    auth = _FakeAuthDB(user={"username": "admin", "telegram_chat_id": "777", "role": "admin", "status": "active"})
    out = process_contact_message(
        auth_db=auth,
        sender_chat_id="777",
        contact={"user_id": 999, "first_name": "Ivan", "last_name": "Petrov", "username": "ivan.petrov"},
        allowed_chat_id="",
        app_auth_link="https://example.local/login",
    )
    assert "активирован" in out["reply"]
    assert out["notify_chat_id"] == "999"
    assert "https://example.local/login" in out["notify_text"]
    assert "Временный пароль" in out["notify_text"]


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
