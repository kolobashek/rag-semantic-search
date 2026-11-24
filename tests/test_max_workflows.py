from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from rag_catalog.core.shift_attachments import MAX_FILE_BYTES
from rag_catalog.core.user_auth_db import UserAuthDB
from rag_catalog.integrations.max_api import MaxEvent, MaxFile, normalize_update
from rag_catalog.integrations.max_workflows import MaxWorkflows


@pytest.fixture
def bot(tmp_path):
    auth = UserAuthDB(str(tmp_path / "users.sqlite"))
    client = Mock()
    worker = MaxWorkflows(dict(work_shifts_db_path=str(tmp_path / "shifts.sqlite"),
                               users_db_path=str(tmp_path / "users.sqlite"),
                               max_state_dir=str(tmp_path / "max"),
                               max_office_url="https://office.example", max_bot_link="https://max.ru/office_bot"), auth, client)
    for uid, name, roles in ((11, "alice", ["driver"]), (22, "bob", ["driver"]), (33, "dispatch", ["dispatcher"]),
                             (44, "office", ["user"]), (55, "chief", ["admin"])):
        auth.admin_create_user(username=name, roles=roles, password="test-password",
                               must_change_password=False, display_name=name.title())
        session = auth.create_session(username=name)
        token = auth.create_max_link_token(session_token=session)["token"]
        event = MaxEvent(uid, f"start-{uid}", f"/start link_{token}")
        worker.handle(event)
        assert auth.get_user_by_max_user_id(uid)["username"] == name
    client.reset_mock()
    return worker


def response(bot, uid, text):
    bot.client.reset_mock()
    bot.handle(MaxEvent(uid, "mid", text))
    return bot.client.send_message.call_args.args[1]

def click(bot, uid, label):
    buttons = bot.client.send_message.call_args.kwargs["buttons"]
    button = next(b for row in buttons for b in row if b["text"] == label)
    bot.handle(MaxEvent(uid, "callback-" + button["payload"], button["payload"], callback_id="button"))
    return bot.client.send_message.call_args.args[1]


def test_first_start_and_navigation_from_normalized_max_updates(bot):
    def deliver(update):
        event = normalize_update(update)
        assert event is not None
        bot.client.reset_mock()
        bot.handle(event)
        call = bot.client.send_message.call_args
        assert call.args[0] == event.user_id
        return call.args[1], {b["text"]: b for row in call.kwargs["buttons"] for b in row}

    def start(uid, payload=None):
        return deliver({"update_type": "bot_started", "timestamp": 1573226679188, "chat_id": 500 + uid,
                        "user": {"user_id": uid, "first_name": "Иван", "is_bot": False},
                        "payload": payload, "user_locale": "ru"})

    def message(uid, text):
        return deliver({"update_type": "message_created", "message": {
            "recipient": {"chat_type": "dialog"}, "sender": {"user_id": uid},
            "body": {"mid": "mid." + text, "text": text}}})

    def press(uid, buttons, label):
        return deliver({"update_type": "message_callback", "message": {"recipient": {"chat_type": "dialog"}},
                        "callback": {"callback_id": "cb." + label, "user": {"user_id": uid},
                                     "payload": buttons[label]["payload"]}})

    welcome, guest = start(90)
    assert "Здравствуйте" in welcome and "Нажмите кнопку под сообщением" in welcome
    assert list(guest) == ["Подать заявку", "У меня уже есть кабинет", "Статус заявки"]
    assert message(90, "Привет") == (welcome, guest)
    _, existing = press(90, guest, "У меня уже есть кабинет")
    assert existing["Открыть кабинет"]["url"] == bot.office_url
    assert press(90, existing, "Главное меню") == (welcome, guest)
    prompt, _ = press(90, guest, "Подать заявку")
    assert "фамилию" in prompt.lower()
    assert start(90, "menu") == (welcome, guest)
    assert message(90, "Иван Тестов") == (welcome, guest)  # Menu link closed the unfinished request.
    assert message(90, "/start") == (welcome, guest)
    assert bot.auth.submit_employee_request(provider="max", sender_id="90", display_name="Иван Тестов")["ok"]
    pending, actions = start(90)
    assert "ожидает" in pending
    assert start(90, "menu") == (welcome, guest)
    assert press(90, actions, "Главное меню") == (welcome, guest)
    assert "ожидает" in press(90, guest, "Статус заявки")[0]

    for uid, role, action in [(11, "Водитель", "Ближайшие смены"), (33, "Диспетчер", "Ближайшие смены"),
                              (44, "Пользователь облака", "Офис"), (55, "Администратор", "Заявки")]:
        title, menu = start(uid)
        assert role in title and action in menu and "Нажмите кнопку под сообщением" in title
        assert message(uid, "Привет!") == (title, menu)
        assert message(uid, "Начать") == (title, menu)
        if uid != 44:
            assert menu["Открыть кабинет"] == {"type": "link", "text": "Открыть кабинет",
                                              "url": "https://office.example/shifts"}
            assert "Офис" not in menu and "Поиск по имени" not in menu
        if uid == 11:
            assert "нужна роль" in message(uid, "/office")[0]
        assert message(uid, "/start") == (title, menu)
        assert message(uid, "Главное меню") == (title, menu)
        profile, buttons = press(uid, menu, "Мой профиль")
        assert role in profile
        if uid == 55:
            assert "Водитель" not in title and "Водитель" not in profile
        _, details = press(uid, menu, action)
        assert press(uid, details, "Главное меню") == (title, menu)
        assert press(uid, buttons, "Главное меню") == (title, menu)


def test_greeting_preserves_name_prompts_and_file_handling(bot):
    assert "фамилию" in response(bot, 55, "/add_user").lower()
    shift = response(bot, 55, f"/new_shift {date.today()} | 1 | ООО | Кран | Объект | 8 | 123")
    assert "Смена #1 записана" in shift
    assert "10 минут" in response(bot, 55, "/attach_next 1")
    photo = normalize_update({"update_type": "message_created", "message": {
        "recipient": {"chat_type": "dialog"}, "sender": {"user_id": 55}, "body": {
            "mid": "photo", "attachments": [{"type": "image", "payload": {"url": "https://cdn.max.ru/photo"}}]}}})
    bot.client.download_file.side_effect = lambda _, dest, max_bytes: dest.write_bytes(b"photo")
    assert "Путевые листы приложены" in response_with_media(bot, photo)
    review = response(bot, 55, "Добрый День")
    assert "Добрый День" in review and "Рабочий кабинет" not in review

    event = MaxEvent(11, "file-with-caption", "Привет!", (MaxFile("scan.pdf", "https://cdn.max.ru/signed", 3),))
    assert "отправьте файл" in response_with_media(bot, event)


def test_unexpected_private_input_shows_buttons_for_the_sender(bot):
    def send(uid, body):
        event = normalize_update({"update_type": "message_created", "message": {
            "recipient": {"chat_type": "dialog"}, "sender": {"user_id": uid}, "body": body}})
        assert event is not None
        bot.handle(event)
        return bot.client.send_message.call_args

    for uid, action in [(90, "Подать заявку"), (11, "Ближайшие смены"), (44, "Офис"), (55, "Заявки")]:
        menu = send(uid, {"mid": "menu", "text": "/menu"})
        assert action in {b["text"] for row in menu.kwargs["buttons"] for b in row}
        assert send(uid, {"mid": "unsupported", "text": "/хочу_домой сейчас"}) == menu
        assert send(uid, {"mid": "empty"}) == menu
        assert send(uid, {"mid": "sticker", "attachments": [{"type": "sticker", "payload": {"code": "1"}}]}) == menu
        if uid != 44:
            assert send(uid, {"mid": "arbitrary", "text": "Куда мне дальше?"}) == menu
        else:
            offline = send(uid, {"mid": "search", "text": "инструкция"})
            assert "каталог ещё не настроен" in offline.args[1] and offline.kwargs == menu.kwargs


def test_free_text_and_stickers_repeat_active_button_step(bot):
    assert "фамилию" in response(bot, 91, "/request")
    review = response(bot, 91, "Елена Тестова")
    buttons = bot.client.send_message.call_args.kwargs["buttons"]
    assert "Отправить заявку" in {b["text"] for row in buttons for b in row}
    for msg in ["Как подтвердить?", ""]:
        assert response(bot, 91, msg) == review
        assert bot.client.send_message.call_args.kwargs["buttons"] == buttons
    assert bot.auth.employee_request_status(provider="max", sender_id="91")["status"] == "none"
    assert "ожидает решения" in click(bot, 91, "Отправить заявку")
    assert "Здравствуйте" in response(bot, 91, "Как дела?")


def test_menus_login_and_employee_invites(bot):
    assert "привяжите MAX" in response(bot, 123, "/menu")
    assert "Водитель" in response(bot, 11, "/menu")
    buttons = bot.client.send_message.call_args.kwargs["buttons"]
    assert "/add_user" not in repr(buttons) and "/files" not in repr(buttons)
    assert "администратору" in response(bot, 11, "/add_user jane admin Jane")
    assert bot.auth.get_user(username="jane") is None
    response(bot, 55, "/add_user jane user,driver Jane")
    assert bot.auth.get_user(username="jane") is None
    click(bot, 55, "Далее")
    initial = click(bot, 55, "Создать приглашение").split("?start=invite_")[1]
    response(bot, 55, "/reinvite jane")
    replacement = click(bot, 55, "Обновить приглашение").split("?start=invite_")[1]
    assert "недоступно" in response(bot, 77, "/start invite_" + initial)
    response(bot, 77, "/start invite_" + replacement)
    token = bot.client.send_message.call_args.kwargs["buttons"][0][0]["url"].split("#token=")[1]
    assert bot.auth.activate_account(token=token, new_password="staff-secret")["ok"]
    assert set(bot.auth.get_user(username="jane")["roles"]) == {"user", "driver"}
    assert "#token=" not in response(bot, 55, "/reinvite jane")
    challenge = bot.auth.create_max_login_challenge()
    assert "сами начали" in response(bot, 44, "/start login_" + challenge["token"])
    button = bot.client.send_message.call_args.kwargs["buttons"][0][0]
    assert "кнопкой" in response(bot, 44, button["payload"])
    assert bot.auth.consume_confirmed_max_login(verifier=challenge["verifier"])["reason"] == "pending"
    bot.handle(MaxEvent(44, "callback-1", button["payload"], callback_id="callback-1"))
    assert bot.auth.consume_confirmed_max_login(verifier=challenge["verifier"])["username"] == "office"


def test_shifts_waybills_visibility_upload_replay_and_dates(bot):
    day = (date.today() + timedelta(days=2)).isoformat()
    assert "недоступен" in response(bot, 44, "/shifts")
    cmd = f"/new_shift {day} | 2 | Компания | Кран | Платформа | 7,5 | 237"
    assert "другого" in response(bot, 11, cmd + " | bob")
    assert "Смена #1 записана" in response(bot, 11, cmd)
    assert "Смена #1 записана" in response(bot, 11, cmd)
    assert "Платформа" in response(bot, 11, "/shifts")
    assert "Платформа" not in response(bot, 22, "/shifts")
    assert "Alice" in response(bot, 33, f"/shifts {day}")
    media = MaxFile("../scan.pdf", "https://cdn.max.ru/signed", 3)
    event = MaxEvent(22, "message:22:123", "/waybill 1", (media,))
    bot.handle(event)
    bot.client.download_file.assert_not_called()
    assert "недоступна" in bot.client.send_message.call_args.args[1]
    event = replace(event, user_id=11)
    bot.client.download_file.side_effect = lambda _, dest, max_bytes: dest.write_bytes(b"PDF")
    bot.handle(event)
    bot.handle(event)
    assert "Приложено файлов: 1" in response(bot, 11, "/waybill 1")
    files = []
    bot.client.send_document.side_effect = lambda uid, path: files.append((uid, path.name, path.read_bytes()))
    bot.handle(MaxEvent(33, "cb", "/attachment 1 1", callback_id="1"))
    assert files == [(33, "scan.pdf", b"PDF")]
    assert "недоступна" in response(bot, 22, "/attachment 1 1")
    assert "Максимальный" in response_with_media(bot, replace(event, files=(replace(media, size=MAX_FILE_BYTES + 1),)))
    bot.auth.unlink_max_user(11)
    assert "привяжите MAX" in response(bot, 11, "/attachment 1 1")


def response_with_media(bot, event):
    bot.client.reset_mock()
    bot.handle(event)
    return bot.client.send_message.call_args.args[1]


def test_cloud_search_and_download_recheck_access(bot):
    bot.cfg.update(cloud_drive_db_path="configured", cloud_drive_acl={"users": {"office": ["Office"]}})
    doc = SimpleNamespace(id="file1", path="Office/report.pdf", name="report.pdf",
                          storage_key="objects/123", deleted_at="")
    bot.drive = Mock()
    bot.drive.search_nodes.return_value = {
        "items": [{"node_type": "file", "path": path, "id": f"file{i}"} for i, path in enumerate(
            ["Personal/report.pdf", "Office/report.pdf", "Office/secret.pdf"])], "next_offset": None}
    bot.drive.user_access_map.return_value = {("Office/report.pdf", "file1"): True}
    bot.drive.registry.get_file_by_id.return_value = doc
    assert "Выберите файл" in response(bot, 44, "/files report")
    buttons = bot.client.send_message.call_args.kwargs["buttons"]
    assert [btn["payload"] for row in buttons for btn in row] == ["/download file1", "/menu"]
    assert "офисному каталогу" in response(bot, 11, "/download file1")
    bot.drive.registry.get_file_by_id.return_value = doc
    bot.drive.user_can_access.return_value = False
    assert "Файл недоступен" in response(bot, 44, "/download file1")
    bot.drive.storage.download_file.assert_not_called()
    bot.drive.user_can_access.return_value = True
    allowed = []
    bot.drive.storage.download_file.side_effect = lambda key, path: path.write_bytes(b"file")
    bot.client.send_document.side_effect = lambda uid, path: allowed.append((uid, Path(path).read_bytes()))
    bot.handle(MaxEvent(44, "cb1", "/download file1", callback_id="cb1"))
    assert allowed == [(44, b"file")]
    bot.drive.user_can_access.side_effect = [True, False]
    assert "Файл недоступен" in response(bot, 44, "/download file1")
    bot.client.send_document.assert_not_called()


def test_office_upload_rag_search_and_unlink(bot, tmp_path, monkeypatch):
    from rag_catalog.ui import helpers
    bot.cfg.update(cloud_drive_db_path=str(tmp_path / "drive.sqlite"), cloud_drive_storage="local",
                   cloud_drive_storage_root=str(tmp_path / "objects"), cloud_drive_max_upload_mb=1)
    media = MaxFile("doc.pdf", "https://cdn.max.ru/test", 4)
    event = MaxEvent(44, "msg-44", "/upload", (media,))
    bot.client.download_file.side_effect = lambda doc, path, max_bytes: path.write_bytes(b"scan")
    assert "приняты" in response_with_media(bot, event)
    assert "приняты" in response_with_media(bot, event)  # Same delivery, no version or job inflation.
    assert bot.drive.registry.stats().pending_jobs == 1
    assert bot.drive.registry.stats().versions == 1
    original = bot.drive.registry.get_file_by_path("office/doc.pdf")
    assert original
    assert "приняты" in response_with_media(bot, replace(event, event_id="msg-45"))
    assert bot.drive.registry.stats().versions == 2
    assert "офисному каталогу" in response_with_media(bot, replace(event, user_id=22))

    source = tmp_path / "private.pdf"
    source.write_bytes(b"secret")
    home = bot.drive.ensure_user_home_folder(username="chief")
    private = bot.drive.upload_file(parent_path=home["path"], filename="private.pdf", source_path=str(source))
    visible = {"filename": "doc.pdf", "cloud_file_id": original.id, "chunk_text": "Visible text"}
    hidden = {"filename": "private.pdf", "cloud_file_id": private["id"], "chunk_text": "hidden text"}
    fake = Mock()
    fake.search.return_value = [hidden, visible]  # Recheck results even if the retriever ignores result_filter.
    monkeypatch.setattr(helpers, "_cached_searcher_if_ready", lambda *a, **kw: fake)
    answer = response(bot, 44, "What files are there?")
    assert "Visible text" in answer and "hidden text" not in answer
    assert fake.search.call_args.kwargs["result_filter"]([visible, hidden])[0]["filename"] == "doc.pdf"
    assert "/download " + original.id in repr(bot.client.send_message.call_args.kwargs["buttons"])
    fake.search.return_value = [hidden]
    assert "Доступных документов по запросу не найдено" in response(bot, 44, "другой запрос")
    labels = {b["text"] for row in bot.client.send_message.call_args.kwargs["buttons"] for b in row}
    assert {"Офис", "Поиск по имени", "Мой профиль"} <= labels
    assert not any(label.startswith("Скачать") for label in labels)
    assert "Логин: office" in response(bot, 44, "/whoami")
    response(bot, 44, "/logout confirm office")
    assert bot.auth.get_user_by_max_user_id(44) is not None
    payload = bot.client.send_message.call_args.kwargs["buttons"][0][0]["payload"]
    bot.handle(MaxEvent(44, "cb", payload, callback_id="cb"))
    assert not bot.auth.get_user_by_max_user_id(44)
    assert "привяжите MAX" in response(bot, 44, "/files отчет")
from concurrent.futures import ThreadPoolExecutor


def test_next_waybill_persists_binds_expires_and_claims_once(bot, monkeypatch):
    from rag_catalog.integrations import max_waybill

    response(bot, 11, f"/new_shift {date.today()} | 1 | ООО | Грузовик | Шахта | 8 | 334")
    response(bot, 11, "/waybill 1")
    payload = bot.client.send_message.call_args.kwargs["buttons"][0][0]["payload"]
    choose = MaxEvent(11, "choose-1", payload, callback_id="chosen")
    bot.handle(choose)
    assert "10 минут" in bot.client.send_message.call_args.args[1]
    photo = MaxFile("photo.jpg", "https://cdn.max.ru/photo", 4, "image")
    image = MaxEvent(11, "photo-1", files=(photo,))
    bot.client.download_file.side_effect = lambda doc, path, max_bytes: path.write_bytes(b"test")
    # A second worker must see the selection. Concurrent messages cannot claim the same prompt.
    again = MaxWorkflows(bot.cfg, bot.auth, bot.client)
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(again.handle, [image, replace(image, event_id="photo-2")]))
    assert "Приложено файлов: 1" in response(bot, 11, "/waybill 1")
    again.handle(image)
    again.handle(replace(image, event_id="photo-2"))
    again.handle(choose)
    assert "завершён" in bot.client.send_message.call_args.args[1]
    assert "Приложено файлов: 1" in response(bot, 11, "/waybill 1")

    # A different driver cannot claim the selection; even re-linking the same employee invalidates it.
    bot.handle(replace(choose, event_id="choose-2"))
    assert "НОМЕР_ЗАПИСИ" in response_with_media(bot, replace(image, user_id=22, event_id="bob-photo"))
    bot.auth.unlink_max_user(11)
    session = bot.auth.create_session(username="alice")
    token = bot.auth.create_max_link_token(session_token=session)["token"]
    bot.handle(MaxEvent(11, "relink", "/start link_" + token))
    assert "НОМЕР_ЗАПИСИ" in response_with_media(bot, replace(image, event_id="after-relink"))

    clock = SimpleNamespace(time=lambda: 1000)
    monkeypatch.setattr(max_waybill, "time", clock)
    bot.handle(replace(choose, event_id="choose-3"))
    clock.time = lambda: 1601
    assert "НОМЕР_ЗАПИСИ" in response_with_media(bot, replace(image, event_id="expired-photo"))
