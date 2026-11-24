import sqlite3
from itertools import count

import pytest

from rag_catalog.core.user_auth_db import UserAuthDB
from rag_catalog.integrations.employee_onboarding import OnboardingFlow
from rag_catalog.integrations.max_api import normalize_update


class Conversation:
    def __init__(self, db, provider, sender, username=None):
        self.db, self.provider, self.sender, self.username = db, provider, sender, username
        self.ids = count()

    def send(self, text, *, callback=False, contact=None):
        # Each incoming event constructs the same workflow as a fresh process.
        bot = "https://max.ru/new_bot" if self.provider == "max" else "https://t.me/new_bot"
        flow = OnboardingFlow(self.db, provider=self.provider, bot_link=bot, office_url="https://office.example")
        user = self.db.get_user(username=self.username) if self.username else None
        self.reply = flow.handle(sender_id=self.sender, text=text, display_name="Имя профиля", user=user,
                                 is_callback=callback, contact=contact, event_id=str(next(self.ids)))
        return self.reply

    def click(self, label):
        b = next(b for row in self.reply.buttons for b in row if b["text"].startswith(label))
        assert len(b["payload"].encode()) <= 64
        return self.send(b["payload"], callback=True)


@pytest.fixture
def staff(tmp_path, monkeypatch):
    monkeypatch.setenv("RAG_DISABLE_DEFAULT_ADMIN", "1")
    db = UserAuthDB(str(tmp_path / "staff.sqlite"))
    db.admin_create_user(username="office_admin", roles=["admin"], password="test-password", must_change_password=False)
    db.admin_create_user(username="west_admin", roles=["admin"], password="test-password", must_change_password=False)
    with sqlite3.connect(db.db_path) as conn:
        conn.execute("INSERT INTO employee_organizations VALUES('west','Запад')")
        conn.execute("UPDATE employee_memberships SET organization_id='west' WHERE username='west_admin'")
    return db


@pytest.mark.parametrize("provider", ["max", "telegram"])
def test_self_request_organization_scope_confirmation_and_status(staff, provider):
    applicant = Conversation(staff, provider, 987)
    reviewer = Conversation(staff, "telegram" if provider == "max" else "max", 22, "west_admin")
    outsider = Conversation(staff, provider, 33, "office_admin")
    applicant.send("/start")
    applicant.click("Подать заявку")
    applicant.click("Запад")
    applicant.send("Анна Водитель")
    assert not staff.list_employee_requests(admin_username="west_admin")
    assert "ожидает" in applicant.click("Отправить заявку").text
    assert "ожидает" in applicant.send("/status").text
    assert "рассмотрении нет" in outsider.send("/requests").text
    assert "недоступна" in outsider.send("/approve 1").text
    reviewer.send("/requests")
    reviewer.click("#1")
    choices = [b["text"] for row in reviewer.reply.buttons for b in row if b["text"].startswith("✓")]
    assert choices == ["✓ Водитель"]
    reviewer.click("Далее")
    summary = reviewer.reply.text
    assert "Запад" in summary and "Водитель" in summary and "Администратор" not in summary
    approved = reviewer.click("Одобрить заявку")
    assert approved.notice["provider"] == provider and approved.notice["sender_id"] == "987"
    assert "token=" not in repr(approved) and "одобрена" in approved.text
    result = applicant.send("/status")
    activation = result.buttons[0][0]["url"].split("#token=")[1]
    assert staff.activate_account(token=activation, new_password="staff-password")["ok"]
    user = (staff.get_user_by_max_user_id(987) if provider == "max" else staff.get_user_by_telegram_chat_id("987"))
    assert user["organization_roles"] == {"west": ["driver"]}


def test_contact_is_hint_manual_invite_uses_the_recipient_and_rejects_stale_role_buttons(staff):
    admin = Conversation(staff, "max", 21, "office_admin")
    event = normalize_update({"update_type": "message_created", "message": {
        "sender": {"user_id": 21}, "recipient": {"chat_type": "dialog"},
        "body": {"mid": "contact-1", "attachments": [{"type": "contact", "payload": {
            "vcf_info": "BEGIN:VCARD\r\nFN:Иван Тестов\r\nTEL:+79990000000\r\nEND:VCARD",
            "max_info": {"user_id": 91},
        }}]},
    }})
    assert event.contact.name == "Иван Тестов" and event.contact.sender_id == "91"
    response = admin.send("", contact=event.contact)
    assert "Иван Тестов" in response.text and staff.get_user_by_max_user_id(91) is None
    old = next(b["payload"] for row in response.buttons for b in row if b["text"] == "Далее")
    admin.click("○ Диспетчер")
    elevated = admin.reply
    assert "устарела" in admin.send(old, callback=True).text
    admin.reply = elevated
    assert "Диспетчер" in admin.click("Далее").text
    personal = admin.click("Создать приглашение").text.split("?start=invite_")[1]
    assert staff.get_user_by_max_user_id(91) is None
    assert "недоступно" in Conversation(staff, "max", 999).send("/start invite_" + personal).text
    employee = Conversation(staff, "max", 91)
    claimed = employee.send("/start invite_" + personal)
    assert claimed.buttons[0][0]["url"].startswith("https://office.example/auth/activate#token=")
    assert staff.get_user_by_max_user_id(91) is None
    token = employee.send("/status").buttons[0][0]["url"].split("#token=")[1]
    assert staff.activate_account(token=token, new_password="staff-pass")["ok"]
    assert set(staff.get_user_by_max_user_id(91)["roles"]) == {"driver", "dispatcher"}


def test_plain_phone_cannot_claim_an_existing_employee_and_guest_can_resubmit_after_denial(staff):
    phone = normalize_update({"update_type": "message_created", "message": {
        "sender": {"user_id": 77}, "recipient": {"chat_type": "dialog"},
        "body": {"mid": "v", "attachments": [{"type": "contact", "payload": {
            "vcf_info": "BEGIN:VCARD\nTEL:12345\nX-USER-ID:12345\nEND:VCARD"}}]}}}).contact
    assert not phone.sender_id and not phone.name
    admin = Conversation(staff, "max", 77, "office_admin")
    prompt = admin.send("", contact=phone)
    assert prompt.pick_contact and staff.get_user_by_max_user_id(12345) is None
    admin.send("Ольга Водитель")
    admin.click("Далее")
    assert "invite_" in admin.click("Создать приглашение").text
    assert staff.get_user_by_max_user_id(12345) is None
    guest = Conversation(staff, "max", 12345)
    guest.send("/register Сотрудник")
    guest.click("Основная организация")
    guest.click("Отправить заявку")
    admin.send("/reject 1")
    assert "отклонена" in admin.click("Да, отклонить").text
    assert "отклонил" in guest.send("/status").text
    guest.click("Подать заявку")
    assert "организац" in guest.reply.text


def test_forwarded_contact_uses_current_sender_even_without_message_wrapper():
    event = normalize_update({"update_type": "message_created", "message": {
        "sender": {"user_id": 22}, "recipient": {"chat_type": "dialog"}, "timestamp": 1456780,
        "link": {"type": "forward", "sender": {"user_id": 77}, "message": {
            "mid": "mid.original", "attachments": [{"type": "contact", "payload": {
                "max_info": {"user_id": 51, "first_name": "Мария"}, "vcf_info": "BEGIN:VCARD\nEND:VCARD"}}]}}}})
    assert event.user_id == 22
    assert event.contact.name == "Мария" and event.contact.sender_id == "51"
