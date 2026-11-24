from types import SimpleNamespace

from rag_catalog.integrations.employee_portal import dispatch_notice, employee_office_url, reviewer_notice


def test_new_request_notifies_only_current_org_admins_once(monkeypatch):
    staff = {"status": "active", "organization_roles": {"east": ["admin"]}, "telegram_chat_id": "21"}
    users = [staff, {**staff, "telegram_chat_id": "22", "organization_roles": {"west": ["admin"]}},
             {**staff, "telegram_chat_id": "23", "status": "blocked"},
             {**staff, "telegram_chat_id": "24", "must_change_password": True},
             {**staff, "telegram_chat_id": "25", "max_user_id": "26", "is_platform_admin": True}]
    notice = reviewer_notice(SimpleNamespace(list_users=lambda: users), "east", 8)
    delivered = []
    monkeypatch.setattr("rag_catalog.integrations.telegram_bot.send_message",
                        lambda _, sender, message: delivered.append((sender, message)))
    assert dispatch_notice({"telegram_enabled": True, "telegram_bot_token": "fixture"}, notice)
    assert [sender for sender, _ in delivered] == ["21", "25"]
    assert all("№8" in message for _, message in delivered)


def test_employee_links_only_use_configured_https_origin(tmp_path):
    cfg = {"max_state_dir": str(tmp_path)}
    assert employee_office_url(cfg) == ""
    assert employee_office_url({**cfg, "employee_office_url": "https://office.test/"}) == "https://office.test"
    assert employee_office_url({**cfg, "employee_office_url": "http://office.test"}) == ""
