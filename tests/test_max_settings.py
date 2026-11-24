import pytest

from rag_catalog.integrations import max_settings as setup


class StubAPI:
    calls = []
    fail = False
    commands = []
    patches = []
    subscriptions_live = []

    def __init__(self, token):
        assert token == "test-token"

    def get_me(self):
        return {"username": "employee_bot", "commands": self.commands}

    def set_commands(self, commands):
        self.patches.append(commands)
        self.commands[:] = commands

    def subscriptions(self):
        return self.subscriptions_live

    def subscribe(self, url, secret):
        self.calls.append((url, secret))
        if self.fail:
            raise setup.MaxAPIError("temporary")
        self.subscriptions_live[:] = [{"url": url, "update_types": list(setup.UPDATE_TYPES)}]


@pytest.fixture(autouse=True)
def isolated_api(monkeypatch):
    for key in ("calls", "commands", "patches", "subscriptions_live"):
        monkeypatch.setattr(StubAPI, key, [])
    monkeypatch.setattr(setup, "MaxClient", StubAPI)


def test_install_retry_keeps_secret_and_does_not_retarget_another_bot(tmp_path, monkeypatch):
    monkeypatch.setattr(setup, "MaxClient", StubAPI)
    monkeypatch.setattr(StubAPI, "calls", [])
    cfg = {"max_state_dir": str(tmp_path)}
    assert setup.public_bot_link(cfg) == ""
    link = "https://max.ru/employee_bot"
    origin = "https://office.example"
    with pytest.raises(ValueError, match="другому боту"):
        setup.configure(cfg, origin, "https://max.ru/old_bot", "test-token")
    assert not list(tmp_path.iterdir())
    monkeypatch.setattr(StubAPI, "fail", True)
    with pytest.raises(setup.MaxAPIError):
        setup.configure(cfg, origin, link, "test-token")
    assert setup.read_settings(cfg).secret
    assert not setup.public_bot_link(cfg)
    monkeypatch.setattr(StubAPI, "fail", False)
    settings = setup.configure(cfg, origin + "/", link, "test-token")
    assert StubAPI.calls == [(origin + "/api/max/webhook", settings.secret)] * 2
    assert setup.read_settings(cfg) == settings
    assert setup.public_bot_link(cfg) == link
    assert settings.secret not in repr(settings) and settings.token not in repr(settings)
    with pytest.raises(ValueError, match="другому боту или домену"):
        setup.configure(cfg, "https://other.example", link, "test-token")
    assert setup.read_settings(cfg) == settings


@pytest.mark.parametrize("url", ["http://office.test", "https://user@office.test",
                                 "https://office.test:444", "https://office.test/a"])
def test_setup_rejects_unusable_webhook_origins(url, tmp_path):
    with pytest.raises(ValueError):
        setup.configure({"max_state_dir": str(tmp_path)}, url, "https://max.ru/employee_bot", "dummy")


def test_reconcile_preserves_unrelated_commands_and_repeated_setup_is_idempotent(tmp_path, monkeypatch, capsys):
    from rag_catalog.core import rag_core

    custom = {"name": "support", "description": "Помощь"}
    StubAPI.commands.extend([custom, {"name": "menu", "description": "Old"}])
    cfg = {"max_state_dir": str(tmp_path)}
    monkeypatch.setattr(rag_core, "load_config", lambda: cfg)
    setup.configure(cfg, "https://office.test", "https://max.ru/employee_bot", "test-token")
    assert StubAPI.commands[0] == custom
    assert {cmd["name"] for cmd in StubAPI.commands} == {"menu", "request", "status", "start", "support"}
    assert setup.main(["recover"]) == setup.main(["commands"]) == setup.main(["status"]) == 0
    assert len(StubAPI.patches) == len(StubAPI.calls) == 1
    StubAPI.commands.remove(next(cmd for cmd in StubAPI.commands if cmd["name"] == "request"))
    assert setup.main(["status"]) == 2
    assert len(StubAPI.patches) == 1
    assert setup.main(["commands"]) == 0
    assert len(StubAPI.patches) == 2 and len(StubAPI.calls) == 1
    assert "test-token" not in capsys.readouterr().out


@pytest.mark.parametrize("failure", ["full", "network", "stale_response", "bad_list"])
def test_menu_failure_keeps_webhook_and_reports_retry(tmp_path, monkeypatch, failure):
    cfg = {"max_state_dir": str(tmp_path)}
    link = "https://max.ru/employee_bot"
    if failure == "full":
        StubAPI.commands.extend({"name": f"custom{i}"} for i in range(30))
    elif failure == "bad_list":
        StubAPI.commands.append({"description":"нет имени"})
    else:
        def fail_or_ignore(self, commands):
            if failure == "network":
                raise setup.MaxAPIError("MAX /me/commands: HTTP 503")
        monkeypatch.setattr(StubAPI, "set_commands", fail_or_ignore)
    with pytest.raises(setup.MenuSetupPending, match="max_settings commands") as error:
        setup.configure(cfg, "https://office.test", link, "test-token")
    assert setup.public_bot_link(cfg) == link
    assert "test-token" not in str(error.value)
    if failure in {"full","bad_list"}:
        assert not StubAPI.patches
