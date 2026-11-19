import pytest

from rag_catalog.integrations import max_settings as setup


class StubAPI:
    calls = []
    fail = False

    def __init__(self, token):
        assert token == "test-token"

    def get_me(self):
        return {"username": "employee_bot"}

    def subscribe(self, url, secret):
        self.calls.append((url, secret))
        if self.fail:
            raise setup.MaxAPIError("temporary")


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
