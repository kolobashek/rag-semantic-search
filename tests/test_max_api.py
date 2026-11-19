import json
import ssl
import traceback
from pathlib import Path

import pytest
import requests

from rag_catalog.integrations import max_api as api


def response(data, status=200, **headers):
    resp = requests.Response()
    resp.status_code = status
    resp._content = data if isinstance(data, bytes) else json.dumps(data).encode()
    resp._content_consumed = True
    resp.headers.update(headers)
    return resp


def scripted(monkeypatch, *responses):
    calls = []
    replies = iter(responses)

    def request(method, url, **kwargs):
        if "data" in kwargs:
            kwargs["body"] = b"".join(kwargs["data"])
            assert len(kwargs["body"]) == len(kwargs["data"])
        calls.append((method, url, kwargs))
        return next(replies)

    monkeypatch.setattr(requests.Session, "request", staticmethod(request))
    monkeypatch.setattr(api.time, "sleep", lambda seconds: None)
    return calls


def private_update():
    return {"update_type": "message_created", "message": {
        "sender": {"user_id": 17, "first_name": "Вера", "last_name": "Иванова", "is_bot": False},
        "recipient": {"chat_type": "dialog", "user_id": 101},
        "body": {"mid": "mid.1", "text": "путевой лист", "attachments": [
            {"type": "file", "filename": "../лист.pdf", "size": 123,
             "payload": {"url": "https://fd.oneme.ru/getfile?sig=test"}},
            {"type": "image", "payload": {"url": "https://i.oneme.ru/image"}},
            {"type": "share", "payload": {"url": "https://example.org"}},
        ]},
    }}


def test_default_uses_verified_system_trust_and_respects_custom_bundle(tmp_path):
    prepared = requests.Request("GET", api.API_BASE + "/me").prepare()
    adapter = api._MaxTLSAdapter()
    _, options = adapter.build_connection_pool_key_attributes(prepared, True)
    assert options["ssl_context"].verify_mode == ssl.CERT_REQUIRED
    assert options["ssl_context"].check_hostname
    bundle = str(tmp_path / "operator-ca.pem")
    _, options = adapter.build_connection_pool_key_attributes(prepared, bundle)
    assert options.get("ssl_context") is None and options["ca_certs"] == bundle


def test_normalization_uses_actual_sender_and_native_file_attachments():
    update = private_update()
    event = api.normalize_update(update)
    assert event.user_id == 17 and event.display_name == "Вера Иванова"
    assert event.text == "путевой лист" and event.event_id == "message_created:17:mid.1"
    assert [f.name for f in event.files] == ["лист.pdf", "photo.jpg"]
    assert [f.size for f in event.files] == [123, None]
    assert "sig=test" not in repr(event)
    for chat_type in ("chat", "channel", "unknown", None):
        update["message"]["recipient"]["chat_type"] = chat_type
        assert api.normalize_update(update) is None
    update["message"]["recipient"]["chat_type"] = "dialog"
    update["message"]["sender"]["is_bot"] = True
    assert api.normalize_update(update) is None


def test_callbacks_and_deeplinks_use_authenticated_actor():
    event = api.normalize_update({"update_type": "bot_started", "user": {"user_id": 4},
                                  "timestamp": 121, "chat_id": 3, "payload": "link_abcd"})
    assert (event.user_id, event.text) == (4, "/start link_abcd")
    update = private_update()
    update.update(update_type="message_callback", callback={
        "user": {"user_id": 4}, "payload": "/shifts", "callback_id": "cb",
    })
    event = api.normalize_update(update)
    assert (event.user_id, event.text, event.callback_id, event.files) == (4, "/shifts", "cb", ())
    update.pop("message")
    assert api.normalize_update(update) is None


def test_messages_and_callbacks_use_private_address_and_correct_payload(monkeypatch):
    sent = {"message": {"body": {"mid": "mid.11"}}}
    calls = scripted(monkeypatch, response(sent), response(sent), response({"success": True}))
    client = api.MaxClient("api-key")
    buttons = [[{"type": "callback", "text": "Файлы", "payload": "/files"}]]
    assert client.send_message(17, "a" * 4010, buttons=buttons) == "mid.11"
    client.answer_callback("cb", "Готово")
    assert {call[2]["headers"]["Authorization"] for call in calls} == {"api-key"}
    assert all(call[1].startswith("https://platform-api2.max.ru/") for call in calls)
    assert calls[0][2]["params"] == {"user_id": 17, "disable_link_preview": "true"}
    assert calls[0][2]["json"] == {"text": "a" * 4000, "attachments": []}
    assert calls[1][2]["json"]["attachments"] == [{"type": "inline_keyboard", "payload": {"buttons": buttons}}]
    assert calls[2][2]["params"] == {"callback_id": "cb"}


def test_subscription_polling_and_get_me(monkeypatch):
    updates = [private_update()]
    calls = scripted(monkeypatch, response({"user_id": 4, "is_bot": True}), response({"success": True}),
                     response({"updates": updates, "marker": 453}),
                     response({"subscriptions": [{"url": "https://example.org/hook", "secret": "test_secret"}]}))
    client = api.MaxClient("api-key")
    assert client.get_me()["user_id"] == 4
    client.subscribe("https://example.org/hook", "test_secret")
    assert client.get_updates(0, 5) == (updates, 453)
    assert calls[2][2]["params"] == {"marker": 0, "timeout": 5}
    assert calls[1][2]["json"]["update_types"] == list(api.UPDATE_TYPES)
    assert client.subscriptions() == [{"url": "https://example.org/hook"}]
    with pytest.raises(ValueError):
        client.subscribe("http://example.org/hook", "test_secret")
    with pytest.raises(ValueError):
        client.subscribe("https://example.org/hook", "bad secret")
    assert len(calls) == 4


def test_send_file_upload_once_without_api_credentials_and_retry_processing(monkeypatch, tmp_path):
    doc = tmp_path / "лист.pdf"
    doc.write_bytes(b"a" * 830000)
    calls = scripted(monkeypatch, response({"url": "https://fu.oneme.ru/upload?sig=test"}),
                     response({"fileId": 47, "token": "file-key"}),
                     response({"code": "attachment.not.ready"}, 400),
                     response({"message": {"body": {"mid": "mid.21"}}}))
    assert api.MaxClient("api-key").send_document(17, doc, "Путевой лист") == "mid.21"
    assert calls[0][2]["params"] == {"type": "file"}
    upload = calls[1][2]
    assert "Authorization" not in upload["headers"] and not upload["allow_redirects"]
    assert 'name="data"; filename="лист.pdf"'.encode() in upload["body"] and doc.read_bytes() in upload["body"]
    sent = calls[2][2]["json"]
    assert sent == calls[3][2]["json"]
    assert sent["attachments"] == [{"type": "file", "payload": {"token": "file-key"}}]


def test_download_redirect_and_atomic_size_limit(monkeypatch, tmp_path):
    original = tmp_path / "original.pdf"
    original.write_bytes(b"original")
    remote = api.MaxFile("file.pdf", "https://fd.oneme.ru/getfile?sig=secret")
    calls = scripted(monkeypatch, response(b"", 302, Location="https://cdn.okcdn.ru/document"),
                     response(b"bad" * 200000))
    client = api.MaxClient("api-key")
    with pytest.raises(api.MaxAPIError, match="размера"):
        client.download_file(remote, original, max_bytes=300000)
    assert original.read_bytes() == b"original" and list(tmp_path.iterdir()) == [original]
    assert all(not c[2].get("headers") and not c[2]["allow_redirects"] for c in calls)
    scripted(monkeypatch, response(b"document", **{"Content-Length": "8"}))
    assert client.download_file(remote, original, max_bytes=8) == 8
    assert original.read_bytes() == b"document"


def test_reject_foreign_attachments_and_cdn_redirects(monkeypatch, tmp_path):
    client = api.MaxClient("api-key")
    allowed = "https://fd.oneme.ru/download?sig=secret"
    calls = scripted(monkeypatch, response(b"", 302, Location="https://example.org/private"))
    for url in ["http://fd.oneme.ru/file", "https://localhost/file", "https://oneme.ru.example.org/file",
                "https://user@cdn.okcdn.ru/secret", "https://fd.oneme.ru:8080/file", allowed]:
        with pytest.raises(api.MaxAPIError, match="адрес"):
            client.download_file(api.MaxFile("example", url), tmp_path / "waybill", max_bytes=1024)
    assert len(calls) == 1 and not list(tmp_path.iterdir())


def test_network_errors_omit_signed_urls_from_tracebacks(monkeypatch, tmp_path):
    def fail(method, url, **kwargs):
        raise requests.RequestException(url)

    remote = api.MaxFile("a", "https://fd.oneme.ru/example?sig=secret-ticket")
    monkeypatch.setattr(requests.Session, "request", staticmethod(fail))
    with pytest.raises(api.MaxAPIError) as exc:
        api.MaxClient("api-key").download_file(remote, Path(tmp_path / "new"), max_bytes=99)
    printed = "".join(traceback.format_exception(exc.type, exc.value, exc.tb))
    assert "sig=secret-ticket" not in printed
