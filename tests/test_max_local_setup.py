import re
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from urllib.parse import urlsplit

import pytest
import requests

from rag_catalog.integrations import max_local_setup


def test_only_same_origin_can_configure_once(monkeypatch):
    calls = []
    monkeypatch.setattr(max_local_setup, "configure", lambda *args: calls.append(args))
    addresses = Queue()
    with ThreadPoolExecutor() as pool, requests.Session() as client:
        client.trust_env = False
        job = pool.submit(max_local_setup.serve, {}, "https://office.test", "https://max.ru/demo",
                          ready=addresses.put, lifetime=6)
        url = addresses.get(timeout=3)
        assert client.get(url, headers={"Host": "another.test"}).status_code == 404
        response = client.get(url)
        assert response.headers["Referrer-Policy"] == "same-origin"
        csrf = re.search('name="csrf" value="([^"]+)"', response.text)[1]
        data = {"token": "mock-token", "csrf": csrf}
        assert client.post(url, data=data, headers={"Origin": "https://another.test"}).status_code == 403
        origin = "http://" + urlsplit(url).netloc
        assert client.post(url, data={**data, "csrf": "wrong"}, headers={"Origin": origin}).status_code == 403
        assert not calls
        assert client.post(url, data=data, headers={"Origin": origin}).status_code == 200
        assert job.result(timeout=3)
        assert calls == [({}, "https://office.test", "https://max.ru/demo", "mock-token")]


def test_local_form_reports_connected_webhook_and_command_retry(monkeypatch):
    def partial(*args):
        raise max_local_setup.MenuSetupPending("Вебхук подключён. Повторите max_settings commands.")

    monkeypatch.setattr(max_local_setup, "configure", partial)
    addresses = Queue()
    with ThreadPoolExecutor() as pool, requests.Session() as client:
        client.trust_env = False
        job = pool.submit(max_local_setup.serve, {}, "https://office.test", "https://max.ru/demo",
                          ready=addresses.put, lifetime=6)
        url = addresses.get(timeout=3)
        csrf = re.search('name="csrf" value="([^"]+)"', client.get(url).text)[1]
        response = client.post(url, data={"token": "test-token", "csrf": csrf},
                               headers={"Origin": "http://" + urlsplit(url).netloc})
        assert response.status_code == 200 and "max_settings commands" in response.text
        assert "test-token" not in response.text
        with pytest.raises(max_local_setup.MenuSetupPending):
            job.result(timeout=3)
