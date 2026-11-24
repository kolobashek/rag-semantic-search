from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from urllib.parse import urlsplit
import re

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
        csrf = re.search('name="csrf" value="([^"]+)"', response.text)[1]
        data = {"token": "mock-token", "csrf": csrf}
        assert client.post(url, data=data, headers={"Origin": "https://another.test"}).status_code == 403
        origin = "http://" + urlsplit(url).netloc
        assert client.post(url, data={**data, "csrf": "wrong"}, headers={"Origin": origin}).status_code == 403
        assert not calls
        assert client.post(url, data=data, headers={"Origin": origin}).status_code == 200
        assert job.result(timeout=3)
        assert calls == [({}, "https://office.test", "https://max.ru/demo", "mock-token")]
