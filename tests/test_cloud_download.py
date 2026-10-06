from io import BytesIO

import pytest
from fastapi import FastAPI, Header, HTTPException
from fastapi.testclient import TestClient

from rag_catalog.ui.cloud_download import storage_download_response


class MemoryStorage:
    def __init__(self, data=b'%PDF-1.6\n0123456789\n%%EOF'):
        self.data = data
        self.calls = []
        self.body = None

    def open_download(self, key, *, byte_range=''):
        self.calls.append((key, byte_range))
        payload = self.data
        result = {}
        if byte_range:
            first, last = map(int, byte_range.removeprefix('bytes=').split('-'))
            payload = payload[first:last + 1]
            result['ContentRange'] = f'bytes {first}-{last}/{len(self.data)}'
        self.body = BytesIO(payload)
        return {**result, 'Body': self.body, 'ContentLength': len(payload)}


def descriptor(storage):
    return dict(filename='scan.pdf', mime_type='application/pdf', size_bytes=len(storage.data), storage_key='object')


@pytest.mark.parametrize('range_value,start,end', [('', 0, None), ('bytes=0-3', 0, 4),
    ('bytes=4-7', 4, 8), ('bytes=8-', 8, None), ('bytes=-5', -5, None), ('bytes=0-9999', 0, None)])
def test_stream_download_range_and_close(range_value, start, end):
    storage = MemoryStorage()
    app = FastAPI()

    @app.get('/download')
    def download(range: str = Header(default='')):
        return storage_download_response(storage, descriptor(storage), range)

    with TestClient(app) as client:
        response = client.get('/download', headers={'Range': range_value} if range_value else {})
    assert response.status_code == (206 if range_value else 200)
    assert response.content == storage.data[start:end]
    assert response.headers['content-length'] == str(len(response.content))
    assert response.headers['accept-ranges'] == 'bytes'
    assert 'location' not in response.headers
    assert storage.body.closed


@pytest.mark.parametrize('range_value', ['bytes=999-', 'bytes=5-1', 'bytes=-0', 'bytes=-',
    'bytes=0-1,4-5', 'items=1-3', 'bytes=abc-def', 'bytes=' + '9' * 200 + '-'])
def test_invalid_range_never_opens_storage(range_value):
    storage = MemoryStorage()
    with pytest.raises(HTTPException) as caught:
        storage_download_response(storage, descriptor(storage), range_value)
    assert caught.value.status_code == 416
    assert caught.value.headers['Content-Range'] == f'bytes */{len(storage.data)}'
    assert not storage.calls


@pytest.mark.parametrize('range_value', ['', 'bytes=0-2'])
def test_metadata_mismatch_fails_closed(range_value):
    storage = MemoryStorage()
    desc = {**descriptor(storage), 'size_bytes': 1000}
    with pytest.raises(HTTPException) as caught:
        storage_download_response(storage, desc, range_value)
    assert caught.value.status_code == 502
    assert storage.body.closed


def test_backend_ignoring_range_fails_closed():
    storage = MemoryStorage()
    original = storage.open_download
    storage.open_download = lambda key, **kwargs: original(key)
    with pytest.raises(HTTPException):
        storage_download_response(storage, descriptor(storage), 'bytes=0-2')
    assert storage.body.closed


def test_empty_file():
    storage = MemoryStorage(b'')
    response = storage_download_response(storage, descriptor(storage))
    assert response.headers['content-length'] == '0'
    storage.body.close()
    with pytest.raises(HTTPException):
        storage_download_response(storage, descriptor(storage), 'bytes=0-')


def test_storage_failure_is_not_a_redirect():
    storage = MemoryStorage()
    def fail(*args, **kwargs):
        raise OSError('private storage endpoint')
    storage.open_download = fail
    with pytest.raises(HTTPException) as caught:
        storage_download_response(storage, descriptor(storage))
    assert caught.value.status_code == 502
    assert 'private' not in caught.value.detail


def test_truncated_body_raises_and_closes():
    storage = MemoryStorage()
    response = storage_download_response(storage, descriptor(storage))
    storage.body.truncate(2)
    import asyncio
    async def consume():
        return [chunk async for chunk in response.body_iterator]
    with pytest.raises(OSError):
        asyncio.run(consume())
    assert storage.body.closed
