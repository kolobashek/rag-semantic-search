"""Bounded S3 downloads through the authenticated application origin."""

from __future__ import annotations

import re
from urllib.parse import quote

from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask


def storage_download_response(storage, descriptor: dict, byte_range: str = '', *,
                              inline: bool = False, media_type: str = '', headers: dict | None = None):
    size = int(descriptor['size_bytes'])
    response_headers = {
        'Accept-Ranges': 'bytes',
        'Cache-Control': 'private, no-store',
        'X-Content-Type-Options': 'nosniff',
        **(headers or {}),
    }
    start, end = 0, size - 1
    if byte_range:
        match = re.fullmatch(r'bytes=(\d*)-(\d*)', byte_range.strip()) if len(byte_range) < 128 else None
        if not match or not any(match.groups()) or size == 0:
            raise HTTPException(416, 'Invalid byte range', headers={'Content-Range': f'bytes */{size}'})
        first, last = match.groups()
        if first:
            start = int(first)
            end = min(int(last), size - 1) if last else size - 1
        else:
            start = max(0, size - int(last))
        if start > end or start >= size:
            raise HTTPException(416, 'Invalid byte range', headers={'Content-Range': f'bytes */{size}'})
    requested_range = f'bytes={start}-{end}' if byte_range else ''
    try:
        obj = storage.open_download(descriptor['storage_key'], byte_range=requested_range)
    except Exception as exc:
        code = str((getattr(exc, 'response', {}) or {}).get('Error', {}).get('Code', ''))
        if code in {'NoSuchKey', '404', 'NotFound'}:
            raise HTTPException(404, 'File is missing from storage') from exc
        raise HTTPException(502, 'Storage download failed') from exc
    body = obj['Body']
    expected_length = end - start + 1
    expected_range = f'bytes {start}-{end}/{size}'
    if (int(obj.get('ContentLength', -1)) != expected_length
            or (byte_range and obj.get('ContentRange') != expected_range)):
        body.close()
        raise HTTPException(502, 'Storage size or range does not match file metadata')
    response_headers['Content-Length'] = str(expected_length)
    disposition = 'inline' if inline else 'attachment'
    response_headers['Content-Disposition'] = f"{disposition}; filename*=UTF-8''{quote(descriptor['filename'], safe='')}"
    if byte_range:
        response_headers['Content-Range'] = expected_range

    def chunks():
        try:
            remaining = expected_length
            while remaining:
                chunk = body.read(min(256 * 1024, remaining))
                if not chunk:
                    raise OSError('Storage download ended before Content-Length')
                remaining -= len(chunk)
                yield chunk
        finally:
            body.close()

    return StreamingResponse(
        chunks(), status_code=206 if byte_range else 200,
        media_type=media_type or descriptor['mime_type'], headers=response_headers,
        background=BackgroundTask(body.close),
    )
