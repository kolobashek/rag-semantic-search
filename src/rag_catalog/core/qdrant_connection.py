from __future__ import annotations

import ipaddress
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from qdrant_client import QdrantClient


def is_loopback_qdrant_url(url: str) -> bool:
    """Return whether a Qdrant HTTP URL targets the local machine."""
    host = str(urlparse(str(url or "").strip()).hostname or "").strip().lower()
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def create_qdrant_client(
    *,
    url: str = "",
    path: str | Path | None = None,
    timeout: int = 60,
) -> QdrantClient:
    """Create a Qdrant client with stable loopback HTTP connection reuse."""
    normalized_url = str(url or "").strip()
    normalized_timeout = max(1, int(timeout or 60))
    if normalized_url:
        kwargs: dict[str, Any] = {}
        if is_loopback_qdrant_url(normalized_url):
            # httpx otherwise honors the Windows system proxy and qdrant-client
            # disables keep-alive for localhost, exhausting sockets on full scans.
            kwargs.update(
                {
                    "trust_env": False,
                    "check_compatibility": False,
                    "limits": httpx.Limits(
                        max_connections=32,
                        max_keepalive_connections=8,
                        keepalive_expiry=30.0,
                    ),
                }
            )
        return QdrantClient(url=normalized_url, timeout=normalized_timeout, **kwargs)
    if path is None:
        raise ValueError("Qdrant URL or local path is required")
    return QdrantClient(path=str(path), timeout=normalized_timeout)
