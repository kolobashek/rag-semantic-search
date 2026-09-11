"""Протухший heartbeat sync-клиента не должен выглядеть как «online».

Клиент шлёт «offline» только при корректном завершении. Если машину выключили
или процесс упал, в БД навсегда остаётся «online» — так пилотный клиент на
виртуалке числился работающим 49 дней после последнего контакта.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from rag_catalog.core.cloud_drive.registry import (
    SYNC_CLIENT_OFFLINE_AFTER_SECONDS,
    CloudDriveRegistryDB,
    sync_client_heartbeat_is_stale,
)


def _ago(seconds: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()


# ── чистая функция ─────────────────────────────────────────────────────


def test_fresh_heartbeat_is_not_stale() -> None:
    assert not sync_client_heartbeat_is_stale(_ago(30))


def test_missed_heartbeats_are_stale() -> None:
    assert sync_client_heartbeat_is_stale(_ago(SYNC_CLIENT_OFFLINE_AFTER_SECONDS + 60))


def test_unknown_or_broken_timestamps_count_as_stale() -> None:
    assert sync_client_heartbeat_is_stale("")
    assert sync_client_heartbeat_is_stale("никогда")


def test_naive_timestamp_is_treated_as_utc() -> None:
    naive = (datetime.now(timezone.utc) - timedelta(seconds=30)).replace(tzinfo=None).isoformat()
    assert not sync_client_heartbeat_is_stale(naive)


# ── реестр ─────────────────────────────────────────────────────────────


@pytest.fixture
def registry(tmp_path) -> CloudDriveRegistryDB:
    return CloudDriveRegistryDB(str(tmp_path / "cloud_drive.db"))


def _register(registry: CloudDriveRegistryDB, *, device_id: str = "win-ABC") -> str:
    client = registry.register_sync_client(
        username="kris", device_id=device_id, display_name="VM", platform="windows-cfapi"
    )
    return client.id


def _set_last_seen(registry: CloudDriveRegistryDB, client_id: str, when: str) -> None:
    with registry._connect() as conn:  # noqa: SLF001 - подделываем время в тесте
        conn.execute(
            "UPDATE cloud_sync_clients SET status='online', last_seen_at=? WHERE id=?",
            (when, client_id),
        )


def test_recent_heartbeat_stays_online(registry: CloudDriveRegistryDB) -> None:
    client_id = _register(registry)
    registry.update_sync_client_status(client_id, "online")

    client = registry.get_sync_client(client_id)
    assert client is not None
    assert client.status == "online"
    assert client.stale is False


def test_client_without_heartbeat_is_reported_offline(registry: CloudDriveRegistryDB) -> None:
    client_id = _register(registry)
    _set_last_seen(registry, client_id, _ago(SYNC_CLIENT_OFFLINE_AFTER_SECONDS + 600))

    client = registry.get_sync_client(client_id)
    assert client is not None
    assert client.status == "offline"
    assert client.stale is True


def test_stale_client_is_excluded_from_active_list(registry: CloudDriveRegistryDB) -> None:
    stale_id = _register(registry, device_id="win-STALE")
    fresh_id = _register(registry, device_id="win-FRESH")
    _set_last_seen(registry, stale_id, _ago(SYNC_CLIENT_OFFLINE_AFTER_SECONDS + 600))
    registry.update_sync_client_status(fresh_id, "online")

    active = registry.list_sync_clients(include_offline=False)
    assert [c.id for c in active] == [fresh_id]

    everything = registry.list_sync_clients(include_offline=True)
    assert {c.id for c in everything} == {stale_id, fresh_id}


def test_honest_offline_is_not_marked_stale(registry: CloudDriveRegistryDB) -> None:
    """Корректно завершившийся клиент — offline, но не «протух»."""
    client_id = _register(registry)
    registry.update_sync_client_status(client_id, "offline")

    client = registry.get_sync_client(client_id)
    assert client is not None
    assert client.status == "offline"
    assert client.stale is False
