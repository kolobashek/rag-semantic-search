from __future__ import annotations

from datetime import datetime, timezone

from rag_catalog.ui import system


def test_schedules_due_catches_up_after_daily_window() -> None:
    schedules = [
        {
            "id": "daily",
            "enabled": 1,
            "cadence": "daily",
            "time": "03:00",
            "days_json": "[]",
            "last_run_at": "",
        }
    ]

    due = system._schedules_due(schedules, now=datetime(2026, 5, 14, 3, 7, tzinfo=timezone.utc))

    assert [item["id"] for item in due] == ["daily"]


def test_schedules_due_does_not_repeat_after_scheduled_slot() -> None:
    schedules = [
        {
            "id": "daily",
            "enabled": 1,
            "cadence": "daily",
            "time": "03:00",
            "days_json": "[]",
            "last_run_at": "2026-05-14T03:01:00+00:00",
        }
    ]

    due = system._schedules_due(schedules, now=datetime(2026, 5, 14, 5, 0, tzinfo=timezone.utc))

    assert due == []


def test_hourly_schedule_runs_once_per_hour_after_minute_zero() -> None:
    schedules = [
        {
            "id": "hourly",
            "enabled": 1,
            "cadence": "hourly",
            "time": "",
            "days_json": "[]",
            "last_run_at": "2026-05-14T02:59:00+00:00",
        }
    ]

    due = system._schedules_due(schedules, now=datetime(2026, 5, 14, 3, 42, tzinfo=timezone.utc))

    assert [item["id"] for item in due] == ["hourly"]


def test_scheduler_prioritizes_full_index_over_hourly_metadata(monkeypatch) -> None:
    schedules = [
        {
            "id": "hourly-metadata",
            "stage": "metadata",
            "created_at": "2026-04-27T14:10:54+00:00",
        },
        {
            "id": "daily-all",
            "stage": "all",
            "created_at": "2026-04-28T05:28:39+00:00",
        },
    ]
    launched: list[str] = []
    touched: list[str] = []

    class FakeTelemetryDB:
        def __init__(self, path: str) -> None:
            self.path = path

        def list_index_schedules(self):
            return list(schedules)

        def get_index_settings(self):
            return {}

        def touch_index_schedule(self, *, id: str) -> None:
            touched.append(id)

    monkeypatch.setattr(system, "TelemetryDB", FakeTelemetryDB)
    monkeypatch.setattr(system, "_schedules_due", lambda value: list(value))
    monkeypatch.setattr(system, "_launch_indexer", lambda cfg, *, stage, **kwargs: launched.append(stage) or 1234)
    monkeypatch.setattr(system, "_launch_ocr", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected OCR launch")))

    system._run_scheduler_tick({"telemetry_db_path": "unused.db", "index_read_workers": 4})

    assert launched == ["all"]
    assert touched == ["daily-all", "hourly-metadata"]


def test_full_index_schedule_covers_partial_index_stages() -> None:
    assert system._schedule_stage_covers("all", "metadata")
    assert system._schedule_stage_covers("all", "small")
    assert system._schedule_stage_covers("metadata", "metadata")
    assert not system._schedule_stage_covers("metadata", "all")
    assert not system._schedule_stage_covers("all", "ocr")
