from __future__ import annotations

from rag_catalog.ui import system


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
