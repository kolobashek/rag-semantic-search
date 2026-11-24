from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from rag_catalog.core import index_rag, nightly_index
from rag_catalog.core.telemetry_db import TelemetryDB
from rag_catalog.ui import system


def test_daily_slot_roundtrip(tmp_path):
    db = TelemetryDB(str(tmp_path / "telemetry.db"))
    db.save_index_schedule(stage=nightly_index.STAGE, cadence="daily", time="03:00", days=[])
    rows = db.list_index_schedules()
    assert len(rows) == 1 and rows[0]["stage"] == nightly_index.STAGE
    start = datetime(2026, 11, 29, 3).astimezone()  # Sunday in the server's local time.
    for minutes, expected in [(-1, False), (0, True), (9, True), (10, False), (600, False)]:
        assert bool(system._schedules_due(rows, now=start + timedelta(minutes=minutes))) == expected
    rows[0]["last_run_at"] = start.isoformat()
    assert not system._schedules_due(rows, now=start + timedelta(minutes=1))


@pytest.mark.parametrize("mib,source_ready", [(4096, True), (4095, True), (8192, False)])
def test_scheduler_respects_budget(monkeypatch, tmp_path, mib, source_ready):
    db = TelemetryDB(str(tmp_path / "telemetry.db"))
    db.save_index_schedule(stage=nightly_index.STAGE)
    db.save_index_settings({"workers": 8, "ocr_enabled": True, "recreate": True, "max_chunks": 0})
    cfg = {"telemetry_db_path": str(db.db_path), "catalog_path": str(tmp_path if source_ready else tmp_path / "missing")}
    monkeypatch.setattr("rag_catalog.core.rag_core.load_config", lambda: cfg)
    monkeypatch.setattr(system, "_schedules_due", lambda rows: rows)
    monkeypatch.setattr(nightly_index.psutil, "virtual_memory", lambda: SimpleNamespace(available=mib << 20))
    launch = Mock(return_value=123)
    monkeypatch.setattr(system, "_launch_indexer", launch)
    system._run_scheduler_tick(cfg)
    if mib >= 4096 and source_ready:
        launch.assert_called_once_with(cfg, stage=nightly_index.STAGE, workers=1, max_chunks=5,
                                       skip_inline_ocr=True, force_ocr=False, recreate=False)
        assert db.list_index_schedules()[0]["last_run_at"]
    else:
        launch.assert_not_called()
        assert not db.list_index_schedules()[0]["last_run_at"]
        assert db.fetch_dicts("SELECT action FROM app_events ORDER BY id DESC LIMIT 1")[0]["action"] == "launch_blocked"


def test_nightly_cli_never_requests_large_or_ocr(monkeypatch, tmp_path):
    config = dict(index_rag.load_config(), log_file="", catalog_path=str(tmp_path), qdrant_db_path=str(tmp_path))
    monkeypatch.setattr(index_rag, "load_config", lambda: config)
    monkeypatch.setattr(nightly_index, "check_resources", lambda _: None)
    engine = Mock()
    engine.index_all_stages.return_value = {}
    engine._run_deleted_files = 0
    create = Mock(return_value=engine)
    monkeypatch.setattr(index_rag, "RAGIndexer", create)
    monkeypatch.setattr("sys.argv", ["index_rag", "--stage", nightly_index.STAGE, "--skip-config-check"])
    index_rag.main()
    args = create.call_args.kwargs
    assert (args["read_workers"], args["skip_ocr"], args["recreate_collection"], args["max_chunks_per_file"]) == (1, True, False, 5)
    assert engine.nightly
    engine.index_all_stages.assert_called_once_with(stages=["metadata", "small"])
    engine.index_directory.assert_not_called()


@pytest.mark.parametrize("active", [False, True])
def test_watchdog_never_relaunches_nightly_run(monkeypatch, active):
    row = {"run_id": "night", "note": "stage=nightly_quick | interrupted", "ts_started": datetime.now().astimezone().isoformat()}
    db = Mock()
    db.get_index_settings.return_value = {}
    db.get_active_index_run.return_value = row if active else None
    db.get_active_ocr_run.return_value = None
    db.fetch_dicts.side_effect = lambda sql, *args: [row] if "FROM index_runs" in sql else []
    monkeypatch.setattr(system, "TelemetryDB", lambda _: db)
    monkeypatch.setattr(system, "_find_live_running_index_run", lambda _: None)
    monkeypatch.setattr(system, "_find_live_running_ocr_run", lambda _: None)
    launch = Mock()
    monkeypatch.setattr(system, "_launch_indexer", launch)
    system._recover_background_tasks({}, allow_failed_restart=True)
    launch.assert_not_called()


def test_nightly_does_not_start_quick_phase_under_memory_pressure(monkeypatch, tmp_path):
    check = Mock(side_effect=[None, RuntimeError("low RAM")])
    monkeypatch.setattr(nightly_index, "check_resources", check)
    engine = SimpleNamespace(nightly=True, catalog_path=tmp_path, index_directory=Mock(return_value={}))
    with pytest.raises(RuntimeError, match="low RAM"):
        index_rag.RAGIndexer.index_all_stages(engine, list(nightly_index.PHASES))
    engine.index_directory.assert_called_once_with(stage="metadata")
