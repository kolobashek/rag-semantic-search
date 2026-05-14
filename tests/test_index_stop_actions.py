from __future__ import annotations

from rag_catalog.ui import system


def test_stop_active_indexer_terminates_process_and_finalizes(monkeypatch) -> None:
    finalized: list[tuple[str, str, bool]] = []
    terminated: list[int] = []
    cleared: list[tuple[str, int]] = []

    class FakeTelemetryDB:
        def __init__(self, path: str) -> None:
            self.path = path

        def finalize_running_index_runs(self, *, status: str, note: str, skip_alive_pids: bool = True) -> int:
            finalized.append((status, note, skip_alive_pids))
            return 1

    monkeypatch.setattr(system, "TelemetryDB", FakeTelemetryDB)
    monkeypatch.setattr(system, "_find_live_running_index_run", lambda telemetry: {"worker_pid": 4321})
    monkeypatch.setattr(system, "_terminate_process", lambda pid: terminated.append(pid) or True)
    monkeypatch.setattr(system, "_clear_runtime_marker", lambda kind, *, pid=0: cleared.append((kind, pid)))

    assert system.stop_active_indexer({"telemetry_db_path": "unused.db"}, reason="manual_stop") is True
    assert terminated == [4321]
    assert cleared == [("index", 4321)]
    assert finalized == [("cancelled", "manual_stop", False)]


def test_stop_active_ocr_terminates_process_and_finalizes(monkeypatch) -> None:
    finalized: list[tuple[str, str, bool]] = []
    terminated: list[int] = []
    cleared: list[tuple[str, int]] = []

    class FakeTelemetryDB:
        def __init__(self, path: str) -> None:
            self.path = path

        def finalize_running_ocr_runs(self, *, status: str, note: str, skip_alive_pids: bool = True) -> int:
            finalized.append((status, note, skip_alive_pids))
            return 1

    monkeypatch.setattr(system, "TelemetryDB", FakeTelemetryDB)
    monkeypatch.setattr(system, "_find_live_running_ocr_run", lambda telemetry: {"worker_pid": 9876})
    monkeypatch.setattr(system, "_terminate_process", lambda pid: terminated.append(pid) or True)
    monkeypatch.setattr(system, "_clear_runtime_marker", lambda kind, *, pid=0: cleared.append((kind, pid)))

    assert system.stop_active_ocr({"telemetry_db_path": "unused.db"}, reason="manual_stop") is True
    assert terminated == [9876]
    assert cleared == [("ocr", 9876)]
    assert finalized == [("cancelled", "manual_stop", False)]


def test_stop_active_indexer_returns_false_without_active_run(monkeypatch) -> None:
    class FakeTelemetryDB:
        def __init__(self, path: str) -> None:
            self.path = path

    monkeypatch.setattr(system, "TelemetryDB", FakeTelemetryDB)
    monkeypatch.setattr(system, "_find_live_running_index_run", lambda telemetry: None)
    monkeypatch.setattr(
        system,
        "_terminate_process",
        lambda pid: (_ for _ in ()).throw(AssertionError("unexpected terminate")),
    )

    assert system.stop_active_indexer({"telemetry_db_path": "unused.db"}) is False
