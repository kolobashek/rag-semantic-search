from __future__ import annotations

import pytest

from rag_catalog.core.telemetry_db import TelemetryDB
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


def test_find_live_indexer_ignores_reused_stale_pid(monkeypatch, tmp_path) -> None:
    db = TelemetryDB(str(tmp_path / "telemetry.db"))
    db.start_index_run(
        catalog_path="O:\\Обмен",
        collection_name="catalog",
        recreate=False,
        worker_pid=31124,
    )

    monkeypatch.setattr(
        system,
        "_process_matches_module",
        lambda pid, module: int(pid) == 7936 and module == "rag_catalog.core.index_rag",
    )
    monkeypatch.setattr(system, "_find_module_process_pids", lambda module: [7936])

    active = system._find_live_running_index_run(db)

    assert active is not None
    assert active["worker_pid"] == 7936
    assert active["_process_scan_only"] is True


def test_launch_indexer_is_blocked_while_ocr_is_running(monkeypatch) -> None:
    class FakeTelemetryDB:
        def __init__(self, path: str) -> None:
            self.path = path

    monkeypatch.setattr(system, "TelemetryDB", FakeTelemetryDB)
    monkeypatch.setattr(system, "_find_live_running_index_run", lambda telemetry: None)
    monkeypatch.setattr(system, "_find_live_running_ocr_run", lambda telemetry: {"worker_pid": 7264})
    monkeypatch.setattr(
        system.subprocess,
        "Popen",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected indexer launch")),
    )

    with pytest.raises(RuntimeError, match="OCR уже запущен"):
        system._launch_indexer({"catalog_path": "O:\\Обмен", "collection_name": "catalog"})


def test_launch_indexer_for_content_stages_disables_inline_ocr(monkeypatch, tmp_path) -> None:
    captured: list[list[str]] = []

    class FakeTelemetryDB:
        def __init__(self, path: str) -> None:
            self.path = path

    class FakeProc:
        pid = 4321

    class FakeLog:
        def close(self) -> None:
            pass

    monkeypatch.setattr(system, "TelemetryDB", FakeTelemetryDB)
    monkeypatch.setattr(system, "_find_live_running_index_run", lambda telemetry: None)
    monkeypatch.setattr(system, "_find_live_running_ocr_run", lambda telemetry: None)
    monkeypatch.setattr(system, "_open_log", lambda *_args, **_kwargs: FakeLog())
    monkeypatch.setattr(system, "_write_runtime_marker", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(system, "_windows_detached_creationflags", lambda: 0)

    def fake_popen(args, **_kwargs):
        captured.append(list(args))
        return FakeProc()

    monkeypatch.setattr(system.subprocess, "Popen", fake_popen)

    system._launch_indexer(
        {
            "telemetry_db_path": str(tmp_path / "telemetry.db"),
            "catalog_path": "O:\\Обмен",
            "collection_name": "catalog",
            "qdrant_url": "http://localhost:6333",
        },
        stage="large",
    )

    assert "--no-ocr" in captured[0]
    assert captured[0][captured[0].index("--max-chunks") + 1] == "0"


def test_terminate_process_stops_children_before_parent(monkeypatch) -> None:
    calls: list[tuple[str, int]] = []

    class FakeProcess:
        def __init__(self, pid: int, children: list["FakeProcess"] | None = None) -> None:
            self.pid = pid
            self._children = children or []

        def children(self, recursive: bool = False) -> list["FakeProcess"]:
            assert recursive is True
            return list(self._children)

        def terminate(self) -> None:
            calls.append(("terminate", self.pid))

        def kill(self) -> None:
            calls.append(("kill", self.pid))

    child_a = FakeProcess(101)
    child_b = FakeProcess(102)
    parent = FakeProcess(100, [child_a, child_b])

    monkeypatch.setattr(system.psutil, "Process", lambda pid: parent)
    monkeypatch.setattr(system.psutil, "wait_procs", lambda processes, timeout: (list(processes), []))

    assert system._terminate_process(100) is True
    assert calls == [("terminate", 101), ("terminate", 102), ("terminate", 100)]
