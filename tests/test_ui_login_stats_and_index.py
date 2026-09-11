"""UI-хелперы: реальные данные экрана входа, слепые зоны, heartbeat, чанки, ключ синонима,
проброс recreate/--force-ocr в запуск индексатора и планировщик."""

from __future__ import annotations

import json
import time
from pathlib import Path

from rag_catalog.core.index_state_db import IndexStateDB
from rag_catalog.core.telemetry_db import TelemetryDB
from rag_catalog.ui import helpers, system


def _cfg(tmp_path: Path) -> dict:
    return {
        "qdrant_db_path": str(tmp_path / "qdrant"),
        "telemetry_db_path": str(tmp_path / "telemetry.db"),
        "indexer_heartbeat_path": str(tmp_path / "heartbeat.json"),
    }


# ── экран входа ────────────────────────────────────────────────────────────


def test_login_stats_empty_when_nothing_exists(tmp_path: Path) -> None:
    out = helpers._read_login_screen_stats(_cfg(tmp_path))

    assert out["documents"] is None
    assert out["searches_today"] is None
    assert out["avg_seconds"] is None
    assert out["recent_searches"] == []
    assert out["index_status"]["dot"] == "info"


def test_login_stats_reads_real_values(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    (tmp_path / "qdrant").mkdir()
    state_db = IndexStateDB(str(tmp_path / "qdrant" / "index_state.db"))
    state_db.upsert_many([
        {"full_path": r"D:\docs\a.pdf", "stage": "content", "status": "ok"},
        {"full_path": r"D:\docs\b.xlsx", "stage": "partial", "status": "ok"},
        {"full_path": r"D:\docs\c.pdf", "stage": "metadata", "status": "deferred_ocr"},
        {"full_path": r"D:\docs\d.docx", "stage": "metadata", "status": "error"},
    ])
    telemetry = TelemetryDB(cfg["telemetry_db_path"])
    for query, ms in (("паспорт котла", 400), ("договор 442", 800)):
        telemetry.log_search(
            source="web", query=query, limit_value=10, file_type=None, content_only=False,
            results_count=3, duration_ms=ms, ok=True, username="ivanov",
        )
    from rag_catalog.core.indexing.heartbeat import write_heartbeat

    write_heartbeat(cfg["indexer_heartbeat_path"], stage="large", processed=10, total=10, status="finished")

    out = helpers._read_login_screen_stats(cfg)

    assert out["documents"] == 2  # content + partial, без deferred_ocr/error
    assert out["searches_today"] == 2
    assert out["avg_seconds"] == 0.6
    assert [row["query"] for row in out["recent_searches"]] == ["договор 442", "паспорт котла"]
    assert all(set(row) == {"time", "query"} for row in out["recent_searches"])  # без имён пользователей
    assert out["index_status"]["dot"] == "ok"
    assert out["index_status"]["label"] == "индекс актуален"
    assert out["index_status"]["sub"] == time.strftime("%d.%m.%Y")


def test_login_stats_searches_today_excludes_yesterday(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    telemetry = TelemetryDB(cfg["telemetry_db_path"])
    telemetry.log_search(
        source="web", query="старый", limit_value=10, file_type=None, content_only=False,
        results_count=0, duration_ms=100, ok=True,
    )
    telemetry.fetch_dicts("UPDATE search_logs SET ts = '2000-01-01T00:00:00+00:00' WHERE query = 'старый'")

    out = helpers._read_login_screen_stats(cfg)

    assert out["searches_today"] == 0
    assert out["avg_seconds"] is None
    assert out["recent_searches"][0]["query"] == "старый"


# ── heartbeat / слепые зоны ────────────────────────────────────────────────


def test_heartbeat_status_kinds(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    assert helpers._read_heartbeat_status(cfg)["kind"] == "missing"

    hb_path = Path(cfg["indexer_heartbeat_path"])
    now = time.time()
    hb_path.write_text(json.dumps({"ts": now, "stage": "small", "processed": 5, "total": 20, "status": "running"}), encoding="utf-8")
    running = helpers._read_heartbeat_status(cfg, now=now)
    assert running["kind"] == "running"
    assert "прогон идёт" in running["label"]
    assert "5/20" in running["detail"]

    dead = helpers._read_heartbeat_status(cfg, now=now + 3 * 3600)
    assert dead["kind"] == "dead"
    assert "3 ч" in dead["label"]

    hb_path.write_text(json.dumps({"ts": now, "stage": "large", "processed": 20, "total": 20, "status": "failed"}), encoding="utf-8")
    assert helpers._read_heartbeat_status(cfg, now=now)["kind"] == "failed"


def test_coverage_summary_reports_blind_spot_reasons(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    assert helpers._read_coverage_summary(cfg)["found"] is False

    (tmp_path / "qdrant").mkdir()
    IndexStateDB(str(tmp_path / "qdrant" / "index_state.db")).upsert_many([
        {"full_path": r"D:\docs\a.pdf", "stage": "content", "status": "ok"},
        {"full_path": r"D:\docs\scan.pdf", "stage": "metadata", "status": "deferred_ocr"},
        {"full_path": r"D:\docs\empty.txt", "stage": "empty"},
        {"full_path": r"D:\docs\broken.docx", "stage": "error"},
    ])

    out = helpers._read_coverage_summary(cfg)

    assert out["found"] is True
    assert out["total"] == 4
    assert out["covered"] == 1
    assert out["uncovered"] == 3
    assert out["uncovered_by_status"] == {"deferred_ocr": 1, "empty": 1, "error": 1}


# ── чанки ──────────────────────────────────────────────────────────────────


def test_list_index_chunks_sorted_and_truncated(monkeypatch) -> None:
    class FakeSearcher:
        connected = True

        def _content_chunks_for_paths(self, paths, max_chunks=100):
            assert paths == [r"D:\docs\a.pdf"]
            assert max_chunks == 200
            return [
                {"chunk_index": 2, "type": "content", "page": 3, "sheet": "", "text": "x" * 500},
                {"chunk_index": 0, "type": "content", "page": 1, "sheet": "", "text": "  first\n\nchunk "},
                {"chunk_index": None, "type": "content", "page": None, "sheet": "Лист1", "text": "no index"},
            ]

    monkeypatch.setattr(helpers, "_cached_searcher_if_ready", lambda cfg: FakeSearcher())

    out = helpers._list_index_chunks({}, r"D:\docs\a.pdf", limit=200)

    assert out["ok"] is True
    assert [c["chunk_index"] for c in out["chunks"]] == [0, 2, None]
    assert len(out["chunks"][1]["text"]) == 200
    assert out["chunks"][0]["page"] == 1
    assert out["chunks"][2]["sheet"] == "Лист1"


def test_list_index_chunks_without_searcher(monkeypatch) -> None:
    monkeypatch.setattr(helpers, "_cached_searcher_if_ready", lambda cfg: None)
    out = helpers._list_index_chunks({}, r"D:\docs\a.pdf")
    assert out["ok"] is False and out["chunks"] == []
    assert helpers._list_index_chunks({}, "")["ok"] is False


# ── ключ синонима ──────────────────────────────────────────────────────────


def test_alias_key_transliterates_cyrillic() -> None:
    assert helpers._alias_key_from_text("Карточка предприятия") == "kartochka_predpriyatiya"
    assert helpers._alias_key_from_text("расчётный счёт") == "raschetnyy_schet"
    assert helpers._alias_key_from_text("company card") == "company_card"
    assert helpers._alias_key_from_text("") == "alias"
    assert helpers._alias_key_from_text("реквизиты") != helpers._alias_key_from_text("паспорт")


# ── recreate / --force-ocr ─────────────────────────────────────────────────


def test_recreate_applies_only_to_full_runs() -> None:
    assert system._recreate_applies("all", True) is True
    assert system._recreate_applies("full", True) is True
    assert system._recreate_applies("metadata", True) is False
    assert system._recreate_applies("large", True) is False
    assert system._recreate_applies("all", False) is False


def test_launch_indexer_passes_recreate_and_force_ocr(monkeypatch, tmp_path: Path) -> None:
    captured: dict = {}

    class FakeProc:
        pid = 4242

    def fake_popen(args, **kwargs):
        captured["args"] = list(args)
        return FakeProc()

    monkeypatch.setattr(system.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(system, "_find_live_running_index_run", lambda telemetry: None)
    monkeypatch.setattr(system, "_find_live_running_ocr_run", lambda telemetry: None)
    monkeypatch.setattr(system, "_write_runtime_marker", lambda *a, **k: None)
    monkeypatch.setattr(system, "_open_log", lambda *a, **k: open(tmp_path / "log.txt", "a", encoding="utf-8"))
    cfg = {"telemetry_db_path": str(tmp_path / "t.db"), "catalog_path": "D:/docs", "collection_name": "catalog", "qdrant_url": "http://x"}

    system._launch_indexer(cfg, stage="all", recreate=True, force_ocr=True)
    assert "--recreate" in captured["args"]
    assert "--force-ocr" in captured["args"]
    assert "--no-ocr" not in captured["args"]

    system._launch_indexer(cfg, stage="all", recreate=False, force_ocr=False)
    assert "--recreate" not in captured["args"]
    assert "--no-ocr" in captured["args"]


def test_scheduler_forwards_recreate_force_ocr_and_ocr_min_text_len(monkeypatch) -> None:
    schedules = [
        {"id": "daily-all", "stage": "all", "created_at": "2026-04-28T05:28:39+00:00"},
        {"id": "ocr", "stage": "ocr", "created_at": "2026-04-28T05:28:40+00:00"},
    ]
    index_calls: list[dict] = []
    ocr_calls: list[dict] = []

    class FakeTelemetryDB:
        def __init__(self, path: str) -> None:
            pass

        def list_index_schedules(self):
            return list(schedules)

        def get_index_settings(self):
            return {"recreate": True, "ocr_enabled": True, "ocr_min_text_len": 120, "workers": 2}

        def touch_index_schedule(self, *, id: str) -> None:
            pass

        def log_app_event(self, **kwargs) -> None:
            pass

    monkeypatch.setattr(system, "TelemetryDB", FakeTelemetryDB)
    monkeypatch.setattr(system, "_schedules_due", lambda value: list(value))
    monkeypatch.setattr(system, "_launch_indexer", lambda cfg, **kwargs: index_calls.append(kwargs) or 1)
    monkeypatch.setattr(system, "_launch_ocr", lambda cfg, **kwargs: ocr_calls.append(kwargs) or 2)

    # первый тик: индексация «all» запускается, OCR пропускается как busy
    system._run_scheduler_tick({"telemetry_db_path": "unused.db"})
    assert index_calls and index_calls[0]["recreate"] is True
    assert index_calls[0]["force_ocr"] is True
    assert ocr_calls == []

    # только OCR-расписание: min_text_len берётся из настроек, как и при ручном запуске
    schedules[:] = [schedules[1]]
    system._run_scheduler_tick({"telemetry_db_path": "unused.db"})
    assert ocr_calls and ocr_calls[0]["min_text_len"] == 120


def test_scheduler_does_not_recreate_on_partial_stage(monkeypatch) -> None:
    index_calls: list[dict] = []

    class FakeTelemetryDB:
        def __init__(self, path: str) -> None:
            pass

        def list_index_schedules(self):
            return [{"id": "hourly-metadata", "stage": "metadata", "created_at": "2026-04-27T14:10:54+00:00"}]

        def get_index_settings(self):
            return {"recreate": True}

        def touch_index_schedule(self, *, id: str) -> None:
            pass

        def log_app_event(self, **kwargs) -> None:
            pass

    monkeypatch.setattr(system, "TelemetryDB", FakeTelemetryDB)
    monkeypatch.setattr(system, "_schedules_due", lambda value: list(value))
    monkeypatch.setattr(system, "_launch_indexer", lambda cfg, **kwargs: index_calls.append(kwargs) or 1)

    system._run_scheduler_tick({"telemetry_db_path": "unused.db"})

    assert index_calls[0]["recreate"] is False
