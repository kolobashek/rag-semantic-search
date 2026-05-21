from __future__ import annotations

import logging
import os
from pathlib import Path

from rag_catalog.core import log_history
from rag_catalog.ui import helpers as ui_helpers


def test_open_run_log_creates_dated_segment(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(log_history, "PROJECT_ROOT", tmp_path)

    fh = log_history.open_run_log("web.log", "WEB")
    try:
        fh.write("hello\n")
    finally:
        fh.close()

    segments = log_history.list_log_segments("web.log", include_legacy=False)
    assert len(segments) == 1
    assert segments[0].parent.parent == tmp_path / "logs" / "history" / "web"
    assert "WEB" in segments[0].read_text(encoding="utf-8")
    assert "hello" in log_history.read_history_tail("web.log")


def test_size_handler_rotates_by_limit(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(log_history, "PROJECT_ROOT", tmp_path)
    logger = logging.getLogger("test-log-history-rotate")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = log_history.build_log_handler("indexer.log", max_bytes=1024 * 1024, label="INDEX")
    logger.addHandler(handler)
    try:
        logger.info("a" * 800_000)
        logger.info("b" * 800_000)
    finally:
        handler.close()
        logger.handlers.clear()

    segments = log_history.list_log_segments("indexer.log", include_legacy=False)
    assert len(segments) >= 2
    assert "a" * 20 in log_history.read_history_tail("indexer.log", max_chars=2_000_000)
    assert "b" * 20 in log_history.read_history_tail("indexer.log", max_chars=2_000_000)


def test_history_includes_legacy_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(log_history, "PROJECT_ROOT", tmp_path)
    legacy = tmp_path / "logs" / "runtime" / "telegram_bot.log"
    legacy.parent.mkdir(parents=True)
    legacy.write_text("legacy error\n", encoding="utf-8")
    with log_history.open_run_log("telegram_bot.log", "BOT") as fh:
        fh.write("new segment\n")

    text = log_history.read_history_tail_lines("telegram_bot.log", max_lines=10)

    assert "legacy error" in text
    assert "new segment" in text


def test_history_tail_lines_returns_newest_lines_first(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(log_history, "PROJECT_ROOT", tmp_path)
    old = tmp_path / "logs" / "history" / "web" / "2026-05-20" / "100000-p1-run-web.log"
    new = tmp_path / "logs" / "history" / "web" / "2026-05-21" / "100000-p2-run-web.log"
    old.parent.mkdir(parents=True)
    new.parent.mkdir(parents=True)
    old.write_text("old1\nold2\n", encoding="utf-8")
    new.write_text("new1\nnew2\n", encoding="utf-8")
    old_mtime = 1_000_000
    new_mtime = 2_000_000
    old.touch()
    new.touch()
    os.utime(old, (old_mtime, old_mtime))
    os.utime(new, (new_mtime, new_mtime))

    text = log_history.read_history_tail_lines("web.log", max_lines=3)

    assert text.splitlines() == ["new2", "new1", "old2"]


def test_ui_log_entries_returns_newest_entries_first(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(log_history, "PROJECT_ROOT", tmp_path)
    old = tmp_path / "logs" / "history" / "indexer" / "2026-05-20" / "100000-p1-run-indexer.log"
    new = tmp_path / "logs" / "history" / "indexer" / "2026-05-21" / "100000-p2-run-indexer.log"
    old.parent.mkdir(parents=True)
    new.parent.mkdir(parents=True)
    old.write_text("2026-05-20 10:00:00,000 - INFO - old entry\n", encoding="utf-8")
    new.write_text("2026-05-21 11:00:00,000 - INFO - new entry\n", encoding="utf-8")
    os.utime(old, (1_000_000, 1_000_000))
    os.utime(new, (2_000_000, 2_000_000))

    entries = ui_helpers._read_log_entries(Path("indexer.log"), max_entries=2)

    assert [entry["message"] for entry in entries] == ["new entry", "old entry"]


def test_last_error_can_ignore_info_fallback(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(log_history, "PROJECT_ROOT", tmp_path)
    with log_history.open_run_log("telegram_bot.log", "BOT") as fh:
        fh.write("2026-05-14 10:00:00 - INFO - Telegram bot started\n")

    assert log_history.last_error_from_history("telegram_bot.log", include_fallback=False) == ""
    assert "Telegram bot started" in log_history.last_error_from_history("telegram_bot.log")
