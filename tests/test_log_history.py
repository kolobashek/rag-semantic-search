from __future__ import annotations

import logging
from pathlib import Path

from rag_catalog.core import log_history


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
