"""Тесты кооперативного контроля индексатора (pause/cancel через JSON)."""
from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from rag_catalog.core import indexer_control as ic
from rag_catalog.core.index_rag import IndexerCancelled, RAGIndexer


def _set_control_path(monkeypatch, tmp_path: Path) -> Path:
    """Перенаправить путь контрольного файла в tmp_path для изоляции."""
    p = tmp_path / "indexer_control.json"
    monkeypatch.setattr(ic, "_project_logs_dir", lambda: tmp_path)
    monkeypatch.setattr(ic, "indexer_control_path", lambda: p)
    return p


def test_default_command_is_running(tmp_path, monkeypatch):
    _set_control_path(monkeypatch, tmp_path)
    assert ic.get_current_command() == "running"


def test_write_and_read_roundtrip(tmp_path, monkeypatch):
    _set_control_path(monkeypatch, tmp_path)
    ic.write_indexer_control("pause")
    assert ic.get_current_command() == "pause"
    ic.write_indexer_control("cancel", run_id="abc")
    data = ic.read_indexer_control()
    assert data["command"] == "cancel"
    assert data["run_id"] == "abc"


def test_invalid_command_rejected(tmp_path, monkeypatch):
    _set_control_path(monkeypatch, tmp_path)
    with pytest.raises(ValueError):
        ic.write_indexer_control("bogus")


def test_check_control_cancel_raises(tmp_path, monkeypatch):
    _set_control_path(monkeypatch, tmp_path)
    ic.write_indexer_control("cancel")

    idx = RAGIndexer.__new__(RAGIndexer)
    idx.run_id = ""
    with pytest.raises(IndexerCancelled):
        idx._check_indexer_control(stage="metadata", stage_stats={})


def test_check_control_running_no_op(tmp_path, monkeypatch):
    _set_control_path(monkeypatch, tmp_path)
    ic.write_indexer_control("running")
    idx = RAGIndexer.__new__(RAGIndexer)
    idx.run_id = ""
    # Должно вернуться без исключения
    idx._check_indexer_control(stage="metadata", stage_stats={})


def test_check_control_pause_then_resume(tmp_path, monkeypatch):
    """Пауза — блокирует, пока не сменится команда; затем продолжает."""
    _set_control_path(monkeypatch, tmp_path)
    ic.write_indexer_control("pause")

    idx = RAGIndexer.__new__(RAGIndexer)
    idx.run_id = ""

    finished = threading.Event()

    def runner():
        idx._check_indexer_control(stage="metadata", stage_stats={"processed_files": 1})
        finished.set()

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    # Под паузой поток не должен завершиться сразу
    assert not finished.wait(timeout=1.5)
    # Снимаем паузу — поток должен выйти из цикла ожидания (poll каждую 1с)
    ic.write_indexer_control("running")
    assert finished.wait(timeout=3.0), "ожидаем выход из паузы за 3с"
    t.join(timeout=1.0)


def test_check_control_pause_then_cancel(tmp_path, monkeypatch):
    """Из паузы можно сразу выйти отменой."""
    _set_control_path(monkeypatch, tmp_path)
    ic.write_indexer_control("pause")

    idx = RAGIndexer.__new__(RAGIndexer)
    idx.run_id = ""

    raised: list = []

    def runner():
        try:
            idx._check_indexer_control(stage="metadata", stage_stats={})
        except IndexerCancelled:
            raised.append("ok")

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    time.sleep(1.5)
    ic.write_indexer_control("cancel")
    t.join(timeout=3.0)
    assert raised == ["ok"]
