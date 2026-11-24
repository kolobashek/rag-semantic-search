import asyncio
import sqlite3
import threading

import pytest
from nicegui.testing import user_simulation

from rag_catalog.ui import index_view as view
from rag_catalog.ui.state import PageState


def test_dashboard_stats_only_need_existing_entries(tmp_path):
    path = tmp_path / "index_state.db"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE state_entries(extension, size_bytes, stage)")
        db.executemany("INSERT INTO state_entries VALUES(?,?,?)", [
            (".pdf", 4, "content"), (".pdf", 6, ""), (".txt", 3, "metadata"),
        ])
    stats = view._read_index_stats({"qdrant_db_path": str(tmp_path)})
    assert (stats["total"], stats["total_size_bytes"]) == (3, 13)
    assert stats["by_ext_size"] == {".pdf": 10, ".txt": 3}
    assert stats["by_stage"] == {"metadata": 2, "content": 1}
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT name FROM sqlite_master").fetchall() == [("state_entries",)]


@pytest.mark.parametrize("outcome", ["loaded", "navigation", "error"])
def test_slow_dashboard_does_not_block_navigation(monkeypatch, outcome):
    state = PageState({}, screen="index", current_user={"role": "admin"})
    started, release, finished = (threading.Event() for _ in range(3))

    def snapshot(_state):
        started.set()
        try:
            assert release.wait(5)
            if outcome == "error":
                raise ValueError("test error")
            return dict(stats={"total": 0}, telemetry={}, settings={}, schedules=[], daily=[], gpu=False, db=None)
        finally:
            finished.set()

    monkeypatch.setattr(view, "_load_dashboard", snapshot)

    def root():
        view.render_index_screen(state, render_fn=root, access_denied=lambda **_: pytest.fail("unexpected denial"))

    async def exercise():
        async with user_simulation(root) as user:
            await user.open("/")
            try:
                await user.should_see("Загружаю статистику индекса")
                assert await asyncio.to_thread(started.wait, 3)
                await user.should_not_see("Pipeline индексации", retries=1)
                if outcome == "navigation":
                    state.screen = "shifts"
            finally:
                release.set()
            assert await asyncio.to_thread(finished.wait, 3)
            if outcome == "loaded":
                await user.should_see("Автозапуск по расписанию выключен")
                await user.should_see("Pipeline индексации")
                await user.should_not_see("Загружаю статистику индекса")
            elif outcome == "error":
                await user.should_see("Повторить")
            else:
                await asyncio.sleep(0.1)
                await user.should_not_see("Pipeline индексации")
                assert state.stage_status_timer is state.index_progress_timer is None

    asyncio.run(exercise())
