import asyncio
import logging
import threading

from rag_catalog.ui.loop_health import LoopWatchdog, _stack_locations


def test_loop_warning_is_rate_limited_and_recovers(caplog):
    watcher = LoopWatchdog(None)
    watcher.last_tick = 100
    with caplog.at_level(logging.INFO):
        watcher.check(101)
        assert not caplog.records
        watcher.check(103)
        watcher.check(110)
        assert len(caplog.records) == 1
        assert 'lag_ms=3000' in caplog.text
        watcher.check(134)
        assert len(caplog.records) == 2
        watcher.last_tick = 135
        watcher.check(136)
        watcher.check(136.5)
        assert len(caplog.records) == 3
        assert 'web_event_loop_recovered' in caplog.text


def test_stack_contains_locations_but_not_source_or_local_values():
    secret_token = 'this-must-not-appear-in-log'
    stack = _stack_locations(threading.get_ident())
    assert 'test_stack_contains_locations_but_not_source_or_local_values' in stack
    assert secret_token not in stack


def test_stopped_watchdog_does_not_schedule_more_ticks():
    async def exercise():
        watcher = LoopWatchdog(asyncio.get_running_loop())
        watcher.start()
        handle = watcher.handle
        watcher.stop()
        watcher._tick()
        assert watcher.stopped.is_set()
        assert handle.cancelled()
        assert watcher.handle is handle
    asyncio.run(exercise())
