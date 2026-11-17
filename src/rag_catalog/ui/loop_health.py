"""Read-only event-loop stall diagnostics without request data or frame locals."""

import asyncio
import logging
import sys
import threading
import time
from pathlib import Path

logger = logging.getLogger(__name__)


def _stack_locations(thread_id: int) -> str:
    frame = sys._current_frames().get(thread_id)
    locations = []
    while frame is not None and len(locations) < 12:
        locations.append(f'{Path(frame.f_code.co_filename).name}:{frame.f_lineno}:{frame.f_code.co_name}')
        frame = frame.f_back
    return ' <- '.join(locations)


class LoopWatchdog:
    def __init__(self, loop, *, threshold=2.0, cooldown=30.0):
        self.loop = loop
        self.thread_id = threading.get_ident()
        self.threshold = threshold
        self.cooldown = cooldown
        self.last_tick = time.monotonic()
        self.last_warning = float('-inf')
        self.stalled = False
        self.stopped = threading.Event()
        self.handle = None

    def _tick(self):
        if not self.stopped.is_set():
            self.last_tick = time.monotonic()
            self.handle = self.loop.call_later(.5, self._tick)

    def check(self, now):
        lag = now - self.last_tick
        if lag >= self.threshold:
            self.stalled = True
            if now - self.last_warning >= self.cooldown:
                self.last_warning = now
                logger.warning('web_event_loop_stall lag_ms=%.0f stack=%s',
                               lag * 1000, _stack_locations(self.thread_id))
        elif self.stalled:
            self.stalled = False
            logger.info('web_event_loop_recovered')

    def _watch(self):
        while not self.stopped.wait(.5):
            self.check(time.monotonic())

    def start(self):
        self._tick()
        threading.Thread(target=self._watch, name='web-loop-watchdog', daemon=True).start()

    def stop(self):
        self.stopped.set()
        if self.handle is not None:
            self.handle.cancel()


def register_loop_watchdog(app):
    watchdog = None

    async def start():
        nonlocal watchdog
        if watchdog is not None:
            watchdog.stop()
        watchdog = LoopWatchdog(asyncio.get_running_loop())
        watchdog.start()

    async def stop():
        if watchdog is not None:
            watchdog.stop()

    app.on_startup(start)
    app.on_shutdown(stop)
