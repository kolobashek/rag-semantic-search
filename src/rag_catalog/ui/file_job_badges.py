"""Optional per-page status badges; fetching them must not delay opening a file."""

import logging
from threading import BoundedSemaphore

from nicegui import run, ui

_readers = BoundedSemaphore(2)
_log = logging.getLogger(__name__)


def _read(fetch, ids):
    # Under contention omit optional badges instead of queuing work behind stale pages.
    if not _readers.acquire(blocking=False):
        return None
    try:
        return fetch(ids)
    except Exception:
        _log.warning("Could not load file job badges", exc_info=True)
        return None
    finally:
        _readers.release()


class FileJobBadges:
    def __init__(self):
        self.targets = []

    def place(self, file_id, paint):
        if file_id:
            target = ui.element("span").classes("contents")
            self.targets.append((str(file_id), target, paint))

    async def load(self, fetch):
        ids = list(dict.fromkeys(key for key, target, _ in self.targets if not target.is_deleted))
        if not ids:
            return
        jobs = await run.io_bound(_read, fetch, ids)
        if jobs is None:
            return
        for key, target, paint in self.targets:
            if not target.is_deleted:
                with target:
                    paint(jobs.get(key))

    def schedule(self, fetch):
        if self.targets:
            async def load_visible():
                await self.load(fetch)

            ui.timer(0, load_visible, once=True)
