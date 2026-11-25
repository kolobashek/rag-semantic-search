"""Share one background read of public login counters across concurrent visitors."""

import asyncio
import time
from datetime import date

from nicegui import run

from .helpers import _index_state_db_file, _read_login_screen_stats, _resolve_heartbeat_file, _telemetry_db_path


class LoginStats:
    def __init__(self):
        self.key = None
        self.expires = 0.0
        self.task = None

    async def get(self, cfg):
        key = (_index_state_db_file(cfg), _telemetry_db_path(cfg), _resolve_heartbeat_file(cfg), date.today())
        task = self.task
        if key != self.key or task is None or (task.done() and time.monotonic() >= self.expires):
            task = asyncio.create_task(run.io_bound(_read_login_screen_stats, cfg))
            self.key, self.task = key, task
            self.expires = time.monotonic() + 60
        return await asyncio.shield(task)


login_stats = LoginStats()
