from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor

from rag_catalog.core.telemetry_db import TelemetryDB


def test_telemetry_db_concurrent_search_writes(tmp_path) -> None:
    db = TelemetryDB(str(tmp_path / "telemetry.db"))
    total = 80

    def write_one(i: int) -> None:
        db.log_search(
            source=f"worker-{i % 8}",
            query=f"query-{i}",
            limit_value=10,
            file_type=None,
            content_only=False,
            results_count=i % 5,
            duration_ms=i,
            ok=True,
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(write_one, range(total)))

    with sqlite3.connect(str(db.db_path)) as conn:
        rows = conn.execute("SELECT query FROM search_logs").fetchall()
    assert len(rows) == total
    assert {row[0] for row in rows} == {f"query-{i}" for i in range(total)}
