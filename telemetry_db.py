from __future__ import annotations

from _entrypoint_shim import run_shim

run_shim(__name__, globals(), 'rag_catalog.core.telemetry_db')
