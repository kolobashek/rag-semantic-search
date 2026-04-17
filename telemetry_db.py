from __future__ import annotations

import importlib
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

_module = importlib.import_module("rag_catalog.core.telemetry_db")
globals().update(_module.__dict__)

if __name__ == "__main__":
    _main = getattr(_module, "main", None)
    if _main is not None:
        result = _main()
        if result is not None:
            raise SystemExit(result)
else:
    sys.modules[__name__] = _module
