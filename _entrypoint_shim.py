from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any, MutableMapping


def run_shim(shim_name: str, namespace: MutableMapping[str, Any], target_module: str) -> None:
    """Load a package module from a root compatibility shim.

    Root files are kept only for old commands like `python index_rag.py` and
    legacy imports. Runtime implementation lives under `src/rag_catalog`.
    """
    src = Path(__file__).resolve().parent / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))

    is_main = shim_name == "__main__"
    module = importlib.import_module(target_module)
    namespace.update({key: value for key, value in module.__dict__.items() if not key.startswith("__")})

    if is_main:
        main = getattr(module, "main", None)
        if main is not None:
            result = main()
            if result is not None:
                raise SystemExit(result)
    else:
        sys.modules[shim_name] = module
