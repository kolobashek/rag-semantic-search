from __future__ import annotations

import importlib
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parent / 'src'
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

_is_main = __name__ == '__main__'
_shim_name = __name__

_module = importlib.import_module('rag_catalog.cli.cloud_drive')
globals().update({k: v for k, v in _module.__dict__.items() if not k.startswith('__')})

if _is_main:
    _main = getattr(_module, 'main', None)
    if _main is not None:
        result = _main()
        if result is not None:
            raise SystemExit(result)
else:
    sys.modules[_shim_name] = _module
