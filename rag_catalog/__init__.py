"""Import bootstrap for src/ layout.

This repo uses a `src/` layout. When running without `pip install -e .`,
`python -m rag_catalog...` does not see the package by default.

This lightweight namespace shim makes `rag_catalog` importable from the repo root
by extending package search path to `src/rag_catalog`.

It is intentionally excluded from packaging because setuptools only discovers
packages under `src/` (see `pyproject.toml`).
"""

from __future__ import annotations

import sys
from pathlib import Path
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)  # type: ignore[name-defined]

_src_root = Path(__file__).resolve().parent.parent / "src"
_src_pkg = _src_root / "rag_catalog"
if _src_pkg.exists():
    # Make submodules resolvable as `rag_catalog.*`
    __path__.append(str(_src_pkg))  # type: ignore[attr-defined]
    # Also allow absolute imports that rely on `src` being on sys.path
    if str(_src_root) not in sys.path:
        sys.path.insert(0, str(_src_root))

