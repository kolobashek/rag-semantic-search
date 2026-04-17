from __future__ import annotations

import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from rag_catalog.core import _platform_compat as _impl

platform = _impl.platform
_WMI_FALLBACK = _impl._WMI_FALLBACK
_PATCHED = _impl._PATCHED


def apply_windows_platform_workarounds() -> None:
    """Backward-compatible wrapper around the package implementation."""
    global _PATCHED
    _impl._PATCHED = _PATCHED
    _impl.apply_windows_platform_workarounds()
    _PATCHED = _impl._PATCHED
