"""
Cross-module platform workarounds.

Keep all Windows-specific runtime monkey patches in one place to avoid
duplication and inconsistent behavior.
"""

from __future__ import annotations

import platform
from typing import Final

_PATCHED: bool = False
_WMI_FALLBACK: Final[tuple[str, str, str, str, str]] = (
    "10.0.19041",
    "1",
    "Multiprocessor Free",
    "0",
    "0",
)


def apply_windows_platform_workarounds() -> None:
    """
    Apply idempotent Windows workarounds before heavy ML imports.

    - platform._wmi_query can hang on some Python/Windows combos
    - platform.processor may block in restricted environments
    """
    global _PATCHED
    if _PATCHED:
        return
    if platform.system().lower() != "windows":
        _PATCHED = True
        return

    if hasattr(platform, "_wmi_query"):
        platform._wmi_query = lambda *a, **kw: _WMI_FALLBACK  # type: ignore[attr-defined]
    platform.processor = lambda: "Unknown"  # type: ignore[assignment]
    _PATCHED = True

