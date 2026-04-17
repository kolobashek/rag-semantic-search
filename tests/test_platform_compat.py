from __future__ import annotations

import _platform_compat as compat


def test_apply_windows_platform_workarounds_is_idempotent(monkeypatch) -> None:
    monkeypatch.setattr(compat, "_PATCHED", False)
    monkeypatch.setattr(compat.platform, "system", lambda: "Windows")
    monkeypatch.setattr(compat.platform, "_wmi_query", lambda *a, **k: ("x",), raising=False)
    monkeypatch.setattr(compat.platform, "processor", lambda: "old")

    compat.apply_windows_platform_workarounds()
    first_processor = compat.platform.processor()
    first_wmi = compat.platform._wmi_query()  # type: ignore[attr-defined]
    assert first_processor == "Unknown"
    assert first_wmi == compat._WMI_FALLBACK

    # second call should keep patched behavior without changing semantics
    compat.apply_windows_platform_workarounds()
    assert compat.platform.processor() == "Unknown"
    assert compat.platform._wmi_query() == compat._WMI_FALLBACK  # type: ignore[attr-defined]

