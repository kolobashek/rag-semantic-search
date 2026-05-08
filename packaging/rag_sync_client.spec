# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for rag_sync_client.exe"""

a = Analysis(
    ['../rag_sync_client.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[
        'watchdog.observers.winapi',
        'watchdog.observers.read_directory_changes',
        'watchdog.observers.polling',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['tkinter', 'matplotlib', 'numpy', 'PIL'],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='rag_sync_client',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
