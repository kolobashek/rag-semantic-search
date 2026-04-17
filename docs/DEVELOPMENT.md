# Development

## Test

```powershell
python -m pytest -q tests
python -m py_compile app_ui.py rag_core.py index_rag.py telegram_bot.py
```

## Web UI

```powershell
streamlit run app_ui.py
```

## Native UI

```powershell
python windows_app.py
```

## Cleanup

```powershell
.\scripts\clean_project.ps1
```

The cleanup script removes Python caches, pytest cache, and build/dist folders.
It does not remove source code, tests, configuration, logs archive, Qdrant data,
or user databases.

