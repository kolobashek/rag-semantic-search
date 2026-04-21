# RAG Catalog Architecture

## Layout

- `src/rag_catalog/core` — search core, indexing, OCR, auth storage, telemetry.
- `src/rag_catalog/ui` — Streamlit web UI and PyQt native UI.
- `src/rag_catalog/integrations` — Telegram integration.
- `src/rag_catalog/cli` — command-line entrypoints.
- `packaging` — PyInstaller `.spec` files.
- `logs/archive` — archived runtime logs.
- `artifacts` — generated release/build artifacts.
- `tmp` — temporary local work files.

## Backward Compatibility

Root-level files such as `app_ui.py`, `rag_core.py`, `index_rag.py`, and
`telegram_bot.py` are compatibility shims. They add `src` to `sys.path` and
delegate to package modules.

Existing commands continue to work:

```powershell
streamlit run app_ui.py
python rag_search.py --query "паспорта"
python index_rag.py
python telegram_bot.py
```

New code should import from package modules directly:

```python
from rag_catalog.core.rag_core import RAGSearcher, load_config
from rag_catalog.core.user_auth_db import UserAuthDB
```

## Runtime Assets

`config.json` and `icon.ico` remain in the project root. Package code resolves
them through the project root to preserve desktop, web, and build compatibility.

