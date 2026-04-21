# RAG Catalog

Internal semantic search tool for DOCX, XLSX, XLS, and PDF catalogs.

The project uses Qdrant for vector search, `sentence-transformers` for
embeddings, NiceGUI for the main web UI, Streamlit as the legacy web UI, PyQt6
for the Windows UI, and optional OCR for scanned PDF files.

## Quick Start

```powershell
pip install -r requirements.txt
python index_rag.py --help
python rag_search.py --help
python nice_app.py
```

Legacy Streamlit UI is still available with `streamlit run app_ui.py`.

Use `config.example.json` as the template for local machine settings in
`config.json`. Keep real tokens, paths, logs, databases, and build artifacts out
of Git.

Docker deployment is available via `docker-compose.yml`; see `docs/DOCKER.md`.

## Documentation

- Full Russian operations guide: `ИНСТРУКЦИЯ.md`
- Architecture notes: `docs/ARCHITECTURE.md`
- Development checks: `docs/DEVELOPMENT.md`

## Checks

```powershell
python -m pytest -q tests
python -m py_compile app_ui.py nice_app.py rag_core.py index_rag.py telegram_bot.py windows_app.py run_automation.py
python test_imports.py
```
