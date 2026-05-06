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
# единый запуск сервисов (web + qdrant + telegram при необходимости)
python -m rag_catalog.cli.launcher start
```

Legacy Streamlit UI is still available with `streamlit run app_ui.py`.

## Unified Launcher

Unified launcher controls web UI, Qdrant, and Telegram bot from one command.

```powershell
python -m rag_catalog.cli.launcher status
python -m rag_catalog.cli.launcher start
python -m rag_catalog.cli.launcher stop
python -m rag_catalog.cli.launcher restart
```

Behavior:

- starts web UI on `127.0.0.1:8080`
- starts local Docker Qdrant only when `qdrant_url` points to local host and port is down
- skips bot by default if `telegram_enabled=false` or token is empty

Use `config.example.json` as the template for local machine settings in
`config.json`. Keep real tokens, paths, logs, databases, and build artifacts out
of Git.

Docker deployment is available via `docker-compose.yml`; see `docs/DOCKER.md`.

## OCR Runtime (Bundled Tools)

OCR does not depend on global `PATH` anymore if binaries are placed in project
`tools/`:

- `tools/tesseract/tesseract.exe` (or `tools/tesseract/bin/tesseract.exe`)
- `tools/poppler/Library/bin` (or `tools/poppler/bin`)

Optional explicit overrides in `config.json`:

- `ocr_tesseract_cmd`
- `ocr_poppler_bin`

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
