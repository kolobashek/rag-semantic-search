# Bundled OCR Tools

Place portable OCR binaries here to avoid machine-level PATH dependencies.

Expected layout:

- `tools/tesseract/tesseract.exe` or `tools/tesseract/bin/tesseract.exe`
- `tools/poppler/Library/bin/*` or `tools/poppler/bin/*`

Optional overrides in `config.json`:

- `ocr_tesseract_cmd`: absolute path to `tesseract.exe`
- `ocr_poppler_bin`: absolute path to poppler `bin` directory

You can also override via env vars:

- `RAG_TESSERACT_CMD`
- `RAG_POPPLER_BIN`
