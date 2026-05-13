from __future__ import annotations

import sys

from _entrypoint_shim import run_shim

if __name__ == "__main__" and any(arg in {"-h", "--help"} for arg in sys.argv[1:]):
    print(
        """usage: index_rag.py [-h] [--catalog CATALOG] [--db DB] [--url QDRANT_URL]
                    [--model MODEL] [--collection COLLECTION] [--recreate]
                    [--no-ocr] [--max-chunks MAX_CHUNKS] [--workers WORKERS]
                    [--onnx] [--stage {all,metadata,small,large,content}]
                    [--metadata-only-for METADATA_ONLY_FOR] [--metadata-only]
                    [--cleanup] [--mark-stage-metadata-for MARK_STAGE_METADATA_FOR]

RAG Indexer для DOCX/XLSX/XLS/PDF файлов

options:
  -h, --help            show this help message and exit
  --stage {all,metadata,small,large,content}
                        Этап индексирования. По умолчанию 'all'.
  --recreate            Пересоздать коллекцию и очистить state
  --cleanup             Только очистить индекс от файлов, удалённых с диска
"""
    )
    raise SystemExit(0)

run_shim(__name__, globals(), 'rag_catalog.core.index_rag')
