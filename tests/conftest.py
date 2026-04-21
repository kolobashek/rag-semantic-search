"""
conftest.py — общие фикстуры и мок-модули для pytest.

sentence_transformers и qdrant_client не установлены в CI-окружении,
поэтому регистрируем заглушки до того, как их попытаются импортировать.
Тесты, которым нужна реальная семантическая функциональность,
должны использовать @pytest.mark.skip или отдельный venv.
"""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock


def _install_stub(name: str) -> types.ModuleType:
    """Зарегистрировать пустой stub-модуль если он ещё не установлен."""
    if name in sys.modules:
        return sys.modules[name]  # уже есть — не трогаем
    mod = types.ModuleType(name)
    sys.modules[name] = mod
    return mod


# ── sentence_transformers ─────────────────────────────────────────────────────
if "sentence_transformers" not in sys.modules:
    _st = _install_stub("sentence_transformers")

    class _FakeST:
        def __init__(self, *a, **kw):
            pass
        def encode(self, texts, normalize_embeddings=True, batch_size=256, show_progress_bar=False):
            import numpy as np  # numpy обычно есть
            if isinstance(texts, str):
                return np.zeros(384, dtype="float32")
            return np.zeros((len(texts), 384), dtype="float32")

    _st.SentenceTransformer = _FakeST

# ── qdrant_client ────────────────────────────────────────────────────────────
if "qdrant_client" not in sys.modules:
    try:
        import qdrant_client  # noqa: F401
        import qdrant_client.models  # noqa: F401
    except ImportError:
        _qc = _install_stub("qdrant_client")
        _qc_models = _install_stub("qdrant_client.models")

        # Минимальные stubs для того, чтобы index_rag.py смог импортироваться
        for _cls in (
            "QdrantClient", "Distance", "FieldCondition", "Filter", "FilterSelector",
            "MatchValue", "PointStruct", "VectorParams",
        ):
            setattr(_qc, _cls, MagicMock())
            setattr(_qc_models, _cls, MagicMock())

        _qc.QdrantClient = MagicMock  # сделаем instantiable

# ── tqdm ─────────────────────────────────────────────────────────────────────
if "tqdm" not in sys.modules:
    _tqdm_mod = _install_stub("tqdm")

    def _tqdm(iterable=None, *a, **kw):
        return iterable or []

    _tqdm_mod.tqdm = _tqdm
