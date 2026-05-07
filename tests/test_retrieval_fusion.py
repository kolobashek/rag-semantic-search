from __future__ import annotations

from rag_catalog.core.retrieval import rrf_fuse


def test_rrf_fuse_merges_duplicates_and_prefers_best_payload() -> None:
    dense = [
        {"full_path": "A.docx", "chunk_index": 0, "type": "file", "score": 0.7},
        {"full_path": "B.docx", "chunk_index": 0, "type": "file", "score": 0.6},
    ]
    lexical = [
        {"full_path": "B.docx", "chunk_index": 0, "type": "file", "score": 0.95},
        {"full_path": "C.docx", "chunk_index": 0, "type": "file", "score": 0.5},
    ]

    fused = rrf_fuse([dense, lexical], limit=3, k=60)

    assert [item["full_path"] for item in fused] == ["B.docx", "A.docx", "C.docx"]
    assert fused[0]["score"] == 0.95
    assert fused[0]["fusion"] == "rrf"
    assert fused[0]["rank_score"] > fused[1]["rank_score"]
