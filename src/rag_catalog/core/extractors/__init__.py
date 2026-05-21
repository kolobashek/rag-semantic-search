"""File text extractors used by indexing pipelines."""

from .files import (
    extract_doc_meta,
    extract_csv,
    extract_doc,
    extract_docx,
    extract_image,
    extract_pdf,
    extract_pptx,
    extract_rtf,
    extract_spreadsheet,
    extract_text,
    extract_xls,
    extract_xlsx,
    ocr_pdf,
)
from .contract import ExtractedDocument, TextBlock, blocks_from_legacy_text, document_from_legacy_text

__all__ = [
    "ExtractedDocument",
    "TextBlock",
    "blocks_from_legacy_text",
    "document_from_legacy_text",
    "extract_doc_meta",
    "extract_csv",
    "extract_doc",
    "extract_docx",
    "extract_image",
    "extract_pdf",
    "extract_pptx",
    "extract_rtf",
    "extract_spreadsheet",
    "extract_text",
    "extract_xls",
    "extract_xlsx",
    "ocr_pdf",
]
