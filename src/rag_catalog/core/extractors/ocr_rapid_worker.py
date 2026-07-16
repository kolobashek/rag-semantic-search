"""Short-lived fail-closed RapidOCR worker used to release DirectML memory."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .ocr_rapid import _ocr_image_rapid_impl, _ocr_pdf_rapid_impl


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("pdf", "image"), required=True)
    parser.add_argument("--path", required=True)
    parser.add_argument("--first", type=int, required=True)
    parser.add_argument("--last", type=int, required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--poppler-bin", default="")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    source = Path(args.path)
    if args.mode == "pdf":
        text = _ocr_pdf_rapid_impl(
            source,
            poppler_bin=str(args.poppler_bin or ""),
            first_page=max(1, int(args.first)),
            last_page=max(1, int(args.last)),
        )
    else:
        text = _ocr_image_rapid_impl(
            source,
            max_pages=max(1, int(args.last)),
            first_frame=max(1, int(args.first)),
            last_frame=max(1, int(args.last)),
        )
    Path(args.output).write_text(text, encoding="utf-8", errors="replace")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
