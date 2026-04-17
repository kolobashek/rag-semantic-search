"""
Backward-compatible shim for legacy entrypoint.

Deprecated: use `rag_search.py` directly.
"""

from __future__ import annotations

import logging
import sys

from rag_catalog.core._platform_compat import apply_windows_platform_workarounds

apply_windows_platform_workarounds()

from rag_catalog.cli.rag_search import main as rag_search_main

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    logger.warning(
        "rag_search_fixed.py устарел и будет удалён. Используйте rag_search.py."
    )
    return rag_search_main()


if __name__ == "__main__":
    sys.exit(main())

