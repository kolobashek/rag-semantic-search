"""Bounded profile for unattended catalogue updates."""
from pathlib import Path

import psutil

STAGE = "nightly_quick"
PHASES = ("metadata", "small")
MAX_CHUNKS = 5
MIN_AVAILABLE_MIB = 4096
START_WINDOW_SECONDS = 600


def check_resources(catalog: str | Path) -> None:
    try:
        available = psutil.virtual_memory().available // (1024 * 1024)
        accessible = bool(catalog) and Path(catalog).is_dir()
    except (OSError, psutil.Error) as exc:
        raise RuntimeError("Не удалось проверить ресурсы для ночной индексации") from exc
    if available < MIN_AVAILABLE_MIB:
        raise RuntimeError(f"Недостаточно RAM: доступно {available} МиБ, требуется {MIN_AVAILABLE_MIB} МиБ")
    if not accessible:
        raise RuntimeError("Исходный каталог недоступен")


def is_nightly_run(row: dict) -> bool:
    return str(row.get("note") or "").split(" | ", 1)[0] == f"stage={STAGE}"
