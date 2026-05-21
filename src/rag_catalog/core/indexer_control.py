"""
indexer_control.py — кооперативное управление индексатором через JSON-файл.

UI (`nice_app.py`) пишет в `logs/indexer_control.json` команду пользователя
(`running` / `pause` / `cancel`); индексатор (`index_rag.py`) читает её в
основном цикле и реагирует. Это лёгкий модуль без тяжёлых зависимостей,
чтобы UI мог его импортировать без подтягивания sentence-transformers.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

INDEXER_CONTROL_FILENAME = "indexer_control.json"
ALLOWED_COMMANDS = ("running", "pause", "cancel")


def _project_logs_dir() -> Path:
    """Каталог logs/ проекта."""
    return Path(__file__).resolve().parents[3] / "logs"


def indexer_control_path() -> Path:
    return _project_logs_dir() / INDEXER_CONTROL_FILENAME


def read_indexer_control() -> Dict[str, Any]:
    """Читает управляющий файл; если файла нет/повреждён — считает командой 'running'."""
    p = indexer_control_path()
    try:
        with p.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            return data
    except (OSError, ValueError):
        pass
    return {"command": "running"}


def write_indexer_control(command: str, *, run_id: str = "") -> None:
    """Атомарно перезаписывает управляющий файл."""
    if command not in ALLOWED_COMMANDS:
        raise ValueError(
            f"Недопустимая команда: {command!r}. Допустимо: {ALLOWED_COMMANDS}"
        )
    payload = {
        "command": command,
        "run_id": run_id,
        "ts": datetime.now().isoformat(timespec="seconds"),
    }
    logs_dir = _project_logs_dir()
    try:
        logs_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass
    p = indexer_control_path()
    tmp = p.with_suffix(p.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False)
    os.replace(tmp, p)


def get_current_command() -> str:
    """Возвращает текущую команду в нижнем регистре ('running' если файла нет)."""
    return str(read_indexer_control().get("command") or "running").lower()
