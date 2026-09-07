"""Heartbeat индексатора: атомарный JSON-файл с прогрессом текущей стадии.

Используется супервизором/UI, чтобы отличить «работает медленно» от «умер».
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

DEFAULT_MAX_AGE_SEC = 900
STATUS_RUNNING = "running"
STATUS_FINISHED = "finished"
STATUS_FAILED = "failed"


def write_heartbeat(
    path: str | Path,
    *,
    stage: str,
    processed: int,
    total: int,
    run_id: str = "",
    status: str = STATUS_RUNNING,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Записать heartbeat атомарно (tmp + rename). Ошибки записи не фатальны."""
    payload: Dict[str, Any] = {
        "ts": time.time(),
        "stage": str(stage or ""),
        "processed": int(processed or 0),
        "total": int(total or 0),
        "run_id": str(run_id or ""),
        "status": str(status or STATUS_RUNNING),
        "pid": os.getpid(),
    }
    if extra:
        payload.update(extra)
    target = Path(path)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(f"{target.name}.{os.getpid()}.tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, target)
    except OSError as exc:
        logger.debug("Heartbeat не записан в %s: %s", target, exc)
    return payload


def read_heartbeat(path: str | Path) -> Optional[Dict[str, Any]]:
    """Прочитать heartbeat; None если файла нет или он повреждён."""
    target = Path(path)
    try:
        raw = target.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        data = json.loads(raw)
    except ValueError:
        return None
    return data if isinstance(data, dict) else None


def heartbeat_age_sec(hb: Optional[Dict[str, Any]], *, now: Optional[float] = None) -> float:
    if not hb:
        return float("inf")
    try:
        ts = float(hb.get("ts") or 0.0)
    except (TypeError, ValueError):
        ts = 0.0
    return max(0.0, float(time.time() if now is None else now) - ts)


def is_stale(
    hb: Optional[Dict[str, Any]],
    max_age_sec: float = DEFAULT_MAX_AGE_SEC,
    *,
    now: Optional[float] = None,
) -> bool:
    """True если heartbeat отсутствует или старше max_age_sec."""
    return heartbeat_age_sec(hb, now=now) > float(max_age_sec)


def describe_heartbeat(
    hb: Optional[Dict[str, Any]],
    *,
    max_age_sec: float = DEFAULT_MAX_AGE_SEC,
    now: Optional[float] = None,
) -> str:
    """Человекочитаемая строка для --status."""
    if not hb:
        return "heartbeat отсутствует (индексатор ещё не запускался или файл удалён)"
    age = heartbeat_age_sec(hb, now=now)
    status = str(hb.get("status") or STATUS_RUNNING)
    line = (
        f"stage={hb.get('stage')} status={status} "
        f"processed={hb.get('processed')}/{hb.get('total')} "
        f"run_id={hb.get('run_id') or '-'} pid={hb.get('pid') or '-'} "
        f"age={age / 60:.1f} мин"
    )
    if status == STATUS_RUNNING and is_stale(hb, max_age_sec, now=now):
        line += f"\nПРОГОН МЁРТВ: heartbeat не обновлялся {age / 60:.0f} мин"
    return line
