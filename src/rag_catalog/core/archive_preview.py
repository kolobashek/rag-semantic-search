"""Bounded, stdout-only archive member extraction; callers must authorize the archive."""

from __future__ import annotations

import shutil
import subprocess
import tempfile
import threading
from pathlib import Path

MAX_ARCHIVE_BYTES = 256 * 1024 * 1024
MAX_MEMBER_BYTES = 32 * 1024 * 1024
_SLOTS = threading.BoundedSemaphore(2)


def validate_member(member: str) -> str:
    clean = str(member or "").replace("\\", "/")
    if (not clean or len(clean) > 1024 or clean.startswith(("/", "-"))
            or any(part in {"", ".", ".."} for part in clean.split("/"))
            or any(char in clean for char in ':*?[]\x00\r\n')):
        raise ValueError("Invalid archive member path")
    return clean


def _copy_bounded(source, target, limit: int) -> None:
    total = 0
    while block := source.read(64 * 1024):
        total += len(block)
        if total > limit:
            raise ValueError("Archive preview exceeds its size limit")
        target.write(block)


def _extract(archive: Path, member: str, target: Path, *, timeout: float = 30) -> None:
    # Reuse the indexing tool discovery, but never its unbounded capture_output path.
    from .indexing.stage_runner import _command_archive_tool

    tool = _command_archive_tool()
    if not tool:
        raise RuntimeError("Archive reader (7z/bsdtar) is unavailable")
    name, executable = tool
    args = ([executable, "-xOf", str(archive), "--", member] if name == "bsdtar" else
            [executable, "x", "-so", "-y", "-spd", "-sccUTF-8", "--", str(archive), member])
    proc = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL,
                            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    timed_out = threading.Event()

    def expire():
        timed_out.set()
        proc.kill()

    timer = threading.Timer(timeout, expire)
    timer.start()
    try:
        with target.open("wb") as output:
            _copy_bounded(proc.stdout, output, MAX_MEMBER_BYTES)
        code = proc.wait(timeout=2)
        if timed_out.is_set():
            raise TimeoutError("Archive extraction timed out")
        if code or target.stat().st_size == 0:
            raise ValueError("Archive member is missing, empty, encrypted or unreadable")
    finally:
        timer.cancel()
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=5)
        proc.stdout.close()


def prepare_member(storage, descriptor: dict, member: str):
    """Return (TemporaryDirectory, file); release temporary storage after the response."""
    member = validate_member(member)
    if Path(str(descriptor.get("filename") or "")).suffix.lower() not in {".zip", ".rar", ".7z"}:
        raise ValueError("Unsupported archive format")
    if not 0 < int(descriptor.get("size_bytes") or 0) <= MAX_ARCHIVE_BYTES:
        raise ValueError("Archive is too large for online preview")
    if not _SLOTS.acquire(blocking=False):
        raise BlockingIOError("Archive preview is busy; retry later")
    temp = None
    try:
        if shutil.disk_usage(tempfile.gettempdir()).free < 1024 * 1024 * 1024:
            raise BlockingIOError("Not enough temporary disk space")
        temp = tempfile.TemporaryDirectory(prefix="rag-archive-preview-")
        archive = Path(temp.name) / ("source" + Path(descriptor["filename"]).suffix)
        if descriptor.get("mode") == "local_file":
            archive = Path(descriptor["file_path"])
            if archive.stat().st_size > MAX_ARCHIVE_BYTES:
                raise ValueError("Archive is too large for online preview")
        elif descriptor.get("mode") == "storage_stream":
            response = storage.open_download(descriptor["storage_key"])
            body = response["Body"]
            try:
                with archive.open("wb") as output:
                    _copy_bounded(body, output, MAX_ARCHIVE_BYTES)
            finally:
                body.close()
        else:
            raise ValueError("Archive storage does not support bounded reading")
        target = Path(temp.name) / "member"
        _extract(archive, member, target)
        return temp, target
    except BaseException:
        if temp is not None:
            temp.cleanup()
        raise
    finally:
        _SLOTS.release()
