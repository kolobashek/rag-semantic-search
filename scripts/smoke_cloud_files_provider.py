"""Isolated Windows CfAPI placeholder/hydration smoke test."""

from __future__ import annotations

import argparse
import ctypes
import json
import shutil
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

CONTENT = b"RAG Cloud Files hydration smoke test\n"
CONTENT_V2 = b"RAG Cloud Files updated content\n"
FILE_ATTRIBUTE_OFFLINE = 0x00001000
FILE_ATTRIBUTE_RECALL_ON_OPEN = 0x00040000
FILE_ATTRIBUTE_PINNED = 0x00080000
FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS = 0x00400000
INVALID_FILE_ATTRIBUTES = 0xFFFFFFFF
CF_PLACEHOLDER_STATE_PLACEHOLDER = 0x00000001
CF_PLACEHOLDER_STATE_IN_SYNC = 0x00000008


class Handler(BaseHTTPRequestHandler):
    range_requests: list[str] = []
    uploads: list[dict[str, object]] = []
    deletes: list[str] = []
    content = CONTENT
    version = "version-1"

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == "/api/cloud-drive/sync/clients":
            self._json({"id": "smoke-client"})
            return
        if path == "/api/cloud-drive/sync/heartbeat":
            self._json({"ok": True})
            return
        if path == "/api/cloud-drive/upload":
            parsed = urlparse(self.path)
            parent = parse_qs(parsed.query).get("parent_path", [""])[0]
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            content_type = self.headers.get("Content-Type", "")
            boundary = content_type.split("boundary=", 1)[-1].strip().strip('"').encode()
            parts = body.split(b"--" + boundary)
            file_part = next(part for part in parts if b"filename=" in part)
            headers, payload = file_part.split(b"\r\n\r\n", 1)
            payload = payload.rsplit(b"\r\n", 1)[0]
            disposition = next(
                line for line in headers.split(b"\r\n") if line.lower().startswith(b"content-disposition:")
            )
            filename = (
                disposition.split(b"filename=", 1)[1]
                .split(b";", 1)[0]
                .strip()
                .strip(b'"')
                .decode("utf-8")
            )
            cloud_path = f"{parent}/{filename}".strip("/")
            Handler.uploads.append({"path": cloud_path, "content": payload})
            self._json(
                {
                    "node_type": "file",
                    "id": f"uploaded-{len(Handler.uploads)}",
                    "path": cloud_path,
                    "name": filename,
                    "created_at": "2026-07-23T00:00:00+00:00",
                    "updated_at": "2026-07-23T00:00:00+00:00",
                    "deleted_at": "",
                    "current_version_id": f"upload-version-{len(Handler.uploads)}",
                    "mime_type": "application/octet-stream",
                    "size_bytes": len(payload),
                    "checksum": "uploaded",
                }
            )
            return
        if path == "/api/cloud-drive/delete":
            parsed = urlparse(self.path)
            cloud_path = parse_qs(parsed.query).get("path", [""])[0]
            Handler.deletes.append(cloud_path)
            self._json({"ok": True, "path": cloud_path})
            return
        self.send_error(404)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/cloud-drive/changes":
            cursor = parse_qs(parsed.query).get("since", [""])[0]
            if not cursor:
                self._json(
                    {
                        "next_cursor": "cursor-1",
                        "acl_revision": "acl-1",
                        "changes": [
                            {
                                "node_type": "folder",
                                "id": "folder-1",
                                "path": "Demo",
                                "name": "Demo",
                                "created_at": "2026-07-21T00:00:00+00:00",
                                "updated_at": "2026-07-21T00:00:00+00:00",
                                "deleted_at": "",
                                "size_bytes": 0,
                            },
                            {
                                "node_type": "file",
                                "id": "file-1",
                                "path": "Demo/report.txt",
                                "name": "report.txt",
                                "created_at": "2026-07-21T00:00:00+00:00",
                                "updated_at": "2026-07-21T00:00:00+00:00",
                                "deleted_at": "",
                                "current_version_id": Handler.version,
                                "mime_type": "text/plain",
                                "size_bytes": len(Handler.content),
                                "checksum": "smoke",
                            },
                        ],
                    }
                )
            else:
                self._json({"next_cursor": cursor, "acl_revision": "acl-1", "changes": []})
            return
        if parsed.path == "/api/cloud-drive/download":
            range_header = self.headers.get("Range", "")
            Handler.range_requests.append(range_header)
            start, end = 0, len(Handler.content) - 1
            if range_header.startswith("bytes="):
                raw_start, raw_end = range_header[6:].split("-", 1)
                start = int(raw_start)
                end = min(int(raw_end), end)
            body = Handler.content[start : end + 1]
            self.send_response(206)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(Handler.content)}")
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_error(404)

    def log_message(self, _format: str, *_args: object) -> None:
        return

    def _json(self, payload: object) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def file_attributes(path: Path) -> int:
    value = ctypes.windll.kernel32.GetFileAttributesW(str(path))
    if value == INVALID_FILE_ATTRIBUTES:
        raise ctypes.WinError()
    return int(value)


def placeholder_state(path: Path) -> int:
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
    ]
    create_file.restype = ctypes.c_void_p
    handle = create_file(
        str(path),
        0x80,
        0x1 | 0x2 | 0x4,
        None,
        3,
        0x02000000,
        None,
    )
    if handle == ctypes.c_void_p(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        cldapi = ctypes.WinDLL("cldapi", use_last_error=True)
        get_info = cldapi.CfGetPlaceholderInfo
        get_info.argtypes = [
            ctypes.c_void_p,
            ctypes.c_int,
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.POINTER(ctypes.c_uint32),
        ]
        get_info.restype = ctypes.c_long
        buffer = ctypes.create_string_buffer(8192)
        returned = ctypes.c_uint32()
        result = get_info(handle, 0, buffer, len(buffer), ctypes.byref(returned))
        if result < 0:
            return 0
        in_sync = int.from_bytes(buffer.raw[4:8], "little", signed=True)
        state = CF_PLACEHOLDER_STATE_PLACEHOLDER
        if in_sync == 1:
            state |= CF_PLACEHOLDER_STATE_IN_SYNC
        return state
    finally:
        kernel32.CloseHandle(handle)


def wait_for_upload(
    path: Path,
    expected: bytes,
    process: subprocess.Popen[str],
    timeout: float = 20.0,
) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(
                f"Provider exited before upload ({process.returncode}); uploads={Handler.uploads!r}.\n"
                f"{stdout}\n{stderr}"
            )
        upload = next(
            (
                item
                for item in reversed(Handler.uploads)
                if item["path"] == "Demo/local.txt" and item["content"] == expected
            ),
            None,
        )
        if upload is not None:
            state = placeholder_state(path)
            if state & CF_PLACEHOLDER_STATE_PLACEHOLDER and state & CF_PLACEHOLDER_STATE_IN_SYNC:
                return state
        time.sleep(0.1)
    raise TimeoutError(f"Local file was not uploaded and marked in sync: {path}")


def wait_for_delete(cloud_path: str, process: subprocess.Popen[str], timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(f"Provider exited before delete ({process.returncode}).\n{stdout}\n{stderr}")
        if cloud_path in Handler.deletes:
            return
        time.sleep(0.1)
    raise TimeoutError(f"Local deletion was not sent to the server: {cloud_path}")


def wait_for_file(path: Path, process: subprocess.Popen[str], timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(f"Provider exited early ({process.returncode}).\n{stdout}\n{stderr}")
        time.sleep(0.1)
    raise TimeoutError(f"Placeholder was not created: {path}")


def wait_for_recall(path: Path, expected_size: int, process: subprocess.Popen[str], timeout: float = 20.0) -> int:
    deadline = time.monotonic() + timeout
    recall_mask = FILE_ATTRIBUTE_OFFLINE | FILE_ATTRIBUTE_RECALL_ON_OPEN | FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(f"Provider exited early ({process.returncode}).\n{stdout}\n{stderr}")
        if path.exists() and path.stat().st_size == expected_size:
            attributes = file_attributes(path)
            if attributes & recall_mask:
                return attributes
        time.sleep(0.1)
    raise TimeoutError(f"Updated placeholder was not dehydrated: {path}")


def wait_for_background_hydration(
    path: Path,
    process: subprocess.Popen[str],
    timeout: float = 20.0,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(f"Offline provider exited early ({process.returncode}).\n{stdout}\n{stderr}")
        if path.exists() and Handler.range_requests:
            return
        time.sleep(0.1)
    raise TimeoutError(f"Pinned placeholder was not hydrated in background: {path}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("provider", type=Path)
    parser.add_argument("--previous-provider", type=Path)
    args = parser.parse_args()
    provider = args.provider.resolve()
    if not provider.is_file():
        raise FileNotFoundError(provider)
    previous_provider = args.previous_provider.resolve() if args.previous_provider else None
    if previous_provider is not None and not previous_provider.is_file():
        raise FileNotFoundError(previous_provider)

    workspace = Path(tempfile.mkdtemp(prefix="rag-cfapi-smoke-"))
    root = workspace / "RAG Cloud Drive"
    config = workspace / "main" / "config.json"
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    origin = f"http://127.0.0.1:{server.server_port}"
    command = [
        str(provider),
        "--config",
        str(config),
        "--server",
        origin,
        "--token",
        "smoke-token",
        "--root",
        str(root),
        "--run-seconds",
        "12",
    ]
    process: subprocess.Popen[str] | None = None
    offline_process: subprocess.Popen[str] | None = None
    previous_process: subprocess.Popen[str] | None = None
    upgraded_process: subprocess.Popen[str] | None = None
    placeholder = root / "Demo" / "report.txt"
    offline_root = workspace / "RAG Cloud Drive Offline"
    offline_config = workspace / "offline" / "config.json"
    upgrade_root = workspace / "RAG Cloud Drive Upgrade"
    upgrade_config = workspace / "upgrade" / "config.json"
    try:
        Handler.uploads.clear()
        Handler.deletes.clear()
        upgrade_tested = False
        if previous_provider is not None:
            previous_command = [
                str(previous_provider),
                "--config",
                str(upgrade_config),
                "--server",
                origin,
                "--token",
                "smoke-token",
                "--root",
                str(upgrade_root),
                "--run-seconds",
                "2",
            ]
            previous_process = subprocess.Popen(
                previous_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            wait_for_file(upgrade_root / "Demo" / "report.txt", previous_process)
            stdout, stderr = previous_process.communicate(timeout=10)
            if previous_process.returncode != 0:
                raise RuntimeError(f"Previous provider failed.\n{stdout}\n{stderr}")

            upgraded_process = subprocess.Popen(
                [str(provider), *previous_command[1:]],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            stdout, stderr = upgraded_process.communicate(timeout=15)
            if upgraded_process.returncode != 0:
                raise RuntimeError(f"Provider registration upgrade failed.\n{stdout}\n{stderr}")
            upgrade_tested = True

        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        wait_for_file(placeholder, process)
        if placeholder.stat().st_size != len(CONTENT):
            raise AssertionError("Placeholder logical size is incorrect.")
        before = file_attributes(placeholder)
        initial_state = placeholder_state(placeholder)
        if not initial_state & CF_PLACEHOLDER_STATE_PLACEHOLDER:
            stat_result = placeholder.lstat()
            raise AssertionError(
                "Server file is not a placeholder: "
                f"state=0x{initial_state:08x}, "
                f"attrs=0x{getattr(stat_result, 'st_file_attributes', 0):08x}, "
                f"tag=0x{getattr(stat_result, 'st_reparse_tag', 0):08x}"
            )
        if not initial_state & CF_PLACEHOLDER_STATE_IN_SYNC:
            raise AssertionError(f"Server file is not marked in sync: 0x{initial_state:08x}")
        recall_mask = FILE_ATTRIBUTE_OFFLINE | FILE_ATTRIBUTE_RECALL_ON_OPEN | FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS
        if not before & recall_mask:
            raise AssertionError(f"Placeholder has no recall attribute before open: 0x{before:08x}")
        if Handler.range_requests:
            raise AssertionError("Content was downloaded before the first file open.")

        local_content = b"Created locally and uploaded on demand\n"
        local_file = root / "Demo" / "local.txt"
        local_file.write_bytes(local_content)
        local_state = wait_for_upload(local_file, local_content, process)
        updated_local_content = b"Edited locally and uploaded again\n"
        local_file.write_bytes(updated_local_content)
        local_state = wait_for_upload(local_file, updated_local_content, process)
        local_file.unlink()
        wait_for_delete("Demo/local.txt", process)

        if placeholder.read_bytes() != CONTENT:
            raise AssertionError("Hydrated content does not match the server content.")
        if not Handler.range_requests or not Handler.range_requests[-1].startswith("bytes=0-"):
            raise AssertionError(f"Hydration did not use HTTP Range: {Handler.range_requests}")
        stdout, stderr = process.communicate(timeout=20)
        if process.returncode != 0:
            raise RuntimeError(f"Provider did not shut down cleanly.\n{stdout}\n{stderr}")

        Handler.content = CONTENT_V2
        Handler.version = "version-2"
        Handler.range_requests.clear()
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        updated_attributes = wait_for_recall(placeholder, len(CONTENT_V2), process)
        if Handler.range_requests:
            raise AssertionError("Updated content was downloaded before the next open.")
        if placeholder.read_bytes() != CONTENT_V2:
            raise AssertionError("Updated hydrated content does not match the server content.")
        if not Handler.range_requests or not Handler.range_requests[-1].startswith("bytes=0-"):
            raise AssertionError(f"Updated hydration did not use HTTP Range: {Handler.range_requests}")
        stdout, stderr = process.communicate(timeout=20)
        if process.returncode != 0:
            raise RuntimeError(f"Updated provider did not shut down cleanly.\n{stdout}\n{stderr}")

        Handler.range_requests.clear()
        offline_command = [
            str(provider),
            "--config",
            str(offline_config),
            "--server",
            origin,
            "--token",
            "smoke-token",
            "--root",
            str(offline_root),
            "--keep-all-offline",
            "--run-seconds",
            "5",
        ]
        offline_process = subprocess.Popen(
            offline_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        offline_placeholder = offline_root / "Demo" / "report.txt"
        wait_for_background_hydration(offline_placeholder, offline_process)
        stdout, stderr = offline_process.communicate(timeout=10)
        if offline_process.returncode != 0:
            raise RuntimeError(f"Offline provider did not shut down cleanly.\n{stdout}\n{stderr}")
        if offline_placeholder.read_bytes() != CONTENT_V2:
            raise AssertionError("Pinned placeholder content does not match the server content.")
        offline_attributes = file_attributes(offline_placeholder)
        if not offline_attributes & FILE_ATTRIBUTE_PINNED:
            raise AssertionError(f"Offline placeholder is not pinned: 0x{offline_attributes:08x}")
        if offline_attributes & recall_mask:
            raise AssertionError(f"Offline placeholder still requires recall: 0x{offline_attributes:08x}")
        print(
            json.dumps(
                {
                    "ok": True,
                    "placeholder": str(placeholder),
                    "logical_size": placeholder.stat().st_size,
                    "attributes_before": f"0x{before:08x}",
                    "initial_placeholder_state": f"0x{initial_state:08x}",
                    "local_upload_state": f"0x{local_state:08x}",
                    "local_uploads": len(Handler.uploads),
                    "local_deletes": len(Handler.deletes),
                    "updated_attributes_before": f"0x{updated_attributes:08x}",
                    "range_requests": Handler.range_requests,
                    "offline_hydrated": True,
                    "offline_attributes": f"0x{offline_attributes:08x}",
                    "registration_upgrade": upgrade_tested,
                },
                ensure_ascii=False,
            )
        )
        return 0
    finally:
        if process is not None:
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        for upgrade_process in (previous_process, upgraded_process):
            if upgrade_process is not None and upgrade_process.poll() is None:
                upgrade_process.terminate()
                try:
                    upgrade_process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    upgrade_process.kill()
                    upgrade_process.wait(timeout=5)
        if offline_process is not None and offline_process.poll() is None:
            offline_process.terminate()
            try:
                offline_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                offline_process.kill()
                offline_process.wait(timeout=5)
        time.sleep(1)
        subprocess.run(
            [str(provider), "--config", str(config), "--root", str(root), "--unregister"],
            check=False,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            [str(provider), "--config", str(offline_config), "--root", str(offline_root), "--unregister"],
            check=False,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            [str(provider), "--config", str(upgrade_config), "--root", str(upgrade_root), "--unregister"],
            check=False,
            capture_output=True,
            text=True,
        )
        server.shutdown()
        server.server_close()
        shutil.rmtree(workspace, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
