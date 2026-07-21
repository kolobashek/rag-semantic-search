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
FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS = 0x00400000
INVALID_FILE_ATTRIBUTES = 0xFFFFFFFF


class Handler(BaseHTTPRequestHandler):
    range_requests: list[str] = []
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("provider", type=Path)
    args = parser.parse_args()
    provider = args.provider.resolve()
    if not provider.is_file():
        raise FileNotFoundError(provider)

    workspace = Path(tempfile.mkdtemp(prefix="rag-cfapi-smoke-"))
    root = workspace / "RAG Cloud Drive"
    config = workspace / "config.json"
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
        "5",
    ]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    placeholder = root / "Demo" / "report.txt"
    try:
        wait_for_file(placeholder, process)
        if placeholder.stat().st_size != len(CONTENT):
            raise AssertionError("Placeholder logical size is incorrect.")
        before = file_attributes(placeholder)
        recall_mask = FILE_ATTRIBUTE_OFFLINE | FILE_ATTRIBUTE_RECALL_ON_OPEN | FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS
        if not before & recall_mask:
            raise AssertionError(f"Placeholder has no recall attribute before open: 0x{before:08x}")
        if Handler.range_requests:
            raise AssertionError("Content was downloaded before the first file open.")

        if placeholder.read_bytes() != CONTENT:
            raise AssertionError("Hydrated content does not match the server content.")
        if not Handler.range_requests or not Handler.range_requests[-1].startswith("bytes=0-"):
            raise AssertionError(f"Hydration did not use HTTP Range: {Handler.range_requests}")
        stdout, stderr = process.communicate(timeout=10)
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
        stdout, stderr = process.communicate(timeout=10)
        if process.returncode != 0:
            raise RuntimeError(f"Updated provider did not shut down cleanly.\n{stdout}\n{stderr}")
        print(
            json.dumps(
                {
                    "ok": True,
                    "placeholder": str(placeholder),
                    "logical_size": placeholder.stat().st_size,
                    "attributes_before": f"0x{before:08x}",
                    "updated_attributes_before": f"0x{updated_attributes:08x}",
                    "range_requests": Handler.range_requests,
                },
                ensure_ascii=False,
            )
        )
        return 0
    finally:
        if process.poll() is None:
            process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        time.sleep(1)
        subprocess.run(
            [str(provider), "--config", str(config), "--root", str(root), "--unregister"],
            check=False,
            capture_output=True,
            text=True,
        )
        server.shutdown()
        server.server_close()
        shutil.rmtree(workspace, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
