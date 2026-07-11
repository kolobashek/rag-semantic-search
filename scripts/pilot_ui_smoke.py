from __future__ import annotations

import argparse
import json
import os
import secrets
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from rag_catalog.core.cloud_drive import CloudDriveService
from rag_catalog.core.telemetry_db import TelemetryDB
from rag_catalog.core.user_auth_db import UserAuthDB

SCREEN_ROUTES = ("search", "explorer", "jobs", "index", "stats", "settings")
VIEWPORTS = ((480, 900), (900, 900), (1280, 900))


def _free_port(host: str = "127.0.0.1") -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def _browser_executable(explicit: str = "") -> str:
    candidates = [
        explicit,
        os.environ.get("RAG_PLAYWRIGHT_BROWSER", ""),
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    ]
    for candidate in candidates:
        path = Path(str(candidate or "").strip()).expanduser()
        if str(path) and path.is_file():
            return str(path.resolve())
    try:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as playwright:
            bundled = Path(playwright.chromium.executable_path)
        if bundled.is_file():
            return str(bundled)
    except Exception:
        pass
    raise RuntimeError(
        "Chromium/Chrome/Edge executable не найден. Укажите --browser-executable "
        "или установите browser командой `playwright install chromium`."
    )


def _wait_http(url: str, *, timeout_sec: float = 30.0) -> float:
    started = time.perf_counter()
    deadline = started + max(1.0, timeout_sec)
    last_error = ""
    while time.perf_counter() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1.0) as response:
                if 200 <= int(response.status) < 500:
                    return round((time.perf_counter() - started) * 1000, 1)
        except (OSError, urllib.error.URLError) as exc:
            last_error = str(exc)
        time.sleep(0.2)
    raise RuntimeError(f"NiceGUI smoke contour не поднялся за {timeout_sec:.0f} с: {last_error}")


def _prepare_contour(root: Path, *, qdrant_url: str) -> dict[str, str]:
    catalog = root / "catalog"
    state = root / "state"
    storage = root / "storage"
    catalog.mkdir(parents=True, exist_ok=True)
    state.mkdir(parents=True, exist_ok=True)
    (catalog / "Договор поставки.txt").write_text(
        "Договор поставки для browser smoke. Номер TEST-2026.", encoding="utf-8"
    )
    folder = catalog / "Документы"
    folder.mkdir(exist_ok=True)
    (folder / "Карточка предприятия.txt").write_text(
        "Карточка предприятия Smoke Test.", encoding="utf-8"
    )

    config = {
        "catalog_path": str(catalog),
        "qdrant_db_path": str(state),
        # A closed HTTP endpoint keeps the smoke isolated and prevents fallback
        # to an embedded Qdrant instance, model loading and background indexing.
        "qdrant_url": str(qdrant_url or "http://127.0.0.1:9"),
        "collection_name": "pilot_ui_smoke",
        "embedding_model": "sentence-transformers/all-MiniLM-L6-v2",
        "search_warmup_enabled": False,
        "telegram_enabled": False,
        "telegram_bot_token": "",
        "users_db_path": str(state / "users.db"),
        "telemetry_db_path": str(state / "telemetry.db"),
        "cloud_drive_enabled": True,
        "cloud_drive_db_path": str(state / "cloud_drive.db"),
        "cloud_drive_storage": "local",
        "cloud_drive_storage_root": str(storage),
        "cloud_drive_autosync_minutes": 0,
        "cloud_drive_public_links_enabled": False,
        "cloud_drive_acl": {
            "roles": {"admin": ["*"], "user": ["Users/pilot-smoke-user"]},
        },
        "llm_enabled": False,
        "ui_reconnect_timeout_sec": 5,
    }
    config_path = root / "config.json"
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")

    service = CloudDriveService.from_config(config)
    service.bootstrap_from_catalog(str(catalog), import_files=True)
    username = "pilot-smoke-admin"
    password = secrets.token_urlsafe(24)
    auth = UserAuthDB(config["users_db_path"])
    if not auth.admin_create_user(
        username=username,
        display_name="Pilot Smoke Admin",
        password=password,
        role="admin",
        status="active",
        must_change_password=False,
    ):
        raise RuntimeError("Не удалось создать isolated smoke admin.")
    if not auth.admin_create_user(
        username="pilot-smoke-user",
        display_name="Pilot Smoke User",
        password=secrets.token_urlsafe(24),
        role="user",
        status="active",
        must_change_password=False,
    ):
        raise RuntimeError("Не удалось создать isolated smoke user.")
    return {
        "config_path": str(config_path),
        "username": username,
        "password": password,
        "admin_token": auth.create_session(username=username),
        "user_token": auth.create_session(username="pilot-smoke-user"),
        "telemetry_db_path": str(config["telemetry_db_path"]),
    }


def _layout_probe(page: Any) -> dict[str, Any]:
    return dict(
        page.evaluate(
            """
            () => {
              const root = document.documentElement;
              const body = document.body;
              const visible = (el) => !!el && getComputedStyle(el).display !== 'none' &&
                getComputedStyle(el).visibility !== 'hidden' && el.getBoundingClientRect().width > 0;
              const reconnect = document.querySelector('#popup.nicegui-error-popup[aria-hidden="false"]');
              const busy = document.querySelector('.rag-global-busy');
              const nav = document.querySelector('.rag-hdr-nav');
              const burger = document.querySelector('.rag-mobile-menu-button');
              return {
                viewport_width: window.innerWidth,
                document_width: root.scrollWidth,
                body_width: body ? body.scrollWidth : 0,
                horizontal_overflow: Math.max(root.scrollWidth, body ? body.scrollWidth : 0) > window.innerWidth + 1,
                reconnect_visible: visible(reconnect),
                busy_visible: visible(busy) && !busy.classList.contains('hidden'),
                navigation_available: visible(nav) || visible(burger),
                main_text_length: (document.querySelector('main')?.innerText || '').trim().length,
              };
            }
            """
        )
    )


def _record_check(checks: list[dict[str, Any]], name: str, started: float, **details: Any) -> None:
    checks.append(
        {
            "name": name,
            "ok": True,
            "duration_ms": round((time.perf_counter() - started) * 1000, 1),
            "details": details,
        }
    )


def _click_only_visible(locator: Any, *, label: str) -> None:
    visible = [item for item in locator.all() if item.is_visible()]
    if len(visible) != 1:
        raise RuntimeError(f"Ожидался один видимый элемент {label!r}, найдено {len(visible)}.")
    q_item = visible[0].locator(
        "xpath=ancestor::*[contains(concat(' ', normalize-space(@class), ' '), ' q-item ')][1]"
    )
    if q_item.count() == 1:
        q_item.click()
    else:
        visible[0].click()


def _run_browser_smoke(
    *,
    base_url: str,
    username: str,
    password: str,
    admin_token: str,
    user_token: str,
    browser_executable: str,
    artifact_dir: Path,
) -> dict[str, Any]:
    from playwright.sync_api import sync_playwright

    checks: list[dict[str, Any]] = []
    console_errors: list[str] = []
    page_errors: list[str] = []
    started_at = datetime.now().astimezone().isoformat(timespec="seconds")
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True, executable_path=browser_executable)
        context = browser.new_context(viewport={"width": 1280, "height": 900}, color_scheme="dark")
        page = context.new_page()
        page.on(
            "console",
            lambda message: console_errors.append(message.text)
            if message.type == "error" and "favicon" not in message.text.lower()
            else None,
        )
        page.on("pageerror", lambda error: page_errors.append(str(error)))
        page.set_default_timeout(15_000)
        try:
            started = time.perf_counter()
            page.goto(f"{base_url}/search", wait_until="domcontentloaded")
            page.get_by_label("Логин или email", exact=True).fill(username)
            page.get_by_label("Пароль", exact=True).fill(password)
            page.get_by_role("button", name="Войти", exact=True).click()
            page.get_by_text("ВНУТРЕННИЙ ПОИСК КОМПАНИИ", exact=True).wait_for(state="visible")
            _record_check(checks, "authenticated_login", started)

            started = time.perf_counter()
            query = "договор поставки TEST-2026"
            search_input = page.get_by_placeholder(
                "Введите название, номер, контрагента или фразу из документа", exact=True
            )
            search_input.fill(query)
            page.get_by_role("button", name="Файлы", exact=True).click()
            page.wait_for_url("**/explorer")
            page.get_by_role("button", name="Поиск", exact=True).click()
            page.wait_for_url("**/search")
            search_input = page.get_by_placeholder(
                "Введите название, номер, контрагента или фразу из документа", exact=True
            )
            if search_input.input_value() != query:
                raise RuntimeError("Search query потерян после перехода search -> explorer -> search.")
            _record_check(checks, "search_state_transition", started, query=query)

            started = time.perf_counter()
            page.goto(f"{base_url}/settings", wait_until="domcontentloaded")
            main = page.locator("main")
            main.get_by_text("Настройки", exact=True).wait_for(state="visible")
            users_button = main.get_by_text("Пользователи", exact=True)
            if users_button.count() == 1:
                users_button.click()
            groups = main.get_by_text("Группы доступа", exact=True)
            groups.wait_for(state="visible")
            _click_only_visible(groups, label="Группы доступа")
            main.get_by_text("Группы объединяют пользователей", exact=False).wait_for(state="visible")
            _click_only_visible(main.get_by_text("Создать группу", exact=True), label="Создать группу")
            group_name = main.get_by_label("Название группы", exact=True)
            group_name.wait_for(state="visible")
            group_name.fill("Pilot smoke group")
            main.get_by_role("button", name="Создать группу", exact=True).click()
            main.get_by_text("Pilot smoke group", exact=False).wait_for(state="attached")
            _record_check(checks, "group_management", started)

            started = time.perf_counter()
            success_correlation = f"pilot-smoke-success-{uuid.uuid4().hex[:12]}"
            success_response = page.request.get(
                f"{base_url}/api/cloud-drive/list?path=",
                headers={
                    "Authorization": f"Bearer {admin_token}",
                    "X-Correlation-ID": success_correlation,
                },
            )
            if success_response.status != 200:
                raise RuntimeError(f"Admin Cloud Drive list API вернул {success_response.status} вместо 200.")
            if success_response.headers.get("x-correlation-id") != success_correlation:
                raise RuntimeError("Admin API не вернул переданный correlation ID.")
            denied_correlation = f"pilot-smoke-denied-{uuid.uuid4().hex[:12]}"
            denied_response = page.request.get(
                f"{base_url}/api/cloud-drive/list?path=%D0%94%D0%BE%D0%BA%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D1%8B",
                headers={
                    "Authorization": f"Bearer {user_token}",
                    "X-Correlation-ID": denied_correlation,
                },
            )
            if denied_response.status != 403:
                raise RuntimeError(f"Закрытый Cloud Drive path вернул {denied_response.status} вместо 403.")
            if denied_response.headers.get("x-correlation-id") != denied_correlation:
                raise RuntimeError("Denied API не вернул переданный correlation ID.")
            _record_check(
                checks,
                "acl_api_enforcement",
                started,
                success_correlation_id=success_correlation,
                denied_correlation_id=denied_correlation,
            )

            for width, height in VIEWPORTS:
                page.set_viewport_size({"width": width, "height": height})
                for screen in SCREEN_ROUTES:
                    started = time.perf_counter()
                    page.goto(f"{base_url}/{screen}", wait_until="domcontentloaded")
                    page.locator("main").wait_for(state="visible")
                    page.wait_for_timeout(250)
                    probe = _layout_probe(page)
                    if probe["horizontal_overflow"]:
                        raise RuntimeError(
                            f"Horizontal overflow на /{screen} при {width}px: "
                            f"document={probe['document_width']} body={probe['body_width']}"
                        )
                    if probe["reconnect_visible"]:
                        raise RuntimeError(f"Reconnect overlay видим на /{screen} при {width}px.")
                    if not probe["navigation_available"]:
                        raise RuntimeError(f"Навигация недоступна на /{screen} при {width}px.")
                    if int(probe["main_text_length"] or 0) < 1:
                        raise RuntimeError(f"Пустой main на /{screen} при {width}px.")
                    _record_check(
                        checks,
                        "responsive_screen",
                        started,
                        route=f"/{screen}",
                        viewport={"width": width, "height": height},
                        layout=probe,
                    )
        except Exception:
            artifact_dir.mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(artifact_dir / "failure.png"), full_page=True)
            raise
        finally:
            context.close()
            browser.close()

    unexpected_console = [item for item in console_errors if "net::ERR_ABORTED" not in item]
    if unexpected_console or page_errors:
        raise RuntimeError(
            f"Browser errors: console={unexpected_console[:3]}, page={page_errors[:3]}"
        )
    return {
        "ok": True,
        "started_at": started_at,
        "completed_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "base_url": base_url,
        "browser_executable": browser_executable,
        "checks": checks,
        "checks_passed": len(checks),
        "console_errors": unexpected_console,
        "page_errors": page_errors,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run isolated authenticated Paid Pilot UI smoke.")
    parser.add_argument("--output-dir", default="", help="Artifact directory under runtime/pilot-ui-smoke by default")
    parser.add_argument("--browser-executable", default="", help="Chrome/Edge/Chromium executable")
    parser.add_argument("--qdrant-url", default="", help="Optional Qdrant URL for the isolated contour")
    parser.add_argument("--keep-contour", action="store_true", help="Keep isolated config and SQLite files")
    args = parser.parse_args(argv)

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output = (
        Path(args.output_dir).expanduser().resolve()
        if str(args.output_dir or "").strip()
        else (Path("runtime") / "pilot-ui-smoke" / timestamp).resolve()
    )
    contour = output / "contour"
    output.mkdir(parents=True, exist_ok=True)
    contour_state = _prepare_contour(contour, qdrant_url=str(args.qdrant_url or ""))
    config_path = Path(contour_state["config_path"])
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    log_path = output / "nice_app.log"
    env = dict(os.environ)
    env.update(
        {
            "RAG_CONFIG_PATH": str(config_path),
            "RAG_DISABLE_DEFAULT_ADMIN": "1",
            "PYTHONUNBUFFERED": "1",
        }
    )
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    process: subprocess.Popen[Any] | None = None
    report: dict[str, Any]
    try:
        with log_path.open("w", encoding="utf-8") as log_handle:
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "rag_catalog.ui.nice_app",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(port),
                    "--no-show",
                ],
                cwd=Path.cwd(),
                env=env,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                creationflags=creationflags,
            )
            startup_ms = _wait_http(f"{base_url}/search")
            report = _run_browser_smoke(
                base_url=base_url,
                username=contour_state["username"],
                password=contour_state["password"],
                admin_token=contour_state["admin_token"],
                user_token=contour_state["user_token"],
                browser_executable=_browser_executable(str(args.browser_executable or "")),
                artifact_dir=output,
            )
            report["server_startup_ms"] = startup_ms
            acl_check = next(
                check for check in report["checks"] if check.get("name") == "acl_api_enforcement"
            )
            expected_ids = {
                str(acl_check["details"]["success_correlation_id"]),
                str(acl_check["details"]["denied_correlation_id"]),
            }
            audit_events = TelemetryDB(contour_state["telemetry_db_path"]).list_app_events(
                feature="cloud_drive", limit=100
            )
            matched = [
                event
                for event in audit_events
                if str((event.get("details") or {}).get("correlation_id") or "") in expected_ids
            ]
            matched_ids = {
                str((event.get("details") or {}).get("correlation_id") or "") for event in matched
            }
            if matched_ids != expected_ids or not any(event.get("ok") for event in matched) or not any(
                not event.get("ok") for event in matched
            ):
                raise RuntimeError("Audit telemetry не содержит ожидаемые success/denied correlation events.")
            report["checks"].append(
                {
                    "name": "audit_correlation_evidence",
                    "ok": True,
                    "duration_ms": 0.0,
                    "details": {
                        "events": [
                            {
                                "action": event.get("action"),
                                "ok": event.get("ok"),
                                "correlation_id": (event.get("details") or {}).get("correlation_id"),
                            }
                            for event in matched
                        ]
                    },
                }
            )
            report["checks_passed"] = len(report["checks"])
    except Exception as exc:
        report = {
            "ok": False,
            "completed_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "base_url": base_url,
            "error": str(exc),
            "log_path": str(log_path),
        }
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

    report["artifact_dir"] = str(output)
    report_path = output / "pilot-ui-smoke.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if not args.keep_contour:
        import shutil

        shutil.rmtree(contour, ignore_errors=True)
    summary = {
        "ok": bool(report.get("ok")),
        "checks_passed": int(report.get("checks_passed") or 0),
        "server_startup_ms": report.get("server_startup_ms"),
        "error": str(report.get("error") or ""),
        "artifact_path": str(report_path),
    }
    print(json.dumps(summary, ensure_ascii=True, indent=2))
    return 0 if report.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
