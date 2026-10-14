"""Real browser usage collection and dashboard checks in an isolated catalog."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from pilot_ui_smoke import _browser_executable, _free_port, _layout_probe, _prepare_contour, _wait_http
from playwright.sync_api import sync_playwright


def main():
    output = Path("runtime/usage-smoke") / datetime.now().strftime("%Y%m%d-%H%M%S")
    output.mkdir(parents=True)
    fixture = _prepare_contour((output / "contour").resolve(), qdrant_url="")
    port = _free_port()
    base = f"http://127.0.0.1:{port}"
    env = dict(os.environ, RAG_CONFIG_PATH=str(fixture["config_path"]), RAG_DISABLE_DEFAULT_ADMIN="1")
    errors = []
    batches = []
    statuses = []
    with (output / "server.log").open("w", encoding="utf-8") as log:
        server = subprocess.Popen([sys.executable, "-m", "rag_catalog.ui.nice_app", "--host", "127.0.0.1",
                                   "--port", str(port), "--no-show"], env=env, stdout=log, stderr=log,
                                  creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        try:
            _wait_http(base + "/search")
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True, executable_path=_browser_executable(""))
                context = browser.new_context(viewport={"width": 1366, "height": 900})
                page = context.new_page()
                page.set_default_timeout(20000)
                page.on("pageerror", lambda error: errors.append(str(error)))
                page.on("request", lambda req: batches.append(req.post_data_json)
                        if req.url.endswith("/api/usage/events") else None)
                page.on("response", lambda res: statuses.append(res.status)
                        if res.url.endswith("/api/usage/events") else None)
                page.goto(base + "/search")
                page.get_by_label("Логин или email", exact=True).fill(fixture["username"])
                page.get_by_label("Пароль", exact=True).fill(fixture["password"])
                page.get_by_role("button", name="Войти", exact=True).click()
                page.get_by_role("button", name="Смены", exact=True).wait_for()
                page.locator('input[aria-label="Поиск по каталогу документов"]').fill("SECRET-ANALYTICS-PROBE")
                page.locator('[data-rag-usage="navigation.explorer"]').filter(visible=True).first.click()
                page.wait_for_url("**/explorer")
                page.locator('[data-rag-usage="navigation.shifts"]').filter(visible=True).first.click()
                page.wait_for_url("**/shifts")
                page.get_by_role("button", name="Новая смена", exact=True).wait_for()
                page.wait_for_timeout(6000)
                response = context.request.get(base + "/api/usage/report")
                assert response.status == 200, response.text()
                report = response.json()
                assert report["overview"]["clicks"] >= 2, report
                assert {r["username"] for r in report["recent"]} == {fixture["username"]}
                assert any(r["source"] == "browser" for r in report["timings"])
                assert "SECRET-ANALYTICS-PROBE" not in json.dumps(batches)
                assert statuses and all(s == 200 for s in statuses), statuses
                page.goto(base + "/stats")
                page.get_by_text("Сбор включён", exact=False).wait_for()
                page.get_by_text("Популярные действия", exact=True).wait_for()
                page.wait_for_timeout(1500)
                page.screenshot(path=str(output / "usage-desktop.png"), full_page=True)
                assert not _layout_probe(page)["horizontal_overflow"]
                with page.expect_download():
                    page.get_by_role("button", name="Скачать события CSV", exact=True).click()
                page.get_by_label("Период", exact=True).click()
                page.get_by_role("option", name="30 дней", exact=True).click()
                page.get_by_text("Популярные действия", exact=True).wait_for()
                page.set_viewport_size({"width": 480, "height": 900})
                page.wait_for_timeout(1600)
                page.screenshot(path=str(output / "usage-mobile.png"), full_page=True)
                assert not _layout_probe(page)["horizontal_overflow"]
                user_headers = {"Authorization": "Bearer " + fixture["user_token"]}
                assert context.request.get(base + "/api/usage/report", headers=user_headers).status == 403
                assert not errors, errors
                (output / "report.json").write_text(json.dumps({"ok": True, "batches": len(batches),
                    "events": report["overview"]["events"], "browser_errors": errors}, indent=2), encoding="utf-8")
                print(str(output / "report.json"))
                browser.close()
        finally:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=10)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
