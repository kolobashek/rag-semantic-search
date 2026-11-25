"""Authenticated NiceGUI search submission with a cold, isolated index."""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.pilot_ui_smoke import SRC, _browser_executable, _prepare_contour, _wait_http


def test_search_recovers_in_same_tab_after_index_becomes_available(tmp_path):
    playwright = pytest.importorskip('playwright.sync_api')
    from qdrant_client import QdrantClient, models

    try:
        executable = _browser_executable()
    except RuntimeError as exc:
        pytest.skip(str(exc))
    contour = _prepare_contour(tmp_path, qdrant_url='')
    registry = contour['cloud_drive_service'].registry
    card = registry.get_file_by_path('Документы/Карточка предприятия.txt')
    assert card is not None
    registry.queue_job(job_type='preview', status='failed', file_id=card.id)
    config = contour['config']
    config.update(qdrant_url='', qdrant_db_path=str(tmp_path / 'vectors'), embedding_collection_versioning=False)
    Path(contour['config_path']).write_text(json.dumps(config), encoding='utf-8')
    db = QdrantClient(path=config['qdrant_db_path'])
    try:
        db.create_collection(config['collection_name'], vectors_config=models.VectorParams(size=384, distance=models.Distance.COSINE))
        name = 'Договор поставки.txt'
        db.upsert(config['collection_name'], points=[models.PointStruct(
            id=1, vector=[1.0] + [0.0] * 383,
            payload={'filename': name, 'path': name, 'full_path': str(Path(config['catalog_path']) / name),
                     'type': 'text_chunk', 'extension': '.txt',
                     'text': 'Доставка груза выполняется по заявке покупателя. В заявке указываются адрес разгрузки, '
                             'дата поставки и ответственное лицо. Срок доставки стороны согласуют при оформлении заказа.'},
        )])
    finally:
        db.close()
    # Stub readiness and the query vector, not the index, ACL, search or rendering.
    server = '''
from pathlib import Path
import sys
from types import SimpleNamespace
import numpy as np
from rag_catalog.ui import helpers, login_stats, nice_app
import time
read_stats = login_stats._read_login_screen_stats
def gated_login_stats(cfg):
    Path('stats-started').touch()
    until = time.monotonic() + 10
    while not Path('stats-release').exists() and time.monotonic() < until:
        time.sleep(.03)
    return read_stats(cfg)
login_stats._read_login_screen_stats = gated_login_stats
helpers.RAGSearcher.embedder = property(lambda self: SimpleNamespace(encode=lambda *a, **kw: np.eye(1, 384)[0]))
helpers._qdrant_http_ready = nice_app._qdrant_http_ready = lambda cfg: Path('index-ready').exists()
helpers._warm_searcher_cache(nice_app.load_config())
nice_app.ui.run(host='127.0.0.1', port=int(sys.argv[1]), reload=False, show=False, storage_secret='isolated-test')
'''
    # Avoid Windows dynamic-port exclusions used by Hyper-V/WSL.
    import socket
    port = None
    for candidate in range(18800, 18900):
        with socket.socket() as probe:
            try:
                probe.bind(('127.0.0.1', candidate))
                port = candidate
                break
            except OSError:
                continue
    assert port is not None
    env = {**os.environ, 'PYTHONPATH': str(SRC), 'RAG_CONFIG_PATH': contour['config_path'],
           'HF_HUB_OFFLINE': '1', 'RAG_UI_SESSION_HTTPS_ONLY': '0', 'NICEGUI_STORAGE_PATH': str(tmp_path / 'nicegui'),
           'NICEGUI_SCREEN_TEST_PORT': str(port)}
    url = f'http://127.0.0.1:{port}'
    logfile = tmp_path / 'web.log'
    with logfile.open('wb') as log:
        proc = subprocess.Popen([sys.executable, '-c', server, str(port)], env=env, cwd=tmp_path,
                                stdout=log, stderr=subprocess.STDOUT, creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        try:
            _wait_http(url + '/api/ping', timeout_sec=60)
            with playwright.sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True, executable_path=executable)
                try:
                    page = browser.new_page()
                    page.set_default_timeout(30_000)
                    page.goto(url + '/search')
                    page.get_by_label('Логин или email', exact=True).fill(contour['username'])
                    import time
                    deadline = time.monotonic() + 5
                    while not (tmp_path / 'stats-started').exists() and time.monotonic() < deadline:
                        time.sleep(.03)
                    assert (tmp_path / 'stats-started').exists()
                    assert page.request.get(url + '/api/ping', timeout=1500).ok
                    assert all(value == '—' for value in page.locator('.rag-login-stat-num').all_text_contents())
                    (tmp_path / 'stats-release').touch()
                    page.get_by_label('Пароль', exact=True).fill(contour['password'])
                    page.get_by_role('button', name='Войти', exact=True).click()
                    page.get_by_role('button', name='Файлы', exact=True).click()
                    folder = page.locator('[data-rag-context="explorer-item"][data-rag-type="folder"]', has_text='Документы').first
                    folder.locator('[data-rag-open]').first.wait_for(state='attached')
                    # Double-click the non-interactive row, not its existing single-click button.
                    folder.screenshot(path=str(tmp_path / 'folder-before.png'))
                    folder.dblclick(position={'x': 600, 'y': 20})
                    page.get_by_role('button', name='Карточка предприятия.txt', exact=True).wait_for()
                    page.locator('.cd-status-badge', has_text='Ошибка preview').first.wait_for()
                    page.get_by_role('button', name='Поиск', exact=True).click()
                    box = page.locator('.rag-search-box input:not([type=checkbox]):visible').first
                    box.fill('Договор')
                    page.locator('[data-rag-usage="search.submit"]:visible').click()
                    page.get_by_text('Поисковый индекс недоступен.', exact=False).wait_for()
                    (tmp_path / 'index-ready').touch()
                    box.fill('Договор поставки')
                    box.press('Enter')
                    page.get_by_text('Договор поставки.txt', exact=False).first.wait_for()
                    page.get_by_text('точных совпадений: 1', exact=False).wait_for()
                    box.fill('доставка груза')
                    page.locator('[data-rag-usage="search.submit"]:visible').click()
                    page.get_by_text('после догрузки: 1', exact=False).wait_for()
                    page.get_by_text('Договор поставки.txt', exact=False).first.wait_for()
                    assert 'slot stack' not in page.locator('body').inner_text()
                    box.fill('Карточка предприятия')
                    box.press('Enter')
                    result = page.locator('.rag-result', has_text='Карточка предприятия.txt').first
                    result.locator('.cd-status-badge', has_text='Ошибка preview').wait_for()
                    result.get_by_role('button', name='В Cloud Drive', exact=True).click()
                    page.get_by_role('button', name='Карточка предприятия.txt', exact=True).wait_for()
                    page.screenshot(path=str(tmp_path / 'explorer-location.png'), full_page=True)
                finally:
                    browser.close()
        finally:
            proc.terminate()
            proc.wait(timeout=15)
    assert b'slot stack for this task is empty' not in logfile.read_bytes()
