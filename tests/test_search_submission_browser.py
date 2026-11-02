"""Authenticated NiceGUI search submission with a cold, isolated index."""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.pilot_ui_smoke import SRC, _browser_executable, _free_port, _prepare_contour, _wait_http


def test_search_recovers_in_same_tab_after_index_becomes_available(tmp_path):
    playwright = pytest.importorskip('playwright.sync_api')
    from qdrant_client import QdrantClient, models

    try:
        executable = _browser_executable()
    except RuntimeError as exc:
        pytest.skip(str(exc))
    contour = _prepare_contour(tmp_path, qdrant_url='')
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
from rag_catalog.ui import helpers, nice_app
helpers.RAGSearcher.embedder = property(lambda self: SimpleNamespace(encode=lambda *a, **kw: np.eye(1, 384)[0]))
helpers._qdrant_http_ready = nice_app._qdrant_http_ready = lambda cfg: Path('index-ready').exists()
helpers._warm_searcher_cache(nice_app.load_config())
nice_app.ui.run(host='127.0.0.1', port=int(sys.argv[1]), reload=False, show=False, storage_secret='isolated-test')
'''
    port = _free_port()
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
                    page.get_by_label('Пароль', exact=True).fill(contour['password'])
                    page.get_by_role('button', name='Войти', exact=True).click()
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
                finally:
                    browser.close()
        finally:
            proc.terminate()
            proc.wait(timeout=15)
    assert b'slot stack for this task is empty' not in logfile.read_bytes()
