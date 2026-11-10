import asyncio
import io
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from rag_catalog.core import archive_preview
from rag_catalog.core.cloud_drive import CloudDriveService
from rag_catalog.core.index_state_db import IndexStateDB
from rag_catalog.core.rag_core import RAGSearcher
from rag_catalog.ui import api, helpers


@pytest.fixture
def cloud(tmp_path, monkeypatch):
    cfg = dict(cloud_drive_db_path=str(tmp_path / 'cloud.db'), cloud_drive_storage='local',
               cloud_drive_storage_root=str(tmp_path / 'storage'))
    svc = CloudDriveService.from_config(cfg)
    svc.registry.ensure_root_folder(root_name='root', source_path=str(tmp_path / 'catalog'))
    monkeypatch.setattr(api, 'load_config', lambda: cfg)
    monkeypatch.setattr(api, '_require_cloud_drive_api_user', lambda *a, **kw: {'role': 'admin'})
    return cfg, svc


def test_split_path_terms_and_cyrillic_case_are_searchable(cloud, tmp_path):
    _, svc = cloud
    svc.create_folder(parent_path='', name='ООО ТСК')
    source = tmp_path / 'charter.txt'
    source.write_text('test')
    svc.upload_file(parent_path='ООО ТСК', filename='УСТАВ.txt', source_path=str(source))
    for query in ['TSK Устав', 'ТСК устав', 'устав тск']:
        _, files = helpers._cd_search_by_name(svc.registry, query)
        assert [f.path for f in files] == ['ООО ТСК/УСТАВ.txt']


def test_quick_gate_keeps_all_terms_across_folder_and_name(tmp_path):
    folder = tmp_path / 'ООО ТСК'
    folder.mkdir()
    (folder / 'УСТАВ.pdf').touch()
    (folder / 'Договор.pdf').touch()
    searcher = object.__new__(RAGSearcher)
    searcher.config = {'catalog_path': str(tmp_path), 'retrieval_relevance_gate_enabled': True}
    searcher._fs_cache = {'ts': 0.0, 'items': []}
    found = helpers._run_quick_name_search(searcher, query='TSK Устав', limit=20, file_type=None)
    assert [r['filename'] for r in found] == ['УСТАВ.pdf']


def test_metadata_cache_can_read_index_when_mapped_drive_is_offline(tmp_path, monkeypatch):
    catalog = tmp_path / 'offline'
    state_path = tmp_path / 'index_state.db'
    state_path.touch()
    rows = [{'full_path': str(catalog / 'TSK' / 'docs.rar') + '::inner/УСТАВ.pdf', 'extension': '.pdf'}]
    monkeypatch.setattr(IndexStateDB, '__init__', lambda self, *a: None)
    monkeypatch.setattr(IndexStateDB, 'iter_search_entries', lambda self: rows)
    s = object.__new__(RAGSearcher)
    s.config = {'catalog_path': str(catalog), 'qdrant_db_path': str(tmp_path)}
    s._fs_cache = {'ts': 0., 'items': []}
    items = s._refresh_fs_cache()
    files = [item for item in items if item['kind'] == 'file']
    assert files[0]['filename'] == 'УСТАВ.pdf'
    assert files[0]['path'].endswith('docs.rar/inner/УСТАВ.pdf')
    assert all('::' not in item['path'] for item in items if item['kind'] == 'folder')


def test_tree_does_not_enumerate_child_files(cloud, monkeypatch):
    cfg, svc = cloud
    svc.create_folder(parent_path='', name='Shared')
    monkeypatch.setattr(svc.registry, 'list_files_in_folder', lambda *a: pytest.fail('tree loaded files'))
    folders, files = helpers._cd_list_children(svc, '', cfg=cfg, user={'role': 'admin'}, folders_only=True)
    assert [f.name for f in folders] == ['Shared']
    assert files == []


def test_archive_search_resolves_container_without_losing_member(cloud, tmp_path):
    cfg, svc = cloud
    src = str(tmp_path / 'catalog' / 'docs.rar')
    root = svc.registry.get_root_folder()
    node = svc.registry.upsert_file(folder_id=root.id, path='docs.rar', name='docs.rar',
                                   storage_key='docs.rar', mime_type='', size_bytes=2, checksum='x', source_path=src)
    item = {'full_path': src + '::folder/charter.pdf', 'archive_path': '', 'archive_member': ''}
    out = helpers._filter_cloud_drive_search_results(cfg, {'role': 'admin'}, [item], service=svc)
    assert out[0]['cloud_file_id'] == node.id
    assert out[0]['cloud_path'] == 'docs.rar'
    assert out[0]['archive_member'] == 'folder/charter.pdf'


@pytest.mark.parametrize('member', ['../secret', '/file', 'C:/file', '-so', 'folder/*', 'a//b', 'a\x00b'])
def test_archive_rejects_unsafe_members(member):
    with pytest.raises(ValueError):
        archive_preview.validate_member(member)


def test_archive_copy_has_hard_byte_limit():
    with pytest.raises(ValueError):
        archive_preview._copy_bounded(io.BytesIO(b'123456'), io.BytesIO(), 5)


def test_archive_preview_reads_exact_member_and_neutralizes_html(cloud, tmp_path):
    from rag_catalog.core.indexing.stage_runner import _command_archive_tool
    if not _command_archive_tool():
        pytest.skip('archive reader unavailable')
    _, svc = cloud
    source = tmp_path / 'docs.zip'
    with zipfile.ZipFile(source, 'w') as z:
        z.writestr('folder/Устав.html', '<script>bad()</script>')
        z.writestr('another.txt', 'not this member')
    svc.upload_file(parent_path='', filename='docs.zip', source_path=str(source))
    response = api.api_cloud_drive_preview('docs.zip', member='folder/Устав.html')
    try:
        assert Path(response.path).read_text() == '<script>bad()</script>'
        assert response.media_type.startswith('text/plain')
        assert 'sandbox' in response.headers['content-security-policy']
        assert response.filename == 'Устав.html'
    finally:
        asyncio.run(response.background())
    assert not Path(response.path).exists()


def test_archive_acl_denial_precedes_any_storage_read(cloud, monkeypatch):
    def deny(*a, **kw):
        raise HTTPException(403)
    monkeypatch.setattr(api, '_require_cloud_drive_path_access', deny)
    monkeypatch.setattr(archive_preview, 'prepare_member', lambda *a: pytest.fail('read before ACL'))
    for endpoint in (api.api_cloud_drive_preview, api.api_cloud_drive_download):
        with pytest.raises(HTTPException) as exc:
            endpoint('private.rar', member='doc.pdf')
        assert exc.value.status_code == 403


def test_rar_extraction_uses_stdout_and_literal_member(monkeypatch, tmp_path):
    from rag_catalog.core.indexing import stage_runner
    monkeypatch.setattr(stage_runner, '_command_archive_tool', lambda: ('7z', '7z.exe'))
    calls = []
    def popen(args, **kwargs):
        calls.append(args)
        return SimpleNamespace(stdout=io.BytesIO(b'%PDF-document'), wait=lambda **kw: 0,
                               poll=lambda: 0, kill=lambda: None)
    monkeypatch.setattr(archive_preview.subprocess, 'Popen', popen)
    archive_preview._extract(tmp_path / 'docs.rar', 'Устав.pdf', tmp_path / 'out')
    assert '-so' in calls[0] and '-spd' in calls[0]
    assert calls[0][-2:] == [str(tmp_path / 'docs.rar'), 'Устав.pdf']
