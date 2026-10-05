"""Round-trip a unique probe folder through SMB and the configured cloud storage."""
from __future__ import annotations

import argparse
import gc
import json
import shutil
import sqlite3
import tempfile
import time
import uuid
from pathlib import Path

from rag_catalog.core.cloud_drive.registry import CloudDriveRegistryDB
from rag_catalog.core.cloud_drive.service import CloudDriveService
from rag_catalog.core.cloud_drive.storage import resolve_storage_adapter


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True)
    parser.add_argument('--config', default='config.json')
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text(encoding='utf-8-sig'))
    source = Path(args.source).resolve(strict=True)
    probe = source / ('__rag_probe_' + uuid.uuid4().hex)
    probe.mkdir()
    storage = resolve_storage_adapter(config)
    keys = []
    with tempfile.TemporaryDirectory(prefix='rag-shared-smoke-') as temp:
        local = Path(temp)
        registry = CloudDriveRegistryDB(str(local / 'registry.db'))
        service = CloudDriveService(registry=registry, storage=storage, shared_folders=[
            {'path': 'Probe', 'source_path': str(probe), 'settle_seconds': 1}])
        service._ensure_folder_path('Probe')
        data = local / 'probe.txt'
        data.write_text('SMB round trip ' + uuid.uuid4().hex, encoding='utf-8')
        service.upload_file(parent_path='Probe', filename='probe.txt', source_path=str(data))
        assert (probe / 'probe.txt').read_bytes() == data.read_bytes()
        service.move_node(source_path='Probe/probe.txt', dest_parent_path='Probe', new_name='renamed.txt')
        service.delete_node('Probe/renamed.txt')
        assert not (probe / 'renamed.txt').exists()
        service.restore_node('Probe/renamed.txt')
        assert (probe / 'renamed.txt').read_bytes() == data.read_bytes()
        (probe / 'renamed.txt').write_text('SMB changed ' + uuid.uuid4().hex, encoding='utf-8')
        bridge = service.shared_folders
        bridge.scan(service, bridge.shares[0])
        time.sleep(1.1)
        assert bridge.scan(service, bridge.shares[0])['imported'] == 1
        node = registry.get_file_by_path('Probe/renamed.txt')
        downloaded = local / 'download.txt'
        storage.download_file(node.storage_key, downloaded)
        assert downloaded.read_bytes() == (probe / 'renamed.txt').read_bytes()
        with registry._connect() as conn:
            keys = [r[0] for r in conn.execute('SELECT DISTINCT storage_key FROM cloud_file_versions')]
        conn.close()
        gc.collect()
    # Only this unique probe tree is removed, never the supplied share root.
    if probe.resolve().parent != source or not probe.name.startswith('__rag_probe_'):
        raise RuntimeError('Unsafe probe cleanup path')
    shutil.rmtree(probe)
    with sqlite3.connect(config['cloud_drive_db_path']) as conn:
        for key in keys:
            referenced = conn.execute('SELECT 1 FROM cloud_files WHERE storage_key=? UNION ALL '
                                      'SELECT 1 FROM cloud_file_versions WHERE storage_key=? LIMIT 1', (key, key)).fetchone()
            if not referenced:
                storage.delete(key)
    print(json.dumps({'ok': True, 'source': str(source), 'checks': [
        'cloud_upload', 'smb_read', 'rename', 'trash', 'restore', 'smb_edit', 'cloud_download'],
        'probe_removed': True}, ensure_ascii=True))


if __name__ == '__main__':
    main()
