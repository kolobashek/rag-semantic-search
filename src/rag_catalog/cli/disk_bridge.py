"""Run an explicit shared-folder disk bridge in the logged-in user's session."""
from __future__ import annotations

import argparse
import json
import logging
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

import psutil
from filelock import Timeout

from rag_catalog.core.cloud_drive.disk_bridge import DiskBridge
from rag_catalog.core.cloud_drive.service import CloudDriveService
from rag_catalog.core.rag_core import load_config


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True, type=Path)
    parser.add_argument('--apply', action='store_true', help='Enable version-preserving writes in both directions')
    parser.add_argument('--watch', action='store_true', help='Repeat until stopped, with one process lock')
    parser.add_argument('--max-files', type=int, default=50)
    args = parser.parse_args(argv)
    spec = json.loads(args.config.read_text(encoding='utf-8'))
    state = Path(spec['state_path'])
    state.parent.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger('rag.disk_bridge')
    log.setLevel(logging.INFO)
    handler = RotatingFileHandler(str(state) + '.log', maxBytes=2*1024*1024, backupCount=5, encoding='utf-8')
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    log.addHandler(handler)
    bridge = DiskBridge(CloudDriveService.from_config(load_config()), source_root=spec['source_root'],
                        mappings=spec['mappings'], state_path=str(state),
                        settle_seconds=spec.get('settle_seconds', 60))
    report_path = Path(str(state) + '.status.json')
    try:
        with bridge.lock:
            while True:
                try:
                    if psutil.virtual_memory().available < spec.get('min_free_memory_mb', 1536) * 1024 * 1024:
                        raise RuntimeError('Low available host memory; exchange deferred')
                    report = bridge.run(apply=args.apply, max_files=args.max_files)
                except Exception as exc:
                    report = {'finished_at': time.time(), 'complete': False, 'applied': args.apply,
                              'errors': [{'error': str(exc)[:1000]}]}
                temporary = report_path.with_suffix('.tmp')
                temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
                temporary.replace(report_path)
                summary = {k: v for k, v in report.items() if k != 'changes'}
                log.info('%s', json.dumps(summary, ensure_ascii=False))
                if not args.watch:
                    print(json.dumps(report, ensure_ascii=False))
                    return 0 if not report['errors'] else 1
                time.sleep(max(60, spec.get('interval_seconds', 180)))
    except Timeout:
        log.warning('Another bridge instance is already running')
        return 2
    finally:
        handler.close()
        log.removeHandler(handler)


if __name__ == '__main__':
    raise SystemExit(main())
