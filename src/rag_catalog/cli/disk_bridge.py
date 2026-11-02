"""Run an explicit shared-folder disk bridge in the logged-in user's session."""
from __future__ import annotations

import argparse
import json
import logging
import ntpath
import time
from collections import Counter
from logging.handlers import RotatingFileHandler
from pathlib import Path

import psutil
from filelock import Timeout

from rag_catalog.core.cloud_drive.disk_bridge import DiskBridge
from rag_catalog.core.cloud_drive.service import CloudDriveService
from rag_catalog.core.rag_core import load_config

_MIB = 1024 * 1024
_TOP = 10


def _process_memory_snapshot():
    rss_by_name = Counter()
    instances = Counter()
    rows = []
    unreadable = 0
    for proc in psutil.process_iter():
        try:
            info = proc.as_dict(attrs=['name', 'memory_info'], ad_value=None)
        except (psutil.Error, OSError):
            unreadable += 1
            continue
        mem = info['memory_info']
        if mem is None:
            unreadable += 1
            continue
        name = ntpath.basename(info['name'] or '')[:96] or '<unknown>'
        rss_by_name[name.casefold()] += mem.rss
        instances[name.casefold()] += 1
        rows.append((mem.rss, proc.pid, name, getattr(mem, 'private', None)))
    top = sorted(rows, key=lambda r: (-r[0], r[1]))[:_TOP]
    return {
        'sampled_processes': len(rows), 'unreadable_processes': unreadable,
        'top_processes': [
            {'pid': pid, 'name': name, 'rss_mib': round(rss / _MIB, 1),
             'private_commit_mib': round(private / _MIB, 1) if private is not None else None}
            for rss, pid, name, private in top
        ],
        'top_groups': [
            {'name': name, 'processes': instances[name], 'rss_sum_mib': round(rss / _MIB, 1)}
            for name, rss in rss_by_name.most_common(_TOP)
        ],
    }


def _memory_deferral(memory, threshold_mib, apply):
    # Preserve the exact sample that tripped the guard, not a later free-RAM reading.
    pressure = {'sampled_at': time.time(), 'available_bytes': memory.available,
                'available_mib': memory.available // _MIB, 'total_mib': memory.total // _MIB,
                'threshold_mib': threshold_mib}
    try:
        pressure.update(_process_memory_snapshot())
    except Exception as exc:
        # Diagnostics are best effort. A collection error must never allow the exchange.
        pressure['process_snapshot_error'] = type(exc).__name__
    return {'finished_at': time.time(), 'complete': False, 'applied': apply,
            'errors': [{'error': 'Low available host memory; exchange deferred'}],
            'memory_pressure': pressure}


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
                    threshold_mib = spec.get('min_free_memory_mb', 1536)
                    memory = psutil.virtual_memory()
                    if memory.available < threshold_mib * _MIB:
                        report = _memory_deferral(memory, threshold_mib, args.apply)
                    else:
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
