from __future__ import annotations

import json
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import psutil
import pytest

from rag_catalog.cli import disk_bridge as cli

MIB = 1024 * 1024


def process(pid, name, rss, private=None, denied=False):
    def fields(*, attrs, ad_value):
        assert set(attrs) == {'name', 'memory_info'} and ad_value is None
        if denied:
            raise psutil.AccessDenied(pid)
        mem = SimpleNamespace(rss=rss * MIB) if rss is not None else None
        if private is not None:
            mem.private = private * MIB
        return {'name': name, 'memory_info': mem}

    return SimpleNamespace(pid=pid, as_dict=fields)


def test_process_snapshot_ranks_residents_and_counts_small_group_members(monkeypatch):
    workers = [process(n, 'Browser.exe' if n % 2 else 'browser.exe', 70) for n in range(1, 13)]
    others = [process(n, f'tool{n}', n) for n in range(80, 92)]
    processes = [process(501, r'C:\private-path\native.exe', 300, 900),
                 process(502, None, None), process(503, 'restricted', None, denied=True), *workers, *others]
    monkeypatch.setattr(cli.psutil, 'process_iter', lambda: iter(processes))
    snapshot = cli._process_memory_snapshot()
    assert snapshot['sampled_processes'] == 25
    assert snapshot['unreadable_processes'] == 2
    assert len(snapshot['top_processes']) == len(snapshot['top_groups']) == 10
    assert snapshot['top_processes'][0] == {'pid': 501, 'name': 'native.exe', 'rss_mib': 300, 'private_commit_mib': 900}
    assert snapshot['top_processes'][1]['private_commit_mib'] is None
    assert snapshot['top_groups'][0] == {'name': 'browser.exe', 'processes': 12, 'rss_sum_mib': 840}
    assert not any(p['name'].lower() == 'browser.exe' for p in snapshot['top_processes'])
    assert 'private-path' not in json.dumps(snapshot)


@pytest.fixture
def bridge_cli(tmp_path, monkeypatch):
    state = tmp_path / 'state.db'
    config = tmp_path / 'config.json'
    config.write_text(json.dumps({'state_path': str(state), 'source_root': 'not-mounted',
                                  'mappings': [], 'min_free_memory_mb': 2048}), encoding='utf-8')
    calls = []

    def run(*, apply, max_files):
        calls.append((apply, max_files))
        return {'errors': [], 'complete': True, 'changes': []}

    monkeypatch.setattr(cli, 'load_config', lambda: {})
    monkeypatch.setattr(cli.CloudDriveService, 'from_config', lambda cfg: object())
    monkeypatch.setattr(cli, 'DiskBridge', lambda *a, **kw: SimpleNamespace(lock=nullcontext(), run=run))
    return ['--config', str(config), '--apply'], state, calls


def logged_reports(state):
    return [json.loads(line[line.index('{'):]) for line in
            state.with_suffix('.db.log').read_text(encoding='utf-8').splitlines()]


def test_each_deferral_is_logged_then_exchange_resumes_at_threshold(bridge_cli, monkeypatch):
    args, state, calls = bridge_cli
    readings = iter((2048 * MIB - 1, 1000 * MIB, 2048 * MIB))
    monkeypatch.setattr(cli.psutil, 'virtual_memory', lambda: SimpleNamespace(available=next(readings), total=32 * 1024 * MIB))
    pids = iter((101, 202))
    monkeypatch.setattr(cli.psutil, 'process_iter', lambda: iter([process(next(pids), 'worker', 800)]))
    for _ in range(2):
        assert cli.main(args) == 1
        assert not calls
        last = logged_reports(state)[-1]
        assert json.loads(state.with_suffix('.db.status.json').read_text(encoding='utf-8')) == last
        assert not last['complete'] and last['errors'] == [{'error': 'Low available host memory; exchange deferred'}]
    assert cli.main(args) == 0
    assert calls == [(True, 50)]
    first, second, good = logged_reports(state)
    a, b = first['memory_pressure'], second['memory_pressure']
    assert a['available_bytes'] == 2048 * MIB - 1
    assert (a['available_mib'], a['threshold_mib'], a['total_mib']) == (2047, 2048, 32768)
    assert 0 < a['sampled_at'] <= first['finished_at']
    assert b['available_mib'] == 1000
    assert [s['top_processes'][0]['pid'] for s in (a, b)] == [101, 202]
    assert 'memory_pressure' not in good


def test_collector_failure_keeps_original_deferral_and_no_sensitive_message(bridge_cli, monkeypatch):
    args, state, calls = bridge_cli
    monkeypatch.setattr(cli.psutil, 'virtual_memory', lambda: SimpleNamespace(available=MIB, total=32 * 1024 * MIB))

    def unavailable():
        raise OSError('private path or command line')

    monkeypatch.setattr(cli.psutil, 'process_iter', unavailable)
    assert cli.main(args) == 1
    assert not calls
    (report,) = logged_reports(state)
    assert report['errors'] == [{'error': 'Low available host memory; exchange deferred'}]
    assert report['memory_pressure']['process_snapshot_error'] == 'OSError'
    assert report['memory_pressure']['available_mib'] == 1
    assert 'private path' not in json.dumps(report)


def test_real_process_snapshot_in_isolated_guard_log(bridge_cli):
    args, state, calls = bridge_cli
    config = Path(args[1])
    spec = json.loads(config.read_text(encoding='utf-8'))
    spec['min_free_memory_mb'] = psutil.virtual_memory().total // MIB + 1
    config.write_text(json.dumps(spec), encoding='utf-8')
    assert cli.main(args) == 1
    assert not calls
    (report,) = logged_reports(state)
    pressure = report['memory_pressure']
    assert pressure['sampled_processes'] >= 1
    assert pressure['available_mib'] < pressure['threshold_mib']
    assert 1 <= len(pressure['top_processes']) <= 10
    assert all(p['name'] and p['pid'] >= 0 and p['rss_mib'] >= 0 for p in pressure['top_processes'])
    assert pressure['top_groups'][0]['rss_sum_mib'] >= pressure['top_processes'][0]['rss_mib']
