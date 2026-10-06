from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from rag_catalog.ui import system


class StopCycle(Exception):
    pass


@pytest.mark.parametrize('enabled', [False, True, None])
def test_cloud_index_worker_pause_keeps_shared_worker(monkeypatch, enabled):
    cfg = {'cloud_drive_enabled': True}
    if enabled is not None:
        cfg['cloud_drive_index_worker_enabled'] = enabled
    targets = []
    monkeypatch.setattr(system, '_CLOUD_JOB_WORKER_STARTED', False)
    monkeypatch.setattr(system.threading, 'Thread',
                        lambda **kw: SimpleNamespace(start=lambda: targets.append(kw['target'])))
    monkeypatch.setattr('rag_catalog.core.rag_core.load_config', lambda: cfg)
    service = Mock()
    factory = Mock(return_value=service)
    monkeypatch.setattr(system, 'CloudDriveService', SimpleNamespace(from_config=factory))
    autosync = Mock()
    monkeypatch.setattr(system, '_cloud_autosync_tick', autosync)

    def sleep(_):
        raise StopCycle

    monkeypatch.setattr(system.time, 'sleep', sleep)
    system._start_cloud_drive_job_worker(cfg)
    assert len(targets) == 2
    with pytest.raises(StopCycle):
        targets[1]()
    if enabled is False:
        factory.assert_not_called()
        autosync.assert_not_called()
    else:
        service.run_pending_reindex_jobs.assert_called_once_with(index_config=cfg, limit=3)
        service.recover_stale_jobs.assert_called_once()
        autosync.assert_called_once_with(cfg, service)

    # The independent SMB polling loop remains enabled while indexing is paused.
    cfg['cloud_drive_shared_folders'] = [{'path': 'Scans'}]
    with pytest.raises(StopCycle):
        targets[0]()
    service.shared_folders.tick.assert_called_once_with(service)
