from types import SimpleNamespace

from rag_catalog.core.indexing.heartbeat import write_heartbeat
from rag_catalog.core.indexing.monitor import IndexHeartbeatMonitor


def test_index_monitor_notifies_admin_once_across_restarts(tmp_path):
    cfg = {"qdrant_db_path": str(tmp_path)}
    auth = SimpleNamespace(list_users=lambda: [
        {"role": "admin", "status": "active", "telegram_chat_id": "1"},
        {"role": "user", "status": "active", "telegram_chat_id": "2"},
        {"role": "admin", "status": "blocked", "telegram_chat_id": "3"},
    ])
    sent = []
    write_heartbeat(tmp_path / "indexer_heartbeat.json", stage="large", processed=2, total=10,
                    run_id="test", status="failed")
    for _ in range(2):
        IndexHeartbeatMonitor(cfg, auth, lambda *args: sent.append(args)).poll()
    assert len(sent) == 1
    assert sent[0][0] == "1"


def test_index_monitor_does_not_mark_failed_delivery_as_sent(tmp_path):
    auth = SimpleNamespace(list_users=lambda: [{"role": "admin", "status": "active", "telegram_chat_id": "1"}])
    write_heartbeat(tmp_path / "indexer_heartbeat.json", stage="large", processed=1, total=2,
                    run_id="test", status="failed")
    def fail(*args):
        raise ConnectionError("offline")
    monitor = IndexHeartbeatMonitor({"qdrant_db_path": str(tmp_path)}, auth, fail)
    monitor.poll()
    assert not monitor.receipts
