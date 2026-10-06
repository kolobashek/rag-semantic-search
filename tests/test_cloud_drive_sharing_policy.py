from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi import HTTPException

from rag_catalog.core.cloud_drive.service import CloudDriveService
from rag_catalog.ui import api, explorer_view


@pytest.fixture
def sharing(monkeypatch, tmp_path):
    cfg = {
        "cloud_drive_db_path": str(tmp_path / "cloud.db"),
        "cloud_drive_storage": "local",
        "cloud_drive_storage_root": str(tmp_path / "storage"),
        "cloud_drive_public_links_enabled": True,
    }
    service = CloudDriveService.from_config(cfg)
    home = service.registry.ensure_user_home_folder(username="owner")
    source = tmp_path / "sample.txt"
    source.write_text("sample", encoding="utf-8")
    service.upload_file(parent_path=home.path, filename="sample.txt", source_path=str(source))
    user = {"username": "employee", "role": "user", "status": "active"}
    monkeypatch.setattr(api, "load_config", lambda: dict(cfg))
    monkeypatch.setattr(api, "_require_cloud_drive_api_user", lambda *a, **kw: user)
    monkeypatch.setattr(api, "_audit_cloud_drive_api_event", lambda *a, **kw: None)
    return cfg, service, user


def test_role_editor_can_share_but_not_escalate_or_revoke_acl(sharing):
    _, service, _ = sharing
    service.grant_path_permission(subject_type="role", subject_id="user", path="owner", access_level="editor")
    permission = api.api_cloud_drive_permissions(
        subject_type="user", subject_id="recipient", path="owner/sample.txt", access_level="viewer")
    assert service.user_can_access(username="recipient", role="viewer", path="owner/sample.txt")
    assert permission in api.api_cloud_drive_permissions_list(path="owner/sample.txt")
    for level in ("admin", "owner"):
        with pytest.raises(HTTPException) as exc:
            api.api_cloud_drive_permissions(subject_type="user", subject_id="employee", path="owner", access_level=level)
        assert exc.value.status_code == 403
    with pytest.raises(HTTPException) as exc:
        api.api_cloud_drive_permission_revoke(permission_id=permission["id"], path="owner/sample.txt")
    assert exc.value.status_code == 403
    link = api.api_cloud_drive_share_link_create(SimpleNamespace(base_url="https://test/"), path="owner/sample.txt")
    assert api.api_cloud_drive_public_node(token=link["token"])["path"] == "owner/sample.txt"
    assert api.api_cloud_drive_share_links(path="owner/sample.txt")[0]["token"] == link["token"]
    assert api.api_cloud_drive_share_link_revoke(token=link["token"], path="owner/sample.txt")["ok"]


@pytest.mark.parametrize("level", ["viewer", None])
def test_viewer_or_outsider_cannot_grant_or_publish(sharing, level):
    _, service, _ = sharing
    if level:
        service.grant_path_permission(subject_type="user", subject_id="employee", path="owner", access_level=level)
    actions = [
        lambda: api.api_cloud_drive_permissions(subject_type="user", subject_id="recipient", path="owner/sample.txt"),
        lambda: api.api_cloud_drive_share_link_create(SimpleNamespace(base_url="https://test/"), path="owner/sample.txt"),
        lambda: api.api_cloud_drive_share_links(path="owner/sample.txt"),
    ]
    for action in actions:
        with pytest.raises(HTTPException) as exc:
            action()
        assert exc.value.status_code == 403


def test_editor_obeys_public_link_kill_switch_and_path_scope(sharing):
    cfg, service, _ = sharing
    service.grant_path_permission(subject_type="user", subject_id="employee", path="owner/sample.txt", access_level="editor")
    with pytest.raises(HTTPException) as exc:
        api.api_cloud_drive_share_link_create(SimpleNamespace(base_url="https://test/"), path="owner")
    assert exc.value.status_code == 403
    cfg["cloud_drive_public_links_enabled"] = False
    with pytest.raises(HTTPException) as exc:
        api.api_cloud_drive_share_link_create(SimpleNamespace(base_url="https://test/"), path="owner/sample.txt")
    assert exc.value.status_code == 403


def test_child_admin_cannot_revoke_parent_permission(sharing):
    _, service, _ = sharing
    inherited = service.grant_path_permission(subject_type="user", subject_id="recipient", path="owner", access_level="viewer")
    service.grant_path_permission(subject_type="user", subject_id="employee", path="owner/sample.txt", access_level="admin")
    with pytest.raises(HTTPException) as exc:
        api.api_cloud_drive_permission_revoke(permission_id=inherited["id"], path="owner/sample.txt")
    assert exc.value.status_code == 403
    assert service.user_can_access(username="recipient", role="viewer", path="owner/sample.txt")


def test_internal_share_links_do_not_create_grants_or_tokens(sharing):
    _, service, _ = sharing
    before = service.list_permissions()
    paths = ["owner", "owner/sample.txt"]
    links = explorer_view._cloud_drive_internal_share_links(service, paths, lambda p: True)
    assert [parse_qs(urlsplit(link).query)["kind"][0] for link in links] == ["folder", "file"]
    assert service.list_permissions() == before
    assert service.list_share_links() == []
    with pytest.raises(PermissionError):
        explorer_view._cloud_drive_internal_share_links(service, paths, lambda p: p == "owner")
