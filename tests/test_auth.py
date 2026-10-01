from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from fastapi.testclient import TestClient

from vectorlake.api import create_app
from vectorlake.auth import AuthenticationFailed, LocalTokenAuthenticator, Principal
from vectorlake.ingestion import UploadSession


def test_local_token_binds_tenant_and_source() -> None:
    auth = LocalTokenAuthenticator("secret", "camera-1")
    assert auth.authenticate("Bearer secret") == Principal("local-dev", frozenset({"camera-1"}))
    with pytest.raises(AuthenticationFailed):
        auth.authenticate("Bearer wrong")


def test_injected_principal_controls_source_and_tenant() -> None:
    auth = Mock()
    auth.authenticate.return_value = Principal("tenant-2", frozenset({"camera-2"}))
    service = Mock()
    service.create_session.return_value = UploadSession("image-1", "url", {}, 900, "pending_upload")
    service.get_status.return_value = {"image_id": "image-1", "status": "received"}
    headers = {"Idempotency-Key": "key", "Authorization": "Bearer opaque"}
    with TestClient(create_app(service, auth)) as client:
        allowed = client.post(
            "/v1/upload-sessions",
            headers=headers,
            json={
                "content_type": "image/png",
                "size_bytes": 10,
                "source_id": "camera-2",
                "capture_id": "frame-1",
            },
        )
        assert allowed.status_code == 201
        assert service.create_session.call_args.args[0] == "tenant-2"
        denied = client.post(
            "/v1/upload-sessions",
            headers=headers,
            json={
                "content_type": "image/png",
                "size_bytes": 10,
                "source_id": "camera-1",
                "capture_id": "frame-1",
            },
        )
        assert denied.status_code == 403
        assert client.get("/v1/images/image-1", headers=headers).status_code == 200
        service.get_status.assert_called_once_with("tenant-2", "image-1")


def test_production_default_fails_closed() -> None:
    with patch(
        "vectorlake.api.get_settings", return_value=SimpleNamespace(environment="production")
    ):
        with TestClient(create_app(Mock())) as client:
            response = client.get(
                "/v1/images/image-1", headers={"Authorization": "Bearer local-token"}
            )
    assert response.status_code == 503


def test_production_authenticator_does_not_enable_local_upload_backend() -> None:
    auth = Mock()
    auth.authenticate.return_value = Principal("tenant-1", frozenset({"camera-1"}))
    with patch(
        "vectorlake.api.get_settings", return_value=SimpleNamespace(environment="production")
    ):
        with TestClient(create_app(authenticator=auth)) as client:
            response = client.post(
                "/v1/upload-sessions",
                headers={"Idempotency-Key": "key", "Authorization": "Bearer opaque"},
                json={"content_type": "image/png", "size_bytes": 10},
            )
    assert response.status_code == 503
    assert response.json()["detail"] == "upload backend is not configured"
