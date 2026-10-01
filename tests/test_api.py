from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from vectorlake.api import create_app
from vectorlake.ingestion import UploadSession


def test_health_and_readiness() -> None:
    with TestClient(create_app()) as client:
        assert client.get("/healthz").json() == {"status": "ok"}
        assert client.get("/readyz").json() == {"status": "ready"}


def test_upload_capture_id_is_forwarded_and_validated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VECTORLAKE_ENVIRONMENT", "local")
    service = Mock()
    service.create_session.return_value = UploadSession("image-1", "url", {}, 900, "pending_upload")
    headers = {"Authorization": "Bearer vectorlake-local-dev-token", "Idempotency-Key": "request-1"}
    with TestClient(create_app(service)) as client:
        response = client.post(
            "/v1/upload-sessions",
            headers=headers,
            json={
                "content_type": "image/png",
                "size_bytes": 123,
                "source_id": "camera-1",
                "capture_id": "frame-2",
            },
        )
        assert response.status_code == 201
        assert service.create_session.call_args.args[-2:] == ("frame-2", "camera-1")
        invalid = client.post(
            "/v1/upload-sessions",
            headers=headers,
            json={"content_type": "image/png", "size_bytes": 123, "capture_id": "bad/path"},
        )
        assert invalid.status_code == 422
        missing_source = client.post(
            "/v1/upload-sessions",
            headers=headers,
            json={"content_type": "image/png", "size_bytes": 123, "capture_id": "frame-2"},
        )
        assert missing_source.status_code == 422
        assert service.create_session.call_count == 1
        forbidden = client.post(
            "/v1/upload-sessions",
            headers=headers,
            json={
                "content_type": "image/png",
                "size_bytes": 123,
                "source_id": "camera-2",
                "capture_id": "frame-2",
            },
        )
        assert forbidden.status_code == 403
        assert service.create_session.call_count == 1


def test_local_source_binding_is_configurable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VECTORLAKE_LOCAL_API_SOURCE_ID", "camera-2")
    service = Mock()
    service.create_session.return_value = UploadSession("image-1", "url", {}, 900, "pending_upload")
    headers = {"Authorization": "Bearer vectorlake-local-dev-token", "Idempotency-Key": "key"}
    with TestClient(create_app(service)) as client:
        allowed = client.post(
            "/v1/upload-sessions",
            headers=headers,
            json={
                "content_type": "image/png",
                "size_bytes": 123,
                "source_id": "camera-2",
                "capture_id": "frame-1",
            },
        )
        assert allowed.status_code == 201
        denied = client.post(
            "/v1/upload-sessions",
            headers=headers,
            json={
                "content_type": "image/png",
                "size_bytes": 123,
                "source_id": "camera-1",
                "capture_id": "frame-1",
            },
        )
        assert denied.status_code == 403
