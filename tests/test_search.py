from __future__ import annotations

from io import BytesIO
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from fastapi.testclient import TestClient
from PIL import Image

from vectorlake.api import create_app
from vectorlake.auth import Principal
from vectorlake.embedding import EMBEDDING_DIMENSION, MODEL_NAME, MODEL_VERSION, PREPROCESS_VERSION
from vectorlake.ingestion import UploadSession
from vectorlake.search import ImageSearchService
from vectorlake.semantic_index import SearchHit
from vectorlake.validation import MAX_OBJECT_BYTES
from vectorlake.vector_client import VectorUnavailable


def png() -> bytes:
    output = BytesIO()
    with Image.new("RGB", (32, 32), "green") as image:
        image.save(output, format="PNG")
    return output.getvalue()


def service() -> tuple[ImageSearchService, Mock, Mock, Mock]:
    index = Mock()
    index.search.return_value = [SearchHit("embedding-1", "image-1", "group-1", 0.9)]
    scope = Mock(return_value="registered-scope")
    engine = Mock(
        model_name=MODEL_NAME,
        model_version=MODEL_VERSION,
        preprocess_version=PREPROCESS_VERSION,
        dimension=EMBEDDING_DIMENSION,
    )
    engine.embed_images.return_value = [[1.0] + [0.0] * 511]
    return ImageSearchService(index, scope, engine=engine), index, scope, engine


def test_search_binds_principal_scope_and_model_contract() -> None:
    search, index, scope, _ = service()
    auth = Mock()
    auth.authenticate.return_value = Principal("tenant-2", frozenset({"camera-2"}))
    uploads = Mock()
    with TestClient(create_app(uploads, auth, search)) as client:
        response = client.post(
            "/v1/images/search?top_k=3",
            content=png(),
            headers={"Authorization": "Bearer opaque", "Content-Type": "image/png"},
        )
    assert response.status_code == 200
    assert response.json()["hits"][0]["image_id"] == "image-1"
    assert response.json()["model_version"] == MODEL_VERSION
    scope.assert_called_once_with("tenant-2")
    assert index.search.call_args.kwargs == {
        "tenant_id": "tenant-2",
        "dedup_scope_id": "registered-scope",
        "model_version": MODEL_VERSION,
        "preprocess_version": PREPROCESS_VERSION,
        "top_k": 3,
    }
    uploads.create_session.assert_not_called()


def test_search_authentication_precedes_decode_and_inference() -> None:
    search, index, scope, engine = service()
    with TestClient(create_app(search_service=search)) as client:
        response = client.post(
            "/v1/images/search", content=b"not an image", headers={"Content-Type": "image/png"}
        )
    assert response.status_code == 401
    index.search.assert_not_called()
    scope.assert_not_called()
    engine.embed_images.assert_not_called()


@pytest.mark.parametrize(
    ("payload", "content_type", "expected"),
    [(b"broken", "image/png", 422), (png(), "image/jpeg", 422), (png(), "text/plain", 415)],
)
def test_invalid_search_images_never_reach_model(
    payload: bytes, content_type: str, expected: int
) -> None:
    search, index, _, engine = service()
    with TestClient(create_app(search_service=search)) as client:
        response = client.post(
            "/v1/images/search",
            content=payload,
            headers={
                "Authorization": "Bearer vectorlake-local-dev-token",
                "Content-Type": content_type,
            },
        )
    assert response.status_code == expected
    engine.embed_images.assert_not_called()
    index.search.assert_not_called()


def test_search_limits_size_top_k_and_empty_body() -> None:
    search, _, _, engine = service()
    headers = {"Authorization": "Bearer vectorlake-local-dev-token", "Content-Type": "image/png"}
    with TestClient(create_app(search_service=search)) as client:
        assert (
            client.post("/v1/images/search?top_k=101", content=png(), headers=headers).status_code
            == 422
        )
        assert client.post("/v1/images/search", content=b"", headers=headers).status_code == 422
        too_large = client.post(
            "/v1/images/search",
            content=png(),
            headers=headers | {"Content-Length": str(MAX_OBJECT_BYTES + 1)},
        )
        assert too_large.status_code == 413
    engine.embed_images.assert_not_called()


def test_search_outage_does_not_disable_uploads() -> None:
    search, index, _, _ = service()
    index.ensure_collection.side_effect = VectorUnavailable()
    uploads = Mock()
    uploads.get_status.return_value = {"image_id": "image-1", "status": "received"}
    uploads.create_session.return_value = UploadSession("image-2", "url", {}, 900, "pending_upload")
    headers = {"Authorization": "Bearer vectorlake-local-dev-token", "Content-Type": "image/png"}
    with TestClient(create_app(uploads, search_service=search)) as client:
        assert client.post("/v1/images/search", content=png(), headers=headers).status_code == 503
        assert client.get("/v1/images/image-1", headers=headers).status_code == 200
        assert (
            client.post(
                "/v1/upload-sessions",
                json={"content_type": "image/png", "size_bytes": 123},
                headers=headers | {"Content-Type": "application/json", "Idempotency-Key": "key-1"},
            ).status_code
            == 201
        )


def test_chunked_search_body_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("vectorlake.api.MAX_OBJECT_BYTES", 1000)
    search, index, _, engine = service()
    with TestClient(create_app(search_service=search)) as client:
        response = client.post(
            "/v1/images/search",
            content=iter([png(), b"x" * 1000]),
            headers={
                "Authorization": "Bearer vectorlake-local-dev-token",
                "Content-Type": "image/png",
            },
        )
    assert response.status_code == 413
    index.search.assert_not_called()
    engine.embed_images.assert_not_called()


def test_model_mismatch_and_search_busy() -> None:
    search, index, _, engine = service()
    engine.preprocess_version = "incompatible"
    headers = {"Authorization": "Bearer vectorlake-local-dev-token", "Content-Type": "image/png"}
    with TestClient(create_app(search_service=search)) as client:
        assert client.post("/v1/images/search", content=png(), headers=headers).status_code == 503
        search.lock.acquire()
        try:
            response = client.post("/v1/images/search", content=png(), headers=headers)
            assert response.status_code == 429
            assert response.headers["Retry-After"] == "1"
        finally:
            search.lock.release()
    engine.embed_images.assert_not_called()
    index.search.assert_not_called()


def test_production_search_backend_fails_closed() -> None:
    auth = Mock()
    auth.authenticate.return_value = Principal("tenant-1", frozenset())
    with (
        patch(
            "vectorlake.api.get_settings", return_value=SimpleNamespace(environment="production")
        ),
        TestClient(create_app(authenticator=auth)) as client,
    ):
        response = client.post(
            "/v1/images/search", content=png(), headers={"Content-Type": "image/png"}
        )
    assert response.status_code == 503
    assert response.json()["detail"] == "search backend is not configured"
