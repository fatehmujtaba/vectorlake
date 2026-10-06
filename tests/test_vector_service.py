from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from unittest.mock import Mock

import httpx
import pytest
from fastapi.testclient import TestClient

from vectorlake.embedding import MODEL_VERSION, PREPROCESS_VERSION, embedding_id
from vectorlake.semantic_index import SemanticIndex, SemanticRecord
from vectorlake.vector_api import create_vector_app
from vectorlake.vector_client import VectorClient, VectorUnavailable

HEADERS = {"Authorization": "Bearer vectorlake-local-vector-token"}


def record(image_id: str, tenant: str = "tenant-a", scope: str = "scope-a") -> SemanticRecord:
    return SemanticRecord(
        embedding_id(image_id, MODEL_VERSION, PREPROCESS_VERSION),
        image_id,
        tenant,
        scope,
        "group-1",
        MODEL_VERSION,
        PREPROCESS_VERSION,
        [1.0] + [0.0] * 511,
    )


def test_vector_service_scopes_queries_and_replays_upserts(tmp_path: Path) -> None:
    index = SemanticIndex(str(tmp_path / "vectors.db"))
    try:
        with TestClient(create_vector_app(index)) as client:
            first = record("image-1")
            for row in [
                first,
                first,
                record("image-2", "tenant-b"),
                record("image-3", scope="scope-b"),
            ]:
                assert (
                    client.post(
                        "/internal/v1/embeddings/upsert", json=asdict(row), headers=HEADERS
                    ).status_code
                    == 200
                )
            query = {
                "tenant_id": "tenant-a",
                "dedup_scope_id": "scope-a",
                "model_version": MODEL_VERSION,
                "preprocess_version": PREPROCESS_VERSION,
                "vector": list(first.vector),
                "top_k": 5,
            }
            response = client.post("/internal/v1/embeddings/search", json=query, headers=HEADERS)
            assert response.status_code == 200
            assert [hit["image_id"] for hit in response.json()["hits"]] == ["image-1"]
            assert client.post("/internal/v1/embeddings/search", json=query).status_code == 401
            assert (
                client.post(
                    "/internal/v1/embeddings/search",
                    json=query | {"model_version": "wrong-model"},
                    headers=HEADERS,
                ).status_code
                == 422
            )
            assert (
                client.post(
                    "/internal/v1/embeddings/upsert",
                    json=asdict(first) | {"embedding_id": "wrong-id"},
                    headers=HEADERS,
                ).status_code
                == 422
            )
    finally:
        index.close()


def test_vector_client_fails_closed_on_mismatched_scope_response() -> None:
    def respond(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "tenant_id": "other-tenant",
                "dedup_scope_id": "scope-a",
                "model_version": MODEL_VERSION,
                "preprocess_version": PREPROCESS_VERSION,
                "hits": [],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(respond), base_url="http://vector") as client:
        vector = VectorClient(client=client)
        with pytest.raises(VectorUnavailable):
            vector.search(
                [1.0] + [0.0] * 511,
                tenant_id="tenant-a",
                dedup_scope_id="scope-a",
                model_version=MODEL_VERSION,
                preprocess_version=PREPROCESS_VERSION,
            )


def test_index_outage_returns_retryable_status() -> None:
    index = Mock()
    index.search.side_effect = RuntimeError("index unavailable")
    with TestClient(create_vector_app(index)) as client:
        response = client.post(
            "/internal/v1/embeddings/search",
            json={
                "tenant_id": "tenant-a",
                "dedup_scope_id": "scope-a",
                "model_version": MODEL_VERSION,
                "preprocess_version": PREPROCESS_VERSION,
                "vector": [1.0] + [0.0] * 511,
            },
            headers=HEADERS,
        )
    assert response.status_code == 503
