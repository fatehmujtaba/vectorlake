from __future__ import annotations

from collections.abc import Sequence

import httpx
from pydantic import ValidationError

from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.semantic_index import COLLECTION, SearchHit, SemanticRecord
from vectorlake.vector_contract import VectorQuery, VectorResults, VectorUpsert, VectorUpsertResult


class VectorUnavailable(Exception):
    pass


class VectorClient:
    collection = COLLECTION

    def __init__(
        self,
        settings: LocalInfrastructureSettings | None = None,
        *,
        client: httpx.Client | None = None,
    ) -> None:
        settings = settings or LocalInfrastructureSettings()
        self.client = client or httpx.Client(
            base_url=settings.vector_service_url,
            headers={"Authorization": f"Bearer {settings.vector_service_token}"},
            timeout=httpx.Timeout(30, connect=3),
            trust_env=False,
        )

    def close(self) -> None:
        self.client.close()

    def ensure_collection(self) -> None:
        try:
            self.client.get("/readyz").raise_for_status()
        except httpx.HTTPError as exc:
            raise VectorUnavailable("vector service is unavailable") from exc

    def upsert(self, record: SemanticRecord) -> None:
        request = VectorUpsert(
            embedding_id=record.embedding_id,
            image_id=record.image_id,
            tenant_id=record.tenant_id,
            dedup_scope_id=record.dedup_scope_id,
            duplicate_group_id=record.duplicate_group_id,
            model_version=record.model_version,
            preprocess_version=record.preprocess_version,
            vector=list(record.vector),
        )
        try:
            response = self.client.post("/internal/v1/embeddings/upsert", json=request.model_dump())
            response.raise_for_status()
            result = VectorUpsertResult.model_validate(response.json())
            if (
                result.embedding_id != record.embedding_id
                or result.collection_name != self.collection
            ):
                raise ValueError("vector upsert acknowledgement does not match request")
        except (httpx.HTTPError, ValidationError, ValueError) as exc:
            raise VectorUnavailable("vector upsert failed") from exc

    def search(
        self,
        vector: Sequence[float],
        *,
        tenant_id: str,
        dedup_scope_id: str,
        model_version: str,
        preprocess_version: str,
        top_k: int = 10,
    ) -> list[SearchHit]:
        request = VectorQuery(
            tenant_id=tenant_id,
            dedup_scope_id=dedup_scope_id,
            model_version=model_version,
            preprocess_version=preprocess_version,
            vector=list(vector),
            top_k=top_k,
        )
        try:
            response = self.client.post("/internal/v1/embeddings/search", json=request.model_dump())
            response.raise_for_status()
            result = VectorResults.model_validate(response.json())
            if (
                result.tenant_id != tenant_id
                or result.dedup_scope_id != dedup_scope_id
                or result.model_version != model_version
                or result.preprocess_version != preprocess_version
                or len(result.hits) > top_k
                or len({hit.embedding_id for hit in result.hits}) != len(result.hits)
            ):
                raise ValueError("vector response does not match the requested search contract")
            return [SearchHit(**hit.model_dump()) for hit in result.hits]
        except (httpx.HTTPError, ValidationError, ValueError) as exc:
            raise VectorUnavailable("vector search failed") from exc
