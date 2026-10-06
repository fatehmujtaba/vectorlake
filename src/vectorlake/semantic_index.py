from __future__ import annotations

import math
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from pymilvus import DataType, MilvusClient

DIMENSION = 512
COLLECTION = "visual_resnet18_v1"
_IDENTIFIER = re.compile(r"[A-Za-z0-9._:-]{1,128}\Z")


def _identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError("identifier must be 1-128 ASCII letters, digits, or ._:-")
    return value


def _vector(values: Sequence[float]) -> list[float]:
    if len(values) != DIMENSION:
        raise ValueError(f"expected {DIMENSION} dimensions")
    vector = [float(value) for value in values]
    if not all(math.isfinite(value) for value in vector):
        raise ValueError("vector must contain finite values")
    norm = math.sqrt(math.fsum(value * value for value in vector))
    if not 0.99 <= norm <= 1.01:
        raise ValueError("vector must be L2-normalized")
    return vector


@dataclass(frozen=True)
class SemanticRecord:
    embedding_id: str
    image_id: str
    tenant_id: str
    dedup_scope_id: str
    duplicate_group_id: str
    model_version: str
    preprocess_version: str
    vector: Sequence[float]


@dataclass(frozen=True)
class SearchHit:
    embedding_id: str
    image_id: str
    duplicate_group_id: str
    score: float


class SemanticStore(Protocol):
    collection: str

    def ensure_collection(self) -> None: ...

    def upsert(self, record: SemanticRecord) -> None: ...

    def search(
        self,
        vector: Sequence[float],
        *,
        tenant_id: str,
        dedup_scope_id: str,
        model_version: str,
        preprocess_version: str,
        top_k: int = 10,
    ) -> list[SearchHit]: ...


class SemanticIndex:
    """Milvus collection adapter for per-observation 512-D embeddings."""

    def __init__(self, uri: str, *, collection: str = COLLECTION) -> None:
        self.collection = _identifier(collection)
        self.client = MilvusClient(uri)

    def close(self) -> None:
        self.client.close()

    def ensure_collection(self) -> None:
        if self.client.has_collection(self.collection):
            return
        schema = MilvusClient.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("embedding_id", DataType.VARCHAR, is_primary=True, max_length=128)
        for name in (
            "image_id",
            "tenant_id",
            "dedup_scope_id",
            "duplicate_group_id",
            "model_version",
            "preprocess_version",
        ):
            schema.add_field(name, DataType.VARCHAR, max_length=128)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=DIMENSION)
        indexes = MilvusClient.prepare_index_params()
        indexes.add_index(field_name="vector", index_type="FLAT", metric_type="COSINE")
        self.client.create_collection(self.collection, schema=schema, index_params=indexes)

    def upsert(self, record: SemanticRecord) -> None:
        self.client.upsert(
            self.collection,
            {
                "embedding_id": _identifier(record.embedding_id),
                "image_id": _identifier(record.image_id),
                "tenant_id": _identifier(record.tenant_id),
                "dedup_scope_id": _identifier(record.dedup_scope_id),
                "duplicate_group_id": _identifier(record.duplicate_group_id),
                "model_version": _identifier(record.model_version),
                "preprocess_version": _identifier(record.preprocess_version),
                "vector": _vector(record.vector),
            },
        )

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
        if not 1 <= top_k <= 100:
            raise ValueError("top_k must be between 1 and 100")
        values = (
            _identifier(tenant_id),
            _identifier(dedup_scope_id),
            _identifier(model_version),
            _identifier(preprocess_version),
        )
        expression = " and ".join(
            f'{field} == "{value}"'
            for field, value in zip(
                ("tenant_id", "dedup_scope_id", "model_version", "preprocess_version"),
                values,
                strict=True,
            )
        )
        results = self.client.search(
            self.collection,
            data=[_vector(vector)],
            anns_field="vector",
            filter=expression,
            limit=top_k,
            output_fields=["image_id", "duplicate_group_id"],
            search_params={"metric_type": "COSINE"},
        )[0]
        return [
            SearchHit(
                embedding_id=str(hit["embedding_id"]),
                image_id=str(hit["entity"]["image_id"]),
                duplicate_group_id=str(hit["entity"]["duplicate_group_id"]),
                score=float(hit["distance"]),
            )
            for hit in results
        ]
