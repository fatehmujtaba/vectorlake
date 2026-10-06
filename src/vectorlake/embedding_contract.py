from __future__ import annotations

from dataclasses import dataclass

from vectorlake.embedding import (
    EMBEDDING_DIMENSION,
    MODEL_NAME,
    MODEL_VERSION,
    PREPROCESS_VERSION,
    embedding_id,
    validate_embedding,
)
from vectorlake.events.v1 import events_pb2


@dataclass(frozen=True)
class ValidatedEmbedding:
    embedding_id: str
    image_id: str
    tenant_id: str
    dedup_scope_id: str
    duplicate_group_id: str
    model_name: str
    model_version: str
    preprocess_version: str
    vector: tuple[float, ...]
    source_event_id: str
    occurred_at_epoch_ms: int
    traceparent: str


def validate_generated(event: events_pb2.EmbeddingGeneratedV1) -> ValidatedEmbedding:
    context = event.context
    if (
        not context.event_id
        or not context.image_id
        or not context.tenant_id
        or not context.dedup_scope_id
        or context.occurred_at_epoch_ms <= 0
        or context.schema_version != 1
        or context.event_type != "embedding.generated.v1"
        or not event.duplicate_group_id
    ):
        raise ValueError("embedding event context is incomplete")
    if (
        event.model_name != MODEL_NAME
        or event.model_version != MODEL_VERSION
        or event.preprocess_version != PREPROCESS_VERSION
        or event.dimension != EMBEDDING_DIMENSION
    ):
        raise ValueError("embedding model contract does not match active version")
    if event.embedding_id != embedding_id(
        context.image_id, event.model_version, event.preprocess_version
    ):
        raise ValueError("embedding ID does not match image and model versions")
    if event.vector_ref:
        raise ValueError("referenced vectors are not supported by this archive version")
    vector = list(event.vector)
    validate_embedding(vector)
    return ValidatedEmbedding(
        embedding_id=event.embedding_id,
        image_id=context.image_id,
        tenant_id=context.tenant_id,
        dedup_scope_id=context.dedup_scope_id,
        duplicate_group_id=event.duplicate_group_id,
        model_name=event.model_name,
        model_version=event.model_version,
        preprocess_version=event.preprocess_version,
        vector=tuple(vector),
        source_event_id=context.event_id,
        occurred_at_epoch_ms=context.occurred_at_epoch_ms,
        traceparent=context.traceparent,
    )
