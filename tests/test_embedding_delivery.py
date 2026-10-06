from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from vectorlake.archive import ArchiveWriter
from vectorlake.embedding import (
    EMBEDDING_DIMENSION,
    MODEL_NAME,
    MODEL_VERSION,
    PREPROCESS_VERSION,
    embedding_id,
)
from vectorlake.embedding_indexer import EmbeddingIndexer, PendingInferenceState
from vectorlake.events.v1 import events_pb2


def generated() -> events_pb2.EmbeddingGeneratedV1:
    image_id = "image-1"
    return events_pb2.EmbeddingGeneratedV1(
        context=events_pb2.EventContext(
            event_id="event-1",
            image_id=image_id,
            tenant_id="tenant-a",
            dedup_scope_id="scope-a",
            occurred_at_epoch_ms=1_700_000_000_000,
            schema_version=1,
            event_type="embedding.generated.v1",
        ),
        embedding_id=embedding_id(image_id, MODEL_VERSION, PREPROCESS_VERSION),
        duplicate_group_id="group-1",
        model_name=MODEL_NAME,
        model_version=MODEL_VERSION,
        preprocess_version=PREPROCESS_VERSION,
        dimension=EMBEDDING_DIMENSION,
        vector=[1.0] + [0.0] * (EMBEDDING_DIMENSION - 1),
    )


def test_archive_rejects_conflicts_before_storage() -> None:
    s3, producer = MagicMock(), MagicMock()
    writer = ArchiveWriter(s3=s3, producer=producer)
    first = generated()
    conflicting = generated()
    conflicting.duplicate_group_id = "group-2"
    with pytest.raises(ValueError, match="conflicting"):
        writer.write_batch([first, conflicting])
    s3.put_object.assert_not_called()
    producer.produce.assert_not_called()


def test_archive_does_not_publish_when_storage_fails() -> None:
    s3, producer = MagicMock(), MagicMock()
    s3.put_object.side_effect = RuntimeError("storage unavailable")
    writer = ArchiveWriter(s3=s3, producer=producer)
    with pytest.raises(RuntimeError, match="storage unavailable"):
        writer.write_batch([generated()])
    producer.produce.assert_not_called()


def test_indexer_waits_for_inference_marker() -> None:
    index, ddb, producer = MagicMock(), MagicMock(), MagicMock()
    ddb.get_item.return_value = {
        "Item": {
            "tenant_id": {"S": "tenant-a"},
            "dedup_scope_id": {"S": "scope-a"},
            "duplicate_group_id": {"S": "group-1"},
            "dedup_status": {"S": "unique"},
        }
    }
    service = EmbeddingIndexer(index, ddb=ddb, producer=producer)
    with pytest.raises(PendingInferenceState):
        service.process(generated())
    index.upsert.assert_not_called()
    producer.produce.assert_not_called()


def test_indexer_rejects_mismatched_identity() -> None:
    index, ddb, producer = MagicMock(), MagicMock(), MagicMock()
    ddb.get_item.return_value = {"Item": {"tenant_id": {"S": "other-tenant"}}}
    service = EmbeddingIndexer(index, ddb=ddb, producer=producer)
    with pytest.raises(ValueError, match="identity"):
        service.process(generated())
    index.upsert.assert_not_called()
