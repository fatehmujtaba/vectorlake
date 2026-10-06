from __future__ import annotations

from unittest.mock import Mock

import pytest

from services.archive_writer.main import process_events
from vectorlake.embedding import MODEL_NAME, MODEL_VERSION, PREPROCESS_VERSION, embedding_id
from vectorlake.events.v1 import events_pb2


def event(image_id: str, timestamp: int) -> events_pb2.EmbeddingGeneratedV1:
    event = events_pb2.EmbeddingGeneratedV1(
        context=events_pb2.EventContext(
            event_id=f"event-{image_id}",
            image_id=image_id,
            tenant_id="tenant-1",
            dedup_scope_id="scope-1",
            occurred_at_epoch_ms=timestamp,
            schema_version=1,
            event_type="embedding.generated.v1",
        ),
        embedding_id=embedding_id(image_id, MODEL_VERSION, PREPROCESS_VERSION),
        duplicate_group_id="group-1",
        model_name=MODEL_NAME,
        model_version=MODEL_VERSION,
        preprocess_version=PREPROCESS_VERSION,
        dimension=512,
        vector=[1.0] + [0.0] * 511,
    )
    return event


@pytest.mark.parametrize("next_day", [False, True])
def test_archive_adapter_batches_and_partitions_by_utc_day(next_day: bool) -> None:
    first = event("image-1", 1_700_000_000_000)
    second = event("image-2", 1_700_000_000_000 + (86_400_000 if next_day else 0))
    writer = Mock()
    process_events(writer, [first, second])
    assert writer.write_batch.call_count == (2 if next_day else 1)
    if not next_day:
        writer.write_batch.assert_called_once_with([first, second])


def test_archive_adapter_propagates_failures_to_retry_executor() -> None:
    writer = Mock()
    writer.write_batch.side_effect = RuntimeError("storage unavailable")
    with pytest.raises(RuntimeError, match="storage unavailable"):
        process_events(writer, [event("image-1", 1_700_000_000_000)])
