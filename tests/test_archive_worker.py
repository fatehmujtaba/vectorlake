from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from services.archive_writer.main import main
from vectorlake.embedding import MODEL_NAME, MODEL_VERSION, PREPROCESS_VERSION, embedding_id
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import serialize_event


def message(image_id: str, timestamp: int) -> Mock:
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
    result = Mock()
    result.error.return_value = None
    result.value.return_value = serialize_event("embedding.generated.v1", event)
    return result


@pytest.mark.parametrize("next_day", [False, True])
def test_archive_worker_batches_and_commits_after_all_writes(next_day: bool) -> None:
    first = message("image-1", 1_700_000_000_000)
    second = message("image-2", 1_700_000_000_000 + (86_400_000 if next_day else 0))
    consumer = Mock()
    consumer.poll.side_effect = [first, second, None, KeyboardInterrupt()]
    with (
        patch("services.archive_writer.main.Consumer", return_value=consumer),
        patch("services.archive_writer.main.ArchiveWriter") as writer_type,
    ):

        def write(events: list[events_pb2.EmbeddingGeneratedV1]) -> None:
            consumer.commit.assert_not_called()
            assert len(events) == (1 if next_day else 2)

        writer_type.return_value.write_batch.side_effect = write
        main()
    assert writer_type.return_value.write_batch.call_count == (2 if next_day else 1)
    assert consumer.commit.call_count == 2
    consumer.close.assert_called_once()


def test_archive_worker_leaves_offsets_uncommitted_on_write_failure() -> None:
    failed = Mock()
    failed.poll.side_effect = [message("image-1", 1_700_000_000_000), None]
    retry = Mock()
    retry.poll.side_effect = KeyboardInterrupt()
    with (
        patch("services.archive_writer.main.Consumer", side_effect=[failed, retry]),
        patch("services.archive_writer.main.ArchiveWriter") as writer_type,
        patch("services.archive_writer.main.time.sleep"),
    ):
        writer_type.return_value.write_batch.side_effect = RuntimeError("storage unavailable")
        main()
    failed.commit.assert_not_called()
    failed.close.assert_called_once()
