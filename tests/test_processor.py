from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from vectorlake.events.v1 import events_pb2
from vectorlake.near_copy import Fingerprint
from vectorlake.processor import ImageProcessor, ProcessingOutcome, RetryableProcessingError


def received() -> events_pb2.ImageReceivedV1:
    return events_pb2.ImageReceivedV1(
        context=events_pb2.EventContext(
            event_id="event-1",
            image_id="image-1",
            tenant_id="tenant-1",
            dedup_scope_id="scope-1",
            occurred_at_epoch_ms=1,
            schema_version=1,
            event_type="image.received.v1",
        ),
        bucket="vectorlake-raw-images",
        object_key="tenants/tenant-1/images/image-1/source",
        upload_intent_id="intent-1",
    )


def outcome() -> ProcessingOutcome:
    event = received()
    message = events_pb2.ProcessingFailedV1(
        context=events_pb2.EventContext(
            event_id="failure-1",
            image_id=event.context.image_id,
            tenant_id=event.context.tenant_id,
            dedup_scope_id=event.context.dedup_scope_id,
            occurred_at_epoch_ms=1,
            schema_version=1,
            event_type="processing.failed.v1",
        ),
        stage="validation",
        error_code="invalid_image",
    )
    return ProcessingOutcome("processing.failed.v1", message, "not_evaluated", "invalid")


@pytest.mark.parametrize("pending", [False, True])
def test_kafka_ack_controls_state_update(pending: bool) -> None:
    processor = object.__new__(ImageProcessor)
    processor.producer = Mock()
    processor.producer.flush.return_value = 1 if pending else 0
    with (
        patch.object(ImageProcessor, "evaluate", return_value=outcome()),
        patch.object(ImageProcessor, "_record_outcome") as record,
    ):
        if pending:
            with pytest.raises(RetryableProcessingError, match="Kafka delivery failed"):
                processor.process(received())
            record.assert_not_called()
        else:
            assert processor.process(received()).validation_status == "invalid"
            record.assert_called_once()


def test_event_scope_must_match_upload_intent() -> None:
    processor = object.__new__(ImageProcessor)
    processor.ddb = Mock()
    processor.ddb.get_item.return_value = {
        "Item": {
            "tenant_id": {"S": "tenant-1"},
            "dedup_scope_id": {"S": "other-scope"},
            "bucket": {"S": "vectorlake-raw-images"},
            "object_key": {"S": "tenants/tenant-1/images/image-1/source"},
            "upload_intent_id": {"S": "intent-1"},
        }
    }
    processor.s3 = Mock()
    with pytest.raises(ValueError, match="does not match upload intent"):
        processor.evaluate(received())
    processor.s3.get_object.assert_not_called()


def test_index_only_after_event_and_state_are_recorded() -> None:
    processor = object.__new__(ImageProcessor)
    processor.producer = Mock()
    processor.producer.flush.return_value = 0
    processor.index = Mock()
    event = received()
    ready = ProcessingOutcome(
        "image.inference-ready.v1",
        events_pb2.ImageInferenceReadyV1(),
        "unique",
        "valid",
        fingerprint=Fingerprint("0" * 64, 90),
    )
    with (
        patch.object(ImageProcessor, "evaluate", return_value=ready),
        patch.object(ImageProcessor, "_publish") as publish,
        patch.object(ImageProcessor, "_record_outcome") as record,
    ):
        processor.process(event)
    publish.assert_called_once()
    record.assert_called_once()
    processor.index.add.assert_called_once_with("scope-1", "image-1", Fingerprint("0" * 64, 90))
