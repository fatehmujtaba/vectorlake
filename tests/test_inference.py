from __future__ import annotations

from io import BytesIO
from unittest.mock import Mock

import pytest
from PIL import Image

from vectorlake.common.resources import RAW_BUCKET
from vectorlake.embedding import EMBEDDING_DIMENSION, embedding_id
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import deserialize_event
from vectorlake.inference import InferenceService


class FakeEngine:
    model_name = "resnet18"
    model_version = "model-v1"
    preprocess_version = "preprocess-v1"
    dimension = EMBEDDING_DIMENSION

    def embed_images(self, images: list[Image.Image]) -> list[list[float]]:
        vectors: list[list[float]] = []
        for _image in images:
            vectors.append([1.0] + [0.0] * (EMBEDDING_DIMENSION - 1))
        return vectors


def ready(image_id: str = "image-1") -> events_pb2.ImageInferenceReadyV1:
    return events_pb2.ImageInferenceReadyV1(
        context=events_pb2.EventContext(
            event_id=f"ready-{image_id}",
            image_id=image_id,
            tenant_id="tenant-1",
            dedup_scope_id="scope-1",
            occurred_at_epoch_ms=123,
            schema_version=1,
            event_type="image.inference-ready.v1",
        ),
        duplicate_group_id="group-1",
        bucket=RAW_BUCKET,
        object_key=f"tenants/tenant-1/images/{image_id}/source",
        model_route="resnet18-v1",
    )


def _png() -> bytes:
    output = BytesIO()
    Image.new("RGB", (8, 8), "red").save(output, format="PNG")
    return output.getvalue()


def test_inference_publishes_before_state_and_skips_completed_replay() -> None:
    data = _png()
    event = ready()
    state = {
        "tenant_id": {"S": "tenant-1"},
        "dedup_scope_id": {"S": "scope-1"},
        "bucket": {"S": RAW_BUCKET},
        "object_key": {"S": event.object_key},
        "duplicate_group_id": {"S": "group-1"},
        "dedup_status": {"S": "unique"},
        "expected_size_bytes": {"N": str(len(data))},
        "content_type": {"S": "image/png"},
    }
    ddb = Mock()
    ddb.get_item.side_effect = lambda **_kwargs: {"Item": state}
    s3 = Mock()
    s3.get_object.side_effect = lambda **_kwargs: {"Body": BytesIO(data)}
    producer = Mock()
    producer.flush.return_value = 0

    def record(**kwargs: object) -> None:
        assert producer.produce.call_count == 1
        state["embedding_generated_id"] = kwargs["ExpressionAttributeValues"][  # type: ignore[index]
            ":embedding"
        ]

    ddb.update_item.side_effect = record
    service = InferenceService(FakeEngine(), s3=s3, ddb=ddb, producer=producer)
    generated = service.process_batch([event, event])
    assert len(generated) == 1
    assert generated[0].embedding_id == embedding_id("image-1", "model-v1", "preprocess-v1")
    assert generated[0].dimension == EMBEDDING_DIMENSION
    assert generated[0].context.occurred_at_epoch_ms == event.context.occurred_at_epoch_ms
    payload = producer.produce.call_args.kwargs["value"]
    assert deserialize_event("embedding.generated.v1", payload) == generated[0]
    assert service.process_batch([event]) == []
    producer.produce.assert_called_once()
    s3.get_object.assert_called_once()


def test_inference_rejects_state_or_batch_identity_conflicts() -> None:
    ddb = Mock()
    ddb.get_item.return_value = {"Item": {"tenant_id": {"S": "other"}}}
    s3 = Mock()
    service = InferenceService(FakeEngine(), s3=s3, ddb=ddb, producer=Mock())
    with pytest.raises(ValueError, match="does not match"):
        service.process_batch([ready()])
    s3.get_object.assert_not_called()

    conflicting = ready()
    conflicting.duplicate_group_id = "group-2"
    valid_event = ready()
    data = _png()
    ddb.get_item.return_value = {
        "Item": {
            "tenant_id": {"S": "tenant-1"},
            "dedup_scope_id": {"S": "scope-1"},
            "bucket": {"S": RAW_BUCKET},
            "object_key": {"S": valid_event.object_key},
            "duplicate_group_id": {"S": "group-1"},
            "dedup_status": {"S": "unique"},
            "expected_size_bytes": {"N": str(len(data))},
            "content_type": {"S": "image/png"},
        }
    }
    s3.get_object.return_value = {"Body": BytesIO(data)}
    with pytest.raises(ValueError, match="conflicting inference events"):
        service.process_batch([valid_event, conflicting])


def test_inference_skips_stale_ready_and_never_records_failed_delivery() -> None:
    data = _png()
    event = ready()
    state = {
        "tenant_id": {"S": "tenant-1"},
        "dedup_scope_id": {"S": "scope-1"},
        "bucket": {"S": RAW_BUCKET},
        "object_key": {"S": event.object_key},
        "duplicate_group_id": {"S": "old-group"},
        "dedup_status": {"S": "unique"},
        "expected_size_bytes": {"N": str(len(data))},
        "content_type": {"S": "image/png"},
    }
    ddb = Mock()
    ddb.get_item.side_effect = lambda **_kwargs: {"Item": state}
    s3 = Mock()
    s3.get_object.side_effect = lambda **_kwargs: {"Body": BytesIO(data)}
    producer = Mock()
    producer.flush.return_value = 1
    service = InferenceService(FakeEngine(), s3=s3, ddb=ddb, producer=producer)
    assert service.process_batch([event]) == []
    s3.get_object.assert_not_called()
    state["duplicate_group_id"] = {"S": "group-1"}
    with pytest.raises(RuntimeError, match="Kafka delivery failed"):
        service.process_batch([event])
    ddb.update_item.assert_not_called()
