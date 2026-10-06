from __future__ import annotations

from unittest.mock import Mock

from services.inference_worker.main import process_events
from vectorlake.common.resources import RAW_BUCKET
from vectorlake.events.v1 import events_pb2


def test_inference_adapter_preserves_micro_batching() -> None:
    event = events_pb2.ImageInferenceReadyV1(
        context=events_pb2.EventContext(
            event_id="ready-image-1",
            image_id="image-1",
            tenant_id="tenant-1",
            dedup_scope_id="scope-1",
            occurred_at_epoch_ms=123,
            schema_version=1,
            event_type="image.inference-ready.v1",
        ),
        duplicate_group_id="group-1",
        bucket=RAW_BUCKET,
        object_key="tenants/tenant-1/images/image-1/source",
        model_route="resnet18-v1",
    )
    second = events_pb2.ImageInferenceReadyV1()
    second.CopyFrom(event)
    second.context.image_id = "image-2"
    service = Mock()
    process_events(service, [event, second])
    service.process_batch.assert_called_once_with([event, second])
