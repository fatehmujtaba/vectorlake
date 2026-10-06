from __future__ import annotations

from unittest.mock import Mock, patch

from services.inference_worker.main import main
from vectorlake.common.resources import RAW_BUCKET
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import serialize_event


def test_inference_worker_commits_only_after_batch_processing() -> None:
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
    message = Mock()
    message.error.return_value = None
    message.value.return_value = serialize_event("image.inference-ready.v1", event)
    consumer = Mock()
    consumer.poll.side_effect = [message, None, KeyboardInterrupt()]
    with (
        patch("services.inference_worker.main.Consumer", return_value=consumer),
        patch("services.inference_worker.main.TorchvisionResNet18EmbeddingEngine"),
        patch("services.inference_worker.main.InferenceService") as service_type,
    ):

        def processed(events: object) -> list[object]:
            assert events == [event]
            consumer.commit.assert_not_called()
            return []

        service_type.return_value.process_batch.side_effect = processed
        main()
    consumer.commit.assert_called_once_with(message=message, asynchronous=False)
    consumer.close.assert_called_once()
