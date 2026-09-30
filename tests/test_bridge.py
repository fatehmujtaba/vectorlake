from __future__ import annotations

import json
from typing import Any
from unittest.mock import Mock, patch

import pytest

from vectorlake.bridge import EventBridge
from vectorlake.common.resources import RAW_BUCKET
from vectorlake.events.v1 import events_pb2


def event() -> events_pb2.ImageReceivedV1:
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
        bucket="raw",
        object_key="key",
    )


def bridge_with_mocks() -> tuple[EventBridge, Mock, Mock, Mock]:
    bridge = object.__new__(EventBridge)
    sqs = Mock()
    ddb = Mock()
    producer = Mock()
    bridge.sqs = sqs
    bridge.ddb = ddb
    bridge.producer = producer
    bridge.queue_url = "queue-url"
    return bridge, sqs, ddb, producer


def notification() -> dict[str, Any]:
    return {"Body": json.dumps({"Records": [{}]}), "ReceiptHandle": "receipt"}


def test_sqs_deleted_after_kafka_ack() -> None:
    bridge, sqs, ddb, producer = bridge_with_mocks()
    producer.flush.return_value = 0
    with patch.object(EventBridge, "_event_from_record", return_value=event()):
        assert bridge.process_message(notification()) == ["event-1"]
    producer.produce.assert_called_once()
    ddb.update_item.assert_called_once()
    sqs.delete_message.assert_called_once_with(QueueUrl="queue-url", ReceiptHandle="receipt")


@pytest.mark.parametrize("failure", ["pending", "delivery-error"])
def test_kafka_failure_preserves_sqs_message(failure: str) -> None:
    bridge, sqs, ddb, producer = bridge_with_mocks()
    if failure == "pending":
        producer.flush.return_value = 1
    else:
        producer.flush.return_value = 0

        def fail_delivery(*_args: Any, **kwargs: Any) -> None:
            kwargs["on_delivery"]("broker unavailable", None)

        producer.produce.side_effect = fail_delivery
    with patch.object(EventBridge, "_event_from_record", return_value=event()):
        with pytest.raises(RuntimeError, match="Kafka delivery failed"):
            bridge.process_message(notification())
    ddb.update_item.assert_not_called()
    sqs.delete_message.assert_not_called()


def test_bad_notification_preserves_sqs_message() -> None:
    bridge, sqs, _ddb, producer = bridge_with_mocks()
    with pytest.raises(ValueError, match="no records"):
        bridge.process_message({"Body": '{"Records": []}', "ReceiptHandle": "receipt"})
    producer.produce.assert_not_called()
    sqs.delete_message.assert_not_called()


def test_cross_tenant_notification_rejected_before_object_lookup() -> None:
    bridge, _sqs, ddb, _producer = bridge_with_mocks()
    bridge.s3 = Mock()
    ddb.get_item.return_value = {
        "Item": {
            "tenant_id": {"S": "tenant-a"},
            "bucket": {"S": RAW_BUCKET},
            "object_key": {"S": "tenants/tenant-a/images/image-1/source"},
        }
    }
    record = {
        "eventName": "ObjectCreated:Put",
        "s3": {
            "bucket": {"name": RAW_BUCKET},
            "object": {"key": "tenants/tenant-b/images/image-1/source"},
        },
    }
    with pytest.raises(ValueError, match="matching upload intent"):
        bridge._event_from_record(record)
    bridge.s3.head_object.assert_not_called()
