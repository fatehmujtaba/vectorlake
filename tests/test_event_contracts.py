from __future__ import annotations

import pytest

from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import EventCodecError, deserialize_event, serialize_event

TOPIC = "image.received.v1"


def received_event() -> events_pb2.ImageReceivedV1:
    return events_pb2.ImageReceivedV1(
        context=events_pb2.EventContext(
            event_id="event-1",
            image_id="image-1",
            tenant_id="tenant-1",
            dedup_scope_id="scope-1",
            occurred_at_epoch_ms=1,
            schema_version=1,
            event_type=TOPIC,
        ),
        bucket="raw",
        object_key="images/1",
        upload_intent_id="intent-1",
    )


def test_event_round_trip() -> None:
    event = received_event()
    assert deserialize_event(TOPIC, serialize_event(TOPIC, event)) == event


def test_additive_field_keeps_old_payload_readable() -> None:
    old_event = received_event()
    old_event.ClearField("upload_intent_id")
    old_payload = old_event.SerializeToString()
    decoded = deserialize_event(TOPIC, old_payload)
    assert isinstance(decoded, events_pb2.ImageReceivedV1)
    assert decoded.upload_intent_id == ""


@pytest.mark.parametrize(
    ("topic", "payload"),
    [
        (TOPIC, b"\xff"),
        (TOPIC, b""),
        ("unknown.v1", b""),
        (TOPIC, b"x" * 1_048_577),
    ],
)
def test_invalid_payload_rejected(topic: str, payload: bytes) -> None:
    with pytest.raises(EventCodecError):
        deserialize_event(topic, payload)


def test_topic_and_schema_must_match() -> None:
    event = received_event()
    event.context.event_type = "image.duplicate.v1"
    with pytest.raises(EventCodecError, match="event type"):
        serialize_event(TOPIC, event)
    event.context.event_type = TOPIC
    event.context.schema_version = 2
    with pytest.raises(EventCodecError, match="schema version"):
        serialize_event(TOPIC, event)
