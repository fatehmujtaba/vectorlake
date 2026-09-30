from typing import cast

from google.protobuf.message import DecodeError, Message

from vectorlake.events.v1 import events_pb2

EVENT_TYPES: dict[str, type[Message]] = {
    "image.received.v1": events_pb2.ImageReceivedV1,
    "image.duplicate.v1": events_pb2.ImageDuplicateV1,
    "image.inference-ready.v1": events_pb2.ImageInferenceReadyV1,
    "embedding.generated.v1": events_pb2.EmbeddingGeneratedV1,
    "embedding.archived.v1": events_pb2.EmbeddingArchivedV1,
    "image.indexed.v1": events_pb2.ImageIndexedV1,
    "processing.failed.v1": events_pb2.ProcessingFailedV1,
}

REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "image.received.v1": ("bucket", "object_key"),
    "image.duplicate.v1": ("duplicate_group_id", "representative_image_id", "policy_version"),
    "image.inference-ready.v1": ("bucket", "object_key", "model_route"),
    "embedding.generated.v1": (
        "embedding_id",
        "model_name",
        "model_version",
        "preprocess_version",
        "dimension",
    ),
    "embedding.archived.v1": ("embedding_id", "archive_bucket", "archive_key"),
    "image.indexed.v1": ("embedding_id", "collection_name"),
    "processing.failed.v1": ("stage", "error_code"),
}

MAX_EVENT_BYTES = 1_048_576


class EventCodecError(ValueError):
    """An event is malformed or violates the versioned contract."""


def _event_type(topic: str) -> type[Message]:
    try:
        return EVENT_TYPES[topic]
    except KeyError as exc:
        raise EventCodecError(f"unknown event topic: {topic}") from exc


def _validate(topic: str, message: Message) -> None:
    if not message.HasField("context"):
        raise EventCodecError("event context is required")
    context = cast(events_pb2.ImageReceivedV1, message).context
    if context.schema_version != 1:
        raise EventCodecError(f"unsupported schema version: {context.schema_version}")
    if context.event_type != topic:
        raise EventCodecError("event type does not match topic")
    for field in ("event_id", "image_id", "tenant_id", "dedup_scope_id"):
        if not getattr(context, field):
            raise EventCodecError(f"event context {field} is required")
    if context.occurred_at_epoch_ms <= 0:
        raise EventCodecError("event timestamp must be positive")
    for field in REQUIRED_FIELDS[topic]:
        if not getattr(message, field):
            raise EventCodecError(f"event field {field} is required")
    if (
        topic == "embedding.generated.v1"
        and isinstance(message, events_pb2.EmbeddingGeneratedV1)
        and not (message.vector or message.vector_ref)
    ):
        raise EventCodecError("embedding vector or reference is required")


def serialize_event(topic: str, message: Message) -> bytes:
    expected = _event_type(topic)
    if not isinstance(message, expected):
        raise EventCodecError(f"message type does not match topic: {topic}")
    _validate(topic, message)
    payload = message.SerializeToString(deterministic=True)
    if len(payload) > MAX_EVENT_BYTES:
        raise EventCodecError("event payload exceeds size limit")
    return payload


def deserialize_event(topic: str, payload: bytes) -> Message:
    expected = _event_type(topic)
    if len(payload) > MAX_EVENT_BYTES:
        raise EventCodecError("event payload exceeds size limit")
    message = expected()
    try:
        message.ParseFromString(payload)
    except DecodeError as exc:
        raise EventCodecError("invalid Protobuf payload") from exc
    _validate(topic, message)
    return message
