from __future__ import annotations

import hashlib
from io import BytesIO
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError
from PIL import Image, PngImagePlugin

from vectorlake.common.resources import IMAGE_TABLE, RAW_BUCKET
from vectorlake.events.v1 import events_pb2
from vectorlake.processor import EXACT_TABLE, PIXEL_TABLE, ImageProcessor, observation_namespace


def png(comment: str = "") -> bytes:
    output = BytesIO()
    metadata = PngImagePlugin.PngInfo()
    if comment:
        metadata.add_text("Comment", comment)
    Image.new("RGB", (8, 8), (41, 97, 133)).save(output, format="PNG", pnginfo=metadata)
    return output.getvalue()


def event(image_id: str, data: bytes) -> events_pb2.ImageReceivedV1:
    return events_pb2.ImageReceivedV1(
        context=events_pb2.EventContext(
            event_id=f"event-{image_id}",
            image_id=image_id,
            tenant_id="tenant-1",
            dedup_scope_id="scope-1",
            occurred_at_epoch_ms=1,
            schema_version=1,
            event_type="image.received.v1",
        ),
        bucket=RAW_BUCKET,
        object_key=f"tenants/tenant-1/images/{image_id}/source",
        size_bytes=len(data),
        upload_intent_id=f"intent-{image_id}",
    )


class EvaluationHarness:
    def __init__(self) -> None:
        self.states: dict[str, dict[str, dict[str, str]]] = {}
        self.objects: dict[str, bytes] = {}
        self.registries: dict[str, dict[str, dict[str, dict[str, str]]]] = {
            EXACT_TABLE: {},
            PIXEL_TABLE: {},
        }
        self.processor = object.__new__(ImageProcessor)
        self.processor.ddb = Mock()
        self.processor.ddb.get_item.side_effect = self.get_item
        self.processor.ddb.put_item.side_effect = self.put_item
        self.processor.s3 = Mock()
        self.processor.s3.get_object.side_effect = self.get_object

    def add(
        self, image_id: str, data: bytes, capture_id: str | None, source_id: str = "camera-1"
    ) -> events_pb2.ImageReceivedV1:
        received = event(image_id, data)
        state = {
            "tenant_id": {"S": "tenant-1"},
            "dedup_scope_id": {"S": "scope-1"},
            "bucket": {"S": RAW_BUCKET},
            "object_key": {"S": received.object_key},
            "upload_intent_id": {"S": received.upload_intent_id},
            "content_type": {"S": "image/png"},
        }
        if capture_id is not None:
            state["capture_id"] = {"S": capture_id}
            state["source_id"] = {"S": source_id}
        self.states[image_id] = state
        self.objects[received.object_key] = data
        return received

    def get_item(
        self, *, TableName: str, Key: dict[str, dict[str, str]], **_kwargs: object
    ) -> dict[str, object]:
        if TableName == IMAGE_TABLE:
            return {"Item": self.states[Key["image_id"]["S"]]}
        item = self.registries[TableName].get(Key["PK"]["S"])
        return {"Item": item} if item else {}

    def put_item(
        self, *, TableName: str, Item: dict[str, dict[str, str]], **_kwargs: object
    ) -> None:
        key = Item["PK"]["S"]
        if key in self.registries[TableName]:
            raise ClientError(
                {"Error": {"Code": "ConditionalCheckFailedException", "Message": "claimed"}},
                "PutItem",
            )
        self.registries[TableName][key] = Item

    def get_object(self, *, Key: str, **_kwargs: object) -> dict[str, object]:
        data = self.objects[Key]
        return {"Body": BytesIO(data), "ContentLength": len(data)}


def test_exact_dedup_preserves_distinct_and_legacy_observations() -> None:
    harness = EvaluationHarness()
    data = png()
    with (
        patch.object(ImageProcessor, "_validated_owner", return_value=True),
        patch.object(ImageProcessor, "_shadow_evidence", return_value=None),
    ):
        first = harness.processor.evaluate(harness.add("first", data, "frame-1"))
        same = harness.processor.evaluate(harness.add("same", data, "frame-1"))
        distinct = harness.processor.evaluate(harness.add("distinct", data, "frame-2"))
        other_source = harness.processor.evaluate(
            harness.add("other-source", data, "frame-1", "camera-2")
        )
        legacy_a = harness.processor.evaluate(harness.add("legacy-a", data, None))
        legacy_b = harness.processor.evaluate(harness.add("legacy-b", data, None))

    assert first.dedup_status == "unique"
    assert same.dedup_status == "duplicate_exact"
    assert same.owner_image_id == "first"
    assert isinstance(same.message, events_pb2.ImageDuplicateV1)
    assert same.message.policy_version == "exact-v3"
    assert distinct.dedup_status == "unique"
    assert distinct.duplicate_group_id != first.duplicate_group_id
    assert other_source.dedup_status == "unique"
    assert legacy_a.dedup_status == legacy_b.dedup_status == "unique"
    assert legacy_a.duplicate_group_id != legacy_b.duplicate_group_id


def test_pixel_dedup_requires_same_capture_id() -> None:
    harness = EvaluationHarness()
    with (
        patch.object(ImageProcessor, "_validated_owner", return_value=True),
        patch.object(ImageProcessor, "_shadow_evidence", return_value=None),
    ):
        first = harness.processor.evaluate(harness.add("first", png(), "frame-1"))
        same = harness.processor.evaluate(harness.add("same", png("metadata"), "frame-1"))
        distinct = harness.processor.evaluate(
            harness.add("distinct", png("other metadata"), "frame-2")
        )

    assert first.dedup_status == "unique"
    assert same.dedup_status == "duplicate_pixel"
    assert same.owner_image_id == "first"
    assert isinstance(same.message, events_pb2.ImageDuplicateV1)
    assert same.message.policy_version == "pixel-v3"
    assert distinct.dedup_status == "unique"


def test_source_qualification_preserves_capture_only_registry_key() -> None:
    old_digest = hashlib.sha256(b"frame-1").hexdigest()
    assert observation_namespace("scope-1", "image-1", "frame-1", None) == (
        f"SCOPE#scope-1#CAPTURE#{old_digest}"
    )
    assert observation_namespace("scope-1", "image-1", "frame-1", "camera-1") != (
        f"SCOPE#scope-1#CAPTURE#{old_digest}"
    )
