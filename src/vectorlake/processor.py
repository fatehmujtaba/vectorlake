from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass
from tempfile import SpooledTemporaryFile
from typing import Any, BinaryIO, cast
from uuid import NAMESPACE_URL, uuid5

from botocore.exceptions import ClientError
from confluent_kafka import Producer
from google.protobuf.message import Message

from vectorlake.common.aws import aws_client
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.common.resources import IMAGE_TABLE, RAW_BUCKET
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import serialize_event
from vectorlake.near_copy import POLICY_VERSION, Fingerprint, fingerprint, verify
from vectorlake.near_copy_index import ScopedPDQIndex
from vectorlake.revocation import DECISION_TABLE
from vectorlake.validation import (
    CANONICALIZATION_VERSION,
    MAX_OBJECT_BYTES,
    ValidationFailure,
    canonicalize_image,
    stream_image,
)

EXACT_TABLE = "exact_hash_registry"
PIXEL_TABLE = "pixel_hash_registry"
MODEL_ROUTE = "resnet18-v1"
EVIDENCE_TABLE = "near_copy_evidence"
logger = logging.getLogger(__name__)


def observation_namespace(scope_id: str, image_id: str, capture_id: str | None) -> str:
    if capture_id:
        capture_digest = hashlib.sha256(capture_id.encode()).hexdigest()
        return f"SCOPE#{scope_id}#CAPTURE#{capture_digest}"
    return f"SCOPE#{scope_id}#IMAGE#{image_id}"


class RetryableProcessingError(Exception):
    pass


@dataclass(frozen=True)
class ProcessingOutcome:
    topic: str
    message: Message
    dedup_status: str
    validation_status: str
    owner_image_id: str = ""
    duplicate_group_id: str = ""
    fingerprint: Fingerprint | None = None


class ImageProcessor:
    def __init__(
        self,
        settings: LocalInfrastructureSettings | None = None,
        producer: Any | None = None,
    ) -> None:
        self.settings = settings or LocalInfrastructureSettings()
        self.s3 = aws_client("s3", self.settings)
        self.ddb = aws_client("dynamodb", self.settings)
        self.index = ScopedPDQIndex(self.ddb)
        self.producer = producer or Producer(
            {"bootstrap.servers": self.settings.kafka_bootstrap_servers}
        )

    def _state(self, event: events_pb2.ImageReceivedV1) -> dict[str, Any]:
        state = self.ddb.get_item(
            TableName=IMAGE_TABLE,
            Key={"image_id": {"S": event.context.image_id}},
            ConsistentRead=True,
        ).get("Item")
        if state is None or any(
            state[name]["S"] != expected
            for name, expected in (
                ("tenant_id", event.context.tenant_id),
                ("dedup_scope_id", event.context.dedup_scope_id),
                ("bucket", event.bucket),
                ("object_key", event.object_key),
                ("upload_intent_id", event.upload_intent_id),
            )
        ):
            raise ValueError("event identity does not match upload intent")
        if event.bucket != RAW_BUCKET:
            raise ValueError("event references unexpected source bucket")
        recorded_version = state.get("object_version", {}).get("S")
        if recorded_version and recorded_version != event.object_version:
            raise ValueError("event references unexpected source version")
        return cast(dict[str, Any], state)

    def _event_context(
        self, event: events_pb2.ImageReceivedV1, result: str
    ) -> events_pb2.EventContext:
        return events_pb2.EventContext(
            event_id=str(uuid5(NAMESPACE_URL, f"outcome/{event.context.event_id}/{result}")),
            image_id=event.context.image_id,
            tenant_id=event.context.tenant_id,
            dedup_scope_id=event.context.dedup_scope_id,
            occurred_at_epoch_ms=event.context.occurred_at_epoch_ms,
            traceparent=event.context.traceparent,
            schema_version=1,
            event_type=result,
        )

    def _duplicate(
        self,
        event: events_pb2.ImageReceivedV1,
        kind: str,
        owner_image_id: str,
        group_id: str,
    ) -> ProcessingOutcome:
        duplicate_class = (
            events_pb2.ImageDuplicateV1.EXACT
            if kind == "exact"
            else events_pb2.ImageDuplicateV1.PIXEL
        )
        message = events_pb2.ImageDuplicateV1(
            context=self._event_context(event, "image.duplicate.v1"),
            duplicate_group_id=group_id,
            representative_image_id=owner_image_id,
            duplicate_class=duplicate_class,
            policy_version=f"{kind}-v2",
            decision_record_id=str(
                uuid5(NAMESPACE_URL, f"decision/{event.context.image_id}/{kind}/{owner_image_id}")
            ),
        )
        return ProcessingOutcome(
            "image.duplicate.v1", message, f"duplicate_{kind}", "valid", owner_image_id, group_id
        )

    def _failed(self, event: events_pb2.ImageReceivedV1, code: str) -> ProcessingOutcome:
        message = events_pb2.ProcessingFailedV1(
            context=self._event_context(event, "processing.failed.v1"),
            stage="validation",
            error_code=code,
            retryable=False,
            safe_public_message="Image validation failed",
        )
        return ProcessingOutcome("processing.failed.v1", message, "not_evaluated", "invalid")

    def _claim(self, table: str, key: str, image_id: str, group_id: str) -> tuple[str, str]:
        item = {
            "PK": {"S": key},
            "owner_image_id": {"S": image_id},
            "duplicate_group_id": {"S": group_id},
            "created_at_epoch_s": {"N": str(int(time.time()))},
        }
        try:
            self.ddb.put_item(
                TableName=table,
                Item=item,
                ConditionExpression="attribute_not_exists(PK)",
            )
            return image_id, group_id
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
        existing = self.ddb.get_item(
            TableName=table, Key={"PK": {"S": key}}, ConsistentRead=True
        ).get("Item")
        if existing is None:
            raise RetryableProcessingError("claim owner not visible")
        return existing["owner_image_id"]["S"], existing["duplicate_group_id"]["S"]

    def _validated_owner(self, image_id: str) -> bool:
        owner = self.ddb.get_item(
            TableName=IMAGE_TABLE,
            Key={"image_id": {"S": image_id}},
            ConsistentRead=True,
        ).get("Item")
        return bool(owner and owner.get("validation_status", {}).get("S") == "valid")

    def _record_validation(
        self,
        event: events_pb2.ImageReceivedV1,
        byte_digest: str,
        pixel_digest: str,
        width: int,
        height: int,
        sample_mode: str,
    ) -> None:
        self.ddb.update_item(
            TableName=IMAGE_TABLE,
            Key={"image_id": {"S": event.context.image_id}},
            ConditionExpression="tenant_id = :tenant AND dedup_scope_id = :scope",
            UpdateExpression=(
                "SET validation_status = :valid, byte_sha256 = :byte, "
                "canonical_pixel_sha256 = :pixel, pixel_canonicalization_version = :version, "
                "width = :width, height = :height, sample_mode = :mode"
            ),
            ExpressionAttributeValues={
                ":tenant": {"S": event.context.tenant_id},
                ":scope": {"S": event.context.dedup_scope_id},
                ":valid": {"S": "valid"},
                ":byte": {"S": byte_digest},
                ":pixel": {"S": pixel_digest},
                ":version": {"S": CANONICALIZATION_VERSION},
                ":width": {"N": str(width)},
                ":height": {"N": str(height)},
                ":mode": {"S": sample_mode},
            },
        )

    def _shadow_evidence(
        self,
        event: events_pb2.ImageReceivedV1,
        source: SpooledTemporaryFile[bytes],
        own_group_id: str,
    ) -> Fingerprint | None:
        try:
            pdq = fingerprint(cast(BinaryIO, source))
            candidates = self.index.find(event.context.dedup_scope_id, event.context.image_id, pdq)
            records: list[dict[str, Any]] = []
            shadow_decision: dict[str, Any] | None = None
            for candidate in candidates:
                state = self.ddb.get_item(
                    TableName=IMAGE_TABLE,
                    Key={"image_id": {"S": candidate.image_id}},
                    ConsistentRead=True,
                ).get("Item")
                if not state or (
                    state.get("dedup_scope_id", {}).get("S") != event.context.dedup_scope_id
                    or state.get("tenant_id", {}).get("S") != event.context.tenant_id
                    or state.get("bucket", {}).get("S") != RAW_BUCKET
                    or state.get("validation_status", {}).get("S") != "valid"
                    or state.get("dedup_status", {}).get("S") != "unique"
                ):
                    continue
                request = {"Bucket": state["bucket"]["S"], "Key": state["object_key"]["S"]}
                version = state.get("object_version", {}).get("S")
                if version:
                    request["VersionId"] = version
                response = self.s3.get_object(**request)
                body = response["Body"]
                try:
                    expected = int(state["expected_size_bytes"]["N"])
                    if int(response["ContentLength"]) != expected:
                        continue
                    with SpooledTemporaryFile(max_size=1_048_576, mode="w+b") as other:
                        streamed = stream_image(body, other, expected)
                        canonicalize_image(other, streamed.media_type)
                        result = verify(cast(BinaryIO, source), cast(BinaryIO, other))
                    records.append(
                        {
                            "candidate_image_id": candidate.image_id,
                            "pdq_distance": candidate.distance,
                            **result.__dict__,
                        }
                    )
                    if result.accepted and shadow_decision is None:
                        owner_group = state.get("duplicate_group_id", {}).get("S")
                        if owner_group:
                            shadow_decision = {
                                "image_id": {"S": event.context.image_id},
                                "decision_id": {
                                    "S": str(
                                        uuid5(
                                            NAMESPACE_URL,
                                            f"shadow/{event.context.image_id}/{candidate.image_id}/{POLICY_VERSION}",
                                        )
                                    )
                                },
                                "tenant_id": {"S": event.context.tenant_id},
                                "dedup_scope_id": {"S": event.context.dedup_scope_id},
                                "status": {"S": "SHADOW"},
                                "duplicate_class": {"S": "DERIVED"},
                                "duplicate_group_id": {"S": owner_group},
                                "own_group_id": {"S": own_group_id},
                                "representative_image_id": {"S": candidate.image_id},
                                "policy_version": {"S": POLICY_VERSION},
                                "created_at_epoch_ms": {"N": str(int(time.time() * 1000))},
                            }
                finally:
                    body.close()
            self.ddb.put_item(
                TableName=EVIDENCE_TABLE,
                Item={
                    "image_id": {"S": event.context.image_id},
                    "dedup_scope_id": {"S": event.context.dedup_scope_id},
                    "policy_version": {"S": POLICY_VERSION},
                    "pdq_hex": {"S": pdq.hex_value},
                    "pdq_quality": {"N": str(pdq.quality)},
                    "candidates_json": {"S": json.dumps(records, sort_keys=True)},
                },
            )
            if shadow_decision is not None:
                try:
                    self.ddb.put_item(
                        TableName=DECISION_TABLE,
                        Item=shadow_decision,
                        ConditionExpression="attribute_not_exists(image_id)",
                    )
                except ClientError as exc:
                    if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                        raise
            return pdq
        except Exception as exc:
            # Shadow analysis must never prevent the normal inference handoff.
            logger.warning(
                "near-copy shadow evaluation skipped for %s: %s", event.context.image_id, exc
            )
            return None

    def evaluate(self, event: events_pb2.ImageReceivedV1) -> ProcessingOutcome:
        state = self._state(event)
        if int(event.size_bytes) > MAX_OBJECT_BYTES:
            return self._failed(event, "object_size_out_of_bounds")
        request: dict[str, str] = {"Bucket": event.bucket, "Key": event.object_key}
        if event.object_version:
            request["VersionId"] = event.object_version
        try:
            response = self.s3.get_object(**request)
        except ClientError as exc:
            raise RetryableProcessingError("source object unavailable") from exc

        body = response["Body"]
        try:
            if int(response["ContentLength"]) != int(event.size_bytes):
                raise RetryableProcessingError("source object version changed")
            with SpooledTemporaryFile(max_size=1_048_576, mode="w+b") as temporary:
                try:
                    streamed = stream_image(body, temporary, int(event.size_bytes))
                    if streamed.media_type != state["content_type"]["S"]:
                        raise ValidationFailure("content_type_mismatch")
                except ValidationFailure as exc:
                    return self._failed(event, exc.code)

                scope = event.context.dedup_scope_id
                namespace = observation_namespace(
                    scope, event.context.image_id, state.get("capture_id", {}).get("S")
                )
                exact_key = f"{namespace}#SHA256#{streamed.byte_sha256}"
                existing = self.ddb.get_item(
                    TableName=EXACT_TABLE,
                    Key={"PK": {"S": exact_key}},
                    ConsistentRead=True,
                ).get("Item")
                if existing and existing["owner_image_id"]["S"] != event.context.image_id:
                    owner_id = existing["owner_image_id"]["S"]
                    if not self._validated_owner(owner_id):
                        raise RetryableProcessingError("exact owner validation pending")
                    return self._duplicate(
                        event, "exact", owner_id, existing["duplicate_group_id"]["S"]
                    )

                try:
                    canonical = canonicalize_image(temporary, streamed.media_type)
                except ValidationFailure as exc:
                    return self._failed(event, exc.code)
                self._record_validation(
                    event,
                    streamed.byte_sha256,
                    canonical.pixel_sha256,
                    canonical.width,
                    canonical.height,
                    canonical.sample_mode,
                )
                group_id = str(uuid5(NAMESPACE_URL, f"group/{namespace}/{canonical.pixel_sha256}"))
                exact_owner, exact_group = self._claim(
                    EXACT_TABLE, exact_key, event.context.image_id, group_id
                )
                if exact_owner != event.context.image_id:
                    if not self._validated_owner(exact_owner):
                        raise RetryableProcessingError("exact owner validation pending")
                    return self._duplicate(event, "exact", exact_owner, exact_group)

                pixel_key = f"{namespace}#PIXEL#{CANONICALIZATION_VERSION}#{canonical.pixel_sha256}"
                pixel_owner, pixel_group = self._claim(
                    PIXEL_TABLE, pixel_key, event.context.image_id, group_id
                )
                if pixel_owner != event.context.image_id:
                    if not self._validated_owner(pixel_owner):
                        raise RetryableProcessingError("pixel owner validation pending")
                    return self._duplicate(event, "pixel", pixel_owner, pixel_group)

                pdq = self._shadow_evidence(event, temporary, group_id)

                message = events_pb2.ImageInferenceReadyV1(
                    context=self._event_context(event, "image.inference-ready.v1"),
                    duplicate_group_id=group_id,
                    bucket=event.bucket,
                    object_key=event.object_key,
                    model_route=MODEL_ROUTE,
                )
                return ProcessingOutcome(
                    "image.inference-ready.v1", message, "unique", "valid", "", group_id, pdq
                )
        finally:
            body.close()

    def _publish(self, outcome: ProcessingOutcome, image_id: str) -> None:
        errors: list[str] = []

        def delivered(error: Any, _message: Any) -> None:
            if error is not None:
                errors.append(str(error))

        self.producer.produce(
            outcome.topic,
            key=image_id.encode(),
            value=serialize_event(outcome.topic, outcome.message),
            on_delivery=delivered,
        )
        remaining = self.producer.flush(10)
        if remaining or errors:
            raise RetryableProcessingError(f"Kafka delivery failed: {errors or remaining}")

    def _record_outcome(
        self, event: events_pb2.ImageReceivedV1, outcome: ProcessingOutcome
    ) -> None:
        values = {
            ":validation": {"S": outcome.validation_status},
            ":dedup": {"S": outcome.dedup_status},
            ":group": {"S": outcome.duplicate_group_id},
            ":owner": {"S": outcome.owner_image_id},
        }
        self.ddb.update_item(
            TableName=IMAGE_TABLE,
            Key={"image_id": {"S": event.context.image_id}},
            UpdateExpression=(
                "SET validation_status = :validation, dedup_status = :dedup, "
                "duplicate_group_id = :group, representative_image_id = :owner"
            ),
            ExpressionAttributeValues=values,
        )

    def process(self, event: events_pb2.ImageReceivedV1) -> ProcessingOutcome:
        outcome = self.evaluate(event)
        self._publish(outcome, event.context.image_id)
        self._record_outcome(event, outcome)
        if outcome.fingerprint is not None:
            try:
                self.index.add(
                    event.context.dedup_scope_id, event.context.image_id, outcome.fingerprint
                )
            except Exception as exc:
                logger.warning(
                    "near-copy index write skipped for %s: %s", event.context.image_id, exc
                )
        return outcome
