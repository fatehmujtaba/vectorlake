from __future__ import annotations

from tempfile import SpooledTemporaryFile
from typing import Any, cast
from uuid import NAMESPACE_URL, uuid5

from botocore.exceptions import ClientError
from confluent_kafka import Producer
from PIL import Image

from vectorlake.common.aws import aws_client
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.common.resources import IMAGE_TABLE, RAW_BUCKET
from vectorlake.embedding import (
    EMBEDDING_DIMENSION,
    MAX_INFERENCE_BATCH,
    EmbeddingEngine,
    embedding_id,
    validate_embedding,
)
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import serialize_event
from vectorlake.processor import MODEL_ROUTE
from vectorlake.validation import (
    MAX_OBJECT_BYTES,
    ValidationFailure,
    canonicalize_image,
    stream_image,
)

OUTPUT_TOPIC = "embedding.generated.v1"


class StaleInferenceEvent(Exception):
    pass


class InferenceService:
    def __init__(
        self,
        engine: EmbeddingEngine,
        settings: LocalInfrastructureSettings | None = None,
        *,
        s3: Any | None = None,
        ddb: Any | None = None,
        producer: Any | None = None,
    ) -> None:
        self.engine = engine
        self.settings = settings or LocalInfrastructureSettings()
        self.s3 = s3 or aws_client("s3", self.settings)
        self.ddb = ddb or aws_client("dynamodb", self.settings)
        self.producer = producer or Producer(
            {"bootstrap.servers": self.settings.kafka_bootstrap_servers}
        )

    def _state(self, event: events_pb2.ImageInferenceReadyV1) -> dict[str, Any]:
        state = self.ddb.get_item(
            TableName=IMAGE_TABLE,
            Key={"image_id": {"S": event.context.image_id}},
            ConsistentRead=True,
        ).get("Item")
        expected = (
            ("tenant_id", event.context.tenant_id),
            ("dedup_scope_id", event.context.dedup_scope_id),
            ("bucket", event.bucket),
            ("object_key", event.object_key),
        )
        if (
            state is None
            or event.bucket != RAW_BUCKET
            or event.model_route != MODEL_ROUTE
            or any(state.get(name, {}).get("S") != value for name, value in expected)
        ):
            raise ValueError("inference event does not match image identity")
        if (
            state.get("dedup_status", {}).get("S") != "unique"
            or state.get("duplicate_group_id", {}).get("S") != event.duplicate_group_id
        ):
            raise StaleInferenceEvent(event.context.image_id)
        return cast(dict[str, Any], state)

    def _image(self, event: events_pb2.ImageInferenceReadyV1, state: dict[str, Any]) -> Image.Image:
        size = int(state["expected_size_bytes"]["N"])
        if not 1 <= size <= MAX_OBJECT_BYTES:
            raise ValidationFailure("object_size_out_of_bounds")
        request: dict[str, str] = {"Bucket": event.bucket, "Key": event.object_key}
        version = state.get("object_version", {}).get("S")
        if version:
            request["VersionId"] = version
        response = self.s3.get_object(**request)
        body = response["Body"]
        try:
            with SpooledTemporaryFile(max_size=2 * 1024 * 1024) as temporary:
                streamed = stream_image(body, temporary, size)
                if streamed.media_type != state["content_type"]["S"]:
                    raise ValidationFailure("format_mismatch")
                canonicalize_image(temporary, streamed.media_type)
                temporary.seek(0)
                with Image.open(temporary) as image:
                    return image.convert("RGB")
        finally:
            body.close()

    def _event(
        self, source: events_pb2.ImageInferenceReadyV1, vector: list[float]
    ) -> events_pb2.EmbeddingGeneratedV1:
        validate_embedding(vector, self.engine.dimension)
        image_id = source.context.image_id
        identifier = embedding_id(
            image_id, self.engine.model_version, self.engine.preprocess_version
        )
        return events_pb2.EmbeddingGeneratedV1(
            context=events_pb2.EventContext(
                event_id=str(
                    uuid5(NAMESPACE_URL, f"embedding/{source.context.event_id}/{identifier}")
                ),
                image_id=image_id,
                tenant_id=source.context.tenant_id,
                dedup_scope_id=source.context.dedup_scope_id,
                occurred_at_epoch_ms=source.context.occurred_at_epoch_ms,
                traceparent=source.context.traceparent,
                schema_version=1,
                event_type=OUTPUT_TOPIC,
            ),
            embedding_id=identifier,
            duplicate_group_id=source.duplicate_group_id,
            model_name=self.engine.model_name,
            model_version=self.engine.model_version,
            preprocess_version=self.engine.preprocess_version,
            vector=vector,
            dimension=EMBEDDING_DIMENSION,
        )

    def _publish(self, event: events_pb2.EmbeddingGeneratedV1) -> None:
        errors: list[str] = []

        def delivered(error: Any, _message: Any) -> None:
            if error is not None:
                errors.append(str(error))

        self.producer.produce(
            OUTPUT_TOPIC,
            key=event.context.image_id.encode(),
            value=serialize_event(OUTPUT_TOPIC, event),
            on_delivery=delivered,
        )
        remaining = self.producer.flush(10)
        if errors or remaining:
            raise RuntimeError(f"embedding Kafka delivery failed: {errors or remaining}")

    def process_batch(
        self, events: list[events_pb2.ImageInferenceReadyV1]
    ) -> list[events_pb2.EmbeddingGeneratedV1]:
        if not 1 <= len(events) <= MAX_INFERENCE_BATCH:
            raise ValueError("inference batch is empty or exceeds the configured maximum")
        pending: list[tuple[events_pb2.ImageInferenceReadyV1, dict[str, Any]]] = []
        images: list[Image.Image] = []
        seen: dict[str, events_pb2.ImageInferenceReadyV1] = {}
        try:
            for event in events:
                prior = seen.get(event.context.image_id)
                if prior is not None:
                    if prior != event:
                        raise ValueError("conflicting inference events for one image in a batch")
                    continue
                seen[event.context.image_id] = event
                try:
                    state = self._state(event)
                except StaleInferenceEvent:
                    continue
                expected_id = embedding_id(
                    event.context.image_id,
                    self.engine.model_version,
                    self.engine.preprocess_version,
                )
                if state.get("embedding_generated_id", {}).get("S") == expected_id:
                    continue
                if "embedding_generated_id" in state:
                    raise ValueError("image already has an incompatible embedding version")
                images.append(self._image(event, state))
                pending.append((event, state))
            if not pending:
                return []
            vectors = self.engine.embed_images(images)
            if len(vectors) != len(pending):
                raise ValueError("embedding engine returned the wrong batch size")
            produced: list[events_pb2.EmbeddingGeneratedV1] = []
            for (event, _state), vector in zip(pending, vectors, strict=True):
                generated = self._event(event, vector)
                self._publish(generated)
                try:
                    self.ddb.update_item(
                        TableName=IMAGE_TABLE,
                        Key={"image_id": {"S": event.context.image_id}},
                        UpdateExpression="SET embedding_generated_id = :embedding",
                        ConditionExpression=(
                            "attribute_not_exists(embedding_generated_id) "
                            "AND dedup_status = :unique AND duplicate_group_id = :group"
                        ),
                        ExpressionAttributeValues={
                            ":embedding": {"S": generated.embedding_id},
                            ":unique": {"S": "unique"},
                            ":group": {"S": event.duplicate_group_id},
                        },
                    )
                except ClientError as exc:
                    if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                        raise
                    current = self._state(event)
                    if current.get("embedding_generated_id", {}).get("S") != generated.embedding_id:
                        raise
                produced.append(generated)
            return produced
        finally:
            for image in images:
                image.close()
