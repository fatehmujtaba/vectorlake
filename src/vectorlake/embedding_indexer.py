from __future__ import annotations

from typing import Any
from uuid import NAMESPACE_URL, uuid5

from botocore.exceptions import ClientError
from confluent_kafka import Producer

from vectorlake.common.aws import aws_client
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.common.resources import IMAGE_TABLE
from vectorlake.embedding_contract import validate_generated
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import serialize_event
from vectorlake.semantic_index import SemanticRecord, SemanticStore

OUTPUT_TOPIC = "image.indexed.v1"


class PendingInferenceState(Exception):
    pass


class EmbeddingIndexer:
    def __init__(
        self,
        index: SemanticStore,
        settings: LocalInfrastructureSettings | None = None,
        *,
        ddb: Any | None = None,
        producer: Any | None = None,
    ) -> None:
        self.index = index
        self.settings = settings or LocalInfrastructureSettings()
        self.ddb = ddb or aws_client("dynamodb", self.settings)
        self.producer = producer or Producer(
            {"bootstrap.servers": self.settings.kafka_bootstrap_servers}
        )

    def process(self, event: events_pb2.EmbeddingGeneratedV1) -> events_pb2.ImageIndexedV1:
        row = validate_generated(event)
        state = self.ddb.get_item(
            TableName=IMAGE_TABLE,
            Key={"image_id": {"S": row.image_id}},
            ConsistentRead=True,
        ).get("Item")
        if state is None:
            raise PendingInferenceState(row.image_id)
        expected = (
            ("tenant_id", row.tenant_id),
            ("dedup_scope_id", row.dedup_scope_id),
            ("duplicate_group_id", row.duplicate_group_id),
        )
        if any(state.get(name, {}).get("S") != value for name, value in expected):
            raise ValueError("embedding event does not match image identity")
        generated_id = state.get("embedding_generated_id", {}).get("S")
        if generated_id is None:
            raise PendingInferenceState(row.image_id)
        if generated_id != row.embedding_id or state.get("dedup_status", {}).get("S") != "unique":
            raise ValueError("embedding event conflicts with active image state")
        self.index.ensure_collection()
        self.index.upsert(
            SemanticRecord(
                embedding_id=row.embedding_id,
                image_id=row.image_id,
                tenant_id=row.tenant_id,
                dedup_scope_id=row.dedup_scope_id,
                duplicate_group_id=row.duplicate_group_id,
                model_version=row.model_version,
                preprocess_version=row.preprocess_version,
                vector=row.vector,
            )
        )
        indexed = events_pb2.ImageIndexedV1(
            context=events_pb2.EventContext(
                event_id=str(
                    uuid5(NAMESPACE_URL, f"indexed/{row.source_event_id}/{self.index.collection}")
                ),
                image_id=row.image_id,
                tenant_id=row.tenant_id,
                dedup_scope_id=row.dedup_scope_id,
                occurred_at_epoch_ms=row.occurred_at_epoch_ms,
                traceparent=row.traceparent,
                schema_version=1,
                event_type=OUTPUT_TOPIC,
            ),
            embedding_id=row.embedding_id,
            collection_name=self.index.collection,
            indexed_at_epoch_ms=row.occurred_at_epoch_ms,
        )
        errors: list[str] = []
        self.producer.produce(
            OUTPUT_TOPIC,
            key=row.image_id.encode(),
            value=serialize_event(OUTPUT_TOPIC, indexed),
            on_delivery=lambda error, _message: errors.append(str(error)) if error else None,
        )
        remaining = self.producer.flush(10)
        if errors or remaining:
            raise RuntimeError(f"index Kafka delivery failed: {errors or remaining}")
        try:
            self.ddb.update_item(
                TableName=IMAGE_TABLE,
                Key={"image_id": {"S": row.image_id}},
                UpdateExpression="SET embedding_indexed_id = :embedding",
                ConditionExpression=(
                    "embedding_generated_id = :embedding AND dedup_status = :unique "
                    "AND duplicate_group_id = :group AND "
                    "(attribute_not_exists(embedding_indexed_id) OR "
                    "embedding_indexed_id = :embedding)"
                ),
                ExpressionAttributeValues={
                    ":embedding": {"S": row.embedding_id},
                    ":unique": {"S": "unique"},
                    ":group": {"S": row.duplicate_group_id},
                },
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
            current = self.ddb.get_item(
                TableName=IMAGE_TABLE,
                Key={"image_id": {"S": row.image_id}},
                ConsistentRead=True,
            ).get("Item", {})
            if current.get("embedding_indexed_id", {}).get("S") != row.embedding_id:
                raise
        return indexed
