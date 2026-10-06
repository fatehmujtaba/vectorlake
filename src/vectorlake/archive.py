from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from io import BytesIO
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from botocore.exceptions import ClientError
from confluent_kafka import Producer

from vectorlake.common.aws import aws_client
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.common.resources import ARCHIVE_BUCKET
from vectorlake.embedding import EMBEDDING_DIMENSION
from vectorlake.embedding_contract import ValidatedEmbedding, validate_generated
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import serialize_event

OUTPUT_TOPIC = "embedding.archived.v1"
MAX_ARCHIVE_BATCH = 32


def archive_schema() -> Any:
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("embedding_id", pa.string()),
            pa.field("image_id", pa.string()),
            pa.field("tenant_id", pa.string()),
            pa.field("dedup_scope_id", pa.string()),
            pa.field("duplicate_group_id", pa.string()),
            pa.field("model_name", pa.string()),
            pa.field("model_version", pa.string()),
            pa.field("preprocess_version", pa.string()),
            pa.field("vector", pa.list_(pa.float32(), EMBEDDING_DIMENSION)),
            pa.field("source_event_id", pa.string()),
            pa.field("occurred_at_epoch_ms", pa.int64()),
            pa.field("traceparent", pa.string()),
        ]
    )


def _rows(events: list[events_pb2.EmbeddingGeneratedV1]) -> list[ValidatedEmbedding]:
    if not 1 <= len(events) <= MAX_ARCHIVE_BATCH:
        raise ValueError("archive batch is empty or exceeds the configured maximum")
    seen: dict[str, ValidatedEmbedding] = {}
    for event in events:
        row = validate_generated(event)
        prior = seen.get(row.embedding_id)
        if prior is not None and prior != row:
            raise ValueError("conflicting embeddings in archive batch")
        seen[row.embedding_id] = row
    return sorted(seen.values(), key=lambda row: row.embedding_id)


class ArchiveWriter:
    def __init__(
        self,
        settings: LocalInfrastructureSettings | None = None,
        *,
        s3: Any | None = None,
        producer: Any | None = None,
    ) -> None:
        self.settings = settings or LocalInfrastructureSettings()
        self.s3 = s3 or aws_client("s3", self.settings)
        self.producer = producer or Producer(
            {"bootstrap.servers": self.settings.kafka_bootstrap_servers}
        )

    def write_batch(
        self, events: list[events_pb2.EmbeddingGeneratedV1]
    ) -> list[events_pb2.EmbeddingArchivedV1]:
        import pyarrow as pa
        import pyarrow.parquet as pq

        rows = _rows(events)
        partitions = {(row.model_version, row.occurred_at_epoch_ms // 86_400_000) for row in rows}
        if len(partitions) != 1:
            raise ValueError("archive batch must share model version and UTC date")
        table = pa.Table.from_pylist(
            [vars(row) | {"vector": list(row.vector)} for row in rows],
            schema=archive_schema(),
        )
        buffer = BytesIO()
        pq.write_table(table, buffer, compression="zstd")
        payload = buffer.getvalue()
        digest = hashlib.sha256(payload).hexdigest()
        day = datetime.fromtimestamp(rows[0].occurred_at_epoch_ms / 1000, UTC).date()
        key = (
            f"model={rows[0].model_name}/version={rows[0].model_version}/"
            f"date={day}/part-{digest}.parquet"
        )
        try:
            self.s3.put_object(
                Bucket=ARCHIVE_BUCKET,
                Key=key,
                Body=payload,
                IfNoneMatch="*",
                Metadata={"sha256": digest},
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in (
                "PreconditionFailed",
                "ConditionalRequestConflict",
            ):
                raise
            head = self.s3.head_object(Bucket=ARCHIVE_BUCKET, Key=key)
            if head.get("Metadata", {}).get("sha256") != digest or head["ContentLength"] != len(
                payload
            ):
                raise ValueError("archive object conflicts with deterministic key") from exc
        head = self.s3.head_object(Bucket=ARCHIVE_BUCKET, Key=key)
        if head.get("Metadata", {}).get("sha256") != digest or head["ContentLength"] != len(
            payload
        ):
            raise ValueError("archive object verification failed")
        archived_at = int(head["LastModified"].timestamp() * 1000)
        produced = []
        for row in rows:
            context = events_pb2.EventContext(
                event_id=str(uuid5(NAMESPACE_URL, f"archive/{row.source_event_id}/{key}")),
                image_id=row.image_id,
                tenant_id=row.tenant_id,
                dedup_scope_id=row.dedup_scope_id,
                occurred_at_epoch_ms=row.occurred_at_epoch_ms,
                traceparent=row.traceparent,
                schema_version=1,
                event_type=OUTPUT_TOPIC,
            )
            archived = events_pb2.EmbeddingArchivedV1(
                context=context,
                embedding_id=row.embedding_id,
                archive_bucket=ARCHIVE_BUCKET,
                archive_key=key,
                archived_at_epoch_ms=archived_at,
            )
            errors: list[str] = []

            def delivered(error: Any, _message: Any, failures: list[str] = errors) -> None:
                if error is not None:
                    failures.append(str(error))

            self.producer.produce(
                OUTPUT_TOPIC,
                key=row.image_id.encode(),
                value=serialize_event(OUTPUT_TOPIC, archived),
                on_delivery=delivered,
            )
            remaining = self.producer.flush(10)
            if errors or remaining:
                raise RuntimeError(f"archive Kafka delivery failed: {errors or remaining}")
            produced.append(archived)
        return produced
