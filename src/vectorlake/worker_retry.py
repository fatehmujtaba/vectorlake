from __future__ import annotations

import hashlib
import json
import random
import time
from base64 import b64encode
from dataclasses import dataclass
from typing import Any, cast
from uuid import uuid4

from botocore.exceptions import ClientError
from confluent_kafka import Producer
from google.protobuf.message import DecodeError

from vectorlake.common.aws import aws_client
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.common.resources import WORKER_DLQ_BUCKET, WORKER_DLQ_TOPIC, WORKER_RETRY_TABLE
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import EVENT_TYPES, EventCodecError, serialize_event


@dataclass(frozen=True)
class WorkerSpec:
    stage: str
    topic: str
    group_id: str
    max_batch: int = 1
    max_wait_seconds: float = 0


WORKERS = {
    "processor": WorkerSpec("processor", "image.received.v1", "vectorlake-processor-v1"),
    "inference": WorkerSpec(
        "inference", "image.inference-ready.v1", "vectorlake-inference-v1", 2, 0.025
    ),
    "archive": WorkerSpec("archive", "embedding.generated.v1", "vectorlake-archive-v1", 32, 0.1),
    "index": WorkerSpec("index", "embedding.generated.v1", "vectorlake-index-v1"),
}


@dataclass(frozen=True)
class SourceRecord:
    topic: str
    partition: int
    offset: int
    payload: bytes | None
    key: bytes | None
    headers: tuple[tuple[str, bytes | None], ...] = ()

    @classmethod
    def from_message(cls, message: Any) -> SourceRecord:
        return cls(
            message.topic(),
            message.partition(),
            message.offset(),
            message.value(),
            message.key(),
            tuple(message.headers() or ()),
        )

    def snapshot(self) -> bytes:
        def encode(value: bytes | None) -> str | None:
            return None if value is None else b64encode(value).decode("ascii")

        return json.dumps(
            {
                "schema_version": 1,
                "topic": self.topic,
                "partition": self.partition,
                "offset": self.offset,
                "payload_base64": encode(self.payload),
                "key_base64": encode(self.key),
                "headers": [[name, encode(value)] for name, value in self.headers],
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()

    def identifier(self, spec: WorkerSpec) -> str:
        digest = hashlib.sha256(self.snapshot()).hexdigest()
        return hashlib.sha256(
            json.dumps([spec.stage, spec.group_id, digest], separators=(",", ":")).encode()
        ).hexdigest()


class RetryNotDue(Exception):
    def __init__(self, seconds: float) -> None:
        self.seconds = max(0.001, seconds)


class RetryLedger:
    """Count attempt reservations durably and fence concurrent/stale workers."""

    def __init__(self, settings: LocalInfrastructureSettings, *, ddb: Any | None = None) -> None:
        self.settings = settings
        self.ddb = ddb or aws_client("dynamodb", settings)

    def get(self, identifier: str) -> dict[str, Any]:
        return dict(
            self.ddb.get_item(
                TableName=WORKER_RETRY_TABLE,
                Key={"PK": {"S": identifier}},
                ConsistentRead=True,
            ).get("Item", {})
        )

    def claim(self, identifier: str) -> dict[str, Any]:
        state = self.get(identifier)
        status = state.get("status", {}).get("S", "")
        if status in {"completed", "deadletter_pending", "deadlettered"}:
            return state
        now = int(time.time() * 1000)
        due = int(state.get("next_attempt_at", {}).get("N", "0"))
        if status == "processing":
            due = int(state["lease_until"]["N"])
        if due > now:
            raise RetryNotDue((due - now) / 1000)
        attempts = int(state.get("attempts", {}).get("N", "0"))
        if attempts >= self.settings.worker_max_attempts:
            # An interrupted final attempt is uncertain and requires operator repair.
            self._update(
                identifier,
                state,
                "SET #status = :pending, error_code = :code, error_type = :kind, "
                "retryable = :retryable, failed_at = :now",
                {
                    ":pending": {"S": "deadletter_pending"},
                    ":code": {"S": "attempt_budget_exhausted_after_interruption"},
                    ":kind": {"S": "InterruptedAttempt"},
                    ":retryable": {"BOOL": True},
                    ":now": {"N": str(now)},
                },
            )
            return self.get(identifier)
        self._update(
            identifier,
            state,
            "SET #status = :processing, attempts = :next, "
            "attempt_token = :token, lease_until = :lease",
            {
                ":processing": {"S": "processing"},
                ":next": {"N": str(attempts + 1)},
                ":token": {"S": uuid4().hex},
                ":lease": {"N": str(now + self.settings.worker_lease_seconds * 1000)},
            },
        )
        return self.get(identifier)

    def _update(
        self,
        identifier: str,
        state: dict[str, Any],
        expression: str,
        values: dict[str, Any],
    ) -> None:
        condition = "attribute_not_exists(PK)"
        if state:
            condition = "#status = :previous AND attempts = :seen"
            values |= {":previous": state["status"], ":seen": state["attempts"]}
            if "attempt_token" in state:
                condition += " AND attempt_token = :owner"
                values[":owner"] = state["attempt_token"]
        try:
            self.ddb.update_item(
                TableName=WORKER_RETRY_TABLE,
                Key={"PK": {"S": identifier}},
                UpdateExpression=expression,
                ConditionExpression=condition,
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues=values,
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                raise RetryNotDue(0.1) from exc
            raise

    def complete(self, identifier: str, state: dict[str, Any]) -> None:
        self._update(identifier, state, "SET #status = :done", {":done": {"S": "completed"}})

    def fail(
        self,
        identifier: str,
        state: dict[str, Any],
        error: Exception,
        *,
        isolate_batch: bool = False,
    ) -> None:
        retryable = isolate_batch or not isinstance(error, (ValueError, TypeError))
        code = (
            "invalid_event"
            if isinstance(error, EventCodecError)
            else ("invalid_input_or_state" if not retryable else "processing_or_dependency_failure")
        )
        attempts = int(state["attempts"]["N"])
        terminal = not retryable or attempts >= self.settings.worker_max_attempts
        cap = min(
            self.settings.worker_retry_cap_seconds,
            self.settings.worker_retry_base_seconds * 2 ** (attempts - 1),
        )
        delay = 0 if isolate_batch else random.uniform(0, cap)
        now = int(time.time() * 1000)
        self._update(
            identifier,
            state,
            "SET #status = :next_status, error_code = :code, error_type = :kind, "
            "retryable = :retryable, failed_at = :now, next_attempt_at = :due",
            {
                ":next_status": {"S": "deadletter_pending" if terminal else "retry_pending"},
                ":code": {"S": code},
                ":kind": {"S": type(error).__name__[:128]},
                ":retryable": {"BOOL": retryable},
                ":now": {"N": str(now)},
                ":due": {"N": str(now + int(delay * 1000))},
            },
        )

    def deadlettered(self, identifier: str, state: dict[str, Any]) -> None:
        self._update(identifier, state, "SET #status = :done", {":done": {"S": "deadlettered"}})


class DeadLetterPublisher:
    def __init__(
        self,
        settings: LocalInfrastructureSettings,
        *,
        s3: Any | None = None,
        producer: Any | None = None,
    ) -> None:
        self.s3 = s3 or aws_client("s3", settings)
        self.producer = producer or Producer(
            {"bootstrap.servers": settings.kafka_bootstrap_servers}
        )

    def publish(
        self,
        spec: WorkerSpec,
        record: SourceRecord,
        state: dict[str, Any],
    ) -> events_pb2.WorkerDeadLetterV1:
        identifier = record.identifier(spec)
        snapshot = record.snapshot()
        digest = hashlib.sha256(snapshot).hexdigest()
        key = f"stage={spec.stage}/record={identifier}.json"
        try:
            self.s3.put_object(
                Bucket=WORKER_DLQ_BUCKET,
                Key=key,
                Body=snapshot,
                IfNoneMatch="*",
                Metadata={"sha256": digest},
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "PreconditionFailed":
                raise
        head = self.s3.head_object(Bucket=WORKER_DLQ_BUCKET, Key=key)
        if head.get("Metadata", {}).get("sha256") != digest or head["ContentLength"] != len(
            snapshot
        ):
            raise RuntimeError("dead-letter source snapshot verification failed")
        deadletter = events_pb2.WorkerDeadLetterV1(
            record_id=identifier,
            stage=spec.stage,
            consumer_group=spec.group_id,
            source_topic=record.topic,
            source_partition=record.partition,
            source_offset=record.offset,
            record_bucket=WORKER_DLQ_BUCKET,
            record_key=key,
            record_sha256=digest,
            attempts=int(state["attempts"]["N"]),
            error_code=state["error_code"]["S"],
            error_type=state["error_type"]["S"],
            retryable=state["retryable"]["BOOL"],
            failed_at_epoch_ms=int(state["failed_at"]["N"]),
            schema_version=1,
        )
        if record.payload is not None and spec.topic in EVENT_TYPES:
            source = EVENT_TYPES[spec.topic]()
            try:
                source.ParseFromString(record.payload)
                if source.HasField("context"):
                    deadletter.source_context.CopyFrom(
                        cast(events_pb2.ImageReceivedV1, source).context
                    )
            except (DecodeError, ValueError):
                pass
        errors: list[str] = []

        def delivered(error: Any, _message: Any) -> None:
            if error is not None:
                errors.append(str(error))

        self.producer.produce(
            WORKER_DLQ_TOPIC,
            key=identifier.encode(),
            value=serialize_event(WORKER_DLQ_TOPIC, deadletter),
            on_delivery=delivered,
        )
        remaining = self.producer.flush(10)
        if errors or remaining:
            raise RuntimeError("dead-letter Kafka delivery failed")
        return deadletter
