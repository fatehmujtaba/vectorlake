from __future__ import annotations

import hashlib
import json
import time
from collections import Counter
from typing import Any
from uuid import uuid4

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]
from google.protobuf.message import Message

from infrastructure.bootstrap import bootstrap
from vectorlake.common.aws import aws_client
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.common.resources import WORKER_DLQ_BUCKET, WORKER_DLQ_TOPIC
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import deserialize_event, serialize_event
from vectorlake.worker import ReliableExecutor, consume_once
from vectorlake.worker_retry import DeadLetterPublisher, RetryLedger, SourceRecord, WorkerSpec


def verify() -> dict[str, object]:
    bootstrap()
    settings = LocalInfrastructureSettings(
        worker_max_attempts=3,
        worker_retry_base_seconds=0,
        worker_retry_cap_seconds=0,
    )
    identifier = uuid4().hex
    topic = f"vectorlake.m6-retry-drill.{identifier}"
    spec = WorkerSpec("m6-drill", "image.received.v1", f"vectorlake-m6-drill-{identifier}")
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    for future in admin.create_topics([NewTopic(topic, 1, 1)]).values():
        future.result(timeout=10)
    payloads: dict[str, bytes] = {}
    offsets: dict[str, int] = {}
    errors: list[str] = []
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    for name in ("transient", "permanent", "malformed", "exhausted", "restart"):
        event = events_pb2.ImageReceivedV1(
            context=events_pb2.EventContext(
                event_id=f"{identifier}-{name}",
                image_id=name,
                tenant_id="drill-tenant",
                dedup_scope_id="drill-scope",
                occurred_at_epoch_ms=int(time.time() * 1000),
                traceparent="00-" + "a" * 32 + "-" + "b" * 16 + "-01",
                schema_version=1,
                event_type=spec.topic,
            ),
            bucket="drill-bucket",
            object_key=f"drill/{name}",
        )
        payload = b"\xff" if name == "malformed" else serialize_event(spec.topic, event)
        payloads[name] = payload

        def delivered(error: Any, message: Any, label: str = name) -> None:
            if error:
                errors.append(str(error))
            else:
                offsets[label] = message.offset()

        producer.produce(
            topic,
            partition=0,
            key=b"drill-key",
            value=payload,
            headers=[("traceparent", b"drill-trace")],
            on_delivery=delivered,
        )
    assert producer.flush(10) == 0 and not errors
    sources = {
        name: SourceRecord(
            topic, 0, offsets[name], payload, b"drill-key", (("traceparent", b"drill-trace"),)
        )
        for name, payload in payloads.items()
    }
    ledger = RetryLedger(settings)
    for _ in range(2):
        claim = ledger.claim(sources["restart"].identifier(spec))
        ledger.fail(
            sources["restart"].identifier(spec),
            claim,
            RuntimeError("simulated outage before restart"),
        )
    calls: Counter[str] = Counter()

    def handle(events: list[Message]) -> None:
        for event in events:
            assert isinstance(event, events_pb2.ImageReceivedV1)
            name = event.context.image_id
            calls[name] += 1
            if name == "permanent":
                raise ValueError("simulated permanent state conflict")
            if name in {"exhausted", "restart"} or (name == "transient" and calls[name] < 3):
                raise RuntimeError("simulated transient dependency outage")

    # A new ledger/executor instance proves the budget is persisted in DynamoDB.
    executor = ReliableExecutor(spec, handle, settings)
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": spec.group_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    try:
        consumer.assign([TopicPartition(topic, 0, 0)])
        processed = 0
        deadline = time.monotonic() + 30
        while processed < len(sources) and time.monotonic() < deadline:
            if consume_once(consumer, executor, spec):
                processed += 1
        assert processed == len(sources)
        assert (
            consumer.committed([TopicPartition(topic, 0)], timeout=10)[0].offset
            == max(offsets.values()) + 1
        )
    finally:
        consumer.close()
    assert calls == {"transient": 3, "permanent": 1, "exhausted": 3, "restart": 1}
    terminal = {name: ledger.get(source.identifier(spec)) for name, source in sources.items()}
    assert terminal["transient"]["status"]["S"] == "completed"
    for name in ("permanent", "malformed", "exhausted", "restart"):
        assert terminal[name]["status"]["S"] == "deadlettered"
    before = dict(calls)
    for source in sources.values():
        executor.process([source])
    assert dict(calls) == before

    wanted = {source.identifier(spec) for name, source in sources.items() if name != "transient"}
    found: set[str] = set()
    verifier = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"m6-dlq-verifier-{identifier}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    try:
        verifier.subscribe([WORKER_DLQ_TOPIC])
        deadline = time.monotonic() + 20
        while found != wanted and time.monotonic() < deadline:
            message = verifier.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            dlq_payload = message.value()
            assert dlq_payload is not None
            deadletter = deserialize_event(WORKER_DLQ_TOPIC, dlq_payload)
            assert isinstance(deadletter, events_pb2.WorkerDeadLetterV1)
            if deadletter.record_id not in wanted:
                continue
            response = aws_client("s3", settings).get_object(
                Bucket=WORKER_DLQ_BUCKET, Key=deadletter.record_key
            )
            try:
                snapshot = response["Body"].read()
            finally:
                response["Body"].close()
            assert hashlib.sha256(snapshot).hexdigest() == deadletter.record_sha256
            assert any(snapshot == source.snapshot() for source in sources.values())
            if deadletter.HasField("source_context"):
                assert deadletter.source_context.traceparent.startswith("00-")
            found.add(deadletter.record_id)
    finally:
        verifier.close()
    assert found == wanted
    # Re-publication after a commit gap yields an identical logical DLQ envelope.
    publisher = DeadLetterPublisher(settings)
    frozen = terminal["permanent"]
    first = publisher.publish(spec, sources["permanent"], frozen)
    assert publisher.publish(spec, sources["permanent"], frozen) == first
    return {
        "isolated_input_topic": topic,
        "transient_attempts": 3,
        "permanent_attempts": 1,
        "malformed_handler_calls": 0,
        "exhausted_attempts": 3,
        "restart_remaining_attempts": 1,
        "durable_deadletters_verified": len(found),
        "source_offsets_committed": True,
        "replay_skipped": True,
        "deadletter_republication_identical": True,
        "http_servers_started": 0,
    }


if __name__ == "__main__":
    print(json.dumps(verify(), indent=2))
