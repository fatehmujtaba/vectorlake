from __future__ import annotations

import argparse
import hashlib
import json
import time
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

from confluent_kafka import Consumer, TopicPartition
from google.protobuf.message import Message

from infrastructure.reconcile_outcomes import compare, load_states, read_events
from vectorlake.common.aws import aws_client
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.common.resources import IMAGE_TABLE, RAW_BUCKET
from vectorlake.embedding import MODEL_VERSION, PREPROCESS_VERSION, embedding_id
from vectorlake.embedding_contract import validate_generated
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import EventCodecError, deserialize_event
from vectorlake.processor import MODEL_ROUTE
from vectorlake.worker_retry import WORKERS, WorkerSpec

BLOCKERS = {
    "malformed",
    "missing_state",
    "identity_conflict",
    "stale",
    "state_pending",
    "model_conflict",
}


def classify(spec: WorkerSpec, event: Message, state: dict[str, Any] | None) -> str:
    if not isinstance(
        event,
        (
            events_pb2.ImageReceivedV1,
            events_pb2.ImageInferenceReadyV1,
            events_pb2.EmbeddingGeneratedV1,
        ),
    ):
        return "malformed"
    context = event.context
    if state is None:
        return "missing_state"
    expected = [("tenant_id", context.tenant_id), ("dedup_scope_id", context.dedup_scope_id)]
    if isinstance(event, events_pb2.ImageReceivedV1):
        expected += [
            ("bucket", event.bucket),
            ("object_key", event.object_key),
            ("upload_intent_id", event.upload_intent_id),
        ]
        if event.bucket != RAW_BUCKET:
            return "identity_conflict"
        recorded_version = state.get("object_version", {}).get("S")
        if recorded_version and recorded_version != event.object_version:
            return "identity_conflict"
        if any(state.get(field, {}).get("S") != value for field, value in expected):
            return "identity_conflict"
        return "completed_replay" if state.get("dedup_status", {}).get("S") else "ready"
    expected.append(("duplicate_group_id", event.duplicate_group_id))
    if isinstance(event, events_pb2.ImageInferenceReadyV1):
        expected += [("bucket", event.bucket), ("object_key", event.object_key)]
        if event.bucket != RAW_BUCKET or event.model_route != MODEL_ROUTE:
            return "model_conflict"
    else:
        try:
            validate_generated(event)
        except ValueError:
            return "model_conflict"
    if any(state.get(field, {}).get("S") != value for field, value in expected):
        return "identity_conflict"
    status = state.get("dedup_status", {}).get("S")
    if status is None:
        return "state_pending"
    if status != "unique":
        return "stale"
    expected_id = embedding_id(context.image_id, MODEL_VERSION, PREPROCESS_VERSION)
    generated_id = state.get("embedding_generated_id", {}).get("S")
    if generated_id and generated_id != expected_id:
        return "model_conflict"
    if spec.stage == "inference":
        return "completed_replay" if generated_id == expected_id else "ready"
    if spec.stage == "index" and generated_id is None:
        return "state_pending"
    indexed = state.get("embedding_indexed_id", {}).get("S")
    if spec.stage == "index" and indexed is not None:
        return "completed_replay" if indexed == expected_id else "model_conflict"
    return "ready"


def scan_backlog(
    consumer: Any,
    ddb: Any,
    spec: WorkerSpec,
    *,
    max_messages: int,
    max_seconds: int,
) -> dict[str, Any]:
    metadata = consumer.list_topics(timeout=10).topics.get(spec.topic)
    if metadata is None or metadata.error is not None:
        return {"stage": spec.stage, "complete_scan": False, "missing_topic": True, "blocked": True}
    partitions = [TopicPartition(spec.topic, partition) for partition in metadata.partitions]
    committed = {tp.partition: tp.offset for tp in consumer.committed(partitions, timeout=10)}
    assignments = []
    positions: dict[int, int] = {}
    highs: dict[int, int] = {}
    bounds = []
    for tp in partitions:
        low, high = consumer.get_watermark_offsets(tp, timeout=10, cached=False)
        offset = committed.get(tp.partition, -1)
        start = low if offset < 0 else max(low, offset)
        positions[tp.partition] = start
        highs[tp.partition] = high
        assignments.append(TopicPartition(spec.topic, tp.partition, start))
        bounds.append(
            {
                "partition": tp.partition,
                "low": low,
                "high": high,
                "committed": offset,
                "start": start,
                "retention_gap": low > 0 and (offset < 0 or offset < low),
                "offset_beyond_high": offset > high,
            }
        )
    consumer.assign(assignments)
    counts: Counter[str] = Counter()
    samples: list[dict[str, Any]] = []
    cache: dict[str, dict[str, Any] | None] = {}
    event_ids: dict[str, str] = {}
    embedding_ids: dict[str, str] = {}
    messages = 0
    conflicting_ids = 0
    conflicting_embeddings = 0
    deadline = time.monotonic() + max_seconds
    while any(positions[partition] < high for partition, high in highs.items()):
        if messages >= max_messages or time.monotonic() >= deadline:
            break
        message = consumer.poll(min(1, max(0, deadline - time.monotonic())))
        if message is None:
            continue
        if message.error():
            raise RuntimeError(str(message.error()))
        partition = message.partition()
        offset = message.offset()
        if offset >= highs[partition]:
            positions[partition] = highs[partition]
            continue
        if offset < positions[partition]:
            continue
        positions[partition] = offset + 1
        messages += 1
        image_id = ""
        try:
            payload = message.value()
            if payload is None:
                raise EventCodecError("missing payload")
            event = deserialize_event(spec.topic, payload)
            if not isinstance(
                event,
                (
                    events_pb2.ImageReceivedV1,
                    events_pb2.ImageInferenceReadyV1,
                    events_pb2.EmbeddingGeneratedV1,
                ),
            ):
                raise EventCodecError("unexpected stage input")
            context = event.context
            image_id = context.image_id
            if image_id not in cache:
                cache[image_id] = ddb.get_item(
                    TableName=IMAGE_TABLE,
                    Key={"image_id": {"S": image_id}},
                    ConsistentRead=True,
                ).get("Item")
            result = classify(spec, event, cache[image_id])
            digest = hashlib.sha256(payload).hexdigest()
            prior = event_ids.setdefault(context.event_id, digest)
            if prior != digest:
                conflicting_ids += 1
                result = "identity_conflict"
            if isinstance(event, events_pb2.EmbeddingGeneratedV1):
                logical = events_pb2.EmbeddingGeneratedV1()
                logical.CopyFrom(event)
                logical.ClearField("context")
                logical_digest = hashlib.sha256(
                    logical.SerializeToString(deterministic=True)
                ).hexdigest()
                prior_embedding = embedding_ids.setdefault(event.embedding_id, logical_digest)
                if prior_embedding != logical_digest:
                    conflicting_embeddings += 1
                    result = "identity_conflict"
        except EventCodecError:
            result = "malformed"
        counts[result] += 1
        if result in BLOCKERS and len(samples) < 50:
            samples.append(
                {"partition": partition, "offset": offset, "image_id": image_id, "reason": result}
            )
    complete = all(positions[partition] >= high for partition, high in highs.items())
    gaps = any(row["retention_gap"] or row["offset_beyond_high"] for row in bounds)
    return {
        "stage": spec.stage,
        "topic": spec.topic,
        "consumer_group": spec.group_id,
        "messages_read": messages,
        "counts": dict(counts),
        "partition_bounds": bounds,
        "conflicting_event_ids": conflicting_ids,
        "conflicting_embedding_ids": conflicting_embeddings,
        "blocker_samples": samples,
        "complete_scan": complete,
        "blocked": not complete or gaps or any(counts[reason] for reason in BLOCKERS),
    }


def preflight(
    settings: LocalInfrastructureSettings,
    *,
    max_messages: int = 10_000,
    max_seconds: int = 30,
) -> dict[str, Any]:
    if not 1 <= max_messages <= 100_000 or not 1 <= max_seconds <= 60:
        raise ValueError("preflight limits are outside supported bounds")
    hosts = [
        urlparse(f"//{server.strip()}").hostname
        for server in settings.kafka_bootstrap_servers.split(",")
    ]
    loopback = {"localhost", "127.0.0.1", "::1"}
    if urlparse(settings.aws_endpoint_url).hostname not in loopback or any(
        host not in loopback for host in hosts
    ):
        raise ValueError("preflight is restricted to loopback local infrastructure")
    ddb = aws_client("dynamodb", settings)
    stages = []
    for spec in WORKERS.values():
        consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": spec.group_id,
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
            }
        )
        try:
            stages.append(
                scan_backlog(
                    consumer, ddb, spec, max_messages=max_messages, max_seconds=max_seconds
                )
            )
        finally:
            consumer.close()
    states, state_scan = load_states(ddb, max_pages=100, page_size=100)
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"vectorlake-preflight-readonly-{uuid4().hex}",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    try:
        events, event_scan = read_events(
            consumer, max_messages=max_messages, max_seconds=max_seconds
        )
    finally:
        consumer.close()
    outcomes = compare(states, events)
    historical_blocked = any(
        count
        for result, count in outcomes["result_counts"].items()
        if result not in {"matched", "compensated"}
    ) or bool(outcomes["events_without_completed_state"])
    coverage = (
        state_scan["complete_scan"]
        and event_scan["complete_scan"]
        and not event_scan["retention_gap_partitions"]
        and not event_scan["malformed_messages"]
        and not event_scan["conflicting_event_id_payloads"]
    )
    return {
        "read_only": True,
        "snapshot_consistent": False,
        "generated_at_epoch_ms": int(time.time() * 1000),
        "stages": stages,
        "historical_outcomes": {"state_scan": state_scan, "event_scan": event_scan, **outcomes},
        "activation_allowed": bool(
            coverage and not historical_blocked and not any(stage["blocked"] for stage in stages)
        ),
        "actions_performed": {"commits": 0, "publishes": 0, "state_writes": 0, "replays": 0},
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read-only worker backlog and historical outcome preflight"
    )
    parser.add_argument("--max-messages", type=int, default=10_000)
    parser.add_argument("--max-seconds", type=int, default=30)
    parser.add_argument("--output", type=Path, default=Path(".artifacts/m6/worker_preflight.json"))
    args = parser.parse_args()
    report = preflight(
        LocalInfrastructureSettings(), max_messages=args.max_messages, max_seconds=args.max_seconds
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    if not report["activation_allowed"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
