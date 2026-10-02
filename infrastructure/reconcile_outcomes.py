from __future__ import annotations

import argparse
import json
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

from confluent_kafka import Consumer, TopicPartition

from vectorlake.common.aws import aws_client
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.common.resources import IMAGE_TABLE
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import EventCodecError, deserialize_event
from vectorlake.processor import DETERMINISTIC_POLICY_VERSION

OUTCOME_TOPICS = (
    "image.inference-ready.v1",
    "image.duplicate.v1",
    "processing.failed.v1",
)


@dataclass(frozen=True)
class OutcomeEvent:
    topic: str
    event_id: str
    image_id: str
    tenant_id: str
    scope_id: str
    group_id: str = ""
    owner_id: str = ""
    duplicate_class: int = 0
    bucket: str = ""
    object_key: str = ""
    stage: str = ""


def _value(item: dict[str, Any], name: str) -> str:
    return str(item.get(name, {}).get("S", ""))


def load_states(
    ddb: Any, *, max_pages: int, page_size: int
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    if not 1 <= max_pages <= 1000 or not 1 <= page_size <= 1000:
        raise ValueError("max_pages and page_size must be between 1 and 1000")
    states: dict[str, dict[str, Any]] = {}
    cursor: dict[str, Any] | None = None
    pages = 0
    incomplete = 0
    while pages < max_pages:
        request: dict[str, Any] = {
            "TableName": IMAGE_TABLE,
            "ProjectionExpression": (
                "image_id,tenant_id,dedup_scope_id,dedup_status,validation_status,"
                "deterministic_policy_version,duplicate_group_id,representative_image_id,"
                "#bucket,object_key,revoked_decision_id"
            ),
            "ExpressionAttributeNames": {"#bucket": "bucket"},
            "ConsistentRead": True,
            "Limit": page_size,
        }
        if cursor is not None:
            request["ExclusiveStartKey"] = cursor
        response = ddb.scan(**request)
        pages += 1
        for item in response.get("Items", []):
            if "dedup_status" in item:
                states[_value(item, "image_id")] = item
            else:
                incomplete += 1
        cursor = response.get("LastEvaluatedKey")
        if cursor is None:
            break
    return states, {
        "pages_scanned": pages,
        "completed_images": len(states),
        "incomplete_images": incomplete,
        "complete_scan": cursor is None,
        "snapshot_consistent": False,
    }


def _outcome(topic: str, payload: bytes) -> OutcomeEvent:
    message = deserialize_event(topic, payload)
    if isinstance(message, events_pb2.ImageDuplicateV1):
        return OutcomeEvent(
            topic,
            message.context.event_id,
            message.context.image_id,
            message.context.tenant_id,
            message.context.dedup_scope_id,
            message.duplicate_group_id,
            message.representative_image_id,
            message.duplicate_class,
        )
    if isinstance(message, events_pb2.ImageInferenceReadyV1):
        return OutcomeEvent(
            topic,
            message.context.event_id,
            message.context.image_id,
            message.context.tenant_id,
            message.context.dedup_scope_id,
            group_id=message.duplicate_group_id,
            bucket=message.bucket,
            object_key=message.object_key,
        )
    if isinstance(message, events_pb2.ProcessingFailedV1):
        return OutcomeEvent(
            topic,
            message.context.event_id,
            message.context.image_id,
            message.context.tenant_id,
            message.context.dedup_scope_id,
            stage=message.stage,
        )
    raise ValueError(f"unexpected outcome topic: {topic}")


def read_events(
    consumer: Any, *, max_messages: int, max_seconds: int
) -> tuple[dict[str, list[OutcomeEvent]], dict[str, Any]]:
    if not 1 <= max_messages <= 1_000_000 or not 1 <= max_seconds <= 600:
        raise ValueError("max_messages or max_seconds is out of bounds")
    metadata = consumer.list_topics(timeout=10)
    assignments: list[TopicPartition] = []
    positions: dict[tuple[str, int], int] = {}
    high_watermarks: dict[tuple[str, int], int] = {}
    missing_topics: list[str] = []
    retention_gaps = 0
    for topic in OUTCOME_TOPICS:
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            missing_topics.append(topic)
            continue
        for partition in topic_metadata.partitions:
            tp = TopicPartition(topic, partition)
            low, high = consumer.get_watermark_offsets(tp, timeout=10, cached=False)
            key = (topic, partition)
            assignments.append(TopicPartition(topic, partition, low))
            positions[key] = low
            high_watermarks[key] = high
            if low > 0:
                retention_gaps += 1
    if assignments:
        consumer.assign(assignments)
    events: dict[str, list[OutcomeEvent]] = defaultdict(list)
    seen_ids: dict[tuple[str, str], set[bytes]] = {}
    replayed = 0
    conflicting_ids = 0
    malformed = 0
    read_count = 0
    deadline = time.monotonic() + max_seconds
    while (
        any(positions[key] < high for key, high in high_watermarks.items())
        and read_count < max_messages
        and time.monotonic() < deadline
    ):
        message = consumer.poll(1)
        if message is None:
            continue
        if message.error():
            raise RuntimeError(str(message.error()))
        key = (message.topic(), message.partition())
        high = high_watermarks[key]
        if message.offset() >= high:
            positions[key] = high
            continue
        positions[key] = max(positions[key], message.offset() + 1)
        read_count += 1
        payload = message.value()
        if payload is None:
            malformed += 1
            continue
        try:
            event = _outcome(message.topic(), payload)
        except (EventCodecError, ValueError):
            malformed += 1
            continue
        event_key = (event.topic, event.event_id)
        variants = seen_ids.setdefault(event_key, set())
        if payload in variants:
            replayed += 1
            continue
        if variants:
            conflicting_ids += 1
        variants.add(payload)
        events[event.image_id].append(event)
    return dict(events), {
        "messages_read": read_count,
        "replayed_event_ids": replayed,
        "conflicting_event_id_payloads": conflicting_ids,
        "malformed_messages": malformed,
        "missing_topics": missing_topics,
        "retention_gap_partitions": retention_gaps,
        "complete_scan": not missing_topics
        and all(positions[key] >= high for key, high in high_watermarks.items()),
    }


def _expected_topic(status: str) -> str | None:
    if status == "unique":
        return "image.inference-ready.v1"
    if status in {"duplicate_exact", "duplicate_pixel", "duplicate_derived"}:
        return "image.duplicate.v1"
    if status == "not_evaluated":
        return "processing.failed.v1"
    return None


def _matches(state: dict[str, Any], event: OutcomeEvent) -> bool:
    if event.tenant_id != _value(state, "tenant_id") or event.scope_id != _value(
        state, "dedup_scope_id"
    ):
        return False
    status = _value(state, "dedup_status")
    if event.topic == "image.inference-ready.v1":
        return (
            event.group_id == _value(state, "duplicate_group_id")
            and event.bucket == _value(state, "bucket")
            and event.object_key == _value(state, "object_key")
        )
    if event.topic == "image.duplicate.v1":
        classes = {
            "duplicate_exact": events_pb2.ImageDuplicateV1.EXACT,
            "duplicate_pixel": events_pb2.ImageDuplicateV1.PIXEL,
            "duplicate_derived": events_pb2.ImageDuplicateV1.DERIVED,
        }
        return (
            event.group_id == _value(state, "duplicate_group_id")
            and event.owner_id == _value(state, "representative_image_id")
            and event.duplicate_class == classes.get(status)
        )
    return event.stage == "validation" and _value(state, "validation_status") == "invalid"


def compare(
    states: dict[str, dict[str, Any]], events: dict[str, list[OutcomeEvent]]
) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    by_policy: dict[str, Counter[str]] = defaultdict(Counter)
    samples: dict[str, list[str]] = defaultdict(list)
    for image_id, state in states.items():
        expected_topic = _expected_topic(_value(state, "dedup_status"))
        observed = events.get(image_id, [])
        if expected_topic is None:
            result = "unsupported_status"
        elif not observed:
            result = "missing_event"
        else:
            expected = [event for event in observed if event.topic == expected_topic]
            matching = [event for event in expected if _matches(state, event)]
            other = [event for event in observed if event.topic != expected_topic]
            if not expected:
                result = "different_outcome"
            elif not matching:
                result = "payload_mismatch"
            elif len(matching) != len(expected):
                result = "conflicting_expected_events"
            elif other and _value(state, "revoked_decision_id"):
                result = "compensated"
            elif other:
                result = "conflicting_outcomes"
            else:
                result = "matched"
        counts[result] += 1
        policy = (
            "current"
            if _value(state, "deterministic_policy_version") == DETERMINISTIC_POLICY_VERSION
            else "prior_or_unversioned"
        )
        by_policy[policy][result] += 1
        if result != "matched" and len(samples[result]) < 20:
            samples[result].append(image_id)
    return {
        "result_counts": dict(sorted(counts.items())),
        "result_counts_by_policy": {
            policy: dict(sorted(results.items())) for policy, results in sorted(by_policy.items())
        },
        "sample_image_ids": dict(sorted(samples.items())),
        "events_without_completed_state": len(set(events) - set(states)),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only local outcome reconciliation")
    parser.add_argument("--max-pages", type=int, default=100)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--max-messages", type=int, default=100_000)
    parser.add_argument("--max-seconds", type=int, default=60)
    args = parser.parse_args()
    settings = LocalInfrastructureSettings()
    hosts = [
        urlparse(f"//{server.strip()}").hostname
        for server in settings.kafka_bootstrap_servers.split(",")
    ]
    if urlparse(settings.aws_endpoint_url).hostname not in {"localhost", "127.0.0.1", "::1"} or any(
        host not in {"localhost", "127.0.0.1", "::1"} for host in hosts
    ):
        raise ValueError("reconciliation is restricted to loopback local services")
    states, state_scan = load_states(
        aws_client("dynamodb", settings), max_pages=args.max_pages, page_size=args.page_size
    )
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"vectorlake-reconcile-{uuid4().hex}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        events, event_scan = read_events(
            consumer, max_messages=args.max_messages, max_seconds=args.max_seconds
        )
    finally:
        consumer.close()
    report = {
        "state_scan": state_scan,
        "event_scan": event_scan,
        **compare(states, events),
    }
    report["coverage_complete"] = bool(
        state_scan["complete_scan"]
        and event_scan["complete_scan"]
        and not event_scan["missing_topics"]
        and not event_scan["retention_gap_partitions"]
        and not event_scan["malformed_messages"]
    )
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
