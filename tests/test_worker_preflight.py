from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from confluent_kafka import TopicPartition

from infrastructure.worker_preflight import classify, preflight, scan_backlog
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.common.resources import RAW_BUCKET
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import serialize_event
from vectorlake.processor import MODEL_ROUTE
from vectorlake.worker_retry import WORKERS


def received() -> events_pb2.ImageReceivedV1:
    return events_pb2.ImageReceivedV1(
        context=events_pb2.EventContext(
            event_id="event-1",
            image_id="image-1",
            tenant_id="tenant-1",
            dedup_scope_id="scope-1",
            occurred_at_epoch_ms=123,
            schema_version=1,
            event_type="image.received.v1",
        ),
        bucket=RAW_BUCKET,
        object_key="images/image-1",
        upload_intent_id="intent-1",
    )


def state() -> dict[str, object]:
    return {
        name: {"S": value}
        for name, value in {
            "tenant_id": "tenant-1",
            "dedup_scope_id": "scope-1",
            "bucket": RAW_BUCKET,
            "object_key": "images/image-1",
            "upload_intent_id": "intent-1",
            "duplicate_group_id": "group-1",
            "dedup_status": "unique",
        }.items()
    }


def test_inference_preflight_reports_stale_or_conflicting_groups() -> None:
    source = received()
    source.context.event_type = "image.inference-ready.v1"
    event = events_pb2.ImageInferenceReadyV1(
        context=source.context,
        bucket=RAW_BUCKET,
        object_key=source.object_key,
        model_route=MODEL_ROUTE,
        duplicate_group_id="group-1",
    )
    assert classify(WORKERS["inference"], event, state()) == "ready"
    assert (
        classify(WORKERS["inference"], event, state() | {"dedup_status": {"S": "duplicate_exact"}})
        == "stale"
    )
    event.duplicate_group_id = "old-group"
    assert classify(WORKERS["inference"], event, state()) == "identity_conflict"


@pytest.mark.parametrize("limit", [1, 2])
def test_backlog_snapshot_is_read_only_and_incomplete_scans_block(limit: int) -> None:
    spec = WORKERS["processor"]
    consumer, ddb = Mock(), Mock()
    consumer.list_topics.return_value = SimpleNamespace(
        topics={spec.topic: SimpleNamespace(error=None, partitions={0: object()})},
    )
    consumer.committed.return_value = [TopicPartition(spec.topic, 0, 0)]
    consumer.get_watermark_offsets.return_value = (0, 2)
    messages = []
    for offset in range(2):
        message = Mock()
        message.error.return_value = None
        message.partition.return_value, message.offset.return_value = 0, offset
        message.value.return_value = serialize_event(spec.topic, received())
        messages.append(message)
    consumer.poll.side_effect = messages
    ddb.get_item.return_value = {"Item": state()}
    report = scan_backlog(consumer, ddb, spec, max_messages=limit, max_seconds=1)
    assert report["complete_scan"] == (limit == 2)
    assert report["blocked"] == (limit == 1)
    consumer.commit.assert_not_called()
    consumer.subscribe.assert_not_called()
    ddb.update_item.assert_not_called()
    ddb.put_item.assert_not_called()


def test_historical_outcome_conflicts_block_even_when_stage_backlogs_are_clean() -> None:
    with (
        patch("infrastructure.worker_preflight.aws_client"),
        patch("infrastructure.worker_preflight.Consumer"),
        patch("infrastructure.worker_preflight.scan_backlog", return_value={"blocked": False}),
        patch(
            "infrastructure.worker_preflight.load_states",
            return_value=({}, {"complete_scan": True}),
        ),
        patch(
            "infrastructure.worker_preflight.read_events",
            return_value=(
                {},
                {
                    "complete_scan": True,
                    "retention_gap_partitions": 0,
                    "malformed_messages": 0,
                    "conflicting_event_id_payloads": 0,
                },
            ),
        ),
        patch(
            "infrastructure.worker_preflight.compare",
            return_value={
                "result_counts": {"conflicting_expected_events": 1},
                "events_without_completed_state": 0,
            },
        ),
    ):
        report = preflight(LocalInfrastructureSettings())
    assert report["activation_allowed"] is False
    assert report["actions_performed"] == {
        "commits": 0,
        "publishes": 0,
        "state_writes": 0,
        "replays": 0,
    }
