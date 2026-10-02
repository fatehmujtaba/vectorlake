from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

from infrastructure.reconcile_outcomes import OutcomeEvent, compare, load_states, read_events
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import serialize_event
from vectorlake.processor import DETERMINISTIC_POLICY_VERSION


def state(status: str, *, image_id: str = "image-1", group: str = "group-1") -> dict[str, object]:
    return {
        "image_id": {"S": image_id},
        "tenant_id": {"S": "tenant-1"},
        "dedup_scope_id": {"S": "scope-1"},
        "dedup_status": {"S": status},
        "validation_status": {"S": "valid"},
        "duplicate_group_id": {"S": group},
        "representative_image_id": {"S": "owner-1"},
        "bucket": {"S": "bucket-1"},
        "object_key": {"S": "key-1"},
    }


def ready(image_id: str = "image-1", group: str = "group-1") -> OutcomeEvent:
    return OutcomeEvent(
        "image.inference-ready.v1",
        "event-1",
        image_id,
        "tenant-1",
        "scope-1",
        group_id=group,
        bucket="bucket-1",
        object_key="key-1",
    )


def duplicate(image_id: str = "image-1") -> OutcomeEvent:
    return OutcomeEvent(
        "image.duplicate.v1",
        "event-2",
        image_id,
        "tenant-1",
        "scope-1",
        group_id="group-1",
        owner_id="owner-1",
        duplicate_class=events_pb2.ImageDuplicateV1.EXACT,
    )


def test_compare_matches_outcomes_and_flags_payload_mismatch() -> None:
    unique = state("unique")
    exact = state("duplicate_exact", image_id="image-2")
    exact["deterministic_policy_version"] = {"S": DETERMINISTIC_POLICY_VERSION}
    report = compare(
        {"image-1": unique, "image-2": exact},
        {"image-1": [ready()], "image-2": [duplicate("image-2")]},
    )
    assert report["result_counts"] == {"matched": 2}
    assert report["result_counts_by_policy"] == {
        "current": {"matched": 1},
        "prior_or_unversioned": {"matched": 1},
    }

    mismatch = compare({"image-1": unique}, {"image-1": [ready(group="other-group")]})
    assert mismatch["result_counts"] == {"payload_mismatch": 1}
    assert mismatch["sample_image_ids"] == {"payload_mismatch": ["image-1"]}


def test_compare_handles_compensated_and_missing_outcomes() -> None:
    revoked = state("unique")
    revoked["revoked_decision_id"] = {"S": "decision-1"}
    report = compare(
        {"image-1": revoked, "image-2": state("unique", image_id="image-2")},
        {"image-1": [duplicate(), ready()]},
    )
    assert report["result_counts"] == {"compensated": 1, "missing_event": 1}


def test_load_states_is_bounded_and_reports_incomplete_rows() -> None:
    ddb = Mock()
    ddb.scan.side_effect = [
        {"Items": [state("unique")], "LastEvaluatedKey": {"image_id": {"S": "image-1"}}},
        {"Items": [{"image_id": {"S": "pending"}}]},
    ]
    states, report = load_states(ddb, max_pages=2, page_size=1)
    assert list(states) == ["image-1"]
    assert report["complete_scan"] is True
    assert report["incomplete_images"] == 1
    assert ddb.scan.call_args_list[1].kwargs["ExclusiveStartKey"] == {"image_id": {"S": "image-1"}}


def test_read_events_deduplicates_replayed_event_id() -> None:
    event = events_pb2.ImageInferenceReadyV1(
        context=events_pb2.EventContext(
            event_id="event-1",
            image_id="image-1",
            tenant_id="tenant-1",
            dedup_scope_id="scope-1",
            occurred_at_epoch_ms=1,
            schema_version=1,
            event_type="image.inference-ready.v1",
        ),
        duplicate_group_id="group-1",
        bucket="bucket-1",
        object_key="key-1",
        model_route="resnet18-v1",
    )
    payload = serialize_event("image.inference-ready.v1", event)
    changed = events_pb2.ImageInferenceReadyV1()
    changed.CopyFrom(event)
    changed.duplicate_group_id = "other-group"
    changed_payload = serialize_event("image.inference-ready.v1", changed)
    metadata = SimpleNamespace(
        topics={
            topic: SimpleNamespace(error=None, partitions={0: object()})
            for topic in (
                "image.inference-ready.v1",
                "image.duplicate.v1",
                "processing.failed.v1",
            )
        }
    )
    consumer = Mock()
    consumer.list_topics.return_value = metadata
    consumer.get_watermark_offsets.side_effect = lambda tp, **_kwargs: (
        (0, 3) if tp.topic == "image.inference-ready.v1" else (0, 0)
    )
    messages = []
    for offset, value in enumerate((payload, payload, changed_payload)):
        message = Mock()
        message.error.return_value = None
        message.topic.return_value = "image.inference-ready.v1"
        message.partition.return_value = 0
        message.offset.return_value = offset
        message.value.return_value = value
        messages.append(message)
    consumer.poll.side_effect = messages
    events, report = read_events(consumer, max_messages=10, max_seconds=2)
    assert len(events["image-1"]) == 2
    assert report["replayed_event_ids"] == 1
    assert report["conflicting_event_id_payloads"] == 1
    assert report["complete_scan"] is True
