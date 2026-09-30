from __future__ import annotations

from unittest.mock import Mock

import pytest

from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import deserialize_event
from vectorlake.revocation import RevocationService


def item(**values: str) -> dict[str, dict[str, str]]:
    return {key: {"S": value} for key, value in values.items()}


def service(status: str = "ACTIVE") -> tuple[RevocationService, Mock, Mock]:
    ddb = Mock()
    ddb.get_item.side_effect = [
        {
            "Item": item(
                image_id="image-1",
                decision_id="decision-1",
                tenant_id="tenant-1",
                dedup_scope_id="scope-1",
                status=status,
                duplicate_class="DERIVED",
                duplicate_group_id="group-other",
                own_group_id="group-own",
            )
        },
        {
            "Item": item(
                image_id="image-1",
                tenant_id="tenant-1",
                dedup_scope_id="scope-1",
                bucket="vectorlake-raw-images",
                object_key="tenants/tenant-1/images/image-1/source",
                dedup_status="duplicate_derived",
                duplicate_group_id="group-other",
            )
        },
    ]
    producer = Mock()
    producer.flush.return_value = 0
    return RevocationService(ddb, producer), ddb, producer


def test_revocation_publishes_reentry_then_updates_state() -> None:
    revoker, ddb, producer = service()
    assert revoker.revoke("image-1", "tenant-1", "scope-1", "reviewed false positive")
    payload = producer.produce.call_args.kwargs["value"]
    event = deserialize_event("image.inference-ready.v1", payload)
    assert isinstance(event, events_pb2.ImageInferenceReadyV1)
    assert event.context.image_id == "image-1"
    assert event.duplicate_group_id == "group-own"
    assert ddb.update_item.call_count == 3
    state_update = ddb.update_item.call_args_list[1].kwargs
    assert state_update["ExpressionAttributeValues"][":own_group"] == {"S": "group-own"}


def test_cross_scope_is_rejected_before_mutation() -> None:
    revoker, ddb, producer = service()
    with pytest.raises(ValueError, match="scope"):
        revoker.revoke("image-1", "tenant-1", "other-scope", "reviewed")
    ddb.update_item.assert_not_called()
    producer.produce.assert_not_called()


def test_revoked_decision_is_idempotent() -> None:
    revoker, ddb, producer = service("REVOKED")
    assert not revoker.revoke("image-1", "tenant-1", "scope-1", "reviewed")
    ddb.update_item.assert_not_called()
    producer.produce.assert_not_called()


def test_shadow_decision_cannot_be_revoked_as_active() -> None:
    revoker, ddb, producer = service("SHADOW")
    with pytest.raises(ValueError, match="not active"):
        revoker.revoke("image-1", "tenant-1", "scope-1", "reviewed")
    ddb.update_item.assert_not_called()
    producer.produce.assert_not_called()


def test_kafka_failure_keeps_state_duplicate_and_decision_retryable() -> None:
    revoker, ddb, producer = service()
    producer.flush.return_value = 1
    with pytest.raises(RuntimeError, match="Kafka delivery"):
        revoker.revoke("image-1", "tenant-1", "scope-1", "reviewed")
    assert ddb.update_item.call_count == 1


def test_retry_from_revoking_republishes_same_logical_event() -> None:
    revoker, ddb, producer = service("REVOKING")
    assert revoker.revoke("image-1", "tenant-1", "scope-1", "reviewed")
    event = deserialize_event(
        "image.inference-ready.v1", producer.produce.call_args.kwargs["value"]
    )
    assert isinstance(event, events_pb2.ImageInferenceReadyV1)
    assert event.context.event_id
    assert ddb.update_item.call_count == 2


def test_retry_after_state_update_only_finishes_decision() -> None:
    revoker, ddb, producer = service("REVOKING")
    decision_response = next(ddb.get_item.side_effect)
    ddb.get_item.side_effect = [
        decision_response,
        {
            "Item": item(
                image_id="image-1",
                tenant_id="tenant-1",
                dedup_scope_id="scope-1",
                bucket="vectorlake-raw-images",
                object_key="tenants/tenant-1/images/image-1/source",
                dedup_status="unique",
                duplicate_group_id="group-own",
                revoked_decision_id="decision-1",
            )
        },
    ]
    assert revoker.revoke("image-1", "tenant-1", "scope-1", "reviewed")
    producer.produce.assert_not_called()
    assert ddb.update_item.call_count == 1
