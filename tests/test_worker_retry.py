from __future__ import annotations

from copy import deepcopy
from typing import Any
from unittest.mock import Mock, patch

import pytest

from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import serialize_event
from vectorlake.worker import ReliableExecutor, consume_once, require_worker_rollout
from vectorlake.worker_retry import (
    DeadLetterPublisher,
    RetryLedger,
    RetryNotDue,
    SourceRecord,
    WorkerSpec,
)

SPEC = WorkerSpec("test", "image.received.v1", "test-group", 2, 0.025)


class MemoryLedger(RetryLedger):
    def __init__(
        self, settings: LocalInfrastructureSettings, data: dict[str, Any] | None = None
    ) -> None:
        self.settings = settings
        self.data: dict[str, Any] = data if data is not None else {}

    def get(self, identifier: str) -> dict[str, Any]:
        return deepcopy(self.data.get(identifier, {}))

    def _update(
        self,
        identifier: str,
        state: dict[str, Any],
        expression: str,
        values: dict[str, Any],
    ) -> None:
        if self.get(identifier) != state:
            raise RetryNotDue(0.1)
        fields = {
            ":processing": "status",
            ":pending": "status",
            ":next_status": "status",
            ":done": "status",
            ":next": "attempts",
            ":token": "attempt_token",
            ":lease": "lease_until",
            ":code": "error_code",
            ":kind": "error_type",
            ":retryable": "retryable",
            ":now": "failed_at",
            ":due": "next_attempt_at",
        }
        current = self.data.setdefault(identifier, {"PK": {"S": identifier}})
        for key, value in values.items():
            current[fields[key]] = deepcopy(value)


def settings() -> LocalInfrastructureSettings:
    return LocalInfrastructureSettings(
        worker_max_attempts=3,
        worker_retry_base_seconds=0,
        worker_retry_cap_seconds=0,
    )


def record(image_id: str = "image-1", offset: int = 0) -> SourceRecord:
    event = events_pb2.ImageReceivedV1(
        context=events_pb2.EventContext(
            event_id=f"event-{image_id}",
            image_id=image_id,
            tenant_id="tenant-1",
            dedup_scope_id="scope-1",
            occurred_at_epoch_ms=123,
            schema_version=1,
            event_type=SPEC.topic,
            traceparent="trace-fixture",
        ),
        bucket="raw",
        object_key=f"images/{image_id}",
    )
    return SourceRecord(
        SPEC.topic,
        0,
        offset,
        serialize_event(SPEC.topic, event),
        b"key",
        (("traceparent", b"trace-fixture"),),
    )


def test_transient_failure_recovers_with_persisted_attempt_count() -> None:
    ledger, deadletters = MemoryLedger(settings()), Mock()
    handler = Mock(side_effect=[RuntimeError("temporary dependency outage"), None])
    source = record()
    ReliableExecutor(SPEC, handler, settings(), ledger=ledger, deadletters=deadletters).process(
        [source]
    )
    state = ledger.get(source.identifier(SPEC))
    assert state["attempts"]["N"] == "2"
    assert state["status"]["S"] == "completed"
    deadletters.publish.assert_not_called()


def test_retry_budget_survives_a_new_executor() -> None:
    source = record()
    first = MemoryLedger(settings())
    for _ in range(2):
        state = first.claim(source.identifier(SPEC))
        first.fail(source.identifier(SPEC), state, RuntimeError("temporary"))
    restarted = MemoryLedger(settings(), first.data)
    deadletters, handler = Mock(), Mock(side_effect=RuntimeError("still unavailable"))
    ReliableExecutor(SPEC, handler, settings(), ledger=restarted, deadletters=deadletters).process(
        [source]
    )
    assert handler.call_count == 1
    assert restarted.get(source.identifier(SPEC))["attempts"]["N"] == "3"
    assert restarted.get(source.identifier(SPEC))["status"]["S"] == "deadlettered"
    deadletters.publish.assert_called_once()


def test_malformed_record_goes_to_dlq_without_handler_calls() -> None:
    source = SourceRecord(SPEC.topic, 0, 0, b"\xff", None)
    ledger, deadletters, handler = MemoryLedger(settings()), Mock(), Mock()
    ReliableExecutor(SPEC, handler, settings(), ledger=ledger, deadletters=deadletters).process(
        [source]
    )
    handler.assert_not_called()
    state = ledger.get(source.identifier(SPEC))
    assert state["attempts"]["N"] == "1"
    assert state["error_code"]["S"] == "invalid_event"
    assert state["retryable"]["BOOL"] is False
    deadletters.publish.assert_called_once()


def test_batch_failure_isolates_poison_from_healthy_records() -> None:
    bad, good = record("bad", 0), record("good", 1)
    ledger, deadletters = MemoryLedger(settings()), Mock()

    def process(events: list[Any]) -> None:
        if any(event.context.image_id == "bad" for event in events):
            raise ValueError("invalid state")

    ReliableExecutor(SPEC, process, settings(), ledger=ledger, deadletters=deadletters).process(
        [bad, good]
    )
    assert ledger.get(bad.identifier(SPEC))["status"]["S"] == "deadlettered"
    assert ledger.get(good.identifier(SPEC))["status"]["S"] == "completed"
    assert deadletters.publish.call_args.args[1] == bad
    assert deadletters.publish.call_count == 1


def test_failed_dlq_delivery_replays_terminal_state_without_reprocessing() -> None:
    source = record()
    ledger, deadletters = MemoryLedger(settings()), Mock()
    deadletters.publish.side_effect = RuntimeError("Kafka unavailable")
    handler = Mock(side_effect=ValueError("permanent"))
    worker = ReliableExecutor(SPEC, handler, settings(), ledger=ledger, deadletters=deadletters)
    with pytest.raises(RuntimeError, match="Kafka unavailable"):
        worker.process([source])
    terminal = ledger.get(source.identifier(SPEC))
    assert terminal["status"]["S"] == "deadletter_pending"
    deadletters.publish.side_effect = None
    worker.process([source])
    assert handler.call_count == 1
    assert deadletters.publish.call_args.args[2] == terminal
    assert ledger.get(source.identifier(SPEC))["attempts"]["N"] == "1"


def test_active_lease_and_stale_claim_are_fenced() -> None:
    source = record()
    ledger = MemoryLedger(settings())
    claim = ledger.claim(source.identifier(SPEC))
    with pytest.raises(RetryNotDue):
        ledger.claim(source.identifier(SPEC))
    ledger.fail(source.identifier(SPEC), claim, RuntimeError())
    with pytest.raises(RetryNotDue):
        ledger.complete(source.identifier(SPEC), claim)


def test_final_interrupted_attempt_is_quarantined_after_lease_expires() -> None:
    source = record()
    ledger = MemoryLedger(settings())
    for _ in range(2):
        claim = ledger.claim(source.identifier(SPEC))
        ledger.fail(source.identifier(SPEC), claim, RuntimeError())
    claim = ledger.claim(source.identifier(SPEC))
    with patch(
        "vectorlake.worker_retry.time.time", return_value=int(claim["lease_until"]["N"]) / 1000 + 1
    ):
        exhausted = ledger.claim(source.identifier(SPEC))
    assert exhausted["status"]["S"] == "deadletter_pending"
    assert exhausted["error_code"]["S"] == "attempt_budget_exhausted_after_interruption"


@pytest.mark.parametrize("failure", [False, True])
def test_offsets_commit_only_after_success_or_durable_dlq(failure: bool) -> None:
    source = record()
    message = Mock()
    message.error.return_value = None
    message.topic.return_value, message.partition.return_value, message.offset.return_value = (
        source.topic,
        0,
        0,
    )
    message.value.return_value, message.key.return_value, message.headers.return_value = (
        source.payload,
        source.key,
        source.headers,
    )
    consumer, executor = Mock(), Mock()
    consumer.poll.side_effect = [message, None]

    def process(_records: list[SourceRecord]) -> None:
        consumer.commit.assert_not_called()
        if failure:
            raise RuntimeError("DLQ delivery failed")

    executor.process.side_effect = process
    if failure:
        with pytest.raises(RuntimeError):
            consume_once(consumer, executor, SPEC)
        consumer.commit.assert_not_called()
    else:
        assert consume_once(consumer, executor, SPEC)
        consumer.commit.assert_called_once_with(message=message, asynchronous=False)


def test_worker_rollout_is_disabled_by_default() -> None:
    with pytest.raises(RuntimeError, match="rollout is disabled"):
        require_worker_rollout(LocalInfrastructureSettings(workers_enabled=False))


def test_backoff_is_jittered_and_persisted_before_another_claim() -> None:
    ledger = MemoryLedger(LocalInfrastructureSettings())
    source = record()
    with (
        patch("vectorlake.worker_retry.time.time", return_value=1_700_000_000),
        patch("vectorlake.worker_retry.random.uniform", return_value=0.25) as jitter,
    ):
        claim = ledger.claim(source.identifier(SPEC))
        ledger.fail(source.identifier(SPEC), claim, RuntimeError())
        jitter.assert_called_once_with(0, 0.5)
        with pytest.raises(RetryNotDue) as pending:
            ledger.claim(source.identifier(SPEC))
    assert pending.value.seconds == 0.25
    assert ledger.get(source.identifier(SPEC))["next_attempt_at"]["N"] == "1700000000250"


def test_deadletter_does_not_publish_before_snapshot_is_stored() -> None:
    s3, producer = Mock(), Mock()
    s3.put_object.side_effect = RuntimeError("storage unavailable")
    publisher = DeadLetterPublisher(settings(), s3=s3, producer=producer)
    with pytest.raises(RuntimeError, match="storage unavailable"):
        publisher.publish(SPEC, record(), {})
    producer.produce.assert_not_called()


def test_deadletter_delivery_callback_failure_is_not_acknowledged() -> None:
    import hashlib

    source = record()
    ledger = MemoryLedger(settings())
    claim = ledger.claim(source.identifier(SPEC))
    ledger.fail(source.identifier(SPEC), claim, ValueError())
    terminal = ledger.get(source.identifier(SPEC))
    s3, producer = Mock(), Mock()
    snapshot = source.snapshot()
    s3.head_object.return_value = {
        "Metadata": {"sha256": hashlib.sha256(snapshot).hexdigest()},
        "ContentLength": len(snapshot),
    }
    producer.flush.return_value = 0

    def failed(*args: Any, **kwargs: Any) -> None:
        kwargs["on_delivery"]("delivery-error", None)

    producer.produce.side_effect = failed
    publisher = DeadLetterPublisher(settings(), s3=s3, producer=producer)
    with pytest.raises(RuntimeError, match="Kafka delivery failed"):
        publisher.publish(SPEC, source, terminal)
    assert ledger.get(source.identifier(SPEC)) == terminal
