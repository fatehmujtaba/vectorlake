from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from confluent_kafka import Consumer
from google.protobuf.message import Message

from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.events.v1.codec import EventCodecError, deserialize_event
from vectorlake.worker_retry import (
    DeadLetterPublisher,
    RetryLedger,
    RetryNotDue,
    SourceRecord,
    WorkerSpec,
)

logger = logging.getLogger(__name__)
Handler = Callable[[list[Message]], None]


def require_worker_rollout(settings: LocalInfrastructureSettings) -> None:
    if not settings.workers_enabled:
        raise RuntimeError(
            "worker rollout is disabled; review the backlog preflight before enabling"
        )


class ReliableExecutor:
    def __init__(
        self,
        spec: WorkerSpec,
        handler: Handler,
        settings: LocalInfrastructureSettings,
        *,
        ledger: RetryLedger | None = None,
        deadletters: DeadLetterPublisher | None = None,
    ) -> None:
        self.spec = spec
        self.handler = handler
        self.ledger = ledger or RetryLedger(settings)
        self.deadletters = deadletters or DeadLetterPublisher(settings)

    def _claim(self, record: SourceRecord) -> dict[str, Any]:
        while True:
            try:
                return self.ledger.claim(record.identifier(self.spec))
            except RetryNotDue as exc:
                time.sleep(min(1, exc.seconds))

    def _terminal(self, record: SourceRecord, state: dict[str, Any]) -> bool:
        status = state["status"]["S"]
        if status == "deadletter_pending":
            self.deadletters.publish(self.spec, record, state)
            self.ledger.deadlettered(record.identifier(self.spec), state)
            return True
        return status in {"completed", "deadlettered"}

    def _decode(self, record: SourceRecord) -> Message:
        if record.payload is None:
            raise EventCodecError("input record has no payload")
        return deserialize_event(self.spec.topic, record.payload)

    def _failed(
        self,
        record: SourceRecord,
        state: dict[str, Any],
        error: Exception,
        *,
        isolate_batch: bool = False,
    ) -> None:
        self.ledger.fail(record.identifier(self.spec), state, error, isolate_batch=isolate_batch)
        logger.warning(
            "worker attempt failed stage=%s record=%s attempt=%s error_type=%s",
            self.spec.stage,
            record.identifier(self.spec),
            state["attempts"]["N"],
            type(error).__name__,
        )

    def _single(self, record: SourceRecord) -> None:
        while True:
            state = self._claim(record)
            if self._terminal(record, state):
                return
            try:
                self.handler([self._decode(record)])
            except Exception as exc:
                self._failed(record, state, exc)
            else:
                self.ledger.complete(record.identifier(self.spec), state)
                return

    def process(self, records: list[SourceRecord]) -> None:
        if not 1 <= len(records) <= self.spec.max_batch:
            raise ValueError("worker batch is empty or exceeds its configured maximum")
        pending: list[tuple[SourceRecord, dict[str, Any], Message]] = []
        for record in records:
            state = self._claim(record)
            if self._terminal(record, state):
                continue
            try:
                decoded = self._decode(record)
            except Exception as exc:
                self._failed(record, state, exc)
                self._single(record)
                continue
            pending.append((record, state, decoded))
        if not pending:
            return
        try:
            self.handler([decoded for _, _, decoded in pending])
        except Exception as exc:
            # Retry a failed micro-batch individually so one poison record cannot quarantine peers.
            for record, state, _ in pending:
                self._failed(record, state, exc, isolate_batch=len(pending) > 1)
            for record, _, _ in pending:
                self._single(record)
        else:
            for record, state, _ in pending:
                self.ledger.complete(record.identifier(self.spec), state)


def consume_once(
    consumer: Any,
    executor: ReliableExecutor,
    spec: WorkerSpec,
) -> bool:
    first = consumer.poll(1)
    if first is None:
        return False
    if first.error():
        raise RuntimeError(str(first.error()))
    messages = [first]
    deadline = time.monotonic() + spec.max_wait_seconds
    while len(messages) < spec.max_batch:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        message = consumer.poll(remaining)
        if message is None:
            break
        if message.error():
            raise RuntimeError(str(message.error()))
        messages.append(message)
    executor.process([SourceRecord.from_message(message) for message in messages])
    # All records must resolve before any offset can advance beyond another pending batch member.
    for message in messages:
        consumer.commit(message=message, asynchronous=False)
    return True


def run_worker(spec: WorkerSpec, handler: Handler, settings: LocalInfrastructureSettings) -> None:
    require_worker_rollout(settings)
    executor = ReliableExecutor(spec, handler, settings)
    while True:
        consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": spec.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "max.poll.interval.ms": 1_800_000,
            }
        )
        try:
            consumer.subscribe([spec.topic])
            while True:
                consume_once(consumer, executor, spec)
        except KeyboardInterrupt:
            break
        except Exception:
            logger.exception("worker transport/ledger failed; offsets remain uncommitted")
            time.sleep(2)
        finally:
            consumer.close()
