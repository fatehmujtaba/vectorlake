from __future__ import annotations

import logging
import time
from typing import Any

from confluent_kafka import Consumer

from vectorlake.archive import MAX_ARCHIVE_BATCH, ArchiveWriter
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import deserialize_event

logger = logging.getLogger(__name__)
TOPIC = "embedding.generated.v1"
MAX_WAIT_SECONDS = 0.1


def _event(message: Any) -> events_pb2.EmbeddingGeneratedV1:
    if message.error():
        raise RuntimeError(str(message.error()))
    payload = message.value()
    if payload is None:
        raise ValueError("embedding event has no payload")
    event = deserialize_event(TOPIC, payload)
    if not isinstance(event, events_pb2.EmbeddingGeneratedV1):
        raise TypeError("unexpected embedding event type")
    return event


def main() -> None:
    settings = LocalInfrastructureSettings()
    writer = ArchiveWriter(settings)
    while True:
        consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": "vectorlake-archive-v1",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        try:
            consumer.subscribe([TOPIC])
            while True:
                message = consumer.poll(1)
                if message is None:
                    continue
                messages = [message]
                deadline = time.monotonic() + MAX_WAIT_SECONDS
                while len(messages) < MAX_ARCHIVE_BATCH:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    next_message = consumer.poll(remaining)
                    if next_message is None:
                        break
                    messages.append(next_message)
                partitions: dict[tuple[str, int], list[events_pb2.EmbeddingGeneratedV1]] = {}
                for pending in messages:
                    event = _event(pending)
                    partition = (
                        event.model_version,
                        event.context.occurred_at_epoch_ms // 86_400_000,
                    )
                    partitions.setdefault(partition, []).append(event)
                for events in partitions.values():
                    writer.write_batch(events)
                for completed in messages:
                    consumer.commit(message=completed, asynchronous=False)
        except KeyboardInterrupt:
            break
        except Exception:
            logger.exception("archive write failed; Kafka offset remains uncommitted")
            time.sleep(2)
        finally:
            consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
