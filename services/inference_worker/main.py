from __future__ import annotations

import logging
import time
from typing import Any

from confluent_kafka import Consumer

from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.embedding import MAX_INFERENCE_BATCH, TorchvisionResNet18EmbeddingEngine
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import deserialize_event
from vectorlake.inference import InferenceService

logger = logging.getLogger(__name__)
INPUT_TOPIC = "image.inference-ready.v1"


def _event(message: Any) -> events_pb2.ImageInferenceReadyV1:
    payload = message.value()
    if payload is None:
        raise ValueError("image.inference-ready.v1 has no payload")
    decoded = deserialize_event(INPUT_TOPIC, payload)
    if not isinstance(decoded, events_pb2.ImageInferenceReadyV1):
        raise TypeError("unexpected inference event type")
    return decoded


def main() -> None:
    settings = LocalInfrastructureSettings()
    service = InferenceService(TorchvisionResNet18EmbeddingEngine(), settings)
    while True:
        consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": "vectorlake-inference-v1",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "max.poll.interval.ms": 600_000,
            }
        )
        try:
            consumer.subscribe([INPUT_TOPIC])
            while True:
                first = consumer.poll(1)
                if first is None:
                    continue
                if first.error():
                    raise RuntimeError(str(first.error()))
                messages = [first]
                while len(messages) < MAX_INFERENCE_BATCH:
                    next_message = consumer.poll(0.025)
                    if next_message is None:
                        break
                    if next_message.error():
                        raise RuntimeError(str(next_message.error()))
                    messages.append(next_message)
                service.process_batch([_event(message) for message in messages])
                for message in messages:
                    consumer.commit(message=message, asynchronous=False)
        except KeyboardInterrupt:
            break
        except Exception:
            logger.exception("inference batch failed; Kafka offsets remain uncommitted")
            time.sleep(2)
        finally:
            consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
