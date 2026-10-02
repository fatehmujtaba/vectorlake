from __future__ import annotations

import logging
import time

from confluent_kafka import Consumer

from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import deserialize_event
from vectorlake.processor import AlreadyProcessedOutcome, ImageProcessor

logger = logging.getLogger(__name__)


def main() -> None:
    settings = LocalInfrastructureSettings()
    processor = ImageProcessor(settings)
    while True:
        consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": "vectorlake-processor-v1",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "max.poll.interval.ms": 600_000,
            }
        )
        try:
            consumer.subscribe(["image.received.v1"])
            while True:
                message = consumer.poll(1)
                if message is None:
                    continue
                if message.error():
                    raise RuntimeError(str(message.error()))
                payload = message.value()
                if payload is None:
                    raise ValueError("image.received.v1 has no payload")
                event = deserialize_event("image.received.v1", payload)
                if not isinstance(event, events_pb2.ImageReceivedV1):
                    raise TypeError("unexpected event type")
                try:
                    processor.process(event)
                except AlreadyProcessedOutcome:
                    logger.info("skipping completed image %s", event.context.image_id)
                consumer.commit(message=message, asynchronous=False)
        except KeyboardInterrupt:
            break
        except Exception:
            logger.exception("processor failed; Kafka offset remains uncommitted")
            time.sleep(2)
        finally:
            consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
