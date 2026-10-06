from __future__ import annotations

import logging
import os
import time

from confluent_kafka import Consumer

from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.embedding_indexer import EmbeddingIndexer
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import deserialize_event
from vectorlake.semantic_index import SemanticIndex

logger = logging.getLogger(__name__)
TOPIC = "embedding.generated.v1"


def main() -> None:
    settings = LocalInfrastructureSettings()
    uri = os.environ.get("VECTORLAKE_SEMANTIC_DB", ".artifacts/semantic-index.db")
    index = SemanticIndex(uri)
    service = EmbeddingIndexer(index, settings)
    try:
        while True:
            consumer = Consumer(
                {
                    "bootstrap.servers": settings.kafka_bootstrap_servers,
                    "group.id": "vectorlake-index-v1",
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
                    if message.error():
                        raise RuntimeError(str(message.error()))
                    payload = message.value()
                    if payload is None:
                        raise ValueError("embedding event has no payload")
                    event = deserialize_event(TOPIC, payload)
                    if not isinstance(event, events_pb2.EmbeddingGeneratedV1):
                        raise TypeError("unexpected embedding event type")
                    service.process(event)
                    consumer.commit(message=message, asynchronous=False)
            except KeyboardInterrupt:
                break
            except Exception:
                logger.exception("semantic index failed; Kafka offset remains uncommitted")
                time.sleep(2)
            finally:
                consumer.close()
    finally:
        index.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
