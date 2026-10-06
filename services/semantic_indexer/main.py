from __future__ import annotations

import logging

from google.protobuf.message import Message

from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.embedding_indexer import EmbeddingIndexer
from vectorlake.events.v1 import events_pb2
from vectorlake.vector_client import VectorClient
from vectorlake.worker import require_worker_rollout, run_worker
from vectorlake.worker_retry import WORKERS


def process_events(service: EmbeddingIndexer, events: list[Message]) -> None:
    for event in events:
        if not isinstance(event, events_pb2.EmbeddingGeneratedV1):
            raise TypeError("unexpected index event type")
        service.process(event)


def main() -> None:
    settings = LocalInfrastructureSettings()
    require_worker_rollout(settings)
    index = VectorClient(settings)
    service = EmbeddingIndexer(index, settings)
    try:
        run_worker(WORKERS["index"], lambda events: process_events(service, events), settings)
    finally:
        index.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
