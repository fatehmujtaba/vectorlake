from __future__ import annotations

import logging

from google.protobuf.message import Message

from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.embedding import TorchvisionResNet18EmbeddingEngine
from vectorlake.events.v1 import events_pb2
from vectorlake.inference import InferenceService
from vectorlake.worker import require_worker_rollout, run_worker
from vectorlake.worker_retry import WORKERS


def process_events(service: InferenceService, events: list[Message]) -> None:
    batch = []
    for event in events:
        if not isinstance(event, events_pb2.ImageInferenceReadyV1):
            raise TypeError("unexpected inference event type")
        batch.append(event)
    service.process_batch(batch)


def main() -> None:
    settings = LocalInfrastructureSettings()
    require_worker_rollout(settings)
    service = InferenceService(TorchvisionResNet18EmbeddingEngine(), settings)
    run_worker(WORKERS["inference"], lambda events: process_events(service, events), settings)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
