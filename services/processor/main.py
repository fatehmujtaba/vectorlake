from __future__ import annotations

import logging

from google.protobuf.message import Message

from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.events.v1 import events_pb2
from vectorlake.processor import AlreadyProcessedOutcome, ImageProcessor
from vectorlake.worker import require_worker_rollout, run_worker
from vectorlake.worker_retry import WORKERS

logger = logging.getLogger(__name__)


def process_events(processor: ImageProcessor, events: list[Message]) -> None:
    for event in events:
        if not isinstance(event, events_pb2.ImageReceivedV1):
            raise TypeError("unexpected processor event type")
        try:
            processor.process(event)
        except AlreadyProcessedOutcome:
            logger.info("skipping completed image %s", event.context.image_id)


def main() -> None:
    settings = LocalInfrastructureSettings()
    require_worker_rollout(settings)
    processor = ImageProcessor(settings)
    run_worker(WORKERS["processor"], lambda events: process_events(processor, events), settings)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
