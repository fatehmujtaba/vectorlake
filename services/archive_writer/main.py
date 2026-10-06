from __future__ import annotations

import logging

from google.protobuf.message import Message

from vectorlake.archive import ArchiveWriter
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.events.v1 import events_pb2
from vectorlake.worker import require_worker_rollout, run_worker
from vectorlake.worker_retry import WORKERS


def process_events(writer: ArchiveWriter, events: list[Message]) -> None:
    partitions: dict[tuple[str, int], list[events_pb2.EmbeddingGeneratedV1]] = {}
    for event in events:
        if not isinstance(event, events_pb2.EmbeddingGeneratedV1):
            raise TypeError("unexpected archive event type")
        partition = (event.model_version, event.context.occurred_at_epoch_ms // 86_400_000)
        partitions.setdefault(partition, []).append(event)
    for batch in partitions.values():
        writer.write_batch(batch)


def main() -> None:
    settings = LocalInfrastructureSettings()
    require_worker_rollout(settings)
    writer = ArchiveWriter(settings)
    run_worker(WORKERS["archive"], lambda events: process_events(writer, events), settings)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
