from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast
from uuid import uuid4

from confluent_kafka import Consumer

from infrastructure.e2e_m5_inference import verify as verify_inference
from vectorlake.archive import ArchiveWriter
from vectorlake.archive_rebuild import rebuild_index
from vectorlake.common.aws import aws_client
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.embedding import MODEL_VERSION, PREPROCESS_VERSION
from vectorlake.embedding_indexer import EmbeddingIndexer
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import deserialize_event
from vectorlake.semantic_index import SemanticIndex


def _find(topic: str, image_id: str) -> object:
    settings = LocalInfrastructureSettings()
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"vectorlake-m5-archive-verifier-{uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    try:
        consumer.subscribe([topic])
        for _ in range(60):
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            payload = message.value()
            if payload is None:
                raise ValueError("event has no payload")
            event = deserialize_event(topic, payload)
            if cast(Any, event).context.image_id == image_id:
                return event
    finally:
        consumer.close()
    raise AssertionError(f"event not found: {topic} / {image_id}")


def verify() -> dict[str, object]:
    inference = verify_inference()
    image_id = str(inference["image_id"])
    generated = _find("embedding.generated.v1", image_id)
    assert isinstance(generated, events_pb2.EmbeddingGeneratedV1)
    settings = LocalInfrastructureSettings()
    writer = ArchiveWriter(settings)
    archived = writer.write_batch([generated])[0]
    replay = writer.write_batch([generated])[0]
    assert replay == archived
    assert _find("embedding.archived.v1", image_id) == archived

    with TemporaryDirectory(prefix="vectorlake-m5-") as directory:
        live = SemanticIndex(str(Path(directory) / "live.db"))
        try:
            indexed = EmbeddingIndexer(live, settings).process(generated)
            assert _find("image.indexed.v1", image_id) == indexed
            hits = live.search(
                generated.vector,
                tenant_id=generated.context.tenant_id,
                dedup_scope_id=generated.context.dedup_scope_id,
                model_version=MODEL_VERSION,
                preprocess_version=PREPROCESS_VERSION,
            )
            assert any(hit.image_id == image_id for hit in hits)
        finally:
            live.close()

        rebuilt = SemanticIndex(str(Path(directory) / "rebuilt.db"))
        try:
            count = rebuild_index(aws_client("s3", settings), rebuilt)
            rebuilt_hits = rebuilt.search(
                generated.vector,
                tenant_id=generated.context.tenant_id,
                dedup_scope_id=generated.context.dedup_scope_id,
                model_version=MODEL_VERSION,
                preprocess_version=PREPROCESS_VERSION,
            )
            assert any(hit.image_id == image_id for hit in rebuilt_hits)
        finally:
            rebuilt.close()
    return {
        "image_id": image_id,
        "archive_key": archived.archive_key,
        "replay_same_event": replay == archived,
        "indexed_event_found": True,
        "rebuilt_rows": count,
        "rebuilt_hit": True,
    }


if __name__ == "__main__":
    print(json.dumps(verify(), indent=2))
