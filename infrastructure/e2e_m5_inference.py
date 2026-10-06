from __future__ import annotations

import json
import math
from uuid import uuid4

from confluent_kafka import Consumer

from infrastructure.bootstrap import bootstrap
from infrastructure.e2e_m3 import png, received_event
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.common.resources import IMAGE_TABLE
from vectorlake.embedding import TorchvisionResNet18EmbeddingEngine
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import deserialize_event
from vectorlake.inference import InferenceService
from vectorlake.ingestion import UploadService
from vectorlake.processor import ImageProcessor


def verify() -> dict[str, object]:
    stack = bootstrap()
    settings = LocalInfrastructureSettings()
    uploads = UploadService(settings)
    processor = ImageProcessor(settings)
    original = received_event(
        uploads,
        png("RGB", pixels=uuid4().bytes * 3),
        capture_id=uuid4().hex,
    )
    outcome = processor.process(original)
    assert outcome.topic == "image.inference-ready.v1"
    assert isinstance(outcome.message, events_pb2.ImageInferenceReadyV1)
    inference = InferenceService(TorchvisionResNet18EmbeddingEngine(), settings)
    produced = inference.process_batch([outcome.message])
    assert len(produced) == 1
    generated = produced[0]
    assert generated.dimension == 512
    assert inference.process_batch([outcome.message]) == []
    state = uploads.ddb.get_item(
        TableName=IMAGE_TABLE,
        Key={"image_id": {"S": original.context.image_id}},
        ConsistentRead=True,
    )["Item"]
    assert state["embedding_generated_id"]["S"] == generated.embedding_id

    consumer = Consumer(
        {
            "bootstrap.servers": stack.kafka_bootstrap_servers,
            "group.id": f"vectorlake-m5-verifier-{uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    found = False
    try:
        consumer.subscribe(["embedding.generated.v1"])
        for _ in range(30):
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            payload = message.value()
            assert payload is not None
            decoded = deserialize_event("embedding.generated.v1", payload)
            if decoded == generated:
                found = True
                break
    finally:
        consumer.close()
    assert found, "generated embedding was not observed in Kafka"
    norm = math.sqrt(sum(value * value for value in generated.vector))
    return {
        "image_id": original.context.image_id,
        "embedding_id": generated.embedding_id,
        "dimension": generated.dimension,
        "norm": norm,
        "event_found": found,
        "replay_skipped": True,
    }


if __name__ == "__main__":
    print(json.dumps(verify(), indent=2))
