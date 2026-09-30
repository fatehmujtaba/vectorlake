from __future__ import annotations

import json
import time
from io import BytesIO
from uuid import uuid4

from confluent_kafka import Consumer, Producer
from PIL import Image

from infrastructure.bootstrap import bootstrap
from infrastructure.e2e_m3 import received_event
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import deserialize_event, serialize_event
from vectorlake.ingestion import UploadService


def verify() -> dict[str, str]:
    stack = bootstrap()
    settings = LocalInfrastructureSettings()
    service = UploadService(settings)
    output = BytesIO()
    Image.frombytes("RGB", (4, 4), uuid4().bytes * 3).save(output, format="PNG")
    valid = received_event(service, output.getvalue())
    malformed = received_event(service, b"\x89PNG\r\n\x1a\nnot a PNG")
    after_output = BytesIO()
    Image.frombytes("RGB", (4, 4), uuid4().bytes * 3).save(after_output, format="PNG")
    valid_after = received_event(service, after_output.getvalue())

    producer = Producer({"bootstrap.servers": stack.kafka_bootstrap_servers})
    for event in (valid, malformed, valid_after):
        producer.produce(
            "image.received.v1",
            key=event.context.image_id,
            value=serialize_event("image.received.v1", event),
        )
    assert producer.flush(10) == 0

    expected = {
        valid.context.image_id: "unique",
        malformed.context.image_id: "not_evaluated",
        valid_after.context.image_id: "unique",
    }
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        statuses = {image_id: service.get_status("local-dev", image_id) for image_id in expected}
        if all(
            status is not None and status.get("dedup_status") == expected[image_id]
            for image_id, status in statuses.items()
        ):
            break
        time.sleep(0.5)
    else:
        raise AssertionError(f"worker did not project expected states: {statuses}")

    consumer = Consumer(
        {
            "bootstrap.servers": stack.kafka_bootstrap_servers,
            "group.id": f"vectorlake-m3-verifier-{uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    found: dict[str, str] = {}
    try:
        consumer.subscribe(["image.inference-ready.v1", "processing.failed.v1"])
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and len(found) < len(expected):
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            value = message.value()
            assert value is not None
            topic = message.topic()
            assert topic is not None
            decoded = deserialize_event(topic, value)
            if (
                isinstance(decoded, events_pb2.ImageInferenceReadyV1)
                and decoded.context.image_id in expected
            ):
                found[decoded.context.image_id] = topic
            elif (
                isinstance(decoded, events_pb2.ProcessingFailedV1)
                and decoded.context.image_id in expected
            ):
                assert decoded.retryable is False
                found[decoded.context.image_id] = topic
        assert found.get(valid.context.image_id) == "image.inference-ready.v1"
        assert found.get(malformed.context.image_id) == "processing.failed.v1"
        assert found.get(valid_after.context.image_id) == "image.inference-ready.v1"
    finally:
        consumer.close()

    return {
        "valid_image_id": valid.context.image_id,
        "valid_result": found[valid.context.image_id],
        "malformed_image_id": malformed.context.image_id,
        "malformed_result": found[malformed.context.image_id],
        "valid_after_malformed_result": found[valid_after.context.image_id],
    }


if __name__ == "__main__":
    print(json.dumps(verify(), indent=2))
