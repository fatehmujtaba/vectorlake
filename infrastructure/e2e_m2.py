from __future__ import annotations

import base64
import json
import time
from uuid import uuid4

import httpx
from confluent_kafka import Consumer
from fastapi.testclient import TestClient

from infrastructure.bootstrap import bootstrap
from vectorlake.api import create_app
from vectorlake.bridge import EventBridge
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import deserialize_event

PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/"
    "ScL/nwAAAABJRU5ErkJggg=="
)


def verify() -> dict[str, object]:
    stack = bootstrap()
    settings = LocalInfrastructureSettings()
    bridge = EventBridge(settings)
    headers = {
        "Authorization": f"Bearer {settings.api_token}",
        "Idempotency-Key": uuid4().hex,
    }
    request = {"content_type": "image/png", "size_bytes": len(PNG)}
    with TestClient(create_app()) as api:
        assert (
            api.post(
                "/v1/upload-sessions",
                headers={"Idempotency-Key": headers["Idempotency-Key"]},
                json=request,
            ).status_code
            == 401
        )
        created = api.post("/v1/upload-sessions", headers=headers, json=request)
        assert created.status_code == 201, created.text
        session = created.json()
        replay = api.post("/v1/upload-sessions", headers=headers, json=request)
        assert replay.status_code == 201 and replay.json()["image_id"] == session["image_id"]
        conflict = api.post(
            "/v1/upload-sessions",
            headers=headers,
            json={**request, "size_bytes": len(PNG) + 1},
        )
        assert conflict.status_code == 409, conflict.text
        assert (
            api.get(f"/v1/images/{session['image_id']}", headers=headers).json()["status"]
            == "pending_upload"
        )

        uploaded = httpx.put(
            session["upload_url"], content=PNG, headers=session["upload_headers"], timeout=10
        )
        assert uploaded.status_code == 200, uploaded.text
        overwritten = httpx.put(
            session["upload_url"], content=PNG, headers=session["upload_headers"], timeout=10
        )
        assert overwritten.status_code == 412, overwritten.text

        deadline = time.monotonic() + 30
        notification = None
        first_ids: list[str] = []
        while time.monotonic() < deadline:
            messages = bridge.sqs.receive_message(
                QueueUrl=bridge.queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=1
            ).get("Messages", [])
            if messages:
                ids = bridge.process_message(messages[0])
                if (
                    api.get(f"/v1/images/{session['image_id']}", headers=headers).json()["status"]
                    == "received"
                ):
                    notification = messages[0]
                    first_ids = ids
                    break
        assert notification is not None, "this upload's S3 notification did not reach SQS"
        assert len(first_ids) == 1
        bridge.sqs.send_message(QueueUrl=bridge.queue_url, MessageBody=notification["Body"])
        repeated_ids: list[str] = []
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            repeated_ids = bridge.poll_once(wait_seconds=1)
            if repeated_ids == first_ids:
                break
        assert repeated_ids == first_ids
        assert (
            api.get(f"/v1/images/{session['image_id']}", headers=headers).json()["status"]
            == "received"
        )

    consumer = Consumer(
        {
            "bootstrap.servers": stack.kafka_bootstrap_servers,
            "group.id": f"vectorlake-m2-{uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    received: list[events_pb2.ImageReceivedV1] = []
    received_payloads: list[bytes] = []
    try:
        consumer.subscribe(["image.received.v1"])
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and len(received) < 2:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            value = message.value()
            assert value is not None
            event = deserialize_event("image.received.v1", value)
            assert isinstance(event, events_pb2.ImageReceivedV1)
            if event.context.image_id == session["image_id"]:
                received.append(event)
                received_payloads.append(value)
        assert len(received) == 2
        assert all(event.context.event_id == first_ids[0] for event in received)
        assert received_payloads[0] == received_payloads[1]
    finally:
        consumer.close()

    return {
        "image_id": session["image_id"],
        "event_id": first_ids[0],
        "kafka_deliveries": len(received),
        "status": "received",
    }


if __name__ == "__main__":
    print(json.dumps(verify(), indent=2))
