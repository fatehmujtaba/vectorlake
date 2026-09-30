from __future__ import annotations

import json
import time
from io import BytesIO
from uuid import uuid4

from confluent_kafka import Consumer, Producer
from PIL import Image

from infrastructure.bootstrap import bootstrap
from vectorlake.common.aws import aws_client
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.common.resources import IMAGE_TABLE, RAW_BUCKET
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import deserialize_event
from vectorlake.revocation import DECISION_TABLE, RevocationService


def verify() -> dict[str, object]:
    bootstrap()
    settings = LocalInfrastructureSettings()
    ddb = aws_client("dynamodb", settings)
    s3 = aws_client("s3", settings)
    image_id = f"revoke-{uuid4().hex}"
    decision_id = f"decision-{uuid4().hex}"
    old_group = f"group-{uuid4().hex}"
    own_group = f"group-{uuid4().hex}"
    key = f"revocation-e2e/{image_id}/source"
    data = BytesIO()
    Image.new("RGB", (16, 16), (45, 85, 125)).save(data, format="PNG")
    s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=data.getvalue(), ContentType="image/png")

    ddb.put_item(
        TableName=IMAGE_TABLE,
        Item={
            "image_id": {"S": image_id},
            "tenant_id": {"S": "local-dev"},
            "dedup_scope_id": {"S": "local-dev"},
            "bucket": {"S": RAW_BUCKET},
            "object_key": {"S": key},
            "validation_status": {"S": "valid"},
            "dedup_status": {"S": "duplicate_derived"},
            "duplicate_group_id": {"S": old_group},
        },
        ConditionExpression="attribute_not_exists(image_id)",
    )
    ddb.put_item(
        TableName=DECISION_TABLE,
        Item={
            "image_id": {"S": image_id},
            "decision_id": {"S": decision_id},
            "tenant_id": {"S": "local-dev"},
            "dedup_scope_id": {"S": "local-dev"},
            "status": {"S": "ACTIVE"},
            "duplicate_class": {"S": "DERIVED"},
            "duplicate_group_id": {"S": old_group},
            "own_group_id": {"S": own_group},
        },
        ConditionExpression="attribute_not_exists(image_id)",
    )

    revoker = RevocationService(
        ddb, Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    )
    assert revoker.revoke(image_id, "local-dev", "local-dev", "M4 compensation drill")
    assert not revoker.revoke(image_id, "local-dev", "local-dev", "M4 compensation drill")
    state = ddb.get_item(TableName=IMAGE_TABLE, Key={"image_id": {"S": image_id}})["Item"]
    decision = ddb.get_item(TableName=DECISION_TABLE, Key={"image_id": {"S": image_id}})["Item"]
    assert state["dedup_status"]["S"] == "unique"
    assert state["duplicate_group_id"]["S"] == own_group
    assert state["revoked_decision_id"]["S"] == decision_id
    assert decision["status"]["S"] == "REVOKED"
    assert s3.head_object(Bucket=RAW_BUCKET, Key=key)["ContentLength"] == len(data.getvalue())

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"vectorlake-revocation-check-{uuid4().hex}",
            "auto.offset.reset": "earliest",
        }
    )
    found = False
    try:
        consumer.subscribe(["image.inference-ready.v1"])
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is None or message.error():
                continue
            payload = message.value()
            if payload is None:
                continue
            event = deserialize_event("image.inference-ready.v1", payload)
            if (
                isinstance(event, events_pb2.ImageInferenceReadyV1)
                and event.context.image_id == image_id
            ):
                assert event.duplicate_group_id == own_group
                found = True
                break
    finally:
        consumer.close()
    assert found, "compensation event was not found"
    return {"decision_status": "REVOKED", "image_status": "unique", "event_found": found}


if __name__ == "__main__":
    print(json.dumps(verify(), indent=2))
