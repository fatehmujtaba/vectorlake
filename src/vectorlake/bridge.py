from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from urllib.parse import unquote_plus
from uuid import NAMESPACE_URL, uuid5

from confluent_kafka import Producer

from vectorlake.common.aws import aws_client
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.common.resources import IMAGE_TABLE, OBJECT_QUEUE, RAW_BUCKET
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import serialize_event

TOPIC = "image.received.v1"


class EventBridge:
    def __init__(
        self,
        settings: LocalInfrastructureSettings | None = None,
        producer: Any | None = None,
    ) -> None:
        self.settings = settings or LocalInfrastructureSettings()
        self.sqs = aws_client("sqs", self.settings)
        self.ddb = aws_client("dynamodb", self.settings)
        self.s3 = aws_client("s3", self.settings)
        self.producer = producer or Producer(
            {"bootstrap.servers": self.settings.kafka_bootstrap_servers}
        )
        self.queue_url = self.sqs.get_queue_url(QueueName=OBJECT_QUEUE)["QueueUrl"]

    def _event_from_record(self, record: dict[str, Any]) -> events_pb2.ImageReceivedV1:
        if not str(record.get("eventName", "")).startswith("ObjectCreated:"):
            raise ValueError("unexpected S3 event type")
        bucket = record["s3"]["bucket"]["name"]
        object_data = record["s3"]["object"]
        key = unquote_plus(object_data["key"])
        parts = key.split("/")
        if (
            bucket != RAW_BUCKET
            or len(parts) != 5
            or parts[0] != "tenants"
            or parts[2] != "images"
            or parts[4] != "source"
        ):
            raise ValueError("S3 event is outside an upload intent")
        tenant_id, image_id = parts[1], parts[3]
        image = self.ddb.get_item(
            TableName=IMAGE_TABLE,
            Key={"image_id": {"S": image_id}},
            ConsistentRead=True,
        ).get("Item")
        if (
            image is None
            or image["tenant_id"]["S"] != tenant_id
            or image["bucket"]["S"] != bucket
            or image["object_key"]["S"] != key
        ):
            raise ValueError("S3 event has no matching upload intent")
        version = str(object_data.get("versionId", ""))
        head_kwargs = {"Bucket": bucket, "Key": key}
        if version:
            head_kwargs["VersionId"] = version
        head = self.s3.head_object(**head_kwargs)
        if head["ContentType"] != image["content_type"]["S"]:
            raise ValueError("uploaded content type differs from intent")
        if int(head["ContentLength"]) != int(image["expected_size_bytes"]["N"]):
            raise ValueError("uploaded size differs from intent")
        event_id = str(uuid5(NAMESPACE_URL, f"s3://{bucket}/{key}?versionId={version}"))
        occurred_at_ms = (
            int(
                datetime.fromisoformat(record["eventTime"].replace("Z", "+00:00")).timestamp()
                * 1000
            )
            if record.get("eventTime")
            else int(image["created_at_epoch_s"]["N"]) * 1000
        )
        return events_pb2.ImageReceivedV1(
            context=events_pb2.EventContext(
                event_id=event_id,
                image_id=image_id,
                tenant_id=tenant_id,
                dedup_scope_id=image["dedup_scope_id"]["S"],
                occurred_at_epoch_ms=occurred_at_ms,
                traceparent=image.get("traceparent", {}).get("S", ""),
                schema_version=1,
                event_type=TOPIC,
            ),
            bucket=bucket,
            object_key=key,
            object_version=version,
            size_bytes=int(head["ContentLength"]),
            upload_intent_id=image["upload_intent_id"]["S"],
        )

    def _publish(self, event: events_pb2.ImageReceivedV1) -> None:
        errors: list[str] = []

        def delivered(error: Any, _message: Any) -> None:
            if error is not None:
                errors.append(str(error))

        self.producer.produce(
            TOPIC,
            key=event.context.image_id.encode(),
            value=serialize_event(TOPIC, event),
            on_delivery=delivered,
        )
        remaining = self.producer.flush(10)
        if remaining or errors:
            raise RuntimeError(f"Kafka delivery failed: {errors or remaining}")

    def process_message(self, message: dict[str, Any]) -> list[str]:
        body = json.loads(message["Body"])
        records = body["Records"]
        if not records:
            raise ValueError("S3 notification has no records")
        events = [self._event_from_record(record) for record in records]
        for event in events:
            self._publish(event)
            self.ddb.update_item(
                TableName=IMAGE_TABLE,
                Key={"image_id": {"S": event.context.image_id}},
                UpdateExpression="SET ingestion_status = :status, object_version = :version",
                ExpressionAttributeValues={
                    ":status": {"S": "received"},
                    ":version": {"S": event.object_version},
                },
            )
        self.sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=message["ReceiptHandle"])
        return [event.context.event_id for event in events]

    def poll_once(self, wait_seconds: int = 1) -> list[str]:
        messages = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=wait_seconds,
        ).get("Messages", [])
        if not messages:
            return []
        return self.process_message(messages[0])
