from __future__ import annotations

import json
import time
from uuid import uuid4

import psycopg
from botocore.exceptions import ClientError
from confluent_kafka import Consumer, Producer

from infrastructure.bootstrap import bootstrap
from infrastructure.config import LocalInfrastructureSettings
from vectorlake.common.aws import aws_client
from vectorlake.common.resources import RAW_BUCKET, SMOKE_TOPIC


def smoke() -> dict[str, str]:
    settings = LocalInfrastructureSettings()
    stack = bootstrap(settings)
    marker = uuid4().hex
    results: dict[str, str] = {}

    s3 = aws_client("s3", settings)
    object_key = f"smoke/{marker}"
    payload = marker.encode()
    s3.put_object(Bucket=RAW_BUCKET, Key=object_key, Body=payload)
    try:
        assert s3.get_object(Bucket=RAW_BUCKET, Key=object_key)["Body"].read() == payload
        results["s3"] = "put/get"
    finally:
        s3.delete_object(Bucket=RAW_BUCKET, Key=object_key)

    sqs = aws_client("sqs", settings)
    queue_url = sqs.create_queue(QueueName=f"vectorlake-smoke-{marker[:16]}")["QueueUrl"]
    try:
        sqs.send_message(QueueUrl=queue_url, MessageBody=marker)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            messages = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=1).get(
                "Messages", []
            )
            for message in messages:
                if message["Body"] == marker:
                    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])
                    results["sqs"] = "send/receive/delete"
                    break
            if "sqs" in results:
                break
        assert "sqs" in results, "SQS smoke message was not received"
    finally:
        sqs.delete_queue(QueueUrl=queue_url)

    ddb = aws_client("dynamodb", settings)
    item = {"PK": {"S": f"smoke#{marker}"}}
    ddb.put_item(
        TableName="request_idempotency",
        Item=item,
        ConditionExpression="attribute_not_exists(PK)",
    )
    try:
        assert ddb.get_item(TableName="request_idempotency", Key=item)["Item"] == item
        try:
            ddb.put_item(
                TableName="request_idempotency",
                Item=item,
                ConditionExpression="attribute_not_exists(PK)",
            )
        except ClientError as exc:
            assert exc.response["Error"]["Code"] == "ConditionalCheckFailedException"
        else:
            raise AssertionError("DynamoDB accepted a duplicate conditional claim")
        results["dynamodb"] = "conditional claim/rejection"
    finally:
        ddb.delete_item(TableName="request_idempotency", Key=item)

    with psycopg.connect(
        host=stack.postgres_host,
        port=stack.postgres_port,
        dbname=stack.postgres_database,
        user=settings.postgres_username,
        password=settings.postgres_password,
    ) as connection:
        row = connection.execute(
            "SELECT tenant_id FROM vectorlake.tenants WHERE tenant_id = %s", ("local-dev",)
        ).fetchone()
        assert row == ("local-dev",)
        results["postgres"] = "schema/query"

    consumer = Consumer(
        {
            "bootstrap.servers": stack.kafka_bootstrap_servers,
            "group.id": f"vectorlake-smoke-{marker}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    try:
        consumer.subscribe([SMOKE_TOPIC])
        producer = Producer({"bootstrap.servers": stack.kafka_bootstrap_servers})
        producer.produce(SMOKE_TOPIC, key=marker, value=payload)
        assert producer.flush(10) == 0, "Kafka producer did not flush"
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            if message.key() == payload and message.value() == payload:
                results["kafka"] = "produce/consume"
                break
        assert "kafka" in results, "Kafka smoke message was not consumed"
    finally:
        consumer.close()

    return results


if __name__ == "__main__":
    print(json.dumps(smoke(), indent=2))
