from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import psycopg
from botocore.exceptions import ClientError
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]

from infrastructure.config import LocalInfrastructureSettings
from vectorlake.common.aws import aws_client
from vectorlake.common.resources import (
    ARCHIVE_BUCKET,
    OBJECT_DLQ,
    OBJECT_QUEUE,
    RAW_BUCKET,
    SMOKE_TOPIC,
    WORKER_DLQ_BUCKET,
    WORKER_RETRY_TABLE,
)
from vectorlake.events.v1.codec import EVENT_TYPES

DDB_TABLES = {
    "image_state": "image_id",
    "exact_hash_registry": "PK",
    "pixel_hash_registry": "PK",
    "duplicate_groups": "PK",
    "request_idempotency": "PK",
    "near_copy_evidence": "image_id",
    "duplicate_decisions": "image_id",
    WORKER_RETRY_TABLE: "PK",
}
PDQ_TABLE = "pdq_fingerprints"


@dataclass(frozen=True)
class BootstrapResult:
    aws_endpoint_url: str
    kafka_bootstrap_servers: str
    postgres_host: str
    postgres_port: int
    postgres_database: str


def _error_code(exc: ClientError) -> str:
    return str(exc.response["Error"]["Code"])


def _wait_for(description: str, fetch: Any, timeout_seconds: int = 180) -> Any:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        result = fetch()
        if result is not None:
            return result
        time.sleep(2)
    raise TimeoutError(f"timed out waiting for {description}")


def _ensure_buckets(s3: Any) -> None:
    existing = {bucket["Name"] for bucket in s3.list_buckets()["Buckets"]}
    for name in (RAW_BUCKET, ARCHIVE_BUCKET, WORKER_DLQ_BUCKET):
        if name not in existing:
            s3.create_bucket(Bucket=name)


def _queue_url(sqs: Any, name: str, attributes: dict[str, str] | None = None) -> str:
    try:
        return str(sqs.get_queue_url(QueueName=name)["QueueUrl"])
    except ClientError as exc:
        if _error_code(exc) not in {"AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist"}:
            raise
    return str(sqs.create_queue(QueueName=name, Attributes=attributes or {})["QueueUrl"])


def _ensure_queues(sqs: Any) -> None:
    dlq_url = _queue_url(sqs, OBJECT_DLQ)
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    redrive = json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "5"})
    queue_url = _queue_url(sqs, OBJECT_QUEUE, {"RedrivePolicy": redrive})
    sqs.set_queue_attributes(QueueUrl=queue_url, Attributes={"RedrivePolicy": redrive})


def _ensure_notifications(s3: Any, sqs: Any) -> None:
    queue_url = sqs.get_queue_url(QueueName=OBJECT_QUEUE)["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    s3.put_bucket_versioning(
        Bucket=RAW_BUCKET,
        VersioningConfiguration={"Status": "Enabled"},
    )
    s3.put_bucket_notification_configuration(
        Bucket=RAW_BUCKET,
        NotificationConfiguration={
            "QueueConfigurations": [
                {
                    "Id": "vectorlake-object-created",
                    "QueueArn": queue_arn,
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {"Key": {"FilterRules": [{"Name": "prefix", "Value": "tenants/"}]}},
                }
            ]
        },
    )


def _ensure_tables(ddb: Any) -> None:
    existing = set(ddb.list_tables()["TableNames"])
    for table_name, key_name in DDB_TABLES.items():
        if table_name not in existing:
            ddb.create_table(
                TableName=table_name,
                AttributeDefinitions=[{"AttributeName": key_name, "AttributeType": "S"}],
                KeySchema=[{"AttributeName": key_name, "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
    if PDQ_TABLE not in existing:
        ddb.create_table(
            TableName=PDQ_TABLE,
            AttributeDefinitions=[
                {"AttributeName": "dedup_scope_id", "AttributeType": "S"},
                {"AttributeName": "image_id", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "dedup_scope_id", "KeyType": "HASH"},
                {"AttributeName": "image_id", "KeyType": "RANGE"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )


def _find_rds_instance(rds: Any, identifier: str) -> Any:
    instances = rds.describe_db_instances()["DBInstances"]
    return next((db for db in instances if db["DBInstanceIdentifier"] == identifier), None)


def _ensure_postgres(rds: Any, settings: LocalInfrastructureSettings) -> tuple[str, int]:
    if _find_rds_instance(rds, settings.postgres_instance_id) is None:
        rds.create_db_instance(
            DBInstanceIdentifier=settings.postgres_instance_id,
            DBInstanceClass="db.t3.micro",
            Engine="postgres",
            MasterUsername=settings.postgres_username,
            MasterUserPassword=settings.postgres_password,
            DBName=settings.postgres_database,
            AllocatedStorage=20,
        )

    def ready_instance() -> Any:
        instance = _find_rds_instance(rds, settings.postgres_instance_id)
        if (
            instance
            and instance.get("DBInstanceStatus") == "available"
            and instance.get("Endpoint")
        ):
            return instance
        return None

    instance = _wait_for("PostgreSQL instance", ready_instance, timeout_seconds=240)
    endpoint = instance["Endpoint"]
    host, port = str(endpoint["Address"]), int(endpoint["Port"])
    sql = Path(__file__).with_name("sql").joinpath("001_control_plane.sql").read_text()

    def initialize_schema() -> bool | None:
        try:
            with psycopg.connect(
                host=host,
                port=port,
                dbname=settings.postgres_database,
                user=settings.postgres_username,
                password=settings.postgres_password,
                connect_timeout=5,
            ) as connection:
                connection.execute(sql)
            return True
        except psycopg.OperationalError:
            return None

    _wait_for("PostgreSQL connection", initialize_schema, timeout_seconds=120)
    return host, port


def _ensure_topics(bootstrap_servers: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers, "socket.timeout.ms": 10000})
    metadata = _wait_for(
        "Kafka broker",
        lambda: admin.list_topics(timeout=5),
        timeout_seconds=90,
    )
    existing = set(metadata.topics)
    names = set(EVENT_TYPES) | {SMOKE_TOPIC}
    missing = [NewTopic(name, num_partitions=3, replication_factor=1) for name in names - existing]
    if missing:
        for future in admin.create_topics(missing, operation_timeout=30).values():
            future.result(timeout=30)


def bootstrap(settings: LocalInfrastructureSettings | None = None) -> BootstrapResult:
    settings = settings or LocalInfrastructureSettings()
    s3 = aws_client("s3", settings)
    sqs = aws_client("sqs", settings)
    _ensure_buckets(s3)
    _ensure_queues(sqs)
    _ensure_notifications(s3, sqs)
    _ensure_tables(aws_client("dynamodb", settings))
    postgres_host, postgres_port = _ensure_postgres(aws_client("rds", settings), settings)
    brokers = settings.kafka_bootstrap_servers
    _ensure_topics(brokers)
    return BootstrapResult(
        aws_endpoint_url=settings.aws_endpoint_url,
        kafka_bootstrap_servers=brokers,
        postgres_host=postgres_host,
        postgres_port=postgres_port,
        postgres_database=settings.postgres_database,
    )


if __name__ == "__main__":
    print(json.dumps(asdict(bootstrap()), indent=2))
