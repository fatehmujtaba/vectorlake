from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import psycopg
from botocore.exceptions import ClientError

from vectorlake.common.aws import aws_client
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.common.resources import IDEMPOTENCY_TABLE, IMAGE_TABLE, RAW_BUCKET

UPLOAD_URL_SECONDS = 900


class IdempotencyConflict(Exception):
    pass


@dataclass(frozen=True)
class UploadSession:
    image_id: str
    upload_url: str
    upload_headers: dict[str, str]
    expires_in_seconds: int
    status: str


class UploadService:
    def __init__(self, settings: LocalInfrastructureSettings | None = None) -> None:
        self.settings = settings or LocalInfrastructureSettings()
        self.ddb = aws_client("dynamodb", self.settings)
        self.s3 = aws_client("s3", self.settings)

    def _scope(self, tenant_id: str) -> str:
        with psycopg.connect(
            host=self.settings.postgres_host,
            port=self.settings.postgres_port,
            dbname=self.settings.postgres_database,
            user=self.settings.postgres_username,
            password=self.settings.postgres_password,
        ) as connection:
            row = connection.execute(
                "SELECT scope_id FROM vectorlake.dedup_scopes WHERE tenant_id = %s "
                "ORDER BY scope_id LIMIT 1",
                (tenant_id,),
            ).fetchone()
        if row is None:
            raise LookupError("tenant has no deduplication scope")
        return str(row[0])

    def create_session(
        self,
        tenant_id: str,
        idempotency_key: str,
        content_type: str,
        size_bytes: int,
        traceparent: str = "",
    ) -> UploadSession:
        scope_id = self._scope(tenant_id)
        request_hash = hashlib.sha256(
            json.dumps(
                {"content_type": content_type, "size_bytes": size_bytes},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        idempotency_pk = f"IDEMPOTENCY#{tenant_id}#{idempotency_key}"
        image_id = str(uuid4())
        intent_id = str(uuid4())
        object_key = f"tenants/{tenant_id}/images/{image_id}/source"
        now = int(time.time())
        intent = {
            "PK": {"S": idempotency_pk},
            "request_hash": {"S": request_hash},
            "image_id": {"S": image_id},
            "upload_intent_id": {"S": intent_id},
            "expires_at": {"N": str(now + 86_400)},
        }
        image = {
            "image_id": {"S": image_id},
            "tenant_id": {"S": tenant_id},
            "dedup_scope_id": {"S": scope_id},
            "bucket": {"S": RAW_BUCKET},
            "object_key": {"S": object_key},
            "upload_intent_id": {"S": intent_id},
            "content_type": {"S": content_type},
            "expected_size_bytes": {"N": str(size_bytes)},
            "ingestion_status": {"S": "pending_upload"},
            "created_at_epoch_s": {"N": str(now)},
        }
        if traceparent:
            image["traceparent"] = {"S": traceparent}
        try:
            self.ddb.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": IDEMPOTENCY_TABLE,
                            "Item": intent,
                            "ConditionExpression": "attribute_not_exists(PK)",
                        }
                    },
                    {
                        "Put": {
                            "TableName": IMAGE_TABLE,
                            "Item": image,
                            "ConditionExpression": "attribute_not_exists(image_id)",
                        }
                    },
                ]
            )
        except ClientError:
            saved = self.ddb.get_item(
                TableName=IDEMPOTENCY_TABLE,
                Key={"PK": {"S": idempotency_pk}},
                ConsistentRead=True,
            ).get("Item")
            if saved is None:
                raise
            if saved["request_hash"]["S"] != request_hash:
                raise IdempotencyConflict from None
            image_id = saved["image_id"]["S"]
            image = self.ddb.get_item(
                TableName=IMAGE_TABLE,
                Key={"image_id": {"S": image_id}},
                ConsistentRead=True,
            )["Item"]
            object_key = image["object_key"]["S"]

        upload_url = self.s3.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": RAW_BUCKET,
                "Key": object_key,
                "ContentType": content_type,
                "IfNoneMatch": "*",
            },
            ExpiresIn=UPLOAD_URL_SECONDS,
            HttpMethod="PUT",
        )
        return UploadSession(
            image_id=image_id,
            upload_url=upload_url,
            upload_headers={"Content-Type": content_type, "If-None-Match": "*"},
            expires_in_seconds=UPLOAD_URL_SECONDS,
            status=image["ingestion_status"]["S"],
        )

    def get_status(self, tenant_id: str, image_id: str) -> dict[str, Any] | None:
        image = self.ddb.get_item(
            TableName=IMAGE_TABLE,
            Key={"image_id": {"S": image_id}},
            ConsistentRead=True,
        ).get("Item")
        if image is None or image["tenant_id"]["S"] != tenant_id:
            return None
        status: dict[str, Any] = {
            "image_id": image_id,
            "status": image["ingestion_status"]["S"],
        }
        for name in ("validation_status", "dedup_status"):
            if name in image:
                status[name] = image[name]["S"]
        return status
