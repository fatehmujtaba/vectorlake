from __future__ import annotations

import time
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from botocore.exceptions import ClientError

from vectorlake.common.resources import IMAGE_TABLE, RAW_BUCKET
from vectorlake.events.v1 import events_pb2
from vectorlake.events.v1.codec import serialize_event

DECISION_TABLE = "duplicate_decisions"
MODEL_ROUTE = "resnet18-v1"


class RevocationService:
    """Compensates an active derived decision; no public endpoint is exposed."""

    def __init__(self, ddb: Any, producer: Any) -> None:
        self.ddb = ddb
        self.producer = producer

    def _load(self, table: str, image_id: str) -> dict[str, Any]:
        item = self.ddb.get_item(
            TableName=table,
            Key={"image_id": {"S": image_id}},
            ConsistentRead=True,
        ).get("Item")
        if item is None:
            raise ValueError(f"image not found in {table}")
        return dict(item)

    def _publish(self, event: events_pb2.ImageInferenceReadyV1) -> None:
        errors: list[str] = []

        def delivered(error: Any, _message: Any) -> None:
            if error is not None:
                errors.append(str(error))

        topic = "image.inference-ready.v1"
        self.producer.produce(
            topic,
            key=event.context.image_id.encode(),
            value=serialize_event(topic, event),
            on_delivery=delivered,
        )
        remaining = self.producer.flush(10)
        if remaining or errors:
            raise RuntimeError(f"Kafka delivery failed: {errors or remaining}")

    def revoke(self, image_id: str, tenant_id: str, scope_id: str, reason: str) -> bool:
        if not reason.strip():
            raise ValueError("revocation reason is required")
        decision = self._load(DECISION_TABLE, image_id)
        if decision["tenant_id"]["S"] != tenant_id or decision["dedup_scope_id"]["S"] != scope_id:
            raise ValueError("decision scope does not match request")
        if decision["duplicate_class"]["S"] != "DERIVED":
            raise ValueError("only derived duplicate decisions may be revoked here")
        status = decision["status"]["S"]
        if status == "REVOKED":
            return False
        if status not in {"ACTIVE", "REVOKING"}:
            raise ValueError(f"decision is not active: {status}")

        state = self._load(IMAGE_TABLE, image_id)
        if (
            state["tenant_id"]["S"] != tenant_id
            or state["dedup_scope_id"]["S"] != scope_id
            or state["bucket"]["S"] != RAW_BUCKET
        ):
            raise ValueError("image scope or source does not match decision")
        old_group = decision["duplicate_group_id"]["S"]
        own_group = decision["own_group_id"]["S"]
        already_unique = (
            state.get("dedup_status", {}).get("S") == "unique"
            and state.get("duplicate_group_id", {}).get("S") == own_group
            and state.get("revoked_decision_id", {}).get("S") == decision["decision_id"]["S"]
        )
        if not already_unique and (
            state.get("dedup_status", {}).get("S") != "duplicate_derived"
            or state.get("duplicate_group_id", {}).get("S") != old_group
        ):
            raise ValueError("image no longer has the derived duplicate decision")

        if status == "ACTIVE":
            try:
                self.ddb.update_item(
                    TableName=DECISION_TABLE,
                    Key={"image_id": {"S": image_id}},
                    ConditionExpression="#status = :active AND dedup_scope_id = :scope",
                    UpdateExpression="SET #status = :revoking, revocation_reason = :reason",
                    ExpressionAttributeNames={"#status": "status"},
                    ExpressionAttributeValues={
                        ":active": {"S": "ACTIVE"},
                        ":scope": {"S": scope_id},
                        ":revoking": {"S": "REVOKING"},
                        ":reason": {"S": reason},
                    },
                )
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                    raise
                current = self._load(DECISION_TABLE, image_id)
                if current["status"]["S"] != "REVOKING":
                    raise ValueError("decision changed during revocation") from exc

        if not already_unique:
            event = events_pb2.ImageInferenceReadyV1(
                context=events_pb2.EventContext(
                    event_id=str(
                        uuid5(NAMESPACE_URL, f"revocation/{decision['decision_id']['S']}")
                    ),
                    image_id=image_id,
                    tenant_id=tenant_id,
                    dedup_scope_id=scope_id,
                    occurred_at_epoch_ms=int(time.time() * 1000),
                    schema_version=1,
                    event_type="image.inference-ready.v1",
                ),
                duplicate_group_id=own_group,
                bucket=RAW_BUCKET,
                object_key=state["object_key"]["S"],
                model_route=MODEL_ROUTE,
            )
            self._publish(event)
            try:
                self.ddb.update_item(
                    TableName=IMAGE_TABLE,
                    Key={"image_id": {"S": image_id}},
                    ConditionExpression=(
                        "tenant_id = :tenant AND dedup_scope_id = :scope AND "
                        "dedup_status = :duplicate AND duplicate_group_id = :old_group"
                    ),
                    UpdateExpression=(
                        "SET dedup_status = :unique, duplicate_group_id = :own_group, "
                        "representative_image_id = :empty, revoked_decision_id = :decision_id"
                    ),
                    ExpressionAttributeValues={
                        ":tenant": {"S": tenant_id},
                        ":scope": {"S": scope_id},
                        ":duplicate": {"S": "duplicate_derived"},
                        ":old_group": {"S": old_group},
                        ":unique": {"S": "unique"},
                        ":own_group": {"S": own_group},
                        ":empty": {"S": ""},
                        ":decision_id": {"S": decision["decision_id"]["S"]},
                    },
                )
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                    raise
                current = self._load(IMAGE_TABLE, image_id)
                if not (
                    current.get("dedup_status", {}).get("S") == "unique"
                    and current.get("duplicate_group_id", {}).get("S") == own_group
                    and current.get("revoked_decision_id", {}).get("S")
                    == decision["decision_id"]["S"]
                ):
                    raise ValueError("image changed during revocation") from exc

        self.ddb.update_item(
            TableName=DECISION_TABLE,
            Key={"image_id": {"S": image_id}},
            ConditionExpression="#status = :revoking AND dedup_scope_id = :scope",
            UpdateExpression="SET #status = :revoked, revoked_at_epoch_ms = :now",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":revoking": {"S": "REVOKING"},
                ":scope": {"S": scope_id},
                ":revoked": {"S": "REVOKED"},
                ":now": {"N": str(int(time.time() * 1000))},
            },
        )
        return True
