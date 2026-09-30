from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from vectorlake.near_copy import DEFAULT_POLICY, Fingerprint, NearCopyPolicy, hamming

PDQ_TABLE = "pdq_fingerprints"


@dataclass(frozen=True)
class Candidate:
    image_id: str
    distance: int


class ScopedPDQIndex:
    """Bounded local adapter; use an indexed binary-vector backend at production scale."""

    def __init__(
        self,
        ddb: Any,
        *,
        max_scope_entries: int = 2048,
        top_k: int = 5,
        policy: NearCopyPolicy = DEFAULT_POLICY,
    ) -> None:
        self.ddb = ddb
        self.max_scope_entries = max_scope_entries
        self.top_k = top_k
        self.policy = policy

    def find(self, scope_id: str, image_id: str, query: Fingerprint) -> list[Candidate]:
        if query.quality < self.policy.min_pdq_quality:
            return []
        items: list[dict[str, Any]] = []
        cursor: dict[str, Any] | None = None
        while True:
            previous_cursor = cursor
            previous_count = len(items)
            request: dict[str, Any] = {
                "TableName": PDQ_TABLE,
                "KeyConditionExpression": "dedup_scope_id = :scope",
                "ExpressionAttributeValues": {":scope": {"S": scope_id}},
                "ConsistentRead": True,
                "Limit": self.max_scope_entries + 1 - len(items),
            }
            if cursor is not None:
                request["ExclusiveStartKey"] = cursor
            response = self.ddb.query(**request)
            items.extend(response.get("Items", []))
            cursor = response.get("LastEvaluatedKey")
            if len(items) > self.max_scope_entries or (
                cursor
                and (
                    len(items) >= self.max_scope_entries
                    or len(items) == previous_count
                    or cursor == previous_cursor
                )
            ):
                return []
            if not cursor:
                break
        candidates = [
            Candidate(item["image_id"]["S"], hamming(query.hex_value, item["pdq_hex"]["S"]))
            for item in items
            if item["image_id"]["S"] != image_id
            and int(item["quality"]["N"]) >= self.policy.min_pdq_quality
        ]
        return sorted(
            (
                candidate
                for candidate in candidates
                if candidate.distance <= self.policy.max_pdq_distance
            ),
            key=lambda candidate: (candidate.distance, candidate.image_id),
        )[: self.top_k]

    def add(self, scope_id: str, image_id: str, value: Fingerprint) -> None:
        if value.quality < self.policy.min_pdq_quality:
            return
        self.ddb.put_item(
            TableName=PDQ_TABLE,
            Item={
                "dedup_scope_id": {"S": scope_id},
                "image_id": {"S": image_id},
                "pdq_hex": {"S": value.hex_value},
                "quality": {"N": str(value.quality)},
            },
        )
