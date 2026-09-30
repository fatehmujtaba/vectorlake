from __future__ import annotations

import json
from io import BytesIO
from uuid import uuid4

import numpy as np
import psycopg
from PIL import Image
from skimage import data

from infrastructure.bootstrap import bootstrap
from infrastructure.e2e_m3 import received_event
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.ingestion import UploadService
from vectorlake.processor import EVIDENCE_TABLE, ImageProcessor
from vectorlake.revocation import DECISION_TABLE


def _png(pixels: np.ndarray) -> bytes:
    output = BytesIO()
    Image.fromarray(pixels, "RGB").save(output, format="PNG")
    return output.getvalue()


def verify() -> dict[str, object]:
    stack = bootstrap()
    settings = LocalInfrastructureSettings()
    service = UploadService(settings)
    processor = ImageProcessor(settings)
    tenant_id = f"m4-{uuid4().hex[:12]}"
    other_tenant_id = f"m4-{uuid4().hex[:12]}"
    with psycopg.connect(
        host=stack.postgres_host,
        port=stack.postgres_port,
        dbname=stack.postgres_database,
        user=settings.postgres_username,
        password=settings.postgres_password,
    ) as connection:
        for current_tenant in (tenant_id, other_tenant_id):
            connection.execute(
                "INSERT INTO vectorlake.tenants (tenant_id, name) VALUES (%s, %s)",
                (current_tenant, "M4 integration test"),
            )
            connection.execute(
                "INSERT INTO vectorlake.dedup_scopes (scope_id, tenant_id, name) "
                "VALUES (%s, %s, %s)",
                (current_tenant, current_tenant, "M4 integration test"),
            )
    original_pixels = data.astronaut().copy()  # type: ignore[no-untyped-call]
    original_pixels[0, :16, 0] = np.frombuffer(uuid4().bytes, dtype=np.uint8)
    original = received_event(service, _png(original_pixels), tenant_id)
    first = processor.process(original)
    assert first.dedup_status == "unique"
    assert first.fingerprint is not None

    modified = np.clip(original_pixels.astype(np.int16) + 2, 0, 255).astype(np.uint8)
    near_copy = received_event(service, _png(modified), tenant_id)
    second = processor.process(near_copy)
    assert second.dedup_status == "unique"
    evidence = processor.ddb.get_item(
        TableName=EVIDENCE_TABLE,
        Key={"image_id": {"S": near_copy.context.image_id}},
        ConsistentRead=True,
    )["Item"]
    candidates = json.loads(evidence["candidates_json"]["S"])
    assert any(
        item["candidate_image_id"] == original.context.image_id and item["accepted"]
        for item in candidates
    )
    decision = processor.ddb.get_item(
        TableName=DECISION_TABLE,
        Key={"image_id": {"S": near_copy.context.image_id}},
        ConsistentRead=True,
    )["Item"]
    assert decision["status"]["S"] == "SHADOW"
    assert decision["duplicate_group_id"]["S"] == first.duplicate_group_id

    cross_scope = received_event(service, _png(modified), other_tenant_id)
    cross_scope_outcome = processor.process(cross_scope)
    cross_scope_evidence = processor.ddb.get_item(
        TableName=EVIDENCE_TABLE,
        Key={"image_id": {"S": cross_scope.context.image_id}},
        ConsistentRead=True,
    )["Item"]
    assert cross_scope_outcome.dedup_status == "unique"
    assert json.loads(cross_scope_evidence["candidates_json"]["S"]) == []
    return {
        "original_status": first.dedup_status,
        "near_copy_status": second.dedup_status,
        "shadow_match_count": len(candidates),
        "shadow_accepted": sum(bool(item["accepted"]) for item in candidates),
        "fuzzy_suppression_enabled": False,
        "decision_status": decision["status"]["S"],
        "cross_scope_candidates": 0,
    }


if __name__ == "__main__":
    print(json.dumps(verify(), indent=2))
