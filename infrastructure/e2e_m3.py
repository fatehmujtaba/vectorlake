from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from unittest.mock import patch
from uuid import NAMESPACE_URL, uuid4, uuid5

import httpx
import psycopg
from PIL import Image, PngImagePlugin

from infrastructure.bootstrap import bootstrap
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.events.v1 import events_pb2
from vectorlake.ingestion import UploadService
from vectorlake.processor import ImageProcessor


def png(mode: str, *, comment: str = "", pixels: bytes | None = None) -> bytes:
    output = BytesIO()
    image = Image.frombytes(mode, (4, 4), pixels) if pixels is not None else Image.new(mode, (4, 4))
    info = PngImagePlugin.PngInfo()
    if comment:
        info.add_text("Comment", comment)
    image.save(output, format="PNG", pnginfo=info)
    return output.getvalue()


def received_event(
    service: UploadService, data: bytes, tenant_id: str = "local-dev"
) -> events_pb2.ImageReceivedV1:
    session = service.create_session(tenant_id, uuid4().hex, "image/png", len(data))
    response = httpx.put(
        session.upload_url, content=data, headers=session.upload_headers, timeout=10
    )
    assert response.status_code == 200, response.text
    item = service.ddb.get_item(TableName="image_state", Key={"image_id": {"S": session.image_id}})[
        "Item"
    ]
    key = item["object_key"]["S"]
    bucket = item["bucket"]["S"]
    version = service.s3.head_object(Bucket=bucket, Key=key).get("VersionId", "")
    return events_pb2.ImageReceivedV1(
        context=events_pb2.EventContext(
            event_id=str(uuid5(NAMESPACE_URL, f"s3://{bucket}/{key}?versionId={version}")),
            image_id=session.image_id,
            tenant_id=tenant_id,
            dedup_scope_id=item["dedup_scope_id"]["S"],
            occurred_at_epoch_ms=int(time.time() * 1000),
            schema_version=1,
            event_type="image.received.v1",
        ),
        bucket=bucket,
        object_key=key,
        object_version=version,
        size_bytes=len(data),
        upload_intent_id=item["upload_intent_id"]["S"],
    )


def verify() -> dict[str, object]:
    stack = bootstrap()
    settings = LocalInfrastructureSettings()
    service = UploadService(settings)
    processor = ImageProcessor(settings)

    original_pixels = uuid4().bytes * 3
    original_bytes = png("RGB", comment="original", pixels=original_pixels)
    original = received_event(service, original_bytes)
    original_outcome = processor.process(original)
    assert original_outcome.dedup_status == "unique"
    replay_outcome = processor.process(original)
    assert replay_outcome.dedup_status == "unique"
    assert replay_outcome.message.context.event_id == original_outcome.message.context.event_id  # type: ignore[attr-defined]

    exact = received_event(service, original_bytes)
    with patch(
        "vectorlake.processor.canonicalize_image", side_effect=AssertionError("decoded exact copy")
    ):
        exact_outcome = processor.process(exact)
    assert exact_outcome.dedup_status == "duplicate_exact"
    assert exact_outcome.owner_image_id == original.context.image_id

    metadata = received_event(
        service, png("RGB", comment="metadata changed", pixels=original_pixels)
    )
    pixel_outcome = processor.process(metadata)
    assert pixel_outcome.dedup_status == "duplicate_pixel"
    assert pixel_outcome.owner_image_id == original.context.image_id

    malformed = received_event(service, b"\x89PNG\r\n\x1a\nnot a PNG")
    invalid_outcome = processor.process(malformed)
    assert invalid_outcome.validation_status == "invalid"
    assert isinstance(invalid_outcome.message, events_pb2.ProcessingFailedV1)
    assert invalid_outcome.message.retryable is False

    gray_pixels = uuid4().bytes
    gray8 = received_event(service, png("L", pixels=gray_pixels))
    gray8_outcome = processor.process(gray8)
    gray16_data = BytesIO()
    Image.frombytes("I;16", (4, 4), b"".join(bytes((sample, 0)) for sample in gray_pixels)).save(
        gray16_data, format="PNG"
    )
    gray16 = received_event(service, gray16_data.getvalue())
    gray16_outcome = processor.process(gray16)
    assert gray8_outcome.dedup_status == "unique"
    assert gray16_outcome.dedup_status == "unique"

    concurrent_bytes = png("RGB", pixels=uuid4().bytes * 3)
    concurrent_events = [received_event(service, concurrent_bytes) for _ in range(2)]
    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(
            executor.map(lambda event: ImageProcessor(settings).process(event), concurrent_events)
        )
    assert sorted(outcome.dedup_status for outcome in outcomes) == ["duplicate_exact", "unique"]

    pixel_race_pixels = uuid4().bytes * 3
    pixel_race_events = [
        received_event(service, png("RGB", comment=comment, pixels=pixel_race_pixels))
        for comment in ("pixel race A", "pixel race B")
    ]
    with ThreadPoolExecutor(max_workers=2) as executor:
        pixel_race = list(
            executor.map(lambda event: ImageProcessor(settings).process(event), pixel_race_events)
        )
    assert sorted(outcome.dedup_status for outcome in pixel_race) == ["duplicate_pixel", "unique"]

    tenant_id = f"m3-{uuid4().hex[:8]}"
    with psycopg.connect(
        host=stack.postgres_host,
        port=stack.postgres_port,
        dbname=stack.postgres_database,
        user=settings.postgres_username,
        password=settings.postgres_password,
    ) as connection:
        connection.execute(
            "INSERT INTO vectorlake.tenants (tenant_id, name) VALUES (%s, %s)",
            (tenant_id, "M3 isolation test"),
        )
        connection.execute(
            "INSERT INTO vectorlake.dedup_scopes (scope_id, tenant_id, name) VALUES (%s, %s, %s)",
            (tenant_id, tenant_id, "M3 isolation test"),
        )
    other_scope = received_event(service, original_bytes, tenant_id)
    other_outcome = processor.process(other_scope)
    assert other_outcome.dedup_status == "unique"

    status = service.get_status("local-dev", malformed.context.image_id)
    assert status is not None and status["validation_status"] == "invalid"
    return {
        "original": original_outcome.dedup_status,
        "exact": exact_outcome.dedup_status,
        "pixel": pixel_outcome.dedup_status,
        "malformed": invalid_outcome.message.error_code,
        "bit_depth": [gray8_outcome.dedup_status, gray16_outcome.dedup_status],
        "concurrent": sorted(outcome.dedup_status for outcome in outcomes),
        "concurrent_pixel": sorted(outcome.dedup_status for outcome in pixel_race),
        "cross_scope": other_outcome.dedup_status,
    }


if __name__ == "__main__":
    print(json.dumps(verify(), indent=2))
