from __future__ import annotations

import json
from io import BytesIO
from uuid import uuid4

import numpy as np
from PIL import Image
from skimage import data

from infrastructure.bootstrap import bootstrap
from infrastructure.e2e_m3 import received_event
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.ingestion import UploadService
from vectorlake.processor import EVIDENCE_TABLE, ImageProcessor


def _png(pixels: np.ndarray) -> bytes:
    output = BytesIO()
    Image.fromarray(pixels, "RGB").save(output, format="PNG")
    return output.getvalue()


def verify() -> dict[str, object]:
    bootstrap()
    settings = LocalInfrastructureSettings()
    service = UploadService(settings)
    processor = ImageProcessor(settings)
    original_pixels = data.astronaut().copy()  # type: ignore[no-untyped-call]
    original_pixels[0, :16, 0] = np.frombuffer(uuid4().bytes, dtype=np.uint8)
    original = received_event(service, _png(original_pixels))
    first = processor.process(original)
    assert first.dedup_status == "unique"
    assert first.fingerprint is not None

    modified = np.clip(original_pixels.astype(np.int16) + 2, 0, 255).astype(np.uint8)
    near_copy = received_event(service, _png(modified))
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
    return {
        "original_status": first.dedup_status,
        "near_copy_status": second.dedup_status,
        "shadow_match_count": len(candidates),
        "shadow_accepted": sum(bool(item["accepted"]) for item in candidates),
        "fuzzy_suppression_enabled": False,
    }


if __name__ == "__main__":
    print(json.dumps(verify(), indent=2))
