from __future__ import annotations

import argparse
import json
from io import BytesIO
from pathlib import Path
from uuid import uuid4

from PIL import Image

from infrastructure.bootstrap import bootstrap
from infrastructure.e2e_m3 import received_event
from vectorlake.archive import ArchiveWriter
from vectorlake.embedding import TorchvisionResNet18EmbeddingEngine
from vectorlake.embedding_indexer import EmbeddingIndexer
from vectorlake.events.v1 import events_pb2
from vectorlake.inference import InferenceService
from vectorlake.ingestion import UploadService
from vectorlake.processor import ImageProcessor
from vectorlake.vector_client import VectorClient


def seed(image_path: Path) -> dict[str, str]:
    bootstrap()
    with Image.open(image_path) as source, source.convert("RGB") as image:
        buffer = BytesIO()
        image.save(buffer, format="PNG")
    # Each demo run represents a separate capture; it does not rewrite old outcomes.
    received = received_event(UploadService(), buffer.getvalue(), capture_id=uuid4().hex)
    outcome = ImageProcessor().process(received)
    if not isinstance(outcome.message, events_pb2.ImageInferenceReadyV1):
        raise RuntimeError("demo image did not become inference-ready")
    generated = InferenceService(TorchvisionResNet18EmbeddingEngine()).process_batch(
        [outcome.message]
    )[0]
    archived = ArchiveWriter().write_batch([generated])[0]
    index = VectorClient()
    try:
        EmbeddingIndexer(index).process(generated)
    finally:
        index.close()
    return {
        "image_id": generated.context.image_id,
        "embedding_id": generated.embedding_id,
        "archive_key": archived.archive_key,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Seed one real capture through the local M5 pipeline"
    )
    parser.add_argument("--image", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(seed(args.image), indent=2))


if __name__ == "__main__":
    main()
