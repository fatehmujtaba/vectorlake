from __future__ import annotations

import argparse
import json
from dataclasses import replace
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import httpx
from PIL import Image

from infrastructure.bootstrap import bootstrap
from infrastructure.e2e_m3 import received_event
from infrastructure.search_stack import local_search_stack
from vectorlake.archive import ArchiveWriter
from vectorlake.embedding import TorchvisionResNet18EmbeddingEngine, embedding_id
from vectorlake.embedding_indexer import EmbeddingIndexer
from vectorlake.events.v1 import events_pb2
from vectorlake.inference import InferenceService
from vectorlake.ingestion import UploadService
from vectorlake.processor import ImageProcessor
from vectorlake.semantic_index import SemanticRecord
from vectorlake.vector_client import VectorClient


def verify(image_path: Path) -> dict[str, object]:
    bootstrap()
    with image_path.open("rb") as source, Image.open(source) as image:
        with image.convert("RGB") as rgb:
            buffer = BytesIO()
            rgb.save(buffer, format="PNG")
    data = buffer.getvalue()
    uploads = UploadService()
    received = received_event(uploads, data, capture_id=uuid4().hex)
    outcome = ImageProcessor().process(received)
    assert isinstance(outcome.message, events_pb2.ImageInferenceReadyV1)
    inference = InferenceService(TorchvisionResNet18EmbeddingEngine())
    generated = inference.process_batch([outcome.message])[0]
    archived = ArchiveWriter().write_batch([generated])[0]
    with TemporaryDirectory(prefix="vectorlake-m5-search-") as directory:
        with local_search_stack(Path(directory) / "vectors.db") as stack:
            index = VectorClient(stack.settings)
            try:
                indexer = EmbeddingIndexer(index)
                indexed = indexer.process(generated)
                assert indexer.process(generated) == indexed
                baseline = SemanticRecord(
                    embedding_id=generated.embedding_id,
                    image_id=received.context.image_id,
                    tenant_id=received.context.tenant_id,
                    dedup_scope_id=received.context.dedup_scope_id,
                    duplicate_group_id=generated.duplicate_group_id,
                    model_version=generated.model_version,
                    preprocess_version=generated.preprocess_version,
                    vector=generated.vector,
                )
                for image_id, tenant, scope in (
                    ("other-tenant-image", "other-tenant", baseline.dedup_scope_id),
                    ("other-scope-image", baseline.tenant_id, "other-scope"),
                ):
                    index.upsert(
                        replace(
                            baseline,
                            embedding_id=embedding_id(
                                image_id, baseline.model_version, baseline.preprocess_version
                            ),
                            image_id=image_id,
                            tenant_id=tenant,
                            dedup_scope_id=scope,
                        )
                    )
                with httpx.Client(base_url=stack.api_url, timeout=60, trust_env=False) as api:
                    headers = {
                        "Authorization": f"Bearer {stack.settings.api_token}",
                        "Content-Type": "image/png",
                    }
                    response = api.post("/v1/images/search?top_k=5", content=data, headers=headers)
                    response.raise_for_status()
                    hits = response.json()["hits"]
                    assert len(hits) == 1 and hits[0]["image_id"] == received.context.image_id
                    assert hits[0]["score"] >= 0.999
                    assert (
                        api.post(
                            "/v1/images/search", content=data, headers={"Content-Type": "image/png"}
                        ).status_code
                        == 401
                    )
                    assert (
                        api.post(
                            "/v1/images/search", content=b"broken", headers=headers
                        ).status_code
                        == 422
                    )
            finally:
                index.close()
    return {
        "image_id": received.context.image_id,
        "embedding_id": generated.embedding_id,
        "archive_key": archived.archive_key,
        "query_top1_score": hits[0]["score"],
        "cross_tenant_and_scope_excluded": True,
        "index_replay_same_event": True,
        "unauthenticated_and_corrupt_queries_rejected": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify real upload-to-search over isolated HTTP services"
    )
    parser.add_argument("--image", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.image), indent=2))


if __name__ == "__main__":
    main()
