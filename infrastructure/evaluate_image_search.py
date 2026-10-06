from __future__ import annotations

import argparse
import hashlib
import json
import math
import time
from collections import defaultdict
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import httpx
from PIL import Image

from infrastructure.bootstrap import bootstrap
from infrastructure.search_stack import local_search_stack
from vectorlake.embedding import (
    MAX_INFERENCE_BATCH,
    MODEL_VERSION,
    PREPROCESS_VERSION,
    WEIGHTS_SHA256,
    TorchvisionResNet18EmbeddingEngine,
    embedding_id,
)
from vectorlake.semantic_index import SemanticRecord
from vectorlake.validation import MAX_OBJECT_BYTES
from vectorlake.vector_client import VectorClient

TRANSFORMS = ("original", "jpeg_75", "resize_70", "crop_10")
MIN_VARIANT_RECALL_AT_5 = 0.90


def load_calibration(manifest: Path) -> list[dict[str, Any]]:
    rows = [json.loads(line) for line in manifest.read_text().splitlines() if line.strip()]
    calibration = [row for row in rows if row["split"] == "calibration"]
    if not calibration or len(calibration) > 4000:
        raise ValueError("calibration manifest is empty or exceeds 4000 rows")
    sources: dict[str, set[str]] = defaultdict(set)
    for row in calibration:
        transform = row["transformation"]
        if transform not in TRANSFORMS or transform in sources[row["source_id"]]:
            raise ValueError("unexpected or duplicate calibration transformation")
        sources[row["source_id"]].add(transform)
        path = Path(row["path"])
        if not 0 < path.stat().st_size <= MAX_OBJECT_BYTES:
            raise ValueError("calibration image is outside query limits")
        with path.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
        if digest != row["sha256"]:
            raise ValueError("calibration image checksum mismatch")
    if any(transforms != set(TRANSFORMS) for transforms in sources.values()):
        raise ValueError("each calibration source needs an original and all three variants")
    return calibration


def _percentile(values: list[float], fraction: float) -> float:
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


def evaluate(manifest: Path) -> dict[str, Any]:
    rows = load_calibration(manifest)
    originals = [row for row in rows if row["transformation"] == "original"]
    bootstrap()
    engine = TorchvisionResNet18EmbeddingEngine()
    source_ids = {
        row["source_id"]: hashlib.sha256(row["image_id"].encode()).hexdigest() for row in originals
    }
    results: list[dict[str, Any]] = []
    with TemporaryDirectory(prefix="vectorlake-search-evaluation-") as directory:
        with local_search_stack(Path(directory) / "evaluation.db") as stack:
            vector = VectorClient(stack.settings)
            try:
                for start in range(0, len(originals), MAX_INFERENCE_BATCH):
                    batch = originals[start : start + MAX_INFERENCE_BATCH]
                    images: list[Image.Image] = []
                    try:
                        for row in batch:
                            with Image.open(row["path"]) as image:
                                images.append(image.convert("RGB"))
                        embeddings = engine.embed_images(images)
                        for row, values in zip(batch, embeddings, strict=True):
                            image_id = source_ids[row["source_id"]]
                            vector.upsert(
                                SemanticRecord(
                                    embedding_id(image_id, MODEL_VERSION, PREPROCESS_VERSION),
                                    image_id,
                                    "local-dev",
                                    "local-dev",
                                    image_id,
                                    MODEL_VERSION,
                                    PREPROCESS_VERSION,
                                    values,
                                )
                            )
                    finally:
                        for decoded in images:
                            decoded.close()
                headers = {
                    "Authorization": f"Bearer {stack.settings.api_token}",
                    "Content-Type": "image/jpeg",
                }
                with httpx.Client(base_url=stack.api_url, timeout=60, trust_env=False) as api:
                    # Warm the lazy model before timing; cold startup is reported separately.
                    started = time.perf_counter()
                    warm = api.post(
                        "/v1/images/search?top_k=5",
                        content=Path(originals[0]["path"]).read_bytes(),
                        headers=headers,
                    )
                    warm.raise_for_status()
                    cold_ms = (time.perf_counter() - started) * 1000
                    for row in rows:
                        data = Path(row["path"]).read_bytes()
                        started = time.perf_counter()
                        response = api.post(
                            "/v1/images/search?top_k=5", content=data, headers=headers
                        )
                        elapsed_ms = (time.perf_counter() - started) * 1000
                        response.raise_for_status()
                        hits = response.json()["hits"]
                        expected = source_ids[row["source_id"]]
                        rank = next(
                            (i + 1 for i, hit in enumerate(hits) if hit["image_id"] == expected),
                            None,
                        )
                        results.append(
                            {
                                "image_id": row["image_id"],
                                "recording_id": row["recording_id"],
                                "transformation": row["transformation"],
                                "expected_rank": rank,
                                "top1_score": hits[0]["score"] if hits else None,
                                "elapsed_ms": elapsed_ms,
                            }
                        )
            finally:
                vector.close()
    summary = {}
    for transform in TRANSFORMS:
        queries = [row for row in results if row["transformation"] == transform]
        summary[transform] = {
            "queries": len(queries),
            "recall_at_1": sum(row["expected_rank"] == 1 for row in queries) / len(queries),
            "recall_at_5": sum(row["expected_rank"] is not None for row in queries) / len(queries),
        }
    passed = summary["original"]["recall_at_1"] == 1 and all(
        summary[transform]["recall_at_5"] >= MIN_VARIANT_RECALL_AT_5 for transform in TRANSFORMS[1:]
    )
    latencies = [row["elapsed_ms"] for row in results]
    return {
        "purpose": "local transformed-image retrieval engineering evaluation; no dedup activation",
        "split": "calibration",
        "held_out_scored": False,
        "manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
        "model_version": MODEL_VERSION,
        "preprocess_version": PREPROCESS_VERSION,
        "weights_sha256": WEIGHTS_SHA256,
        "gallery_images": len(originals),
        "recordings": len({row["recording_id"] for row in originals}),
        "queries": len(results),
        "metrics": summary,
        "latency_ms": {
            "cold_first_query": cold_ms,
            "warm_p50": _percentile(latencies, 0.5),
            "warm_p95": _percentile(latencies, 0.95),
        },
        "gate": {
            "original_recall_at_1_min": 1.0,
            "each_variant_recall_at_5_min": MIN_VARIANT_RECALL_AT_5,
            "passed": passed,
        },
        "per_query": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Evaluate image retrieval on the calibration split only"
    )
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = evaluate(args.manifest)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key != "per_query"}, indent=2))
    if not report["gate"]["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
