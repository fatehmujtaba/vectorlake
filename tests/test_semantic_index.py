from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from vectorlake.semantic_index import DIMENSION, SemanticIndex, SemanticRecord


def unit_vector(axis: int = 0) -> list[float]:
    values = [0.0] * DIMENSION
    values[axis] = 1.0
    return values


def record(
    image_id: str, *, tenant_id: str = "tenant-a", scope_id: str = "scope-a"
) -> SemanticRecord:
    return SemanticRecord(
        embedding_id=f"embed-{image_id}",
        image_id=image_id,
        tenant_id=tenant_id,
        dedup_scope_id=scope_id,
        duplicate_group_id="group-1",
        model_version="torchvision-imagenet1k-v1",
        preprocess_version="resnet18-default-v1",
        vector=unit_vector(),
    )


def test_semantic_index_scopes_results_and_upserts_replays(tmp_path: Path) -> None:
    index = SemanticIndex(str(tmp_path / "semantic.db"))
    try:
        index.ensure_collection()
        index.ensure_collection()
        first = record("image-1")
        index.upsert(first)
        index.upsert(first)
        index.upsert(record("image-2"))
        index.upsert(record("other-tenant", tenant_id="tenant-b"))
        index.upsert(record("other-scope", scope_id="scope-b"))
        index.upsert(replace(record("other-model"), model_version="other-model-v1"))
        index.upsert(replace(record("other-preprocess"), preprocess_version="other-input-v1"))

        hits = index.search(
            unit_vector(),
            tenant_id="tenant-a",
            dedup_scope_id="scope-a",
            model_version=first.model_version,
            preprocess_version=first.preprocess_version,
        )
        assert len(hits) == 2
        assert {hit.image_id for hit in hits} == {"image-1", "image-2"}
        assert {hit.embedding_id for hit in hits} == {"embed-image-1", "embed-image-2"}
        assert all(hit.score == pytest.approx(1.0) for hit in hits)
        assert (
            index.search(
                unit_vector(),
                tenant_id="tenant-c",
                dedup_scope_id="scope-a",
                model_version=first.model_version,
                preprocess_version=first.preprocess_version,
            )
            == []
        )
    finally:
        index.close()


def test_semantic_index_rejects_invalid_vectors_and_filter_values(tmp_path: Path) -> None:
    index = SemanticIndex(str(tmp_path / "semantic.db"))
    try:
        index.ensure_collection()
        with pytest.raises(ValueError, match="512 dimensions"):
            index.upsert(replace(record("image-1"), vector=[1.0]))
        with pytest.raises(ValueError, match="L2-normalized"):
            index.upsert(replace(record("image-1"), vector=[0.0] * DIMENSION))
        with pytest.raises(ValueError, match="finite"):
            index.upsert(replace(record("image-1"), vector=[float("nan")] + [0.0] * 511))
        with pytest.raises(ValueError, match="identifier"):
            index.search(
                unit_vector(),
                tenant_id='tenant-a" or tenant_id == "tenant-b',
                dedup_scope_id="scope-a",
                model_version="torchvision-imagenet1k-v1",
                preprocess_version="resnet18-default-v1",
            )
        with pytest.raises(ValueError, match="top_k"):
            index.search(
                unit_vector(),
                tenant_id="tenant-a",
                dedup_scope_id="scope-a",
                model_version="torchvision-imagenet1k-v1",
                preprocess_version="resnet18-default-v1",
                top_k=101,
            )
    finally:
        index.close()
