from __future__ import annotations

import math
from pathlib import Path

import pytest
from PIL import Image

from vectorlake.embedding import (
    EMBEDDING_DIMENSION,
    MAX_INFERENCE_BATCH,
    TorchvisionResNet18EmbeddingEngine,
    _verify_weights,
    embedding_id,
    validate_embedding,
)


def test_embedding_identity_is_per_image_and_version() -> None:
    first = embedding_id("image-1", "model-v1", "preprocess-v1")
    assert first == embedding_id("image-1", "model-v1", "preprocess-v1")
    assert first != embedding_id("image-2", "model-v1", "preprocess-v1")
    assert first != embedding_id("image-1", "model-v2", "preprocess-v1")


def test_untrained_resnet_smoke_produces_normalized_512d_vectors() -> None:
    pytest.importorskip("torch")
    pytest.importorskip("torchvision")
    engine = TorchvisionResNet18EmbeddingEngine(pretrained=False)
    assert engine.model_version == "untrained-test-only"
    image = Image.new("RGB", (32, 32), "red")
    vectors = engine.embed_images([image, image])
    assert len(vectors) == 2
    assert all(len(vector) == EMBEDDING_DIMENSION for vector in vectors)
    assert all(
        math.sqrt(sum(value * value for value in vector)) == pytest.approx(1.0)
        for vector in vectors
    )
    assert vectors[0] == pytest.approx(vectors[1])
    with pytest.raises(ValueError, match="maximum"):
        engine.embed_images([image] * (MAX_INFERENCE_BATCH + 1))


def test_embedding_validation_rejects_bad_vectors() -> None:
    with pytest.raises(ValueError, match="dimensions"):
        validate_embedding([1.0])
    with pytest.raises(ValueError, match="non-finite"):
        validate_embedding([float("nan")] + [0.0] * (EMBEDDING_DIMENSION - 1))
    with pytest.raises(ValueError, match="L2-normalized"):
        validate_embedding([0.0] * EMBEDDING_DIMENSION)


def test_cached_pretrained_weights_require_pinned_digest(tmp_path: Path) -> None:
    cached = tmp_path / "resnet18-f37072fd.pth"
    cached.write_bytes(b"not the pinned weights")
    with pytest.raises(ValueError, match="pinned checksum"):
        _verify_weights(cached)
