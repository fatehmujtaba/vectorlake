from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Protocol

from PIL import Image

MODEL_NAME = "resnet18"
MODEL_VERSION = "torchvision-0.23.0-imagenet1k-v1"
PREPROCESS_VERSION = "resnet18-imagenet1k-v1-256-224"
EMBEDDING_DIMENSION = 512
MAX_INFERENCE_BATCH = 2
WEIGHTS_FILENAME = "resnet18-f37072fd.pth"
WEIGHTS_SHA256 = "f37072fd47e89c5e827621c5baffa7500819f7896bbacec160b1a16c560e07ec"


def _verify_weights(path: Path) -> None:
    with path.open("rb") as source:
        actual = hashlib.file_digest(source, "sha256").hexdigest()
    if actual != WEIGHTS_SHA256:
        raise ValueError("pretrained ResNet-18 weights do not match the pinned checksum")


class EmbeddingEngine(Protocol):
    model_name: str
    model_version: str
    preprocess_version: str
    dimension: int

    def embed_images(self, images: list[Image.Image]) -> list[list[float]]: ...


def embedding_id(image_id: str, model_version: str, preprocess_version: str) -> str:
    identity = json.dumps([image_id, model_version, preprocess_version], separators=(",", ":"))
    return hashlib.sha256(identity.encode()).hexdigest()


def validate_embedding(vector: list[float], dimension: int = EMBEDDING_DIMENSION) -> None:
    if len(vector) != dimension or not all(math.isfinite(value) for value in vector):
        raise ValueError("embedding has invalid dimensions or non-finite values")
    norm = math.sqrt(math.fsum(value * value for value in vector))
    if not 0.999 <= norm <= 1.001:
        raise ValueError("embedding is not L2-normalized")


class TorchvisionResNet18EmbeddingEngine:
    model_name = MODEL_NAME
    model_version = MODEL_VERSION
    preprocess_version = PREPROCESS_VERSION
    dimension = EMBEDDING_DIMENSION

    def __init__(self, *, pretrained: bool = True) -> None:
        import torch
        from torchvision.models import ResNet18_Weights, resnet18

        weights = ResNet18_Weights.IMAGENET1K_V1
        torch.set_num_threads(1)
        if not pretrained:
            self.model_version = "untrained-test-only"
        path = Path(torch.hub.get_dir()) / "checkpoints" / WEIGHTS_FILENAME
        if pretrained and path.exists():
            _verify_weights(path)
        self.preprocess = weights.transforms()
        self.model = resnet18(weights=weights if pretrained else None)
        if pretrained:
            _verify_weights(path)
        self.model.fc = torch.nn.Identity()
        self.model.eval()
        self.torch = torch

    def embed_images(self, images: list[Image.Image]) -> list[list[float]]:
        if not 1 <= len(images) <= MAX_INFERENCE_BATCH:
            raise ValueError("inference batch is empty or exceeds the configured maximum")
        batch = self.torch.stack([self.preprocess(image.convert("RGB")) for image in images])
        with self.torch.inference_mode():
            features = self.model(batch)
            normalized = self.torch.nn.functional.normalize(features, p=2, dim=1)
        vectors: list[list[float]] = normalized.tolist()
        for vector in vectors:
            validate_embedding(vector)
        return vectors
