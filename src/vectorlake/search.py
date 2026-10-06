from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from tempfile import SpooledTemporaryFile
from threading import Lock
from typing import BinaryIO

from PIL import Image

from vectorlake.embedding import (
    EMBEDDING_DIMENSION,
    MODEL_NAME,
    MODEL_VERSION,
    PREPROCESS_VERSION,
    EmbeddingEngine,
    TorchvisionResNet18EmbeddingEngine,
    validate_embedding,
)
from vectorlake.semantic_index import SearchHit, SemanticStore
from vectorlake.validation import ValidationFailure, canonicalize_image, stream_image
from vectorlake.vector_client import VectorUnavailable


class SearchUnavailable(Exception):
    pass


class SearchBusy(Exception):
    pass


class SearchScopeUnavailable(Exception):
    pass


@dataclass(frozen=True)
class SearchResponse:
    model_name: str
    model_version: str
    preprocess_version: str
    dedup_scope_id: str
    hits: list[SearchHit]


class ImageSearchService:
    def __init__(
        self,
        index: SemanticStore,
        scope_for_tenant: Callable[[str], str],
        *,
        engine: EmbeddingEngine | None = None,
    ) -> None:
        self.index = index
        self.scope_for_tenant = scope_for_tenant
        self.engine = engine
        self.lock = Lock()

    def search(
        self,
        source: BinaryIO,
        size_bytes: int,
        media_type: str,
        *,
        tenant_id: str,
        top_k: int = 10,
    ) -> SearchResponse:
        if not 1 <= top_k <= 100:
            raise ValueError("top_k must be between 1 and 100")
        if not self.lock.acquire(blocking=False):
            raise SearchBusy
        try:
            with SpooledTemporaryFile(max_size=2 * 1024 * 1024) as temporary:
                streamed = stream_image(source, temporary, size_bytes)
                if streamed.media_type != media_type:
                    raise ValidationFailure("format_mismatch")
                canonicalize_image(temporary, streamed.media_type)
                try:
                    scope_id = self.scope_for_tenant(tenant_id)
                except LookupError as exc:
                    raise SearchScopeUnavailable from exc
                except Exception as exc:
                    raise SearchUnavailable("scope registry is unavailable") from exc
                try:
                    self.index.ensure_collection()
                    if self.engine is None:
                        self.engine = TorchvisionResNet18EmbeddingEngine()
                    if (
                        self.engine.model_name != MODEL_NAME
                        or self.engine.model_version != MODEL_VERSION
                        or self.engine.preprocess_version != PREPROCESS_VERSION
                        or self.engine.dimension != EMBEDDING_DIMENSION
                    ):
                        raise SearchUnavailable("query model does not match the active collection")
                    temporary.seek(0)
                    with Image.open(temporary) as image, image.convert("RGB") as rgb:
                        vectors = self.engine.embed_images([rgb])
                    if len(vectors) != 1:
                        raise SearchUnavailable("query model returned the wrong batch size")
                    validate_embedding(vectors[0])
                    hits = self.index.search(
                        vectors[0],
                        tenant_id=tenant_id,
                        dedup_scope_id=scope_id,
                        model_version=MODEL_VERSION,
                        preprocess_version=PREPROCESS_VERSION,
                        top_k=top_k,
                    )
                except (VectorUnavailable, ImportError, OSError, RuntimeError, ValueError) as exc:
                    raise SearchUnavailable("image search backend is unavailable") from exc
            return SearchResponse(MODEL_NAME, MODEL_VERSION, PREPROCESS_VERSION, scope_id, hits)
        finally:
            self.lock.release()
