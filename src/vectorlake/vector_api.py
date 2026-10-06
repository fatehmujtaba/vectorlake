from __future__ import annotations

import hmac
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path
from threading import Lock
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException

from vectorlake.common.config import get_settings
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.semantic_index import SemanticIndex, SemanticRecord, SemanticStore
from vectorlake.vector_contract import VectorQuery, VectorResults, VectorUpsert

logger = logging.getLogger(__name__)


def create_vector_app(
    index: SemanticStore | None = None,
    settings: LocalInfrastructureSettings | None = None,
) -> FastAPI:
    local = settings or LocalInfrastructureSettings()
    environment = get_settings().environment
    owned_index: SemanticIndex | None = None
    lock = Lock()

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        nonlocal index, owned_index
        if index is None and environment == "local":
            Path(local.semantic_db).parent.mkdir(parents=True, exist_ok=True)
            owned_index = SemanticIndex(local.semantic_db)
            index = owned_index
        try:
            if index is not None:
                index.ensure_collection()
            yield
        finally:
            if owned_index is not None:
                owned_index.close()
                index = None

    app = FastAPI(title="VectorLake Vector Service", version="0.1.0", lifespan=lifespan)

    def authorize(authorization: Annotated[str | None, Header()] = None) -> None:
        if environment != "local":
            raise HTTPException(status_code=503, detail="vector service identity is not configured")
        if (
            not local.vector_service_token
            or authorization is None
            or not hmac.compare_digest(authorization, f"Bearer {local.vector_service_token}")
        ):
            raise HTTPException(status_code=401, detail="service authentication required")

    def store() -> SemanticStore:
        if index is None:
            raise HTTPException(status_code=503, detail="vector index is unavailable")
        return index

    @app.get("/healthz")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    def ready() -> dict[str, str]:
        store()
        return {"status": "ready"}

    @app.post("/internal/v1/embeddings/upsert", dependencies=[Depends(authorize)])
    def upsert(request: VectorUpsert) -> dict[str, str]:
        backend = store()
        try:
            with lock:
                backend.upsert(
                    SemanticRecord(
                        embedding_id=request.embedding_id,
                        image_id=request.image_id,
                        tenant_id=request.tenant_id,
                        dedup_scope_id=request.dedup_scope_id,
                        duplicate_group_id=request.duplicate_group_id,
                        model_version=request.model_version,
                        preprocess_version=request.preprocess_version,
                        vector=request.vector,
                    )
                )
        except Exception as exc:
            logger.exception("vector upsert failed")
            raise HTTPException(status_code=503, detail="vector index is unavailable") from exc
        return {"embedding_id": request.embedding_id, "collection_name": backend.collection}

    @app.post(
        "/internal/v1/embeddings/search",
        dependencies=[Depends(authorize)],
        response_model=VectorResults,
    )
    def search(request: VectorQuery) -> dict[str, object]:
        backend = store()
        try:
            with lock:
                hits = backend.search(
                    request.vector,
                    tenant_id=request.tenant_id,
                    dedup_scope_id=request.dedup_scope_id,
                    model_version=request.model_version,
                    preprocess_version=request.preprocess_version,
                    top_k=request.top_k,
                )
        except Exception as exc:
            logger.exception("vector search failed")
            raise HTTPException(status_code=503, detail="vector index is unavailable") from exc
        return {
            "tenant_id": request.tenant_id,
            "dedup_scope_id": request.dedup_scope_id,
            "model_version": request.model_version,
            "preprocess_version": request.preprocess_version,
            "hits": [asdict(hit) for hit in hits],
        }

    return app
