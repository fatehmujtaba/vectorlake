from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict
from tempfile import SpooledTemporaryFile
from typing import Annotated, BinaryIO, cast

from fastapi import FastAPI, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field, model_validator
from starlette.concurrency import run_in_threadpool

from vectorlake.auth import (
    AuthenticationFailed,
    AuthenticationUnavailable,
    Authenticator,
    LocalTokenAuthenticator,
    Principal,
    UnconfiguredAuthenticator,
)
from vectorlake.common.config import get_settings
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.ingestion import IdempotencyConflict, UploadService
from vectorlake.search import (
    ImageSearchService,
    SearchBusy,
    SearchResponse,
    SearchScopeUnavailable,
    SearchUnavailable,
)
from vectorlake.validation import IMAGE_FORMATS, MAX_OBJECT_BYTES, ValidationFailure
from vectorlake.vector_client import VectorClient


class UploadSessionRequest(BaseModel):
    content_type: str = Field(pattern=r"^image/(jpeg|png|webp)$")
    size_bytes: int = Field(ge=1, le=20_000_000)
    capture_id: str | None = Field(
        default=None, min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._:-]+$"
    )
    source_id: str | None = Field(
        default=None, min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._:-]+$"
    )

    @model_validator(mode="after")
    def source_and_capture_are_paired(self) -> UploadSessionRequest:
        if (self.source_id is None) != (self.capture_id is None):
            raise ValueError("source_id and capture_id must be supplied together")
        return self


def create_app(
    upload_service: UploadService | None = None,
    authenticator: Authenticator | None = None,
    search_service: ImageSearchService | None = None,
) -> FastAPI:
    vector_client: VectorClient | None = None

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            if vector_client is not None:
                vector_client.close()

    app = FastAPI(title="VectorLake", version="0.1.0", lifespan=lifespan)
    local = LocalInfrastructureSettings()
    environment = get_settings().environment
    auth = (
        authenticator
        if authenticator is not None
        else (
            LocalTokenAuthenticator(local.api_token, local.api_source_id)
            if environment == "local"
            else UnconfiguredAuthenticator()
        )
    )

    def principal(authorization: Annotated[str | None, Header()] = None) -> Principal:
        try:
            return auth.authenticate(authorization)
        except AuthenticationUnavailable as exc:
            raise HTTPException(
                status_code=503, detail="authentication provider is not configured"
            ) from exc
        except AuthenticationFailed as exc:
            raise HTTPException(status_code=401, detail="authentication required") from exc

    def uploads() -> UploadService:
        nonlocal upload_service
        if upload_service is None:
            if environment != "local":
                raise HTTPException(status_code=503, detail="upload backend is not configured")
            upload_service = UploadService(local)
        return upload_service

    def searches() -> ImageSearchService:
        nonlocal search_service, vector_client
        if search_service is None:
            if environment != "local":
                raise HTTPException(status_code=503, detail="search backend is not configured")
            vector_client = VectorClient(local)
            search_service = ImageSearchService(vector_client, uploads().scope_for_tenant)
        return search_service

    @app.get("/healthz")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    def readiness() -> dict[str, str]:
        return {"status": "ready"}

    @app.post("/v1/upload-sessions", status_code=201)
    def create_upload_session(
        request: UploadSessionRequest,
        idempotency_key: Annotated[str, Header(min_length=1, max_length=128)],
        authorization: Annotated[str | None, Header()] = None,
        traceparent: Annotated[str | None, Header()] = None,
    ) -> dict[str, object]:
        identity = principal(authorization)
        if request.source_id is not None and request.source_id not in identity.source_ids:
            raise HTTPException(status_code=403, detail="source is not authorized")
        try:
            session = uploads().create_session(
                identity.tenant_id,
                idempotency_key,
                request.content_type,
                request.size_bytes,
                traceparent or "",
                request.capture_id,
                request.source_id,
            )
        except IdempotencyConflict as exc:
            raise HTTPException(
                status_code=409, detail="idempotency key conflicts with request"
            ) from exc
        return asdict(session)

    @app.get("/v1/images/{image_id}")
    def get_image_status(
        image_id: str, authorization: Annotated[str | None, Header()] = None
    ) -> dict[str, object]:
        identity = principal(authorization)
        status = uploads().get_status(identity.tenant_id, image_id)
        if status is None:
            raise HTTPException(status_code=404, detail="image not found")
        return status

    @app.post(
        "/v1/images/search",
        response_model=SearchResponse,
        openapi_extra={
            "requestBody": {
                "required": True,
                "content": {
                    media_type: {"schema": {"type": "string", "format": "binary"}}
                    for media_type in IMAGE_FORMATS.values()
                },
            }
        },
    )
    async def search_images(
        request: Request,
        top_k: Annotated[int, Query(ge=1, le=100)] = 10,
        authorization: Annotated[str | None, Header()] = None,
    ) -> SearchResponse:
        identity = principal(authorization)
        media_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if media_type not in IMAGE_FORMATS.values():
            raise HTTPException(status_code=415, detail="JPEG, PNG, or WebP image bytes required")
        length = request.headers.get("content-length")
        if length is not None:
            try:
                declared_size = int(length)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="invalid Content-Length") from exc
            if declared_size < 0:
                raise HTTPException(status_code=400, detail="invalid Content-Length")
            if declared_size > MAX_OBJECT_BYTES:
                raise HTTPException(status_code=413, detail="query image exceeds 20 MB")
        size = 0
        with SpooledTemporaryFile(max_size=2 * 1024 * 1024) as temporary:
            async for chunk in request.stream():
                size += len(chunk)
                if size > MAX_OBJECT_BYTES:
                    raise HTTPException(status_code=413, detail="query image exceeds 20 MB")
                temporary.write(chunk)
            if length is not None and size != declared_size:
                raise HTTPException(status_code=400, detail="Content-Length does not match body")
            temporary.seek(0)
            try:
                return await run_in_threadpool(
                    searches().search,
                    cast(BinaryIO, temporary),
                    size,
                    media_type,
                    tenant_id=identity.tenant_id,
                    top_k=top_k,
                )
            except ValidationFailure as exc:
                raise HTTPException(status_code=422, detail=exc.code) from exc
            except SearchScopeUnavailable as exc:
                raise HTTPException(status_code=403, detail="tenant has no search scope") from exc
            except SearchBusy as exc:
                raise HTTPException(
                    status_code=429, detail="search capacity is busy", headers={"Retry-After": "1"}
                ) from exc
            except SearchUnavailable as exc:
                raise HTTPException(status_code=503, detail="image search is unavailable") from exc

    return app
