from __future__ import annotations

from dataclasses import asdict
from typing import Annotated

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field, model_validator

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
    upload_service: UploadService | None = None, authenticator: Authenticator | None = None
) -> FastAPI:
    app = FastAPI(title="VectorLake", version="0.1.0")
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

    return app
