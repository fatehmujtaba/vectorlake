from __future__ import annotations

import hmac
from dataclasses import asdict
from typing import Annotated

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field, model_validator

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


def create_app(upload_service: UploadService | None = None) -> FastAPI:
    app = FastAPI(title="VectorLake", version="0.1.0")
    local = LocalInfrastructureSettings()

    def tenant(authorization: Annotated[str | None, Header()] = None) -> str:
        if get_settings().environment != "local":
            raise HTTPException(status_code=503, detail="authentication provider is not configured")
        if not authorization or not hmac.compare_digest(authorization, f"Bearer {local.api_token}"):
            raise HTTPException(status_code=401, detail="authentication required")
        return "local-dev"

    def uploads() -> UploadService:
        nonlocal upload_service
        if upload_service is None:
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
        tenant_id = tenant(authorization)
        try:
            session = uploads().create_session(
                tenant_id,
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
        tenant_id = tenant(authorization)
        status = uploads().get_status(tenant_id, image_id)
        if status is None:
            raise HTTPException(status_code=404, detail="image not found")
        return status

    return app
