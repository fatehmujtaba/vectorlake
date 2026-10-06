from __future__ import annotations

from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from vectorlake.embedding import (
    MODEL_VERSION,
    PREPROCESS_VERSION,
    embedding_id,
    validate_embedding,
)

Identifier = Annotated[str, Field(pattern=r"^[A-Za-z0-9._:-]{1,128}$")]


class VectorInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: Identifier
    dedup_scope_id: Identifier
    model_version: Identifier
    preprocess_version: Identifier
    vector: list[float] = Field(min_length=512, max_length=512)

    @field_validator("vector")
    @classmethod
    def normalized_vector(cls, value: list[float]) -> list[float]:
        validate_embedding(value)
        return value

    @model_validator(mode="after")
    def active_contract(self) -> Self:
        if self.model_version != MODEL_VERSION or self.preprocess_version != PREPROCESS_VERSION:
            raise ValueError("model and preprocessing versions must match the active collection")
        return self


class VectorQuery(VectorInput):
    top_k: int = Field(default=10, ge=1, le=100)


class VectorUpsert(VectorInput):
    embedding_id: Identifier
    image_id: Identifier
    duplicate_group_id: Identifier

    @model_validator(mode="after")
    def deterministic_identity(self) -> Self:
        if self.embedding_id != embedding_id(
            self.image_id, self.model_version, self.preprocess_version
        ):
            raise ValueError("embedding ID does not match image and model versions")
        return self


class VectorHit(BaseModel):
    model_config = ConfigDict(extra="forbid")

    embedding_id: Identifier
    image_id: Identifier
    duplicate_group_id: Identifier
    score: float = Field(ge=-1.001, le=1.001, allow_inf_nan=False)


class VectorResults(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: Identifier
    dedup_scope_id: Identifier
    model_version: Identifier
    preprocess_version: Identifier
    hits: list[VectorHit] = Field(max_length=100)


class VectorUpsertResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    embedding_id: Identifier
    collection_name: Identifier
