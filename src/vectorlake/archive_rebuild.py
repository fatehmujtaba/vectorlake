from __future__ import annotations

import hashlib
from io import BytesIO
from typing import Any

from vectorlake.archive import archive_schema
from vectorlake.common.resources import ARCHIVE_BUCKET
from vectorlake.embedding import (
    MODEL_NAME,
    MODEL_VERSION,
    PREPROCESS_VERSION,
    embedding_id,
    validate_embedding,
)
from vectorlake.semantic_index import SemanticIndex, SemanticRecord


def rebuild_index(s3: Any, index: SemanticIndex, *, max_rows: int = 10_000) -> int:
    """Rebuild a fresh index from bounded archive data, without model inference."""
    import pyarrow.parquet as pq

    if max_rows < 1:
        raise ValueError("max_rows must be positive")
    if index.client.has_collection(index.collection):
        raise ValueError("rebuild requires a fresh collection")
    seen: dict[str, SemanticRecord] = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=ARCHIVE_BUCKET):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.endswith(".parquet"):
                continue
            if item["Size"] > 64 * 1024 * 1024:
                raise ValueError(f"archive object exceeds rebuild size limit: {key}")
            response = s3.get_object(Bucket=ARCHIVE_BUCKET, Key=key)
            body = response["Body"]
            try:
                payload = body.read()
            finally:
                body.close()
            digest = hashlib.sha256(payload).hexdigest()
            if (
                not key.endswith(f"part-{digest}.parquet")
                or response.get("Metadata", {}).get("sha256") != digest
            ):
                raise ValueError(f"archive checksum mismatch: {key}")
            table = pq.read_table(BytesIO(payload))
            if not table.schema.equals(archive_schema()):
                raise ValueError(f"archive schema mismatch: {key}")
            if table.num_rows > max_rows:
                raise ValueError("archive object exceeds rebuild row limit")
            for row in table.to_pylist():
                if len(seen) >= max_rows and row["embedding_id"] not in seen:
                    raise ValueError("archive rebuild row limit reached")
                if (
                    row["model_name"] != MODEL_NAME
                    or row["model_version"] != MODEL_VERSION
                    or row["preprocess_version"] != PREPROCESS_VERSION
                    or row["embedding_id"]
                    != embedding_id(
                        row["image_id"], row["model_version"], row["preprocess_version"]
                    )
                ):
                    raise ValueError("archive model or identity mismatch")
                validate_embedding(row["vector"])
                record = SemanticRecord(
                    embedding_id=row["embedding_id"],
                    image_id=row["image_id"],
                    tenant_id=row["tenant_id"],
                    dedup_scope_id=row["dedup_scope_id"],
                    duplicate_group_id=row["duplicate_group_id"],
                    model_version=row["model_version"],
                    preprocess_version=row["preprocess_version"],
                    vector=row["vector"],
                )
                prior = seen.get(record.embedding_id)
                if prior is not None and prior != record:
                    raise ValueError("conflicting archived embedding rows")
                seen[record.embedding_id] = record
    index.ensure_collection()
    for record in seen.values():
        index.upsert(record)
    return len(seen)
