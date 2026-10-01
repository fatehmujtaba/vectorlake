from __future__ import annotations

import hashlib
import json
from unittest.mock import Mock, patch

import pytest
from botocore.exceptions import ClientError

from vectorlake.ingestion import IdempotencyConflict, UploadService


def service() -> UploadService:
    upload = object.__new__(UploadService)
    upload.ddb = Mock()
    upload.s3 = Mock()
    return upload


def test_capture_id_is_persisted_and_reported() -> None:
    upload = service()
    with patch.object(UploadService, "_scope", return_value="scope-1"):
        session = upload.create_session("tenant-1", "key-1", "image/png", 12, capture_id="frame-1")
    assert session.image_id
    transaction = upload.ddb.transact_write_items.call_args.kwargs["TransactItems"]
    assert transaction[1]["Put"]["Item"]["capture_id"] == {"S": "frame-1"}
    upload.ddb.get_item.return_value = {
        "Item": {
            "tenant_id": {"S": "tenant-1"},
            "ingestion_status": {"S": "pending_upload"},
            "capture_id": {"S": "frame-1"},
        }
    }
    status = upload.get_status("tenant-1", session.image_id)
    assert status is not None
    assert status["capture_id"] == "frame-1"


def test_capture_id_participates_in_idempotency_request_hash() -> None:
    upload = service()
    with patch.object(UploadService, "_scope", return_value="scope-1"):
        upload.create_session("tenant-1", "key-1", "image/png", 12, capture_id="frame-1")
    transaction = upload.ddb.transact_write_items.call_args.kwargs["TransactItems"]
    original_hash = transaction[0]["Put"]["Item"]["request_hash"]
    upload.ddb.transact_write_items.side_effect = ClientError(
        {"Error": {"Code": "TransactionCanceledException", "Message": "conflict"}},
        "TransactWriteItems",
    )
    upload.ddb.get_item.return_value = {"Item": {"request_hash": original_hash}}
    with (
        patch.object(UploadService, "_scope", return_value="scope-1"),
        pytest.raises(IdempotencyConflict),
    ):
        upload.create_session("tenant-1", "key-1", "image/png", 12, capture_id="frame-2")


def test_legacy_upload_omits_capture_id() -> None:
    upload = service()
    with patch.object(UploadService, "_scope", return_value="scope-1"):
        upload.create_session("tenant-1", "key-1", "image/png", 12)
    transaction = upload.ddb.transact_write_items.call_args.kwargs["TransactItems"]
    assert "capture_id" not in transaction[1]["Put"]["Item"]
    expected = hashlib.sha256(
        json.dumps(
            {"content_type": "image/png", "size_bytes": 12},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    assert transaction[0]["Put"]["Item"]["request_hash"] == {"S": expected}
