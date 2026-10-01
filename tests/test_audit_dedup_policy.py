from __future__ import annotations

from unittest.mock import Mock

import pytest

from infrastructure.audit_dedup_policy import audit
from vectorlake.processor import DETERMINISTIC_POLICY_VERSION


def item(image_id: str, status: str | None, policy: str | None = None) -> dict[str, object]:
    result: dict[str, object] = {"image_id": {"S": image_id}}
    if status is not None:
        result["dedup_status"] = {"S": status}
    if policy is not None:
        result["deterministic_policy_version"] = {"S": policy}
    return result


def test_audit_paginates_and_separates_completed_prior_policy() -> None:
    ddb = Mock()
    ddb.scan.side_effect = [
        {
            "Items": [item("old", "unique"), item("new", "unique", DETERMINISTIC_POLICY_VERSION)],
            "LastEvaluatedKey": {"image_id": {"S": "new"}},
        },
        {"Items": [{**item("pending", None), "capture_id": {"S": "frame-1"}}]},
    ]
    report = audit(ddb, max_pages=2, page_size=2)
    assert report["complete_scan"] is True
    assert report["images_seen"] == 3
    assert report["status_counts"] == {
        "current_complete": 1,
        "incomplete": 1,
        "prior_complete": 1,
    }
    assert report["prior_complete_sample_image_ids"] == ["old"]
    assert report["provenance_counts"] == {"none": 2, "partial": 1}
    assert ddb.scan.call_args_list[1].kwargs["ExclusiveStartKey"] == {"image_id": {"S": "new"}}


def test_audit_reports_truncation() -> None:
    ddb = Mock()
    ddb.scan.return_value = {
        "Items": [item("old", "duplicate_exact", "capture-v2")],
        "LastEvaluatedKey": {"image_id": {"S": "old"}},
    }
    report = audit(ddb, max_pages=1)
    assert report["complete_scan"] is False
    assert report["policy_counts"] == {"capture-v2": 1}
    assert report["duplicate_link_counts"] == {"invalid_link_fields": 1}


def test_audit_checks_duplicate_owner_scope_and_group() -> None:
    ddb = Mock()
    duplicate = {
        **item("duplicate", "duplicate_exact"),
        "tenant_id": {"S": "tenant-1"},
        "dedup_scope_id": {"S": "scope-1"},
        "duplicate_group_id": {"S": "group-1"},
        "representative_image_id": {"S": "owner"},
    }
    ddb.scan.return_value = {"Items": [duplicate]}
    ddb.get_item.return_value = {
        "Item": {
            "tenant_id": {"S": "tenant-2"},
            "dedup_scope_id": {"S": "scope-1"},
            "duplicate_group_id": {"S": "group-1"},
        }
    }
    report = audit(ddb)
    assert report["duplicate_link_counts"] == {"owner_scope_mismatch": 1}
    assert report["bad_link_sample_image_ids"] == ["duplicate"]


def test_audit_rejects_unbounded_page_request() -> None:
    with pytest.raises(ValueError, match="between 1 and 1000"):
        audit(Mock(), max_pages=0)
    with pytest.raises(ValueError, match="between 1 and 1000"):
        audit(Mock(), page_size=1001)
