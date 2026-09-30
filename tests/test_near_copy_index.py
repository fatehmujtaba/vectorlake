from __future__ import annotations

from unittest.mock import Mock

from vectorlake.near_copy import Fingerprint
from vectorlake.near_copy_index import ScopedPDQIndex


def test_query_uses_scope_key_and_orders_by_hamming() -> None:
    ddb = Mock()
    ddb.query.return_value = {
        "Items": [
            {"image_id": {"S": "far"}, "pdq_hex": {"S": "f" * 64}, "quality": {"N": "90"}},
            {"image_id": {"S": "near"}, "pdq_hex": {"S": "0" * 64}, "quality": {"N": "90"}},
        ]
    }
    index = ScopedPDQIndex(ddb, max_scope_entries=10)
    candidates = index.find("scope-1", "self", Fingerprint("0" * 64, 90))
    assert [candidate.image_id for candidate in candidates] == ["near"]
    assert ddb.query.call_args.kwargs["ExpressionAttributeValues"][":scope"] == {"S": "scope-1"}


def test_overflow_returns_no_candidates() -> None:
    ddb = Mock()
    ddb.query.return_value = {"Items": [], "LastEvaluatedKey": {"image_id": {"S": "last"}}}
    assert ScopedPDQIndex(ddb).find("scope-1", "self", Fingerprint("0" * 64, 90)) == []


def test_low_quality_skips_query() -> None:
    ddb = Mock()
    assert ScopedPDQIndex(ddb).find("scope-1", "self", Fingerprint("0" * 64, 0)) == []
    ddb.query.assert_not_called()
