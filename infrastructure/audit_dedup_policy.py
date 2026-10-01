from __future__ import annotations

import argparse
import json
from collections import Counter
from typing import Any
from urllib.parse import urlparse

from vectorlake.common.aws import aws_client
from vectorlake.common.local import LocalInfrastructureSettings
from vectorlake.common.resources import IMAGE_TABLE
from vectorlake.processor import DETERMINISTIC_POLICY_VERSION


def audit(ddb: Any, *, max_pages: int = 100, page_size: int = 100) -> dict[str, Any]:
    if not 1 <= max_pages <= 1000 or not 1 <= page_size <= 1000:
        raise ValueError("max_pages and page_size must be between 1 and 1000")
    status_counts: Counter[str] = Counter()
    policy_counts: Counter[str] = Counter()
    provenance_counts: Counter[str] = Counter()
    prior_samples: list[str] = []
    cursor: dict[str, Any] | None = None
    pages = 0
    images = 0
    while pages < max_pages:
        request: dict[str, Any] = {
            "TableName": IMAGE_TABLE,
            "ProjectionExpression": (
                "image_id,dedup_status,deterministic_policy_version,source_id,capture_id"
            ),
            "ConsistentRead": True,
            "Limit": page_size,
        }
        if cursor is not None:
            request["ExclusiveStartKey"] = cursor
        response = ddb.scan(**request)
        pages += 1
        for item in response.get("Items", []):
            images += 1
            status = item.get("dedup_status", {}).get("S")
            policy = item.get("deterministic_policy_version", {}).get("S")
            if status is None:
                status_counts["incomplete"] += 1
            elif policy == DETERMINISTIC_POLICY_VERSION:
                status_counts["current_complete"] += 1
            else:
                status_counts["prior_complete"] += 1
                if len(prior_samples) < 20:
                    prior_samples.append(item["image_id"]["S"])
            policy_counts[policy or "unversioned"] += 1
            source = bool(item.get("source_id", {}).get("S"))
            capture = bool(item.get("capture_id", {}).get("S"))
            provenance_counts[
                "paired"
                if source and capture
                else "none"
                if not source and not capture
                else "partial"
            ] += 1
        cursor = response.get("LastEvaluatedKey")
        if cursor is None:
            break
    return {
        "policy_version": DETERMINISTIC_POLICY_VERSION,
        "images_seen": images,
        "pages_scanned": pages,
        "complete_scan": cursor is None,
        "status_counts": dict(sorted(status_counts.items())),
        "policy_counts": dict(sorted(policy_counts.items())),
        "provenance_counts": dict(sorted(provenance_counts.items())),
        "prior_complete_sample_image_ids": prior_samples,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only local dedup policy inventory")
    parser.add_argument("--max-pages", type=int, default=100)
    parser.add_argument("--page-size", type=int, default=100)
    args = parser.parse_args()
    settings = LocalInfrastructureSettings()
    if urlparse(settings.aws_endpoint_url).hostname not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("audit is restricted to a loopback local AWS endpoint")
    report = audit(
        aws_client("dynamodb", settings), max_pages=args.max_pages, page_size=args.page_size
    )
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
