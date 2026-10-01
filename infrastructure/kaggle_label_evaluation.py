from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any

from infrastructure.kaggle_manifest import validate_image
from vectorlake.near_copy import (
    DEFAULT_POLICY,
    POLICY_VERSION,
    Fingerprint,
    fingerprint,
    hamming,
    verify,
)

FRAME_NUMBER = re.compile(r"^animal_evening_time_frame_(\d+)\.jpg$", re.IGNORECASE)
LABEL_BASIS = (
    "owner-approved: different frame-numbered captures in animal_evening_time "
    "are distinct observations"
)


def _digest(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def _frame_number(path: Path) -> int:
    match = FRAME_NUMBER.fullmatch(path.name)
    if match is None:
        raise ValueError(f"not a timestamped animal frame: {path}")
    return int(match.group(1))


def _review_path(animal_dir: Path, value: str, expected_sha: str) -> tuple[Path, int]:
    path = Path(value).resolve()
    if path.parent != animal_dir.resolve():
        raise ValueError(f"review image outside animal directory: {path}")
    timestamp = _frame_number(path)
    validate_image(path)
    if _digest(path) != expected_sha:
        raise ValueError(f"review image checksum changed: {path}")
    return path, timestamp


def _candidate_ids(query: Path, hashes: dict[Path, Fingerprint]) -> list[Path]:
    query_hash = hashes[query]
    query_time = _frame_number(query)
    if query_hash.quality < DEFAULT_POLICY.min_pdq_quality:
        return []
    distances = sorted(
        (
            (hamming(query_hash.hex_value, value.hex_value), path)
            for path, value in hashes.items()
            if _frame_number(path) < query_time and value.quality >= DEFAULT_POLICY.min_pdq_quality
        ),
        key=lambda item: (item[0], str(item[1])),
    )
    return [path for distance, path in distances[:5] if distance <= DEFAULT_POLICY.max_pdq_distance]


def label_and_evaluate(
    animal_dir: Path,
    review_json: Path,
    labels_output: Path,
    report_output: Path,
    sample_size: int = 48,
) -> dict[str, Any]:
    if sample_size < 1:
        raise ValueError("sample_size must be positive")
    if labels_output == report_output or labels_output.exists() or report_output.exists():
        raise ValueError("labeled or report output already exists")
    rows = json.loads(review_json.read_text())
    if not isinstance(rows, list) or not rows:
        raise ValueError("review queue is empty or invalid")

    labeled: list[dict[str, Any]] = []
    seen_pairs: set[frozenset[Path]] = set()
    for row in rows:
        if row.get("label") != "UNREVIEWED":
            raise ValueError("review queue contains an existing label")
        left, left_time = _review_path(animal_dir, row["left"], row["left_sha256"])
        right, right_time = _review_path(animal_dir, row["right"], row["right_sha256"])
        if left_time == right_time or left == right:
            raise ValueError("the distinct-frame rule requires different timestamps")
        pair_id = frozenset((left, right))
        if pair_id in seen_pairs:
            raise ValueError("duplicate review pair")
        seen_pairs.add(pair_id)
        with left.open("rb") as query, right.open("rb") as candidate:
            verification = verify(query, candidate)
        labeled.append(
            {
                **row,
                "label": "DISTINCT_OBSERVATION",
                "label_basis": LABEL_BASIS,
                "current_policy_version": POLICY_VERSION,
                "current_verifier_accepted": verification.accepted,
                "current_verifier_reason": verification.reason,
            }
        )

    paths = sorted(
        path.resolve()
        for path in animal_dir.glob("*.jpg")
        if path.is_file() and FRAME_NUMBER.fullmatch(path.name)
    )
    hashes: dict[Path, Fingerprint] = {}
    for path in paths:
        validate_image(path)
        with path.open("rb") as source:
            hashes[path] = fingerprint(source)
    sampled = sorted(
        paths, key=lambda path: (hashlib.sha256(path.name.encode()).digest(), path.name)
    )[:sample_size]
    accepted_queries = 0
    accepted_comparisons = 0
    verified_comparisons = 0
    candidate_comparisons = 0
    accepted_examples: list[dict[str, str]] = []
    for query_path in sampled:
        accepted_query = False
        candidates = _candidate_ids(query_path, hashes)
        candidate_comparisons += len(candidates)
        for candidate_path in candidates:
            if _digest(query_path) == _digest(candidate_path):
                raise ValueError("exact-byte collision across distinct animal timestamps")
            with query_path.open("rb") as query, candidate_path.open("rb") as candidate:
                result = verify(query, candidate)
            verified_comparisons += 1
            if result.accepted:
                accepted_comparisons += 1
                accepted_query = True
                if len(accepted_examples) < 20:
                    accepted_examples.append(
                        {"query": query_path.name, "candidate": candidate_path.name}
                    )
        accepted_queries += accepted_query

    report: dict[str, Any] = {
        "policy_version": POLICY_VERSION,
        "label_basis": LABEL_BASIS,
        "candidate_pool_order": "prior_timestamp_only",
        "labeled_distinct_pairs": len(labeled),
        "review_pair_verifier_false_accepts": sum(
            row["current_verifier_accepted"] for row in labeled
        ),
        "animal_pool_size": len(paths),
        "animal_pool_sampled_queries": len(sampled),
        "animal_pool_candidate_pairs": candidate_comparisons,
        "animal_pool_verified_pairs": verified_comparisons,
        "animal_pool_false_positive_queries": accepted_queries,
        "animal_pool_false_positive_pairs": accepted_comparisons,
        "accepted_examples": accepted_examples,
        "suppression_approved": False,
    }
    labels_output.parent.mkdir(parents=True, exist_ok=True)
    report_output.parent.mkdir(parents=True, exist_ok=True)
    labels_output.write_text(json.dumps(labeled, indent=2) + "\n")
    report_output.write_text(json.dumps(report, indent=2) + "\n")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate owner-labeled distinct animal frames")
    parser.add_argument("--animal-dir", type=Path, required=True)
    parser.add_argument("--review-json", type=Path, required=True)
    parser.add_argument("--labels-output", type=Path, required=True)
    parser.add_argument("--report-output", type=Path, required=True)
    parser.add_argument("--sample-size", type=int, default=48)
    args = parser.parse_args()
    print(
        json.dumps(
            label_and_evaluate(
                args.animal_dir,
                args.review_json,
                args.labels_output,
                args.report_output,
                args.sample_size,
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
