from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from PIL import Image

from vectorlake.near_copy import (
    DEFAULT_POLICY,
    POLICY_VERSION,
    Fingerprint,
    fingerprint,
    hamming,
    verify,
)
from vectorlake.validation import MAX_DIMENSION, MAX_OBJECT_BYTES, MAX_PIXELS


def _validated_path(row: dict[str, Any]) -> Path:
    path = Path(row["path"])
    if not path.is_file() or path.stat().st_size > MAX_OBJECT_BYTES:
        raise ValueError(f"missing or oversized image: {path}")
    if path.stat().st_size != row["size_bytes"]:
        raise ValueError(f"manifest size or checksum changed: {path}")
    with path.open("rb") as source:
        if hashlib.file_digest(source, "sha256").hexdigest() != row["sha256"]:
            raise ValueError(f"manifest checksum changed: {path}")
    with Image.open(path) as image:
        width, height = image.size
        if (
            min(width, height) < 1
            or max(width, height) > MAX_DIMENSION
            or width * height > MAX_PIXELS
        ):
            raise ValueError(f"image dimensions outside product limits: {path}")
    return path


def _fingerprint(path: Path) -> Fingerprint:
    with path.open("rb") as source:
        return fingerprint(source)


def _summary(bucket: dict[str, Any]) -> dict[str, Any]:
    positive = bucket["positive_queries"]
    tp = bucket["true_positive"]
    fp = bucket["false_positive"]
    durations = bucket.pop("durations_ms")
    return {
        **bucket,
        "candidate_recall_at_5": bucket["candidate_hits"] / positive if positive else 0.0,
        "precision": tp / (tp + fp) if tp + fp else 0.0,
        "recall": tp / positive if positive else 0.0,
        "verification_median_ms": statistics.median(durations) if durations else 0.0,
        "verification_p95_ms": (
            sorted(durations)[min(len(durations) - 1, int(len(durations) * 0.95))]
            if durations
            else 0.0
        ),
    }


def _bucket() -> dict[str, Any]:
    return {
        "positive_queries": 0,
        "candidate_hits": 0,
        "true_positive": 0,
        "false_positive": 0,
        "false_negative": 0,
        "negative_pairs_screened": 0,
        "negative_candidates_verified": 0,
        "verification_reasons": {},
        "durations_ms": [],
    }


def evaluate_manifest(rows: list[dict[str, Any]]) -> dict[str, Any]:
    originals: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    distractors: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    attacks: dict[str, list[dict[str, Any]]] = defaultdict(list)
    source_splits: dict[str, str] = {}
    image_ids: set[str] = set()
    for row in rows:
        source_id, split, image_id = row["source_id"], row["split"], row["image_id"]
        if split not in {"calibration", "held_out"}:
            raise ValueError(f"unknown split: {split}")
        if source_id in source_splits and source_splits[source_id] != split:
            raise ValueError(f"source split leakage: {source_id}")
        if image_id in image_ids:
            raise ValueError(f"duplicate image ID: {image_id}")
        source_splits[source_id] = split
        image_ids.add(image_id)
        _validated_path(row)
        if row["expected_relationship"] == "ORIGINAL":
            if source_id in originals[split] or source_id in distractors[split]:
                raise ValueError(f"multiple originals for source: {source_id}")
            originals[split][source_id] = row
        elif row["expected_relationship"] == "DISTRACTOR":
            if source_id in originals[split] or source_id in distractors[split]:
                raise ValueError(f"multiple originals for source: {source_id}")
            distractors[split][source_id] = row
        elif row["expected_relationship"] == "DERIVED_COPY":
            attacks[split].append(row)
        else:
            raise ValueError(f"unknown relationship: {row['expected_relationship']}")
    if not originals["calibration"] or not originals["held_out"]:
        raise ValueError("both source-disjoint splits need originals")
    for split in ("calibration", "held_out"):
        if not attacks[split]:
            raise ValueError(f"{split} has no attacked queries")
        for row in attacks[split]:
            if row["source_id"] not in originals[split]:
                raise ValueError(f"no original for attack: {row['image_id']}")

    report: dict[str, Any] = {
        "policy_version": POLICY_VERSION,
        "policy": DEFAULT_POLICY.__dict__,
        "sources": sum(len(group) for group in originals.values()),
        "distractors": sum(len(group) for group in distractors.values()),
        "suppression_approved": False,
        "splits": {},
    }
    for split in ("calibration", "held_out"):
        base = originals[split] | distractors[split]
        base_hashes = {
            source_id: _fingerprint(Path(row["path"])) for source_id, row in base.items()
        }
        totals = _bucket()
        by_transform: dict[str, dict[str, Any]] = defaultdict(_bucket)
        for row in attacks[split]:
            source_id = row["source_id"]
            path = Path(row["path"])
            query_hash = _fingerprint(path)
            metrics = (totals, by_transform[row["transformation"]])
            for bucket in metrics:
                bucket["positive_queries"] += 1
            distances = sorted(
                (
                    (other_id, hamming(query_hash.hex_value, value.hex_value))
                    for other_id, value in base_hashes.items()
                    if query_hash.quality >= DEFAULT_POLICY.min_pdq_quality
                    and value.quality >= DEFAULT_POLICY.min_pdq_quality
                ),
                key=lambda pair: (pair[1], pair[0]),
            )
            negatives = sum(other_id != source_id for other_id, _ in distances)
            for bucket in metrics:
                bucket["negative_pairs_screened"] += negatives
            candidates = [
                other_id
                for other_id, distance in distances[:5]
                if distance <= DEFAULT_POLICY.max_pdq_distance
            ]
            if source_id in candidates:
                for bucket in metrics:
                    bucket["candidate_hits"] += 1
            accepted_self = False
            for other_id in candidates:
                candidate_path = Path(base[other_id]["path"])
                start = time.perf_counter()
                with path.open("rb") as query, candidate_path.open("rb") as candidate:
                    result = verify(query, candidate)
                duration = (time.perf_counter() - start) * 1000
                for bucket in metrics:
                    bucket["durations_ms"].append(duration)
                    reasons = bucket["verification_reasons"]
                    reasons[result.reason] = reasons.get(result.reason, 0) + 1
                    if other_id != source_id:
                        bucket["negative_candidates_verified"] += 1
                    if result.accepted:
                        bucket["true_positive" if other_id == source_id else "false_positive"] += 1
                if result.accepted and other_id == source_id:
                    accepted_self = True
            if not accepted_self:
                for bucket in metrics:
                    bucket["false_negative"] += 1
        negative_queries: dict[str, Any] = {
            "queries": len(distractors[split]),
            "pairs_screened": 0,
            "candidates": 0,
            "verified_candidates": 0,
            "accepted_unlabeled_pairs": 0,
            "verification_reasons": {},
            "accepted_examples": [],
        }
        for source_id, row in distractors[split].items():
            query_hash = base_hashes[source_id]
            distances = sorted(
                (
                    (other_id, hamming(query_hash.hex_value, value.hex_value))
                    for other_id, value in base_hashes.items()
                    if other_id != source_id
                    and query_hash.quality >= DEFAULT_POLICY.min_pdq_quality
                    and value.quality >= DEFAULT_POLICY.min_pdq_quality
                ),
                key=lambda pair: (pair[1], pair[0]),
            )
            negative_queries["pairs_screened"] += len(distances)
            negative_candidates = [
                (other_id, distance)
                for other_id, distance in distances[:5]
                if distance <= DEFAULT_POLICY.max_pdq_distance
            ]
            negative_queries["candidates"] += len(negative_candidates)
            for other_id, distance in negative_candidates:
                with (
                    Path(row["path"]).open("rb") as query,
                    Path(base[other_id]["path"]).open("rb") as candidate,
                ):
                    result = verify(query, candidate)
                negative_queries["verified_candidates"] += 1
                reasons = negative_queries["verification_reasons"]
                reasons[result.reason] = reasons.get(result.reason, 0) + 1
                if result.accepted:
                    negative_queries["accepted_unlabeled_pairs"] += 1
                    if len(negative_queries["accepted_examples"]) < 20:
                        negative_queries["accepted_examples"].append(
                            {
                                "query_image_id": row["image_id"],
                                "candidate_image_id": base[other_id]["image_id"],
                                "pdq_distance": distance,
                            }
                        )
        report["splits"][split] = {
            "source_count": len(originals[split]),
            "distractor_count": len(distractors[split]),
            "overall": _summary(totals),
            "negative_queries": negative_queries,
            "by_transformation": {
                name: _summary(bucket) for name, bucket in sorted(by_transform.items())
            },
        }
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate the M4 policy on a Copydays manifest")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    rows = [json.loads(line) for line in args.manifest.read_text().splitlines() if line]
    report = evaluate_manifest(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
