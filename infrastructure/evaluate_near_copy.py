from __future__ import annotations

import json
import statistics
import time
from io import BytesIO
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image
from skimage import data

from vectorlake.near_copy import DEFAULT_POLICY, fingerprint, hamming, verify

SOURCES = {
    "astronaut": data.astronaut,
    "coffee": data.coffee,
    "chelsea": data.chelsea,
    "rocket": data.rocket,
    "camera": data.camera,
    "coins": data.coins,
}
SPLITS = {
    "calibration": ("astronaut", "coffee", "camera"),
    "held_out": ("chelsea", "rocket", "coins"),
}


def _encode(image: Image.Image, quality: int = 95) -> bytes:
    stream = BytesIO()
    image.convert("RGB").save(stream, format="JPEG", quality=quality)
    return stream.getvalue()


def _variants(image: Image.Image) -> list[tuple[str, bytes, dict[str, Any]]]:
    width, height = image.size
    crop = image.crop(
        (int(width * 0.05), int(height * 0.05), int(width * 0.95), int(height * 0.95))
    )
    scaled = image.resize((int(width * 0.7), int(height * 0.7)), Image.Resampling.LANCZOS)
    return [
        ("jpeg_80", _encode(image, 80), {"quality": 80}),
        ("jpeg_40", _encode(image, 40), {"quality": 40}),
        ("scale_70", _encode(scaled), {"scale": 0.7}),
        ("crop_5", _encode(crop), {"border_fraction": 0.05}),
    ]


def evaluate(output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {"policy": DEFAULT_POLICY.__dict__, "splits": {}}
    manifest: list[dict[str, Any]] = []
    for split, names in SPLITS.items():
        originals = {
            name: _encode(
                Image.fromarray(np.asarray(SOURCES[name]()).astype(np.uint8))  # type: ignore[no-untyped-call]
            )
            for name in names
        }
        hashes = {name: fingerprint(BytesIO(payload)) for name, payload in originals.items()}
        positives = 0
        candidate_hits = 0
        tp = fp = fn = 0
        durations: list[float] = []
        reasons: dict[str, int] = {}
        for source_id in names:
            image = Image.open(BytesIO(originals[source_id]))
            for transformation, payload, parameters in _variants(image):
                positives += 1
                manifest.append(
                    {
                        "image_id": f"{source_id}-{transformation}",
                        "source_id": source_id,
                        "expected_relationship": "near_copy",
                        "transformation": transformation,
                        "parameters": parameters,
                        "split": split,
                    }
                )
                query_hash = fingerprint(BytesIO(payload))
                distances = sorted(
                    (
                        (name, hamming(query_hash.hex_value, value.hex_value))
                        for name, value in hashes.items()
                        if query_hash.quality >= DEFAULT_POLICY.min_pdq_quality
                        and value.quality >= DEFAULT_POLICY.min_pdq_quality
                    ),
                    key=lambda item: item[1],
                )
                candidates = [
                    name
                    for name, distance in distances[:5]
                    if distance <= DEFAULT_POLICY.max_pdq_distance
                ]
                candidate_hits += source_id in candidates
                accepted_self = False
                for name in candidates:
                    start = time.perf_counter()
                    result = verify(BytesIO(payload), BytesIO(originals[name]))
                    durations.append((time.perf_counter() - start) * 1000)
                    reasons[result.reason] = reasons.get(result.reason, 0) + 1
                    if result.accepted:
                        if name == source_id:
                            accepted_self = True
                            tp += 1
                        else:
                            fp += 1
                if not accepted_self:
                    fn += 1
        report["splits"][split] = {
            "source_count": len(names),
            "positive_queries": positives,
            "candidate_recall": candidate_hits / positives,
            "true_positive": tp,
            "false_positive": fp,
            "false_negative": fn,
            "precision": tp / (tp + fp) if tp + fp else 0.0,
            "recall": tp / positives,
            "verification_median_ms": statistics.median(durations) if durations else 0.0,
            "verification_max_ms": max(durations, default=0.0),
            "reasons": reasons,
        }
    (output_dir / "synthetic_manifest.jsonl").write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in manifest)
    )
    (output_dir / "synthetic_report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


if __name__ == "__main__":
    print(json.dumps(evaluate(Path(".artifacts/m4")), indent=2))
