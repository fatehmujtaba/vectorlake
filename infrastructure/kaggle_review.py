from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageOps

from infrastructure.kaggle_manifest import ANIMAL_FRAME, validate_image
from vectorlake.near_copy import DEFAULT_POLICY, fingerprint, hamming, verify

PREVIEW_SIZE = (300, 300)


def _preview(path: Path) -> Image.Image:
    with Image.open(path) as source:
        image = ImageOps.exif_transpose(source).convert("RGB")
    return ImageOps.contain(image, PREVIEW_SIZE, Image.Resampling.LANCZOS)


def make_review(
    animal_dir: Path, output_json: Path, output_image: Path, limit: int = 12
) -> list[dict[str, Any]]:
    if limit < 1:
        raise ValueError("review limit must be positive")
    if output_json.exists():
        existing = json.loads(output_json.read_text())
        if any(pair.get("label") != "UNREVIEWED" for pair in existing):
            raise ValueError("labeled review queue exists; choose a new output path")
    paths = sorted(path for path in animal_dir.glob("*.jpg") if ANIMAL_FRAME.fullmatch(path.name))
    if len(paths) < 2:
        raise ValueError("need at least two animal frames")
    hashes = []
    for path in paths:
        validate_image(path)
        with path.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
            source.seek(0)
            pdq = fingerprint(source)
        hashes.append((digest, pdq))

    candidates = []
    for left in range(len(paths)):
        for right in range(left + 1, len(paths)):
            left_sha, left_pdq = hashes[left]
            right_sha, right_pdq = hashes[right]
            if (
                left_sha == right_sha
                or left_pdq.quality < DEFAULT_POLICY.min_pdq_quality
                or right_pdq.quality < DEFAULT_POLICY.min_pdq_quality
            ):
                continue
            distance = hamming(left_pdq.hex_value, right_pdq.hex_value)
            if distance <= DEFAULT_POLICY.max_pdq_distance:
                candidates.append((distance, left, right))
    candidates.sort()

    used: set[int] = set()
    review: list[dict[str, Any]] = []
    for distance, left, right in candidates:
        if left in used or right in used:
            continue
        with paths[left].open("rb") as query, paths[right].open("rb") as candidate:
            result = verify(query, candidate)
        review.append(
            {
                "left": str(paths[left].resolve()),
                "right": str(paths[right].resolve()),
                "left_sha256": hashes[left][0],
                "right_sha256": hashes[right][0],
                "pdq_distance": distance,
                "shadow_accepted": result.accepted,
                "shadow_reason": result.reason,
                "label": "UNREVIEWED",
            }
        )
        used.update((left, right))
        if len(review) == limit:
            break

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(review, indent=2) + "\n")
    sheet = Image.new("RGB", (640, max(1, len(review)) * 360), "white")
    draw = ImageDraw.Draw(sheet)
    for index, pair in enumerate(review):
        top = index * 360
        for column, key in enumerate(("left", "right")):
            path = Path(pair[key])
            preview = _preview(path)
            sheet.paste(preview, (column * 320, top + 32))
            draw.text((column * 320 + 4, top + 4), f"{index + 1}. {path.name}", fill="black")
        draw.text(
            (4, top + 338),
            f"PDQ {pair['pdq_distance']} | shadow {pair['shadow_reason']} | UNREVIEWED",
            fill="black",
        )
    output_image.parent.mkdir(parents=True, exist_ok=True)
    sheet.save(output_image, format="JPEG", quality=90)
    return review


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare unlabeled animal-frame hard negatives")
    parser.add_argument("--animal-dir", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-image", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=12)
    args = parser.parse_args()
    review = make_review(args.animal_dir, args.output_json, args.output_image, args.limit)
    print(
        json.dumps(
            {
                "unreviewed_pairs": len(review),
                "shadow_accepted": sum(pair["shadow_accepted"] for pair in review),
            }
        )
    )


if __name__ == "__main__":
    main()
