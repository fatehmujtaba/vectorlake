from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

from PIL import Image

from vectorlake.validation import MAX_DIMENSION, MAX_OBJECT_BYTES, MAX_PIXELS

VIDEO_FRAME = re.compile(r"^frame_\d+_vid_(\d+)\.jpg$", re.IGNORECASE)
ANIMAL_FRAME = re.compile(r"^animal_evening_time_frame_\d+\.jpg$", re.IGNORECASE)


def validate_image(path: Path) -> None:
    size = path.stat().st_size
    if not 0 < size <= MAX_OBJECT_BYTES:
        raise ValueError(f"image outside product limits: {path}")
    with Image.open(path) as image:
        width, height = image.size
    if min(width, height) < 1 or max(width, height) > MAX_DIMENSION or width * height > MAX_PIXELS:
        raise ValueError(f"image outside product limits: {path}")


def _recording(relative: Path) -> str | None:
    video = VIDEO_FRAME.fullmatch(relative.name)
    if video:
        return f"video:{video.group(1)}"
    if relative.parts[0] == "animal" and ANIMAL_FRAME.fullmatch(relative.name):
        return "animal_evening_time"
    return None


def _identity(path: Path) -> dict[str, str | int]:
    with path.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    return {"size_bytes": path.stat().st_size, "sha256": digest}


def _variants(original: Path, directory: Path) -> dict[str, Path]:
    directory.mkdir(parents=True, exist_ok=True)
    with Image.open(original) as source:
        image = source.convert("RGB")
    width, height = image.size
    if min(width, height) < 20:
        raise ValueError(f"image too small for crop evaluation: {original}")
    crop_x, crop_y = round(width * 0.1), round(height * 0.1)
    outputs = {
        "jpeg_75": directory / "jpeg_75.jpg",
        "resize_70": directory / "resize_70.jpg",
        "crop_10": directory / "crop_10.jpg",
    }
    image.save(outputs["jpeg_75"], format="JPEG", quality=75)
    image.resize((round(width * 0.7), round(height * 0.7)), Image.Resampling.LANCZOS).save(
        outputs["resize_70"], format="JPEG", quality=95
    )
    image.crop((crop_x, crop_y, width - crop_x, height - crop_y)).save(
        outputs["crop_10"], format="JPEG", quality=95
    )
    return outputs


def build_manifest(
    dataset_dir: Path, generated_dir: Path, max_per_recording: int = 3
) -> list[dict[str, Any]]:
    if not dataset_dir.is_dir():
        raise ValueError(f"dataset directory is missing: {dataset_dir}")
    if max_per_recording < 1:
        raise ValueError("max_per_recording must be positive")
    recordings: dict[str, list[Path]] = defaultdict(list)
    for path in sorted(dataset_dir.rglob("*.jpg")):
        relative = path.relative_to(dataset_dir)
        recording = _recording(relative)
        if recording is not None:
            recordings[recording].append(path)
    if len(recordings) < 2:
        raise ValueError("need at least two independently identified recordings")

    non_animal = [name for name in recordings if name != "animal_evening_time"]
    ranked = sorted(non_animal, key=lambda name: (hashlib.sha256(name.encode()).digest(), name))
    held_out_count = max(1, min(len(recordings) - 1, round(len(recordings) * 0.3)))
    held_out = set(ranked[:held_out_count])

    rows: list[dict[str, Any]] = []
    seen_hashes: dict[str, str] = {}
    for recording, paths in sorted(recordings.items()):
        selected = sorted(
            paths,
            key=lambda path: (
                hashlib.sha256(path.relative_to(dataset_dir).as_posix().encode()).digest(),
                path,
            ),
        )
        count = 0
        for path in selected:
            validate_image(path)
            identity = _identity(path)
            previous = seen_hashes.get(str(identity["sha256"]))
            if previous is not None:
                if previous != recording:
                    raise ValueError(f"recording overlap: {previous} and {recording}")
                continue
            seen_hashes[str(identity["sha256"])] = recording
            relative = path.relative_to(dataset_dir)
            source_id = relative.with_suffix("").as_posix()
            split = "held_out" if recording in held_out else "calibration"
            common = {"source_id": source_id, "recording_id": recording, "split": split}
            rows.append(
                {
                    **common,
                    "image_id": relative.as_posix(),
                    "expected_relationship": "ORIGINAL",
                    "transformation": "original",
                    "parameters": {},
                    "path": str(path.resolve()),
                    **identity,
                }
            )
            for transformation, variant in _variants(path, generated_dir / source_id).items():
                validate_image(variant)
                rows.append(
                    {
                        **common,
                        "image_id": f"generated/{source_id}/{variant.name}",
                        "expected_relationship": "DERIVED_COPY",
                        "transformation": transformation,
                        "parameters": (
                            {"quality": 75}
                            if transformation == "jpeg_75"
                            else {"scale": 0.7}
                            if transformation == "resize_70"
                            else {"border_fraction": 0.1}
                        ),
                        "path": str(variant.resolve()),
                        **_identity(variant),
                    }
                )
            count += 1
            if count == max_per_recording:
                break
    if not {"calibration", "held_out"}.issubset({row["split"] for row in rows}):
        raise ValueError("both recording-disjoint splits need sources")
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a recording-disjoint Kaggle evaluation manifest"
    )
    parser.add_argument("--dataset-dir", type=Path, required=True)
    parser.add_argument("--generated-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-per-recording", type=int, default=3)
    args = parser.parse_args()
    rows = build_manifest(args.dataset_dir, args.generated_dir, args.max_per_recording)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows))
    print(
        json.dumps(
            {
                "sources": sum(row["expected_relationship"] == "ORIGINAL" for row in rows),
                "generated_copies": sum(
                    row["expected_relationship"] == "DERIVED_COPY" for row in rows
                ),
                "recordings": len({row["recording_id"] for row in rows}),
            }
        )
    )


if __name__ == "__main__":
    main()
