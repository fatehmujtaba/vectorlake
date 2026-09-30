from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png"}


def _images(directory: Path) -> list[Path]:
    if not directory.is_dir():
        raise ValueError(f"dataset directory is missing: {directory}")
    return sorted(
        path
        for path in directory.rglob("*")
        if path.is_file() and path.suffix.lower() in IMAGE_SUFFIXES
    )


def _source_id(path: Path) -> str:
    if not path.stem.isdecimal():
        raise ValueError(f"Copydays filename is not numeric: {path.name}")
    return str((int(path.stem) // 100) * 100)


def _file_identity(path: Path) -> dict[str, str | int]:
    with path.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    return {"size_bytes": path.stat().st_size, "sha256": digest}


def build_manifest(original_dir: Path, attack_dirs: list[Path]) -> list[dict[str, Any]]:
    originals: dict[str, Path] = {}
    for path in _images(original_dir):
        source_id = _source_id(path)
        if source_id in originals:
            raise ValueError(f"multiple originals for source {source_id}")
        originals[source_id] = path.resolve()
    if len(originals) < 2:
        raise ValueError("Copydays evaluation requires at least two original sources")

    ranked = sorted(
        originals,
        key=lambda source: (hashlib.sha256(source.encode()).digest(), source),
    )
    held_out_count = max(1, min(len(ranked) - 1, round(len(ranked) * 0.3)))
    held_out = set(ranked[:held_out_count])

    def split(source_id: str) -> str:
        return "held_out" if source_id in held_out else "calibration"

    rows: list[dict[str, Any]] = [
        {
            "image_id": f"original/{path.name}",
            "source_id": source_id,
            "expected_relationship": "ORIGINAL",
            "transformation": "original",
            "parameters": {},
            "split": split(source_id),
            "path": str(path),
            **_file_identity(path),
        }
        for source_id, path in sorted(originals.items())
    ]
    for attack_dir in attack_dirs:
        for path in _images(attack_dir):
            source_id = _source_id(path)
            if source_id not in originals:
                raise ValueError(f"no original for attack {path}")
            relative = path.relative_to(attack_dir)
            transformation = "/".join((attack_dir.name, *relative.parent.parts)).rstrip("/.")
            rows.append(
                {
                    "image_id": f"{attack_dir.name}/{relative.as_posix()}",
                    "source_id": source_id,
                    "expected_relationship": "DERIVED_COPY",
                    "transformation": transformation,
                    "parameters": {},
                    "split": split(source_id),
                    "path": str(path.resolve()),
                    **_file_identity(path),
                }
            )
    if len({row["image_id"] for row in rows}) != len(rows):
        raise ValueError("manifest image IDs are not unique")
    if not any(row["expected_relationship"] == "DERIVED_COPY" for row in rows):
        raise ValueError("no attacked images found")
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a source-disjoint Copydays JSONL manifest")
    parser.add_argument("--original-dir", type=Path, required=True)
    parser.add_argument("--attack-dir", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    rows = build_manifest(args.original_dir, args.attack_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows))
    print(json.dumps({"rows": len(rows), "sources": len({row["source_id"] for row in rows})}))


if __name__ == "__main__":
    main()
