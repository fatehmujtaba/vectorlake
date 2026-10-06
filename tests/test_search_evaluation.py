from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from infrastructure.evaluate_image_search import TRANSFORMS, load_calibration


def manifest(tmp_path: Path) -> Path:
    data = b"fixture image bytes"
    image = tmp_path / "image.jpg"
    image.write_bytes(data)
    rows = [
        {
            "source_id": "source-1",
            "split": "calibration",
            "transformation": transform,
            "path": str(image),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
        for transform in TRANSFORMS
    ]
    rows.append(
        {
            "source_id": "final-source",
            "split": "held_out",
            "transformation": "original",
            "path": str(tmp_path / "held-out-file-must-not-be-opened.jpg"),
            "sha256": "unscored",
        }
    )
    path = tmp_path / "manifest.jsonl"
    path.write_text("\n".join(json.dumps(row) for row in rows))
    return path


def test_retrieval_evaluation_does_not_open_held_out_images(tmp_path: Path) -> None:
    rows = load_calibration(manifest(tmp_path))
    assert len(rows) == 4
    assert {row["split"] for row in rows} == {"calibration"}


def test_retrieval_evaluation_rejects_changed_images(tmp_path: Path) -> None:
    path = manifest(tmp_path)
    (tmp_path / "image.jpg").write_bytes(b"modified bytes")
    with pytest.raises(ValueError, match="checksum"):
        load_calibration(path)
