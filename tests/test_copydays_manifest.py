from __future__ import annotations

from pathlib import Path

import pytest
from PIL import Image

from infrastructure.copydays_manifest import build_manifest


def photo(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (16, 16), (30, 100, 150)).save(path)


def test_source_groups_never_cross_splits(tmp_path: Path) -> None:
    originals = tmp_path / "original"
    attacks = tmp_path / "jpeg"
    for source in (200000, 200100, 200200, 200300):
        photo(originals / f"{source}.jpg")
        photo(attacks / "50" / f"{source + 1}.jpg")
    rows = build_manifest(originals, [attacks])
    assert len(rows) == 8
    assert {row["split"] for row in rows} == {"calibration", "held_out"}
    for source in {row["source_id"] for row in rows}:
        assert len({row["split"] for row in rows if row["source_id"] == source}) == 1
    assert sum(row["expected_relationship"] == "DERIVED_COPY" for row in rows) == 4
    assert all(len(row["sha256"]) == 64 and row["size_bytes"] > 0 for row in rows)


def test_unmatched_attack_is_rejected(tmp_path: Path) -> None:
    originals = tmp_path / "original"
    attacks = tmp_path / "strong"
    photo(originals / "200000.jpg")
    photo(originals / "200100.jpg")
    photo(attacks / "200201.jpg")
    with pytest.raises(ValueError, match="no original"):
        build_manifest(originals, [attacks])


def test_duplicate_original_source_is_rejected(tmp_path: Path) -> None:
    originals = tmp_path / "original"
    photo(originals / "200000.jpg")
    photo(originals / "200001.jpg")
    with pytest.raises(ValueError, match="multiple originals"):
        build_manifest(originals, [])
