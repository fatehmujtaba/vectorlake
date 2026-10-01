from __future__ import annotations

from pathlib import Path

import pytest
from PIL import Image

from infrastructure.kaggle_manifest import build_manifest, validate_image


def photo(path: Path, color: tuple[int, int, int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (80, 80), color).save(path)


def test_manifest_groups_recordings_and_generates_labeled_copies(tmp_path: Path) -> None:
    dataset = tmp_path / "dataset"
    photo(dataset / "animal" / "animal_evening_time_frame_7.jpg", (10, 20, 30))
    photo(dataset / "Climbing" / "frame_1_vid_101.jpg", (40, 50, 60))
    photo(dataset / "Crawling" / "frame_2_vid_101.jpg", (70, 80, 90))
    photo(dataset / "walking" / "frame_1_vid_202.jpg", (100, 110, 120))
    photo(dataset / "walking" / "frame_1.jpg", (130, 140, 150))
    rows = build_manifest(dataset, tmp_path / "generated", max_per_recording=2)
    originals = [row for row in rows if row["expected_relationship"] == "ORIGINAL"]
    assert len(originals) == 4
    assert len(rows) == 16
    assert {row["transformation"] for row in rows} == {
        "original",
        "jpeg_75",
        "resize_70",
        "crop_10",
    }
    assert {row["split"] for row in originals if "vid_101" in row["image_id"]} == {
        next(row["split"] for row in originals if "vid_101" in row["image_id"])
    }
    assert next(row["split"] for row in originals if row["image_id"].startswith("animal/")) == (
        "calibration"
    )
    assert all(Path(row["path"]).is_file() for row in rows)
    assert all(len(row["sha256"]) == 64 for row in rows)


def test_manifest_rejects_exact_source_overlap_across_recordings(tmp_path: Path) -> None:
    dataset = tmp_path / "dataset"
    photo(dataset / "Climbing" / "frame_1_vid_101.jpg", (10, 20, 30))
    photo(dataset / "walking" / "frame_1_vid_202.jpg", (10, 20, 30))
    with pytest.raises(ValueError, match="recording overlap"):
        build_manifest(dataset, tmp_path / "generated", max_per_recording=1)


def test_image_limits_are_checked_before_decoding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "image.jpg"
    photo(path, (10, 20, 30))
    monkeypatch.setattr("infrastructure.kaggle_manifest.MAX_PIXELS", 1000)
    with pytest.raises(ValueError, match="product limits"):
        validate_image(path)
