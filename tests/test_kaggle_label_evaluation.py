from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from PIL import Image
from skimage import data

from infrastructure.kaggle_label_evaluation import label_and_evaluate


def _pair(animal: Path, left: str, right: str) -> dict[str, object]:
    paths = [animal / left, animal / right]
    return {
        "left": str(paths[0]),
        "right": str(paths[1]),
        "left_sha256": hashlib.sha256(paths[0].read_bytes()).hexdigest(),
        "right_sha256": hashlib.sha256(paths[1].read_bytes()).hexdigest(),
        "label": "UNREVIEWED",
    }


def _animal_pair(tmp_path: Path) -> tuple[Path, Path]:
    animal = tmp_path / "animal"
    animal.mkdir()
    original = Image.fromarray(data.astronaut())  # type: ignore[no-untyped-call]
    original.save(animal / "animal_evening_time_frame_10.jpg")
    original.save(animal / "animal_evening_time_frame_20.jpg", quality=80)
    review = tmp_path / "review.json"
    review.write_text(
        json.dumps(
            [
                _pair(
                    animal,
                    "animal_evening_time_frame_10.jpg",
                    "animal_evening_time_frame_20.jpg",
                )
            ]
        )
    )
    return animal, review


def test_different_timestamps_are_labeled_distinct_and_reverified(tmp_path: Path) -> None:
    animal, review = _animal_pair(tmp_path)
    labeled = tmp_path / "labeled.json"
    report_path = tmp_path / "report.json"
    report = label_and_evaluate(animal, review, labeled, report_path)
    assert report["labeled_distinct_pairs"] == 1
    assert report["review_pair_verifier_false_accepts"] == 1
    assert report["animal_pool_candidate_pairs"] == 1
    assert report["animal_pool_false_positive_pairs"] == 1
    assert report["candidate_pool_order"] == "prior_timestamp_only"
    assert report["suppression_approved"] is False
    assert json.loads(labeled.read_text())[0]["label"] == "DISTINCT_OBSERVATION"
    assert json.loads(report_path.read_text()) == report


def test_rejects_changed_image_before_labeling(tmp_path: Path) -> None:
    animal, review = _animal_pair(tmp_path)
    Image.new("RGB", (512, 512), (20, 30, 40)).save(animal / "animal_evening_time_frame_20.jpg")
    with pytest.raises(ValueError, match="checksum"):
        label_and_evaluate(animal, review, tmp_path / "labeled.json", tmp_path / "report.json")


def test_rejects_same_timestamp_and_existing_label(tmp_path: Path) -> None:
    animal, review = _animal_pair(tmp_path)
    rows = json.loads(review.read_text())
    rows[0]["right"] = rows[0]["left"]
    rows[0]["right_sha256"] = rows[0]["left_sha256"]
    review.write_text(json.dumps(rows))
    with pytest.raises(ValueError, match="different timestamps"):
        label_and_evaluate(animal, review, tmp_path / "labeled.json", tmp_path / "report.json")


def test_rejects_overwriting_labeled_output(tmp_path: Path) -> None:
    animal, review = _animal_pair(tmp_path)
    labeled = tmp_path / "labeled.json"
    labeled.write_text("already reviewed")
    with pytest.raises(ValueError, match="exists"):
        label_and_evaluate(animal, review, labeled, tmp_path / "report.json")
    assert labeled.read_text() == "already reviewed"
