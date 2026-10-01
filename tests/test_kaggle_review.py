from __future__ import annotations

from pathlib import Path

import pytest
from PIL import Image, ImageEnhance
from skimage import data

from infrastructure.kaggle_review import make_review


def test_review_surfaces_similar_distinct_files_without_labeling_them(tmp_path: Path) -> None:
    animal = tmp_path / "animal"
    animal.mkdir()
    original = Image.fromarray(data.astronaut())  # type: ignore[no-untyped-call]
    original.save(animal / "animal_evening_time_frame_10.jpg")
    ImageEnhance.Brightness(original).enhance(1.02).save(
        animal / "animal_evening_time_frame_20.jpg"
    )
    Image.fromarray(data.coffee()).save(  # type: ignore[no-untyped-call]
        animal / "animal_evening_time_frame_30.jpg"
    )
    pairs = make_review(animal, tmp_path / "review.json", tmp_path / "review.jpg", limit=1)
    assert len(pairs) == 1
    assert {Path(pairs[0][key]).name for key in ("left", "right")} == {
        "animal_evening_time_frame_10.jpg",
        "animal_evening_time_frame_20.jpg",
    }
    assert pairs[0]["label"] == "UNREVIEWED"
    assert (tmp_path / "review.json").is_file()
    assert (tmp_path / "review.jpg").is_file()


def test_regeneration_does_not_erase_completed_review(tmp_path: Path) -> None:
    animal = tmp_path / "animal"
    animal.mkdir()
    original = Image.fromarray(data.astronaut())  # type: ignore[no-untyped-call]
    original.save(animal / "animal_evening_time_frame_10.jpg")
    ImageEnhance.Brightness(original).enhance(1.02).save(
        animal / "animal_evening_time_frame_20.jpg"
    )
    review_json = tmp_path / "review.json"
    make_review(animal, review_json, tmp_path / "review.jpg", limit=1)
    reviewed = review_json.read_text().replace("UNREVIEWED", "DISTINCT_OBSERVATION")
    review_json.write_text(reviewed)
    with pytest.raises(ValueError, match="labeled review queue"):
        make_review(animal, review_json, tmp_path / "review.jpg", limit=1)
    assert review_json.read_text() == reviewed
