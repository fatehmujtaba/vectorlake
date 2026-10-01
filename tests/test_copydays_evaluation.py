from __future__ import annotations

from pathlib import Path

import pytest
from PIL import Image
from skimage import data

from infrastructure.copydays_evaluation import evaluate_manifest
from infrastructure.copydays_manifest import build_manifest


def photo(path: Path, color: tuple[int, int, int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (32, 32), color).save(path)


def test_report_has_source_disjoint_splits_and_explicit_negative_counts(tmp_path: Path) -> None:
    originals = tmp_path / "original"
    attacks = tmp_path / "jpeg"
    for source, color in ((200000, (20, 30, 40)), (200100, (200, 180, 10))):
        photo(originals / f"{source}.jpg", color)
        photo(attacks / "50" / f"{source + 1}.jpg", color)
    report = evaluate_manifest(build_manifest(originals, [attacks]))
    assert report["sources"] == 2
    assert report["splits"]["calibration"]["overall"]["positive_queries"] == 1
    assert report["splits"]["held_out"]["overall"]["positive_queries"] == 1
    assert report["splits"]["held_out"]["overall"]["negative_pairs_screened"] == 0
    assert report["policy_version"] == "near-copy-shadow-v2"
    assert report["suppression_approved"] is False


def test_report_counts_verification_reasons(tmp_path: Path) -> None:
    originals = tmp_path / "original"
    attacks = tmp_path / "jpeg"
    for source in (200000, 200100):
        photo_path = originals / f"{source}.jpg"
        photo_path.parent.mkdir(parents=True, exist_ok=True)
        Image.fromarray(data.astronaut()).save(photo_path)  # type: ignore[no-untyped-call]
        attack_path = attacks / f"{source + 1}.jpg"
        attack_path.parent.mkdir(parents=True, exist_ok=True)
        attack_path.write_bytes(photo_path.read_bytes())
    report = evaluate_manifest(build_manifest(originals, [attacks]))
    for split in ("calibration", "held_out"):
        assert report["splits"][split]["overall"]["verification_reasons"] == {"accepted": 1}


def test_report_includes_distractors_in_candidate_pool(tmp_path: Path) -> None:
    originals = tmp_path / "original"
    attacks = tmp_path / "jpeg"
    distractors = tmp_path / "coco"
    for source in (200000, 200100):
        path = originals / f"{source}.jpg"
        path.parent.mkdir(parents=True, exist_ok=True)
        Image.fromarray(data.astronaut()).save(path)  # type: ignore[no-untyped-call]
        copy = attacks / f"{source + 1}.jpg"
        copy.parent.mkdir(parents=True, exist_ok=True)
        copy.write_bytes(path.read_bytes())
    distractors.mkdir()
    Image.fromarray(data.coffee()).save(distractors / "scene.jpg")  # type: ignore[no-untyped-call]
    report = evaluate_manifest(build_manifest(originals, [attacks], distractors))
    assert report["sources"] == 2
    assert report["distractors"] == 1
    assert sum(split["distractor_count"] for split in report["splits"].values()) == 1
    assert (
        sum(split["overall"]["negative_pairs_screened"] for split in report["splits"].values()) == 1
    )
    assert sum(split["negative_queries"]["queries"] for split in report["splits"].values()) == 1
    assert (
        sum(split["negative_queries"]["pairs_screened"] for split in report["splits"].values()) == 1
    )


def test_negative_query_reports_accepted_unlabeled_collision(tmp_path: Path) -> None:
    originals = tmp_path / "original"
    attacks = tmp_path / "jpeg"
    distractors = tmp_path / "coco"
    for source in (200000, 200100):
        path = originals / f"{source}.jpg"
        path.parent.mkdir(parents=True, exist_ok=True)
        Image.fromarray(data.astronaut()).save(path)  # type: ignore[no-untyped-call]
        copy = attacks / f"{source + 1}.jpg"
        copy.parent.mkdir(parents=True, exist_ok=True)
        copy.write_bytes(path.read_bytes())
    distractors.mkdir()
    (distractors / "same-scene.jpg").write_bytes((originals / "200000.jpg").read_bytes())
    rows = build_manifest(originals, [attacks], distractors)
    split = next(row["split"] for row in rows if row["image_id"] == "original/200000.jpg")
    next(row for row in rows if row["expected_relationship"] == "DISTRACTOR")["split"] = split
    report = evaluate_manifest(rows)
    negative_queries = report["splits"][split]["negative_queries"]
    assert negative_queries["verified_candidates"] >= 1
    assert negative_queries["accepted_unlabeled_pairs"] >= 1
    assert negative_queries["accepted_examples"][0]["query_image_id"] == (
        "distractor/same-scene.jpg"
    )


def test_evaluation_rejects_split_leakage(tmp_path: Path) -> None:
    originals = tmp_path / "original"
    attacks = tmp_path / "jpeg"
    for source in (200000, 200100):
        photo(originals / f"{source}.jpg", (20, 30, 40))
        photo(attacks / f"{source + 1}.jpg", (20, 30, 40))
    rows = build_manifest(originals, [attacks])
    attacked = next(
        row for row in rows if row["source_id"] == "200000" and row["transformation"] != "original"
    )
    attacked["split"] = "held_out" if attacked["split"] == "calibration" else "calibration"
    with pytest.raises(ValueError, match="source split leakage"):
        evaluate_manifest(rows)


def test_evaluation_rejects_changed_file(tmp_path: Path) -> None:
    originals = tmp_path / "original"
    attacks = tmp_path / "jpeg"
    for source in (200000, 200100):
        photo(originals / f"{source}.jpg", (20, 30, 40))
        photo(attacks / f"{source + 1}.jpg", (20, 30, 40))
    rows = build_manifest(originals, [attacks])
    photo(attacks / "200001.jpg", (80, 90, 100))
    with pytest.raises(ValueError, match="checksum"):
        evaluate_manifest(rows)
