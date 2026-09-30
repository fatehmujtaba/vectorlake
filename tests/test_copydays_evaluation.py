from __future__ import annotations

from pathlib import Path

import pytest
from PIL import Image

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
    assert report["suppression_approved"] is False


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
