from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from PIL import Image

from infrastructure.copydays_manifest import build_manifest, main


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


def test_distractors_have_independent_source_ids_and_splits(tmp_path: Path) -> None:
    originals = tmp_path / "original"
    attacks = tmp_path / "jpeg"
    distractors = tmp_path / "coco"
    for source in (200000, 200100):
        photo(originals / f"{source}.jpg")
        photo(attacks / f"{source + 1}.jpg")
    photo(distractors / "nested" / "scene.jpg")
    rows = build_manifest(originals, [attacks], distractor_dir=distractors)
    distractor = next(row for row in rows if row["expected_relationship"] == "DISTRACTOR")
    assert distractor["source_id"] == "distractor/nested/scene.jpg"
    assert distractor["image_id"] == distractor["source_id"]
    assert distractor["split"] in {"calibration", "held_out"}
    assert len(rows) == 5


def test_cli_reports_copydays_sources_separately_from_distractors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    originals = tmp_path / "original"
    attacks = tmp_path / "jpeg"
    distractors = tmp_path / "coco"
    for source in (200000, 200100):
        photo(originals / f"{source}.jpg")
        photo(attacks / f"{source + 1}.jpg")
    photo(distractors / "scene.jpg")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "copydays_manifest",
            "--original-dir",
            str(originals),
            "--attack-dir",
            str(attacks),
            "--distractor-dir",
            str(distractors),
            "--output",
            str(tmp_path / "manifest.jsonl"),
        ],
    )
    main()
    assert json.loads(capsys.readouterr().out) == {"rows": 5, "sources": 2, "distractors": 1}
