from __future__ import annotations

from io import BytesIO

import cv2
import numpy as np
from PIL import Image
from skimage import data

from vectorlake.near_copy import NearCopyPolicy, fingerprint, hamming, verify


def image_bytes(pixels: np.ndarray, *, quality: int = 95) -> BytesIO:
    stream = BytesIO()
    Image.fromarray(pixels, "RGB").save(stream, format="JPEG", quality=quality)
    stream.seek(0)
    return stream


def textured(seed: int = 1) -> np.ndarray:
    rng = np.random.default_rng(seed)
    pixels = rng.integers(0, 256, (420, 520, 3), dtype=np.uint8)
    for index in range(30):
        center = (int(rng.integers(20, 500)), int(rng.integers(20, 400)))
        cv2.circle(pixels, center, 10 + index % 12, (255, 255, 255), 2)
    return pixels


def test_pdq_hash_is_256_bits_and_deterministic() -> None:
    data = image_bytes(textured())
    first = fingerprint(data)
    second = fingerprint(data)
    assert len(first.hex_value) == 64
    assert first == second
    assert hamming(first.hex_value, second.hex_value) == 0


def test_verifier_accepts_recompressed_copy() -> None:
    pixels = textured()
    result = verify(image_bytes(pixels), image_bytes(pixels, quality=80))
    assert result.accepted, result


def test_verifier_compares_at_smaller_image_resolution() -> None:
    original = Image.fromarray(data.astronaut()).resize(  # type: ignore[no-untyped-call]
        (1024, 1024), Image.Resampling.LANCZOS
    )
    copy = original.resize((256, 256), Image.Resampling.LANCZOS)
    result = verify(image_bytes(np.asarray(copy), quality=75), image_bytes(np.asarray(original)))
    assert result.accepted, result


def test_verifier_is_order_independent_for_scaled_copy() -> None:
    original = Image.fromarray(data.coffee()).resize(  # type: ignore[no-untyped-call]
        (1024, 768), Image.Resampling.LANCZOS
    )
    copy = original.resize((512, 384), Image.Resampling.LANCZOS)
    small = image_bytes(np.asarray(copy), quality=75)
    large = image_bytes(np.asarray(original))
    small_first = verify(small, large)
    large_first = verify(large, small)
    assert small_first.accepted == large_first.accepted
    assert abs(small_first.ssim - large_first.ssim) < 0.02


def test_verifier_rejects_localized_content_change() -> None:
    original = Image.fromarray(data.astronaut()).resize(  # type: ignore[no-untyped-call]
        (1024, 1024), Image.Resampling.LANCZOS
    )
    changed = np.asarray(original.resize((256, 256), Image.Resampling.LANCZOS)).copy()
    changed[80:140, 80:140] = 0
    result = verify(image_bytes(changed), image_bytes(np.asarray(original)))
    assert not result.accepted, result
    assert result.reason == "localized_change"


def test_verifier_rejects_unrelated_and_featureless_images() -> None:
    unrelated = verify(image_bytes(textured(1)), image_bytes(textured(2)))
    blank = np.zeros((128, 128, 3), dtype=np.uint8)
    featureless = verify(image_bytes(blank), image_bytes(blank))
    assert not unrelated.accepted
    assert not featureless.accepted


def test_verifier_rejects_weak_spatial_support() -> None:
    pixels = textured()
    policy = NearCopyPolicy(min_coverage=0.99)
    result = verify(image_bytes(pixels), image_bytes(pixels, quality=80), policy)
    assert not result.accepted
    assert result.reason == "poor_spatial_support"
