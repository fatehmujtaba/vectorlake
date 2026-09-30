from __future__ import annotations

from dataclasses import dataclass
from typing import BinaryIO

import cv2
import numpy as np
import pdqhash
from PIL import Image, ImageOps
from skimage.metrics import structural_similarity

MAX_SIDE = 1024
POLICY_VERSION = "near-copy-shadow-v1"


@dataclass(frozen=True)
class NearCopyPolicy:
    min_pdq_quality: int = 40
    max_pdq_distance: int = 48
    min_keypoints: int = 35
    min_inliers: int = 20
    min_inlier_ratio: float = 0.55
    min_coverage: float = 0.18
    max_reprojection_error: float = 3.0
    min_overlap: float = 0.50
    min_ssim: float = 0.90


DEFAULT_POLICY = NearCopyPolicy()


@dataclass(frozen=True)
class Fingerprint:
    hex_value: str
    quality: int


@dataclass(frozen=True)
class Verification:
    accepted: bool
    reason: str
    matches: int = 0
    inliers: int = 0
    inlier_ratio: float = 0.0
    coverage: float = 0.0
    median_reprojection_error: float = 0.0
    overlap: float = 0.0
    ssim: float = 0.0


def _pixels(source: BinaryIO) -> np.ndarray:
    source.seek(0)
    with Image.open(source) as image:
        oriented = ImageOps.exif_transpose(image).convert("RGB")
        oriented.thumbnail((MAX_SIDE, MAX_SIDE), Image.Resampling.LANCZOS)
        return np.asarray(oriented, dtype=np.uint8).copy()


def fingerprint(source: BinaryIO) -> Fingerprint:
    bits, quality = pdqhash.compute(_pixels(source))
    packed = np.packbits(bits.astype(np.uint8))
    return Fingerprint(packed.tobytes().hex(), int(quality))


def hamming(left: str, right: str) -> int:
    if len(left) != 64 or len(right) != 64:
        raise ValueError("PDQ hash must be 256 bits")
    return (int(left, 16) ^ int(right, 16)).bit_count()


def verify(
    query: BinaryIO, candidate: BinaryIO, policy: NearCopyPolicy = DEFAULT_POLICY
) -> Verification:
    query_rgb = _pixels(query)
    candidate_rgb = _pixels(candidate)
    query_gray = cv2.cvtColor(query_rgb, cv2.COLOR_RGB2GRAY)
    candidate_gray = cv2.cvtColor(candidate_rgb, cv2.COLOR_RGB2GRAY)
    orb = cv2.ORB_create(nfeatures=1600)  # type: ignore[attr-defined]
    query_points, query_desc = orb.detectAndCompute(query_gray, None)
    candidate_points, candidate_desc = orb.detectAndCompute(candidate_gray, None)
    if (
        query_desc is None
        or candidate_desc is None
        or len(query_points) < policy.min_keypoints
        or len(candidate_points) < policy.min_keypoints
    ):
        return Verification(False, "insufficient_features")

    matcher = cv2.BFMatcher(cv2.NORM_HAMMING)
    forward = matcher.knnMatch(query_desc, candidate_desc, k=2)
    reverse = matcher.knnMatch(candidate_desc, query_desc, k=2)
    reverse_best = {
        pair[0].queryIdx: pair[0].trainIdx
        for pair in reverse
        if len(pair) == 2 and pair[0].distance < 0.75 * pair[1].distance
    }
    matches = [
        pair[0]
        for pair in forward
        if len(pair) == 2
        and pair[0].distance < 0.75 * pair[1].distance
        and reverse_best.get(pair[0].trainIdx) == pair[0].queryIdx
    ]
    if len(matches) < policy.min_inliers:
        return Verification(False, "insufficient_matches", matches=len(matches))

    src = np.asarray(
        [query_points[match.queryIdx].pt for match in matches], dtype=np.float32
    ).reshape(-1, 1, 2)
    dst = np.asarray(
        [candidate_points[match.trainIdx].pt for match in matches], dtype=np.float32
    ).reshape(-1, 1, 2)
    matrix, mask = cv2.findHomography(src, dst, cv2.RANSAC, 3.0)
    if matrix is None or mask is None or not np.isfinite(matrix).all():
        return Verification(False, "no_geometry", matches=len(matches))
    inlier_mask = mask.ravel().astype(bool)
    inliers = int(inlier_mask.sum())
    ratio = inliers / len(matches)
    if inliers < policy.min_inliers or ratio < policy.min_inlier_ratio:
        return Verification(False, "weak_geometry", len(matches), inliers, ratio)

    inlier_src = src[inlier_mask].reshape(-1, 2)
    inlier_dst = dst[inlier_mask].reshape(-1, 2)
    query_area = float(query_gray.shape[0] * query_gray.shape[1])
    candidate_area = float(candidate_gray.shape[0] * candidate_gray.shape[1])
    coverage = min(
        cv2.contourArea(cv2.convexHull(inlier_src)) / query_area,
        cv2.contourArea(cv2.convexHull(inlier_dst)) / candidate_area,
    )
    projected = cv2.perspectiveTransform(inlier_src.reshape(-1, 1, 2), matrix).reshape(-1, 2)
    reprojection = float(np.median(np.linalg.norm(projected - inlier_dst, axis=1)))
    if coverage < policy.min_coverage or reprojection > policy.max_reprojection_error:
        return Verification(
            False, "poor_spatial_support", len(matches), inliers, ratio, coverage, reprojection
        )

    height, width = candidate_gray.shape
    aligned = cv2.warpPerspective(query_gray, matrix, (width, height))
    valid = cv2.warpPerspective(
        np.ones(query_gray.shape, dtype=np.uint8), matrix, (width, height), flags=cv2.INTER_NEAREST
    ).astype(bool)
    overlap = float(valid.mean())
    if overlap < policy.min_overlap:
        return Verification(
            False,
            "insufficient_overlap",
            len(matches),
            inliers,
            ratio,
            coverage,
            reprojection,
            overlap,
        )
    score_map = structural_similarity(aligned, candidate_gray, data_range=255, full=True)[1]  # type: ignore[no-untyped-call]
    # SSIM windows crossing the warp boundary are not evidence.
    interior = cv2.erode(valid.astype(np.uint8), np.ones((7, 7), np.uint8)).astype(bool)
    score = float(score_map[interior].mean()) if interior.any() else 0.0
    accepted = score >= policy.min_ssim
    return Verification(
        accepted,
        "accepted" if accepted else "low_ssim",
        len(matches),
        inliers,
        ratio,
        coverage,
        reprojection,
        overlap,
        score,
    )
