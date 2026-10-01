# M4 near-copy shadow evaluation

## Current state

The M4 path computes a 256-bit PDQ hash, searches a tenant-scoped DynamoDB
candidate table, and verifies up to five candidates with ORB/Hamming mutual
matching, RANSAC geometry, spatial coverage, reprojection error, overlap, and
aligned SSIM. The table is a bounded local adapter, not a production-scale
binary-vector index. Scopes with more than 2,048 indexed entries fail open to
inference. Evidence is recorded in `near_copy_evidence` under the image ID.

Fuzzy suppression is **disabled**. Even an accepted shadow match receives
`image.inference-ready.v1`. Exact and canonical-pixel deduplication retain
their existing behavior.

Accepted shadow evidence also creates a `duplicate_decisions` item with
`status=SHADOW`, a deterministic decision ID, the proposed representative,
the representative's group ID, and the image's own group ID. A replay cannot
overwrite a later decision state. `SHADOW` is an audit proposal only; it is
not an active duplicate relationship.

## Copydays preparation

The official page links a 208 MB original archive, a 77 MB scale/JPEG archive,
a 60 MB strong-attack archive, and a 2 GB crop archive. The live
`pascal.inrialpes.fr` host timed out on 2026-09-30, but the [2016 Wayback
capture](https://web.archive.org/web/20181015092553if_/http://pascal.inrialpes.fr/data/holidays/)
contains the original, JPEG, and strong-attack archives. They were downloaded
and extracted under ignored `.artifacts/m4/copydays`; the crop archive was not
downloaded. Archive SHA-256 values, in original/JPEG/strong order, are:

```text
8b85b7914fd0145bc258bde3440f6a622243ca91ee085fe2025ce7bdeb0bfb8e
7abbf87c97b6d8a8044194ea5c34e98d76c6f37381d69ef9c2fbfa0ffaabc8bd
7852c64aa4a37e8329ce08159fc947c16f09c66737db0a2479a90fc9ae40760c
```

Keep the extracted images outside Git. With locally available extracted
archives, run:

```sh
uv run python -m infrastructure.copydays_manifest \
  --original-dir /path/to/copydays_original \
  --attack-dir /path/to/copydays_jpeg \
  --attack-dir /path/to/copydays_strong \
  --distractor-dir /path/to/val2017 \
  --output .artifacts/m4/copydays_manifest.jsonl
uv run python -m infrastructure.copydays_evaluation \
  --manifest .artifacts/m4/copydays_manifest.jsonl \
  --output .artifacts/m4/copydays_report.json
```

The importer derives a source group from the numeric filename in the same
way as published Copydays evaluation code (`image_number // 100`). It refuses
unmatched attacks and duplicate originals, and splits by source group rather
than by image. Rows include SHA-256 and byte size; the evaluator rejects
changed files, split leakage, and files outside the
product's size/dimension limits. It reports candidate recall@5, precision,
recall, false positives, screened negative pairs, verified negative
candidates, verification failure reasons, and latency overall and by attack.
The report always
sets `suppression_approved=false`; metrics alone do not change the live policy.

## First real-data measurement (2026-09-30)

The manifest contains 157 source images and 1,642 attacked queries. It assigns
110 sources to calibration and 47 to held out. The committed policy is
`near-copy-shadow-v1` (PDQ distance <= 48, aligned SSIM >= 0.90). Full local
results are in ignored `.artifacts/m4/copydays_report.json`.

| Split | Queries | Candidate recall@5 | Verified recall | False positives | Nonmatching candidates verified | Median / p95 verification |
|---|---:|---:|---:|---:|---:|---:|
| Calibration | 1,152 | 80.4% | 2.1% | 0 | 0 | 153 / 199 ms |
| Held out | 490 | 76.3% | 0.6% | 0 | 0 | 159 / 215 ms |

The strong-attack candidate recall is 1.9% on calibration and 1.5% held out;
verified recall is zero on both. JPEG variants generally reach candidate
search but seldom pass verification. In a 58-query calibration diagnostic
sample, most JPEG failures were `low_ssim`, while strong-attack failures were
mostly `insufficient_matches` or `poor_spatial_support`. The JPEG archive
images are often one-quarter the original dimensions; alignment and quality
loss may contribute to the SSIM failures, but this is not yet a proven root
cause. These observations do not justify lowering thresholds.

The zero false-positive count is **not** a precision guarantee: although the
report screened 147,446 nonmatching pairs by PDQ, none entered the bounded
candidate list or geometric verifier. Realistic hard negatives and a larger
candidate pool are needed before setting a false-positive gate. The current
policy fails the held-out recall gate, so fuzzy suppression remains disabled.

## Shadow policy v2 measurement (2026-10-01)

The verifier now estimates geometry in a consistent small-to-large direction,
then compares at the smaller image resolution. A connected low-SSIM region
covering more than 1% of valid pixels rejects an otherwise accepted match.
The PDQ radius and global SSIM threshold are unchanged. This is a new shadow
policy version, not an activation. The [COCO 2017 validation
images](https://cocodataset.org/) add 5,000 independent distractors, split by
image identity. The local COCO ZIP SHA-256 is
`4f7e2ccb2866ec5041993c9cf2a952bbed69647b115d0f74da7ce8f4bef82f05`.
Full local results are in ignored
`.artifacts/m4/copydays_coco_report_shadow_v2_reviewed.json`.

| Split | Sources / distractors | Queries | Candidate recall@5 | Verified recall | JPEG 75 recall | Strong recall | Median / p95 verification |
|---|---:|---:|---:|---:|---:|---:|---:|
| Calibration | 110 / 3,456 | 1,152 | 80.4% | 12.1% | 49.1% | 0% | 121 / 176 ms |
| Held out | 47 / 1,544 | 490 | 76.3% | 10.6% | 36.2% | 0% | 122 / 193 ms |

The positive queries screened 4,873,076 nonmatching pairs and the 5,000 COCO
incoming queries screened another 14,736,598 pairs. Neither path verified a
nonmatching candidate: none fell within the PDQ radius and top-five limit.
Thus `false_positive=0` is not evidence of verifier precision. The new local
change check rejects a large altered region, but an exploratory 512x512 scene
with a changed 20x20 region still passed (PDQ distance 4, SSIM 0.997). For
scientific imagery this may be a distinct observation, requiring labeled
domain-specific hard negatives. Because the held-out aggregate was
inspected between verifier iterations, it now serves as validation evidence,
not a pristine final test. A fresh source-disjoint final set is required.
The current release gate
remains **failed** and fuzzy suppression remains **off**.

## Compensation path

`RevocationService` is an internal-only path for a future `ACTIVE` derived
decision. It checks tenant/scope and decision class, moves `ACTIVE` to
`REVOKING`, publishes a deterministic `image.inference-ready.v1` compensation
event with Kafka acknowledgment, changes the image to its own unique group,
and finally marks the decision `REVOKED`. Replays can resume at `REVOKING` or
finish after the image state changed. No source object is deleted. Run
`make e2e-m4-revoke` for a local seeded-decision drill. The live processor
does **not** create `ACTIVE` derived decisions yet.

## Exploratory synthetic evaluation

Run `make evaluate-m4` to reproduce the split manifest and JSON report in
`.artifacts/m4`. Six packaged scikit-image scenes are split by source identity:
three for calibration and three held out. Each scene has four transforms:
JPEG quality 80/40, 70% scaling, and a 5% border crop. This is an engineering
smoke test, **not** a statistically meaningful quality gate.

| Split | Positive queries | Candidate recall | Verified recall | False positives | Median verification |
|---|---:|---:|---:|---:|---:|
| Calibration | 12 | 0.75 | 0.58 | 0 | ~34 ms |
| Held out | 12 | 0.75 | 0.58 | 0 | ~23 ms |

These v2 timings are from one local ARM machine and vary across runs. Precision
was 1.0 on these small sets, but zero observed false positives does not
establish a production false-positive rate. No threshold should be relaxed
solely on this set.

## Remaining M4 gate

1. Obtain the official crop attacks, then measure their candidate and verified
   recall with the v2 policy.
2. Obtain labeled domain-specific unique-but-similar images; test false
   suppression and calibrate the strict policy on calibration data only.
3. Validate the frozen policy against a stated false-positive gate on a fresh
   source-disjoint final set, then add guarded activation of derived decisions
   and duplicate groups.
4. Enable fuzzy suppression only after held-out evidence supports that policy.

The [official Copydays page](https://thoth.inrialpes.fr/~jegou/data.php.html)
describes the originals, crop, scale/JPEG, and strongly attacked sets and their
INRIA copyright. Do not commit those image archives to this repository.
