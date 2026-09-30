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

## Exploratory synthetic evaluation

Run `make evaluate-m4` to reproduce the split manifest and JSON report in
`.artifacts/m4`. Six packaged scikit-image scenes are split by source identity:
three for calibration and three held out. Each scene has four transforms:
JPEG quality 80/40, 70% scaling, and a 5% border crop. This is an engineering
smoke test, **not** a statistically meaningful quality gate.

| Split | Positive queries | Candidate recall | Verified recall | False positives | Median verification |
|---|---:|---:|---:|---:|---:|
| Calibration | 12 | 0.75 | 0.50 | 0 | ~40 ms |
| Held out | 12 | 0.75 | 0.50 | 0 | ~22 ms |

The timings are from one local ARM machine and vary across runs. Precision was
1.0 on these small sets, but zero observed false positives does not establish
a production false-positive rate. The configured thresholds miss half of the
positive transformations. No threshold should be relaxed solely on this set.

## Remaining M4 gate

1. Prepare the official Copydays originals and attacked sets with a
   source-disjoint calibration/held-out manifest. The data is not in this repo.
2. Measure candidate recall@K, verification precision/recall, false positives,
   and latency by transformation on that corpus and realistic distractors.
3. Calibrate a versioned strict policy against a stated false-positive gate,
   then implement auditable duplicate groups and guarded revocation/re-entry.
4. Enable fuzzy suppression only after held-out evidence supports that policy.

The [official Copydays page](https://thoth.inrialpes.fr/~jegou/data.php.html)
describes the originals, crop, scale/JPEG, and strongly attacked sets and their
INRIA copyright. Do not commit those image archives to this repository.
