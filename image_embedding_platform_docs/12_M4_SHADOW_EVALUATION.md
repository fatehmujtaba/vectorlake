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
a 60 MB strong-attack archive, and a 2 GB crop archive. The linked
`pascal.inrialpes.fr` host timed out on 2026-09-30; alternate INRIA hosts
returned 404, so no Copydays measurement is claimed yet. Keep the extracted
images outside Git. With locally available extracted archives, run:

```sh
uv run python -m infrastructure.copydays_manifest \
  --original-dir /path/to/copydays_original \
  --attack-dir /path/to/copydays_jpeg \
  --attack-dir /path/to/copydays_strong \
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
candidates, and verification latency overall and by attack. The report always
sets `suppression_approved=false`; metrics alone do not change the live policy.

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
| Calibration | 12 | 0.75 | 0.50 | 0 | ~40 ms |
| Held out | 12 | 0.75 | 0.50 | 0 | ~22 ms |

The timings are from one local ARM machine and vary across runs. Precision was
1.0 on these small sets, but zero observed false positives does not establish
a production false-positive rate. The configured thresholds miss half of the
positive transformations. No threshold should be relaxed solely on this set.

## Remaining M4 gate

1. Obtain the official Copydays originals and attacked sets, then run the
   source-disjoint manifest and evaluation commands above. The data is not
   in this repo and the official linked download host was unreachable.
2. Measure candidate recall@K, verification precision/recall, false positives,
   and latency by transformation on that corpus and realistic distractors.
3. Calibrate a versioned strict policy against a stated false-positive gate,
   then add guarded activation of derived decisions and duplicate groups.
4. Enable fuzzy suppression only after held-out evidence supports that policy.

The [official Copydays page](https://thoth.inrialpes.fr/~jegou/data.php.html)
describes the originals, crop, scale/JPEG, and strongly attacked sets and their
INRIA copyright. Do not commit those image archives to this repository.
