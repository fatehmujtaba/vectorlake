# Datasets and Evaluation

## 1. Do not use one dataset for every claim

We are evaluating three different things:

1. copy/near-duplicate detection,
2. visual embedding retrieval,
3. systems reliability with corrupt/pathological inputs.

Use a separate dataset strategy for each.

## 2. Dedup benchmark — INRIA Copydays

Official page:

https://thoth.inrialpes.fr/~jegou/data.php.html

Use Copydays because it was designed for image-copy detection and contains original images with transformations such as crops, scaling/JPEG changes, and stronger modifications.

Use it to evaluate:

```text
PDQ candidate recall
ORB verification recall
RANSAC rejection/verification
SSIM behavior
strict policy precision/recall
```

The purpose is not to copy published thresholds. The purpose is to derive our own strict policy.

## 3. Visual retrieval corpus — COCO 2017 validation

Official site:

https://cocodataset.org/

The validation split is a manageable general image corpus for local retrieval experiments. Use image queries and category/object annotations to sanity-check ResNet-18 visual feature retrieval.

Because the initial model is not multimodal, do not present caption-to-image retrieval as a supported feature.

## 4. Synthetic reliability/transform corpus

Create our own controlled variants from a subset of COCO or other legally suitable images.

For each canonical source generate:

```text
byte copy
metadata change
PNG/JPEG re-encode
JPEG quality 95/70/40
resize 90%/50%
crop 5%/20%/50%
rotation
small perspective transform
brightness/contrast change
Gaussian noise
blur
sharpen
border
text overlay
watermark
screenshot-style frame
partial collage
```

Also generate invalid inputs:

```text
zero-byte file
truncated JPEG
truncated PNG
random bytes named .jpg
wrong extension
wrong content type
extreme dimensions
unsupported codec
corrupted metadata
```

## 5. Ground-truth manifest

Every generated artifact must have a manifest row.

Example:

```json
{
  "image_id": "coco_000001_jpeg70",
  "source_id": "coco_000001",
  "expected_relationship": "DERIVED_COPY",
  "transformation": "JPEG_RECOMPRESSION",
  "parameters": {"quality": 70}
}
```

Exact copy:

```json
{
  "image_id": "coco_000001_copy",
  "source_id": "coco_000001",
  "expected_relationship": "EXACT_BYTES",
  "transformation": "BYTE_COPY"
}
```

Corrupt:

```json
{
  "image_id": "bad_00001",
  "expected_validation": "FAILED_PERMANENT",
  "transformation": "TRUNCATED_JPEG"
}
```

## 6. Dedup metrics

Report overall and per-transformation:

```text
precision
recall
F1
false positive count
false negative count
candidate recall@K
average PDQ candidates/image
ORB verification rate
verification latency
```

For our product, **precision is the primary metric** because a false duplicate can suppress a unique scientific image.

A useful report table:

| Transform | Candidate Recall@20 | Final Precision | Final Recall |
|---|---:|---:|---:|
| byte copy | measured | measured | measured |
| metadata-only | measured | measured | measured |
| JPEG 70 | measured | measured | measured |
| resize 50% | measured | measured | measured |
| crop 20% | measured | measured | measured |
| rotation | measured | measured | measured |
| watermark | measured | measured | measured |
| unrelated images | n/a | measured | n/a |

Do not populate numbers before running the tests.

## 7. Strict policy calibration

Threshold process:

1. Split transformed pairs into calibration and validation sets.
2. Measure PDQ distance distributions for positive and negative pairs.
3. Choose a PDQ candidate radius that favors candidate recall; PDQ is only a funnel.
4. Measure ORB/RANSAC/SSIM signals.
5. Define a conservative final policy with very low false-positive rate.
6. Validate on held-out data.
7. Persist policy as `strict-copy-v1`.
8. Never silently change thresholds under the same policy version.

## 8. ResNet retrieval evaluation

Use normalized 512-D vectors and cosine similarity.

Evaluate:

```text
Recall@1
Recall@5
Recall@10
nearest-neighbor category/object agreement
query p50/p95/p99
index size
embedding throughput
```

This is not a claim of perfect scientific semantics. It is a baseline visual embedding system.

## 9. Corruption-validation evaluation

For every invalid class measure:

```text
correct rejection
wrongly accepted
worker crash count
retry classification correctness
```

A malformed image must not kill the worker process.

## 10. System/load metrics

Measure:

```text
images/sec
MB/sec
S3 fetch latency
SHA MB/sec
decode latency
PDQ latency
PDQ search latency
candidate count distribution
ORB/RANSAC latency
SSIM latency
inference images/sec
batch-size distribution
archive write throughput
vector index throughput
end-to-end p50/p95/p99
Kafka consumer lag
oldest unprocessed event age
backlog drain rate
```

## 11. Duplicate compute savings

Track:

```text
inference_avoided_total
inference_required_total
```

If safely deduplicated fraction is `d`:

```text
inference_required ≈ total_images × (1 - d)
```

This lets us turn dedup quality into a direct compute-cost story.

## 12. Optional later benchmark

For larger-scale copy-detection experiments, consider Meta DISC21 after reviewing its current license/terms and resource requirements. It is much larger and should not be a dependency for the laptop-first implementation.
