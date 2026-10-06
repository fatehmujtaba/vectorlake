# M5 local retrieval evaluation

Evaluated on 2026-10-06 using the owner's existing Kaggle dataset and pinned
ResNet-18 CPU engine. This is a local transformed-image retrieval engineering
evaluation. It does not establish general semantic relevance, production
capacity, or duplicate-classification accuracy.

## Corpus and procedure

- Manifest: `.artifacts/m4/kaggle/manifest.jsonl`.
- Manifest SHA-256:
  `ee5b83c6567a52408e604ba8b81e461351f578d16f7ce4797bacc70fd880ae91`.
- Calibration gallery: 33 original images from 11 recordings.
- Queries: 132; each gallery original plus its JPEG quality-75, 70% resize,
  and 10% border crop variants.
- Only the calibration split was opened and scored. The held-out images were
  not scored or used to modify dedup policy.
- Gallery records were written through the authenticated vector HTTP service
  into a fresh Milvus Lite FLAT/COSINE collection.
- Queries went through the public HTTP API, its image validation, tenant/scope
  registry, CPU embedding engine, and vector HTTP service.
- Engine and preprocessing versions match ingestion. Weight SHA-256:
  `f37072fd47e89c5e827621c5baffa7500819f7896bbacec160b1a16c560e07ec`.

Reproduce with `make evaluate-m5-search`. The detailed generated report is
`.artifacts/m5/search_calibration_report.json`; it is ignored local output.
The evaluator checks input file checksums and refuses incomplete or duplicate
transformation rows. It closes its temporary HTTP services and index afterwards.

## Results

| Query type | Queries | Source image Recall@1 | Source image Recall@5 |
|---|---:|---:|---:|
| Original | 33 | 100% | 100% |
| JPEG quality 75 | 33 | 100% | 100% |
| Resize to 70% | 33 | 100% | 100% |
| Crop 10% from each border | 33 | 100% | 100% |

The local gate was defined in the evaluator before scoring: original-image
Recall@1 must be 100%, and every variant family must have Recall@5 of at least
90%. The gate passed without threshold changes.

Measured sequential HTTP query latency on this laptop:

| Measurement | Result |
|---|---:|
| First query, including lazy model loading | 2099 ms |
| Warm median | 70 ms |
| Warm p95 | 79 ms |

Warm measurements include upload of query bytes over loopback, validation,
registry lookup, embedding, vector HTTP search, and response. They exclude
client-side disk reads and service startup. The gallery contained only 33
vectors and queries were sequential. These measurements do not establish
concurrency limits, large-gallery latency, or the production ingestion target.

## End-to-end and isolation evidence

`make e2e-m5-search` separately verified a real captured image through source
upload, processor, pinned inference, independent Parquet archive, the indexer
using vector-service, and public image search. The source image scored 1.0.
Index replay returned the same event. Identical sentinel vectors under another
tenant or scope were excluded. Unauthenticated and corrupt queries were
rejected. The archive rebuild drill from M5c remains available separately.

The full model-enabled regression suite passed 125 tests. `make lint` passed
Ruff validation/formatting and strict mypy checks. The persistent local demo
was also seeded with a real image and queried through port 8000 successfully.

## Limits and next gate

The expected relevant image here is the original source of a known
augmentation. The gallery intentionally contains that source. No claim is made
about class-level relevance, unknown-image rejection, general scene matching,
or fresh final-domain quality. Cosine search has no duplicate-acceptance
threshold; high similarity must not activate near-copy dedup.

M5 is complete locally with its worker rollout boundary intact. M6a's bounded
retries/DLQ and historical preflight are now implemented; activation remains
blocked by historical conflicts. Next is M6b stage projection and safe gap
recovery. M4's fresh source-disjoint quality gate, production identity,
historical repairs, and M7 release acceptance remain open.
