# VectorLake remaining delivery plan

This plan continues the [implementation roadmap](IMPLEMENTATION_ROADMAP.md).
It is an incremental delivery plan, not a claim that the full SRS is complete.
Every increment needs unit tests, a local integration drill, and a clear
rollback boundary before its status changes to complete.

| Order | Increment | Exit check | Status |
|---|---|---|---|
| 1 | M5a: semantic vector adapter | Tenant/scope and model-version filters, deterministic upsert, replay and cross-tenant tests on Milvus Lite | Complete |
| 2 | M5b: ResNet-18 inference | Pinned pretrained weights and preprocessing, bounded CPU batches, normalized 512-D vectors, deterministic per-observation IDs, generated event | Pending |
| 3 | M5c: archive and index consumers | Parquet archive independent of Milvus, idempotent indexer, archive-based rebuild drill | Pending |
| 4 | M5d: image search API | Authenticated image-query Top-K, matching model/preprocess contract, tenant-filtered results, retrieval evaluation | Pending |
| 5 | M4 quality gate | Crop positives and representative labeled hard negatives; frozen calibration policy; fresh source-disjoint final set; activate only on passing precision/recall gates | Blocked by data and current false positives |
| 6 | Production identity | Select provider, verify credentials, bind producer to tenant/source registry, enforce stable capture IDs, configure non-local storage | Provider and producer contract undecided |
| 7 | Historical-state repair | Review group/event variants and downstream mappings; approve explicit backfill and rollback; no automatic rewriting | Read-only triage complete |
| 8 | M6 reliability and observability | Independent stage projection, bounded retries/DLQ, safe reconciler, traces, metrics, dashboards, seeded-gap repair | Pending |
| 9 | M7 release gate | Outage/replay/load drills, archive rebuild, mobile-accessible deployment, full SRS acceptance | Pending |

## Sequencing and safety

M5 can progress while M4 remains shadow-only. Exact/pixel dedup retains the
source/capture identity boundary. An indexed vector must not merge distinct
captures just because they have similar pixels; content reuse and observation
identity are separate concerns. No near-copy threshold is relaxed to make a
demo look complete.

M5a establishes the semantic storage contract first, using synthetic vectors
and a local Milvus Lite file. Each image keeps its own vector row, even when
another capture shares its duplicate group. It does not generate real embeddings
or expose search to callers. M5b must pin model weights and preprocessing before M5c can
archive production-compatible vectors. M5d is only complete after real-image
retrieval tests, not merely a successful synthetic-vector query.

Production authentication and historical repair are separate gates. A local
test token is never accepted as a production identity provider. Historical
conflicts are not replayed or rewritten until affected events, state, and
downstream mappings are reviewed and a rollback procedure is tested.

At each increment, report **Done**, **Evidence**, **Open**, and **Next** with a
commit reference. M7, not M5 alone, is the completion gate for the project.
