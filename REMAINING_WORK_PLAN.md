# VectorLake remaining delivery plan

This plan continues the [implementation roadmap](IMPLEMENTATION_ROADMAP.md).
It is an incremental delivery plan, not a claim that the full SRS is complete.
Every increment needs unit tests, a local integration drill, and a clear
rollback boundary before its status changes to complete.

| Order | Increment | Exit check | Status |
|---|---|---|---|
| 1 | M5a: semantic vector adapter | Tenant/scope and model-version filters, deterministic upsert, replay and cross-tenant tests on Milvus Lite | Complete |
| 2 | M5b: ResNet-18 inference | Pinned pretrained weights and preprocessing, bounded CPU batches, normalized 512-D vectors, deterministic per-observation IDs, generated event | Complete locally; worker backlog preflight remains |
| 3 | M5c: archive and index consumers | Parquet archive independent of Milvus, idempotent indexer, archive-based rebuild drill | Complete locally; worker backlog preflight remains |
| 4 | M5d: image search API | Authenticated image-query Top-K, matching model/preprocess contract, tenant-filtered results, retrieval evaluation | Complete locally; 132 calibration queries passed |
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
or expose search to callers. M5b uses pretrained ResNet-18 on CPU, verifies the
weight checksum, and publishes `embedding.generated.v1` only after bounded
source validation. The direct local M5 drill verifies Kafka delivery, state,
and replay. The standalone worker is coded but should not be enabled against
the historical local backlog until stale/invalid-event handling and its
retry/DLQ policy are reviewed. M5c writes immutable checksummed Parquet objects
before publishing archive events, indexes independently, and rebuilds a fresh
Milvus Lite collection from the archive without running inference. Physical
Parquet rows may repeat if Kafka batch boundaries change; rebuild deduplicates
by embedding ID and rejects conflicting rows. The standalone M5c workers are
not activated against the historical backlog until retry/DLQ and backlog
preflight are complete. The local rebuild is capped at 10,000 unique rows and
64 MiB per object; a production-scale streaming rebuild remains part of M6/M7.
M5d now accepts bounded image queries and uses the same pinned engine as
ingestion, with tenant/scope and version filters. The dedicated vector service
owns the local Milvus file; both indexer and public search call it over HTTP.
The real HTTP upload-to-search drill passed, and 132 calibration queries from
33 originals across 11 recordings passed the fixed local retrieval gate.
See the [API/demo](image_embedding_platform_docs/17_IMAGE_SEARCH_API.md) and
[retrieval evidence](image_embedding_platform_docs/18_M5_RETRIEVAL_EVALUATION.md).

Production authentication and historical repair are separate gates. A local
test token is never accepted as a production identity provider. Historical
conflicts are not replayed or rewritten until affected events, state, and
downstream mappings are reviewed and a rollback procedure is tested.

At each increment, report **Done**, **Evidence**, **Open**, and **Next** with a
commit reference. M7, not M5 alone, is the completion gate for the project.

## Next executable work and finite exit checks

Next is **M6a: bounded retries/DLQ and historical event preflight**. Its exit
check is a transient-failure replay that succeeds, a poison event that reaches
DLQ after a bounded attempt count, and a read-only backlog report identifying
stale/conflicting events before any autonomous worker is activated.

M6 then has two remaining implementation increments: **M6b**, independent
stage projection plus approved-gap reconciliation, and **M6c**, traces,
metrics and dashboards with a seeded-failure drill. These are the three
defined M6 increments, not an indefinite rollout loop.

Five delivery gates remain: M6, M4 quality, production identity,
historical-state repair, and M7 release acceptance. M6 can proceed while
external data and identity-provider choices remain open. M7 is the final
project exit; no local retrieval result substitutes for those gates.
