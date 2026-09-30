# VectorLake implementation roadmap

This roadmap turns the [implementation phases](image_embedding_platform_docs/08_IMPLEMENTATION_PLAN.md)
into testable delivery milestones. The [SRS](image_embedding_platform_docs/04_SRS.md),
[SDS](image_embedding_platform_docs/05_SDS.md), and
[operations plan](image_embedding_platform_docs/09_OPERATIONS_AND_TESTING.md)
remain the detailed contracts. Work proceeds in milestone order; every change
must preserve tenant isolation, idempotency, bounded resource use, and safe
handling of untrusted images.

## Milestones

| ID | Milestone | Source phases | Status |
|---|---|---|---|
| M0 | Engineering foundation | 0 | Complete |
| M1 | Local infrastructure and event contracts | 1-2 | Next |
| M2 | Upload-to-event vertical slice | 3-4 | Pending |
| M3 | Safe validation and deterministic deduplication | 5-6 | Pending |
| M4 | Evaluated near-copy deduplication | 7-9 | Pending |
| M5 | Embeddings, archive, and image search | 10-12 | Pending |
| M6 | State, repair, and observability | 13-15 | Pending |
| M7 | End-to-end reliability and release gate | 16 | Pending |

### M0: Engineering foundation

- Deliver the SDS repository layout, Python package/tool configuration,
  central settings, structured logging, basic telemetry, a Docker base image,
  `.env.example`, and a task runner.
- Keep configuration local-safe and secrets out of Git. Pin runtime and image
  versions when they are introduced.
- Exit: a clean checkout can run `make test`, `make lint`, and `make build`.
- Next: bring up and verify the local dependencies and event contracts in M1.

### M1: Local infrastructure and event contracts

- Deliver a resource-conscious Docker Compose stack for the M1 dependencies,
  idempotent Floci/PostgreSQL bootstrap, Protobuf event definitions, generated
  Python classes, and contract tests.
- Before relying on them, verify the exact Floci operations and Milvus Lite
  binary-search behavior required by this design. Milvus Lite currently
  documents FLAT indexing only, so local PDQ search cannot stand in for an
  indexed production-scale benchmark. Record any incompatibility and its
  adapter-level resolution.
- Exit: repeat bootstrap succeeds; S3, SQS, DynamoDB conditional writes,
  Kafka, and PostgreSQL smoke tests pass; an old event remains readable after
  an additive schema change and invalid payloads fail cleanly.
- Next: connect a presigned upload to a durable domain event in M2.

### M2: Upload-to-event vertical slice

- Deliver authenticated, tenant-scoped upload sessions with idempotency,
  upload intent, presigned S3 URLs, S3 notifications to SQS, and the bridge to
  `image.received.v1` in Kafka. Add a minimal status response.
- Exit: a client uploads directly to S3 and observes the corresponding event;
  a repeated request returns the same image, a conflicting replay returns
  409, and SQS is deleted only after Kafka acknowledges publication.
- Next: validate incoming objects and claim exact/pixel ownership in M3.

### M3: Safe validation and deterministic deduplication

- Deliver bounded object streaming, SHA-256, signature and decoder checks,
  versioned canonical-pixel identity, conditional ownership claims, and
  permanent versus retryable failure classification.
- Exit: concurrent identical uploads converge on one exact owner; metadata-only
  variants converge on one pixel owner; meaningful bit-depth differences stay
  distinct; malformed images do not crash or endlessly retry a worker.
- Next: build and evaluate the near-copy candidate and verification path in M4.

### M4: Evaluated near-copy deduplication

- Deliver PDQ candidate search, ORB/Hamming matching, RANSAC geometry, aligned
  SSIM, a versioned strict policy, auditable duplicate groups, and a reversible
  decision path. All lookups enforce dedup scope.
- Prepare Copydays and synthetic transformation manifests. Calibrate on one
  split and validate on held-out data; publish candidate recall, precision,
  false-positive count, recall, and verification cost before enabling fuzzy
  suppression. Ambiguous cases continue to inference.
- Exit: held-out evaluation report supports the configured policy; cross-scope
  candidates are excluded; low-coverage/repetitive-texture matches are
  rejected; a revoked duplicate can re-enter processing safely.
- Next: generate, archive, index, and search embeddings in M5.

### M5: Embeddings, archive, and image search

- Deliver a versioned ResNet-18 adapter with bounded inference, deterministic
  embedding IDs, 512-D normalized vectors, an independent S3/Parquet archive
  consumer, a Milvus-backed semantic indexer, and tenant-filtered image search.
- Exit: a unique image is embedded once logically, archived, indexed, and
  retrievable; duplicate event replay creates no second logical vector;
  rebuilding the index from Parquet requires no model inference.
- Next: make state, repair, and system health operationally visible in M6.

### M6: State, repair, and observability

- Deliver independent stage state with stale-event guards, complete image
  status, a reconciler for recoverable gaps, trace propagation, metrics,
  dashboards, and useful logs without secrets or high-cardinality labels.
- Exit: stale replay cannot regress state; a seeded inconsistency is repaired
  or alerted; one image can be followed across services in Jaeger and the
  pipeline's lag, failures, duplicate rate, and inference rate are visible.
- Next: test outages, backlog drain, and release acceptance in M7.

### M7: End-to-end reliability and release gate

- Run real-object and event-only load modes separately, plus corruption,
  replay, concurrent duplicate, service kill/restart, backlog drain, and
  vector-index rebuild drills. Record local throughput and stage costs.
- Exit: the SRS acceptance flow passes; accepted originals are not lost;
  duplicate delivery converges to one business outcome; outages recover;
  archive-based rebuild succeeds; dedup quality and observability gates pass.
- Use measured stage costs and burst assumptions for a production capacity
  model. Local laptop results alone do not establish the 10M/day target.
- Next: prioritize production deployment and scaling work from the measured
  bottlenecks and operational findings.

## Delivery rhythm

For each milestone, implement the smallest reviewable increments and keep
their tests with the change. At completion, report:

1. **Done:** the behavior and files delivered, with commit or PR reference.
2. **Evidence:** tests, demo result, and any measured quality or performance.
3. **Open:** remaining risks or decisions, if any.
4. **Next:** the next concrete implementation step and its exit check.

Do not mark a milestone complete until its exit checks pass. Update this table
when a milestone changes state. The immediate implementation step is M1:
bring up the local dependency stack, verify its required operations, and
define the first versioned event contracts.
