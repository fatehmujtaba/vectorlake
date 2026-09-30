# Junior Engineer Implementation Plan

## Rule before starting

Implement in dependency order. Do not start by writing ORB or ResNet code while the event contracts and storage ownership are undefined.

Each phase must have tests and a clear definition of done.

## Phase 0 — Repository and engineering skeleton

Tasks:

- create repository layout from SDS,
- configure Python packaging,
- linting/formatting/type checking,
- central settings module,
- structured logging helper,
- base OpenTelemetry setup,
- Docker base image,
- `.env.example`,
- Makefile/task runner.

Definition of done:

```text
make test
make lint
make build
```

all work on a clean checkout.

## Phase 1 — Floci infrastructure bootstrap

Tasks:

- start `floci/floci:latest` in Docker Compose,
- configure local AWS endpoint/credentials,
- write idempotent bootstrap script,
- create S3 buckets,
- create SQS + DLQ,
- create DynamoDB tables,
- create MSK/Redpanda Kafka cluster/topics,
- create PostgreSQL control-plane database/schema.

Definition of done:

- bootstrap can run twice without breaking,
- S3 upload/download smoke test passes,
- SQS send/receive/delete passes,
- DynamoDB conditional write test passes,
- Kafka produce/consume test passes.

## Phase 2 — Event contracts

Tasks:

- define Protobuf schemas,
- generate Python classes,
- build serializer/deserializer utility,
- add schema-version field,
- write compatibility/contract tests.

Definition of done:

- old test event can still be read after an additive schema change,
- invalid schema payload is rejected cleanly.

## Phase 3 — Ingestion API

Tasks:

- `POST /v1/upload-sessions`,
- request idempotency,
- `image_id`,
- upload intent,
- presigned Floci S3 URL,
- `GET /v1/images/{id}` placeholder state response.

Definition of done:

- repeated same idempotency key returns same image ID,
- conflicting body with same key returns 409,
- client can upload through presigned URL.

## Phase 4 — S3 -> SQS -> Kafka bridge

Tasks:

- configure S3 object-created notification to SQS,
- consume SQS,
- normalize event,
- publish `image.received.v1`,
- delete SQS only after Kafka ACK.

Definition of done:

- upload one image and observe one or more Kafka events safely,
- deliberately redeliver same SQS event and downstream system sees same logical `event_id`/image identity behavior.

## Phase 5 — Safe validation + SHA

Tasks:

- stream object to bounded temp file,
- calculate SHA-256 during stream,
- validate size/magic bytes,
- safe decoder limits,
- error taxonomy,
- exact hash registry conditional ownership.

Definition of done:

- exact copy skips decode-heavy downstream stages when safe,
- two simultaneous exact copies yield one canonical owner,
- malformed input produces permanent failure, not worker crash.

## Phase 6 — Canonical pixel identity

Tasks:

- define canonicalization v1,
- deterministic pixel serializer,
- hash registry,
- tests for metadata-only variants.

Definition of done:

- pixel-equivalent test pair resolves to same canonical pixel hash,
- a meaningful bit-depth/channel difference does not silently collapse because of RGB8 conversion.

## Phase 7 — PDQ and vector-service

Tasks:

- implement `PerceptualHasher` adapter,
- add vector-service using Milvus Lite,
- create 256-bit binary collection,
- PDQ upsert/search with scope filter,
- bounded Top-K.

Definition of done:

- PDQ candidate search returns expected transformed Copydays neighbors,
- unrelated tenant scope cannot see another scope's candidates.

## Phase 8 — ORB + RANSAC + SSIM

Tasks:

- ORB extractor,
- Hamming matcher,
- match filtering,
- RANSAC affine/homography path,
- coverage calculation,
- reprojection error,
- image alignment,
- SSIM on valid overlap,
- decision-evidence object.

Definition of done:

- common derived copies pass on calibration data,
- repetitive-texture false feature matches are rejected geometrically,
- low-coverage patch matches do not become full-image duplicates.

## Phase 9 — Strict dedup policy and groups

Tasks:

- `strict-copy-v1` policy config,
- duplicate-group table,
- representative image logic,
- reversible decision record,
- `image.duplicate.v1`,
- `image.inference-ready.v1`.

Definition of done:

- exact/pixel/derived decisions have explicit reason/evidence,
- ambiguous path becomes unique/inference-ready,
- revoking a duplicate decision republishes work safely.

## Phase 10 — ResNet-18 inference

Tasks:

- model interface,
- pretrained ResNet-18 adapter,
- default weight transforms,
- remove final classifier,
- 512-D output,
- L2 normalization,
- bounded micro-batching,
- deterministic embedding ID,
- `embedding.generated.v1`.

Definition of done:

- same image/version generates same logical embedding ID,
- exact duplicate does not create a second model run,
- vector norms are approximately 1.

## Phase 11 — Semantic/visual index

Tasks:

- create 512-D cosine collection in vector-service,
- semantic-indexer consumer,
- deterministic upsert,
- image-query search endpoint.

Definition of done:

- upload/index corpus,
- query one image,
- receive nearest image/group results,
- replay embedding event without duplicate logical vector.

## Phase 12 — Embedding archive

Tasks:

- archive writer buffer,
- Parquet schema,
- periodic/size flush,
- S3 upload,
- `embedding.archived.v1`.

Definition of done:

- vectors are present in S3 Parquet,
- dropping/rebuilding local semantic collection can use archived data without inference.

## Phase 13 — State projector

Tasks:

- consume lifecycle events,
- update independent stage state,
- generation guards,
- complete status endpoint.

Definition of done:

- stale replay does not move newer state backward,
- status accurately shows duplicate, failed, or complete paths.

## Phase 14 — Reconciler

Tasks:

- stale upload detection,
- stale inference-ready detection,
- embedded-but-unarchived/indexed checks,
- orphan S3 object checks,
- safe republish functions.

Definition of done:

- deliberately create an inconsistency and verify reconciliation repairs it or raises a clear alert.

## Phase 15 — Observability

Tasks:

- OTel trace propagation through Kafka headers,
- processor/inference spans,
- Prometheus metrics,
- Grafana dashboard,
- Jaeger trace query.

Definition of done:

- one `image_id` can be followed end-to-end,
- dashboard displays ingress, duplicate ratio, lag, inference throughput, failures.

## Phase 16 — Load and chaos tests

Tasks:

- configurable upload producer,
- duplicate/corrupt mix,
- event load mode,
- kill/restart processor,
- kill/restart inference,
- stop vector-service,
- restart components and validate catch-up.

Definition of done:

- accepted source data is not lost,
- consumer lag grows during outage and drains afterward,
- duplicate delivery does not duplicate business state.

## Coding expectations

Every service must have:

```text
health endpoint
readiness endpoint
structured logs
OpenTelemetry
unit tests
explicit dependency interfaces
bounded concurrency
error classification
```

## Pull-request checklist

Before requesting review:

- [ ] tests added,
- [ ] retry/idempotency considered,
- [ ] event schema updated safely,
- [ ] no image bytes in Kafka,
- [ ] no unbounded concurrency,
- [ ] no cross-tenant lookup,
- [ ] no new hard-coded dedup threshold without evaluation evidence,
- [ ] logs contain image/event/trace IDs where relevant,
- [ ] no secrets/presigned URLs logged,
- [ ] failure path tested.
