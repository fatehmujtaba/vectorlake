# VectorLake

VectorLake is a local-first image ingestion, deduplication, embedding, and
visual search platform.

The architecture and implementation plan are in the
[documentation pack](image_embedding_platform_docs/00_START_HERE.md).

Track delivery in the [implementation roadmap](IMPLEMENTATION_ROADMAP.md).
The [remaining delivery plan](REMAINING_WORK_PLAN.md) lists the open increments
and their exit checks.

M5a adds a 512-D Milvus semantic-index adapter. Its local synthetic-vector
test is `uv run --locked pytest tests/test_semantic_index.py`; it requires a
loopback port for Milvus Lite. This is an internal storage contract, not yet
image inference or a public search endpoint.

## Local development

Use Python 3.12 through [uv](https://docs.astral.sh/uv/) and a running Docker
engine (Docker Desktop or Colima). See the
[local environment inventory](DEVELOPMENT_ENVIRONMENT.md) for the checked
toolchain and service installation schedule.

```sh
uv python install 3.12
make sync
make test
make lint
make build
```

Copy `.env.example` to `.env` for local settings. `make run` starts the API
on `127.0.0.1:8000`; `/healthz` and `/readyz` are the initial endpoints.

## Local infrastructure

The M1 stack uses Floci for S3, SQS, DynamoDB, and an RDS-backed PostgreSQL
instance. A single Redpanda broker runs directly in Compose for Kafka. On
Colima, Floci's real MSK operation created a broker but advertised a
Docker-only address with no host-published Kafka port, so host-side clients
could not connect. The Compose broker is the local adapter for that limitation.

```sh
make infra-up
make bootstrap
make bootstrap
make smoke
make probe-milvus
make infra-down
```

`make bootstrap` is idempotent. `make smoke` verifies S3 put/get, SQS
send/receive/delete, DynamoDB conditional write rejection, PostgreSQL query,
and Kafka produce/consume. The broker is at `127.0.0.1:19092` and the
Floci API is at `127.0.0.1:4566`. Local credentials in `.env.example` are
development-only. Do not use this Compose stack for production.

The Milvus Lite probe currently reports `unsupported`: version 3.2.1 rejects
`BinaryVector` at collection creation. For M4, use a dedup-scope-filtered,
bounded local Hamming candidate scan behind the PDQ index adapter, and send
over-capacity or uncertain cases to inference instead of suppressing them.
Production Milvus `BIN_FLAT`/`HAMMING` behavior needs its own integration test
against Standalone or Distributed Milvus before rollout. Lite is also limited
to FLAT indexes, so local measurements cannot establish production-scale
candidate latency or recall.

## Upload-to-event path

M2 adds `POST /v1/upload-sessions` and `GET /v1/images/{image_id}`. The local
auth stub accepts `Authorization: Bearer vectorlake-local-dev-token` and binds
the caller to the seeded `local-dev` tenant and scope. Set
`VECTORLAKE_LOCAL_API_TOKEN` to override that development-only token. In
non-local environments, uploads fail closed until a real identity provider is
configured. Captured uploads must use the source bound to that token
(`VECTORLAKE_LOCAL_API_SOURCE_ID`, default `camera-1`); another source returns
403. Send an `Idempotency-Key` header with the session request:

```sh
curl -sS -X POST http://127.0.0.1:8000/v1/upload-sessions \
  -H 'Authorization: Bearer vectorlake-local-dev-token' \
  -H 'Idempotency-Key: example-1' \
  -H 'Content-Type: application/json' \
  -d '{"content_type":"image/png","size_bytes":1234}'
```

Use the returned `upload_url` and *both* `upload_headers` with a direct HTTP
PUT. The URL expires after 15 minutes; replaying the same idempotency key
returns the same image and a fresh URL. A conflicting replay returns 409.
The body may include both `source_id` and `capture_id`, for example
`"source_id":"camera-1","capture_id":"frame-2"`. They are client-asserted,
returned in image status, and included in idempotency. Exact/pixel suppression
applies only within the same source and capture. Different captures or sources
remain separate; uploads without either ID remain separate observations even
when content matches. Derived copies may refer to the same source/capture.
`If-None-Match: *` prevents normal overwrites, and the bucket is versioned.
The pinned Floci image accepts the signed local PUT but its optional signature
validator rejected even a minimal boto3 SigV4 URL in testing. Local expiration
and signature-tamper enforcement are therefore unverified; validate both
against the deployment S3 service before release.
Run `make audit-dedup` against the local emulator to inventory completed
records by deterministic policy version. The command is read-only, paginated,
and reports whether its scan covered the whole table. It does not migrate
records or verify published events. See the
[producer authentication contract](image_embedding_platform_docs/15_PRODUCER_AUTH_CONTRACT.md)
before configuring production uploads.
Run `make reconcile-outcomes` to compare completed local image state with
retained Kafka outcome events. It is read-only and flags conflicting event
variants without changing historical state. Add `--image-id IMAGE_ID` to the
underlying `uv run --locked python -m infrastructure.reconcile_outcomes`
command for one-image triage.
The API does not receive image bytes. Run `make bridge` in another terminal
to forward S3-created notifications from SQS to Kafka; the bridge deletes an
SQS message only after the Kafka delivery callback succeeds and state is
updated. `make e2e-m2` verifies the full flow, including replay identity and
overwrite rejection.

## Validation and deterministic deduplication

M3 consumes `image.received.v1` and streams each object in 64 KiB chunks to a
temporary file, with a 20 MB compressed-size cap. It checks JPEG, PNG, and
WebP signatures and content type before decoding with Pillow. Single-frame
decodes are limited to 8192 pixels per side and 16 million pixels total;
truncated, unsupported, or malformed images emit a permanent
`processing.failed.v1` event. Decoder warnings are treated as errors.

Valid images receive an exact-byte SHA-256 and a versioned canonical-pixel
SHA-256 that includes dimensions and sample mode. The latter preserves
8-bit/16-bit and channel distinctions while ignoring metadata-only changes.
The [pixels-v1 contract](image_embedding_platform_docs/11_CANONICAL_PIXEL_V1.md)
defines the byte layout and decoder policy.
Scope-keyed DynamoDB conditional claims select one exact and pixel owner.
An exact copy of a validated owner skips decoding. Replays retain the same
logical outcome event ID. M4 evaluates near-copy candidates in shadow mode;
unmatched images still emit `image.inference-ready.v1`, including accepted
shadow matches. Fuzzy suppression is not enabled.

```sh
make e2e-m3
make processor-up
make verify-worker-m3
make processor-down
```

M4's scoped PDQ, geometric verifier, evaluation harness, and compensation
drill are described in the [shadow evaluation note](image_embedding_platform_docs/12_M4_SHADOW_EVALUATION.md).
The owner-supplied Kaggle image corpus, controlled augmentations, and
calibration-only results are in the [Kaggle evaluation note](image_embedding_platform_docs/13_M4_KAGGLE_DATA_EVALUATION.md).
The owner-approved distinct-timestamp rule exposes would-be false matches in
the animal recording, so near-copy suppression remains off.
The Copydays release gate remains open. Archived originals, JPEG, and strong
attacks and 5,000 COCO distractors are now available locally. Shadow policy
v2 improves held-out verified recall to 10.6% overall, but no nonmatching
candidate reached verification and distinct images with local changes can
still look like copies. Fuzzy suppression remains disabled; see the
evaluation note for the measured gate.

```sh
make evaluate-m4
make e2e-m4
make e2e-m4-revoke
```

`processor-up` builds a non-root, read-only, memory-capped worker container
with a private temporary filesystem. It commits the Kafka offset only after
the outcome event is acknowledged and image state is updated. The local
decoder is bounded, but production deployment still needs a dedicated image
processing threat review and stronger process/network isolation.
