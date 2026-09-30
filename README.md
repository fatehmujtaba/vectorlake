# VectorLake

VectorLake is a local-first image ingestion, deduplication, embedding, and
visual search platform.

The architecture and implementation plan are in the
[documentation pack](image_embedding_platform_docs/00_START_HERE.md).

Track delivery in the [implementation roadmap](IMPLEMENTATION_ROADMAP.md).

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
