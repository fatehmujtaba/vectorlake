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
