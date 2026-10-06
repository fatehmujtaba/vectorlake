# VectorLake image search API

Status: M5d implemented and verified locally on 2026-10-06. This document is
the client contract for image-to-image search. The local token is a development
identity; production authentication and release acceptance remain open.

## Request

`POST /v1/images/search?top_k=10`

Send the encoded image as the raw HTTP body, with:

```http
Authorization: Bearer vectorlake-local-dev-token
Content-Type: image/jpeg
```

JPEG, PNG, and WebP are supported. This endpoint does not accept multipart
form data, JSON, a URL, or a caller-provided vector. `top_k` defaults to 10 and
must be an integer from 1 through 100. The caller does not supply tenant,
scope, or model versions: the server derives the tenant from authentication,
uses the tenant's first registered dedup scope as ingestion currently does,
and uses the active model contract.

Limits match ingestion: at most 20,000,000 encoded bytes, dimensions at most
8192 on either axis, at most 16,000,000 pixels, and a single image frame.
The server bounds streamed bodies even when Content-Length is absent.
The declared Content-Type must match the image's signature and decoded format.

A search query is temporary. It does not create an upload session, S3 source
object, image state, Kafka event, or stored embedding.

## Response

HTTP 200:

```json
{
  "model_name": "resnet18",
  "model_version": "torchvision-0.23.0-imagenet1k-v1",
  "preprocess_version": "resnet18-imagenet1k-v1-256-224",
  "dedup_scope_id": "local-dev",
  "hits": [
    {
      "embedding_id": "<deterministic embedding ID>",
      "image_id": "<image ID>",
      "duplicate_group_id": "<group ID>",
      "score": 0.97
    }
  ]
}
```

Hits are ordered by cosine similarity, highest first. An empty gallery returns
`"hits": []`. Scores are similarity scores, not probabilities or duplicate
decisions. Separate captures can appear as separate hits even when their
pixels or group are shared. There are no public image download URLs in this
response. `GET /v1/images/{image_id}` remains the image-state endpoint.

The active query engine uses the same pinned weights and preprocessing as
ingestion: normalized 512-dimensional ResNet-18 vectors. Both tenant and scope
filters, plus model and preprocessing version filters, are enforced in Milvus.

## Errors

| HTTP status | Meaning | Client action |
|---|---|---|
| 400 | Invalid or mismatched Content-Length | Correct the request |
| 401 | Missing or invalid authentication | Authenticate before retrying |
| 403 | Authenticated tenant has no registered search scope | Configure the tenant |
| 413 | Query exceeds the encoded byte limit | Reduce image size |
| 415 | Unsupported or missing Content-Type | Send JPEG, PNG, or WebP bytes |
| 422 | Invalid Top-K, corrupt image, format mismatch, or unsupported dimensions/frame count | Correct the image or parameters |
| 429 | Local CPU search slot is busy | Retry after the `Retry-After: 1` delay with backoff |
| 503 | Identity, registry, model, or vector backend unavailable/unconfigured | Retry with bounded backoff; report sustained failures |

Errors use the existing FastAPI `detail` response. Validation errors for query
parameters may have a structured `detail` list; service errors have a string.
Avoid unrestricted retry loops. No production credentials are accepted by
the default local identity provider.

## Run locally

From the repository root, install the optional runtimes and bring up the
existing local infrastructure:

```sh
uv sync --locked --dev --extra inference --extra archive
make infra-up
make bootstrap
```

In separate terminals:

```sh
make vector-service
```

```sh
make run
```

The vector service owns `.artifacts/semantic-index.db` on port 8001. The public
API listens on port 8000; its OpenAPI UI is at `http://127.0.0.1:8000/docs`.
Use one vector-service process and one public API process for this local CPU
configuration. The indexer now calls the vector service over HTTP instead of
opening its Milvus Lite file. Do not open that file from another process.

When another backend needs ports 8000/8001, use alternate ports explicitly:

```sh
make vector-service VECTOR_SERVICE_PORT=8101
```

```sh
VECTORLAKE_LOCAL_VECTOR_SERVICE_URL=http://127.0.0.1:8101 make run API_PORT=8100
```

Use the same vector-service URL for any seed/index commands in that session.
The services are currently stopped following the user's request; these commands
start them only when the user chooses to run a demo again.

With those services running, seed a new capture through the real local
upload, dedup, inference, archive, and indexing components:

```sh
make seed-search
```

The default image is the existing Kaggle calibration animal image. Supply
another image with `make seed-search SEARCH_TEST_IMAGE=/absolute/path/image.jpg`.
Every invocation creates a separate demo capture. This is a controlled local
seed; autonomous workers and historical Kafka replay remain gated on M6.

Query the seeded image:

```sh
curl --fail-with-body \
  -X POST 'http://127.0.0.1:8000/v1/images/search?top_k=5' \
  -H 'Authorization: Bearer vectorlake-local-dev-token' \
  -H 'Content-Type: image/jpeg' \
  --data-binary @.artifacts/m4/kaggle/extracted/dataset/animal/animal_evening_time_frame_7817.jpg
```

The default local token and vector-service token are configurable with
`VECTORLAKE_LOCAL_API_TOKEN` and `VECTORLAKE_LOCAL_VECTOR_SERVICE_TOKEN`.
Service URL and database path use `VECTORLAKE_LOCAL_VECTOR_SERVICE_URL` and
`VECTORLAKE_LOCAL_SEMANTIC_DB`. The internal service token is for backend
processes; mobile clients call only the public API.

For automated verification, `make e2e-m5-search` launches isolated services
on temporary ports, verifies upload-to-search, replay and authorization, then
stops those services. `make evaluate-m5-search` evaluates the calibration
split in an isolated index and stops its services afterwards.

## Delivery boundary

The [retrieval evaluation](18_M5_RETRIEVAL_EVALUATION.md) verifies transformed
image retrieval on a small gallery. Production identity, automatic state
projection, worker rollout, observability and load acceptance remain in the
[remaining plan](../REMAINING_WORK_PLAN.md). Near-copy dedup remains shadow-only.
The [M6a retry and preflight](19_M6A_RETRY_AND_PREFLIGHT.md) implementation is
complete locally; its report blocks activation on historical conflicts.
