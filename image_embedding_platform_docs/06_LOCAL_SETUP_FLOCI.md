# Local Setup with Floci

## 1. Hardware target

Reference machine:

```text
MacBook Pro 14-inch (2021)
Apple M1 Pro
16 GB unified memory
macOS
```

The local stack is deliberately resource-conscious. We use:

- Floci for AWS simulation,
- Redpanda created by Floci MSK for Kafka,
- Milvus Lite instead of full Milvus Standalone,
- ResNet-18 on CPU inside Docker,
- small worker concurrency,
- small local datasets for interactive development.

## 2. Docker Desktop guidance

Start conservatively. On a 16 GB machine, do not allocate all memory to Docker.

A practical initial Docker Desktop resource target is approximately:

```text
memory: ~8 GB
CPU: enough cores for Docker without starving macOS
swap: small enabled swap if needed
```

Exact CPU allocation depends on the M1 Pro configuration and what else is running. Watch Activity Monitor/Docker statistics rather than assuming a fixed number.

## 3. Floci container

Floci's official quick start supports:

```yaml
services:
  floci:
    image: floci/floci:latest
    ports:
      - "4566:4566"
    volumes:
      - ./volumes/floci:/app/data
```

Floci accepts dummy local credentials.

Application environment:

```env
AWS_ENDPOINT_URL=http://floci:4566
AWS_DEFAULT_REGION=us-east-1
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
```

When using AWS CLI from the host:

```env
AWS_ENDPOINT_URL=http://localhost:4566
```

## 4. Required Floci services

Use Floci for:

```text
S3
SQS
DynamoDB
MSK (real Kafka protocol backed by Redpanda)
RDS/PostgreSQL if desired
Glue Schema Registry when event schemas are wired
```

Floci's service matrix and environment docs should be treated as the authoritative reference for exact supported operations.

## 5. Local vector database

Do not run full Milvus Standalone by default on this 16 GB machine. Run Milvus Lite inside `vector-service`.

Install dependency in that service image:

```text
pymilvus[milvus-lite]
```

Local URI:

```text
/data/milvus.db
```

Mount `/data` to a named Docker volume so the vector index survives container restart.

The same Python Milvus client code can later use a remote endpoint.

## 6. Suggested Docker services

```yaml
services:
  floci:
    image: floci/floci:latest
    ports:
      - "4566:4566"
    volumes:
      - floci-data:/app/data

  ingestion-api:
    build: .
    command: python -m services.api.main
    depends_on:
      - floci

  event-bridge:
    build: .
    command: python -m services.event_bridge.main
    depends_on:
      - floci

  processor:
    build: .
    command: python -m services.processor.main
    depends_on:
      - floci
      - vector-service

  inference-worker:
    build: .
    command: python -m services.inference_worker.main
    depends_on:
      - floci

  vector-service:
    build: .
    command: python -m services.vector_service.main
    volumes:
      - vector-data:/data

  archive-writer:
    build: .
    command: python -m services.archive_writer.main

  semantic-indexer:
    build: .
    command: python -m services.semantic_indexer.main
    depends_on:
      - vector-service

  state-projector:
    build: .
    command: python -m services.state_projector.main

  reconciler:
    build: .
    command: python -m services.reconciler.main

  otel-collector:
    image: otel/opentelemetry-collector-contrib:latest

  prometheus:
    image: prom/prometheus:latest

  grafana:
    image: grafana/grafana:latest

  jaeger:
    image: jaegertracing/all-in-one:latest

volumes:
  floci-data:
  vector-data:
```

This is a structural template, not a copy-paste final compose file. Pin image versions before committing a real project.

## 7. Provision local AWS resources

Create at least:

```text
S3 bucket: raw-images
S3 bucket: embedding-archive
SQS queue: image-object-created
SQS DLQ: image-object-created-dlq
DynamoDB table: image_state
DynamoDB table: exact_hash_registry
DynamoDB table: pixel_hash_registry
DynamoDB table: duplicate_groups
DynamoDB table: request_idempotency
MSK cluster / Kafka topics
RDS PostgreSQL database (control plane)
```

Prefer an `infra/bootstrap.py`, Terraform, or Makefile target that creates these resources idempotently. Do not make the junior developer click around manually.

## 8. Example boto3 configuration

```python
import boto3

common = dict(
    endpoint_url="http://floci:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)

s3 = boto3.client("s3", **common)
sqs = boto3.client("sqs", **common)
ddb = boto3.client("dynamodb", **common)
```

In production the adapter should obtain normal AWS credentials/region and not require a custom local endpoint.

## 9. MSK / Kafka locally

Floci's MSK service uses a real Redpanda broker by default unless configured in mock mode. We want the real broker mode because the goal is to exercise Kafka semantics: topics, partitions, consumer groups, offsets, backlog, and replay.

Do not enable MSK mock mode for end-to-end processing tests.

Bootstrap code should:

1. create the local MSK cluster,
2. discover its bootstrap address,
3. create required Kafka topics if missing,
4. validate producer/consumer connectivity.

## 10. Local concurrency defaults

Start with:

```env
PROCESSOR_MAX_INFLIGHT_IMAGES=4
S3_FETCH_CONCURRENCY=8
PDQ_SEARCH_CONCURRENCY=8
ORB_VERIFY_CONCURRENCY=2
PDQ_TOP_K=20
MAX_ORB_CANDIDATES=10

INFERENCE_WORKERS=1
INFERENCE_MAX_BATCH=8
INFERENCE_MAX_WAIT_MS=25

ARCHIVE_BATCH_SIZE=256
ARCHIVE_FLUSH_SECONDS=5
INDEX_BATCH_SIZE=128
```

If the laptop swaps heavily, reduce concurrency before changing architecture.

## 11. Local image model

The reference Docker inference service is CPU-only:

```env
EMBEDDING_MODEL=resnet18
EMBEDDING_DEVICE=cpu
```

This is intentional. The model is small enough for functional and modest performance tests without external GPU services.

Optional host-native MPS experimentation is permitted for developer profiling, but the reference end-to-end deployment should remain Docker reproducible.

## 12. Start-up order

Automate this, but conceptually:

1. Start Floci.
2. Wait for Floci health.
3. Provision S3/SQS/DynamoDB/MSK/RDS resources.
4. Start vector-service and initialize collections.
5. Start OpenTelemetry/Prometheus/Grafana/Jaeger.
6. Start API, bridge, processor, inference, indexer, archive writer, state projector, reconciler.
7. Run smoke test.

## 13. Smoke test

A smoke test should:

1. call `POST /v1/upload-sessions`,
2. upload one valid image to returned URL,
3. wait until status becomes `COMPLETE`,
4. confirm one S3 source object,
5. confirm one image-state item,
6. confirm one ResNet embedding in vector service,
7. confirm embedding archive receives data after flush,
8. query same image and confirm itself/near images appear,
9. upload exact copy,
10. confirm it becomes duplicate and no second embedding is generated.

## 14. Local load tests

Do not attempt to store 10 million real 2 MB images locally.

Use two modes:

### Functional object load

Upload hundreds/thousands of real images through the full S3 path.

### Event/control-plane load

Use a controlled object pool and generate higher-rate domain events to stress Kafka/worker scheduling without filling disk.

Always label simulated/reused-object load separately from genuine object-throughput benchmarks.

## 15. Resource troubleshooting

If memory is tight:

1. reduce inference batch size,
2. reduce processor inflight count,
3. reduce Prometheus retention,
4. stop Grafana/Jaeger when not needed,
5. use smaller dataset slices,
6. keep Milvus Lite rather than Standalone,
7. avoid running multiple inference replicas locally.

Do not solve local RAM pressure by changing correctness semantics.

## References

- Floci Quick Start: https://floci.io/floci/getting-started/quick-start/
- Floci Services: https://floci.io/floci/services/
- Floci MSK/Redpanda config: https://floci.io/floci/configuration/environment-variables/
- Milvus Lite: https://milvus.io/docs/milvus_lite.md
