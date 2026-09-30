# Image Embedding Platform — Start Here

## Audience

This document set is written for the engineer implementing the platform locally first, while preserving production-grade contracts, failure semantics, and service boundaries. The intended developer should be able to implement the system without having to infer architecture decisions from scattered notes.

## Local machine target

The initial development target is a 14-inch 2021 MacBook Pro with Apple M1 Pro and 16 GB unified memory. The canonical local deployment is Docker-based and CPU-first. We intentionally choose a lightweight CNN embedding model so the complete pipeline can be exercised locally without any cloud GPU.

The local environment is **not a separate toy architecture**. It is one small deployment cell of the production architecture. We reduce node count, traffic, dataset size, and compute capacity locally, but we keep the same event flow, source-of-truth rules, idempotency model, deduplication policy, schemas, and recovery semantics.

## Frozen architecture decisions

| Concern | Decision |
|---|---|
| API | FastAPI |
| Local AWS simulation | Floci |
| Object storage | Floci S3 locally, Amazon S3 in production |
| S3 notification buffer | Floci SQS locally, Amazon SQS in production |
| Streaming backbone | Floci MSK using a real Redpanda Kafka-compatible broker locally, Amazon MSK in production |
| Hot image-processing state | Floci DynamoDB locally, Amazon DynamoDB in production |
| Relational control-plane state | Floci RDS PostgreSQL locally or a local PostgreSQL instance; RDS/Aurora PostgreSQL in production |
| Event schemas | Protobuf with schema compatibility checks |
| Exact dedup | SHA-256 |
| Decoded-pixel identity | Versioned canonical-pixel SHA-256 |
| Perceptual candidate generation | PDQ, 256-bit perceptual fingerprint |
| Local-feature verification | ORB descriptors + Hamming matching |
| Geometric verification | RANSAC |
| Structural verification | SSIM after alignment |
| Semantic/visual embedding model | Pretrained ResNet-18, classifier removed, 512-D normalized feature vector |
| Transformer embeddings | Not used in the initial system |
| Vector API | Milvus client API |
| Local vector engine | Milvus Lite behind a dedicated vector-service process |
| Production vector engine | Milvus Standalone/Distributed according to scale |
| Durable embedding archive | S3 + Parquet |
| Observability | OpenTelemetry |
| Metrics | Prometheus |
| Dashboards | Grafana |
| Tracing | Jaeger locally; replaceable production backend |
| Packaging | Docker |
| Local orchestration | Docker Compose |
| Production orchestration | Kubernetes/ECS-class orchestrator |

## What Floci is doing for us

Floci is our local AWS emulator. Our application code uses standard AWS clients and AWS-shaped APIs, but the endpoint is local. Floci currently exposes AWS-compatible S3, SQS, DynamoDB, RDS, Glue, and MSK APIs on a local endpoint. For MSK, Floci launches a real Redpanda broker, so the application is actually using the Kafka protocol rather than a fake in-memory queue.

Local application configuration therefore looks conceptually like:

```env
AWS_ENDPOINT_URL=http://floci:4566
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
```

Production removes the local endpoint override and uses actual AWS service endpoints and IAM credentials.

## Important model decision

We do **not** use CLIP, ViT, DINOv2, SigLIP, or another transformer-based encoder for the first implementation. The initial embedding model is pretrained ResNet-18 from torchvision.

We remove the final classification layer and use the 512-dimensional penultimate representation as the image embedding:

```text
image
  ↓
ResNet-18 convolutional backbone
  ↓
global average pooling
  ↓
512-D feature
  ↓
L2 normalization
  ↓
Milvus cosine search
```

Why ResNet-18:

- It is small enough for this laptop and CPU-based Docker execution.
- torchvision's pretrained ImageNet weights are stable and easy for a junior engineer to use correctly.
- The model is roughly 11.7M parameters and around 44.7 MB in torchvision's current pretrained configuration.
- Its final convolutional representation naturally gives a 512-dimensional feature vector.
- The embedding engine is behind an interface, so the model can later become EfficientNet, ConvNeXt, a domain-specific CNN, or a transformer without rewriting ingestion or storage.

This initial model supports **image-to-image visual retrieval**. It does not provide text-to-image search. If text search becomes a business requirement later, that is a model-contract change rather than an ingestion-architecture rewrite.

## Deduplication policy in one sentence

**SHA-256 handles exact bytes; canonical-pixel hashing handles exact decoded content; PDQ cheaply finds perceptual candidates; ORB establishes local correspondence; RANSAC verifies coherent geometry; aligned SSIM verifies structural similarity; only a strict, benchmarked combination can suppress embedding generation.**

If evidence is ambiguous, the image is treated as unique and processed normally. We prefer wasting one extra embedding operation over incorrectly suppressing a unique scientific image.

## Complete flow in 20 steps

1. Client requests an upload session from FastAPI.
2. API authenticates, authorizes, resolves tenant/dedup scope, creates `image_id`, and applies request idempotency.
3. API creates an upload-intent state record.
4. API returns a presigned Floci S3 upload URL.
5. Client uploads image bytes directly to Floci S3.
6. S3 sends an object-created notification to Floci SQS.
7. Event-bridge consumes the SQS message and publishes `image.received.v1` to Floci MSK/Kafka.
8. Processing worker consumes the event and streams the object from S3 while calculating SHA-256.
9. Worker atomically claims or looks up the exact-byte hash in DynamoDB.
10. If not exact duplicate, worker safely decodes the image and calculates canonical-pixel SHA-256.
11. If still unique, worker calculates PDQ.
12. Worker queries vector-service for the nearest PDQ fingerprints by Hamming distance.
13. Only returned candidates are checked with ORB + Hamming descriptor matching.
14. RANSAC rejects feature matches that do not share coherent geometry.
15. Aligned SSIM provides a final structural signal when the geometry makes SSIM meaningful.
16. A strict versioned policy returns `DUPLICATE`, `UNIQUE`, or `AMBIGUOUS`; ambiguous is processed as unique.
17. Unique images publish `image.inference-ready.v1`.
18. Inference worker runs pretrained ResNet-18 and emits a normalized 512-D embedding.
19. One consumer archives embeddings in batched Parquet files; another indexes them through vector-service.
20. State projector updates the queryable current status and OpenTelemetry records metrics/traces/logs throughout.

## Source-of-truth rules

| Data | Source of truth |
|---|---|
| Original image bytes | S3 |
| Exact-hash ownership | DynamoDB |
| Canonical-pixel ownership | DynamoDB |
| Current image-processing state | DynamoDB projection |
| Tenant/model/policy configuration | PostgreSQL |
| Event history during operational retention | Kafka |
| Long-term embedding artifacts | S3/Parquet |
| PDQ candidate index | Milvus-compatible vector store, rebuildable |
| CNN semantic/visual index | Milvus-compatible vector store, rebuildable |

## Read these files in order

1. `01_ARCHITECTURE.md` — problem, system topology, concurrency, scaling, failure model.
2. `02_DEDUPLICATION.md` — exact dedup logic and verification policy.
3. `03_EMBEDDINGS_AND_VECTOR_SEARCH.md` — ResNet-18 embedding design and vector serving.
4. `04_SRS.md` — formal requirements.
5. `05_SDS.md` — implementation-level software design.
6. `06_LOCAL_SETUP_FLOCI.md` — how the local stack is expected to run on this Mac.
7. `07_DATASETS_AND_EVALUATION.md` — Copydays, COCO, synthetic reliability corpus, metrics.
8. `08_IMPLEMENTATION_PLAN.md` — junior-engineer implementation sequence and definition of done.
9. `09_OPERATIONS_AND_TESTING.md` — observability, chaos, load, recovery, and acceptance tests.
10. `10_ADRS.md` — concise rationale for the major architectural decisions.

## Non-negotiable engineering rules

- Never put raw image bytes into Kafka.
- Never trust filename extension or client MIME type.
- Never use semantic embedding similarity as a duplicate verdict.
- Never physically delete a source image as part of the synchronous dedup decision.
- Never assume S3/SQS/Kafka delivers exactly once.
- Never make correctness depend on an in-memory cache.
- Never let Milvus be the only durable copy of an expensive generated embedding.
- Never silently mix embeddings from different model/preprocessing versions.
- Never use one unbounded `asyncio.gather()` for CPU-heavy OpenCV work.
- Never hard-code similarity thresholds from internet examples; calibrate them from the benchmark corpus.
- Never claim global distributed atomicity across S3, Kafka, DynamoDB, PostgreSQL, and Milvus. Use local atomicity + idempotency + compensation + reconciliation.

## References

- Floci service/API overview: https://floci.io/floci/services/
- Floci quick start: https://floci.io/floci/getting-started/quick-start/
- Floci MSK/Redpanda configuration: https://floci.io/floci/configuration/environment-variables/
- torchvision ResNet-18: https://docs.pytorch.org/vision/stable/models/generated/torchvision.models.resnet18.html
- PyTorch MPS backend: https://docs.pytorch.org/docs/stable/notes/mps.html
- Milvus Lite: https://milvus.io/docs/milvus_lite.md
