# Architecture Decision Records (Condensed)

## ADR-001 — Direct object-storage upload

**Decision:** Client uploads directly to S3 using a presigned URL.

**Why:** Avoid turning API servers into multi-terabyte/day byte proxies. Separate control plane from data plane.

**Rejected:** Uploading normal image bytes through FastAPI.

---

## ADR-002 — SQS at the S3 boundary, Kafka for processing

**Decision:** S3 -> SQS -> event bridge -> Kafka.

**Why:** SQS is a natural durable edge buffer for S3 notifications. Kafka provides replay, consumer groups, long backlog, and independent downstream consumers.

**Rejected:** SQS as the entire processing backbone; direct S3 -> custom synchronous worker.

---

## ADR-003 — Floci for local AWS parity

**Decision:** Use Floci for local S3/SQS/DynamoDB/MSK/RDS/Glue-compatible services.

**Why:** Standard AWS SDK/protocol locally. Floci MSK uses a real Redpanda Kafka-compatible broker, exercising real streaming semantics.

**Rejected:** custom in-memory fake stores; local code paths completely different from production.

---

## ADR-004 — DynamoDB for hot image state

**Decision:** Use DynamoDB access patterns for image state, idempotency, exact/pixel hash registries, duplicate groups.

**Why:** hot workload is key-oriented and concurrency-heavy; conditional writes solve canonical ownership cleanly.

**Rejected:** one global PostgreSQL instance for every image-state mutation at eventual billion/day scale.

---

## ADR-005 — PostgreSQL remains control plane

**Decision:** Use PostgreSQL for tenants, policies, users, model definitions, RBAC, administrative configuration.

**Why:** these relationships benefit from relational transactions/constraints and are not the giant image-event hot path.

---

## ADR-006 — Strict dedup before inference

**Decision:** SHA-256 -> canonical pixel hash -> PDQ -> ORB -> RANSAC -> aligned SSIM.

**Why:** cheap deterministic checks first; fuzzy candidate generation before expensive verification; maximize precision before suppressing expensive inference.

**Rejected:** semantic vector cosine threshold as dedup truth.

---

## ADR-007 — ORB over SIFT initially

**Decision:** ORB is the initial local feature verifier.

**Why:** CPU efficiency, binary descriptors, Hamming matching, high-throughput suitability. The interface remains replaceable if benchmark recall is insufficient.

**Rejected:** keeping both ORB and SIFT in the first path without evidence that dual extraction is worth the CPU cost.

---

## ADR-008 — ResNet-18 pretrained CNN embeddings

**Decision:** torchvision pretrained ResNet-18, classifier removed, 512-D normalized features.

**Why:** no transformer requirement, small model, CPU-friendly local execution, mature pretrained implementation, replaceable model adapter.

**Rejected:** transformer model as initial default; training a CNN from scratch.

---

## ADR-009 — Milvus API with Lite locally

**Decision:** dedicated vector-service uses Milvus Lite locally and Milvus Distributed/Standalone in production.

**Why:** laptop-friendly local footprint, same client/API family, supports migration to large scale.

**Rejected:** full Milvus Standalone as mandatory on a 16 GB laptop; vector DB code scattered throughout application services.

---

## ADR-010 — Vector indexes are derived state

**Decision:** archive embeddings to S3/Parquet independently of semantic index.

**Why:** index loss should require rebuilding the index, not recomputing every embedding.

**Rejected:** Milvus as the only surviving copy of generated vectors.

---

## ADR-011 — At-least-once + idempotent effects

**Decision:** Assume S3/SQS/Kafka can redeliver; make side effects idempotent.

**Why:** global exactly-once across S3, Kafka, DDB, PostgreSQL, and Milvus is not a realistic system-wide transaction boundary.

**Rejected:** correctness depending on messages arriving once.

---

## ADR-012 — Duplicate groups are reversible

**Decision:** logical duplicate groups with stable group ID and mutable representative; do not delete source in hot path.

**Why:** false-positive recovery, auditability, representative quality changes, retention/legal separation.

---

## ADR-013 — High-cardinality observability discipline

**Decision:** `image_id`/`event_id` belong in logs and traces, not Prometheus labels.

**Why:** metric-cardinality explosion is an operational failure mode at high throughput.

---

## ADR-014 — Cell-based long-term scale

**Decision:** scale beyond one giant cluster using processing cells partitioned by tenant/region/dataset.

**Why:** capacity isolation and reduced blast radius at 500M–1B/day scale.
