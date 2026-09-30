# System Requirements Specification (SRS)

## 1. Purpose

Define the functional and non-functional requirements for a continuously ingesting, duplicate-aware image embedding and image-to-image search platform.

## 2. Scope

The system accepts image uploads, stores source bytes, validates inputs, deduplicates exact and high-confidence derived copies, generates visual CNN embeddings for unique images, archives those embeddings, indexes them for retrieval, exposes processing state, and remains recoverable after component outages.

## 3. Functional requirements

### Ingestion

- **FR-001** The system shall expose an authenticated endpoint to create an upload session.
- **FR-002** Upload-session creation shall accept an idempotency key.
- **FR-003** Repeating the same idempotent request shall return the same logical image/upload result.
- **FR-004** Reusing an idempotency key with a conflicting request body shall return a conflict.
- **FR-005** The system shall generate a unique `image_id`.
- **FR-006** Every image shall be associated with a `tenant_id`.
- **FR-007** Every image shall be associated with a `dedup_scope_id`.
- **FR-008** The API shall create an upload-intent record before returning an upload URL.
- **FR-009** The API shall return a time-limited presigned object-storage URL.
- **FR-010** Normal image bytes shall upload directly to S3-compatible storage rather than through the API application process.
- **FR-011** Stored source images shall be treated as immutable in the normal processing path.

### Object event handoff

- **FR-012** Successful object creation shall create a durable notification.
- **FR-013** The notification path shall tolerate repeated object-created notifications.
- **FR-014** The notification path shall tolerate out-of-order infrastructure notifications.
- **FR-015** S3 notifications shall be buffered in SQS before Kafka publication.
- **FR-016** The event bridge shall acknowledge/delete SQS only after successful Kafka publication.
- **FR-017** The bridge shall publish a versioned `image.received.v1` domain event.
- **FR-018** Raw image payloads shall not be embedded in Kafka records.

### Event schemas

- **FR-019** Domain events shall contain a globally unique `event_id`.
- **FR-020** Domain events shall contain `image_id`, tenant/scope information, timestamp, schema version, and trace context.
- **FR-021** Event schemas shall be version controlled.
- **FR-022** Breaking event-schema changes shall require a new version or compatible migration strategy.

### Validation

- **FR-023** The processor shall validate the object exists and is within configured size limits.
- **FR-024** The processor shall inspect magic bytes/signature rather than trusting extension alone.
- **FR-025** The processor shall enforce a supported-format allowlist.
- **FR-026** The processor shall safely decode the image.
- **FR-027** The processor shall enforce maximum dimensions and maximum decoded-pixel limits.
- **FR-028** The processor shall distinguish permanent malformed-input errors from retryable infrastructure failures.

### Exact-byte dedup

- **FR-029** Every received object shall receive a SHA-256 fingerprint while being streamed.
- **FR-030** Exact-hash lookup/ownership shall include dedup scope.
- **FR-031** Exact-hash ownership shall use an atomic conditional write.
- **FR-032** A worker replaying its own successful hash claim shall continue idempotently.
- **FR-033** An image matching another validated exact owner shall become an exact duplicate without running PDQ, ORB, SSIM, or embedding inference.

### Canonical-pixel identity

- **FR-034** Non-exact valid images shall be decoded into a versioned canonical identity representation.
- **FR-035** Canonical identity shall preserve meaningful bit-depth/channel semantics.
- **FR-036** The system shall calculate a SHA-256 hash over the canonical decoded representation.
- **FR-037** Matching canonical-pixel hashes shall be represented as pixel-identical duplicates according to policy.
- **FR-038** The canonicalization algorithm version shall be persisted.

### Perceptual candidate generation

- **FR-039** Remaining valid images shall receive a PDQ perceptual fingerprint.
- **FR-040** PDQ implementation/version and quality score shall be recorded.
- **FR-041** PDQ candidate retrieval shall use an indexed binary similarity search.
- **FR-042** PDQ candidate search shall use Hamming distance.
- **FR-043** Candidate search shall be bounded by configurable Top-K and verification limits.
- **FR-044** PDQ similarity alone shall not suppress embedding generation.

### ORB/geometric/structural verification

- **FR-045** Selected PDQ candidates shall undergo ORB local-feature extraction.
- **FR-046** ORB descriptors shall be compared using Hamming-distance matching.
- **FR-047** Raw ORB match count alone shall not declare a duplicate.
- **FR-048** Filtered feature matches shall undergo RANSAC-based geometric verification.
- **FR-049** Geometric verification shall record inlier count and inlier ratio.
- **FR-050** Geometric verification shall record spatial coverage.
- **FR-051** Geometric verification shall record reprojection/geometric error.
- **FR-052** Strong geometric matches shall optionally be aligned for SSIM verification.
- **FR-053** SSIM shall be calculated only when alignment/overlap makes the comparison meaningful.
- **FR-054** Dedup policy shall be versioned.
- **FR-055** Threshold values shall be configurable and benchmark-derived.
- **FR-056** Ambiguous derived-copy cases shall default to normal unique processing.
- **FR-057** CNN embedding similarity shall not participate in the primary duplicate verdict.

### Duplicate grouping/reversibility

- **FR-058** Confirmed duplicates shall join a stable duplicate group.
- **FR-059** Duplicate groups shall maintain a representative image.
- **FR-060** The representative image may change independently of group identity.
- **FR-061** Duplicate source objects shall not be physically deleted by the synchronous dedup decision.
- **FR-062** Dedup decisions shall be auditable.
- **FR-063** Dedup decisions shall be reversible/compensatable.
- **FR-064** Revoked decisions shall allow the affected image to re-enter inference.

### Embedding

- **FR-065** Unique images shall produce `image.inference-ready.v1`.
- **FR-066** The inference worker shall use a model adapter rather than hard-coded ResNet application logic.
- **FR-067** Initial model implementation shall use pretrained ResNet-18 with the final classifier removed.
- **FR-068** Initial embeddings shall be 512-dimensional vectors.
- **FR-069** Embeddings shall be L2-normalized before cosine indexing.
- **FR-070** Inference shall support bounded micro-batching.
- **FR-071** Every embedding shall record model version.
- **FR-072** Every embedding shall record preprocessing version.
- **FR-073** Embedding IDs shall be deterministic for image/group + model version + preprocessing version.

### Embedding archive and vector indexing

- **FR-074** Generated embeddings shall be durably archived independently of the serving vector index.
- **FR-075** Archive writes shall batch records into Parquet files rather than one small object per vector.
- **FR-076** Semantic/visual indexing shall be an independent consumer from inference.
- **FR-077** Vector writes shall use deterministic IDs and idempotent upsert behavior.
- **FR-078** The system shall support image-to-image Top-K retrieval for the active embedding model.
- **FR-079** Search shall enforce tenant authorization and metadata filters.
- **FR-080** Query image preprocessing shall use the same model/preprocessing contract as the searched collection.

### State, retries, and recovery

- **FR-081** Current image processing state shall be queryable by `image_id`.
- **FR-082** State shall represent major stages independently rather than relying only on one ambiguous status string.
- **FR-083** Retryable failures shall use bounded retry with exponential backoff and jitter.
- **FR-084** Deterministic invalid inputs shall not be retried indefinitely.
- **FR-085** Exhausted failures shall be recorded in a repairable failure/DLQ flow.
- **FR-086** DLQ records shall retain original event/stage/error/trace information.
- **FR-087** A reconciliation service shall detect stale/orphaned state.
- **FR-088** Reconciliation shall republish safely recoverable work.
- **FR-089** Loss of the semantic vector index shall be recoverable from the embedding archive without requiring full model reinference.

### Observability

- **FR-090** Every service shall emit structured logs.
- **FR-091** Every service shall emit OpenTelemetry-compatible traces.
- **FR-092** Every service shall emit operational metrics.
- **FR-093** Trace context shall propagate through message boundaries.
- **FR-094** Operators shall be able to inspect the lifecycle of a specific `image_id` using traces/logs.

## 4. Non-functional requirements

- **NFR-001 Throughput:** Architecture shall support initial average ingress of approximately 10M images/day (~116/sec) with horizontal scaling.
- **NFR-002 Future scale:** Architecture shall have a path toward 500M–1B images/day through partitioning, horizontal workers, and multiple processing cells.
- **NFR-003 Bursts:** Capacity design shall account for burst rate, not only daily average.
- **NFR-004 Backpressure:** Downstream slowdowns shall create durable backlog rather than data loss.
- **NFR-005 Outage recovery:** Multi-hour/day worker outages shall be recoverable by replay/backlog drain.
- **NFR-006 Idempotency:** Reprocessing the same event shall converge to the same logical business outcome.
- **NFR-007 Atomicity:** Exclusive ownership invariants shall use atomic conditional operations.
- **NFR-008 Reversibility:** Incorrect dedup/model/index decisions shall have a compensation path.
- **NFR-009 Consistency:** Stale events shall not move a state machine backward incorrectly.
- **NFR-010 Isolation:** Cross-tenant image existence, dedup matches, vectors, and source objects shall not leak through APIs.
- **NFR-011 Durability:** Accepted originals shall remain durable independently of worker health.
- **NFR-012 Derived durability:** Embeddings shall remain durable independently of vector-index health.
- **NFR-013 Availability:** Search outage shall not stop ingestion.
- **NFR-014 Availability:** Inference outage shall not stop source uploads/dedup stages.
- **NFR-015 Availability:** Indexer outage shall not stop embedding generation.
- **NFR-016 Fault tolerance:** Network calls shall use bounded timeouts and classified retries.
- **NFR-017 Security:** Image input shall be treated as untrusted.
- **NFR-018 Security:** Production communications shall use encryption in transit.
- **NFR-019 Security:** Durable production storage shall use encryption at rest.
- **NFR-020 Maintainability:** Core algorithms and infrastructure shall be behind explicit interfaces/adapters.
- **NFR-021 Versioning:** Model, preprocessing, canonicalization, PDQ implementation, event schema, and dedup policy shall be versioned.
- **NFR-022 Resource safety:** Local and production workers shall have bounded memory, I/O, candidate fan-out, and CPU concurrency.
- **NFR-023 Observability:** High-cardinality values such as `image_id` shall be used in traces/logs, not Prometheus labels.
- **NFR-024 Performance measurement:** Thresholds/concurrency shall be derived from benchmark/load data, not hard-coded assumptions.
- **NFR-025 Dedup quality:** Automatic near-copy deduplication shall optimize for precision; ambiguous cases shall be processed as unique.

## 5. Acceptance outcome

The first complete release is acceptable when a junior engineer can:

1. upload images through a presigned Floci S3 URL,
2. observe SQS and Kafka events,
3. detect exact and controlled derived duplicates,
4. handle corrupt files without worker crashes,
5. generate ResNet-18 embeddings for unique images,
6. search by a query image,
7. observe the complete trace in Jaeger,
8. observe service metrics in Grafana,
9. kill/restart processing services without losing accepted work,
10. replay a duplicate event safely without creating duplicate business state.
