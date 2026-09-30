# Operations, Observability, Load and Failure Testing

## 1. Operational goal

An operator should be able to answer:

- How many images are arriving?
- How far behind is each processing stage?
- Why did one image fail?
- How many GPU/CPU inference calls were avoided through dedup?
- Which dependency is causing latency?
- Can a failed service restart without losing work?
- Can we rebuild the search index without recomputing embeddings?

## 2. Metrics

### Ingestion

```text
upload_sessions_created_total
upload_sessions_conflict_total
images_stored_total
bytes_stored_total
s3_notification_total
sqs_oldest_message_age_seconds
```

### Kafka

```text
consumer_lag{group,topic}
oldest_unprocessed_event_age_seconds{stage}
messages_processed_total{stage}
messages_failed_total{stage}
```

Avoid `image_id` as a metric label.

### Validation/dedup

```text
validation_success_total
validation_failure_total{error_code}
exact_duplicate_total
pixel_duplicate_total
pdq_candidate_count histogram
pdq_search_duration_seconds
orb_verification_total
orb_verification_duration_seconds
ransac_reject_total
derived_duplicate_total
ambiguous_processed_unique_total
```

### Inference

```text
inference_images_total
inference_batch_size histogram
inference_duration_seconds
embedding_generation_failure_total
inference_avoided_total
```

### Archive/index

```text
archive_records_total
archive_flush_duration_seconds
archive_failure_total
index_upsert_total
index_upsert_duration_seconds
index_failure_total
```

### Reconciliation

```text
reconciliation_orphans_found_total{type}
reconciliation_repairs_total{type}
reconciliation_failures_total{type}
```

## 3. Dashboard panels

Recommended first Grafana dashboard:

```text
Ingress images/sec
Ingress MB/sec
SQS oldest message age
Kafka lag by consumer group
Oldest unprocessed event age by stage
Exact duplicate percentage
Pixel duplicate percentage
Derived duplicate percentage
PDQ candidates/image p50/p95
ORB verification rate
Inference throughput
Inference batch size
Embedding archive lag
Vector index lag
Failure/DLQ rate
End-to-end p50/p95/p99
```

## 4. Distributed tracing

Every message preserves W3C trace context where practical.

Typical processor trace:

```text
process_image
├── s3.fetch
├── sha256.stream
├── ddb.exact_claim
├── image.decode
├── canonical_pixel_hash
├── ddb.pixel_claim
├── pdq.compute
├── pdq.search
├── candidate.orb
├── candidate.ransac
├── candidate.ssim
└── kafka.publish
```

Only candidate images should create ORB/RANSAC spans.

## 5. Trace sampling

At production scale, do not keep 100% of successful traces indefinitely.

Suggested policy concept:

```text
errors -> sample 100%
slow requests -> high sampling
normal successful traffic -> probabilistic lower sampling
```

Local development can sample 100%.

## 6. Logs

Structured JSON. Include:

```text
timestamp
severity
service
event
image_id
event_id
trace_id
stage
policy/model version
error_code
retryable
duration_ms
```

Do not include:

```text
raw images
passwords/tokens
full presigned URLs
unbounded binary payloads
```

## 7. Fault-injection tests

### Kill processor

Expected:

```text
uploads continue
SQS/Kafka ingress continues
image.received lag grows
restart processor -> lag drains
```

### Kill inference worker

Expected:

```text
dedup continues
image.inference-ready lag grows
restart -> backlog drains
```

### Kill vector-service during dedup

Expected:

```text
exact/pixel deterministic paths continue if possible
fuzzy copy decision must not invent a duplicate
configured retry/fail-open path processes uncertain image safely
```

### Kill vector-service during semantic indexing

Expected:

```text
inference continues
archive continues
index lag grows
restart -> idempotent catch-up
```

### Duplicate Kafka event

Expected:

```text
same final state
no duplicate logical embedding
no second canonical hash owner
```

### Crash exact owner after DDB claim

Expected:

```text
message replay recognizes same owner and resumes
```

### Corrupt image

Expected:

```text
permanent failure
no endless retry
worker remains healthy
```

## 8. Concurrency race tests

### Exact duplicate storm

Launch hundreds/thousands of identical uploads concurrently.

Assert:

```text
exact_hash_registry has one owner
all others reference same duplicate group
embedding runs once for representative where applicable
```

### State replay ordering

Feed older lifecycle event after newer event.

Assert generation/state guard prevents regression.

## 9. Load testing modes

### Real object path

Use real Copydays/COCO/synthetic files. This measures S3, decode, hashes, ORB, inference.

### Event-only stress

Reuse a controlled object pool and publish many unique event IDs to measure Kafka scheduling/worker overhead. Clearly label this as an event-plane test, not real object throughput.

### Candidate worst-case

Construct images that intentionally produce many perceptual candidates and verify `MAX_ORB_CANDIDATES` protects CPU.

## 10. Performance budgeting

Measure each stage separately before tuning whole-pipeline throughput:

```text
S3 fetch
SHA hashing
safe decode
pixel canonicalization
PDQ
PDQ vector lookup
ORB/RANSAC/SSIM only for candidate subset
ResNet inference
Parquet archive
semantic index upsert
```

Optimization priority should follow measured total CPU/time contribution.

## 11. Backlog recovery test

1. Run producer at a stable local rate.
2. Stop inference for a fixed interval.
3. Observe `image.inference-ready` lag/age increase.
4. Restart inference.
5. Measure backlog drain rate.
6. Confirm no missing/duplicate logical embeddings.

Use the formula:

```text
required rate = arrival rate + backlog / target drain duration
```

to explain production autoscaling.

## 12. Index rebuild drill

1. Archive a set of embeddings to Parquet.
2. Delete/recreate semantic vector collection.
3. Run rebuild importer from archived Parquet.
4. Validate vector count and retrieval regression tests.

No ResNet inference should occur during rebuild.

## 13. Dedup quality gate

Before changing `strict-copy-vN` thresholds:

- run Copydays,
- run synthetic transform benchmark,
- compare false-positive rate,
- compare candidate recall,
- compare ORB/SSIM cost,
- generate a versioned evaluation report,
- create a new policy version if behavior changes.

## 14. Release acceptance

A release candidate passes only if:

- unit/contract/integration tests pass,
- dedup precision gate passes,
- replay/idempotency tests pass,
- corruption corpus causes no service crash,
- kill/restart tests recover,
- vector rebuild works from archive,
- dashboard/traces show expected flow.
