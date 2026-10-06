# M6a retry, dead-letter, and backlog preflight

Implemented and verified locally on 2026-10-06. This increment covers Kafka
processor, inference, archive and semantic-index workers. The SQS bridge
continues to use the existing queue visibility and redrive/DLQ configuration.
The VectorLake HTTP API and vector service were stopped at the user's request;
M6a checks do not start HTTP listeners.

## Durable retry behavior

Every source record is identified by stage, consumer group and a checksum of
its Kafka topic, partition, offset, payload, key and headers. Null values and
header order are retained. Retry state lives in DynamoDB `worker_retries`.
The default budget is five attempt reservations per source record. A restart
or consumer recreation does not reset that budget.

An attempt acquires a conditional token and a 600-second lease. Active leases
prevent a second worker from claiming the same record. Conditional updates
reject stale owners. Interrupted reservations count against the budget; an
interrupted final attempt is quarantined after its lease expires because its
business outcome may be uncertain. Records successfully handled before a
crash still depend on the stage's existing idempotency contract during replay.

Retryable errors receive exponential backoff with full jitter: the delay is
uniformly chosen between zero and the smaller of 8 seconds and
`0.5 * 2^(attempt - 1)` seconds. The next eligible time is persisted.
Malformed Protobuf, invalid input and deterministic state-contract errors
are permanent; a single-record permanent failure is dead-lettered on its
first failure. Infrastructure/runtime failures retry up to the budget.
Exception messages are not copied into the DLQ contract; safe error codes
and exception class names are retained.

The executor preserves inference and archive micro-batching. A failed batch
is retried as individual records so a poison record can be isolated from
healthy peers. The initial failed batch consumes one reservation for each
member. A budget of at least two is required to allow isolation. Interrupted
or already-exhausted records may be quarantined conservatively for repair.

## Dead-letter durability and ordering

Before acknowledging a terminal failure, the worker:

1. Writes an immutable JSON snapshot into S3 `vectorlake-worker-deadletters`.
2. Verifies the object's SHA-256 metadata and encoded length.
3. Publishes and acknowledges `worker.dead-letter.v1` with the snapshot reference.
4. Marks the retry ledger `deadlettered`.
5. Commits the source Kafka offset after every member of that fetched batch resolves.

The S3 snapshot retains original bytes using base64, original key, headers,
and source position. The Protobuf envelope retains stage, consumer group,
attempt count, safe error code/type, frozen failure timestamp, source position,
snapshot reference/checksum and original event context when parseable and at
most 16 KiB. Larger contexts remain intact in the source snapshot; they are
not copied into the envelope, which prevents a near-limit input from producing
an oversized Kafka DLQ message.
Malformed records need not have tenant/image identity; no identity is invented.
Referencing S3 keeps the envelope small even for an input at the Kafka/event
size limit. Snapshot contents require restricted access in a production rollout.

Snapshot, Kafka or ledger failures leave source offsets uncommitted. An
already-terminal record retries DLQ delivery without spending another business
attempt. Re-publication after a commit gap has the same logical record ID and
envelope. Kafka delivery is at least once; downstream DLQ readers must dedup
by `record_id`. A `completed` or `deadlettered` ledger record skips business
reprocessing on offset replay.

Local workers use manual offset storage/commit and a 30-minute maximum poll
interval to cover bounded lease waits and local batch processing. Blocking
dependency outages still stop that partition when durable quarantine itself
is unavailable. Production lease renewal, rebalance/outage acceptance,
retention/cleanup and capacity limits remain part of M6/M7 acceptance.

## Read-only preflight

Run:

```sh
make worker-preflight
```

The tool reads each existing consumer group's committed offsets, captures
Kafka low/high watermarks, and manually assigns the bounded unread ranges.
It does not subscribe/join the worker groups, commit offsets, publish events,
write image state, create resources, or replay historical work.

Checks include malformed events, missing state, tenant/scope/source/group
identity mismatches, stale outcomes, model mismatches, pending stage markers,
conflicting event-ID payloads, conflicting embedding-ID payloads, retention
gaps and offset positions beyond current high watermarks. It also performs
the existing read-only historical outcome comparison. Incomplete scans and
historical conflicts block activation. The report is a point-in-time review
artifact, not an atomic snapshot or a production rollout authorization.

Output: `.artifacts/m6/worker_preflight.json`. The command exits 2 when
activation is blocked; this means the review found blockers, not a tool crash.
Default limits are 10,000 messages and 30 seconds per stage/outcome scan.
The CLI supports `--max-messages`, `--max-seconds` and `--output`.
It is restricted to loopback local infrastructure.

Observed local snapshot on 2026-10-06:

| Worker | Unread records scanned | Result |
|---|---:|---|
| Processor | 0 | No unread work |
| Inference | 196 | 88 ready, 5 completed replays, 103 conflicting records |
| Archive | 5 | 5 ready |
| Semantic index | 5 | 2 ready, 3 completed replays |

All scans completed with no retention gap. The historical comparison covered
183 completed images and 313 outcome records. It found 139 matching images,
34 images with conflicting expected/group events and 10 with conflicting
outcome types. Inference's conflict count includes repeated records and event
payload variants; it is not a count of distinct affected images.
`activation_allowed` was false. No historical outcomes were rewritten.

## Rollout configuration

Autonomous workers now default to disabled. Each entrypoint checks this before
initializing its processing backend; the shared runner checks it again.
Configuration uses the `VECTORLAKE_LOCAL_` prefix:

| Setting | Default | Constraint |
|---|---:|---|
| `WORKERS_ENABLED` | false | Enable only after preflight blockers and rollout review |
| `WORKER_MAX_ATTEMPTS` | 5 | 2 through 10 |
| `WORKER_RETRY_BASE_SECONDS` | 0.5 | 0 through 30 |
| `WORKER_RETRY_CAP_SECONDS` | 8 | 0 through 60 |
| `WORKER_LEASE_SECONDS` | 600 | 60 through 600 |

Bootstrap creates the retry table, snapshot bucket and versioned DLQ topic.
The implementation does not rebuild or restart existing containers. The old
processor container was stopped as part of the user's backend shutdown request.
A future container rollout must rebuild the image to include M6a. The HTTP
services remain stopped and are separate from this worker gate. Local database
and Kafka infrastructure remains available for isolated verification.

## Verification and next increment

`make e2e-m6-retry` used an isolated input topic and real local DynamoDB/S3/Kafka.
It proved recovery on attempt three, one-attempt permanent/malformed
quarantine, bounded exhaustion at three, persisted budget after a new executor,
four verifiable durable DLQ snapshots, source-offset commits, replay skips,
and identical logical DLQ re-publication. The drill starts no HTTP servers
and leaves isolated test records available for audit.

Unit tests additionally cover failed snapshot/Kafka delivery, persisted jitter,
active-lease/stale-owner fencing, interrupted final attempts, poison isolation,
commit ordering, disabled rollout and read-only/incomplete preflight scans.
The full model-enabled regression suite passed 143 tests, and `make lint`
passed Ruff validation/formatting and strict mypy checks.

Next is **M6b**: independent stage projection, complete processing status and
safe reconciliation of explicitly recoverable gaps. It will be validated on
seeded clean records. Historical conflict repair still requires review and a
defined rollback, as recorded in the [remaining plan](../REMAINING_WORK_PLAN.md).
After M6b comes M6c observability, followed by the remaining data/identity/repair
gates and M7 release acceptance.
