# Read-only outcome reconciliation

`make reconcile-outcomes` compares completed local `image_state` rows with the
retained `image.inference-ready.v1`, `image.duplicate.v1`, and
`processing.failed.v1` Kafka events. It reads from bounded low-to-high
partition offsets and never commits offsets, rewrites state, or republishes an
event. The tool refuses non-loopback infrastructure endpoints.

For each completed image, it checks the outcome topic, tenant, dedup scope,
duplicate group, representative (for duplicates), source object (for inference),
and failure stage. Byte-identical deliveries with one event ID count as replay;
different payloads under the same event ID remain visible as conflicting
variants. A revoked derived decision may legitimately have an old duplicate
and a compensating inference event.

The report includes `expected_event_mismatch_fields` to count field-level
differences in retained events with the expected topic. To inspect one image
without modifying state or committing offsets, run
`uv run --locked python -m infrastructure.reconcile_outcomes --image-id IMAGE_ID`.
The optional `inspection` shows the completed row and each retained outcome
variant, including event IDs and mismatched fields. It is subject to the same
scan bounds and coverage caveats as the aggregate report.

`coverage_complete` means both paginated scans reached their bounds, no topic
was missing, and retained Kafka history began at offset zero. It is **not** an
atomic snapshot across DynamoDB and Kafka. If it is false, missing-event
findings are provisional. Kafka retention can erase older outcomes; the tool
cannot prove that a missing event was never published. The report also does
not validate downstream embedding archives or search mappings.

On 2026-10-02, the local scan found 156 completed images and complete retained
coverage: 112 matched state, 34 had conflicting variants of the expected event,
and 10 had different outcome types. No completed image lacked an outcome event.
Samples show group IDs changing across earlier policy revisions. An outdated
local processor container had been running alongside newer direct M3 drills;
it was stopped, rebuilt with the completed-image replay guard, and restarted
without deleting data. Fresh M3 drills and the rebuilt worker's valid/malformed
image verification raised the matched count while the historical conflict
counts stayed unchanged. The scan observed 106 conflicting same-event-ID
payloads across retained messages. All 34 expected-topic mismatches were in
`duplicate_group_id`; an inspected image had two inference events with the
same event ID but different groups. Another had both a duplicate and an
inference outcome. This is development-state evidence, not a
production incident or a completed migration. Do not auto-repair or replay
these rows. Review each conflict against its published event, policy, and
downstream consumers before any explicit backfill with rollback.
