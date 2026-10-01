# M4 capture identity and activation plan

## Decision and current boundary

Different captures of the same video are distinct observations, even when
their bytes or canonical pixels happen to match. A transformed copy of one
capture can share its source observation. Visual similarity alone cannot
establish this relationship for near-identical video frames.

The upload API accepts `source_id` and `capture_id` as a pair. Together they
identify an observation within a tenant's dedup scope. A source might be a
camera or recording; a capture is a stable frame/capture key within it.
Derivatives may reuse the pair; distinct captures or sources must not. Both
fields are stored and included in request idempotency. They remain **client
assertions**, not independent evidence that two files are copies. Existing
callers can omit both fields.

Deterministic dedup policy v3 claims byte/pixel hashes within the source/capture
namespace. Missing provenance gets an image-specific namespace, so such
uploads remain separate observations even if their content is equal. The
worker can still read the v2 key for a stored capture-only image. This favors
avoiding lost observations over saved inference cost; explicit same-source,
same-capture copies still converge. Near-copy suppression remains disabled.

New completed records carry `deterministic_policy_version`. On replay, the
worker skips a completed record with a missing or different marker: its
outcome was published before `dedup_status` was written, and re-evaluating it
under v3 could change the group or event type. Incomplete prior-policy records
can still be processed.
This is a **replay guard**, not a full migration of old state or registries.

## Delivery steps

1. **Capture contract (done).** Persist and expose the source/capture pair; reject
   malformed values and conflicting idempotent replays. Exit: API and storage
   tests pass, including legacy request hash compatibility.
2. **Observation-preserving identity (in progress).** The worker separates
   distinct sources, captures, and missing-provenance uploads; focused tests
   cover exact/pixel keys and the M3 drill covers concurrent same-capture
   claims. Completed prior-policy replays are skipped. Before rollout,
   authenticate/bind the producer to its source, guarantee stable capture
   keys, and inventory/migrate persisted v1/v2 state and registries. Then
   separate content reuse from observation identity in M5 so distinct
   captures can share compute without losing search results. Exit: source
   collision, concurrency, cross-tenant, and replay tests pass; identical
   content from separate captures stays independently observable.
3. **Quality calibration.** Add independently sourced, labeled hard negatives
   and crop positives. Freeze candidate and verification policy on calibration
   only; measure recall, false positives, and stage latency. Exit: explicit
   false-positive and recall gates pass without inspecting the final set.
4. **Final gate and activation.** Score fresh source-disjoint final recordings
   once; review reversible decision and recovery drills. Enable derived
   suppression only for provenance-backed relationships and only if the final
   gate passes. Otherwise leave it shadow-only and proceed with M5 independent
   of fuzzy suppression.

## Open input

The production source of these IDs is not yet defined. A filename-derived pair
is suitable only for this Kaggle evaluation. The authenticated producer must
be bound to its `source_id`, and must assign stable capture IDs (or timestamp/
frame keys) and link derivatives to the source capture. A self-declared pair
alone is not proof of a copy. If two actual captures in one source receive the
same capture ID, exact/pixel v3 can still suppress one; source-side uniqueness
is a release prerequisite. The current local API auth is a development stub.

Before production rollout, inventory unversioned completed rows and their
published events. Do not mass-replay or rewrite them under v3. Keep their
historic outcome, migrate/reconcile consumers and search mappings explicitly,
and retire v1/v2 registries only after a validated backfill and rollback plan.
Drain or stop older processor versions before v3 workers start; mixed worker
versions can race to write different outcomes on the same image state.

The local development token is now bound to one configured source ID; the API
returns 403 for a different claimed source. This exercises the authorization
contract but does not replace production producer authentication. The read-only
`make audit-dedup` command scans the local image table with bounded pagination
and reports partial coverage explicitly. On 2026-10-01, the local table had
122 records: 90 completed without a policy marker, 31 completed under v3, and
one incomplete. Nine records had only part of the source/capture pair. These
are development fixtures, not a production inventory or a migration.
