# M4 capture identity and activation plan

## Decision and current boundary

Different captures of the same video are distinct observations, even when
their bytes or canonical pixels happen to match. A transformed copy of one
capture can share its source observation. Visual similarity alone cannot
establish this relationship for near-identical video frames.

The upload API accepts an optional `capture_id`: an opaque, tenant-supplied
identifier for the source observation, unique within the dedup scope. The same
identifier may be used for derivatives of that observation; different captures
must use different IDs. It is stored with the image and included in request
idempotency. This is an **assertion by the client**, not independent evidence
that two files are copies. Existing callers can omit it.

Deterministic dedup policy v2 claims byte/pixel hashes within a capture
namespace. A missing `capture_id` gets an image-specific namespace, so legacy
uploads are treated as separate observations even if their content is equal.
Distinct capture IDs also remain separate. This favors avoiding lost
observations over saved inference cost; explicit same-capture copies still
converge. This changes M3's old scope-wide ownership behavior and requires a
state migration/replay plan before rolling out over persisted v1 records.
Near-copy suppression remains disabled.

## Delivery steps

1. **Capture contract (done).** Persist and expose `capture_id`; reject
   malformed values and conflicting idempotent replays. Exit: API and storage
   tests pass, including legacy request hash compatibility.
2. **Observation-preserving identity (in progress).** The worker now keeps
   distinct capture IDs and missing-provenance uploads separate; focused tests
   cover exact/pixel behavior, and the M3 drill includes concurrent same-
   capture claims. Before rollout, establish an authenticated producer/source
   namespace, guarantee stable capture keys, and define migration/replay over
   persisted v1 records. Then separate content reuse from observation identity
   in M5 so distinct captures can share compute without losing search results.
   Exit: source collision, concurrency, cross-tenant, and replay tests pass;
   identical content from separate captures stays independently observable.
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

The production source of capture IDs is not yet defined. A filename-derived ID
is suitable only for this Kaggle evaluation. Before step 2 changes production
dedup semantics, decide whether the authenticated producer can supply a stable
camera/recording namespace and capture ID (or timestamp/frame key), and how
derivatives refer to the source capture. A self-declared matching ID alone is
not proof of a copy. Legacy uploads without provenance remain separate
observations. If two actual captures are assigned the same `capture_id`,
exact/pixel v2 can still suppress one, so source-side uniqueness enforcement
is a release prerequisite. The current local API auth is a development stub;
production upload authentication must be configured before these assertions
are trusted.
