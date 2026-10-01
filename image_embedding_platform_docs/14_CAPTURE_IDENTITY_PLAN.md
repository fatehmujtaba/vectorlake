# M4 capture identity and activation plan

## Decision and current boundary

Different captures of the same video are distinct observations, even when
their bytes or canonical pixels happen to match. A transformed copy of one
capture can share its source observation. Visual similarity alone cannot
establish this relationship for near-identical video frames.

The upload API now accepts an optional `capture_id`: an opaque, tenant-supplied
identifier for the source observation. The same identifier may be used for
derivatives of that observation; different captures must use different IDs.
It is stored with the image and included in request idempotency. This is an
**assertion by the client**, not trusted evidence that two files are copies.
Existing callers can omit it, and the current exact/pixel suppression behavior
is unchanged. Near-copy suppression remains disabled.

## Delivery steps

1. **Capture contract (in progress).** Persist and expose `capture_id`; reject
   malformed values and conflicting idempotent replays. Exit: API and storage
   tests pass, including legacy request hash compatibility.
2. **Observation-preserving identity.** Define an authenticated source namespace
   and a stable capture key. Ensure every upload retains its own image record.
   Separate content reuse from observation suppression: exact/pixel equality
   may reuse computed work, but cannot discard an independently identified
   capture. Specify behavior for missing/conflicting provenance and replay.
   Exit: two distinct capture IDs with identical bytes/pixels both remain
   observable; an explicit derivative/retry does not create a second logical
   observation. Include concurrency and cross-tenant tests.
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
not proof of a copy. Legacy uploads without provenance should remain
observation-preserving under the eventual policy rather than be silently
collapsed by visual similarity.
