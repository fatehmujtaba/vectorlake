# Deduplication Design

## 1. Goal

The goal is **high-precision copy detection before embedding generation**, not broad semantic similarity detection. The dedup subsystem should suppress inference only when there is strong evidence that the incoming image is the same underlying image or a controlled derivative of an already processed image.

We deliberately prefer false negatives over false positives:

```text
missed duplicate -> extra compute cost
false duplicate  -> potentially lost/distorted scientific information
```

Therefore ambiguous cases continue as unique.

## 2. Locked stages

```text
1. SHA-256 exact-byte identity
2. Canonical-pixel SHA-256
3. PDQ perceptual candidate generation
4. ORB feature extraction
5. Hamming descriptor matching
6. RANSAC geometric verification
7. Aligned SSIM structural verification
8. Versioned strict policy
```

We do **not** use:

- SIFT in the initial path,
- color histograms as a decision feature,
- RGB/channel hashing,
- CLIP/DINO/transformer embeddings for dedup,
- CNN embedding cosine similarity as a duplicate verdict,
- one arbitrary "90% similarity" threshold.

## 3. Stage 1 — SHA-256

Question answered:

> Are the files byte-for-byte identical?

Implementation:

```python
import hashlib

sha = hashlib.sha256()
for chunk in stream:
    sha.update(chunk)
digest = sha.hexdigest()
```

Calculate during the S3 download to avoid a second full read.

### Concurrency correctness

Two identical new images can arrive at the same time. Do not perform separate `GET` then `PUT` operations and hope nobody races.

Use a DynamoDB conditional write. Deterministic policy v2 keys ownership by
dedup scope **and capture namespace**:

```text
SCOPE#{scope_id}#CAPTURE#{sha256(capture_id)}#SHA256#{digest}
```

For an upload with no `capture_id`, use `SCOPE#{scope_id}#IMAGE#{image_id}`
instead of the capture prefix. Canonical-pixel claims use the same namespace
and append the canonicalization version and pixel digest. This means identical
content from separately identified captures, or legacy uploads with no
capture ID, remains separate observations. The caller must assign a unique
capture ID to each real capture and reuse it only for derivatives; this
client assertion requires trusted source identity before production rollout.
The former scope-wide v1 registry is not compatible with v2 replay without a
migration plan.

The first worker claims ownership. The second receives a conditional conflict and resolves the existing owner.

A retried owner must recognize itself:

```text
existing.owner_image_id == current.image_id
```

means this is a retry, not a duplicate of itself.

## 4. Stage 2 — Canonical-pixel SHA-256

Question answered:

> After deterministic safe decoding, are the meaningful pixel samples identical?

This catches cases such as metadata changes or container-level differences where the file bytes differ but decoded content is exactly the same.

Canonical representation must include enough metadata to preserve scientific meaning:

```text
canonicalization_version
width
height
dtype / bit depth
channel count
channel semantics/order
deterministically oriented pixel bytes
```

Do not blindly convert all scientific data to RGB uint8 for the identity hash. Model preprocessing and identity canonicalization are different concepts.

Example concept:

```python
payload = (
    version_bytes
    + width_bytes
    + height_bytes
    + dtype_bytes
    + channel_descriptor
    + canonical_pixel_bytes
)
pixel_sha256 = sha256(payload)
```

Store the canonicalization version with the fingerprint.

## 5. Stage 3 — PDQ

PDQ is a 256-bit perceptual spectral fingerprint. It uses a Discrete Cosine Transform based process and provides a quality signal. Unlike SHA-256, PDQ is intentionally tolerant to certain visual changes.

PDQ's role is **candidate generation only**.

```text
incoming image
   ↓
PDQ 256 bits
   ↓
Hamming nearest-neighbor search
   ↓
small candidate list
```

The PDQ distance does not independently create a duplicate decision.

### Python implementation

Use a maintained PDQ implementation behind an adapter such as:

```python
class PerceptualHasher(Protocol):
    def hash(self, rgb: np.ndarray) -> tuple[bytes, int]:
        """Return 256-bit fingerprint and quality."""
```

The adapter prevents the whole system from depending directly on one package.

### Why an adapter matters

PDQ implementation, thresholding, rotation handling, or package choice may change. Event/state schema should store:

```text
perceptual_algorithm = "pdq"
perceptual_version = "v1"
quality = ...
```

## 6. PDQ candidate index

The vector service exposes an operation like:

```http
POST /internal/v1/dedup/pdq/search
```

Request:

```json
{
  "scope_id": "tenant-1",
  "fingerprint_hex": "...",
  "top_k": 20
}
```

Local vector service uses Milvus Lite. Production can use Milvus Distributed. Milvus supports binary vectors with Hamming distance, which maps naturally to PDQ.

Candidate metadata:

```text
image_id
duplicate_group_id
dedup_scope_id
object_key
pdq_quality
width
height
```

## 7. Stage 4 — ORB

ORB is selected as the initial local-feature verifier because it is efficient, CPU-friendly, and produces binary descriptors that use Hamming distance.

ORB's role:

> Determine whether distinctive local structures in the incoming image correspond to structures in a PDQ candidate.

Typical flow:

```text
incoming + candidate
       ↓
ORB keypoints/descriptors
       ↓
Hamming descriptor matching
       ↓
match filtering
       ↓
coordinate pairs
```

SIFT is not in the initial path. This is an engineering tradeoff, not a claim that ORB is universally more accurate. The verifier interface must allow a future feature extractor to be substituted if benchmark recall shows ORB is insufficient.

## 8. ORB matching

Possible filtering policy:

1. KNN match descriptors.
2. Apply a ratio/cross-check strategy.
3. Require a minimum number of candidate correspondences.
4. Pass coordinates to RANSAC.

Do not make raw match count the final decision. Repetitive textures can create many false feature matches.

Record metrics such as:

```text
query_keypoints
candidate_keypoints
tentative_matches
filtered_matches
```

## 9. Stage 5 — RANSAC

RANSAC establishes whether descriptor matches agree on a coherent geometric transformation.

Depending on image assumptions, estimate an affine transform or homography.

Record:

```text
ransac_inliers
ransac_inlier_ratio
spatial_coverage_query
spatial_coverage_candidate
median_reprojection_error
transform_type
```

Example of strong evidence:

```text
filtered matches        620
RANSAC inliers          571
inlier ratio            0.92
query coverage           0.86
candidate coverage       0.91
median reprojection err  1.4 px
```

Example of weak evidence:

```text
filtered matches        620
RANSAC inliers           39
inlier ratio             0.06
```

The second candidate is rejected even though raw ORB count is high.

## 10. Stage 6 — aligned SSIM

SSIM should be used only when the candidate geometry makes a structural comparison meaningful.

After RANSAC:

```text
candidate image
   ↓
warp into query coordinates
   ↓
compute overlap mask
   ↓
aligned SSIM on valid overlap
```

Record both SSIM and overlap/coverage. A tiny logo match should not make two unrelated full images duplicates.

## 11. Strict decision policy

The first version should be explicit and rule-based, not a fake probability.

Conceptual policy:

```text
IF exact SHA match:
    EXACT_DUPLICATE

ELSE IF canonical-pixel SHA match:
    PIXEL_DUPLICATE

ELSE:
    PDQ must place candidate inside candidate radius
    AND ORB must pass feature prefilter
    AND RANSAC must show strong coherent geometry
    AND spatial coverage must be sufficient
    AND aligned SSIM must satisfy the calibrated structural rule
    => DERIVED_DUPLICATE

OTHERWISE:
    UNIQUE_OR_AMBIGUOUS -> process as unique
```

Do not hard-code sample thresholds from documentation. Tune on Copydays + our synthetic derived corpus and choose thresholds to maximize precision subject to acceptable recall.

## 12. Why semantic embeddings are not in dedup

Two different photographs of the same subject may have very similar CNN or transformer embeddings. That is useful for retrieval but dangerous for destructive dedup decisions.

Therefore:

```text
CNN embedding similarity -> search/retrieval
PDQ + ORB + geometry + SSIM -> copy verification
```

Embedding generation happens **after** dedup completes.

## 13. Duplicate groups

Never physically delete the incoming source object during the synchronous dedup decision.

Create a stable duplicate group:

```text
duplicate_group_id = dg_123
members = [img_A, img_B, img_C]
representative_image_id = img_A
```

The group ID is stable. The representative may change later if a better-quality copy arrives.

Why this matters:

- reversible decisions,
- auditability,
- representative quality upgrades,
- legal/retention differences between sources,
- no irreversible data loss on a false positive.

## 14. Reversing a bad decision

If a dedup decision is later found wrong:

1. mark the decision revoked,
2. remove the image from the duplicate group,
3. publish a compensation/reprocess event,
4. send the image to inference,
5. index its own embedding.

No source bytes were lost.

## 15. Tenant/dedup scope

Hash matching must not leak cross-tenant existence information.

Every dedup lookup includes `dedup_scope_id`. Default to tenant scope unless the product explicitly permits broader deduplication.

Deterministic policy v2 keys include scope and capture namespace:

```text
SCOPE#{scope_id}#CAPTURE#{sha256(capture_id)}#SHA256#{hash}
SCOPE#{scope_id}#CAPTURE#{sha256(capture_id)}#PIXEL#{version}#{hash}
```

For missing capture IDs, substitute `IMAGE#{image_id}` for the capture
segment. PDQ candidates remain scope-filtered shadow evidence; they do not
cause suppression.

PDQ vector queries also apply a scope filter.

## 16. Corrupt files are not duplicates

If two uploads contain the same invalid bytes, the system may reuse a known validation failure for efficiency, but they should not be treated as valid content duplicates.

Classification example:

```text
validation_status = FAILED_PERMANENT
error_code = IMAGE_DECODE_FAILURE
byte_sha256 = ...
```

## 17. Bounded concurrency

Local defaults:

```env
PROCESSOR_MAX_INFLIGHT_IMAGES=4
S3_FETCH_CONCURRENCY=8
PDQ_SEARCH_CONCURRENCY=8
ORB_VERIFY_CONCURRENCY=2
PDQ_TOP_K=20
MAX_ORB_CANDIDATES=10
```

Why ORB concurrency is lower: ORB + geometric verification is CPU-heavy. PDQ queries are relatively cheap I/O/index operations.

## 18. Persisting ORB descriptors

Do not persist ORB descriptors on day one unless profiling shows repeated recomputation is material.

Optimization path:

```text
first version: calculate candidate descriptors on demand
later: cache/persist descriptors for canonical representatives
```

Correctness does not depend on this cache.

## 19. Decision record schema

Each non-trivial decision should be explainable:

```json
{
  "image_id": "img_123",
  "candidate_image_id": "img_045",
  "policy_version": "strict-copy-v1",
  "decision": "DERIVED_DUPLICATE",
  "pdq_distance": 7,
  "pdq_quality": 92,
  "orb_filtered_matches": 522,
  "ransac_inliers": 491,
  "ransac_inlier_ratio": 0.941,
  "query_coverage": 0.88,
  "candidate_coverage": 0.90,
  "median_reprojection_error": 1.6,
  "aligned_ssim": 0.994,
  "created_at": "..."
}
```

Do not call this `confidence=0.994` unless we have statistically calibrated such a probability.

## 20. Core pseudocode

```python
async def process_received_image(event: ImageReceivedV1) -> None:
    local_path, byte_sha = await object_store.stream_to_temp_and_hash(
        event.bucket,
        event.object_key,
    )

    exact = await exact_registry.claim_or_get(
        scope_id=event.dedup_scope_id,
        digest=byte_sha,
        image_id=event.image_id,
    )
    if exact.is_other_owner:
        await decisions.mark_exact_duplicate(event.image_id, exact.owner_image_id)
        await publisher.publish_duplicate(...)
        return

    decoded = safe_decode(local_path)
    pixel_sha = canonical_pixel_sha(decoded)

    pixel = await pixel_registry.claim_or_get(
        scope_id=event.dedup_scope_id,
        canonicalization_version=PIXEL_CANON_VERSION,
        digest=pixel_sha,
        image_id=event.image_id,
    )
    if pixel.is_other_owner:
        await decisions.mark_pixel_duplicate(event.image_id, pixel.owner_image_id)
        await publisher.publish_duplicate(...)
        return

    pdq, quality = pdq_hasher.hash(decoded.perceptual_view())
    candidates = await vector_service.search_pdq(
        scope_id=event.dedup_scope_id,
        fingerprint=pdq,
        top_k=settings.PDQ_TOP_K,
    )

    for candidate in candidates[: settings.MAX_ORB_CANDIDATES]:
        if not cheap_candidate_filters(decoded, candidate):
            continue

        candidate_img = await load_candidate(candidate)
        orb = orb_matcher.match(decoded, candidate_img)
        if not orb.prefilter_pass:
            continue

        geo = geometry.verify(orb)
        if not geo.pass_geometry:
            continue

        structural = ssim_verifier.compare_aligned(decoded, candidate_img, geo)
        result = policy.decide(pdq, quality, orb, geo, structural)
        if result.is_duplicate:
            await decisions.join_duplicate_group(event.image_id, candidate.group_id, result)
            await publisher.publish_duplicate(...)
            return

    await vector_service.upsert_pdq(...)
    await publisher.publish_inference_ready(...)
```

## 21. Acceptance tests

- 1,000 concurrent exact copies must produce one exact-hash owner.
- Replaying one `image.received` event 100 times must converge to one logical result.
- Metadata-only changes should be caught by canonical-pixel identity when pixel samples truly match.
- PDQ should retrieve transformed Copydays candidates at calibrated recall.
- PDQ-only candidate matches must never automatically suppress inference.
- Repetitive-texture ORB matches should be rejected by weak RANSAC geometry.
- A tiny matching patch with low image coverage must not declare full-image duplication.
- If vector-service is unavailable, the system must not invent a fuzzy duplicate verdict.

## References

- Meta PDQ reference: https://github.com/facebook/ThreatExchange/blob/main/pdq/README.md
- Python PDQ binding example: https://github.com/faustomorales/pdqhash-python
- OpenCV ORB: https://docs.opencv.org/4.x/d1/d89/tutorial_py_orb.html
- Milvus binary/Hamming vectors: https://milvus.io/docs/binary-vector.md
