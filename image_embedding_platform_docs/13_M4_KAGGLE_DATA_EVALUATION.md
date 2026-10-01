# M4 Kaggle data evaluation

## Provenance and scope

The owner supplied [Intrusion Classification Image Dataset](https://www.kaggle.com/datasets/fatehmujtaba/image-dataset)
as an engineering evaluation corpus. We downloaded Kaggle dataset version 1
to ignored local `.artifacts/m4/kaggle`. The 1.94 GiB ZIP has SHA-256
`a6e6ebad66992938f7e38ec227f276cd6222fb45c6d8dc9fd29b92bd80219934`
and passed `unzip -tqq`. Its 5,582 files comprise 5,504 JPEGs and 78 BMPs
across nine class folders. Kaggle lists its license as unknown; the owner
confirmed this is their upload, but the image bytes should not be committed
or redistributed from this repository.

The archive has 105 exact-byte duplicate pairs. Sixty-eight pairs appear in
both `animal` and `non_intrusion`; other pairs occur within or across other
class folders. Class folders therefore cannot be used as source identities or
as independent split boundaries. The animal folder contains 1,001 frames
named for the same `animal_evening_time` recording plus 21 other files. One
recording cannot supply both calibration and a source-disjoint final animal
set.

## Controlled positive corpus

`infrastructure.kaggle_manifest` accepts only frames with an explicit
`frame_<number>_vid_<video>` name and the `animal_evening_time_frame_<number>`
files in the animal folder. It groups all classes sharing a video ID, holds
back complete recordings before any policy scoring, and rejects selected
exact-byte overlap between recordings. Ambiguous names, existing
`_augmented_` files, and BMPs are not used as known-copy ground truth.

Three deterministic source frames per recording yield 48 originals from 16
recordings. Eleven recordings / 33 originals are calibration data; five
recordings / 15 originals are reserved for final evaluation. For every source,
the tool writes JPEG quality 75, 70% resize, and 10% border-crop copies under
ignored local storage. Manifest rows include recording ID, source ID,
relationship, transform parameters, path, file size, and SHA-256.

Run after downloading and extracting the Kaggle ZIP:

```sh
uv run python -m infrastructure.kaggle_manifest \
  --dataset-dir .artifacts/m4/kaggle/extracted/dataset \
  --generated-dir .artifacts/m4/kaggle/generated \
  --output .artifacts/m4/kaggle/manifest.jsonl
uv run python -m infrastructure.copydays_evaluation \
  --manifest .artifacts/m4/kaggle/manifest.jsonl \
  --output .artifacts/m4/kaggle/calibration_report.json \
  --split calibration
```

The held-out split is present in the manifest but was **not** scored in this
pass. The evaluator still checks file identity and split consistency for all
rows. Its `suppression_approved` field remains false.

## Shadow policy v2 calibration result

| Controlled transform | Known copies | Candidate hits | Verified copies |
|---|---:|---:|---:|
| JPEG quality 75 | 33 | 33 | 33 |
| Resize to 70% | 33 | 33 | 24 |
| 10% border crop | 33 | 0 | 0 |
| **Overall** | **99** | **66** | **57** |

Seven other-source candidates reached verification and none was accepted.
Precision and false-positive count are deliberately `null` in this report:
the dataset does not label visually close, distinct frames, and the
calibration subset is small. Crop
candidate recall is zero at the frozen PDQ radius; relaxing it without
negative evidence would increase risk. Fuzzy suppression stays disabled.

## Animal hard-negative review

`infrastructure.kaggle_review` ranks non-byte-identical animal frames by PDQ,
checks the top disjoint pairs through the shadow verifier, and writes an
**unlabeled** JSON review queue plus a contact sheet:

```sh
uv run python -m infrastructure.kaggle_review \
  --animal-dir .artifacts/m4/kaggle/extracted/dataset/animal \
  --output-json .artifacts/m4/kaggle/animal_review.json \
  --output-image .artifacts/m4/kaggle/animal_review.jpg
```

For the first 12 distinct-byte pairs, nine were shadow-accepted. This is a
triage result, **not** nine false positives: whether different video frames
are the same observation must be labeled by the dataset owner. Review each
pair as `SAME_OBSERVATION`, `DISTINCT_OBSERVATION`, or `UNSURE`, based on the
product's actual duplicate policy. Do not derive a false-positive gate from
unreviewed pairs. The review generator refuses to overwrite a queue once any
label has been changed from `UNREVIEWED`.

## Different timestamps are distinct observations

The dataset owner approved a conservative rule: frames from different
captures in the `animal_evening_time` recording are separate observations,
even if their pixels look almost identical. The filename's frame number is
used as a capture-order proxy; the archive provides no separate timestamp
manifest. A transformed copy of one captured frame remains a duplicate. This
rule labels all 12 reviewed different-frame pairs `DISTINCT_OBSERVATION`;
their image checksums are verified before the labels are written. The original
unlabeled queue is preserved.

Run the label and shadow-policy evaluation with new output paths:

```sh
uv run python -m infrastructure.kaggle_label_evaluation \
  --animal-dir .artifacts/m4/kaggle/extracted/dataset/animal \
  --review-json .artifacts/m4/kaggle/animal_review.json \
  --labels-output .artifacts/m4/kaggle/animal_review_labeled_frame_order.json \
  --report-output .artifacts/m4/kaggle/animal_distinct_report_frame_order.json \
  --sample-size 48
```

The 12 reviewed pairs include nine that the geometric verifier accepts.
For a separate deterministic sample of 48 incoming frames, the tool searches
the 1,001 frames in this recording, allowing only earlier timestamps into
each query's top-five PDQ candidate pool. It verified 172 candidate pairs;
101 pairs were accepted, affecting 38 of 48 sampled queries. Under the agreed
label rule, these are **would-be false-positive near-copy decisions** if the
shadow path were activated. The sample is from one deliberately difficult
recording, not a population-wide false-positive-rate estimate. It is enough
to fail the safety gate. The held-out recording split remains unscored, and
fuzzy suppression remains disabled.

The same observation rule also exposes an architecture question outside this
shadow path: byte and canonical-pixel identity are currently scoped by tenant
dedup scope, not by capture identity. Two separately captured observations
with identical bytes or pixels could be coalesced before near-copy search.
Before calling the whole pipeline observation-preserving, the upload contract
needs a trustworthy capture/observation identity and the exact/pixel
deduplication policy must be reviewed against it.

## Remaining gate

1. Review exact/pixel deduplication against capture identity under the agreed
   distinct-timestamp rule.
2. Add a separately sourced, representative hard-negative corpus; this single
   animal recording cannot provide source-disjoint animal calibration and final
   evidence.
3. Investigate candidate retrieval for crops and safe rejection of distinct
   frames, then obtain and evaluate Copydays crop attacks. Freeze a policy on
   calibration data and score the untouched final recordings once.
4. Activate derived decisions only if the explicit false-positive and recall
   gates pass. The current policy fails both the distinct-frame and crop gates.
