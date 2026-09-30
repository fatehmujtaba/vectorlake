# Canonical pixel identity v1

`pixels-v1` is a conservative identity hash for decoded single-frame JPEG,
PNG, and WebP images. It is not model preprocessing or perceptual similarity.
Two source files can share this identity only when their decoded sample mode,
dimensions, and oriented samples agree exactly under the rules below.

## Decoder and bounds

- The source is streamed and SHA-256 hashed before decode, with a 20,000,000
  byte ceiling. Magic bytes and declared content type must agree.
- Pillow 12.3.0 opens only `JPEG`, `PNG`, or `WEBP`. Truncated images remain
  disabled; decompression-bomb warnings are errors.
- Width and height must each be between 1 and 8192. Their product must not
  exceed 16,000,000. Animated/multi-frame images are rejected.
- The decoder loads the entire bounded image, then applies EXIF orientation
  with `ImageOps.exif_transpose` before hashing samples.

## Hash payload

SHA-256 is computed over these bytes, in order:

1. ASCII `pixels-v1` followed by a zero byte.
2. Oriented width as an unsigned 4-byte big-endian integer.
3. Oriented height in the same format.
4. UTF-8 sample-mode descriptor followed by a zero byte.
5. `Image.tobytes()` of the oriented image, in Pillow's row-major order.

Accepted native descriptors are `1`, `L`, `LA`, `RGB`, `RGBA`, `I`, `I;16`,
`I;16B`, and `I;16L`. Palette (`P`) images are rendered to RGB or RGBA
depending on transparency and use `P->RGB` or `P->RGBA` as the descriptor.
Unknown sample modes are rejected. The original byte SHA-256 remains separate.

This contract intentionally does not collapse grayscale into RGB, drop alpha,
convert 16-bit samples to 8-bit, apply ICC transforms, or normalize different
integer endian descriptors. These choices favor extra processing over a false
duplicate. Changing any of them requires `pixels-v2` and a separately keyed
registry; existing `pixels-v1` claims must not be reinterpreted.
