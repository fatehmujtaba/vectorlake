from __future__ import annotations

import hashlib
import warnings
from dataclasses import dataclass
from tempfile import SpooledTemporaryFile
from typing import BinaryIO

from PIL import Image, ImageOps, UnidentifiedImageError

MAX_OBJECT_BYTES = 20_000_000
MAX_DIMENSION = 8192
MAX_PIXELS = 16_000_000
READ_CHUNK_BYTES = 64 * 1024
CANONICALIZATION_VERSION = "pixels-v1"

IMAGE_FORMATS = {"JPEG": "image/jpeg", "PNG": "image/png", "WEBP": "image/webp"}
SAMPLE_MODES = {"1", "L", "LA", "RGB", "RGBA", "I", "I;16", "I;16B", "I;16L"}


class ValidationFailure(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class StreamedImage:
    byte_sha256: str
    size_bytes: int
    media_type: str


@dataclass(frozen=True)
class CanonicalImage:
    pixel_sha256: str
    width: int
    height: int
    sample_mode: str


def _media_type(header: bytes) -> str:
    if header.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if header.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if header.startswith(b"RIFF") and header[8:12] == b"WEBP":
        return "image/webp"
    raise ValidationFailure("unsupported_signature")


def stream_image(
    body: BinaryIO, destination: SpooledTemporaryFile[bytes], expected_size: int
) -> StreamedImage:
    if expected_size < 1 or expected_size > MAX_OBJECT_BYTES:
        raise ValidationFailure("object_size_out_of_bounds")
    digest = hashlib.sha256()
    size = 0
    header = b""
    media_type: str | None = None
    while chunk := body.read(READ_CHUNK_BYTES):
        size += len(chunk)
        if size > MAX_OBJECT_BYTES:
            raise ValidationFailure("object_size_out_of_bounds")
        if len(header) < 12:
            header += chunk[: 12 - len(header)]
        if media_type is None and len(header) >= 12:
            media_type = _media_type(header)
        digest.update(chunk)
        destination.write(chunk)
    if size != expected_size:
        raise ValidationFailure("object_size_mismatch")
    if media_type is None:
        media_type = _media_type(header)
    destination.seek(0)
    return StreamedImage(digest.hexdigest(), size, media_type)


def canonicalize_image(source: SpooledTemporaryFile[bytes], media_type: str) -> CanonicalImage:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            source.seek(0)
            with Image.open(source, formats=tuple(IMAGE_FORMATS)) as image:
                image_format = image.format
                if IMAGE_FORMATS.get(str(image_format)) != media_type:
                    raise ValidationFailure("format_mismatch")
                width, height = image.size
                if (
                    width < 1
                    or height < 1
                    or width > MAX_DIMENSION
                    or height > MAX_DIMENSION
                    or width * height > MAX_PIXELS
                ):
                    raise ValidationFailure("image_dimensions_out_of_bounds")
                if getattr(image, "n_frames", 1) != 1:
                    raise ValidationFailure("animated_image_unsupported")
                image.load()
                oriented = ImageOps.exif_transpose(image)
                sample_mode = oriented.mode
                if sample_mode == "P":
                    target_mode = "RGBA" if "transparency" in oriented.info else "RGB"
                    oriented = oriented.convert(target_mode)
                    sample_mode = f"P->{target_mode}"
                elif sample_mode not in SAMPLE_MODES:
                    raise ValidationFailure("sample_mode_unsupported")
                pixels = oriented.tobytes()
                hash_state = hashlib.sha256()
                hash_state.update(CANONICALIZATION_VERSION.encode() + b"\0")
                hash_state.update(oriented.width.to_bytes(4, "big"))
                hash_state.update(oriented.height.to_bytes(4, "big"))
                hash_state.update(sample_mode.encode() + b"\0")
                hash_state.update(pixels)
                return CanonicalImage(
                    hash_state.hexdigest(), oriented.width, oriented.height, sample_mode
                )
    except ValidationFailure:
        raise
    except (
        UnidentifiedImageError,
        Image.DecompressionBombError,
        Image.DecompressionBombWarning,
        OSError,
        SyntaxError,
        ValueError,
    ) as exc:
        raise ValidationFailure("invalid_image") from exc
