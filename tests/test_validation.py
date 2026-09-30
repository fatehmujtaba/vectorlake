from __future__ import annotations

from io import BytesIO
from tempfile import SpooledTemporaryFile

import pytest
from PIL import Image, PngImagePlugin

from vectorlake.validation import (
    MAX_OBJECT_BYTES,
    ValidationFailure,
    canonicalize_image,
    stream_image,
)


def png_bytes(*, comment: str | None = None, mode: str = "RGB") -> bytes:
    output = BytesIO()
    image = Image.new(mode, (2, 2), 1)
    info = PngImagePlugin.PngInfo()
    if comment:
        info.add_text("Comment", comment)
    image.save(output, format="PNG", pnginfo=info)
    return output.getvalue()


def inspect(data: bytes) -> tuple[str, str]:
    with SpooledTemporaryFile(max_size=1024, mode="w+b") as temporary:
        streamed = stream_image(BytesIO(data), temporary, len(data))
        canonical = canonicalize_image(temporary, streamed.media_type)
    return streamed.byte_sha256, canonical.pixel_sha256


def test_metadata_only_change_keeps_pixel_identity() -> None:
    first = inspect(png_bytes(comment="first"))
    second = inspect(png_bytes(comment="second"))
    assert first[0] != second[0]
    assert first[1] == second[1]


def test_channel_and_bit_depth_remain_distinct() -> None:
    rgb = inspect(png_bytes(mode="RGB"))[1]
    gray8 = inspect(png_bytes(mode="L"))[1]
    output = BytesIO()
    Image.frombytes("I;16", (2, 2), b"\x01\x00" * 4).save(output, format="PNG")
    gray16 = inspect(output.getvalue())[1]
    assert len({rgb, gray8, gray16}) == 3


@pytest.mark.parametrize(
    ("data", "size", "code"),
    [
        (b"not an image", 12, "unsupported_signature"),
        (b"\x89PNG\r\n\x1a\n", 9, "object_size_mismatch"),
        (b"\x89PNG\r\n\x1a\n", MAX_OBJECT_BYTES + 1, "object_size_out_of_bounds"),
    ],
)
def test_stream_rejects_bad_input(data: bytes, size: int, code: str) -> None:
    with SpooledTemporaryFile(max_size=1024, mode="w+b") as temporary:
        with pytest.raises(ValidationFailure, match=code):
            stream_image(BytesIO(data), temporary, size)


def test_decoder_rejects_truncated_png() -> None:
    data = png_bytes()[:33]
    with SpooledTemporaryFile(max_size=1024, mode="w+b") as temporary:
        streamed = stream_image(BytesIO(data), temporary, len(data))
        with pytest.raises(ValidationFailure, match="invalid_image"):
            canonicalize_image(temporary, streamed.media_type)


def test_short_network_reads_do_not_break_signature_check() -> None:
    class ShortReads(BytesIO):
        def read(self, size: int | None = -1) -> bytes:
            return super().read(2 if size is None or size < 0 else min(size, 2))

    data = png_bytes()
    with SpooledTemporaryFile(max_size=1024, mode="w+b") as temporary:
        streamed = stream_image(ShortReads(data), temporary, len(data))
        assert streamed.media_type == "image/png"
        assert canonicalize_image(temporary, streamed.media_type).width == 2


def test_dimension_limit_applies_before_decoding_pixels() -> None:
    output = BytesIO()
    Image.new("L", (8193, 1)).save(output, format="PNG")
    data = output.getvalue()
    with SpooledTemporaryFile(max_size=1024, mode="w+b") as temporary:
        streamed = stream_image(BytesIO(data), temporary, len(data))
        with pytest.raises(ValidationFailure, match="image_dimensions_out_of_bounds"):
            canonicalize_image(temporary, streamed.media_type)
