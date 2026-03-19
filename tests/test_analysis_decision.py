from pathlib import Path

from photo_culling.pairing import (
    PairedImage,
    decide_analysis_image,
)


def _pair(status, raw=None, jpeg=None):
    return PairedImage(
        group_key="x",
        rel_dir=Path("set1"),
        base_name="IMG_0001",
        status=status,
        raw_path=Path(raw) if raw else None,
        jpeg_path=Path(jpeg) if jpeg else None,
        raw_count=1 if raw else 0,
        jpeg_count=1 if jpeg else 0,
        notes=(),
    )


def test_analysis_prefers_companion_jpeg():
    pair = _pair(
        "paired",
        raw="/a/IMG_0001.ARW",
        jpeg="/a/IMG_0001.JPG",
    )

    decision = decide_analysis_image(
        pair,
        source_root=Path("/src"),
        derivative_root=Path("/deriv"),
    )

    assert decision.analysis_path == Path("/a/IMG_0001.JPG")
    assert decision.needs_render is False
    assert decision.analysis_source == "companion_jpeg"


def test_analysis_raw_only_requires_render():
    pair = _pair(
        "raw_only",
        raw="/a/IMG_0001.ARW",
    )

    decision = decide_analysis_image(
        pair,
        source_root=Path("/src"),
        derivative_root=Path("/deriv"),
    )

    assert decision.needs_render is True
    assert decision.analysis_path == Path("/deriv/set1/IMG_0001.jpg")


def test_analysis_standalone_jpeg():
    pair = _pair(
        "jpeg_only",
        jpeg="/a/IMG_0001.JPG",
    )

    decision = decide_analysis_image(
        pair,
        source_root=Path("/src"),
        derivative_root=Path("/deriv"),
    )

    assert decision.analysis_path == Path("/a/IMG_0001.JPG")
    assert decision.needs_render is False
    assert decision.analysis_source == "standalone_jpeg"


def test_analysis_ambiguous():
    pair = _pair("ambiguous")

    decision = decide_analysis_image(
        pair,
        source_root=Path("/src"),
        derivative_root=Path("/deriv"),
    )

    assert decision.analysis_path is None
    assert decision.error is not None
