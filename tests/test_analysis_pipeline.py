from pathlib import Path
from types import SimpleNamespace

from photo_culling.analysis_pipeline import process_pair
from photo_culling.pairing import PairedImage


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


def _config():
    return {
        "tools": {"darktable_cli": "darktable-cli"},
        "render": {"overwrite_existing": False},
    }


def test_pipeline_uses_existing_jpeg():
    pair = _pair(
        "paired",
        raw="/a/IMG_0001.ARW",
        jpeg="/a/IMG_0001.JPG",
    )

    result = process_pair(
        pair,
        source_root=Path("/src"),
        derivative_root=Path("/deriv"),
        config=_config(),
    )

    assert result.final_image == Path("/a/IMG_0001.JPG")
    assert result.used_existing is True
    assert result.render_result is None


def test_pipeline_renders_raw(tmp_path, monkeypatch):
    pair = _pair(
        "raw_only",
        raw=str(tmp_path / "IMG_0001.ARW"),
    )

    # create RAW file
    Path(pair.raw_path).write_bytes(b"raw")

    def fake_render(source_raw, output_jpeg, config, force=False):
        output_jpeg.parent.mkdir(parents=True, exist_ok=True)
        output_jpeg.write_bytes(b"jpeg")
        return SimpleNamespace(
            success=True,
            output_jpeg=output_jpeg,
            error=None,
        )

    def fake_metadata(raw_path, jpeg_path, config, pipeline_version=None):
        return SimpleNamespace(
            success=True,
            error=None,
        )

    monkeypatch.setattr(
        "photo_culling.analysis_pipeline.render_raw_to_jpeg",
        fake_render,
    )

    monkeypatch.setattr(
        "photo_culling.analysis_pipeline.copy_metadata_from_raw_to_jpeg",
        fake_metadata,
    )

    result = process_pair(
        pair,
        source_root=tmp_path,
        derivative_root=tmp_path / "deriv",
        config=_config(),
    )

    assert result.final_image is not None
    assert result.used_existing is False
    assert result.error is None


def test_pipeline_handles_render_failure(tmp_path, monkeypatch):
    pair = _pair(
        "raw_only",
        raw=str(tmp_path / "IMG_0002.ARW"),
    )

    Path(pair.raw_path).write_bytes(b"raw")

    def fake_render(source_raw, output_jpeg, config, force=False):
        return SimpleNamespace(
            success=False,
            output_jpeg=output_jpeg,
            error="render failed",
        )

    monkeypatch.setattr(
        "photo_culling.analysis_pipeline.render_raw_to_jpeg",
        fake_render,
    )

    result = process_pair(
        pair,
        source_root=tmp_path,
        derivative_root=tmp_path / "deriv",
        config=_config(),
    )

    assert result.final_image is None
    assert result.error == "render failed"


def test_pipeline_ambiguous_pair():
    pair = _pair("ambiguous")

    result = process_pair(
        pair,
        source_root=Path("/src"),
        derivative_root=Path("/deriv"),
        config=_config(),
    )

    assert result.final_image is None
    assert result.error is not None
