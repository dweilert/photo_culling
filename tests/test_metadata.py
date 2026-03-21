from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from photo_culling.metadata import (
    build_provenance_args,
    build_tag_args,
    copy_metadata_from_raw_to_jpeg,
)


def _make_file(path: Path, content: bytes = b"test") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _config() -> dict:
    return {
        "tools": {
            "exiftool_path": "exiftool",
        },
        "metadata": {
            "copy_tags": {
                "exif": ["DateTimeOriginal", "Make", "Model"],
                "gps": ["GPSLatitude", "GPSLongitude"],
                "iptc": ["ObjectName"],
                "xmp": ["dc:Title", "xmp:Label"],
            },
            "validate_tags": [
                "EXIF:DateTimeOriginal",
                "EXIF:Make",
                "XMP-xmp:Label",
            ],
            "provenance": {
                "software_tag": "EXIF:Software",
                "software_value": "Photo Cull Pipeline",
                "generated_label_tag": "XMP-xmp:Label",
                "generated_label_value": "GeneratedFromRAW",
                "append_description_tag": "XMP-dc:Description",
                "append_description_value": "Generated from RAW by Photo Cull Pipeline",
                "pipeline_version_tag": "XMP-xmp:CreatorTool",
            },
        },
    }


def test_build_tag_args() -> None:
    args = build_tag_args(_config())

    assert "-EXIF:DateTimeOriginal" in args
    assert "-EXIF:GPSLatitude" in args
    assert "-IPTC:ObjectName" in args
    assert "-XMP-dc:Title" in args
    assert "-XMP-xmp:Label" in args


def test_build_provenance_args() -> None:
    args = build_provenance_args(_config(), pipeline_version="0.1.0")

    assert "-EXIF:Software=Photo Cull Pipeline" in args
    assert "-XMP-xmp:Label=GeneratedFromRAW" in args
    assert "-XMP-dc:Description+=Generated from RAW by Photo Cull Pipeline" in args
    assert "-XMP-xmp:CreatorTool=0.1.0" in args


def test_copy_metadata_missing_raw(tmp_path: Path) -> None:
    jpg = tmp_path / "test.jpg"
    _make_file(jpg)

    result = copy_metadata_from_raw_to_jpeg(
        raw_path=tmp_path / "missing.ARW",
        jpeg_path=jpg,
        config=_config(),
    )

    assert result.success is False
    assert result.error is not None
    assert "RAW file not found" in result.error


def test_copy_metadata_missing_jpeg(tmp_path: Path) -> None:
    raw = tmp_path / "test.ARW"
    _make_file(raw)

    result = copy_metadata_from_raw_to_jpeg(
        raw_path=raw,
        jpeg_path=tmp_path / "missing.jpg",
        config=_config(),
    )

    assert result.success is False
    assert result.error is not None
    assert "JPEG file not found" in result.error


def test_copy_metadata_success_with_mocked_exiftool(tmp_path: Path, monkeypatch) -> None:
    raw = tmp_path / "test.ARW"
    jpg = tmp_path / "test.jpg"
    _make_file(raw)
    _make_file(jpg)

    call_counter = {"count": 0}

    def fake_run(cmd, capture_output, text, check):
        call_counter["count"] += 1

        # first call = copy, second call = validation
        if call_counter["count"] == 1:
            return SimpleNamespace(returncode=0, stdout="copied", stderr="")
        return SimpleNamespace(
            returncode=0,
            stdout="2024:01:01 10:00:00\nCanon\nGeneratedFromRAW\n",
            stderr="",
        )

    monkeypatch.setattr("photo_culling.metadata.subprocess.run", fake_run)

    result = copy_metadata_from_raw_to_jpeg(
        raw_path=raw,
        jpeg_path=jpg,
        config=_config(),
        pipeline_version="0.1.0",
    )

    assert result.success is True
    assert result.error is None
    assert result.exit_code == 0
    assert result.warnings == []


def test_copy_metadata_validation_warnings_on_missing_tags(
    tmp_path: Path, monkeypatch
) -> None:
    raw = tmp_path / "test.ARW"
    jpg = tmp_path / "test.jpg"
    _make_file(raw)
    _make_file(jpg)

    call_counter = {"count": 0}

    def fake_run(cmd, capture_output, text, check):
        call_counter["count"] += 1
        if call_counter["count"] == 1:
            # copy succeeds
            return SimpleNamespace(returncode=0, stdout="copied", stderr="")
        # validation returns fewer lines than expected tags → missing tag warnings
        return SimpleNamespace(returncode=0, stdout="2024:01:01 10:00:00\n", stderr="")

    monkeypatch.setattr("photo_culling.metadata.subprocess.run", fake_run)

    result = copy_metadata_from_raw_to_jpeg(
        raw_path=raw,
        jpeg_path=jpg,
        config=_config(),
        pipeline_version="0.1.0",
    )

    # Still succeeds, but warnings are populated
    assert result.success is True
    assert len(result.warnings) > 0
    assert any("Missing" in w for w in result.warnings)


def test_copy_metadata_validation_failure_recorded_as_warning(
    tmp_path: Path, monkeypatch
) -> None:
    raw = tmp_path / "test.ARW"
    jpg = tmp_path / "test.jpg"
    _make_file(raw)
    _make_file(jpg)

    call_counter = {"count": 0}

    def fake_run(cmd, capture_output, text, check):
        call_counter["count"] += 1
        if call_counter["count"] == 1:
            return SimpleNamespace(returncode=0, stdout="copied", stderr="")
        # validation call fails
        return SimpleNamespace(returncode=1, stdout="", stderr="exiftool read error")

    monkeypatch.setattr("photo_culling.metadata.subprocess.run", fake_run)

    result = copy_metadata_from_raw_to_jpeg(
        raw_path=raw,
        jpeg_path=jpg,
        config=_config(),
    )

    assert result.success is True
    assert len(result.warnings) > 0
    assert any("Validation failed" in w for w in result.warnings)


def test_copy_metadata_exiftool_failure(tmp_path: Path, monkeypatch) -> None:
    raw = tmp_path / "test.ARW"
    jpg = tmp_path / "test.jpg"
    _make_file(raw)
    _make_file(jpg)

    def fake_run(cmd, capture_output, text, check):
        return SimpleNamespace(returncode=1, stdout="", stderr="exiftool failed")

    monkeypatch.setattr("photo_culling.metadata.subprocess.run", fake_run)

    result = copy_metadata_from_raw_to_jpeg(
        raw_path=raw,
        jpeg_path=jpg,
        config=_config(),
    )

    assert result.success is False
    assert result.error is not None
    assert "ExifTool failed" in result.error
