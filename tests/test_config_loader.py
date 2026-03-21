from pathlib import Path

import pytest

from photo_culling.config_loader import PipelineConfig, load_pipeline_config


def test_load_config_smoke() -> None:
    config_path = Path("config/pipeline.yaml")
    config = load_pipeline_config(config_path)

    assert isinstance(config, PipelineConfig)


def test_load_config_has_expected_top_level_keys() -> None:
    config_path = Path("config/pipeline.yaml")
    config = load_pipeline_config(config_path)

    assert hasattr(config, "tools")
    assert hasattr(config, "files")
    assert hasattr(config, "metadata")
    assert hasattr(config, "derivatives")
    assert hasattr(config, "logging")


def test_load_config_missing_file_raises(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist.yaml"

    with pytest.raises(FileNotFoundError):
        load_pipeline_config(missing)


def test_load_config_not_a_mapping_raises(tmp_path: Path) -> None:
    bad_yaml = tmp_path / "bad.yaml"
    bad_yaml.write_text("- item1\n- item2\n")

    with pytest.raises(ValueError, match="top-level mapping"):
        load_pipeline_config(bad_yaml)


def test_load_config_missing_required_section_raises(tmp_path: Path) -> None:
    # Valid YAML dict but missing the 'tools' section
    incomplete = tmp_path / "incomplete.yaml"
    incomplete.write_text("paths:\n  source_root: /photos\n  derivative_root: /out\n")

    with pytest.raises(ValueError):
        load_pipeline_config(incomplete)


def test_load_config_extensions_are_lowercased() -> None:
    config = load_pipeline_config(Path("config/pipeline.yaml"))

    for ext in config.files.raw_extensions:
        assert ext == ext.lower(), f"Extension not lowercased: {ext}"

    for ext in config.files.jpeg_extensions:
        assert ext == ext.lower(), f"Extension not lowercased: {ext}"


def test_load_config_db_path_is_optional(tmp_path: Path) -> None:
    # Build a minimal valid config without db_path
    minimal = tmp_path / "minimal.yaml"
    minimal.write_text(
        """
tools:
  exiftool_path: exiftool
  imagemagick_path: magick
  darktable_cli: darktable-cli
paths:
  source_root: /photos
  derivative_root: /out
files:
  raw_extensions: [.arw]
  jpeg_extensions: [.jpg]
  generated_jpeg_extension: .jpg
metadata:
  copy_tags:
    exif: [DateTimeOriginal]
    gps: [GPSLatitude]
    iptc: [ObjectName]
    xmp: [dc:Title]
  validate_tags: [EXIF:DateTimeOriginal]
  provenance:
    software_tag: EXIF:Software
    software_value: Test
    generated_label_tag: XMP-xmp:Label
    generated_label_value: Generated
    append_description_tag: XMP-dc:Description
    append_description_value: From RAW
    pipeline_version_tag: XMP-xmp:CreatorTool
derivatives:
  preserve_relative_paths: true
  jpeg_quality: 90
  overwrite_existing: false
logging:
  level: INFO
  log_exiftool_stdout: false
  log_exiftool_stderr: false
"""
    )

    config = load_pipeline_config(minimal)
    assert config.paths.db_path is None
