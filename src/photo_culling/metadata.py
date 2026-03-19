from __future__ import annotations

import subprocess

# ============================================================
# Imports
# ============================================================
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# ============================================================
# Result Dataclass
# ============================================================


@dataclass(frozen=True)
class MetadataCopyResult:
    success: bool
    raw_path: Path
    jpeg_path: Path
    command: list[str]
    exit_code: int | None
    stdout: str
    stderr: str
    warnings: list[str]
    error: str | None


# ============================================================
# Config Helpers
# ============================================================


def _get_nested(config: dict[str, Any], *keys: str, default=None):
    cur: Any = config
    for key in keys:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def get_exiftool_path(config: dict[str, Any]) -> str:
    return str(_get_nested(config, "tools", "exiftool_path", default="exiftool"))


def get_validate_tags(config: dict[str, Any]) -> list[str]:
    tags = _get_nested(config, "metadata", "validate_tags", default=[])
    return list(tags)


# ============================================================
# Argument Builders
# ============================================================


def build_tag_args(config: dict[str, Any]) -> list[str]:
    args: list[str] = []

    exif_tags = _get_nested(config, "metadata", "copy_tags", "exif", default=[])
    gps_tags = _get_nested(config, "metadata", "copy_tags", "gps", default=[])
    iptc_tags = _get_nested(config, "metadata", "copy_tags", "iptc", default=[])
    xmp_tags = _get_nested(config, "metadata", "copy_tags", "xmp", default=[])

    for tag in exif_tags:
        args.append(f"-EXIF:{tag}")

    for tag in gps_tags:
        args.append(f"-EXIF:{tag}")

    for tag in iptc_tags:
        args.append(f"-IPTC:{tag}")

    for tag in xmp_tags:
        args.append(f"-XMP-{tag}")

    return args


def build_provenance_args(
    config: dict[str, Any],
    pipeline_version: str | None,
) -> list[str]:
    provenance = _get_nested(config, "metadata", "provenance", default={}) or {}

    software_tag = provenance.get("software_tag")
    software_value = provenance.get("software_value")
    generated_label_tag = provenance.get("generated_label_tag")
    generated_label_value = provenance.get("generated_label_value")
    append_description_tag = provenance.get("append_description_tag")
    append_description_value = provenance.get("append_description_value")
    pipeline_version_tag = provenance.get("pipeline_version_tag")

    args: list[str] = []

    if software_tag and software_value:
        args.append(f"-{software_tag}={software_value}")

    if generated_label_tag and generated_label_value:
        args.append(f"-{generated_label_tag}={generated_label_value}")

    if append_description_tag and append_description_value:
        args.append(f"-{append_description_tag}+={append_description_value}")

    if pipeline_version and pipeline_version_tag:
        args.append(f"-{pipeline_version_tag}={pipeline_version}")

    return args


# ============================================================
# ExifTool Execution
# ============================================================


def run_exiftool(cmd: list[str]) -> tuple[int, str, str]:
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.returncode, proc.stdout, proc.stderr


def validate_metadata(config: dict[str, Any], jpeg_path: Path) -> list[str]:
    warnings: list[str] = []

    validate_tags = get_validate_tags(config)
    if not validate_tags:
        return warnings

    cmd = [
        get_exiftool_path(config),
        "-s",
        "-s",
        "-s",
    ]

    for tag in validate_tags:
        cmd.append(f"-{tag}")

    cmd.append(str(jpeg_path))

    rc, out, err = run_exiftool(cmd)

    if rc != 0:
        warnings.append(f"Validation failed to read tags: {err.strip() or err}")
        return warnings

    lines = out.strip().splitlines()

    for tag, value in zip(validate_tags, lines, strict=False):
        if not value.strip():
            warnings.append(f"Missing or empty tag: {tag}")

    if len(lines) < len(validate_tags):
        for tag in validate_tags[len(lines) :]:
            warnings.append(f"Missing or empty tag: {tag}")

    return warnings


# ============================================================
# Main Function
# ============================================================


def copy_metadata_from_raw_to_jpeg(
    raw_path: Path | str,
    jpeg_path: Path | str,
    config: dict[str, Any],
    pipeline_version: str | None = None,
) -> MetadataCopyResult:
    raw = Path(raw_path)
    jpg = Path(jpeg_path)

    if not raw.exists():
        return MetadataCopyResult(
            success=False,
            raw_path=raw,
            jpeg_path=jpg,
            command=[],
            exit_code=None,
            stdout="",
            stderr="",
            warnings=[],
            error=f"RAW file not found: {raw}",
        )

    if not jpg.exists():
        return MetadataCopyResult(
            success=False,
            raw_path=raw,
            jpeg_path=jpg,
            command=[],
            exit_code=None,
            stdout="",
            stderr="",
            warnings=[],
            error=f"JPEG file not found: {jpg}",
        )

    cmd = [
        get_exiftool_path(config),
        "-overwrite_original",
        "-tagsFromFile",
        str(raw),
        *build_tag_args(config),
        *build_provenance_args(config, pipeline_version),
        str(jpg),
    ]

    rc, out, err = run_exiftool(cmd)

    if rc != 0:
        return MetadataCopyResult(
            success=False,
            raw_path=raw,
            jpeg_path=jpg,
            command=cmd,
            exit_code=rc,
            stdout=out,
            stderr=err,
            warnings=[],
            error=f"ExifTool failed: {err.strip() or err}",
        )

    warnings = validate_metadata(config, jpg)

    return MetadataCopyResult(
        success=True,
        raw_path=raw,
        jpeg_path=jpg,
        command=cmd,
        exit_code=rc,
        stdout=out,
        stderr=err,
        warnings=warnings,
        error=None,
    )
