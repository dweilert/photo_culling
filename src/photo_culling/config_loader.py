from __future__ import annotations

# ============================================================
# Imports
# ============================================================
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

# ============================================================
# Dataclasses
# ============================================================


@dataclass(frozen=True)
class ToolsConfig:
    exiftool_path: str
    imagemagick_path: str
    darktable_cli: str


@dataclass(frozen=True)
class PathsConfig:
    source_root: str
    derivative_root: str
    db_path: str | None = None


@dataclass(frozen=True)
class FilesConfig:
    raw_extensions: list[str]
    jpeg_extensions: list[str]
    generated_jpeg_extension: str


@dataclass(frozen=True)
class MetadataCopyTagsConfig:
    exif: list[str]
    gps: list[str]
    iptc: list[str]
    xmp: list[str]


@dataclass(frozen=True)
class MetadataProvenanceConfig:
    software_tag: str
    software_value: str
    generated_label_tag: str
    generated_label_value: str
    append_description_tag: str
    append_description_value: str
    pipeline_version_tag: str


@dataclass(frozen=True)
class MetadataConfig:
    copy_tags: MetadataCopyTagsConfig
    validate_tags: list[str]
    provenance: MetadataProvenanceConfig


@dataclass(frozen=True)
class DerivativesConfig:
    output_root: str
    preserve_relative_paths: bool
    jpeg_quality: int
    overwrite_existing: bool


@dataclass(frozen=True)
class LoggingConfig:
    level: str
    log_exiftool_stdout: bool
    log_exiftool_stderr: bool


@dataclass(frozen=True)
class PipelineConfig:
    tools: ToolsConfig
    paths: PathsConfig
    files: FilesConfig
    metadata: MetadataConfig
    derivatives: DerivativesConfig
    logging: LoggingConfig


# ============================================================
# Helpers
# ============================================================


def _read_yaml(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise ValueError(f"Config file must contain a top-level mapping: {config_path}")

    return data


def _require_dict(parent: dict[str, Any], key: str) -> dict[str, Any]:
    value = parent.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"Missing or invalid config section: {key}")
    return value


def _optional_dict(parent: dict[str, Any], key: str) -> dict[str, Any]:
    value = parent.get(key)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"Invalid config section: {key}")
    return value


def _require_list(parent: dict[str, Any], key: str) -> list[str]:
    value = parent.get(key)
    if not isinstance(value, list) or not all(isinstance(x, str) for x in value):
        raise ValueError(f"Missing or invalid list for key: {key}")
    return value


def _require_str(parent: dict[str, Any], key: str) -> str:
    value = parent.get(key)
    if not isinstance(value, str):
        raise ValueError(f"Missing or invalid string for key: {key}")
    return value


def _optional_str(parent: dict[str, Any], key: str) -> str | None:
    value = parent.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"Invalid string for key: {key}")
    return value


def _require_bool(parent: dict[str, Any], key: str) -> bool:
    value = parent.get(key)
    if not isinstance(value, bool):
        raise ValueError(f"Missing or invalid bool for key: {key}")
    return value


def _require_int(parent: dict[str, Any], key: str) -> int:
    value = parent.get(key)
    if not isinstance(value, int):
        raise ValueError(f"Missing or invalid int for key: {key}")
    return value


# ============================================================
# Main Loader
# ============================================================


def load_pipeline_config(config_path: str | Path) -> PipelineConfig:
    path = Path(config_path)
    data = _read_yaml(path)

    tools_raw = _require_dict(data, "tools")
    paths_raw = _require_dict(data, "paths")
    files_raw = _require_dict(data, "files")
    metadata_raw = _require_dict(data, "metadata")
    derivatives_raw = _optional_dict(data, "derivatives")
    logging_raw = _require_dict(data, "logging")

    copy_tags_raw = _require_dict(metadata_raw, "copy_tags")
    provenance_raw = _require_dict(metadata_raw, "provenance")

    tools = ToolsConfig(
        exiftool_path=_require_str(tools_raw, "exiftool_path"),
        imagemagick_path=_require_str(tools_raw, "imagemagick_path"),
        darktable_cli=_require_str(tools_raw, "darktable_cli"),
    )

    paths_cfg = PathsConfig(
        source_root=_require_str(paths_raw, "source_root"),
        derivative_root=_require_str(paths_raw, "derivative_root"),
        db_path=_optional_str(paths_raw, "db_path"),
    )

    files = FilesConfig(
        raw_extensions=[x.lower() for x in _require_list(files_raw, "raw_extensions")],
        jpeg_extensions=[x.lower() for x in _require_list(files_raw, "jpeg_extensions")],
        generated_jpeg_extension=_require_str(files_raw, "generated_jpeg_extension").lower(),
    )

    copy_tags = MetadataCopyTagsConfig(
        exif=_require_list(copy_tags_raw, "exif"),
        gps=_require_list(copy_tags_raw, "gps"),
        iptc=_require_list(copy_tags_raw, "iptc"),
        xmp=_require_list(copy_tags_raw, "xmp"),
    )

    provenance = MetadataProvenanceConfig(
        software_tag=_require_str(provenance_raw, "software_tag"),
        software_value=_require_str(provenance_raw, "software_value"),
        generated_label_tag=_require_str(provenance_raw, "generated_label_tag"),
        generated_label_value=_require_str(provenance_raw, "generated_label_value"),
        append_description_tag=_require_str(provenance_raw, "append_description_tag"),
        append_description_value=_require_str(provenance_raw, "append_description_value"),
        pipeline_version_tag=_require_str(provenance_raw, "pipeline_version_tag"),
    )

    metadata = MetadataConfig(
        copy_tags=copy_tags,
        validate_tags=_require_list(metadata_raw, "validate_tags"),
        provenance=provenance,
    )

    derivative_output_root = _optional_str(derivatives_raw, "output_root")
    if derivative_output_root is None:
        derivative_output_root = paths_cfg.derivative_root

    derivatives = DerivativesConfig(
        output_root=derivative_output_root,
        preserve_relative_paths=_require_bool(derivatives_raw, "preserve_relative_paths"),
        jpeg_quality=_require_int(derivatives_raw, "jpeg_quality"),
        overwrite_existing=_require_bool(derivatives_raw, "overwrite_existing"),
    )

    logging_cfg = LoggingConfig(
        level=_require_str(logging_raw, "level"),
        log_exiftool_stdout=_require_bool(logging_raw, "log_exiftool_stdout"),
        log_exiftool_stderr=_require_bool(logging_raw, "log_exiftool_stderr"),
    )

    return PipelineConfig(
        tools=tools,
        paths=paths_cfg,
        files=files,
        metadata=metadata,
        derivatives=derivatives,
        logging=logging_cfg,
    )


# ============================================================
# CLI Smoke Test
# ============================================================

if __name__ == "__main__":
    cfg = load_pipeline_config("../config/pipeline.yaml")
    print("Loaded pipeline config successfully.")
    print(cfg)
