from __future__ import annotations

# ============================================================
# Imports
# ============================================================
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

# ============================================================
# Type Aliases
# ============================================================

AssetKind = Literal["raw", "jpeg", "other"]
PairStatus = Literal["paired", "raw_only", "jpeg_only", "ambiguous"]


# ============================================================
# Data Models
# ============================================================


@dataclass(frozen=True)
class SourceAsset:
    path: Path
    rel_path: Path
    rel_dir: Path
    stem: str
    stem_normalized: str
    extension_lower: str
    kind: AssetKind


@dataclass(frozen=True)
class PairedImage:
    group_key: str
    rel_dir: Path
    base_name: str
    status: PairStatus
    raw_path: Path | None
    jpeg_path: Path | None
    raw_count: int
    jpeg_count: int
    notes: tuple[str, ...] = ()


AnalysisSource = Literal[
    "companion_jpeg",
    "standalone_jpeg",
    "rendered_jpeg",
    "invalid",
]


@dataclass(frozen=True)
class AnalysisDecision:
    pair: PairedImage
    analysis_path: Path | None
    analysis_source: AnalysisSource
    needs_render: bool
    derivative_path: Path | None
    error: str | None = None


# ============================================================
# Config Helpers
# ============================================================


def _get_nested(config: dict, *keys: str, default=None):
    cur = config
    for key in keys:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def _normalize_extension(ext: str) -> str:
    ext = ext.strip().lower()
    if not ext:
        return ext
    if not ext.startswith("."):
        ext = f".{ext}"
    return ext


def get_raw_extensions(config: dict) -> set[str]:
    raw_exts = _get_nested(config, "files", "raw_extensions", default=[])
    return {_normalize_extension(ext) for ext in raw_exts}


def get_jpeg_extensions(config: dict) -> set[str]:
    jpeg_exts = _get_nested(config, "files", "jpeg_extensions", default=[".jpg", ".jpeg"])
    return {_normalize_extension(ext) for ext in jpeg_exts}


# ============================================================
# Classification Helpers
# ============================================================


def normalize_stem(stem: str) -> str:
    """
    Pairing normalization policy:
    - case-insensitive only
    - do not strip suffixes like -edit, _copy, (1), etc.
    """
    return stem.casefold()


def classify_extension(
    extension_lower: str,
    raw_extensions: set[str],
    jpeg_extensions: set[str],
) -> AssetKind:
    if extension_lower in raw_extensions:
        return "raw"
    if extension_lower in jpeg_extensions:
        return "jpeg"
    return "other"


# ============================================================
# Discovery
# ============================================================


def discover_assets(source_root: Path, config: dict) -> list[SourceAsset]:
    source_root = source_root.resolve()

    if not source_root.exists():
        raise FileNotFoundError(f"Source root does not exist: {source_root}")

    if not source_root.is_dir():
        raise NotADirectoryError(f"Source root is not a directory: {source_root}")

    raw_extensions = get_raw_extensions(config)
    jpeg_extensions = get_jpeg_extensions(config)

    assets: list[SourceAsset] = []

    for path in sorted(source_root.rglob("*")):
        if not path.is_file():
            continue

        rel_path = path.relative_to(source_root)
        rel_dir = rel_path.parent
        extension_lower = path.suffix.lower()
        stem = path.stem
        stem_normalized = normalize_stem(stem)
        kind = classify_extension(extension_lower, raw_extensions, jpeg_extensions)

        assets.append(
            SourceAsset(
                path=path,
                rel_path=rel_path,
                rel_dir=rel_dir,
                stem=stem,
                stem_normalized=stem_normalized,
                extension_lower=extension_lower,
                kind=kind,
            )
        )

    return assets


# ============================================================
# Pairing
# ============================================================


def build_derivative_path(
    pair: PairedImage,
    source_root: Path,
    derivative_root: Path,
) -> Path:
    """
    Build output JPEG path preserving relative directory structure.
    """
    return derivative_root / pair.rel_dir / f"{pair.base_name}.jpg"


def decide_analysis_image(
    pair: PairedImage,
    source_root: Path,
    derivative_root: Path,
) -> AnalysisDecision:
    """
    Decide which image should be used for analysis.
    Does NOT perform rendering.
    """

    # Case: valid RAW + JPEG pair
    if pair.status == "paired":
        return AnalysisDecision(
            pair=pair,
            analysis_path=pair.jpeg_path,
            analysis_source="companion_jpeg",
            needs_render=False,
            derivative_path=None,
        )

    # Case: RAW only → must render
    if pair.status == "raw_only":
        derivative_path = build_derivative_path(
            pair,
            source_root,
            derivative_root,
        )

        return AnalysisDecision(
            pair=pair,
            analysis_path=derivative_path,
            analysis_source="rendered_jpeg",
            needs_render=True,
            derivative_path=derivative_path,
        )

    # Case: JPEG only
    if pair.status == "jpeg_only":
        return AnalysisDecision(
            pair=pair,
            analysis_path=pair.jpeg_path,
            analysis_source="standalone_jpeg",
            needs_render=False,
            derivative_path=None,
        )

    # Case: ambiguous
    return AnalysisDecision(
        pair=pair,
        analysis_path=None,
        analysis_source="invalid",
        needs_render=False,
        derivative_path=None,
        error="ambiguous pairing",
    )


def build_group_key(rel_dir: Path, stem_normalized: str) -> str:
    dir_part = "." if str(rel_dir) in {"", "."} else rel_dir.as_posix()
    return f"{dir_part}::{stem_normalized}"


def group_assets_for_pairing(
    assets: list[SourceAsset],
) -> dict[tuple[Path, str], list[SourceAsset]]:
    groups: dict[tuple[Path, str], list[SourceAsset]] = {}

    for asset in assets:
        if asset.kind not in {"raw", "jpeg"}:
            continue

        key = (asset.rel_dir, asset.stem_normalized)
        groups.setdefault(key, []).append(asset)

    return groups


def pair_assets(assets: list[SourceAsset]) -> list[PairedImage]:
    groups = group_assets_for_pairing(assets)
    results: list[PairedImage] = []

    sorted_groups = sorted(
        groups.items(),
        key=lambda item: (item[0][0].as_posix(), item[0][1]),
    )

    for (rel_dir, stem_normalized), members in sorted_groups:
        raw_members = sorted(
            (member for member in members if member.kind == "raw"),
            key=lambda asset: asset.path.name.lower(),
        )
        jpeg_members = sorted(
            (member for member in members if member.kind == "jpeg"),
            key=lambda asset: asset.path.name.lower(),
        )

        raw_count = len(raw_members)
        jpeg_count = len(jpeg_members)
        group_key = build_group_key(rel_dir, stem_normalized)

        if raw_members:
            base_name = raw_members[0].stem
        elif jpeg_members:
            base_name = jpeg_members[0].stem
        else:
            base_name = stem_normalized

        notes: list[str] = []

        if raw_count == 1 and jpeg_count == 1:
            status: PairStatus = "paired"
            raw_path = raw_members[0].path
            jpeg_path = jpeg_members[0].path

        elif raw_count == 1 and jpeg_count == 0:
            status = "raw_only"
            raw_path = raw_members[0].path
            jpeg_path = None

        elif raw_count == 0 and jpeg_count == 1:
            status = "jpeg_only"
            raw_path = None
            jpeg_path = jpeg_members[0].path

        else:
            status = "ambiguous"
            raw_path = raw_members[0].path if raw_count == 1 else None
            jpeg_path = jpeg_members[0].path if jpeg_count == 1 else None

            if raw_count > 1:
                notes.append(f"multiple_raws:{raw_count}")
            if jpeg_count > 1:
                notes.append(f"multiple_jpegs:{jpeg_count}")

        results.append(
            PairedImage(
                group_key=group_key,
                rel_dir=rel_dir,
                base_name=base_name,
                status=status,
                raw_path=raw_path,
                jpeg_path=jpeg_path,
                raw_count=raw_count,
                jpeg_count=jpeg_count,
                notes=tuple(notes),
            )
        )

    return results
