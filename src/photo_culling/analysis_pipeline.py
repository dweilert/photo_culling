from __future__ import annotations

# ============================================================
# Imports
# ============================================================
from dataclasses import dataclass
from pathlib import Path

from photo_culling.metadata import MetadataCopyResult, copy_metadata_from_raw_to_jpeg
from photo_culling.pairing import PairedImage, decide_analysis_image
from photo_culling.raw_render import RenderResult, render_raw_to_jpeg

# ============================================================
# Result Model
# ============================================================


@dataclass(frozen=True)
class AnalysisResult:
    pair: PairedImage
    final_image: Path | None
    used_existing: bool
    render_result: RenderResult | None
    metadata_result: MetadataCopyResult | None
    error: str | None = None


# ============================================================
# Main Orchestration
# ============================================================


def process_pair(
    pair: PairedImage,
    source_root: Path,
    derivative_root: Path,
    config: dict,
) -> AnalysisResult:
    """
    Resolve final analysis image for a PairedImage.

    - uses JPEG if available
    - renders RAW if needed
    - returns structured result
    """

    decision = decide_analysis_image(
        pair=pair,
        source_root=source_root,
        derivative_root=derivative_root,
    )

    # Case: invalid / ambiguous
    if decision.error:
        return AnalysisResult(
            pair=pair,
            final_image=None,
            used_existing=False,
            render_result=None,
            metadata_result=None,
            error=decision.error,
        )

    # Case: use existing JPEG
    if not decision.needs_render:
        return AnalysisResult(
            pair=pair,
            final_image=decision.analysis_path,
            used_existing=True,
            render_result=None,
            metadata_result=None,
            error=None,
        )

    # Case: render required
    render_result = render_raw_to_jpeg(
        source_raw=pair.raw_path,
        output_jpeg=decision.derivative_path,
        config=config,
    )

    if not render_result.success:
        return AnalysisResult(
            pair=pair,
            final_image=None,
            used_existing=False,
            render_result=render_result,
            metadata_result=None,
            error=render_result.error,
        )

    # --- NEW: metadata copy step ---
    metadata_result = copy_metadata_from_raw_to_jpeg(
        raw_path=pair.raw_path,
        jpeg_path=render_result.output_jpeg,
        config=config,
        pipeline_version="0.1.0",
    )

    if not metadata_result.success:
        return AnalysisResult(
            pair=pair,
            final_image=None,
            used_existing=False,
            render_result=render_result,
            metadata_result=metadata_result,
            error=metadata_result.error,
        )

    # success
    return AnalysisResult(
        pair=pair,
        final_image=render_result.output_jpeg,
        used_existing=False,
        render_result=render_result,
        metadata_result=metadata_result,
        error=None,
    )
