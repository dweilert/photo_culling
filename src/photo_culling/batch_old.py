from __future__ import annotations

# ============================================================
# Imports
# ============================================================
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from photo_culling.analysis_pipeline import AnalysisResult, process_pair
from photo_culling.db import finalize_run, get_connection, init_db, record_item_result, start_run
from photo_culling.pairing import PairedImage, PairStatus

# ============================================================
# Status Constants
# ============================================================

STATUS_STARTED = "started"
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"
STATUS_SKIPPED = "skipped"


# ============================================================
# Models
# ============================================================


@dataclass(frozen=True)
class ProgressEvent:
    current: int
    total: int
    pair_id: str
    status: str
    message: str | None = None


ProgressCallback = Callable[[ProgressEvent], None]


@dataclass(frozen=True)
class PairProcessResult:
    pair_id: str
    status: str

    group_key: str
    rel_dir: str
    base_name: str
    pair_status: str

    source_raw: str | None
    source_jpeg: str | None
    final_image: str | None

    used_existing: bool
    render_performed: bool
    metadata_written: bool

    error_message: str | None = None


@dataclass(frozen=True)
class BatchProcessSummary:
    total: int
    succeeded: int
    failed: int
    skipped: int
    results: list[PairProcessResult]


# ============================================================
# Helpers
# ============================================================


def make_pair_id(pair: PairedImage) -> str:
    return pair.group_key


def is_processable_status(status: PairStatus) -> bool:
    return status in ("paired", "raw_only", "jpeg_only")


def emit_progress(
    progress: ProgressCallback | None,
    *,
    current: int,
    total: int,
    pair_id: str,
    status: str,
    message: str | None = None,
) -> None:
    if progress is None:
        return

    progress(
        ProgressEvent(
            current=current,
            total=total,
            pair_id=pair_id,
            status=status,
            message=message,
        )
    )


def build_skipped_result(pair: PairedImage, reason: str) -> PairProcessResult:
    return PairProcessResult(
        pair_id=make_pair_id(pair),
        status=STATUS_SKIPPED,
        group_key=pair.group_key,
        rel_dir=pair.rel_dir.as_posix(),
        base_name=pair.base_name,
        pair_status=pair.status,
        source_raw=str(pair.raw_path) if pair.raw_path is not None else None,
        source_jpeg=str(pair.jpeg_path) if pair.jpeg_path is not None else None,
        final_image=None,
        used_existing=False,
        render_performed=False,
        metadata_written=False,
        error_message=reason,
    )


def build_result_from_analysis(
    pair: PairedImage,
    analysis_result: AnalysisResult,
) -> PairProcessResult:
    status = STATUS_SUCCESS if analysis_result.error is None else STATUS_FAILED

    return PairProcessResult(
        pair_id=make_pair_id(pair),
        status=status,
        group_key=pair.group_key,
        rel_dir=pair.rel_dir.as_posix(),
        base_name=pair.base_name,
        pair_status=pair.status,
        source_raw=str(pair.raw_path) if pair.raw_path is not None else None,
        source_jpeg=str(pair.jpeg_path) if pair.jpeg_path is not None else None,
        final_image=(
            str(analysis_result.final_image) if analysis_result.final_image is not None else None
        ),
        used_existing=analysis_result.used_existing,
        render_performed=analysis_result.render_result is not None,
        metadata_written=(
            analysis_result.metadata_result is not None and analysis_result.metadata_result.success
        ),
        error_message=analysis_result.error,
    )


def build_failed_result_from_exception(
    pair: PairedImage,
    exc: Exception,
) -> PairProcessResult:
    return PairProcessResult(
        pair_id=make_pair_id(pair),
        status=STATUS_FAILED,
        group_key=pair.group_key,
        rel_dir=pair.rel_dir.as_posix(),
        base_name=pair.base_name,
        pair_status=pair.status,
        source_raw=str(pair.raw_path) if pair.raw_path is not None else None,
        source_jpeg=str(pair.jpeg_path) if pair.jpeg_path is not None else None,
        final_image=None,
        used_existing=False,
        render_performed=False,
        metadata_written=False,
        error_message=str(exc),
    )


def write_result_to_db(
    *,
    conn,
    run_id: int,
    result: PairProcessResult,
) -> None:
    record_item_result(
        conn,
        run_id=run_id,
        pair_id=result.pair_id,
        group_key=result.group_key,
        rel_dir=result.rel_dir,
        base_name=result.base_name,
        pair_status=result.pair_status,
        source_raw=result.source_raw,
        source_jpeg=result.source_jpeg,
        final_image=result.final_image,
        used_existing=result.used_existing,
        render_performed=result.render_performed,
        metadata_written=result.metadata_written,
        item_status=result.status,
        error_message=result.error_message,
    )


# ============================================================
# Main Batch Processing Logic
# ============================================================


def process_all_pairs(
    pairs: Iterable[PairedImage],
    *,
    source_root: Path,
    derivative_root: Path,
    config: dict[str, Any],
    progress: ProgressCallback | None = None,
    db_path: Path | None = None,
) -> BatchProcessSummary:
    pair_list = list(pairs)
    total = len(pair_list)
    results: list[PairProcessResult] = []

    conn = None
    run_id: int | None = None

    if db_path is not None:
        conn = get_connection(db_path)
        init_db(conn)
        run_id = start_run(
            conn,
            source_root=source_root,
            derivative_root=derivative_root,
            total_pairs=total,
            config=config,
        )

    try:
        for current, pair in enumerate(pair_list, start=1):
            pair_id = make_pair_id(pair)

            if not is_processable_status(pair.status):
                result = build_skipped_result(pair, reason="pair status is ambiguous")
                results.append(result)

                if conn is not None and run_id is not None:
                    write_result_to_db(conn=conn, run_id=run_id, result=result)

                emit_progress(
                    progress,
                    current=current,
                    total=total,
                    pair_id=pair_id,
                    status=STATUS_SKIPPED,
                    message=result.error_message,
                )
                continue

            emit_progress(
                progress,
                current=current,
                total=total,
                pair_id=pair_id,
                status=STATUS_STARTED,
            )

            try:
                analysis_result = process_pair(
                    pair=pair,
                    source_root=source_root,
                    derivative_root=derivative_root,
                    config=config,
                )
                result = build_result_from_analysis(pair, analysis_result)
                results.append(result)

                if conn is not None and run_id is not None:
                    write_result_to_db(conn=conn, run_id=run_id, result=result)

                emit_progress(
                    progress,
                    current=current,
                    total=total,
                    pair_id=pair_id,
                    status=result.status,
                    message=result.error_message,
                )

            except Exception as exc:
                result = build_failed_result_from_exception(pair, exc)
                results.append(result)

                if conn is not None and run_id is not None:
                    write_result_to_db(conn=conn, run_id=run_id, result=result)

                emit_progress(
                    progress,
                    current=current,
                    total=total,
                    pair_id=pair_id,
                    status=STATUS_FAILED,
                    message=result.error_message,
                )

        succeeded = sum(1 for r in results if r.status == STATUS_SUCCESS)
        failed = sum(1 for r in results if r.status == STATUS_FAILED)
        skipped = sum(1 for r in results if r.status == STATUS_SKIPPED)

        if conn is not None and run_id is not None:
            finalize_run(conn, run_id=run_id, status="completed")

        return BatchProcessSummary(
            total=total,
            succeeded=succeeded,
            failed=failed,
            skipped=skipped,
            results=results,
        )

    except Exception:
        if conn is not None and run_id is not None:
            finalize_run(conn, run_id=run_id, status="failed")
        raise

    finally:
        if conn is not None:
            conn.close()
