from __future__ import annotations

import sqlite3
from pathlib import Path

from photo_culling.analysis_pipeline import AnalysisResult
from photo_culling.batch import process_all_pairs
from photo_culling.pairing import PairedImage


class DummyMetadataResult:
    def __init__(self, success: bool) -> None:
        self.success = success


def make_pair(
    *,
    group_key: str,
    rel_dir: str,
    base_name: str,
    status: str,
    raw_path: str | None,
    jpeg_path: str | None,
    raw_count: int = 0,
    jpeg_count: int = 0,
) -> PairedImage:
    return PairedImage(
        group_key=group_key,
        rel_dir=Path(rel_dir),
        base_name=base_name,
        status=status,
        raw_path=Path(raw_path) if raw_path is not None else None,
        jpeg_path=Path(jpeg_path) if jpeg_path is not None else None,
        raw_count=raw_count,
        jpeg_count=jpeg_count,
        notes=(),
    )


def test_process_all_pairs_writes_sqlite(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "photo_culling.db"
    source_root = tmp_path / "source"
    derivative_root = tmp_path / "derivatives"

    source_root.mkdir()
    derivative_root.mkdir()

    success_pair = make_pair(
        group_key="pair-success",
        rel_dir="album1",
        base_name="img001",
        status="paired",
        raw_path=str(source_root / "album1" / "img001.cr2"),
        jpeg_path=str(source_root / "album1" / "img001.jpg"),
        raw_count=1,
        jpeg_count=1,
    )

    skipped_pair = make_pair(
        group_key="pair-skipped",
        rel_dir="album1",
        base_name="img002",
        status="ambiguous",
        raw_path=str(source_root / "album1" / "img002.cr2"),
        jpeg_path=str(source_root / "album1" / "img002.jpg"),
        raw_count=2,
        jpeg_count=2,
    )

    failed_pair = make_pair(
        group_key="pair-failed",
        rel_dir="album2",
        base_name="img003",
        status="raw_only",
        raw_path=str(source_root / "album2" / "img003.cr2"),
        jpeg_path=None,
        raw_count=1,
        jpeg_count=0,
    )

    def fake_process_pair(*, pair, source_root, derivative_root, config):
        if pair.group_key == "pair-success":
            return AnalysisResult(
                pair=pair,
                final_image=derivative_root / "album1" / "img001.jpg",
                used_existing=True,
                render_result=None,
                metadata_result=DummyMetadataResult(success=True),
                error=None,
            )
        if pair.group_key == "pair-failed":
            return AnalysisResult(
                pair=pair,
                final_image=None,
                used_existing=False,
                render_result=None,
                metadata_result=None,
                error="simulated pipeline failure",
            )
        raise AssertionError(f"Unexpected pair: {pair.group_key}")

    monkeypatch.setattr("photo_culling.batch.process_pair", fake_process_pair)

    summary = process_all_pairs(
        [success_pair, skipped_pair, failed_pair],
        source_root=source_root,
        derivative_root=derivative_root,
        config={"copy_xmp": True},
        db_path=db_path,
    )

    assert summary.total == 3
    assert summary.succeeded == 1
    assert summary.failed == 1
    assert summary.skipped == 1
    assert len(summary.results) == 3

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    run_row = conn.execute("""
        SELECT status, total_pairs, processed_count, success_count, failed_count, skipped_count
        FROM runs
        """).fetchone()

    assert run_row is not None
    assert run_row["status"] == "completed"
    assert run_row["total_pairs"] == 3
    assert run_row["processed_count"] == 3
    assert run_row["success_count"] == 1
    assert run_row["failed_count"] == 1
    assert run_row["skipped_count"] == 1

    item_rows = conn.execute("""
        SELECT pair_id, item_status, pair_status, final_image, error_message
        FROM items
        ORDER BY pair_id
        """).fetchall()

    assert len(item_rows) == 3

    rows_by_pair_id = {row["pair_id"]: row for row in item_rows}

    assert rows_by_pair_id["pair-success"]["item_status"] == "success"
    assert rows_by_pair_id["pair-success"]["pair_status"] == "paired"
    assert rows_by_pair_id["pair-success"]["final_image"] is not None
    assert rows_by_pair_id["pair-success"]["error_message"] is None

    assert rows_by_pair_id["pair-skipped"]["item_status"] == "skipped"
    assert rows_by_pair_id["pair-skipped"]["pair_status"] == "ambiguous"
    assert rows_by_pair_id["pair-skipped"]["final_image"] is None
    assert rows_by_pair_id["pair-skipped"]["error_message"] == "pair status is ambiguous"

    assert rows_by_pair_id["pair-failed"]["item_status"] == "failed"
    assert rows_by_pair_id["pair-failed"]["pair_status"] == "raw_only"
    assert rows_by_pair_id["pair-failed"]["final_image"] is None
    assert rows_by_pair_id["pair-failed"]["error_message"] == "simulated pipeline failure"

    conn.close()
