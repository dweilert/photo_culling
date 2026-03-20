from __future__ import annotations

import sqlite3
from pathlib import Path

from photo_culling.batch import process_all_pairs
from photo_culling.pairing import PairedImage


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


def test_process_all_pairs_records_exception_as_failed(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "photo_culling.db"
    source_root = tmp_path / "source"
    derivative_root = tmp_path / "derivatives"

    source_root.mkdir()
    derivative_root.mkdir()

    pair1 = make_pair(
        group_key="pair-1",
        rel_dir="album1",
        base_name="img001",
        status="paired",
        raw_path=str(source_root / "album1" / "img001.cr2"),
        jpeg_path=str(source_root / "album1" / "img001.jpg"),
        raw_count=1,
        jpeg_count=1,
    )

    def fake_process_pair(*, pair, source_root, derivative_root, config):
        if pair.group_key == "pair-1":
            raise RuntimeError("boom")
        raise AssertionError(
            "pair-2 should still be processed after exception handling path is proven"
        )

    monkeypatch.setattr("photo_culling.batch.process_pair", fake_process_pair)

    summary = process_all_pairs(
        [pair1],
        source_root=source_root,
        derivative_root=derivative_root,
        config={},
        db_path=db_path,
    )

    assert summary.total == 1
    assert summary.succeeded == 0
    assert summary.failed == 1
    assert summary.skipped == 0

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    row = conn.execute("SELECT pair_id, item_status, error_message FROM items").fetchone()

    assert row is not None
    assert row["pair_id"] == "pair-1"
    assert row["item_status"] == "failed"
    assert "boom" in row["error_message"]

    run_row = conn.execute("SELECT status, failed_count FROM runs").fetchone()

    assert run_row is not None
    assert run_row["status"] == "completed"
    assert run_row["failed_count"] == 1

    conn.close()
