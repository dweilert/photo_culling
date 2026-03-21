from __future__ import annotations

import sqlite3

import pytest

from photo_culling.db import (
    finalize_run,
    get_connection,
    get_failed_items,
    get_latest_run,
    get_run,
    get_run_items,
    get_skipped_items,
    init_db,
    record_item_result,
    start_run,
)


# ============================================================
# Helpers
# ============================================================


def _open_db(tmp_path):
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)
    return conn


def _start(conn, total_pairs=1):
    return start_run(
        conn,
        source_root=conn,  # dummy — value doesn't matter for edge case tests
        derivative_root=conn,
        total_pairs=total_pairs,
    )


def _start_run(conn, tmp_path, total_pairs=1):
    source_root = tmp_path / "source"
    derivative_root = tmp_path / "derivatives"
    return start_run(
        conn,
        source_root=source_root,
        derivative_root=derivative_root,
        total_pairs=total_pairs,
    )


def _record(conn, run_id, *, pair_id="p1", item_status="success", pair_status="paired"):
    record_item_result(
        conn,
        run_id=run_id,
        pair_id=pair_id,
        group_key=pair_id,
        rel_dir="album1",
        base_name="img001",
        pair_status=pair_status,
        source_raw="/photos/img001.cr2",
        source_jpeg="/photos/img001.jpg",
        final_image="/derivatives/img001.jpg" if item_status == "success" else None,
        used_existing=False,
        render_performed=False,
        metadata_written=False,
        item_status=item_status,
        error_message=None if item_status == "success" else "something went wrong",
    )


# ============================================================
# init_db
# ============================================================


def test_init_db_is_idempotent(tmp_path) -> None:
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)
    init_db(conn)  # should not raise
    conn.close()


# ============================================================
# get_latest_run on empty DB
# ============================================================


def test_get_latest_run_empty_db_returns_none(tmp_path) -> None:
    conn = _open_db(tmp_path)
    result = get_latest_run(conn)
    assert result is None
    conn.close()


# ============================================================
# get_run with nonexistent run_id
# ============================================================


def test_get_run_nonexistent_id_returns_none(tmp_path) -> None:
    conn = _open_db(tmp_path)
    result = get_run(conn, run_id=999)
    assert result is None
    conn.close()


# ============================================================
# get_run_items / get_failed_items / get_skipped_items — empty
# ============================================================


def test_get_run_items_empty_run_returns_empty_list(tmp_path) -> None:
    conn = _open_db(tmp_path)
    run_id = _start_run(conn, tmp_path)
    result = get_run_items(conn, run_id=run_id)
    assert result == []
    conn.close()


def test_get_failed_items_none_failed_returns_empty_list(tmp_path) -> None:
    conn = _open_db(tmp_path)
    run_id = _start_run(conn, tmp_path)
    _record(conn, run_id, pair_id="p1", item_status="success")
    result = get_failed_items(conn, run_id=run_id)
    assert result == []
    conn.close()


def test_get_skipped_items_none_skipped_returns_empty_list(tmp_path) -> None:
    conn = _open_db(tmp_path)
    run_id = _start_run(conn, tmp_path)
    _record(conn, run_id, pair_id="p1", item_status="success")
    result = get_skipped_items(conn, run_id=run_id)
    assert result == []
    conn.close()


# ============================================================
# finalize_run status variants
# ============================================================


def test_finalize_run_with_failed_status(tmp_path) -> None:
    conn = _open_db(tmp_path)
    run_id = _start_run(conn, tmp_path)
    finalize_run(conn, run_id=run_id, status="failed")
    run_row = get_run(conn, run_id=run_id)
    assert run_row["status"] == "failed"
    conn.close()


def test_finalize_run_with_cancelled_status(tmp_path) -> None:
    conn = _open_db(tmp_path)
    run_id = _start_run(conn, tmp_path)
    finalize_run(conn, run_id=run_id, status="cancelled")
    run_row = get_run(conn, run_id=run_id)
    assert run_row["status"] == "cancelled"
    conn.close()


def test_finalize_run_updates_counts(tmp_path) -> None:
    conn = _open_db(tmp_path, )
    run_id = _start_run(conn, tmp_path, total_pairs=3)
    _record(conn, run_id, pair_id="p1", item_status="success")
    _record(conn, run_id, pair_id="p2", item_status="failed")
    _record(conn, run_id, pair_id="p3", item_status="skipped", pair_status="ambiguous")
    finalize_run(conn, run_id=run_id, status="completed")

    run_row = get_run(conn, run_id=run_id)
    assert run_row["processed_count"] == 3
    assert run_row["success_count"] == 1
    assert run_row["failed_count"] == 1
    assert run_row["skipped_count"] == 1
    conn.close()


# ============================================================
# start_run config serialization
# ============================================================


def test_start_run_serializes_config_as_json(tmp_path) -> None:
    conn = _open_db(tmp_path)
    run_id = _start_run(conn, tmp_path)

    # Re-insert with config this time
    source_root = tmp_path / "source"
    derivative_root = tmp_path / "derivatives"
    run_id2 = start_run(
        conn,
        source_root=source_root,
        derivative_root=derivative_root,
        total_pairs=0,
        config={"copy_xmp": True, "quality": 92},
    )

    row = get_run(conn, run_id=run_id2)
    import json
    stored = json.loads(row["config_json"])
    assert stored["copy_xmp"] is True
    assert stored["quality"] == 92
    conn.close()


def test_start_run_no_config_stores_null(tmp_path) -> None:
    conn = _open_db(tmp_path)
    source_root = tmp_path / "source"
    derivative_root = tmp_path / "derivatives"
    run_id = start_run(
        conn,
        source_root=source_root,
        derivative_root=derivative_root,
        total_pairs=0,
        config=None,
    )
    row = get_run(conn, run_id=run_id)
    assert row["config_json"] is None
    conn.close()


# ============================================================
# Duplicate pair_id constraint
# ============================================================


def test_record_item_result_duplicate_pair_id_raises(tmp_path) -> None:
    conn = _open_db(tmp_path)
    run_id = _start_run(conn, tmp_path)
    _record(conn, run_id, pair_id="dup")

    with pytest.raises(sqlite3.IntegrityError):
        _record(conn, run_id, pair_id="dup")

    conn.close()


# ============================================================
# Multiple runs — get_latest_run returns newest
# ============================================================


def test_get_latest_run_returns_most_recent(tmp_path) -> None:
    conn = _open_db(tmp_path)
    run_id_1 = _start_run(conn, tmp_path)
    run_id_2 = _start_run(conn, tmp_path)

    latest = get_latest_run(conn)
    assert latest["run_id"] == run_id_2
    conn.close()


def test_get_run_items_isolated_per_run(tmp_path) -> None:
    conn = _open_db(tmp_path)
    run_id_1 = _start_run(conn, tmp_path, total_pairs=1)
    run_id_2 = _start_run(conn, tmp_path, total_pairs=1)

    _record(conn, run_id_1, pair_id="run1-pair")
    _record(conn, run_id_2, pair_id="run2-pair")

    items_1 = get_run_items(conn, run_id=run_id_1)
    items_2 = get_run_items(conn, run_id=run_id_2)

    assert len(items_1) == 1
    assert items_1[0]["pair_id"] == "run1-pair"
    assert len(items_2) == 1
    assert items_2[0]["pair_id"] == "run2-pair"
    conn.close()


# ============================================================
# get_connection creates parent directories
# ============================================================


def test_get_connection_creates_parent_dirs(tmp_path) -> None:
    nested_path = tmp_path / "a" / "b" / "c" / "test.db"
    assert not nested_path.parent.exists()
    conn = get_connection(nested_path)
    assert nested_path.parent.exists()
    conn.close()
