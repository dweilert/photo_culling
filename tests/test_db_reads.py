from __future__ import annotations

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


def test_db_read_helpers(tmp_path) -> None:
    db_path = tmp_path / "photo_culling.db"
    source_root = tmp_path / "source"
    derivative_root = tmp_path / "derivatives"

    source_root.mkdir()
    derivative_root.mkdir()

    conn = get_connection(db_path)
    init_db(conn)

    run_id = start_run(
        conn,
        source_root=source_root,
        derivative_root=derivative_root,
        total_pairs=3,
        config={"copy_xmp": True},
    )

    record_item_result(
        conn,
        run_id=run_id,
        pair_id="pair-success",
        group_key="pair-success",
        rel_dir="album1",
        base_name="img001",
        pair_status="paired",
        source_raw=str(source_root / "album1" / "img001.cr2"),
        source_jpeg=str(source_root / "album1" / "img001.jpg"),
        final_image=str(derivative_root / "album1" / "img001.jpg"),
        used_existing=True,
        render_performed=False,
        metadata_written=True,
        item_status="success",
        error_message=None,
    )

    record_item_result(
        conn,
        run_id=run_id,
        pair_id="pair-failed",
        group_key="pair-failed",
        rel_dir="album2",
        base_name="img002",
        pair_status="raw_only",
        source_raw=str(source_root / "album2" / "img002.cr2"),
        source_jpeg=None,
        final_image=None,
        used_existing=False,
        render_performed=False,
        metadata_written=False,
        item_status="failed",
        error_message="simulated failure",
    )

    record_item_result(
        conn,
        run_id=run_id,
        pair_id="pair-skipped",
        group_key="pair-skipped",
        rel_dir="album3",
        base_name="img003",
        pair_status="ambiguous",
        source_raw=str(source_root / "album3" / "img003.cr2"),
        source_jpeg=str(source_root / "album3" / "img003.jpg"),
        final_image=None,
        used_existing=False,
        render_performed=False,
        metadata_written=False,
        item_status="skipped",
        error_message="pair status is ambiguous",
    )

    finalize_run(conn, run_id=run_id, status="completed")

    latest_run = get_latest_run(conn)
    assert latest_run is not None
    assert latest_run["run_id"] == run_id
    assert latest_run["status"] == "completed"
    assert latest_run["success_count"] == 1
    assert latest_run["failed_count"] == 1
    assert latest_run["skipped_count"] == 1

    run_row = get_run(conn, run_id=run_id)
    assert run_row is not None
    assert run_row["run_id"] == run_id
    assert run_row["processed_count"] == 3

    all_items = get_run_items(conn, run_id=run_id)
    assert len(all_items) == 3

    failed_items = get_failed_items(conn, run_id=run_id)
    assert len(failed_items) == 1
    assert failed_items[0]["pair_id"] == "pair-failed"
    assert failed_items[0]["error_message"] == "simulated failure"

    skipped_items = get_skipped_items(conn, run_id=run_id)
    assert len(skipped_items) == 1
    assert skipped_items[0]["pair_id"] == "pair-skipped"
    assert skipped_items[0]["error_message"] == "pair status is ambiguous"

    conn.close()
