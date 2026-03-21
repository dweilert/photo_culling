from __future__ import annotations

import pytest

from photo_culling.report_db import print_all_items, print_item_section, print_run_summary


# ============================================================
# Helpers
# ============================================================


def _make_run_row(**overrides) -> dict:
    row = {
        "run_id": 1,
        "status": "completed",
        "started_at": "2024-01-01 10:00:00",
        "completed_at": "2024-01-01 10:05:00",
        "source_root": "/photos",
        "derivative_root": "/derivatives",
        "total_pairs": 10,
        "processed_count": 10,
        "success_count": 8,
        "failed_count": 1,
        "skipped_count": 1,
    }
    row.update(overrides)
    return row


def _make_item_row(**overrides) -> dict:
    row = {
        "pair_id": "set1::img001",
        "pair_status": "paired",
        "item_status": "success",
        "rel_dir": "set1",
        "base_name": "img001",
        "final_image": "/derivatives/set1/img001.jpg",
        "error_message": None,
    }
    row.update(overrides)
    return row


# ============================================================
# print_run_summary
# ============================================================


def test_print_run_summary_contains_run_id(capsys) -> None:
    print_run_summary(_make_run_row(run_id=42))
    captured = capsys.readouterr()
    assert "42" in captured.out


def test_print_run_summary_contains_status(capsys) -> None:
    print_run_summary(_make_run_row(status="failed"))
    captured = capsys.readouterr()
    assert "failed" in captured.out


def test_print_run_summary_contains_counts(capsys) -> None:
    print_run_summary(_make_run_row(success_count=7, failed_count=2, skipped_count=1))
    captured = capsys.readouterr()
    assert "7" in captured.out
    assert "2" in captured.out
    assert "1" in captured.out


def test_print_run_summary_contains_paths(capsys) -> None:
    print_run_summary(_make_run_row(source_root="/my/photos", derivative_root="/my/derivatives"))
    captured = capsys.readouterr()
    assert "/my/photos" in captured.out
    assert "/my/derivatives" in captured.out


def test_print_run_summary_contains_timestamps(capsys) -> None:
    print_run_summary(
        _make_run_row(started_at="2024-06-01 09:00:00", completed_at="2024-06-01 09:30:00")
    )
    captured = capsys.readouterr()
    assert "2024-06-01 09:00:00" in captured.out
    assert "2024-06-01 09:30:00" in captured.out


# ============================================================
# print_item_section
# ============================================================


def test_print_item_section_with_rows_shows_pair_id(capsys) -> None:
    rows = [_make_item_row(pair_id="album1::img005")]
    print_item_section("Failed Items", rows)
    captured = capsys.readouterr()
    assert "album1::img005" in captured.out


def test_print_item_section_with_rows_shows_title(capsys) -> None:
    print_item_section("Skipped Items", [_make_item_row()])
    captured = capsys.readouterr()
    assert "Skipped Items" in captured.out


def test_print_item_section_with_rows_shows_error_message(capsys) -> None:
    rows = [_make_item_row(item_status="failed", error_message="render exploded")]
    print_item_section("Failed Items", rows)
    captured = capsys.readouterr()
    assert "render exploded" in captured.out


def test_print_item_section_empty_prints_none(capsys) -> None:
    print_item_section("Failed Items", [])
    captured = capsys.readouterr()
    assert "None" in captured.out


def test_print_item_section_empty_still_shows_title(capsys) -> None:
    print_item_section("My Section", [])
    captured = capsys.readouterr()
    assert "My Section" in captured.out


def test_print_item_section_multiple_rows(capsys) -> None:
    rows = [
        _make_item_row(pair_id="a::img001"),
        _make_item_row(pair_id="b::img002"),
    ]
    print_item_section("Items", rows)
    captured = capsys.readouterr()
    assert "a::img001" in captured.out
    assert "b::img002" in captured.out


# ============================================================
# print_all_items
# ============================================================


def test_print_all_items_shows_pair_id(capsys) -> None:
    rows = [_make_item_row(pair_id="set2::img010")]
    print_all_items(rows)
    captured = capsys.readouterr()
    assert "set2::img010" in captured.out


def test_print_all_items_shows_status(capsys) -> None:
    rows = [_make_item_row(item_status="skipped", pair_status="ambiguous")]
    print_all_items(rows)
    captured = capsys.readouterr()
    assert "skipped" in captured.out
    assert "ambiguous" in captured.out


def test_print_all_items_empty_prints_none(capsys) -> None:
    print_all_items([])
    captured = capsys.readouterr()
    assert "None" in captured.out


def test_print_all_items_multiple_rows_all_present(capsys) -> None:
    rows = [
        _make_item_row(pair_id="dir::img001", item_status="success"),
        _make_item_row(pair_id="dir::img002", item_status="failed"),
        _make_item_row(pair_id="dir::img003", item_status="skipped"),
    ]
    print_all_items(rows)
    captured = capsys.readouterr()
    assert "img001" in captured.out
    assert "img002" in captured.out
    assert "img003" in captured.out
