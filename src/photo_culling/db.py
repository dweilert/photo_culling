from __future__ import annotations

# ============================================================
# Imports
# ============================================================
import json
import sqlite3
from pathlib import Path
from typing import Any

# ============================================================
# Schema
# ============================================================

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at TEXT,
    status TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
    source_root TEXT,
    derivative_root TEXT,
    config_json TEXT,
    total_pairs INTEGER NOT NULL DEFAULT 0,
    processed_count INTEGER NOT NULL DEFAULT 0,
    success_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    skipped_count INTEGER NOT NULL DEFAULT 0,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS items (
    item_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    pair_id TEXT NOT NULL,
    group_key TEXT NOT NULL,
    rel_dir TEXT NOT NULL,
    base_name TEXT NOT NULL,
    pair_status TEXT NOT NULL
        CHECK (pair_status IN ('paired', 'raw_only', 'jpeg_only', 'ambiguous')),
    source_raw TEXT,
    source_jpeg TEXT,
    final_image TEXT,
    used_existing INTEGER NOT NULL DEFAULT 0,
    render_performed INTEGER NOT NULL DEFAULT 0,
    metadata_written INTEGER NOT NULL DEFAULT 0,
    item_status TEXT NOT NULL
        CHECK (item_status IN ('success', 'failed', 'skipped')),
    error_message TEXT,
    completed_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE,
    UNIQUE (run_id, pair_id)
);

CREATE INDEX IF NOT EXISTS idx_items_run_id
    ON items(run_id);

CREATE INDEX IF NOT EXISTS idx_items_run_status
    ON items(run_id, item_status);

CREATE INDEX IF NOT EXISTS idx_items_pair_id
    ON items(pair_id);

CREATE INDEX IF NOT EXISTS idx_items_pair_status
    ON items(pair_status);

CREATE INDEX IF NOT EXISTS idx_items_rel_dir
    ON items(rel_dir);
"""

# ============================================================
# Connection Helpers
# ============================================================


def get_connection(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()


# ============================================================
# Run Lifecycle
# ============================================================


def start_run(
    conn: sqlite3.Connection,
    *,
    source_root: Path,
    derivative_root: Path,
    total_pairs: int,
    config: dict[str, Any] | None = None,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO runs (
            source_root,
            derivative_root,
            config_json,
            total_pairs,
            status
        ) VALUES (?, ?, ?, ?, 'running')
        """,
        (
            str(source_root),
            str(derivative_root),
            json.dumps(config, sort_keys=True) if config else None,
            total_pairs,
        ),
    )
    conn.commit()
    return int(cursor.lastrowid)


def record_item_result(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    pair_id: str,
    group_key: str,
    rel_dir: str,
    base_name: str,
    pair_status: str,
    source_raw: str | None,
    source_jpeg: str | None,
    final_image: str | None,
    used_existing: bool,
    render_performed: bool,
    metadata_written: bool,
    item_status: str,
    error_message: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO items (
            run_id,
            pair_id,
            group_key,
            rel_dir,
            base_name,
            pair_status,
            source_raw,
            source_jpeg,
            final_image,
            used_existing,
            render_performed,
            metadata_written,
            item_status,
            error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            pair_id,
            group_key,
            rel_dir,
            base_name,
            pair_status,
            source_raw,
            source_jpeg,
            final_image,
            int(used_existing),
            int(render_performed),
            int(metadata_written),
            item_status,
            error_message,
        ),
    )
    conn.commit()


def finalize_run(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    status: str,
) -> None:
    conn.execute(
        """
        UPDATE runs
        SET
            completed_at = datetime('now'),
            status = ?,
            processed_count = (
                SELECT COUNT(*)
                FROM items
                WHERE run_id = ?
            ),
            success_count = (
                SELECT COUNT(*)
                FROM items
                WHERE run_id = ? AND item_status = 'success'
            ),
            failed_count = (
                SELECT COUNT(*)
                FROM items
                WHERE run_id = ? AND item_status = 'failed'
            ),
            skipped_count = (
                SELECT COUNT(*)
                FROM items
                WHERE run_id = ? AND item_status = 'skipped'
            )
        WHERE run_id = ?
        """,
        (status, run_id, run_id, run_id, run_id, run_id),
    )
    conn.commit()
