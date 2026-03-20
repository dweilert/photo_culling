from __future__ import annotations

# ============================================================
# Imports
# ============================================================
import argparse
from pathlib import Path

from photo_culling.config_loader import load_pipeline_config
from photo_culling.db import (
    get_connection,
    get_failed_items,
    get_latest_run,
    get_run,
    get_run_items,
    get_skipped_items,
    init_db,
)

# ============================================================
# Formatting Helpers
# ============================================================


def print_run_summary(run_row) -> None:
    print()
    print("Run Summary")
    print("-----------")
    print(f"Run ID:          {run_row['run_id']}")
    print(f"Status:          {run_row['status']}")
    print(f"Started:         {run_row['started_at']}")
    print(f"Completed:       {run_row['completed_at']}")
    print(f"Source Root:     {run_row['source_root']}")
    print(f"Derivative Root: {run_row['derivative_root']}")
    print(f"Total Pairs:     {run_row['total_pairs']}")
    print(f"Processed:       {run_row['processed_count']}")
    print(f"Succeeded:       {run_row['success_count']}")
    print(f"Failed:          {run_row['failed_count']}")
    print(f"Skipped:         {run_row['skipped_count']}")


def print_item_section(title: str, rows) -> None:
    print()
    print(title)
    print("-" * len(title))

    if not rows:
        print("None")
        return

    for row in rows:
        print(f"pair_id:      {row['pair_id']}")
        print(f"pair_status:  {row['pair_status']}")
        print(f"item_status:  {row['item_status']}")
        print(f"rel_dir:      {row['rel_dir']}")
        print(f"base_name:    {row['base_name']}")
        print(f"final_image:  {row['final_image']}")
        print(f"error:        {row['error_message']}")
        print()


def print_all_items(rows) -> None:
    print()
    print("All Items")
    print("---------")

    if not rows:
        print("None")
        return

    for row in rows:
        print(
            f"{row['pair_id']} | "
            f"{row['item_status']} | "
            f"{row['pair_status']} | "
            f"{row['rel_dir']} | "
            f"{row['base_name']}"
        )


# ============================================================
# Main
# ============================================================


# def main() -> None:
#     parser = argparse.ArgumentParser(
#         description="Inspect photo_culling SQLite batch results."
#     )
#     parser.add_argument(
#         "--db-path",
#         type=Path,
#         default=DEFAULT_DB_PATH,
#         help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
#     )
#     parser.add_argument(
#         "--run-id",
#         type=int,
#         default=None,
#         help="Specific run_id to inspect. Defaults to latest run.",
#     )
#     parser.add_argument(
#         "--show-all-items",
#         action="store_true",
#         help="Show all items for the run, not just failed/skipped items.",
#     )

#     args = parser.parse_args()

#     conn = get_connection(args.db_path)
#     init_db(conn)

#     try:
#         if args.run_id is None:
#             run_row = get_latest_run(conn)
#         else:
#             run_row = get_run(conn, run_id=args.run_id)

#         if run_row is None:
#             print("No run data found.")
#             return

#         run_id = int(run_row["run_id"])

#         print_run_summary(run_row)

#         failed_rows = get_failed_items(conn, run_id=run_id)
#         skipped_rows = get_skipped_items(conn, run_id=run_id)

#         print_item_section("Failed Items", failed_rows)
#         print_item_section("Skipped Items", skipped_rows)

#         if args.show_all_items:
#             all_rows = get_run_items(conn, run_id=run_id)
#             print_all_items(all_rows)

#     finally:
#         conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect photo_culling SQLite batch results."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="Path to SQLite database. Defaults to pipeline.yaml paths.db_path.",
    )
    parser.add_argument(
        "--run-id",
        type=int,
        default=None,
        help="Specific run_id to inspect. Defaults to latest run.",
    )
    parser.add_argument(
        "--show-all-items",
        action="store_true",
        help="Show all items for the run, not just failed/skipped items.",
    )

    args = parser.parse_args()

    if args.db_path is not None:
        db_path = args.db_path
    else:
        cfg = load_pipeline_config("config/pipeline.yaml")
        if not cfg.paths.db_path:
            raise ValueError("No db_path configured in config/pipeline.yaml")
        db_path = Path(cfg.paths.db_path)

    conn = get_connection(db_path)
    init_db(conn)

    try:
        if args.run_id is None:
            run_row = get_latest_run(conn)
        else:
            run_row = get_run(conn, run_id=args.run_id)

        if run_row is None:
            print("No run data found.")
            return

        run_id = int(run_row["run_id"])

        print_run_summary(run_row)

        failed_rows = get_failed_items(conn, run_id=run_id)
        skipped_rows = get_skipped_items(conn, run_id=run_id)

        print_item_section("Failed Items", failed_rows)
        print_item_section("Skipped Items", skipped_rows)

        if args.show_all_items:
            all_rows = get_run_items(conn, run_id=run_id)
            print_all_items(all_rows)

    finally:
        conn.close()

if __name__ == "__main__":
    main()