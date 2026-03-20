from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from time import perf_counter

from photo_culling.batch import process_all_pairs
from photo_culling.config_loader import load_pipeline_config
from photo_culling.pairing import discover_assets, pair_assets


def main() -> None:
    cfg = load_pipeline_config("config/pipeline.yaml")
    config_dict = asdict(cfg)

    source_root = Path(cfg.paths.source_root)
    derivative_root = Path(cfg.paths.derivative_root)
    db_path = Path(cfg.paths.db_path) if cfg.paths.db_path else None

    print("=== CONFIG ===")
    print(f"Source:      {source_root}")
    print(f"Derivative:  {derivative_root}")
    print(f"DB:          {db_path}")

    start = perf_counter()
    assets = discover_assets(source_root, config=config_dict)
    print(f"\nDiscovered {len(assets)} assets")
    discover_secs = perf_counter() - start

    start = perf_counter()

    pairs = pair_assets(assets)
    print(f"Built {len(pairs)} pairs")
    pair_secs = perf_counter() - start
    
    summary = process_all_pairs(
        pairs,
        source_root=source_root,
        derivative_root=derivative_root,
        config=config_dict,
        db_path=db_path,
        max_workers=2,
    )
    process_secs = perf_counter() - start

    print("\n=== SUMMARY ===")
    print(f"Total:     {summary.total}")
    print(f"Succeeded: {summary.succeeded}")
    print(f"Failed:    {summary.failed}")
    print(f"Skipped:   {summary.skipped}")
    print(f"\nDiscover time: {discover_secs:.2f}s")
    print(f"Pair time:     {pair_secs:.2f}s")
    print(f"Process time:  {process_secs:.2f}s")

if __name__ == "__main__":
    main()