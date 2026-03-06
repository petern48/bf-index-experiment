#!/usr/bin/env python3
"""
Run CreateTableSpark + ReadTableSpark for each bloom mode, then write
graphing_scripts/bloom_filter_results.json in the format expected by the plotting scripts.

Usage (from repo root):
  python dev-spark/run_all_experiments.py

Or from dev-spark:
  python run_all_experiments.py

Requires: dev-spark/write-metrics.json and dev-spark/read-metrics.json are produced
by each run (CreateTableSpark writes write-metrics.json, ReadTableSpark writes read-metrics.json).
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
DEV_SPARK = REPO_ROOT / "dev-spark"
GRAPHING = REPO_ROOT / "graphing_scripts"
OUTPUT_JSON = GRAPHING / "bloom_filter_results.json"

# Bloom modes: none, row_group (Parquet only), file_level (Parquet + Puffin)
BLOOM_MODES = ["none", "row_group", "file_level"]
# Dataset sizes: (label, num_files, records_per_file). Add more for bigger experiments.
DATASET_SIZES = [
    ("small", 10, 100_000),       # 10 files x 100K rows = 1M rows
    ("large", 50, 500_000),       # 50 files x 500K rows = 25M rows
]


def run_gradle(task: str, run_args: Optional[str] = None) -> None:
    cmd = ["./gradlew", task]
    if run_args is not None:
        cmd.append(f"-PrunArgs={run_args}")
    print(f"  Running: {' '.join(cmd)}")
    r = subprocess.run(cmd, cwd=REPO_ROOT)
    if r.returncode != 0:
        sys.exit(r.returncode)


def load_json(path: Path) -> Dict[str, Any]:
    with path.open() as f:
        return json.load(f)


def _n(v: Any, default: float = 0) -> float:
    """Coerce value to number; use default if None or missing."""
    if v is None:
        return default
    return float(v)


def _int(v: Any, default: int = 0) -> int:
    if v is None:
        return default
    return int(v)


def build_results(
    experiments: List[Tuple[str, str, Dict[str, Any], Dict[str, Any]]],
    dataset_size_labels: List[str],
) -> Dict[str, Any]:
    """Build bloom_filter_results.json in the format expected by graphing_scripts."""
    bf_key = {
        "none": "no_bloom_filter",
        "row_group": "row_group_bloom_filter",
        "file_level": "file_level_bloom_filter",
    }
    # Index by (mode, size_label) for lookup
    by_mode_size: Dict[Tuple[str, str], Tuple[Dict[str, Any], Dict[str, Any]]] = {}
    for mode, size_label, w, r in experiments:
        by_mode_size[(mode, size_label)] = (w, r)

    pruning_read = {k: [] for k in bf_key.values()}
    disk_storage_bytes = {k: [] for k in bf_key.values()}
    memory_read_mb = {k: [] for k in bf_key.values()}
    memory_write_mb = {k: [] for k in bf_key.values()}
    time_read_ms = {k: [] for k in bf_key.values()}
    time_write_ms = {k: [] for k in bf_key.values()}

    def sec_to_ms(x: Any) -> int:
        return round(_n(x) * 1000)

    for key in bf_key.values():
        mode = next(m for m, k in bf_key.items() if k == key)
        for size_label in dataset_size_labels:
            pair = by_mode_size.get((mode, size_label))
            if not pair:
                continue
            w, r = pair
            # write_rg = w.get("totalRowGroups")
            # read_rg = r.get("totalRowGroups")  # don't use totalRowGroups from read, it's not right (it's less than that of write)
            total_row_groups = w.get("totalRowGroups")
            # if write_rg is not None:
            #     total_rg = write_rg
            #     if read_rg is not None and write_rg != read_rg:
            #         raise Exception(f"Note: totalRowGroups differs (write={write_rg}, read={read_rg});")
            pruning_read[key].append({
                "total_row_groups": _int(total_row_groups),
                "skipped_row_groups": _int(r.get("allSkippedRowGroups")),
                "total_data_files": _int(w.get("totalDataFiles")),
                "manifest_skipped_data_files": _int(r.get("manifestSkippedDataFiles")),
                "bloom_filter_skipped_data_files": _int(r.get("bloomFilterSkippedDataFiles")),
                "result_data_files": _int(r.get("resultDataFiles")),
            })
            disk_storage_bytes[key].append({
                "puffin_bytes": _int(w.get("puffinDiskSizeInBytes")),
                "manifest_overhead_bytes": 0,
            })
            memory_read_mb[key].append({
                "max_mb": _n(r.get("maxMemoryUsage")),
                "puffin_mb": _n(r.get("readPuffinMaxMemory")),
            })
            memory_write_mb[key].append({
                "data_mb": _n(w.get("writeDataMaxMemory")),
                "puffin_mb": _n(w.get("writePuffinMaxMemory")),
            })
            # Read durations: totalReadDuration (bar height), readPuffinDuration (subset)
            total_read_ms = r.get("totalReadDuration")
            if total_read_ms is not None:
                total_read_ms = round(_n(total_read_ms))
            puffin_ms = _n(r.get("readPuffinDuration"))
            time_read_ms[key].append({
                "total_ms": total_read_ms,
                "puffin_ms": puffin_ms,
            })
            total_write_ms = w.get("totalWriteDuration")
            if total_write_ms is not None:
                total_write_ms = round(sec_to_ms(total_write_ms))
            # writeDataDuration and writePuffinDuration are in ms
            data_ms = _n(w.get("writeDataDuration"))
            puffin_ms = _n(w.get("writePuffinDuration"))
            if total_write_ms is None and (data_ms > 0 or puffin_ms > 0):
                total_write_ms = round(data_ms + puffin_ms)
            time_write_ms[key].append({
                "metadata_ms": sec_to_ms(w.get("manifestWriteDuration")),
                "puffin_ms": puffin_ms,
                "data_ms": data_ms,
                "total_ms": total_write_ms,
            })

    return {
        "dataset_sizes": dataset_size_labels,
        "pruning_read": pruning_read,
        "disk_storage_bytes": disk_storage_bytes,
        "memory_read_mb": memory_read_mb,
        "memory_write_mb": memory_write_mb,
        "time_read_ms": time_read_ms,
        "time_write_ms": time_write_ms,
    }


def main() -> None:
    dataset_labels = [label for label, _nf, _rpf in DATASET_SIZES]
    print(
        "Running experiments (CreateTableSpark + ReadTableSpark) for each bloom mode and dataset size..."
    )
    print(f"Dataset sizes: {dataset_labels}")
    experiments = []

    for size_label, num_files, records_per_file in DATASET_SIZES:
        for mode in BLOOM_MODES:
            run_args = f"{mode},{num_files},{records_per_file}"
            print(f"\n--- Dataset: {size_label} ({num_files} files x {records_per_file} rows) | Bloom: {mode} ---")
            run_gradle(":iceberg-dev-spark:run", run_args=run_args)
            write_path = DEV_SPARK / "write-metrics.json"
            if not write_path.exists():
                print(f"  WARNING: {write_path} not found after CreateTableSpark")
                continue
            write_metrics = load_json(write_path)

            run_gradle(":iceberg-dev-spark:runReadTable")
            read_path = DEV_SPARK / "read-metrics.json"
            if not read_path.exists():
                print(f"  WARNING: {read_path} not found after ReadTableSpark")
                continue
            read_metrics = load_json(read_path)

            experiments.append((mode, size_label, write_metrics, read_metrics))

    if not experiments:
        print("No experiments collected. Ensure write-metrics.json and read-metrics.json are produced.")
        sys.exit(1)

    results = build_results(experiments, dataset_labels)

    # Validate: manifestSkipped + bloomFilterSkipped + readDataFiles == total per experiment
    for mode, size_label, w, r in experiments:
        total = w.get("totalDataFiles")
        manifest_skipped = r.get("manifestSkippedDataFiles") or 0
        bloom_skipped = r.get("bloomFilterSkippedDataFiles") or 0
        read_data_files = (total or 0) - manifest_skipped - bloom_skipped
        if total is not None and (read_data_files < 0 or read_data_files + manifest_skipped + bloom_skipped != total):
            raise ValueError(
                f"Validation failed for {mode}/{size_label}: "
                f"readDataFiles ({read_data_files}) + manifestSkipped ({manifest_skipped}) + bloomFilterSkipped ({bloom_skipped}) != totalDataFiles ({total})"
            )
    GRAPHING.mkdir(parents=True, exist_ok=True)
    with OUTPUT_JSON.open("w") as f:
        json.dump(results, f, indent=2)

    print(f"\nWrote {len(experiments)} experiment(s) to {OUTPUT_JSON}")
    print("Run graphing scripts from repo root: python graphing_scripts/plot_all.ipynb")


if __name__ == "__main__":
    main()
