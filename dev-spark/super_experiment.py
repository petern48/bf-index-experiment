#!/usr/bin/env python3
"""
Super experiment: runs each (bloom_mode x dataset_size) combination N times,
averages the noisy metrics (memory, time), and writes bloom_filter_results.json.

Table creation runs once per (mode, size) — it is deterministic.
ReadTableSpark runs N times per combination and the measurements are averaged.

Usage (from repo root):
  python dev-spark/super_experiment.py

Custom run count:
  python dev-spark/super_experiment.py --runs 5
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
DEV_SPARK = REPO_ROOT / "dev-spark"
GRAPHING = REPO_ROOT / "graphing_scripts"
OUTPUT_JSON = GRAPHING / "bloom_filter_results.json"

BLOOM_MODES = ["none", "row_group", "file_level"]
DATASET_SIZES = [
    ("small", 10, 100_000),   # 10 files x 100K rows = 1M rows
    ("large", 50, 500_000),   # 50 files x 500K rows = 25M rows
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


def _n(v: Any, default: float = 0.0) -> float:
    return float(v) if v is not None else default


def _int(v: Any, default: int = 0) -> int:
    return int(v) if v is not None else default


def _avg(values: List[Any]) -> float:
    nums = [float(v) for v in values if v is not None]
    return sum(nums) / len(nums) if nums else 0.0


def _avg_int(values: List[Any]) -> Optional[int]:
    nums = [float(v) for v in values if v is not None]
    return round(sum(nums) / len(nums)) if nums else None


def merge_read_metrics(runs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Average the noisy float measurements across runs.
    Deterministic counts/sizes are taken from the first run unchanged.
    """
    merged = dict(runs[0])
    merged["maxMemoryUsage"] = _avg([r.get("maxMemoryUsage") for r in runs])
    merged["planningMemoryUsage"] = _avg([r.get("planningMemoryUsage") for r in runs])
    merged["totalReadDuration"] = _avg_int([r.get("totalReadDuration") for r in runs])
    return merged


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
    by_mode_size: Dict[Tuple[str, str], Tuple[Dict[str, Any], Dict[str, Any]]] = {}
    for mode, size_label, w, r in experiments:
        by_mode_size[(mode, size_label)] = (w, r)

    pruning_read = {k: [] for k in bf_key.values()}
    disk_storage_bytes = {k: [] for k in bf_key.values()}
    memory_read_mb = {k: [] for k in bf_key.values()}
    planning_memory_read_mb = {k: [] for k in bf_key.values()}
    execution_memory_read_mb = {k: [] for k in bf_key.values()}
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
            total_rg = w.get("totalRowGroups") if w.get("totalRowGroups") is not None else r.get("totalRowGroups")
            pruning_read[key].append({
                "total_row_groups": _int(total_rg),
                "skipped_row_groups": _int(r.get("skippedRowGroups")),
                "total_data_files": _int(w.get("totalDataFiles")),
                "skipped_data_files": _int(r.get("skippedDataFiles")),
            })
            disk_storage_bytes[key].append({
                "puffin_bytes": _int(w.get("puffinDiskSizeInBytes")),
                "manifest_overhead_bytes": 0,
            })
            memory_read_mb[key].append(_n(r.get("maxMemoryUsage")))
            planning_memory_read_mb[key].append(_n(r.get("planningMemoryUsage")))
            execution_memory_read_mb[key].append(
                max(0.0, _n(r.get("maxMemoryUsage")) - _n(r.get("planningMemoryUsage")))
            )
            memory_write_mb[key].append(_n(w.get("maxMemoryUsage")))
            total_read_ms = r.get("totalReadDuration")
            if total_read_ms is not None:
                total_read_ms = round(_n(total_read_ms))
            time_read_ms[key].append({
                "metadata_ms": sec_to_ms(r.get("manifestReadDuration")),
                "puffin_ms": sec_to_ms(r.get("puffinReadDuration")),
                "data_ms": sec_to_ms(r.get("datafileReadDuration")),
                "total_ms": total_read_ms,
            })
            total_write_ms = w.get("totalWriteDuration")
            if total_write_ms is not None:
                total_write_ms = round(sec_to_ms(total_write_ms))
            time_write_ms[key].append({
                "metadata_ms": sec_to_ms(w.get("manifestWriteDuration")),
                "puffin_ms": sec_to_ms(w.get("puffinWriteDuration")),
                "data_ms": sec_to_ms(w.get("datafileWriteDuration")),
                "total_ms": total_write_ms,
            })

    return {
        "dataset_sizes": dataset_size_labels,
        "pruning_read": pruning_read,
        "disk_storage_bytes": disk_storage_bytes,
        "memory_read_mb": memory_read_mb,
        "planning_memory_read_mb": planning_memory_read_mb,
        "execution_memory_read_mb": execution_memory_read_mb,
        "memory_write_mb": memory_write_mb,
        "time_read_ms": time_read_ms,
        "time_write_ms": time_write_ms,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Super experiment: run each bloom mode N times and average results."
    )
    parser.add_argument("--runs", type=int, default=3, help="Read runs per experiment (default: 3)")
    args = parser.parse_args()
    n_runs = args.runs

    dataset_labels = [label for label, _, _ in DATASET_SIZES]
    total = len(DATASET_SIZES) * len(BLOOM_MODES)
    print(f"Super experiment: {n_runs} read run(s) per combination ({total} combinations)")
    print(f"Dataset sizes: {dataset_labels}")

    experiments = []

    for size_label, num_files, records_per_file in DATASET_SIZES:
        for mode in BLOOM_MODES:
            run_args = f"{mode},{num_files},{records_per_file}"
            print(f"\n=== [{size_label}] bloom={mode} ({num_files} files x {records_per_file} rows) ===")

            # Table creation is deterministic — create once, read N times
            print(f"  [write 1/1]")
            run_gradle(":iceberg-dev-spark:run", run_args=run_args)
            write_path = DEV_SPARK / "write-metrics.json"
            if not write_path.exists():
                print(f"  WARNING: {write_path} not found, skipping")
                continue
            write_metrics = load_json(write_path)

            read_runs: List[Dict[str, Any]] = []
            for i in range(n_runs):
                print(f"  [read {i + 1}/{n_runs}]")
                run_gradle(":iceberg-dev-spark:runReadTable")
                read_path = DEV_SPARK / "read-metrics.json"
                if not read_path.exists():
                    print(f"  WARNING: {read_path} not found on run {i + 1}, skipping")
                    continue
                read_runs.append(load_json(read_path))

            if not read_runs:
                print(f"  WARNING: no read runs collected, skipping")
                continue

            avg_read = merge_read_metrics(read_runs)
            print(f"  Averaged {len(read_runs)} run(s):")
            print(f"    maxMemoryUsage:      {avg_read['maxMemoryUsage']:.2f} MB")
            if avg_read.get("planningMemoryUsage") is not None:
                print(f"    planningMemoryUsage: {avg_read['planningMemoryUsage']:.2f} MB")
            if avg_read.get("totalReadDuration") is not None:
                print(f"    totalReadDuration:   {avg_read['totalReadDuration']} ms")

            experiments.append((mode, size_label, write_metrics, avg_read))

    if not experiments:
        print("No experiments collected.")
        sys.exit(1)

    results = build_results(experiments, dataset_labels)
    GRAPHING.mkdir(parents=True, exist_ok=True)
    with OUTPUT_JSON.open("w") as f:
        json.dump(results, f, indent=2)

    print(f"\nWrote {len(experiments)} experiment(s) ({n_runs} read runs each) to {OUTPUT_JSON}")
    print("Run graphing scripts from repo root: python graphing_scripts/plot_all.py")


if __name__ == "__main__":
    main()
