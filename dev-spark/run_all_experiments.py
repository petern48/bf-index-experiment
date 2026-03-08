#!/usr/bin/env python3
"""
Run CreateTableSpark + ReadTableSpark for each bloom mode and experiment, then write
graphing_scripts/bloom_filter_results.json. Each experiment produces a separate figure.

Experiments:
  1) High cardinality: users_random (100M rows), read WHERE user_id IN (...)
  2) Medium cardinality: events_medium_cardinality (200M rows)
     2a) WHERE device_id = 123456
     2b) WHERE device_id = -1 (false positive test)
     2c) WHERE device_id BETWEEN 1000 AND 2000 (range query)

Usage (from repo root):
  python dev-spark/run_all_experiments.py
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
DEV_SPARK = REPO_ROOT / "dev-spark"
GRAPHING = REPO_ROOT / "graphing_scripts"
OUTPUT_JSON = GRAPHING / "bloom_filter_results.json"

BLOOM_MODES = ["none", "row_group", "file_level"]

# (experiment_base, read_query_id) -> (write_query_display, read_query_display, dataset_config)
EXPERIMENTS = [
    (
        "high_cardinality_in",
        "high_cardinality",
        "in",
        "CREATE TABLE users_random AS SELECT id, uuid() AS user_id, substr(md5(rand()),1,20) AS payload FROM range(100000000)",
        "SELECT * FROM users_random WHERE user_id IN ('uuid1','uuid2','uuid3')",
        "100,000,000 records",
    ),
    (
        "medium_cardinality_where",
        "medium_cardinality",
        "where",
        "CREATE TABLE events_medium_cardinality AS SELECT id, cast(rand()*1000000 as int) AS device_id, cast(rand()*1000 as int) AS tenant_id, substr(md5(rand()),1,20) AS payload FROM range(200000000)",
        "SELECT * FROM events_medium_cardinality WHERE device_id = 123456",
        "200,000,000 records",
    ),
    # (
    #     "medium_cardinality_false_positive",
    #     "medium_cardinality",
    #     "false_positive",
    #     "CREATE TABLE events_medium_cardinality AS SELECT id, cast(rand()*1000000 as int) AS device_id, cast(rand()*1000 as int) AS tenant_id, substr(md5(rand()),1,20) AS payload FROM range(200000000)",
    #     "SELECT * FROM events_medium_cardinality WHERE device_id = -1",
    #     "200,000,000 records",
    # ),
    # (
    #     "medium_cardinality_range",
    #     "medium_cardinality",
    #     "range",
    #     "CREATE TABLE events_medium_cardinality AS SELECT id, cast(rand()*1000000 as int) AS device_id, cast(rand()*1000 as int) AS tenant_id, substr(md5(rand()),1,20) AS payload FROM range(200000000)",
    #     "SELECT * FROM events_medium_cardinality WHERE device_id BETWEEN 1000 AND 2000",
    #     "200,000,000 records",
    # ),
]


def run_gradle(task: str, run_args: str | None = None) -> None:
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
    if v is None:
        return default
    return float(v)


def _int(v: Any, default: int = 0) -> int:
    if v is None:
        return default
    return int(v)


def build_results(
    experiments: List[Tuple[str, str, Dict[str, Any], Dict[str, Any]]],
) -> Dict[str, Any]:
    """Build bloom_filter_results.json. experiments: (bloom_mode, experiment_id, write_metrics, read_metrics)."""
    bf_key = {
        "none": "no_bloom_filter",
        "row_group": "row_group_bloom_filter",
        "file_level": "file_level_bloom_filter",
    }
    experiment_ids = list(dict.fromkeys(eid for _m, eid, _w, _r in experiments))
    by_mode_exp: Dict[Tuple[str, str], Tuple[Dict[str, Any], Dict[str, Any]]] = {}
    for mode, exp_id, w, r in experiments:
        by_mode_exp[(mode, exp_id)] = (w, r)

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
        for exp_id in experiment_ids:
            pair = by_mode_exp.get((mode, exp_id))
            if not pair:
                continue
            w, r = pair
            total_row_groups = w.get("totalRowGroups")
            pruning_read[key].append({
                "total_row_groups": _int(total_row_groups),
                "row_groups_read": _int(r.get("rowGroupsRead")),
                "skipped_row_groups": _int(r.get("allSkippedRowGroups")),
                "row_groups_skipped_by_file_bloom_filter": _int(r.get("rowGroupsSkippedByFileBloomFilter")),
                "total_data_files": _int(w.get("totalDataFiles")),
                "manifest_skipped_data_files": _int(r.get("manifestSkippedDataFiles")),
                "bloom_filter_skipped_data_files": _int(r.get("bloomFilterSkippedDataFiles")),
                "result_data_files": _int(r.get("resultDataFiles")),
            })
            disk_storage_bytes[key].append({
                "puffin_bytes": _int(w.get("puffinDiskSizeInBytes")),
                "data_bytes": _int(w.get("dataFileDiskSizeInBytes")),
                "manifest_overhead_bytes": _int(w.get("manifestDiskSizeInBytes")),
            })
            memory_read_mb[key].append({
                "max_mb": _n(r.get("maxMemoryUsage")),
                "puffin_mb": _n(r.get("readPuffinMaxMemory")),
            })
            memory_write_mb[key].append({
                "data_mb": _n(w.get("writeDataMaxMemory")),
                "puffin_mb": _n(w.get("writePuffinMaxMemory")),
            })
            total_read_ms = r.get("totalReadDuration")
            if total_read_ms is not None:
                total_read_ms = round(_n(total_read_ms))
            puffin_ms = _n(r.get("readPuffinDuration"))
            time_read_ms[key].append({
                "total_ms": total_read_ms,
                "puffin_ms": puffin_ms,
            })
            data_ms = _n(w.get("writeDataDuration"))
            puffin_ms = _n(w.get("writePuffinDuration"))
            total_write_ms = round(data_ms + puffin_ms) if (data_ms > 0 or puffin_ms > 0) else None
            time_write_ms[key].append({
                "metadata_ms": 0,
                "puffin_ms": puffin_ms,
                "data_ms": data_ms,
                "total_ms": total_write_ms,
            })

    exp_meta = {eid: {} for eid in experiment_ids}
    for exp_id, _base, _rq, write_display, read_display, dataset_display in EXPERIMENTS:
        if exp_id not in exp_meta:
            continue
        pair = by_mode_exp.get(("none", exp_id)) or next(
            (p for (m, e), p in by_mode_exp.items() if e == exp_id), (None, None)
        )
        if pair:
            w, r = pair
            exp_meta[exp_id] = {
                "write_query": w.get("writeQuery") or write_display,
                "read_query": r.get("readQuery") or read_display,
                "dataset_config": dataset_display,
            }

    return {
        "experiments": experiment_ids,
        "experiment_metadata": exp_meta,
        "pruning_read": pruning_read,
        "disk_storage_bytes": disk_storage_bytes,
        "memory_read_mb": memory_read_mb,
        "memory_write_mb": memory_write_mb,
        "time_read_ms": time_read_ms,
        "time_write_ms": time_write_ms,
    }


def main() -> None:
    print("Running experiments (CreateTableSpark + ReadTableSpark)...")
    experiments: List[Tuple[str, str, Dict[str, Any], Dict[str, Any]]] = []
    # For medium_cardinality, create once per bloom mode then run 3 reads
    seen_create: Set[Tuple[str, str]] = set()

    for exp_id, exp_base, read_query_id, _write_display, _read_display, _dataset in EXPERIMENTS:
        for mode in BLOOM_MODES:
            create_key = (mode, exp_base)
            if create_key not in seen_create:
                print(f"\n--- Create {exp_base} | Bloom: {mode} ---")
                run_gradle(":iceberg-dev-spark:run", run_args=f"{mode},{exp_base}")
                seen_create.add(create_key)

            write_path = DEV_SPARK / "write-metrics.json"
            if not write_path.exists():
                print(f"  WARNING: {write_path} not found")
                continue
            write_metrics = load_json(write_path)

            print(f"\n--- Read {exp_id} | Bloom: {mode} ---")
            run_gradle(":iceberg-dev-spark:runReadTable", run_args=f"{mode},{exp_base},{read_query_id}")
            read_path = DEV_SPARK / "read-metrics.json"
            if not read_path.exists():
                print(f"  WARNING: {read_path} not found")
                continue
            read_metrics = load_json(read_path)

            experiments.append((mode, exp_id, write_metrics, read_metrics))

    if not experiments:
        print("No experiments collected.")
        sys.exit(1)

    results = build_results(experiments)
    GRAPHING.mkdir(parents=True, exist_ok=True)
    with OUTPUT_JSON.open("w") as f:
        json.dump(results, f, indent=2)

    print(f"\nWrote {len(experiments)} experiment(s) to {OUTPUT_JSON}")
    print("Run: python3 graphing_scripts/utils.py")


if __name__ == "__main__":
    main()
