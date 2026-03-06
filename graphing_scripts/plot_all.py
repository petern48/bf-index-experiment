"""Run all bloom filter evaluation charts in one shot."""

import argparse
import json
import os

from common import DEFAULT_DATA, DEFAULT_OUT
import plot_pruning_read
import plot_disk_storage
import plot_memory_read
import plot_execution_memory_read
import plot_planning_memory_read
import plot_memory_write
import plot_time_read
import plot_time_write


def main():
    parser = argparse.ArgumentParser(description="Generate all bloom filter evaluation charts.")
    parser.add_argument("--data", default=DEFAULT_DATA)
    parser.add_argument("--out",  default=DEFAULT_OUT)
    args = parser.parse_args()

    with open(args.data) as f:
        data = json.load(f)

    os.makedirs(args.out, exist_ok=True)
    print(f"Generating graphs → {args.out}/")

    plot_pruning_read.plot(data, args.out)
    plot_disk_storage.plot(data, args.out)
    plot_memory_read.plot(data, args.out)
    plot_execution_memory_read.plot(data, args.out)
    plot_planning_memory_read.plot(data, args.out)
    plot_memory_write.plot(data, args.out)
    plot_time_read.plot(data, args.out)
    plot_time_write.plot(data, args.out)

    print("Done.")


if __name__ == "__main__":
    main()
