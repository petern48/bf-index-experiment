"""Chart: Planning-Phase Memory Usage (Read) – peak heap during query planning per bloom filter type."""

import argparse
import json
import os

import matplotlib.pyplot as plt
import numpy as np

from common import (
    BF_KEYS, BF_COLORS,
    DEFAULT_DATA, DEFAULT_OUT, group_positions, save, bf_color_legend_handles,
)


def plot(data: dict, out_dir: str):
    sizes = data["dataset_sizes"]
    offsets, ticks, _ = group_positions(len(sizes), 3)
    bar_width = 0.22

    fig, ax = plt.subplots(figsize=(10, 5))

    for i, (key, color) in enumerate(zip(BF_KEYS, BF_COLORS)):
        values = np.array(data["planning_memory_read_mb"][key], dtype=float)
        ax.bar(offsets[i], values, bar_width, color=color)

    ax.legend(handles=bf_color_legend_handles(), title="Bloom Filter Type")
    ax.set_xticks(ticks)
    ax.set_xticklabels(sizes)
    ax.set_xlabel("Dataset Size")
    ax.set_ylabel("Peak Memory Usage (MB)")
    ax.set_title("Peak Memory Usage – Planning Phase")
    fig.tight_layout()
    save(fig, out_dir, "3b_planning_memory_read.png")


def main():
    parser = argparse.ArgumentParser(description="Plot planning-phase memory usage chart.")
    parser.add_argument("--data", default=DEFAULT_DATA)
    parser.add_argument("--out",  default=DEFAULT_OUT)
    args = parser.parse_args()
    with open(args.data) as f:
        data = json.load(f)
    os.makedirs(args.out, exist_ok=True)
    plot(data, args.out)


if __name__ == "__main__":
    main()
