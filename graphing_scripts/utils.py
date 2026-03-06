"""Bloom filter evaluation graphs – shared constants, helpers, and plot functions."""

import argparse
import json
import os

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

# ---------------------------------------------------------------------------
# Colour palette
# ---------------------------------------------------------------------------
COLORS = {
    "no_bf":   "#4c72b0",  # muted blue
    "rg_bf":   "#dd8452",  # muted orange
    "file_bf": "#55a868",  # muted green
}

BF_LABELS = {
    "no_bloom_filter":         "No Bloom Filter",
    "row_group_bloom_filter":  "Row-Group BF",
    "file_level_bloom_filter": "File-Level BF",
}

BF_KEYS   = list(BF_LABELS.keys())
BF_COLORS = [COLORS["no_bf"], COLORS["rg_bf"], COLORS["file_bf"]]

STACK_ALPHA_LIGHT = 0.45  # lighter shade for the "active/read" portion of stacked bars

STACK_COLORS = {
    "metadata": "#8da0cb",  # periwinkle
    "puffin":   "#fc8d62",  # coral
    "data":     "#66c2a5",  # teal
}

_HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA = os.path.join(_HERE, "bloom_filter_results.json")
DEFAULT_OUT  = os.path.join(_HERE, "graphs")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def group_positions(n_groups: int, n_bars: int, bar_width: float = 0.22, gap: float = 0.08):
    """Return (offsets, tick_centers, group_width) for a grouped bar chart."""
    group_width = n_bars * bar_width + gap
    group_starts = np.arange(n_groups) * group_width
    offsets = [group_starts + i * bar_width for i in range(n_bars)]
    tick_centers = group_starts + (n_bars * bar_width) / 2
    return offsets, tick_centers, group_width


def save(fig, out_dir: str, name: str, display_inline: bool = False):
    if display_inline:
        plt.show()
    else:
        path = os.path.join(out_dir, name)
        fig.savefig(path, dpi=150, bbox_inches="tight")
        print(f"  Saved {path}")
    plt.close(fig)


def bf_color_legend_handles():
    return [mpatches.Patch(color=c, label=BF_LABELS[k]) for k, c in zip(BF_KEYS, BF_COLORS)]


# ---------------------------------------------------------------------------
# Chart 1: Pruning (Read)
# ---------------------------------------------------------------------------
def plot_pruning_read_row_groups(data: dict, out_dir: str, display_inline: bool = False, ax=None):
    """Stacked bar showing skipped vs read row groups."""
    sizes = data["dataset_sizes"]
    offsets, ticks, _ = group_positions(len(sizes), 3)
    bar_width = 0.22

    own_fig = ax is None
    if own_fig:
        fig, ax = plt.subplots(figsize=(10, 5))
    legend_handles = []

    for i, (key, color) in enumerate(zip(BF_KEYS, BF_COLORS)):
        rows    = data["pruning_read"][key]
        totals  = np.array([r["total_row_groups"]  for r in rows], dtype=float)
        skipped = np.array([r["skipped_row_groups"] for r in rows], dtype=float)
        read_rg = totals - skipped

        ax.bar(offsets[i], skipped, bar_width, color=color)
        ax.bar(offsets[i], read_rg, bar_width, bottom=skipped,
               color=color, alpha=STACK_ALPHA_LIGHT)
        legend_handles.append(mpatches.Patch(color=color, label=BF_LABELS[key]))

    skip_patch = mpatches.Patch(facecolor="dimgray",                       label="Skipped (pruned)")
    read_patch = mpatches.Patch(facecolor="dimgray", alpha=STACK_ALPHA_LIGHT, label="Read (not pruned)")

    color_legend = ax.legend(handles=legend_handles,         loc="upper left",   title="Bloom Filter Type")
    ax.add_artist(color_legend)
    ax.legend(         handles=[skip_patch, read_patch], loc="upper center", title="Bar Segments")

    ax.set_xticks(ticks)
    ax.set_xticklabels(sizes)
    ax.set_xlabel("Dataset Size")
    ax.set_ylabel("Row Groups")
    ax.set_title("Pruning – Row Groups Read vs Skipped (Read Path)")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
    if own_fig:
        fig.tight_layout()
        save(fig, out_dir, "1_pruning_read.png", display_inline=display_inline)


def plot_pruning_read_datafiles(data: dict, out_dir: str, display_inline: bool = False, ax=None):
    """Stacked bar showing skipped vs read data files."""
    sizes = data["dataset_sizes"]
    offsets, ticks, _ = group_positions(len(sizes), 3)
    bar_width = 0.22

    own_fig = ax is None
    if own_fig:
        fig, ax = plt.subplots(figsize=(10, 5))
    legend_handles = []

    for i, (key, color) in enumerate(zip(BF_KEYS, BF_COLORS)):
        rows    = data["pruning_read"][key]
        totals  = np.array([r["total_data_files"]   for r in rows], dtype=float)
        skipped = np.array([r["skipped_data_files"] for r in rows], dtype=float)
        read_df = totals - skipped

        ax.bar(offsets[i], skipped, bar_width, color=color)
        ax.bar(offsets[i], read_df, bar_width, bottom=skipped,
               color=color, alpha=STACK_ALPHA_LIGHT)
        legend_handles.append(mpatches.Patch(color=color, label=BF_LABELS[key]))

    skip_patch = mpatches.Patch(facecolor="dimgray",                       label="Skipped (pruned)")
    read_patch = mpatches.Patch(facecolor="dimgray", alpha=STACK_ALPHA_LIGHT, label="Read (not pruned)")

    color_legend = ax.legend(handles=legend_handles,         loc="upper left",   title="Bloom Filter Type")
    ax.add_artist(color_legend)
    ax.legend(         handles=[skip_patch, read_patch], loc="upper center", title="Bar Segments")

    ax.set_xticks(ticks)
    ax.set_xticklabels(sizes)
    ax.set_xlabel("Dataset Size")
    ax.set_ylabel("Data Files")
    ax.set_title("Pruning – Data Files Read vs Skipped (Read Path)")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
    if own_fig:
        fig.tight_layout()
        save(fig, out_dir, "1b_pruning_read_datafiles.png", display_inline=display_inline)


# ---------------------------------------------------------------------------
# Chart 2: Disk Storage
# ---------------------------------------------------------------------------
def _bytes_to_mb(b: float) -> float:
    return b / (1024 ** 2)


def plot_disk_storage(data: dict, out_dir: str, display_inline: bool = False, ax=None):
    """Grouped bar chart: disk usage by file type (manifest, data files, puffin) x bloom filter mode.

    Each dataset size has 3 groups (one per file type); each group has 3 bars (one per bloom mode).
    This makes it easy to see that only the Puffin column differs significantly across bloom modes.
    """
    sizes = data["dataset_sizes"]
    file_types = ["manifest", "data", "puffin"]
    file_labels = {"manifest": "Manifest", "data": "Data Files", "puffin": "Puffin"}

    # n_groups = len(sizes) * len(file_types), 3 bars per group (one per BF mode)
    n_groups = len(sizes) * len(file_types)
    offsets, ticks, group_width = group_positions(n_groups, 3)
    bar_width = 0.22

    own_fig = ax is None
    if own_fig:
        fig, ax = plt.subplots(figsize=(12, 5))

    for i, (key, color) in enumerate(zip(BF_KEYS, BF_COLORS)):
        rows = data["disk_storage_bytes"][key]
        values = []
        for size_idx in range(len(sizes)):
            r = rows[size_idx]
            values.append(_bytes_to_mb(r.get("manifest_bytes", r.get("manifest_overhead_bytes", 0))))
            values.append(_bytes_to_mb(r.get("data_bytes", 0)))
            values.append(_bytes_to_mb(r.get("puffin_bytes", 0)))
        ax.bar(offsets[i], np.array(values), bar_width, color=color)

    # Tick labels: "small\nManifest", "small\nData Files", "small\nPuffin", "large\n..." etc.
    tick_labels = [f"{s}\n{file_labels[ft]}" for s in sizes for ft in file_types]

    # Dashed divider between dataset size groups
    for j in range(1, len(sizes)):
        boundary = (ticks[j * len(file_types) - 1] + ticks[j * len(file_types)]) / 2
        ax.axvline(boundary, color="gray", linewidth=0.8, linestyle="--", alpha=0.5)

    ax.legend(handles=bf_color_legend_handles(), title="Bloom Filter Type")
    ax.set_xticks(ticks)
    ax.set_xticklabels(tick_labels)
    ax.set_xlabel("Dataset Size / File Type")
    ax.set_ylabel("Disk Usage (MB)")
    ax.set_title("Disk Storage by File Type")
    if own_fig:
        fig.tight_layout()
        save(fig, out_dir, "2_disk_storage.png", display_inline=display_inline)


# ---------------------------------------------------------------------------
# Chart 3: Memory Usage (Read)
# ---------------------------------------------------------------------------
def plot_memory_read(data: dict, out_dir: str, display_inline: bool = False, ax=None):
    """Peak memory per bloom filter type."""
    sizes = data["dataset_sizes"]
    offsets, ticks, _ = group_positions(len(sizes), 3)
    bar_width = 0.22

    own_fig = ax is None
    if own_fig:
        fig, ax = plt.subplots(figsize=(10, 5))

    for i, (key, color) in enumerate(zip(BF_KEYS, BF_COLORS)):
        values = np.array(data["memory_read_mb"][key], dtype=float)
        ax.bar(offsets[i], values, bar_width, color=color)

    ax.legend(handles=bf_color_legend_handles(), title="Bloom Filter Type")
    ax.set_xticks(ticks)
    ax.set_xticklabels(sizes)
    ax.set_xlabel("Dataset Size")
    ax.set_ylabel("Peak Memory Usage (MB)")
    ax.set_title("Peak Memory Usage – Read Path")
    if own_fig:
        fig.tight_layout()
        save(fig, out_dir, "3_memory_read.png", display_inline=display_inline)


# ---------------------------------------------------------------------------
# Chart 4: Memory Usage (Write) – Write Memory Breakdown
# ---------------------------------------------------------------------------
def plot_memory_write(data: dict, out_dir: str, display_inline: bool = False, ax=None):
    """Stacked bar: data write + puffin write memory, similar to write time breakdown.
    Bottom = data write, top = puffin write. Color per bloom filter type."""
    sizes = data["dataset_sizes"]
    offsets, ticks, _ = group_positions(len(sizes), 3)
    bar_width = 0.22

    own_fig = ax is None
    if own_fig:
        fig, ax = plt.subplots(figsize=(10, 5))
    legend_handles = []

    for i, (key, color) in enumerate(zip(BF_KEYS, BF_COLORS)):
        rows = data["memory_write_mb"][key]
        # Support both dict (data_mb, puffin_mb) and legacy float (total only)
        def _get(v, k):
            return v.get(k, 0) if isinstance(v, dict) else (v if k == "data_mb" else 0)
        data_mb = np.array([_get(r, "data_mb") for r in rows], dtype=float)
        puffin_mb = np.array([_get(r, "puffin_mb") for r in rows], dtype=float)

        ax.bar(offsets[i], data_mb, bar_width, color=color)
        ax.bar(offsets[i], puffin_mb, bar_width, bottom=data_mb,
               color=color, alpha=STACK_ALPHA_LIGHT)
        legend_handles.append(mpatches.Patch(color=color, label=BF_LABELS[key]))

    data_patch = mpatches.Patch(facecolor="dimgray",                       label="Data write")
    puffin_patch = mpatches.Patch(facecolor="dimgray", alpha=STACK_ALPHA_LIGHT, label="Puffin write")

    color_legend = ax.legend(handles=legend_handles,         loc="upper left",   title="Bloom Filter Type")
    ax.add_artist(color_legend)
    ax.legend(         handles=[data_patch, puffin_patch], loc="upper center", title="Bar Segments")

    ax.set_xticks(ticks)
    ax.set_xticklabels(sizes)
    ax.set_xlabel("Dataset Size")
    ax.set_ylabel("Peak Memory Usage (MB)")
    ax.set_title("Peak Memory Usage – Write Path")
    if own_fig:
        fig.tight_layout()
        save(fig, out_dir, "4_memory_write.png", display_inline=display_inline)


# ---------------------------------------------------------------------------
# Chart 5: Time (Read)
# ---------------------------------------------------------------------------
def plot_time_read(data: dict, out_dir: str, display_inline: bool = False, ax=None):
    """Stacked bar: metadata + puffin + data read time."""
    sizes = data["dataset_sizes"]
    offsets, ticks, _ = group_positions(len(sizes), 3)
    bar_width = 0.22

    own_fig = ax is None
    if own_fig:
        fig, ax = plt.subplots(figsize=(10, 5))

    has_other = False
    for i, key in enumerate(BF_KEYS):
        rows = data["time_read_ms"][key]
        meta_s = np.array([r.get("metadata_ms", 0) for r in rows], dtype=float) / 1000
        puffin_s = np.array([r.get("puffin_ms", 0) for r in rows], dtype=float) / 1000
        data_s = np.array([r.get("data_ms", 0) for r in rows], dtype=float) / 1000
        total_s = np.array(
            [r.get("total_ms") if r.get("total_ms") is not None else (meta_s[j] + puffin_s[j] + data_s[j]) * 1000
             for j, r in enumerate(rows)],
            dtype=float,
        ) / 1000
        stack_sum = meta_s + puffin_s + data_s
        other_s = np.maximum(0, total_s - stack_sum)
        if np.any(other_s > 0):
            has_other = True

        ax.bar(offsets[i], meta_s, bar_width, color=STACK_COLORS["metadata"])
        ax.bar(offsets[i], puffin_s, bar_width, bottom=meta_s, color=STACK_COLORS["puffin"])
        ax.bar(offsets[i], data_s, bar_width, bottom=meta_s + puffin_s, color=STACK_COLORS["data"])
        if np.any(other_s > 0):
            ax.bar(
                offsets[i], other_s, bar_width,
                bottom=meta_s + puffin_s + data_s,
                color="lightgray",
            )

        hatch = ["", "//", "xx"][i]
        for container in ax.containers[-3:]:
            for bar in container:
                bar.set_hatch(hatch)
                bar.set_edgecolor("white")

    seg_patches = [
        mpatches.Patch(color=STACK_COLORS["metadata"], label="Planning + Metadata read"),
        mpatches.Patch(color=STACK_COLORS["puffin"],   label="Puffin file read"),
        mpatches.Patch(color=STACK_COLORS["data"],     label="Data file read"),
    ]
    if has_other:
        seg_patches.append(mpatches.Patch(color="lightgray", label="Other (total read duration)"))
    hatch_patches = [
        mpatches.Patch(facecolor="lightgrey", hatch="",   edgecolor="grey", label=BF_LABELS["no_bloom_filter"]),
        mpatches.Patch(facecolor="lightgrey", hatch="//", edgecolor="grey", label=BF_LABELS["row_group_bloom_filter"]),
        mpatches.Patch(facecolor="lightgrey", hatch="xx", edgecolor="grey", label=BF_LABELS["file_level_bloom_filter"]),
    ]

    seg_legend = ax.legend(handles=seg_patches,    loc="upper left",   title="Time Components")
    ax.add_artist(seg_legend)
    ax.legend(             handles=hatch_patches,  loc="upper center", title="Bloom Filter Type")

    ax.set_xticks(ticks)
    ax.set_xticklabels(sizes)
    ax.set_xlabel("Dataset Size")
    ax.set_ylabel("Time (seconds)")
    ax.set_title("Read Time Breakdown")
    if own_fig:
        fig.tight_layout()
        save(fig, out_dir, "5_time_read.png", display_inline=display_inline)


# ---------------------------------------------------------------------------
# Chart 6: Time (Write) – Write Time Breakdown
# ---------------------------------------------------------------------------
def plot_time_write(data: dict, out_dir: str, display_inline: bool = False, ax=None):
    """Stacked bar: data write + puffin write time, similar to pruning datafiles.
    Bottom = dataWriteDuration, top = puffinWriteDuration. Color per bloom filter type."""
    sizes = data["dataset_sizes"]
    offsets, ticks, _ = group_positions(len(sizes), 3)
    bar_width = 0.22

    own_fig = ax is None
    if own_fig:
        fig, ax = plt.subplots(figsize=(10, 5))
    legend_handles = []

    for i, (key, color) in enumerate(zip(BF_KEYS, BF_COLORS)):
        rows = data["time_write_ms"][key]
        data_s = np.array([r.get("data_ms", 0) for r in rows], dtype=float) / 1000
        puffin_s = np.array([r.get("puffin_ms", 0) for r in rows], dtype=float) / 1000

        ax.bar(offsets[i], data_s, bar_width, color=color)
        ax.bar(offsets[i], puffin_s, bar_width, bottom=data_s,
               color=color, alpha=STACK_ALPHA_LIGHT)
        legend_handles.append(mpatches.Patch(color=color, label=BF_LABELS[key]))

    data_patch = mpatches.Patch(facecolor="dimgray",                       label="Data write")
    puffin_patch = mpatches.Patch(facecolor="dimgray", alpha=STACK_ALPHA_LIGHT, label="Puffin write")

    color_legend = ax.legend(handles=legend_handles,         loc="upper left",   title="Bloom Filter Type")
    ax.add_artist(color_legend)
    ax.legend(         handles=[data_patch, puffin_patch], loc="upper center", title="Bar Segments")

    ax.set_xticks(ticks)
    ax.set_xticklabels(sizes)
    ax.set_xlabel("Dataset Size")
    ax.set_ylabel("Time (seconds)")
    ax.set_title("Write Time Breakdown")
    if own_fig:
        fig.tight_layout()
        save(fig, out_dir, "6_time_write.png", display_inline=display_inline)


# ---------------------------------------------------------------------------
# Grid layout: all charts in one figure
# ---------------------------------------------------------------------------
def plot_all_grid(data: dict, out_dir: str, display_inline: bool = False):
    """Plot all charts in a grid layout:
    Row 1: read_row_groups, read_datafiles
    Row 2: time_write, time_read
    Row 3: memory_write, memory_read
    Row 4: disk_storage (full width)
    """
    from matplotlib.gridspec import GridSpec

    fig = plt.figure(figsize=(14, 16))
    gs = GridSpec(4, 2, figure=fig, hspace=0.4, wspace=0.3)

    # Row 1: read_row_groups, read_datafiles
    plot_pruning_read_row_groups(data, out_dir, ax=fig.add_subplot(gs[0, 0]))
    plot_pruning_read_datafiles(data, out_dir, ax=fig.add_subplot(gs[0, 1]))

    # Row 2: time_write, time_read
    plot_time_write(data, out_dir, ax=fig.add_subplot(gs[1, 0]))
    plot_time_read(data, out_dir, ax=fig.add_subplot(gs[1, 1]))

    # Row 3: memory_write, memory_read
    plot_memory_write(data, out_dir, ax=fig.add_subplot(gs[2, 0]))
    plot_memory_read(data, out_dir, ax=fig.add_subplot(gs[2, 1]))

    # Row 4: disk_storage (full width)
    plot_disk_storage(data, out_dir, ax=fig.add_subplot(gs[3, :]))

    fig.tight_layout()
    save(fig, out_dir, "all_grid.png", display_inline=display_inline)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate all bloom filter evaluation charts.")
    parser.add_argument("--data", default=DEFAULT_DATA)
    parser.add_argument("--out",  default=DEFAULT_OUT)
    args = parser.parse_args()

    with open(args.data) as f:
        data = json.load(f)

    os.makedirs(args.out, exist_ok=True)
    print(f"Generating graphs → {args.out}/")

    plot_pruning_read_row_groups(data, args.out)
    plot_pruning_read_datafiles(data, args.out)
    plot_disk_storage(data, args.out)
    plot_memory_read(data, args.out)
    plot_memory_write(data, args.out)
    plot_time_read(data, args.out)
    plot_time_write(data, args.out)

    print("Done.")


if __name__ == "__main__":
    main()
