"""Bloom filter evaluation graphs - shared constants, helpers, and plot functions."""

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

EDGE_COLOR = "white"
LINEWIDTH = 0.5

BF_KEYS   = list(BF_LABELS.keys())
BF_COLORS = [COLORS["no_bf"], COLORS["rg_bf"], COLORS["file_bf"]]

STACK_ALPHA_LIGHT = 0.35  # lightest shade for the portion of stacked bars
STACK_ALPHA_MEDIUM = 0.55  # medium shade
STACK_ALPHA_DARK = 0.75  # darker shade for 4-segment stacks

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


def save(fig, out_dir: str, name: str, display_inline: bool = False, dataset_size: str = None):
    if display_inline:
        plt.show()
    else:
        base, ext = os.path.splitext(name)
        filename = f"{base}_{dataset_size}{ext}" if dataset_size else name
        path = os.path.join(out_dir, filename)
        fig.savefig(path, dpi=150, bbox_inches="tight")
        print(f"  Saved {path}")
    plt.close(fig)


def filter_data_by_size(data: dict, size: str) -> dict:
    """Return a copy of data filtered to a single dataset size."""
    if size not in data["dataset_sizes"]:
        raise ValueError(f"Unknown dataset size: {size}. Available: {data['dataset_sizes']}")
    idx = data["dataset_sizes"].index(size)
    filtered = {
        "dataset_sizes": [size],
        "pruning_read": {k: [v[idx]] for k, v in data["pruning_read"].items()},
        "disk_storage_bytes": {k: [v[idx]] for k, v in data["disk_storage_bytes"].items()},
        "memory_read_mb": {k: [v[idx]] for k, v in data["memory_read_mb"].items()},
        "memory_write_mb": {k: [v[idx]] for k, v in data["memory_write_mb"].items()},
        "time_read_ms": {k: [v[idx]] for k, v in data["time_read_ms"].items()},
        "time_write_ms": {k: [v[idx]] for k, v in data["time_write_ms"].items()},
    }
    return filtered


def bf_color_legend_handles():
    return [mpatches.Patch(color=c, label=BF_LABELS[k]) for k, c in zip(BF_KEYS, BF_COLORS)]


# ---------------------------------------------------------------------------
# Chart 1: Pruning (Read)
# ---------------------------------------------------------------------------
def plot_pruning_read_row_groups(data: dict, out_dir: str, display_inline: bool = False, ax=None, dataset_size: str = None):
    """Stacked bar: height = total row groups (from write). Segments (bottom to top):
    Read (instrumented), Row-group BF skips, File-level BF skips, Other (= total - read - rg_bf - file_bf)."""
    sizes = data["dataset_sizes"]
    offsets, ticks, _ = group_positions(len(sizes), 3)
    bar_width = 0.22

    own_fig = ax is None
    if own_fig:
        fig, ax = plt.subplots(figsize=(10, 5))
    legend_handles = []

    for i, (key, color) in enumerate(zip(BF_KEYS, BF_COLORS)):
        rows = data["pruning_read"][key]
        totals = np.array([r["total_row_groups"] for r in rows], dtype=float)
        read_row_groups = np.array([r.get("row_groups_read") for r in rows], dtype=float)
        # read_row_groups = totals - skipped_rg - file_bf_skipped_rg
        # assert all(totals >= skipped_rg + file_bf_skipped_rg), (
        #     f"Note: total row groups < row_group_bf_skipped + file_bf_skipped for {key}"
        # )
        skipped_rg = np.array(
            [r.get("skipped_row_groups", r.get("all_skipped_row_groups", 0)) for r in rows],
            dtype=float,
        )
        file_bf_skipped_rg = np.array(
            [r.get("row_groups_skipped_by_file_bloom_filter", 0) for r in rows],
            dtype=float,
        )
        other_rg = totals - read_row_groups - skipped_rg - file_bf_skipped_rg
        assert all(other_rg >= 0), f"Note: other row groups < 0 for {key}"

        ax.bar(offsets[i], read_row_groups, bar_width, color=color, edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
        ax.bar(offsets[i], skipped_rg, bar_width, bottom=read_row_groups,
               color=color, alpha=STACK_ALPHA_DARK, edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
        ax.bar(offsets[i], file_bf_skipped_rg, bar_width, bottom=read_row_groups + skipped_rg,
               color=color, alpha=STACK_ALPHA_MEDIUM, edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
        ax.bar(offsets[i], other_rg, bar_width, bottom=read_row_groups + skipped_rg + file_bf_skipped_rg,
               color=color, alpha=STACK_ALPHA_LIGHT, edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
        legend_handles.append(mpatches.Patch(color=color, label=BF_LABELS[key]))

    read_patch = mpatches.Patch(facecolor="dimgray", label="Read")
    skip_rg_patch = mpatches.Patch(facecolor="dimgray", alpha=STACK_ALPHA_DARK, label="Skipped (row-group BF)")
    skip_bf_patch = mpatches.Patch(facecolor="dimgray", alpha=STACK_ALPHA_MEDIUM, label="Skipped (file-level BF)")
    other_patch = mpatches.Patch(facecolor="dimgray", alpha=STACK_ALPHA_LIGHT, label="Other (e.g. manifest)")

    color_legend = ax.legend(handles=legend_handles, loc="upper left", title="Bloom Filter Type")
    ax.add_artist(color_legend)
    ax.legend(handles=[read_patch, skip_rg_patch, skip_bf_patch, other_patch],
              loc="upper center", title="Bar Segments")

    ax.set_xticks(ticks)
    ax.set_xticklabels(sizes)
    ax.set_xlabel("Dataset Size")
    ax.set_ylabel("Row Groups")
    ax.set_title("Pruning - Row Groups Read vs Skipped (Read Path)")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
    if own_fig:
        fig.tight_layout()
        save(fig, out_dir, "1_pruning_read.png", display_inline=display_inline, dataset_size=dataset_size)


def plot_pruning_read_datafiles(data: dict, out_dir: str, display_inline: bool = False, ax=None, dataset_size: str = None):
    """Stacked bar: total height = totalDataFiles. Bottom = read, middle = skipped (manifest), top = bloom filter skipped.
    readDataFiles = total - manifestSkipped - bloomFilterSkipped (mutually exclusive phases)."""
    sizes = data["dataset_sizes"]
    offsets, ticks, _ = group_positions(len(sizes), 3)
    bar_width = 0.22

    own_fig = ax is None
    if own_fig:
        fig, ax = plt.subplots(figsize=(10, 5))
    legend_handles = []

    for i, (key, color) in enumerate(zip(BF_KEYS, BF_COLORS)):
        rows = data["pruning_read"][key]
        total = np.array([r["total_data_files"] for r in rows], dtype=float)
        manifest_skipped = np.array(
            [r.get("manifest_skipped_data_files", r.get("all_skipped_data_files", r.get("skipped_data_files", 0))) for r in rows],  # TODO: revisit which one is the actual name
            dtype=float,
        )
        bloom_skipped = np.array([r.get("bloom_filter_skipped_data_files", 0) for r in rows], dtype=float)
        # readDataFiles = total - manifestSkipped - bloomFilterSkipped (mutually exclusive phases)
        read_df = np.maximum(0, total - manifest_skipped - bloom_skipped)
        other_skipped = manifest_skipped  # manifest-level skips (partition/stats)

        ax.bar(offsets[i], read_df, bar_width, color=color, edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
        ax.bar(offsets[i], other_skipped, bar_width, bottom=read_df,
               color=color, alpha=STACK_ALPHA_MEDIUM, edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
        ax.bar(offsets[i], bloom_skipped, bar_width, bottom=read_df + other_skipped,
               color=color, alpha=STACK_ALPHA_LIGHT, edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
        legend_handles.append(mpatches.Patch(color=color, label=BF_LABELS[key]))

    read_patch = mpatches.Patch(facecolor="dimgray",                       label="Read")
    other_patch = mpatches.Patch(facecolor="dimgray", alpha=STACK_ALPHA_MEDIUM, label="Skipped (manifest)")
    bloom_patch = mpatches.Patch(facecolor="dimgray", alpha=STACK_ALPHA_LIGHT, label="Skipped (bloom filter)")

    color_legend = ax.legend(handles=legend_handles,         loc="upper left",   title="Bloom Filter Type")
    ax.add_artist(color_legend)
    ax.legend(handles=[read_patch, other_patch, bloom_patch], loc="upper center", title="Bar Segments")

    ax.set_xticks(ticks)
    ax.set_xticklabels(sizes)
    ax.set_xlabel("Dataset Size")
    ax.set_ylabel("Data Files")
    ax.set_title("Pruning - Data Files Read vs Skipped (Read Path)")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
    if own_fig:
        fig.tight_layout()
        save(fig, out_dir, "1b_pruning_read_datafiles.png", display_inline=display_inline, dataset_size=dataset_size)


# ---------------------------------------------------------------------------
# Chart 2: Disk Storage
# ---------------------------------------------------------------------------
def _bytes_to_mb(b: float) -> float:
    return b / (1024 ** 2)


def plot_disk_storage(data: dict, out_dir: str, display_inline: bool = False, ax=None, dataset_size: str = None):
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
        ax.bar(offsets[i], np.array(values), bar_width, color=color, edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)

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
        save(fig, out_dir, "2_disk_storage.png", display_inline=display_inline, dataset_size=dataset_size)


# ---------------------------------------------------------------------------
# Chart 3: Memory Usage (Read) - Read Memory Breakdown
# ---------------------------------------------------------------------------
def plot_memory_read(data: dict, out_dir: str, display_inline: bool = False, ax=None, dataset_size: str = None):
    """Stacked bar: puffin read (subset) + rest of query. Total height = maxMemoryUsage.
    Bottom = readPuffinMaxMemory, top = maxMemoryUsage - readPuffinMaxMemory."""
    sizes = data["dataset_sizes"]
    offsets, ticks, _ = group_positions(len(sizes), 3)
    bar_width = 0.22

    own_fig = ax is None
    if own_fig:
        fig, ax = plt.subplots(figsize=(10, 5))
    legend_handles = []

    for i, (key, color) in enumerate(zip(BF_KEYS, BF_COLORS)):
        rows = data["memory_read_mb"][key]
        # Support both dict (max_mb, puffin_mb) and legacy float (total only)
        def _get(v, k):
            return v.get(k, 0) if isinstance(v, dict) else (v if k == "max_mb" else 0)
        max_mb = np.array([_get(r, "max_mb") for r in rows], dtype=float)
        puffin_mb = np.array([_get(r, "puffin_mb") for r in rows], dtype=float)
        # Puffin is a subset of max; rest = max - puffin (clamp to avoid negatives)
        rest_mb = np.maximum(0, max_mb - puffin_mb)

        ax.bar(offsets[i], puffin_mb, bar_width, color=color, edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
        ax.bar(offsets[i], rest_mb, bar_width, bottom=puffin_mb,
               color=color, alpha=STACK_ALPHA_LIGHT, edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
        legend_handles.append(mpatches.Patch(color=color, label=BF_LABELS[key]))

    puffin_patch = mpatches.Patch(facecolor="dimgray",                       label="Puffin")
    rest_patch = mpatches.Patch(facecolor="dimgray", alpha=STACK_ALPHA_LIGHT, label="Total")

    color_legend = ax.legend(handles=legend_handles,         loc="upper left",   title="Bloom Filter Type")
    ax.add_artist(color_legend)
    ax.legend(         handles=[puffin_patch, rest_patch], loc="upper center", title="Bar Segments")

    ax.set_xticks(ticks)
    ax.set_xticklabels(sizes)
    ax.set_xlabel("Dataset Size")
    ax.set_ylabel("Peak Memory Usage (MB)")
    ax.set_title("Peak Memory Usage - Read Path")
    if own_fig:
        fig.tight_layout()
        save(fig, out_dir, "3_memory_read.png", display_inline=display_inline, dataset_size=dataset_size)


# ---------------------------------------------------------------------------
# Chart 4: Memory Usage (Write) - Write Memory Breakdown
# ---------------------------------------------------------------------------
def plot_memory_write(data: dict, out_dir: str, display_inline: bool = False, ax=None, dataset_size: str = None):
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

        ax.bar(offsets[i], data_mb, bar_width, color=color, edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
        ax.bar(offsets[i], puffin_mb, bar_width, bottom=data_mb,
               color=color, alpha=STACK_ALPHA_LIGHT, edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
        legend_handles.append(mpatches.Patch(color=color, label=BF_LABELS[key]))

    data_patch = mpatches.Patch(facecolor="dimgray",                       label="Total")
    puffin_patch = mpatches.Patch(facecolor="dimgray", alpha=STACK_ALPHA_LIGHT, label="Puffin")

    color_legend = ax.legend(handles=legend_handles,         loc="upper left",   title="Bloom Filter Type")
    ax.add_artist(color_legend)
    ax.legend(         handles=[data_patch, puffin_patch], loc="upper center", title="Bar Segments")

    ax.set_xticks(ticks)
    ax.set_xticklabels(sizes)
    ax.set_xlabel("Dataset Size")
    ax.set_ylabel("Peak Memory Usage (MB)")
    ax.set_title("Peak Memory Usage - Write Path")
    if own_fig:
        fig.tight_layout()
        save(fig, out_dir, "4_memory_write.png", display_inline=display_inline, dataset_size=dataset_size)


# ---------------------------------------------------------------------------
# Chart 5: Time (Read) - Read Time Breakdown
# ---------------------------------------------------------------------------
def plot_time_read(data: dict, out_dir: str, display_inline: bool = False, ax=None, dataset_size: str = None):
    """Stacked bar: totalReadDuration as height, readPuffinDuration as subset (bottom).
    Bottom = puffin read, top = rest of query. Color per bloom filter type."""
    sizes = data["dataset_sizes"]
    offsets, ticks, _ = group_positions(len(sizes), 3)
    bar_width = 0.22

    own_fig = ax is None
    if own_fig:
        fig, ax = plt.subplots(figsize=(10, 5))
    legend_handles = []

    for i, (key, color) in enumerate(zip(BF_KEYS, BF_COLORS)):
        rows = data["time_read_ms"][key]
        total_s = np.array([r.get("total_ms") or 0 for r in rows], dtype=float) / 1000
        puffin_s = np.array([r.get("puffin_ms", 0) for r in rows], dtype=float) / 1000
        rest_s = np.maximum(0, total_s - puffin_s)

        # Match write time breakdown: bottom = Total (rest), top = Puffin (light)
        ax.bar(offsets[i], rest_s, bar_width, color=color, edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
        ax.bar(offsets[i], puffin_s, bar_width, bottom=rest_s,
               color=color, alpha=STACK_ALPHA_LIGHT, edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
        legend_handles.append(mpatches.Patch(color=color, label=BF_LABELS[key]))

    total_patch = mpatches.Patch(facecolor="dimgray",                       label="Total")
    puffin_patch = mpatches.Patch(facecolor="dimgray", alpha=STACK_ALPHA_LIGHT, label="Puffin")

    color_legend = ax.legend(handles=legend_handles,         loc="upper left",   title="Bloom Filter Type")
    ax.add_artist(color_legend)
    ax.legend(         handles=[total_patch, puffin_patch], loc="upper center", title="Bar Segments")

    ax.set_xticks(ticks)
    ax.set_xticklabels(sizes)
    ax.set_xlabel("Dataset Size")
    ax.set_ylabel("Time (seconds)")
    ax.set_title("Read Time Breakdown")
    if own_fig:
        fig.tight_layout()
        save(fig, out_dir, "5_time_read.png", display_inline=display_inline, dataset_size=dataset_size)


# ---------------------------------------------------------------------------
# Chart 6: Time (Write) - Write Time Breakdown
# ---------------------------------------------------------------------------
def plot_time_write(data: dict, out_dir: str, display_inline: bool = False, ax=None, dataset_size: str = None):
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

        ax.bar(offsets[i], data_s, bar_width, color=color, edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
        ax.bar(offsets[i], puffin_s, bar_width, bottom=data_s,
               color=color, alpha=STACK_ALPHA_LIGHT, edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
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
        save(fig, out_dir, "6_time_write.png", display_inline=display_inline, dataset_size=dataset_size)


# ---------------------------------------------------------------------------
# Grid layout: all charts in one figure
# ---------------------------------------------------------------------------
def plot_all_grid(data: dict, out_dir: str, display_inline: bool = False, dataset_size: str = None):
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
    plot_pruning_read_row_groups(data, out_dir, ax=fig.add_subplot(gs[0, 0]), dataset_size=dataset_size)
    plot_pruning_read_datafiles(data, out_dir, ax=fig.add_subplot(gs[0, 1]), dataset_size=dataset_size)

    # Row 2: time_write, time_read
    plot_time_write(data, out_dir, ax=fig.add_subplot(gs[1, 0]), dataset_size=dataset_size)
    plot_time_read(data, out_dir, ax=fig.add_subplot(gs[1, 1]), dataset_size=dataset_size)

    # Row 3: memory_write, memory_read
    plot_memory_write(data, out_dir, ax=fig.add_subplot(gs[2, 0]), dataset_size=dataset_size)
    plot_memory_read(data, out_dir, ax=fig.add_subplot(gs[2, 1]), dataset_size=dataset_size)

    # Row 4: disk_storage (full width)
    plot_disk_storage(data, out_dir, ax=fig.add_subplot(gs[3, :]), dataset_size=dataset_size)

    fig.tight_layout()
    save(fig, out_dir, "all_grid.png", display_inline=display_inline, dataset_size=dataset_size)


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
    sizes = data["dataset_sizes"]

    # Generate separate plot sets for each dataset size
    for size in sizes:
        print(f"\nGenerating graphs for '{size}' dataset → {args.out}/")
        filtered = filter_data_by_size(data, size)
        plot_pruning_read_row_groups(filtered, args.out, dataset_size=size)
        plot_pruning_read_datafiles(filtered, args.out, dataset_size=size)
        plot_disk_storage(filtered, args.out, dataset_size=size)
        plot_memory_read(filtered, args.out, dataset_size=size)
        plot_memory_write(filtered, args.out, dataset_size=size)
        plot_time_read(filtered, args.out, dataset_size=size)
        plot_time_write(filtered, args.out, dataset_size=size)
        plot_all_grid(filtered, args.out, dataset_size=size)

    print("\nDone.")


if __name__ == "__main__":
    main()
