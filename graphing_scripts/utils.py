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

# Segment colors for stacked bars: same color per segment across all bars
# Row groups: Read, Skipped (row-group BF), Skipped (file-level BF), Other
SEGMENT_COLORS_4 = ["#1565c0", "#ff8f00", "#2e7d32", "#7b1fa2"]  # blue, amber, green, purple
# Datafiles: Read, Skipped (manifest), Skipped (bloom filter)
SEGMENT_COLORS_3 = ["#1565c0", "#ff8f00", "#2e7d32"]
# Two segments: main, puffin/secondary
SEGMENT_COLORS_2 = ["#1565c0", "#00838f"]  # blue, cyan

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


def bar_positions_3_by_bf(bar_width: float = 0.28, gap: float = 0.12):
    """For 3 bars (one per bloom filter type): return (x_positions, tick_centers, tick_labels).
    Use when x-axis should label each bar by bloom filter type."""
    step = bar_width + gap
    x_positions = np.array([0, step, 2 * step])
    tick_centers = x_positions + bar_width / 2
    tick_labels = [BF_LABELS[k] for k in BF_KEYS]
    return x_positions, tick_centers, tick_labels, bar_width


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
    Read (instrumented), Row-group BF skips, File-level BF skips, Other (= total - read - rg_bf - file_bf).
    All bars use same segment colors; x-axis labels = bloom filter type."""
    x_pos, ticks, tick_labels, bar_width = bar_positions_3_by_bf()
    c = SEGMENT_COLORS_4

    own_fig = ax is None
    if own_fig:
        fig, ax = plt.subplots(figsize=(10, 5))

    for i, key in enumerate(BF_KEYS):
        rows = data["pruning_read"][key]
        totals = np.array([r["total_row_groups"] for r in rows], dtype=float)
        read_rg = np.array([r.get("row_groups_read") for r in rows], dtype=float)
        skipped_rg = np.array(
            [r.get("skipped_row_groups", r.get("all_skipped_row_groups", 0)) for r in rows],
            dtype=float,
        )
        file_bf_skipped_rg = np.array(
            [r.get("row_groups_skipped_by_file_bloom_filter", 0) for r in rows],
            dtype=float,
        )
        # Only show row-group BF skips (green) for the row-group BF run; only show file-level BF
        # skips (amber) when that run uses file-level BF. No-BF and file-level-BF bars get 0
        # row-group skips; no-BF and row-group-BF bars get 0 file-level skips (unless we had both).
        if key == "no_bloom_filter":
            skipped_rg = np.zeros_like(skipped_rg)
            file_bf_skipped_rg = np.zeros_like(file_bf_skipped_rg)
        elif key == "file_level_bloom_filter":
            skipped_rg = np.zeros_like(skipped_rg)
        # row_group_bloom_filter: keep both as-is (file_bf_skipped_rg will typically be 0)
        other_rg = totals - read_rg - skipped_rg - file_bf_skipped_rg
        assert all(other_rg >= 0), f"Note: other row groups < 0 for {key}"

        r, s, f, o = read_rg[0], skipped_rg[0], file_bf_skipped_rg[0], other_rg[0]
        ax.bar(x_pos[i], r, bar_width, color=c[0], edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
        ax.bar(x_pos[i], f, bar_width, bottom=r, color=c[1], edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
        ax.bar(x_pos[i], s, bar_width, bottom=r + f, color=c[2], edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
        ax.bar(x_pos[i], o, bar_width, bottom=r + s + f, color=c[3], edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)

    legend_handles = [
        mpatches.Patch(color=c[3], label="Other (e.g. manifest)"),
        mpatches.Patch(color=c[2], label="Skipped (row-group BF)"),
        mpatches.Patch(color=c[1], label="Skipped (file-level BF)"),
        mpatches.Patch(color=c[0], label="Read"),
    ]
    ax.legend(handles=legend_handles, loc="upper left")

    ax.set_xticks(ticks)
    ax.set_xticklabels(tick_labels)
    ax.set_xlabel("Bloom Filter Type")
    ax.set_ylabel("Row Groups")
    ax.set_title("Pruning - Row Groups Read vs Skipped (Read Path)")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
    if own_fig:
        fig.tight_layout()
        save(fig, out_dir, "1_pruning_read.png", display_inline=display_inline, dataset_size=dataset_size)


def plot_pruning_read_datafiles(data: dict, out_dir: str, display_inline: bool = False, ax=None, dataset_size: str = None):
    """Stacked bar: total height = totalDataFiles. Bottom = read, middle = skipped (manifest), top = bloom filter skipped.
    All bars use same segment colors; x-axis labels = bloom filter type."""
    x_pos, ticks, tick_labels, bar_width = bar_positions_3_by_bf()
    c = SEGMENT_COLORS_3
    c[2] = SEGMENT_COLORS_4[3]  # swap this so that it matches the same color as the manifest color in the row-groups graph

    own_fig = ax is None
    if own_fig:
        fig, ax = plt.subplots(figsize=(10, 5))

    for i, key in enumerate(BF_KEYS):
        rows = data["pruning_read"][key]
        total = np.array([r["total_data_files"] for r in rows], dtype=float)
        manifest_skipped = np.array(
            [r.get("manifest_skipped_data_files", r.get("all_skipped_data_files", r.get("skipped_data_files", 0))) for r in rows],
            dtype=float,
        )
        bloom_skipped = np.array([r.get("bloom_filter_skipped_data_files", 0) for r in rows], dtype=float)
        read_df = np.maximum(0, total - manifest_skipped - bloom_skipped)
        rd, ms, bs = read_df[0], manifest_skipped[0], bloom_skipped[0]

        ax.bar(x_pos[i], rd, bar_width, color=c[0], edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
        ax.bar(x_pos[i], bs, bar_width, bottom=rd, color=c[1], edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
        ax.bar(x_pos[i], ms, bar_width, bottom=rd + bs, color=c[2], edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)

    legend_handles = [
        mpatches.Patch(color=c[2], label="Skipped (manifest)"),
        mpatches.Patch(color=c[1], label="Skipped (file bloom filter)"),
        mpatches.Patch(color=c[0], label="Read"),
    ]
    ax.legend(handles=legend_handles, loc="upper left")

    ax.set_xticks(ticks)
    ax.set_xticklabels(tick_labels)
    ax.set_xlabel("Bloom Filter Type")
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

    manifest_annotations = []  # (x, kb) pairs — collected for text labels after bars are drawn

    for i, (key, color) in enumerate(zip(BF_KEYS, BF_COLORS)):
        rows = data["disk_storage_bytes"][key]
        values = []
        for size_idx in range(len(sizes)):
            r = rows[size_idx]
            manifest_b = r.get("manifest_bytes", r.get("manifest_overhead_bytes", 0))
            values.append(_bytes_to_mb(manifest_b))
            values.append(_bytes_to_mb(r.get("data_bytes", 0)))
            values.append(_bytes_to_mb(r.get("puffin_bytes", 0)))
            manifest_annotations.append((offsets[i][size_idx * len(file_types)], manifest_b / 1024))
        ax.bar(offsets[i], np.array(values), bar_width, color=color, edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)

    # Manifest bars are invisible at MB scale (~0.007 MB); annotate each with its actual KB value
    for x, kb in manifest_annotations:
        ax.annotate(
            f"{kb:.1f} KB",
            xy=(x, 0),
            xytext=(0, 4),
            textcoords="offset points",
            ha="center", va="bottom",
            fontsize=8, rotation=90, color="#444444",
        )

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
    """Clustered bar chart: Puffin and Rest as separate bars per bloom filter type.
    One color for Puffin, one color for Rest (rest of query)."""
    COLOR_PUFFIN = "#00838f"  # cyan
    COLOR_REST = "#1565c0"   # blue

    # 3 groups (bloom types), 2 bars per group (puffin, rest)
    offsets, ticks, _ = group_positions(3, 2, bar_width=0.28)
    bar_width = 0.28

    own_fig = ax is None
    if own_fig:
        fig, ax = plt.subplots(figsize=(10, 5))

    puffin_vals = []
    total_vals = []
    for key in BF_KEYS:
        rows = data["memory_read_mb"][key]
        def _get(v, k):
            return v.get(k, 0) if isinstance(v, dict) else (v if k == "max_mb" else 0)
        total_mb = np.array([_get(r, "max_mb") for r in rows], dtype=float)
        puffin_mb = np.array([_get(r, "puffin_mb") for r in rows], dtype=float)
        puffin_vals.append(puffin_mb[0])
        total_vals.append(total_mb[0])

    ax.bar(offsets[0], puffin_vals, bar_width, color=COLOR_PUFFIN, edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
    ax.bar(offsets[1], total_vals, bar_width, color=COLOR_REST, edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)

    ax.legend(handles=[
        mpatches.Patch(color=COLOR_PUFFIN, label="Puffin"),
        mpatches.Patch(color=COLOR_REST, label="Total"),
    ], loc="upper right")
    ax.set_xticks(ticks)
    ax.set_xticklabels([BF_LABELS[k] for k in BF_KEYS])
    ax.set_xlabel("Bloom Filter Type")
    ax.set_ylabel("Peak Memory Usage (MB)")
    ax.set_title("Peak Memory Usage - Read Path")
    if own_fig:
        fig.tight_layout()
        save(fig, out_dir, "3_memory_read.png", display_inline=display_inline, dataset_size=dataset_size)


# ---------------------------------------------------------------------------
# Chart 4: Memory Usage (Write) - Write Memory Breakdown
# ---------------------------------------------------------------------------
def plot_memory_write(data: dict, out_dir: str, display_inline: bool = False, ax=None, dataset_size: str = None):
    """Clustered bar chart: Data and Puffin as separate bars per bloom filter type.
    One color for Data, one color for Puffin."""
    COLOR_DATA = "#1565c0"   # blue
    COLOR_PUFFIN = "#00838f"  # cyan

    # 3 groups (bloom types), 2 bars per group (data, puffin)
    offsets, ticks, _ = group_positions(3, 2, bar_width=0.28)
    bar_width = 0.28

    own_fig = ax is None
    if own_fig:
        fig, ax = plt.subplots(figsize=(10, 5))

    data_vals = []
    puffin_vals = []
    for key in BF_KEYS:
        rows = data["memory_write_mb"][key]
        def _get(v, k):
            return v.get(k, 0) if isinstance(v, dict) else (v if k == "data_mb" else 0)
        data_mb = np.array([_get(r, "data_mb") for r in rows], dtype=float)
        puffin_mb = np.array([_get(r, "puffin_mb") for r in rows], dtype=float)
        data_vals.append(data_mb[0])
        puffin_vals.append(puffin_mb[0])

    ax.bar(offsets[0], data_vals, bar_width, color=COLOR_DATA, edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
    ax.bar(offsets[1], puffin_vals, bar_width, color=COLOR_PUFFIN, edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)

    ax.legend(handles=[
        mpatches.Patch(color=COLOR_DATA, label="Data"),
        mpatches.Patch(color=COLOR_PUFFIN, label="Puffin"),
    ], loc="upper left")
    ax.set_xticks(ticks)
    ax.set_xticklabels([BF_LABELS[k] for k in BF_KEYS])
    ax.set_xlabel("Bloom Filter Type")
    ax.set_ylabel("Peak Memory Usage (MB)")
    ax.set_title("Peak Memory Usage - Write Path")
    if own_fig:
        fig.tight_layout()
        save(fig, out_dir, "4_memory_write.png", display_inline=display_inline, dataset_size=dataset_size)


# ---------------------------------------------------------------------------
# Chart 5: Time (Read) - Read Time Breakdown
# ---------------------------------------------------------------------------
def plot_time_read(data: dict, out_dir: str, display_inline: bool = False, ax=None, dataset_size: str = None):
    """Stacked bar: totalReadDuration as height, readPuffinDuration as subset. All bars use same segment colors; x-axis = bloom filter type."""
    x_pos, ticks, tick_labels, bar_width = bar_positions_3_by_bf()
    c = SEGMENT_COLORS_2

    own_fig = ax is None
    if own_fig:
        fig, ax = plt.subplots(figsize=(10, 5))

    for i, key in enumerate(BF_KEYS):
        rows = data["time_read_ms"][key]
        total_s = np.array([r.get("total_ms") or 0 for r in rows], dtype=float) / 1000
        puffin_s = np.array([r.get("puffin_ms", 0) for r in rows], dtype=float) / 1000
        rest_s = np.maximum(0, total_s - puffin_s)
        r, p = rest_s[0], puffin_s[0]

        ax.bar(x_pos[i], r, bar_width, color=c[0], edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
        ax.bar(x_pos[i], p, bar_width, bottom=r, color=c[1], edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)

    ax.legend(handles=[
        mpatches.Patch(color=c[1], label="Puffin"),
        mpatches.Patch(color=c[0], label="Rest of Query"),
    ], loc="upper right")
    ax.set_xticks(ticks)
    ax.set_xticklabels(tick_labels)
    ax.set_xlabel("Bloom Filter Type")
    ax.set_ylabel("Time (seconds)")
    ax.set_title("Read Time Breakdown")
    if own_fig:
        fig.tight_layout()
        save(fig, out_dir, "5_time_read.png", display_inline=display_inline, dataset_size=dataset_size)


# ---------------------------------------------------------------------------
# Chart 6: Time (Write) - Write Time Breakdown
# ---------------------------------------------------------------------------
def plot_time_write(data: dict, out_dir: str, display_inline: bool = False, ax=None, dataset_size: str = None):
    """Stacked bar: data write + puffin write time. All bars use same segment colors; x-axis = bloom filter type."""
    x_pos, ticks, tick_labels, bar_width = bar_positions_3_by_bf()
    c = SEGMENT_COLORS_2

    own_fig = ax is None
    if own_fig:
        fig, ax = plt.subplots(figsize=(10, 5))

    for i, key in enumerate(BF_KEYS):
        rows = data["time_write_ms"][key]
        data_s = np.array([r.get("data_ms", 0) for r in rows], dtype=float) / 1000
        puffin_s = np.array([r.get("puffin_ms", 0) for r in rows], dtype=float) / 1000
        d, p = data_s[0], puffin_s[0]

        ax.bar(x_pos[i], d, bar_width, color=c[0], edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)
        ax.bar(x_pos[i], p, bar_width, bottom=d, color=c[1], edgecolor=EDGE_COLOR, linewidth=LINEWIDTH)

    ax.legend(handles=[
        mpatches.Patch(color=c[0], label="Data write"),
        mpatches.Patch(color=c[1], label="Puffin write"),
    ], loc="upper left")
    ax.set_xticks(ticks)
    ax.set_xticklabels(tick_labels)
    ax.set_xlabel("Bloom Filter Type")
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
