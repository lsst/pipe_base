"""Plot functions for the runtime tracker.

Provides trend, heatmap, and delta bar visualizations using matplotlib.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import matplotlib.figure

_PLOTS_DIR = Path.home() / ".local" / "share" / "runtime-analyzer-plots"


def plot_trend(
    task_label: str,
    metric: str,
    run_labels: list,
    values: list,
    timestamps: list,
) -> matplotlib.figure.Figure:
    """Create a trend line plot with regression and confidence band.

    Points are drawn against an ordinal run index (0, 1, ... in the
    order given); a regression line is added when at least two values
    are finite, and a 95% confidence band when more than two are.

    Parameters
    ----------
    task_label : `str`
        Task name for the title.
    metric : `str`
        Metric name for the title and y-axis label.
    run_labels : `list`
        Run labels; used as x tick labels only when the label count
        matches the value count.
    values : `list`
        Metric values (seconds) for each run.
    timestamps : `list`
        Timestamps for each run (accepted but not used for x-axis
        positioning).

    Returns
    -------
    fig : `matplotlib.figure.Figure`
        Trend line figure.
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    fig, ax = plt.subplots(figsize=(10, 5))

    values_arr = np.asarray(values, dtype=float)
    n = len(values)
    x = np.arange(n)

    # Nothing to plot: return an empty but valid figure.
    if n == 0:
        ax.set_xlabel("Run index")
        ax.set_ylabel(f"{metric} (seconds)")
        ax.set_title(f"{task_label} {metric} over time")
        ax.grid(True, alpha=0.3)
        return fig

    # Only label the x-axis when the label count matches the data count.
    if len(run_labels) == n:
        ax.set_xticks(x)
        ax.set_xticklabels(run_labels, rotation=45, ha="right")

    ax.scatter(x, values_arr, color="steelblue", zorder=3, s=50, label="Data points")

    # Regression line (needs >= 2 finite points).
    finite = np.isfinite(values_arr)
    if n >= 2 and np.count_nonzero(finite) >= 2:
        coeffs = np.polyfit(x[finite], values_arr[finite], 1)
        poly = np.poly1d(coeffs)
        x_line = np.linspace(x.min(), x.max(), 100)
        ax.plot(x_line, poly(x_line), "r-", linewidth=1.5, label="Regression line")

        # Confidence band (simple analytic CI), requires > 2 finite points
        # and non-degenerate spread in x.
        residuals = values_arr[finite] - poly(x[finite])
        ss_xx = np.sum((x[finite] - x[finite].mean()) ** 2)
        if len(residuals) > 2 and ss_xx > 0:
            std_err = np.std(residuals)
            ci = 1.96 * std_err * np.sqrt(
                1 / len(x[finite]) + (x_line - x[finite].mean()) ** 2 / ss_xx
            )
            ax.fill_between(
                x_line, poly(x_line) - ci, poly(x_line) + ci,
                color="red", alpha=0.1, label="95% CI",
            )

    # Near-constant data would otherwise trigger matplotlib's offset
    # notation (e.g. "1e-10+6.8e1") colliding with the title area.
    finite_vals = values_arr[finite]
    if finite_vals.size > 0:
        vmin, vmax = float(finite_vals.min()), float(finite_vals.max())
        if vmax - vmin <= 1e-9 * max(abs(vmax), 1.0):
            pad = max(abs(vmax) * 0.05, 0.5)
            ax.set_ylim(vmin - pad, vmax + pad)
    from matplotlib.ticker import ScalarFormatter
    yfmt = ScalarFormatter()
    yfmt.set_useOffset(False)
    ax.yaxis.set_major_formatter(yfmt)

    # Value labels per point when the run count is small enough to stay
    # readable without them cluttering the line.
    if n <= 12:
        for xi, yi in zip(x, values_arr):
            if not np.isfinite(yi):
                continue
            fmt = f'{yi:,.1f}' if abs(yi) >= 10 else f'{yi:.2f}'
            ax.annotate(fmt, (float(xi), float(yi)), textcoords='offset points',
                        xytext=(0, 9), ha='center', fontsize=7, zorder=4)

    ax.set_xlabel("Run index")
    ax.set_ylabel(f"{metric} (seconds)")
    ax.set_title(f"{task_label} {metric} over time")
    ax.legend()
    ax.grid(True, alpha=0.3)

    return fig


def plot_heatmap(
    task_labels: list,
    run_labels: list,
    values_2d: list,
    metric: str | None = None,
) -> matplotlib.figure.Figure:
    """Create a heatmap comparing metrics across tasks and runs.

    Parameters
    ----------
    task_labels : `list`
        Row labels (tasks).
    run_labels : `list`
        Column labels (runs).
    values_2d : `list`
        2D list/array of values. Rows are tasks, columns are runs.
    metric : `str` or `None`, optional
        Metric name included in the title and colorbar label.

    Returns
    -------
    fig : `matplotlib.figure.Figure`
        Heatmap figure.
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    fig, ax = plt.subplots(
        figsize=(min(max(8, len(run_labels) * 1.5), 30.0),
                 min(max(5, len(task_labels) * 0.5), 30.0)),
    )

    values_array = np.asarray(values_2d, dtype=float)

    # Zero-size guard: keep a valid, empty figure when there is no data.
    if values_array.size == 0:
        ax.text(0.5, 0.5, "No data", ha="center", va="center")
        ax.axis("off")
        ax.set_title("Runtime metric heatmap")
        plt.tight_layout()
        return fig

    # Mask NaN cells so they render blank, keeping the colormap
    # scaled on real data.
    masked = np.ma.masked_invalid(values_array)

    # Robust color scaling from the finite (non-NaN) values only.  A plain
    # min/max is dominated by a single outlier cell (all the rest collapse
    # to one color), so use a 5-95 percentile clip; extreme cells saturate
    # to the colormap ends, which is informative.
    finite = values_array[np.isfinite(values_array)]
    if finite.size > 0:
        if finite.size >= 20:
            vmin, vmax = np.percentile(finite, 5), np.percentile(finite, 95)
        else:
            vmin, vmax = float(finite.min()), float(finite.max())
        vmin, vmax = float(vmin), float(vmax)
    else:
        vmin, vmax = 0.0, 1.0
    if vmin == vmax:
        vmin, vmax = vmin - 0.5, vmax + 0.5

    title = "Runtime metric heatmap"
    cbar_label = "Value"
    if metric:
        title = f"Runtime metric heatmap ({metric})"
        cbar_label = f"{metric} (seconds)"

    im = ax.pcolormesh(masked, cmap="RdYlGn_r", shading="auto",
                       vmin=vmin, vmax=vmax)
    ax.set_ylabel("Task")
    ax.set_xlabel("Run")
    ax.set_title(title)

    # Only set ticks when the label counts match the array extents.
    n_rows, n_cols = values_array.shape
    if len(run_labels) == n_cols:
        ax.set_xticks(np.arange(n_cols) + 0.5)
        ax.set_xticklabels(run_labels, rotation=45, ha="left")
    if len(task_labels) == n_rows:
        ax.set_yticks(np.arange(n_rows) + 0.5)
        ax.set_yticklabels(task_labels,
                           fontsize=min(8.0, max(5.0, 200.0 / n_rows)))

    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label(cbar_label)

    # Annotate cells (skip NaN cells and any out-of-range label indices).
    midpoint = (vmin + vmax) / 2.0
    for i in range(min(len(task_labels), n_rows)):
        for j in range(min(len(run_labels), n_cols)):
            val = values_array[i, j]
            if not np.isfinite(val):
                continue
            ax.text(j + 0.5, i + 0.5, f"{val:.1f}",
                    ha="center", va="center", fontsize=8,
                    color="black" if val < midpoint else "white")

    plt.tight_layout()
    return fig


def plot_delta_bar(
    task_labels: list,
    delta_pcts: list,
) -> matplotlib.figure.Figure:
    """Create a horizontal bar chart of percentage changes per task.

    Deltas above +0.5% (slower) are red, below -0.5% (faster) are green,
    and the rest (including zero and non-finite values) are gray.

    Parameters
    ----------
    task_labels : `list`
        Task names for y-axis.
    delta_pcts : `list`
        Percentage change per task.

    Returns
    -------
    fig : `matplotlib.figure.Figure`
        Delta bar chart figure.
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    fig, ax = plt.subplots(figsize=(10, min(max(4, len(task_labels) * 0.45), 30.0)))

    # Guard against empty input and mismatched label/value lengths.
    n = min(len(task_labels), len(delta_pcts))
    vals = np.asarray(delta_pcts[:n], dtype=float)
    y_pos = np.arange(n)

    # Label offset and axis padding must scale with the data: a fixed
    # offset blows up the x-axis when every delta is tiny (identical runs)
    # and clips labels when deltas are large.
    finite = vals[np.isfinite(vals)]
    max_abs = float(np.abs(finite).max()) if finite.size else 0.0
    span = max(max_abs, 1.0) * 1.15
    offset = span * 0.015

    def _color(d: float) -> str:
        if not np.isfinite(d):
            return "#95a5a6"
        return "#e74c3c" if d > 0.5 else "#27ae60" if d < -0.5 else "#95a5a6"

    colors = [_color(d) for d in vals]

    bars = ax.barh(y_pos, vals, color=colors)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(task_labels[:n],
                       fontsize=min(8.0, max(5.0, 200.0 / max(n, 1))))
    ax.set_xlabel("Change (%)")
    ax.set_title("Task comparison: percentage change")
    ax.axvline(x=0, color="black", linewidth=0.5)
    ax.set_xlim(-span, span)
    ax.invert_yaxis()
    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(color="#e74c3c", label="slower"),
                       Patch(color="#27ae60", label="faster"),
                       Patch(color="#95a5a6", label="unchanged")],
              fontsize=7, loc="lower right", framealpha=0.9)

    for bar, val in zip(bars, vals):
        if not np.isfinite(val):
            continue
        ha = "left" if val >= 0 else "right"
        ax.text(val + (offset if val >= 0 else -offset),
                bar.get_y() + bar.get_height() / 2,
                f"{val:+.1f}%", ha=ha, va="center", fontsize=9)

    plt.tight_layout()
    return fig


def get_tracker_plot_names() -> list:
    """Return list of available tracker plot names.

    Returns
    -------
    names : `list` of `str`
        ``["trend", "heatmap", "delta"]``.

    Notes
    -----
    This is part of the package's public API (re-exported from
    ``lsst.pipe.base._runtime_analyzer``) so callers can enumerate the
    tracker's plot kinds; the console ``cli.py`` renders the tracker
    plot kinds directly rather than looping over this list.
    """
    return ["trend", "heatmap", "delta"]


def save_tracker_plot(fig: matplotlib.figure.Figure, name: str) -> str:
    """Save a tracker plot to disk.

    Writes ``<name>.png`` (300 dpi, tight bounding box) under
    ``~/.local/share/runtime-analyzer-plots/``, creating the directory if
    needed.

    Parameters
    ----------
    fig : `matplotlib.figure.Figure`
        The figure to save.
    name : `str`
        Plot name (used as the filename stem): e.g. "trend", "heatmap",
        "delta".

    Returns
    -------
    path : `str`
        Path where the file was saved.
    """
    output_dir = _PLOTS_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    ext = "png"
    outpath = output_dir / f"{name}.{ext}"
    fig.savefig(str(outpath), dpi=300, bbox_inches="tight", format=ext)
    return str(outpath)
