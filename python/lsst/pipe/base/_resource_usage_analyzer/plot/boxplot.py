"""Task box-plot comparison of runtime distributions."""

from __future__ import annotations

import numpy as np

from ..core import QuantumRuntimeAnalyzer
from ._style import (
    _ensure_matplotlib,
    _get_tasks_data,
    _make_task_colors,
    _metric_label,
    matplotlib,
    plt,
)


def plot_task_boxplot(
    analyzer: QuantumRuntimeAnalyzer,
    tasks: list[str] | None = None,
    metric: str = "run_time",
    ax: matplotlib.axes.Axes | None = None,
    y_scale: str = "auto",
) -> matplotlib.figure.Figure:
    """Produce a box plot comparing runtime distributions across tasks.

    Parameters
    ----------
    analyzer : `QuantumRuntimeAnalyzer`
        The analyzer instance.
    tasks : `list` of `str` or `None`, optional
        Tasks to include. If None, all tasks are shown.
    metric : `str`, optional
        Metric to plot: "run_time" or "memory".
    ax : `matplotlib.axes.Axes` or `None`, optional
        Axis to draw into. If None, a new figure is created.
    y_scale : `str`, optional
        "auto" (default): log scale when the dynamic range (max divided by
        the smallest positive value) is at least 100x, falling back to
        symlog when non-positive values are present.  "log"/"symlog"/
        "linear" force the choice.  A switch always requires at least two
        positive values.

    Returns
    -------
    fig : `matplotlib.figure.Figure`
        Box plot figure.
    """
    _ensure_matplotlib()

    data, task_groups = _get_tasks_data(analyzer, tasks)
    if len(data) == 0 or not task_groups:
        own_fig = ax is None
        if own_fig:
            fig, ax = plt.subplots()
        else:
            fig = ax.figure
        ax.text(0.5, 0.5, 'No data available', ha='center', va='center')
        ax.set_title('No Data')
        return fig

    # Sort tasks by mean metric value
    task_means = {}
    for tl, mask in sorted(task_groups.items()):
        task_means[tl] = np.mean(data[metric][mask])
    sorted_tasks = sorted(task_means.keys(), key=lambda t: task_means[t])

    box_data = [data[metric][task_groups[tl]] for tl in sorted_tasks]

    n = len(sorted_tasks)
    fs = min(9.0, max(6.0, 200.0 / n))
    own_fig = ax is None
    if own_fig:
        fig, ax = plt.subplots(
            figsize=(max(8.0, min(n, 24) * 0.9), min(max(6.0, n * 0.25), 30.0)))
    else:
        fig = ax.figure

    bp = ax.boxplot(
        box_data, tick_labels=sorted_tasks, patch_artist=True,
        medianprops=dict(color='black'),
        whiskerprops=dict(color='#555555', linestyle='-'),
        capprops=dict(color='#555555', linestyle='-'),
        flierprops=dict(marker='o', markerfacecolor='none',
                        markeredgecolor='#777777', markersize=4),
    )

    colors = _make_task_colors(sorted_tasks)
    for patch, color in zip(bp['boxes'], colors):
        patch.set_facecolor(color)

    # Wide dynamic ranges (one 1100 s task next to thirty ~1 s tasks)
    # collapse every box to the floor on a linear axis: switch to log
    # (symlog when zeros are present) so the spread stays visible.
    scale_note = ''
    all_vals = np.concatenate(box_data) if box_data else np.array([])
    pos_vals = all_vals[all_vals > 0]
    ratio = float(all_vals.max() / pos_vals.min()) if pos_vals.size and all_vals.size else 0.0
    y_scale = (y_scale or 'auto').lower()
    if y_scale == 'auto':
        want_log = ratio >= 100.0
    elif y_scale in ('log', 'symlog'):
        want_log = True
    else:
        want_log = False
    if want_log and pos_vals.size > 1:
        if (all_vals <= 0).any():
            ax.set_yscale('symlog', linthresh=max(float(pos_vals.min()) / 2.0, 1e-6))
            scale_note = ' (symlog)'
        else:
            ax.set_yscale('log')
            scale_note = ' (log)'

    ax.set_ylabel(_metric_label(metric) + scale_note)
    ax.set_xlabel('Task')
    ax.set_title(f'{_metric_label(metric)} Distribution by Task')
    ax.grid(True, axis='y', alpha=0.3, linewidth=0.5)
    ax.set_axisbelow(True)
    ax.tick_params(axis='x', labelsize=fs)
    # At 45 degrees adjacent labels are parallel diagonals and only collide
    # when the per-box spacing (times sin 45) falls below the text height.
    # When it does (dense panels), switch to vertical labels.
    fig_w = fig.get_size_inches()[0] if own_fig else \
        fig.get_size_inches()[0] * 0.45
    rotate90 = (fig_w / n) < fs * 1.5 / 72.0
    for t in ax.get_xticklabels():
        if rotate90:
            t.set_rotation(90)
            t.set_ha('center')
            t.set_fontsize(min(fs, 5.0))
        else:
            t.set_rotation(45)
            t.set_ha('right')
    if own_fig:
        fig.tight_layout()
    return fig
