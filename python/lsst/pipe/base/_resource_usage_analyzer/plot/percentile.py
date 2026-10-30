"""Percentile-curve (p0-p95) line plots per task."""

from __future__ import annotations

import numpy as np

from ..core import QuantumRuntimeAnalyzer
from ._style import (
    _ensure_matplotlib,
    _get_tasks_data,
    matplotlib,
    plt,
)


def plot_percentile_curve(
    analyzer: QuantumRuntimeAnalyzer,
    tasks: list[str] | None = None,
    ax: matplotlib.axes.Axes | None = None,
) -> matplotlib.figure.Figure:
    """Produce a line plot of percentile curves (p0-p95) per task.

    Curves use each task's positive run times only, so percentiles
    reflect actual work time; tasks with fewer than two positive-valued
    quanta are skipped (and the y-axis switches to log when every drawn
    task is positive-only).

    Parameters
    ----------
    analyzer : `QuantumRuntimeAnalyzer`
        The analyzer instance.
    tasks : `list` of `str` or `None`, optional
        Tasks to include.
    ax : `matplotlib.axes.Axes` or `None`, optional
        Axis to draw into. If None, a new figure is created.

    Returns
    -------
    fig : `matplotlib.figure.Figure`
        Percentile curve figure.
    """
    _ensure_matplotlib()

    data, task_groups = _get_tasks_data(analyzer, tasks)
    if len(data['task_label']) == 0:
        own_fig = ax is None
        if own_fig:
            fig, ax = plt.subplots()
        else:
            fig = ax.figure
        ax.text(0.5, 0.5, 'No data available', ha='center', va='center')
        return fig

    own_fig = ax is None
    if own_fig:
        fig, ax = plt.subplots(figsize=(10, 6))
    else:
        fig = ax.figure

    pct_vals = [p for p in range(0, 100, 5)]
    n_drawn = 0
    all_positive = True
    for tl, mask in sorted(task_groups.items()):
        rt = data['run_time'][mask]
        if len(rt) < 2:
            # A single quantum carries no percentile information.
            continue
        rt_pos = rt[rt > 0]
        if len(rt_pos) < 2:
            continue
        if len(rt_pos) < len(rt):
            all_positive = False
        percentiles = np.percentile(rt_pos, pct_vals)
        ax.plot(pct_vals, percentiles, label=f'{tl} (n={len(rt)})',
                linewidth=1.5)
        n_drawn += 1

    if n_drawn == 0:
        ax.text(0.5, 0.5, 'No task has two or more quanta with positive '
                          'run time', ha='center', va='center')
        return fig

    if all_positive:
        # A log axis keeps slow and fast tasks simultaneously readable;
        # on a linear axis the slowest task flattens everything else.
        ax.set_yscale('log')
    ax.set_xlabel('Percentile')
    ax.set_ylabel('Run Time (s)')
    ax.set_title(f'Percentile Curves by Task ({n_drawn} tasks)')
    ax.grid(True, which='both', alpha=0.3, linewidth=0.5)
    if own_fig:
        rows_per_col = max(8, int(6.0 * 72.0 / 12.0))
        ncol = max(1, -(-n_drawn // rows_per_col))
        ax.legend(fontsize=8, loc='center left', bbox_to_anchor=(1.02, 0.5),
                  ncol=ncol)
        fig.tight_layout(rect=(0, 0, {1: 0.78, 2: 0.62}.get(ncol, 0.55), 1))
    else:
        ax.legend(fontsize=6, loc='upper left', framealpha=0.9)
    return fig
