"""Memory vs run-time log-log scatter plot colored by task."""

from __future__ import annotations

import numpy as np

from ..core import QuantumRuntimeAnalyzer
from ._style import (
    _ensure_matplotlib,
    _get_tasks_data,
    matplotlib,
    plt,
)


def plot_scatter_memory_vs_time(
    analyzer: QuantumRuntimeAnalyzer,
    tasks: list[str] | None = None,
    highlight_top_n: int | None = None,
    ax: matplotlib.axes.Axes | None = None,
) -> matplotlib.figure.Figure:
    """Produce a log-log scatter plot of memory vs run_time colored by task.

    Parameters
    ----------
    analyzer : `QuantumRuntimeAnalyzer`
        The analyzer instance.
    tasks : `list` of `str` or `None`, optional
        Tasks to include. If None, all tasks are shown.
    highlight_top_n : `int` or `None`, optional
        Highlight the top N quanta by run_time with larger red points
        (and a legend entry); applied only when the table holds at least
        N quanta.
    ax : `matplotlib.axes.Axes` or `None`, optional
        Axis to draw into. If None, a new figure is created.

    Returns
    -------
    fig : `matplotlib.figure.Figure`
        Scatter plot figure.
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
        fig, ax = plt.subplots(figsize=(10, 7))
    else:
        fig = ax.figure

    for tl, mask in sorted(task_groups.items()):
        ax.scatter(data['run_time'][mask], data['memory'][mask],
                   label=tl, alpha=0.5, s=10)

    if highlight_top_n is not None \
            and len(data['task_label']) >= highlight_top_n:
        top_indices = np.argsort(-data['run_time'])[:highlight_top_n]
        ax.scatter(data['run_time'][top_indices],
                   data['memory'][top_indices],
                   c='red', s=40, edgecolors='darkred', linewidths=0.5,
                   label=f'top-{highlight_top_n}', zorder=5)

    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.set_xlabel('Run Time (s, log)')
    ax.set_ylabel('Memory (MiB, log)')
    ax.set_title('Memory vs Run Time')
    ax.grid(True, which='both', alpha=0.3, linewidth=0.5)
    if own_fig:
        # Legend outside the axes; add columns only when a single column
        # would exceed the figure height (a wider legend would squeeze the
        # plot), so the axes keep >=~55% of the figure width.
        handles, legend_labels = ax.get_legend_handles_labels()
        n_entries = len(legend_labels)
        fig_h = fig.get_size_inches()[1]
        rows_per_col = max(8, int(fig_h * 72.0 / 12.0))
        ncol = max(1, -(-n_entries // rows_per_col))
        rect_right = {1: 0.76, 2: 0.62}.get(ncol, 0.55)
        ax.legend(handles, legend_labels, markerscale=0.5, fontsize=8,
                  loc='center left', bbox_to_anchor=(1.02, 0.5), ncol=ncol)
        fig.tight_layout(rect=(0, 0, rect_right, 1))
    else:
        # Embedded (e.g. overview): a 30+ entry legend would overflow the
        # panel, so show only the highlight entry when present.
        handles, legend_labels = ax.get_legend_handles_labels()
        top = [(h, lab) for h, lab in zip(handles, legend_labels)
               if lab.startswith('top-')]
        if top:
            ax.legend([h for h, _ in top], [lab for _, lab in top],
                      markerscale=0.5, fontsize=6, loc='upper left',
                      framealpha=0.9)
    return fig
