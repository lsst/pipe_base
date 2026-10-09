"""Stacked prep/init/run phase bar charts per task."""

from __future__ import annotations

import numpy as np

from ..core import QuantumRuntimeAnalyzer
from ._style import _columns, _ensure_matplotlib, matplotlib, plt


def plot_time_stacked_bar(
    analyzer: QuantumRuntimeAnalyzer,
    sort_by: str | None = None,
    ax: matplotlib.axes.Axes | None = None,
) -> matplotlib.figure.Figure:
    """Produce a stacked horizontal bar chart showing prep/init/run segments.

    Means are computed per task (averaging the prep, init, and run time of
    each quantum); the default figure pairs an absolute-time panel with a
    100% phase-share panel.  When drawing into an existing ``ax`` only the
    absolute-time panel is rendered.

    Parameters
    ----------
    analyzer : `QuantumRuntimeAnalyzer`
        The analyzer instance.
    sort_by : `str` or `None`, optional
        Sort tasks by "run_pct" (most run-heavy at top) or, for any other
        value (including ``None``), by descending total mean time
        (slowest at top).
    ax : `matplotlib.axes.Axes` or `None`, optional
        Axis to draw into. If None, a new figure is created.

    Returns
    -------
    fig : `matplotlib.figure.Figure`
        Stacked bar chart figure.
    """
    _ensure_matplotlib()

    data = _columns(analyzer.table)
    if len(data['task_label']) == 0:
        own_fig = ax is None
        if own_fig:
            fig, ax = plt.subplots()
        else:
            fig = ax.figure
        ax.text(0.5, 0.5, 'No data available', ha='center', va='center')
        return fig

    task_groups = {}
    for tl in np.unique(data['task_label']):
        mask = data['task_label'] == tl
        task_groups[tl] = mask

    # Compute means per task
    prep_means = {tl: np.mean(data['prep_time'][mask]) for tl, mask in task_groups.items()}
    init_means = {tl: np.mean(data['init_time'][mask]) for tl, mask in task_groups.items()}
    run_means = {tl: np.mean(data['run_time'][mask]) for tl, mask in task_groups.items()}

    sorted_tasks = sorted(task_groups.keys())
    totals = {tl: prep_means[tl] + init_means[tl] + run_means[tl]
              for tl in sorted_tasks}
    if sort_by == 'run_pct':
        run_pcts = {tl: (run_means[tl] / totals[tl] * 100) if totals[tl] > 0
                    else 0 for tl in sorted_tasks}
        # Most run-heavy first: it lands at the top after the axis invert.
        sorted_tasks = sorted(sorted_tasks, key=lambda t: -run_pcts[t])
    else:
        # barh + invert_yaxis puts the first entry at the top: slowest first.
        sorted_tasks = sorted(sorted_tasks, key=lambda t: -totals[t])

    y_pos = np.arange(len(sorted_tasks))

    own_fig = ax is None
    if own_fig:
        n = len(sorted_tasks)
        # Left panel: absolute mean time.  Right panel: 100% phase share, so
        # prep/init remain visible even when run time dominates the absolute
        # scale (on a linear absolute axis tiny phases are sub-pixel).
        fig, (ax, ax_pct) = plt.subplots(
            1, 2, figsize=(13, min(max(5, n * 0.35), 30.0)),
            gridspec_kw={'width_ratios': [3, 2]})
    else:
        fig = ax.figure
        ax_pct = None

    def _draw_stacked(target_ax: matplotlib.axes.Axes, percent: bool) -> None:
        if percent:
            prep_vals = np.array([prep_means[t] for t in sorted_tasks])
            init_vals = np.array([init_means[t] for t in sorted_tasks])
            run_vals = np.array([run_means[t] for t in sorted_tasks])
            tot = prep_vals + init_vals + run_vals
            tot = np.where(tot > 0, tot, 1.0)
            prep_vals, init_vals, run_vals = (prep_vals / tot * 100,
                                              init_vals / tot * 100,
                                              run_vals / tot * 100)
        else:
            prep_vals = np.array([prep_means[t] for t in sorted_tasks])
            init_vals = np.array([init_means[t] for t in sorted_tasks])
            run_vals = np.array([run_means[t] for t in sorted_tasks])
        target_ax.barh(y_pos, prep_vals, label='prep',
                       color='#f4a261', height=0.6)
        target_ax.barh(y_pos, init_vals, left=prep_vals,
                       label='init', color='#2a9d8f', height=0.6)
        target_ax.barh(y_pos, run_vals, left=prep_vals + init_vals,
                       label='run', color='#264653', height=0.6)
        target_ax.set_yticks(y_pos)
        target_ax.set_yticklabels(
            sorted_tasks, fontsize=min(8.0, max(5.0, 200.0 / len(sorted_tasks))))
        target_ax.invert_yaxis()

    _draw_stacked(ax, percent=False)
    ax.set_xlabel('Mean Time (s)')
    ax.set_title('Mean Time by Phase')
    ax.legend(fontsize=min(8.0, max(6.0, 200.0 / len(sorted_tasks))),
              loc='lower right')
    if ax_pct is not None:
        _draw_stacked(ax_pct, percent=True)
        ax_pct.set_xlim(0, 100)
        ax_pct.set_xlabel('Phase share (%)')
        ax_pct.set_title('Phase Share (100%)')
        ax_pct.set_yticklabels([])
    if own_fig:
        fig.tight_layout()
    return fig
