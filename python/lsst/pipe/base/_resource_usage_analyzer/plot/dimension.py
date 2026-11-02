"""Dimension-distribution facet plots (boxplot and histogram forms).

Also owns the shared facet-axis factory used by every facet plot in the
package.
"""

from __future__ import annotations

import numpy as np

from ..core import QuantumRuntimeAnalyzer, _extract_dimension_value
from ._style import (
    Figure,
    _ensure_matplotlib,
    _get_tasks_data,
    matplotlib,
    plt,
)


def plot_dimension_dist(
    analyzer: QuantumRuntimeAnalyzer,
    dimension: str,
    tasks: list[str] | None = None,
    plot_type: str = 'boxplot',
    nrows: int = 4,
    ax: matplotlib.axes.Axes | None = None,
) -> matplotlib.figure.Figure:
    """Produce a boxplot or histogram facet grid grouped by dimension values.

    Parameters
    ----------
    analyzer : `QuantumRuntimeAnalyzer`
        The analyzer instance.
    dimension : `str`
        Data dimension to group by.
    tasks : `list` of `str` or `None`, optional
        Tasks to include.
    plot_type : `str`, optional
        Plot type: "boxplot" or "histogram"; any other value renders the
        boxplot form.
    nrows : `int`, optional
        Target number of rows for the facet grid.
    ax : `matplotlib.axes.Axes` or `None`, optional
        Accepted for compatibility but ignored; this function creates its
        own facet grids.

    Returns
    -------
    fig : `matplotlib.figure.Figure`
        Facet grid figure.
    """
    _ensure_matplotlib()

    data, task_groups = _get_tasks_data(analyzer, tasks)
    if len(data['task_label']) == 0:
        fig, ax = plt.subplots()
        ax.text(0.5, 0.5, 'No data available', ha='center', va='center')
        return fig

    if plot_type == 'boxplot':
        return _plot_dimension_boxplot(data, task_groups, dimension, nrows)
    elif plot_type == 'histogram':
        return _plot_dimension_histogram(data, task_groups, dimension, nrows)
    else:
        return _plot_dimension_boxplot(data, task_groups, dimension, nrows)


def _plot_dimension_boxplot(
    data: dict[str, np.ndarray],
    task_groups: dict[str, np.ndarray],
    dimension: str,
    nrows: int,
) -> Figure:
    """Plot dimension distribution as boxplot facets (internal helper).

    Tasks that have no value for ``dimension`` (every quantum resolves to
    "unknown") are omitted entirely: a single "unknown" box carries no
    information about the dimension.
    """
    all_tasks = list(task_groups.keys())
    dim_map = {
        tl: [_extract_dimension_value(data['data_id'][i], dimension)
             for i in np.flatnonzero(task_groups[tl])]
        for tl in all_tasks
    }
    tasks = [tl for tl in all_tasks
             if any(v != 'unknown' for v in dim_map[tl])]
    if not tasks:
        fig = plt.figure(figsize=(6, 4))
        fig.text(0.5, 0.5, f'No task has a "{dimension}" dimension',
                 ha='center', va='center')
        return fig

    fig, axes = _create_facet_axes(tasks, nrows)
    n_tasks = len(tasks)
    ncols = max(1, -(-n_tasks // max(int(nrows), 1)))

    for i, tl in enumerate(tasks):
        ax = axes[i]
        dim_vals = dim_map[tl]
        rows = np.flatnonzero(task_groups[tl])

        unique_vals = sorted(set(dim_vals),
                             key=lambda x: (x == 'unknown', x))

        box_data = []
        labels = []
        for dv in unique_vals:
            dv_mask = np.array([v == dv for v in dim_vals])
            if np.any(dv_mask):
                box_data.append(data['run_time'][rows[dv_mask]])
                labels.append(dv)

        if box_data:
            ax.boxplot(box_data, tick_labels=labels, patch_artist=True,
                       medianprops=dict(color='black'),
                       whiskerprops=dict(color='#555555'),
                       capprops=dict(color='#555555'))
            for patch in ax.patches:
                patch.set_facecolor('skyblue')

        ax.set_title(tl, fontsize=8)
        ax.set_xlabel(dimension, fontsize=7)
        if i % ncols == 0:
            # Show the y label once per row.
            ax.set_ylabel('Run Time (s)', fontsize=7)
        for t in ax.get_xticklabels():
            t.set_rotation(45)
            t.set_ha('right')
            t.set_fontsize(6)

    fig.suptitle(f'Run Time by "{dimension}" per Task', y=0.995)
    fig.tight_layout(rect=(0, 0, 1, 0.98))
    return fig


def _plot_dimension_histogram(
    data: dict[str, np.ndarray],
    task_groups: dict[str, np.ndarray],
    dimension: str,
    nrows: int,
) -> Figure:
    """Plot dimension distribution as histogram facets (internal helper).

    Numeric dimension values are binned into integer decades and drawn as
    deterministic count bars (one bar per populated decade — no random
    jitter, so figures are reproducible).  Non-numeric dimensions fall back
    to one count bar per unique value.
    """
    tasks = list(task_groups.keys())
    tasks = [
        tl for tl in tasks
        if any(
            _extract_dimension_value(data['data_id'][i], dimension)
            != 'unknown' for i in np.flatnonzero(task_groups[tl])
        )
    ]
    if not tasks:
        fig = plt.figure(figsize=(6, 4))
        fig.text(0.5, 0.5, f'No task has a "{dimension}" dimension',
                 ha='center', va='center')
        return fig
    fig, axes = _create_facet_axes(tasks, nrows)
    n_tasks = len(tasks)
    ncols = max(1, -(-n_tasks // max(int(nrows), 1)))

    for i, tl in enumerate(tasks):
        ax = axes[i]
        rows = np.flatnonzero(task_groups[tl])

        dim_vals = [_extract_dimension_value(data['data_id'][i], dimension)
                    for i in rows]

        # Group integer dimension values by decade (floor division by 10);
        # non-numeric values are excluded from the numeric decades.
        numeric_vals = []
        for dv in dim_vals:
            try:
                numeric_vals.append(int(dv))
            except (ValueError, TypeError):
                continue
        numeric_vals = np.array(numeric_vals, dtype=np.int64)

        if numeric_vals.size > 0:
            decade_idx, counts = np.unique(
                numeric_vals // 10, return_counts=True,
            )
            decade_starts = decade_idx * 10
            ax.bar(decade_starts, counts, width=9, align='edge',
                   color='steelblue', edgecolor='white')
            ax.set_xticks(decade_starts.tolist())
            ax.set_xticklabels([f'{d}-{d + 9}' for d in decade_starts],
                               rotation=45)
            ax.set_title(f'{tl} (decade bins)')
        elif len(dim_vals) > 0:
            # Non-numeric dimension (e.g. band): count bar per unique value.
            unique_vals, counts = np.unique(
                np.array(dim_vals), return_counts=True,
            )
            x_pos = np.arange(len(unique_vals))
            ax.bar(x_pos, counts, color='steelblue', edgecolor='white')
            ax.set_xticks(x_pos.tolist())
            ax.set_xticklabels([str(v) for v in unique_vals], rotation=45)
            ax.set_title(f'{tl} (per value)')

        ax.set_xlabel(dimension, fontsize=7)
        if i % ncols == 0:
            ax.set_ylabel('Quantum count', fontsize=7)

    fig.suptitle(f'Quantum Count by "{dimension}" per Task', y=0.995)
    fig.tight_layout(rect=(0, 0, 1, 0.98))
    return fig


def _create_facet_axes(
    tasks: list[str],
    nrows: int,
) -> tuple[matplotlib.figure.Figure, np.ndarray]:
    """Create facet subplot axes, one per task.

    Always returns a flat 1-D ndarray containing *exactly* ``len(tasks)``
    Axes, for any task count (including 0 and 1).  The grid has
    ``ceil(len(tasks) / nrows)`` columns (capped at 8, adding rows rather
    than columns) and enough rows to hold every task; surplus grid cells
    (when the grid does not divide evenly) are removed from the figure.
    Callers therefore index ``axes[i]`` uniformly, for every task count.

    Parameters
    ----------
    tasks : `list` of `str`
        Task labels for subplots.
    nrows : `int`
        Target number of rows; the column cap can raise the actual row
        count above it.

    Returns
    -------
    fig : `matplotlib.figure.Figure`
        The figure object.  For an empty ``tasks`` list, a blank figure
        with a 'No data available' message.
    axes : `~numpy.ndarray`
        Flat 1-D array of exactly ``len(tasks)`` Axes (empty if no tasks).
    """
    tasks = list(tasks)
    n_tasks = len(tasks)
    if n_tasks == 0:
        # plt.subplots() rejects a 0-sized grid; return a blank figure.
        fig = plt.figure(figsize=(6, 4))
        fig.text(0.5, 0.5, 'No data available', ha='center', va='center')
        return fig, np.array([], dtype=object)

    nrows = max(int(nrows), 1)
    ncols = max(1, -(-n_tasks // nrows))  # ceil division
    ncols = min(ncols, 8)                 # cap width: add rows, not columns
    nrows = max(1, -(-n_tasks // ncols))  # rows actually needed

    fig, axes = plt.subplots(nrows, ncols,
                             figsize=(min(ncols * 3.5, 28.0),
                                      min(nrows * 3, 40.0)),
                             squeeze=False)
    axes = np.asarray(axes).flatten()
    # Remove surplus grid cells so the figure holds exactly len(tasks) axes.
    for extra_ax in axes[n_tasks:]:
        fig.delaxes(extra_ax)
    return fig, axes[:n_tasks]
