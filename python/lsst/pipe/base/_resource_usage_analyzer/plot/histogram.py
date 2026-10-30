"""Multi-panel run-time histogram facets with optional KDE overlay."""

from __future__ import annotations

import numpy as np

from ..core import QuantumRuntimeAnalyzer
from ._style import (
    _ensure_matplotlib,
    _get_tasks_data,
    matplotlib,
    plt,
)
from .dimension import _create_facet_axes


def plot_histogram(
    analyzer: QuantumRuntimeAnalyzer,
    tasks: list[str] | None = None,
    nrows: int = 4,
    ax: matplotlib.axes.Axes | None = None,
) -> matplotlib.figure.Figure:
    """Produce a multi-panel histogram with KDE overlay, faceted by task.

    Each panel histograms one task's positive run times; the KDE overlay
    is drawn only when scipy is available and the task has at least five
    positive-valued quanta.

    Parameters
    ----------
    analyzer : `QuantumRuntimeAnalyzer`
        The analyzer instance.
    tasks : `list` of `str` or `None`, optional
        Tasks to include.
    nrows : `int`, optional
        Target number of rows for facets (the facet grid caps its column
        count and adds rows beyond that target).
    ax : `matplotlib.axes.Axes` or `None`, optional
        Accepted for compatibility but ignored; this function creates its own
        facet grids.

    Returns
    -------
    fig : `matplotlib.figure.Figure`
        Histogram figure.
    """
    _ensure_matplotlib()

    data, task_groups = _get_tasks_data(analyzer, tasks)
    if len(data['task_label']) == 0:
        fig, ax = plt.subplots()
        ax.text(0.5, 0.5, 'No data available', ha='center', va='center')
        return fig

    fig, axes = _create_facet_axes(list(task_groups.keys()), nrows)

    for i, (tl, mask) in enumerate(task_groups.items()):
        ax = axes[i]
        rt = data['run_time'][mask]
        rt_positive = rt[rt > 0]

        if len(rt_positive) > 0:
            counts, edges, _ = ax.hist(
                rt_positive, bins=min(int(np.sqrt(len(rt_positive))) + 1, 50),
                alpha=0.7, color='steelblue', edgecolor='white')
            # Simple KDE by smoothing - scipy is optional; a density curve is
            # meaningless for a handful of samples, gate at n >= 5.
            if len(rt_positive) >= 5:
                try:
                    from scipy.stats import gaussian_kde
                    kde = gaussian_kde(rt_positive)
                    x_range = np.linspace(np.min(rt_positive), np.max(rt_positive), 100)
                    # Scale density onto histogram counts.
                    bin_width = edges[1] - edges[0] if len(edges) > 1 else 1.0
                    ax.plot(x_range, kde(x_range) * len(rt_positive) * bin_width,
                            'r-', linewidth=2)
                except (ImportError, np.linalg.LinAlgError):
                    pass  # KDE disabled without scipy or on degenerate data
            ax.set_ylim(0.5, max(counts.max() * 1.5, 1.5))
        ax.set_title(f'{tl} (n={len(rt)})', fontsize=8)
        ax.set_xlabel('Run Time (s)', fontsize=7)
        ax.set_ylabel('Count', fontsize=7)
        ax.tick_params(labelsize=6)

    fig.suptitle('Run Time Distributions by Task', y=0.995)
    fig.tight_layout(rect=(0, 0, 1, 0.98))
    return fig
