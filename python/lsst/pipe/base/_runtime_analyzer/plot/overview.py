"""Coordinated multi-panel overview layout."""

from __future__ import annotations

from ..core import QuantumRuntimeAnalyzer
from ._style import _ensure_matplotlib, matplotlib, plt
from .bars import plot_top_n_bar
from .boxplot import plot_task_boxplot
from .scatter import plot_scatter_memory_vs_time
from .stacked import plot_time_stacked_bar


def plot_overview(
    analyzer: QuantumRuntimeAnalyzer,
    column: int = 2,
    tasks: list[str] | None = None,
) -> matplotlib.figure.Figure:
    """Produce a coordinated multi-panel overview layout.

    Creates a 2x2 grid (``column=2``) with:

    - Top-left: task box plot
    - Top-right: time stacked bar
    - Bottom-left: scatter memory vs time
    - Bottom-right: top-N bar chart

    ``column=1`` renders the two top panels as a vertical stack instead.

    Parameters
    ----------
    analyzer : `QuantumRuntimeAnalyzer`
        The analyzer instance.
    column : `int`, optional
        Number of columns: 1 for the vertical stack, 2 for the 2x2 grid.
    tasks : `list` of `str` or `None`, optional
        Tasks to include (passed to the box plot and scatter panels).

    Returns
    -------
    fig : `matplotlib.figure.Figure`
        Overview figure.
    """
    _ensure_matplotlib()

    if column == 1:
        fig, axes = plt.subplots(2, 1, figsize=(8, 10))
        ax_list = [axes[0], axes[1]]
    else:
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        ax_list = list(axes.flat)

    # Draw into each subplot axis (2 plots for column=1, 4 for column=2)
    plot_task_boxplot(analyzer, tasks=tasks, ax=ax_list[0])
    plot_time_stacked_bar(analyzer, sort_by='run_pct', ax=ax_list[1])
    if len(ax_list) > 2:
        plot_scatter_memory_vs_time(analyzer, tasks=tasks,
                                    highlight_top_n=20, ax=ax_list[2])
    if len(ax_list) > 3:
        plot_top_n_bar(analyzer, n=15, ax=ax_list[3])

    fig.suptitle('Quantum Runtime Overview', fontsize=14, y=1.02)
    fig.tight_layout()
    return fig
