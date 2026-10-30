"""Name-to-function registry for looking up plot functions by name."""

from __future__ import annotations

from collections.abc import Callable

from ._style import Figure, matplotlib
from .bars import plot_top_n_bar
from .boxplot import plot_task_boxplot
from .dimension import plot_dimension_dist
from .histogram import plot_histogram
from .overview import plot_overview
from .percentile import plot_percentile_curve
from .scatter import plot_scatter_memory_vs_time
from .stacked import plot_time_stacked_bar

# Pre-made layout mapping
_PLOT_FUNCTIONS: dict[str, Callable[..., Figure]] = {
    'box': plot_task_boxplot,
    'scatter': plot_scatter_memory_vs_time,
    'bar': plot_top_n_bar,
    'stacked': plot_time_stacked_bar,
    'dim': plot_dimension_dist,
    'hist': plot_histogram,
    'percentile': plot_percentile_curve,
    'overview': plot_overview,
}


def _embed_figure(
    source_fig: Figure,
    target_ax: matplotlib.axes.Axes,
) -> None:
    """Embed the content of one figure into another's axes.

    .. deprecated::
        This function is a no-op. Call the individual plot functions directly
        with the ``ax`` parameter to draw into a specific axis.

    Parameters
    ----------
    source_fig : `matplotlib.figure.Figure`
        Source figure to copy from.
    target_ax : `matplotlib.axes.Axes`
        Target axes to draw into.
    """
    pass  # No-op; use individual plot functions with ax= instead.


def get_available_plots() -> list[str]:
    """Return a list of available plot names.

    Returns
    -------
    plots : `list` of `str`
        List of plot names: box, scatter, bar, stacked, dim, hist,
        percentile, overview.
    """
    return list(_PLOT_FUNCTIONS.keys())


def get_plot_by_name(name: str) -> Callable[..., Figure] | None:
    """Look up a plot function by name.

    Parameters
    ----------
    name : `str`
        Plot name (e.g. "box", "scatter").

    Returns
    -------
    func : callable or `None`
        The plot function, or ``None`` if not found.
    """
    return _PLOT_FUNCTIONS.get(name)
