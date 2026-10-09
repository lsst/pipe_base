"""Matplotlib visualization functions for quantum runtime analysis.

Provides plot functions for box plots, scatter plots, histograms,
stacked bar charts, dimension distributions, percentile curves,
and overview layouts.

The package is organized as cohesive submodules (one plot family per
module); this ``__init__`` re-exports the public plot functions and the
shared helpers.  The module-level ``matplotlib.use('Agg')`` guard lives
in exactly one place, :mod:`lsst.pipe.base._runtime_analyzer.plot._style`, and runs
when the package is first imported (submodules import their
``plt``/``Figure`` bindings from there).
"""

from __future__ import annotations

__all__ = [
    "plot_dimension_dist",
    "plot_histogram",
    "plot_overview",
    "plot_percentile_curve",
    "plot_scatter_memory_vs_time",
    "plot_task_boxplot",
    "plot_time_stacked_bar",
    "plot_top_n_bar",
]

# Shared setup (single matplotlib 'Agg' backend guard) and helpers.
from ._registry import (  # noqa: F401
    _PLOT_FUNCTIONS,
    _embed_figure,
    get_available_plots,
    get_plot_by_name,
)
from ._style import (  # noqa: F401
    _ensure_matplotlib,
    _HAS_MATPLOTLIB,
    _METRIC_LABELS,
    _metric_label,
    _columns,
    _extract_dimension_value,
    _get_tasks_data,
    _make_task_colors,
    Figure,
    matplotlib,
    np,
    plt,
    QuantumRuntimeTable,
)
from .bars import plot_top_n_bar
from .boxplot import plot_task_boxplot
from .dimension import (  # noqa: F401
    _create_facet_axes,
    _plot_dimension_boxplot,
    _plot_dimension_histogram,
    plot_dimension_dist,
)
from .histogram import plot_histogram
from .overview import plot_overview
from .percentile import plot_percentile_curve
from .scatter import plot_scatter_memory_vs_time
from .stacked import plot_time_stacked_bar
