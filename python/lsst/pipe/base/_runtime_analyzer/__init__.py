"""Quantum Runtime Analyzer — analyze quantum resource usage from pipeline runs.

This package provides a programmatic API and CLI for extracting,
aggregating, and visualizing quantum resource usage data from
ProvenanceQuantumGraph objects.  Extracted runtime tables can be
cached as Parquet files and reloaded directly from the cache, and the
:mod:`.tracker` subpackage persists analysis runs to a local SQLite
database for run-to-run comparison, trend analysis, and alerts.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .tracker import database

__all__ = [
    "QuantumRuntimeAnalyzer",
    "QuantumRuntimeTable",
    "check_alerts",
    "classify_effect",
    "cohens_d",
    "compare_runs",
    "cpu_efficiency",
    "evaluate_significance",
    "export_csv",
    "export_parquet",
    "extract_merged_runtime_table",
    "extract_runtime_table",
    "find_task_changes",
    "format_table",
    "get_tracker_plot_names",
    "get_trend",
    "group_by_codes",
    "io_pct",
    "iqr_outlier_thresholds",
    "linear_regression",
    "list_runs",
    "main",
    "mann_whitney_u_test",
    "percentile",
    "plot_delta_bar",
    "plot_dimension_dist",
    "plot_heatmap",
    "plot_histogram",
    "plot_overview",
    "plot_percentile_curve",
    "plot_scatter_memory_vs_time",
    "plot_task_boxplot",
    "plot_time_stacked_bar",
    "plot_top_n_bar",
    "plot_trend",
    "record_run",
    "save_tracker_plot",
    "welch_t_test_approx",
    "zscore_outlier_thresholds",
]

# Public name -> defining submodule (relative to this package).  PEP 562
# lazy API: importing this package never imports the submodules (and so
# never requires the optional third-party dependencies they pull in —
# click, matplotlib, pyarrow); each name is imported, and cached
# into module globals(), on first attribute access.  ``list_runs`` is
# absent from this map because it is defined below and defers its own
# submodule import.
_SUBMODULE_FOR_NAME = {
    "QuantumRuntimeAnalyzer": ".core",
    "QuantumRuntimeTable": ".runtime_table",
    "extract_merged_runtime_table": ".runtime_table",
    "extract_runtime_table": ".runtime_table",
    "plot_task_boxplot": ".plot.boxplot",
    "plot_top_n_bar": ".plot.bars",
    "plot_scatter_memory_vs_time": ".plot.scatter",
    "plot_time_stacked_bar": ".plot.stacked",
    "plot_dimension_dist": ".plot.dimension",
    "plot_histogram": ".plot.histogram",
    "plot_percentile_curve": ".plot.percentile",
    "plot_overview": ".plot.overview",
    "format_table": ".console",
    "export_csv": ".console",
    "export_parquet": ".console",
    "percentile": ".stats",
    "group_by_codes": ".stats",
    "iqr_outlier_thresholds": ".stats",
    "zscore_outlier_thresholds": ".stats",
    "io_pct": ".stats",
    "cpu_efficiency": ".stats",
    "main": ".cli",
    # Tracker package
    "cohens_d": ".tracker.stats",
    "classify_effect": ".tracker.stats",
    "welch_t_test_approx": ".tracker.stats",
    "mann_whitney_u_test": ".tracker.stats",
    "linear_regression": ".tracker.stats",
    "evaluate_significance": ".tracker.stats",
    "compare_runs": ".tracker.history",
    "find_task_changes": ".tracker.history",
    "get_trend": ".tracker.history",
    "check_alerts": ".tracker.history",
    "plot_trend": ".tracker.plot",
    "plot_heatmap": ".tracker.plot",
    "plot_delta_bar": ".tracker.plot",
    "get_tracker_plot_names": ".tracker.plot",
    "save_tracker_plot": ".tracker.plot",
    "record_run": ".tracker.database",
}


def __getattr__(name: str) -> Any:
    """Resolve public API names on first access (PEP 562).

    Imports the defining submodule, fetches the name from it, and
    caches the value into module globals() so subsequent lookups skip
    this hook entirely.  Unknown names raise ``AttributeError`` (so
    ``hasattr`` and ``dir()`` behave normally).

    ``__version__`` is not defined here: the package ships with
    pipe_base and reports ``lsst.pipe.base.version.__version__``,
    resolved lazily on first access and likewise cached.
    """
    if name == "__version__":
        from lsst.pipe.base.version import __version__ as pipe_base_version
        globals()["__version__"] = pipe_base_version
        return pipe_base_version
    submodule = _SUBMODULE_FOR_NAME.get(name)
    if submodule is None:
        raise AttributeError(
            f"module {__name__!r} has no attribute {name!r}"
        )
    value = getattr(importlib.import_module(submodule, __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Return the public API names (so tab completion lists them)."""
    return sorted(__all__)


def list_runs(
    limit: int = 20, task_filter: str | None = None
) -> list[database.RunRecord]:
    """Top-level alias for ``database.get_runs``."""
    from .tracker.database import get_runs
    return get_runs(limit=limit, task_filter=task_filter)
