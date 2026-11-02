"""Shared matplotlib setup and data-preparation helpers for plotting.

This module is the single home of the module-level
``matplotlib.use('Agg')`` backend guard: every plotting submodule
imports the ``plt``/``Figure`` bindings and the ``_HAS_MATPLOTLIB``
availability flag from here, so the headless backend is configured
exactly once when the plotting package is first imported.
It also holds the metric-label table and the numpy column/grouping
helpers shared by every plot function.
"""

from __future__ import annotations

import numpy as np

# Robust DataCoordinate parsers live in core; do not duplicate them here.
# ``QuantumRuntimeAnalyzer`` (parameter type) and ``_extract_dimension_value``
# are also re-exported by ``plot/__init__.py``.
from ..core import QuantumRuntimeAnalyzer, _extract_dimension_value  # noqa: F401
from ..runtime_table import QuantumRuntimeTable

# matplotlib may not be available in all environments
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.figure import Figure
    _HAS_MATPLOTLIB = True
except ImportError:
    _HAS_MATPLOTLIB = False
    Figure = object  # type: ignore[assignment,misc]
    matplotlib = None  # type: ignore[assignment]
    plt = None  # type: ignore[assignment]


def _ensure_matplotlib() -> None:
    """Ensure matplotlib is available, raising an informative error if not."""
    if not _HAS_MATPLOTLIB:
        raise ImportError(
            "Plotting requires 'matplotlib'. Install pipe_base with the "
            "[runtime] extra."
        )


_METRIC_LABELS = {
    'run_time': 'Run Time (s)',
    'memory': 'Memory (MiB)',
    'io_time': 'IO Time (s)',
    'prep_time': 'Prep Time (s)',
    'init_time': 'Init Time (s)',
}


def _metric_label(metric: str) -> str:
    """Return a human-readable axis label with units for a metric name."""
    return _METRIC_LABELS.get(metric, metric)


def _columns(qt: QuantumRuntimeTable) -> dict[str, np.ndarray]:
    """Numpy column views of an Arrow runtime table.

    Numeric columns are zero-copy views over the Arrow buffers;
    ``task_label`` and ``data_id`` are materialized numpy arrays, and
    ``quantum_id`` is a read-only ``(n, 16)`` uint8 view.

    Parameters
    ----------
    qt : `QuantumRuntimeTable`
        The table to view.

    Returns
    -------
    columns : `dict` [ `str`, `~numpy.ndarray` ]
        Column arrays keyed by quantum field name.
    """
    return {
        'task_label': qt.labels(),
        'status': qt.status_codes,
        'memory': qt.memory,
        'prep_time': qt.prep_time,
        'init_time': qt.init_time,
        'run_time': qt.run_time,
        'run_time_cpu': qt.run_time_cpu,
        'data_id': np.array(qt.data_id_list()),
        'quantum_id': qt.quantum_id_matrix,
    }


def _get_tasks_data(
    analyzer: QuantumRuntimeAnalyzer,
    tasks: list[str] | None = None,
) -> tuple[dict[str, np.ndarray], dict[str, np.ndarray]]:
    """Get filtered column views and task groups from an analyzer.

    Parameters
    ----------
    analyzer : `QuantumRuntimeAnalyzer`
        The analyzer instance.
    tasks : `list` of `str` or `None`, optional
        List of task labels to include.

    Returns
    -------
    columns : `dict` [ `str`, `~numpy.ndarray` ]
        Numpy column views (see :func:`_columns`) of the optionally
        task-filtered runtime table.
    task_groups : `dict` [ `str`, `~numpy.ndarray` ]
        Task label -> boolean mask over the columns.
    """
    columns = _columns(analyzer.table)

    if tasks is not None:
        active_mask = np.zeros(columns['task_label'].shape[0], dtype=bool)
        for tl in tasks:
            active_mask = active_mask | (columns['task_label'] == tl)
        columns = {k: v[active_mask] for k, v in columns.items()}

    task_labels = columns['task_label']
    task_groups: dict[str, np.ndarray]
    if task_labels.size > 0:
        task_groups = {}
        for tl in np.unique(task_labels):
            task_groups[tl] = task_labels == tl
    else:
        task_groups = {}
    return columns, task_groups


def _make_task_colors(task_labels: list[str]) -> list[tuple[float, float, float, float]]:
    """Generate a color for each unique task label using matplotlib tab colors.

    Parameters
    ----------
    task_labels : `list` of `str`
        Task labels to assign colors to.

    Returns
    -------
    colors : `list`
        One RGBA color tuple per distinct label, in sorted-label order.
    """
    import matplotlib.cm as cm
    unique_tasks = sorted(set(task_labels))
    cmap = cm.tab20
    return [cmap(i / max(len(unique_tasks) - 1, 1)) for i in range(len(unique_tasks))]
