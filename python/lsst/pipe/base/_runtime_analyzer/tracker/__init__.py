"""Quantum runtime tracker — record, compare, trend, and alert on analysis runs.

This package provides functionality for persisting quantum runtime analysis
runs to a local SQLite database and comparing them over time.
"""

from __future__ import annotations

# Database layer
from .database import (
    create_connection,
    get_all_task_summaries,
    get_db_path,
    get_run,
    get_run_by_label,
    get_runs,
    get_task_summary,
    hash_graph,
    load_config,
    record_run,
    update_run,
)
# History / comparison logic
from .history import (
    check_alerts,
    compare_runs,
    find_task_changes,
    get_trend,
)
# Plot functions
from .plot import (
    get_tracker_plot_names,
    plot_delta_bar,
    plot_heatmap,
    plot_trend,
    save_tracker_plot,
)
# Statistical functions
from .stats import (
    classify_effect,
    cohens_d,
    evaluate_significance,
    linear_regression,
    mann_whitney_u_test,
    welch_t_test_approx,
)

__all__ = [
    "check_alerts",
    "classify_effect",
    "cohens_d",
    "compare_runs",
    "create_connection",
    "evaluate_significance",
    "find_task_changes",
    "get_all_task_summaries",
    "get_db_path",
    "get_run",
    "get_run_by_label",
    "get_runs",
    "get_task_summary",
    "get_tracker_plot_names",
    "get_trend",
    "hash_graph",
    "linear_regression",
    "load_config",
    "mann_whitney_u_test",
    "plot_delta_bar",
    "plot_heatmap",
    "plot_trend",
    "record_run",
    "save_tracker_plot",
    "update_run",
    "welch_t_test_approx",
]
