"""Console formatting functions for the runtime tracker.

Provides formatted string output for comparison tables, run lists,
trend tables, and alerts.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .database import RunRecord


def format_comparison_table(
    from_run: Mapping[str, object],
    to_run: Mapping[str, object],
    comparisons: list,
    added_tasks: list | None = None,
    removed_tasks: list | None = None,
) -> str:
    """Format a comparison table as a string.

    Parameters
    ----------
    from_run : `Mapping`
        Baseline run metadata (has ``label`` key).
    to_run : `Mapping`
        New run metadata (has ``label`` key).
    comparisons : `list`
        Comparison dicts from :func:`compare_runs`.
    added_tasks : `list` or `None`, optional
        Task labels only in the new run.
    removed_tasks : `list` or `None`, optional
        Task labels only in the baseline run.

    Returns
    -------
    table : `str`
        Formatted string.
    """
    lines = []
    lines.append(f"Comparison: '{from_run.get('label', from_run)}' -> '{to_run.get('label', to_run)}'")
    lines.append("")

    # Column headers (only when there are shared tasks to show; the
    # added/removed sections below report tasks unique to each run).
    if not comparisons:
        lines.append("No shared tasks between the two runs.")
    else:
        header = f"{'Task':<30} {'From':>10} {'To':>10} {'Delta':>10} {'Cohen d':>10} {'Status':>10}"
        sep = "-" * len(header)
        lines.append(header)
        lines.append(sep)

    for comp in comparisons:
        task = comp["task_label"]
        from_val = f"{comp['metric_from']:.3f}"
        to_val = f"{comp['metric_to']:.3f}"

        delta_pct = comp["delta_pct"]
        if delta_pct >= 0:
            delta_str = f"+{delta_pct:+.1f}%"
            arrow = "\u2191"
        else:
            delta_str = f"{delta_pct:+.1f}%"
            arrow = "\u2193"

        d = f"{comp['cohens_d']:.2f}"

        if comp["status"] == "significant":
            status_str = "\U0001f534 signific"
        elif comp["status"] == "uncertain":
            status_str = "\u26a0 uncertain"
        else:
            status_str = "\u2713 stable"

        line = f"{task:<30} {from_val:>10} {to_val:>10} {arrow} {delta_str:>8} {d:>10} {status_str:>10}"
        lines.append(line)

    lines.append("")

    if added_tasks:
        lines.append(f"Tasks added ({len(added_tasks)}):")
        for t in sorted(added_tasks):
            lines.append(f"  + {t}")
        lines.append("")

    if removed_tasks:
        lines.append(f"Tasks removed ({len(removed_tasks)}):")
        for t in sorted(removed_tasks):
            lines.append(f"  - {t}")
        lines.append("")

    return "\n".join(lines)


def format_run_list(runs: Sequence[RunRecord]) -> str:
    """Format a run list as an aligned string table.

    Parameters
    ----------
    runs : `~collections.abc.Sequence` of `RunRecord`
        Run dicts from :func:`database.get_runs`.

    Returns
    -------
    table : `str`
        Formatted string.
    """
    if not runs:
        return "No recorded runs."

    lines = []
    header = f"{'Label':<25} {'Timestamp':<20} {'Quanta':>8} {'Tasks':>6}"
    sep = "-" * len(header)
    lines.append(header)
    lines.append(sep)

    for run in runs:
        label = run.get("label", "unknown")[:24]
        ts = run.get("timestamp", 0)
        if ts:
            dt = datetime.fromtimestamp(ts)
            ts_str = dt.strftime("%Y-%m-%d %H:%M")
        else:
            ts_str = "N/A"

        # Get quanta and task count from task_summary
        from . import database
        summary = database.get_task_summary(run["run_id"])
        total_quanta = sum(s.get("quanta", 0) or 0 for s in summary.values())
        task_count = len(summary)

        lines.append(f"{label:<25} {ts_str:<20} {total_quanta:>8} {task_count:>6}")

    return "\n".join(lines)


def format_trend_table(
    task_label: str,
    metric: str,
    trend_data: dict,
) -> str:
    """Format a trend analysis as a string table.

    Parameters
    ----------
    task_label : `str`
        Task name.
    metric : `str`
        Metric name.
    trend_data : `dict`
        Output of :func:`get_trend`.

    Returns
    -------
    table : `str`
        Formatted string.
    """
    lines = []
    slope = trend_data.get("slope", 0.0)
    r_squared = trend_data.get("r_squared", 0.0)
    p_value = trend_data.get("p_value", 1.0)
    n_runs = trend_data.get("n_runs", 0)

    if slope > 0.01:
        direction = "\u2191 slower"
    elif slope < -0.01:
        direction = "\u2193 faster"
    else:
        direction = "\u2192 stable"

    if n_runs < 3:
        lines.append(f"Need at least 3 recorded runs with task '{task_label}' for trend analysis.")
        return "\n".join(lines)

    if not trend_data.get("run_data"):
        lines.append(f"Task '{task_label}' not found in any recorded run.")
        return "\n".join(lines)

    lines.append(f"Trend for task: {task_label} ({metric})")
    lines.append(f"Runs: {n_runs} | Slope: {slope:+.6f} s/run | R\u00b2: {r_squared:.4f} | p: {p_value:.4f}")
    lines.append(f"Direction: {direction}")
    lines.append("")

    header = f"{'Label':<20} {'Timestamp':<20} {'Value':>10}"
    sep = "-" * len(header)
    lines.append(header)
    lines.append(sep)

    for rd in trend_data["run_data"]:
        ts = datetime.fromtimestamp(rd["timestamp"]).strftime("%Y-%m-%d %H:%M")
        lines.append(f"{rd['label']:<20} {ts:<20} {rd['value']:>10.3f}")

    return "\n".join(lines)


def format_alerts(alerts: list) -> str:
    """Format alerts as a string table (in the order given).

    Parameters
    ----------
    alerts : `list`
        Alert dicts from :func:`check_alerts` (which returns them sorted
        by effect size descending).

    Returns
    -------
    table : `str`
        Formatted string.
    """
    if not alerts:
        return "No significant changes detected."

    lines = []
    header = f"{'Task':<25} {'From':<20} {'To':<20} {'Delta':>10} {'Cohen d':>10} {'p-value':>10}"
    sep = "-" * len(header)
    lines.append(header)
    lines.append(sep)

    for alert in alerts:
        task = alert["task_label"]
        from_r = alert["from_run"]
        to_r = alert["to_run"]
        delta = f"{alert['delta_pct']:+.1f}%"
        # Use signed display
        d_signed = f"{alert['cohens_d']:+.2f}"
        pv = f"{alert['p_value']:.4f}"

        lines.append(f"{task:<25} {from_r:<20} {to_r:<20} {delta:>10} {d_signed:>10} {pv:>10}")

    return "\n".join(lines)
