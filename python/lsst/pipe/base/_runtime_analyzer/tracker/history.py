"""History and comparison logic for the runtime tracker.

Provides functions to compare runs, detect trends, check alerts,
and auto-compare on recording.
"""

from __future__ import annotations

import math
import types
from collections.abc import Mapping
from typing import Any, cast

from . import database
from .stats import (
    cohens_d,
    evaluate_significance,
    linear_regression,
    mann_whitney_u_test,
    welch_t_test_approx,
)


def _import_click() -> types.ModuleType:
    """Import ``click`` on demand so importing this module needs no click."""
    import click
    return click


def _resolve_metric(row: Mapping[str, object], metric: str) -> float:
    """Return ``row[metric]`` as a float, falling back to ``p50``.

    The fallback to ``p50`` triggers *only* when the requested ``metric``
    column is absent or ``None``.  A legitimately-zero metric value
    (``0.0``) is preserved rather than silently replaced by ``p50``.

    Parameters
    ----------
    row : `Mapping`
        A task-summary row (as returned by ``database.get_task_summary``).
    metric : `str`
        The requested metric column name (e.g. ``"p95"``).

    Returns
    -------
    value : `float`
        The resolved metric value; ``0.0`` if neither ``metric`` nor ``p50``
        resolves to a numeric value.
    """
    value = row.get(metric)
    if value is None:
        value = row.get("p50")
    if value is None:
        return 0.0
    try:
        return float(cast(Any, value))
    except (TypeError, ValueError):
        return 0.0


def compare_runs(
    from_label: str,
    to_label: str,
    metric: str = "p50",
    diff_mode: str = "percent",
) -> list[dict]:
    """Compare two recorded runs task-by-task.

    Parameters
    ----------
    from_label : `str`
        Label of the baseline run.
    to_label : `str`
        Label of the new run.
    metric : `str`, optional
        Metric name to compare. Default ``"p50"``.
    diff_mode : `str`, optional
        Either ``"percent"`` or ``"absolute"``. Default ``"percent"``.

    Returns
    -------
    comparisons : `list` of `dict`
        One dict per shared task (sorted by task label) with keys
        ``task_label``, ``metric_from``, ``metric_to``, ``delta``,
        ``delta_pct``, ``cohens_d``, ``status``, ``p_value``,
        ``from_run_id``, and ``to_run_id``.  ``delta_pct`` is relative to
        ``|metric_from|`` and is ``0.0`` when ``metric_from`` is zero.

    Raises
    ------
    ValueError
        Raised if no run matches ``from_label`` or ``to_label``.
    """
    from_run = database.get_run_by_label(from_label)
    to_run = database.get_run_by_label(to_label)

    if from_run is None:
        raise ValueError(f"No run found with label {from_label!r}")
    if to_run is None:
        raise ValueError(f"No run found with label {to_label!r}")

    from_summary = database.get_task_summary(from_run["run_id"])
    to_summary = database.get_task_summary(to_run["run_id"])

    shared_tasks = set(from_summary.keys()) & set(to_summary.keys())
    comparisons = []

    for task_label in sorted(shared_tasks):
        from_row = from_summary[task_label]
        to_row = to_summary[task_label]

        # Resolve the requested metric, falling back to p50 only when the
        # column is absent or None (a legitimate 0.0 value is preserved).
        val_from = _resolve_metric(from_row, metric)
        val_to = _resolve_metric(to_row, metric)

        mean_from = from_row.get("mean_rt", 0.0) or 0.0
        std_from = from_row.get("std_rt", 0.0) or 0.0
        n_from = from_row.get("quanta", 1) or 1
        mean_to = to_row.get("mean_rt", 0.0) or 0.0
        std_to = to_row.get("std_rt", 0.0) or 0.0
        n_to = to_row.get("quanta", 1) or 1

        # Delta
        if diff_mode == "absolute":
            delta = val_to - val_from
            delta_pct = 0.0
            if val_from != 0:
                delta_pct = (delta / abs(val_from)) * 100.0
        else:
            if val_from != 0:
                delta_pct = ((val_to - val_from) / abs(val_from)) * 100.0
            else:
                delta_pct = 0.0
            delta = val_to - val_from

        # Statistical significance
        c_d = cohens_d(mean_from, std_from, n_from, mean_to, std_to, n_to)
        p_val = welch_t_test_approx(mean_from, std_from, n_from, mean_to, std_to, n_to)
        status = evaluate_significance(c_d, p_val)

        comparisons.append({
            "task_label": task_label,
            "metric_from": val_from,
            "metric_to": val_to,
            "delta": delta,
            "delta_pct": delta_pct,
            "cohens_d": c_d,
            "p_value": p_val,
            "status": status,
            "from_run_id": from_run["run_id"],
            "to_run_id": to_run["run_id"],
        })

    return comparisons


def find_task_changes(
    from_label: str,
    to_label: str,
) -> tuple[set, set, set]:
    """Find tasks added, removed, or shared between two runs.

    Parameters
    ----------
    from_label : `str`
        Label of the baseline run.
    to_label : `str`
        Label of the new run.

    Returns
    -------
    added : `set`
        Task labels only present in the newer run.
    removed : `set`
        Task labels only present in the baseline run.
    both : `set`
        Task labels present in both runs.

    Raises
    ------
    ValueError
        Raised if no run matches ``from_label`` or ``to_label``.
    """
    from_run = database.get_run_by_label(from_label)
    to_run = database.get_run_by_label(to_label)

    if from_run is None:
        raise ValueError(f"No run found with label {from_label!r}")
    if to_run is None:
        raise ValueError(f"No run found with label {to_label!r}")

    from_summary = database.get_task_summary(from_run["run_id"])
    to_summary = database.get_task_summary(to_run["run_id"])

    from_tasks = set(from_summary.keys())
    to_tasks = set(to_summary.keys())

    added = to_tasks - from_tasks
    removed = from_tasks - to_tasks
    both = from_tasks & to_tasks

    return added, removed, both


def get_trend(
    task_label: str,
    metric: str = "p50",
) -> dict:
    """Analyze a metric's progression across runs for a task.

    Parameters
    ----------
    task_label : `str`
        Task to analyze.
    metric : `str`, optional
        Metric to track.  Names other than ``p25``, ``p50``, ``p75``,
        ``p95``, and ``mean_rt`` fall back to ``p50``.  Default
        ``"p50"``.

    Returns
    -------
    trend : `dict`
        Keys ``run_data`` (oldest-to-newest dicts with ``label``,
        ``timestamp``, ``run_id``, and ``value``), ``slope``,
        ``intercept``, ``r_squared``, ``p_value``, and ``n_runs``.  With
        fewer than three matching runs, ``run_data`` is empty and the
        statistics are zeros with ``p_value=1.0``.
    """
    all_summaries = database.get_all_task_summaries(task_filter=task_label)

    # Group by run and take most recent value per run
    runs_seen: dict[str, Mapping[str, object]] = {}
    for entry in all_summaries:
        run_id = entry["run_id"]
        if entry["task_label"] != task_label:
            continue
        if run_id not in runs_seen:
            runs_seen[run_id] = entry

    # Get timestamps (get_all_task_summaries is already ordered by
    # timestamp desc).
    # Build a timestamp index from get_runs
    all_runs = database.get_runs(limit=500)
    run_timestamps: dict[str, float] = {r["run_id"]: r["timestamp"] for r in all_runs}

    # Sort runs by timestamp ascending
    run_ids = sorted(runs_seen.keys(), key=lambda rid: run_timestamps.get(rid, 0))

    if len(run_ids) < 3:
        return {
            "run_data": [],
            "slope": 0.0,
            "r_squared": 0.0,
            "p_value": 1.0,
            "intercept": 0.0,
            "n_runs": len(run_ids),
        }

    # Ensure metric key exists, fall back to p50
    metric_col = metric if metric in ("p50", "p95", "mean_rt", "p25", "p75") else "p50"

    run_data = []
    timestamps = []
    values = []

    for run_id in run_ids:
        run_entry = runs_seen[run_id]
        ts = run_timestamps.get(run_id, 0)
        val = run_entry.get(metric_col) or run_entry.get("p50") or 0.0
        try:
            val = float(cast(Any, val))
        except (TypeError, ValueError):
            val = 0.0

        run_data.append({
            "label": run_entry["label"],
            "timestamp": ts,
            "run_id": run_id,
            "value": val,
        })
        timestamps.append(ts)
        values.append(val)

    # Linear regression over timestamps (use ordinal index for x)
    x_data = list(range(len(values)))
    slope, intercept, r_squared, p_value = linear_regression(x_data, values)

    return {
        "run_data": run_data,
        "slope": slope,
        "intercept": intercept,
        "r_squared": r_squared,
        "p_value": p_value,
        "n_runs": len(values),
    }


def _metric_to_raw_column(metric: str) -> str:
    """Map a summary metric name to a ``quanta_raw`` column for testing.

    Runtime percentile metrics (``p50``/``p95``/``mean_rt``/...) are derived
    from the per-quantum ``run_time`` column; memory metrics map to
    ``memory``.

    Parameters
    ----------
    metric : `str`
        Requested metric name.

    Returns
    -------
    column : `str`
        A whitelisted ``quanta_raw`` column name.
    """
    if "mem" in metric:
        return "memory"
    return "run_time"


def _summary_stats(values: list[float]) -> tuple[float, float, int]:
    """Return ``(mean, sample_std, n)`` for a list of floats."""
    n = len(values)
    if n == 0:
        return 0.0, 0.0, 0
    mean = sum(values) / n
    if n < 2:
        return mean, 0.0, n
    var = sum((v - mean) ** 2 for v in values) / (n - 1)
    return mean, math.sqrt(var), n


def check_alerts(
    from_run_label: str | None = None,
    task_filter: str | None = None,
    metric: str = "p50",
    p_threshold: float = 0.05,
    effect_threshold: float = 0.3,
    delta_threshold_pct: float = 10.0,
) -> list[dict]:
    """Scan all recorded runs for significant performance changes.

    Compares each run with its predecessor in time and flags changes that are
    *both* statistically significant and practically large.

    Metric alignment
    ----------------
    The reported ``delta_pct`` is always computed from the requested
    ``metric`` column (falling back to ``p50`` when that column is absent or
    ``NULL``). The significance test statistic is aligned with the requested
    metric whenever possible:

    * If raw per-quantum data exists for *both* runs' task (recorded with
      ``--raw``), a non-parametric Mann-Whitney U test is run directly on the
      per-quantum ``run_time``/``memory`` values, and Cohen's d is computed
      from the raw sample statistics.
    * Otherwise the test falls back to the summary-statistics Welch
      approximation on ``mean_rt``/``std_rt``. This is an *approximation*:
      the significance is inferred from the mean/standard-deviation of the
      full task population rather than the specific percentile the alert
      reports, so it can disagree with the reported ``delta_pct``. Because of
      this, an alert only fires when the test is significant *and* the
      requested metric's own ``|delta_pct|`` reaches ``delta_threshold_pct``.

    Parameters
    ----------
    from_run_label : `str` or `None`, optional
        If provided, only consider runs as the "newer" run starting
        from the run matching this label (inclusive).
    task_filter : `str` or `None`, optional
        Only include alerts for tasks matching this substring.
    metric : `str`, optional
        Metric to evaluate. Default ``"p50"``.
    p_threshold : `float`, optional
        P-value significance cutoff. Default 0.05.
    effect_threshold : `float`, optional
        |Cohen's d| cutoff. Default 0.3.
    delta_threshold_pct : `float`, optional
        Minimum ``|delta_pct|`` (percent) on the requested metric for an
        alert to fire, in addition to the significance thresholds.
        Default 10.0.

    Returns
    -------
    alerts : `list` of `dict`
        Each dict has keys ``task_label``, ``from_run``, ``to_run``,
        ``metric``, ``metric_from``, ``metric_to``, ``delta_pct``,
        ``cohens_d``, ``p_value``, ``status``, ``significance_source``,
        ``from_run_id``, ``to_run_id``; sorted by ``|cohens_d|``
        descending.

    Raises
    ------
    ValueError
        Raised if ``from_run_label`` is given but no recorded run carries
        that label.
    """
    runs = database.get_runs(limit=500)
    if len(runs) < 2:
        return []

    # Find cutoff index if from_run_label is specified. If a label is given
    # but no run matches it, raise rather than silently scanning every run
    # from index 0 (which would produce alerts the caller did not ask for).
    cutoff_idx = 0
    if from_run_label:
        for i in range(len(runs) - 1, -1, -1):
            if runs[i]["label"] == from_run_label:
                cutoff_idx = i
                break
        else:
            raise ValueError(
                f"No run found with label {from_run_label!r}; cannot restrict "
                f"the alert scan to start from this label."
            )

    alerts = []
    for i in range(cutoff_idx, len(runs) - 1):
        run_to = runs[i]
        run_from = runs[i + 1]  # next in list (older since descending order)

        summary_to = database.get_task_summary(run_to["run_id"])
        summary_from = database.get_task_summary(run_from["run_id"])

        shared = set(summary_to.keys()) & set(summary_from.keys())
        for task_label in sorted(shared):
            if task_filter and task_filter not in task_label:
                continue

            row_from = summary_from[task_label]
            row_to = summary_to[task_label]

            # Delta is always reported on the requested metric. The
            # resolver uses an explicit None sentinel so a legitimate 0.0
            # metric is NOT conflated with an absent column and silently
            # replaced by p50.  Division below is guarded so a legitimate
            # metric_from == 0.0 yields delta_pct 0.0.
            metric_from = _resolve_metric(row_from, metric)
            metric_to = _resolve_metric(row_to, metric)
            if metric_from != 0:
                delta_pct = ((metric_to - metric_from) / abs(metric_from)) * 100.0
            else:
                delta_pct = 0.0

            # Significance: prefer raw per-quantum data aligned with metric,
            # otherwise fall back to summary mean/std Welch approximation.
            raw_col = _metric_to_raw_column(metric)
            raw_from = database.get_raw_quantum_values(
                run_from["run_id"], task_label, raw_col
            )
            raw_to = database.get_raw_quantum_values(
                run_to["run_id"], task_label, raw_col
            )

            if len(raw_from) >= 2 and len(raw_to) >= 2:
                mean_from, std_from, n_from = _summary_stats(raw_from)
                mean_to, std_to, n_to = _summary_stats(raw_to)
                c_d = cohens_d(mean_from, std_from, n_from, mean_to, std_to, n_to)
                try:
                    _, p_val = mann_whitney_u_test(raw_from, raw_to)
                except ImportError:
                    p_val = welch_t_test_approx(
                        mean_from, std_from, n_from, mean_to, std_to, n_to
                    )
                significance_source = "raw"
            else:
                mean_from = float(row_from.get("mean_rt", 0.0) or 0.0)
                std_from = float(row_from.get("std_rt", 0.0) or 0.0)
                n_from = int(row_from.get("quanta", 1) or 1)
                mean_to = float(row_to.get("mean_rt", 0.0) or 0.0)
                std_to = float(row_to.get("std_rt", 0.0) or 0.0)
                n_to = int(row_to.get("quanta", 1) or 1)
                c_d = cohens_d(mean_from, std_from, n_from, mean_to, std_to, n_to)
                p_val = welch_t_test_approx(
                    mean_from, std_from, n_from, mean_to, std_to, n_to
                )
                significance_source = "summary"

            # Fire only when the test is significant AND the requested
            # metric itself moved by at least delta_threshold_pct.
            if (
                p_val < p_threshold
                and abs(c_d) > effect_threshold
                and abs(delta_pct) >= delta_threshold_pct
            ):
                alerts.append({
                    "task_label": task_label,
                    "from_run": run_from["label"],
                    "to_run": run_to["label"],
                    "metric": metric,
                    "metric_from": metric_from,
                    "metric_to": metric_to,
                    "delta_pct": delta_pct,
                    "cohens_d": c_d,
                    "p_value": p_val,
                    "status": evaluate_significance(c_d, p_val),
                    "significance_source": significance_source,
                    "from_run_id": run_from["run_id"],
                    "to_run_id": run_to["run_id"],
                })

    # Sort by Cohen's d descending
    alerts.sort(key=lambda a: abs(a["cohens_d"]), reverse=True)
    return alerts


def _auto_compare(label: str) -> list[dict] | None:
    """Auto-compare a newly recorded run with the most recent comparable run.

    Invoked by the ``tracked-run record`` CLI flow after a successful
    record. Renders a compact comparison table (via
    :func:`~lsst.pipe.base._runtime_analyzer.tracker.format.
    format_comparison_table`) to stdout and returns the raw comparison
    list.

    Parameters
    ----------
    label : `str`
        Label of the run that was just recorded.

    Returns
    -------
    comparison : `list` of `dict` or `None`
        Comparison results (see :func:`compare_runs`), or ``None`` if there
        is no older comparable run.
    """
    click = _import_click()

    runs_in_db = database.get_runs(limit=50)
    if len(runs_in_db) < 2:
        return None

    # Find the newly recorded run in the list
    current_run = database.get_run_by_label(label)
    if current_run is None:
        return None

    current_idx = None
    for i, run in enumerate(runs_in_db):
        if run["run_id"] == current_run["run_id"]:
            current_idx = i
            break

    if current_idx is None or current_idx >= len(runs_in_db) - 1:
        return None

    # The most recent other run is the one right after in the list (since
    # get_runs returns runs ordered by timestamp descending).
    older_run = runs_in_db[current_idx + 1]

    comparisons = compare_runs(older_run["label"], label)
    added, removed, _shared = find_task_changes(older_run["label"], label)

    click.echo("")
    click.echo(
        f"--- Auto-comparison with most recent run: '{older_run['label']}' ---"
    )
    click.echo("")

    from . import format as tracker_format

    render = getattr(tracker_format, "format_comparison_table", None)
    if callable(render):
        click.echo(render(
            older_run, current_run, comparisons, sorted(added),
            sorted(removed),
        ))
    else:
        # Fallback: compact inline table if the formatter is unavailable or
        # does not accept this shape.
        if not comparisons:
            click.echo("No shared tasks between the two runs.")
        else:
            for comp in comparisons:
                click.echo(
                    f"{comp['task_label']:<30} "
                    f"{comp['metric_from']:>10.3f} -> {comp['metric_to']:>10.3f} "
                    f"({comp['delta_pct']:+.1f}%) [{comp['status']}]"
                )

    return comparisons
