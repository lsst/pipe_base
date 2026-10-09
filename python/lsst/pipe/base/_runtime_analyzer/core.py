"""Core analysis module: quantum runtime analyzer.

Provides the `QuantumRuntimeAnalyzer` class, which takes an extracted
`QuantumRuntimeTable` (produced by the table producers in
:mod:`.runtime_table`) and offers task summaries,
ranked quanta, dimension-binned distributions, and bottleneck
diagnosis.  ``QuantumRuntimeAnalyzer(table)`` is the only analyzer
construction: nothing sits between producer and analyzer.
"""

from __future__ import annotations

__all__ = [
    "QuantumRuntimeAnalyzer",
    "QuantumRuntimeTable",
    "extract_merged_runtime_table",
    "extract_runtime_table",
]

import logging
import uuid
from typing import Any

import astropy.table
import numpy as np

from .dimensions import parse_dimension_string
from .runtime_table import (
    INT_TO_STATUS,
    STATUS_MAP,
    QuantumRuntimeTable,
    extract_merged_runtime_table,
    extract_runtime_table,
)
from .stats import (
    cpu_efficiency,
    group_by_codes,
    iqr_outlier_thresholds,
    percentile,
    zscore_outlier_thresholds,
)

_LOG = logging.getLogger(__name__)


def _io_pct_safe(run_time: np.ndarray, run_time_cpu: np.ndarray) -> np.ndarray:
    """Compute I/O percentage without RuntimeWarning on zero run_time."""
    rt = np.asarray(run_time, dtype=np.float64)
    rtc = np.asarray(run_time_cpu, dtype=np.float64)
    total = rt - rtc
    result = np.zeros_like(rt)
    with np.errstate(divide='ignore', invalid='ignore'):
        mask = rt > 0
        result[mask] = total[mask] / rt[mask] * 100.0
    return result


# QuantumRuntimeTable, the graph -> table producers, the Arrow schema,
# and the Parquet cache engine are defined in `.runtime_table` (imported
# above): the canonical, Arrow-native seam the analyzer consumes.

# Explicit alias table for user-facing status filters: "failed" matches
# both FAILED and ABORTED (graceful and hard failures), while "aborted"
# matches only ABORTED (never ABORTED_SUCCESS).  Unknown filter names
# match nothing.
_STATUS_FILTER_ALIASES: dict[str, frozenset[int]] = {
    'FAIL': frozenset({STATUS_MAP['FAILED'], STATUS_MAP['ABORTED']}),
    'FAILED': frozenset({STATUS_MAP['FAILED'], STATUS_MAP['ABORTED']}),
    'ABORT': frozenset({STATUS_MAP['ABORTED']}),
    'ABORTED': frozenset({STATUS_MAP['ABORTED']}),
    'ABORTED_SUCCESS': frozenset({STATUS_MAP['ABORTED_SUCCESS']}),
    'SUCCESS': frozenset({STATUS_MAP['SUCCESSFUL']}),
    'SUCCEEDED': frozenset({STATUS_MAP['SUCCESSFUL']}),
    'SUCCESSFUL': frozenset({STATUS_MAP['SUCCESSFUL']}),
    'BLOCKED': frozenset({STATUS_MAP['BLOCKED']}),
    'UNKNOWN': frozenset({STATUS_MAP['UNKNOWN']}),
}


def _outlier_extremity(magnitude: float, median_rt: float) -> float:
    """Symmetric extremity of an outlier ratio from the median.

    ``bottleneck`` ranks outliers before truncating with ``[:top_n]``.
    Ranking by the raw ratio (``run_time / median``) buries extreme FAST
    outliers (ratio < 1) below every SLOW outlier, silently dropping them
    when ``top_n`` is small.  This key measures distance from the median
    symmetrically in log space: a row at 0.1x median is as extreme as one
    at 10x median.

    Parameters
    ----------
    magnitude : `float`
        Raw ratio ``run_time / median`` as reported in the outlier table.
    median_rt : `float`
        The task median the magnitude was computed against.

    Returns
    -------
    extremity : `float`
        ``max(magnitude, 1 / magnitude)`` for positive magnitudes; a zero
        run_time against a positive median is maximally extreme
        (``float('inf')``); anything else (no positive median, negative
        magnitude) ranks as 0.0.
    """
    if magnitude > 0.0:
        return max(magnitude, 1.0 / magnitude)
    if magnitude == 0.0 and median_rt > 0.0:
        # run_time == 0 against a positive median: maximally FAST.
        return float('inf')
    return 0.0


# Metrics supported by QuantumRuntimeAnalyzer.top_quantities(); matches the
# CLI '--metric' click.Choice values in cli.py.  Validating against this
# allow-list keeps unsupported (or purely numeric-but-meaningless here)
# dtype fields from being silently ranked as run_time.
_TOP_QUANTITIES_METRICS: tuple[str, ...] = ("run_time", "memory")


class QuantumRuntimeAnalyzer:
    """Analyzes quantum resource usage from an extracted runtime table.

    Takes a
    :class:`~lsst.pipe.base._runtime_analyzer.runtime_table.QuantumRuntimeTable`
    (the single-chunk Arrow table of per-quantum timing, memory, and
    status data) and analyzes it for runs with hundreds to millions of
    quanta.

    Parameters
    ----------
    table : `QuantumRuntimeTable`
        The extracted runtime table to analyze (e.g. from
        :func:`extract_runtime_table`,
        :func:`extract_merged_runtime_table`, or
        :meth:`QuantumRuntimeTable.from_parquet`).

    Examples
    --------
    >>> with ProvenanceQuantumGraph.from_args(  # doctest: +SKIP
    ...     "my_repo", collection="calibration_v12", datasets=()
    ... ) as (qg, butler):
    ...     analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(qg))
    ...     summary = analyzer.summary()
    ...     print(summary.pformat())

    >>> # The normal loading path (ordered (path, collection) sources):
    >>> analyzer = QuantumRuntimeAnalyzer(extract_merged_runtime_table(
    ...     [("my_repo", "calibration_v12")],
    ... ))  # doctest: +SKIP
    """

    def __init__(self, table: QuantumRuntimeTable) -> None:
        """Initialize the analyzer from an extracted runtime table.

        Parameters
        ----------
        table : `QuantumRuntimeTable`
            The extracted runtime table backing the analyzer.  The
            ``n_expected`` and ``n_sources`` properties mirror the
            table's bookkeeping fields; the source labels on the table
            (``analyzer.table.sources``) are whatever the producing
            call recorded there (extraction producers stamp graph-file
            paths; cache loads stamp the cache path).
        """
        self._table = table
        self._n_expected = table.n_expected
        self._n_sources = table.n_sources

    @property
    def n_loaded(self) -> int:
        """Total quanta loaded (from a single graph or merged sources).

        Always the number of rows in the backing table.  For
        analyzers with more than one source (or programmatically
        built tables), ``n_loaded`` is the reliable count.

        Returns
        -------
        n : `int`
            Number of quanta in the backing table.
        """
        return self._table.n_rows

    @property
    def n_expected(self) -> int:
        """Number of expected quanta recorded on the backing table.

        Mirrors the table's ``n_expected``.  This is ``0`` for
        analyzers backed by tables from
        :func:`~lsst.pipe.base._runtime_analyzer.runtime_table.extract_merged_runtime_table`
        with more than one source (header sums would double-count shared
        quanta), and for Parquet cache loads
        (:meth:`~lsst.pipe.base._runtime_analyzer.runtime_table.QuantumRuntimeTable.from_parquet`/
        :meth:`~lsst.pipe.base._runtime_analyzer.runtime_table.QuantumRuntimeTable.from_parquets`);
        single-source extractions carry the header count.

        Returns
        -------
        n : `int`
            Total expected quanta.
        """
        return self._n_expected

    @property
    def n_sources(self) -> int:
        """Number of sources recorded on the backing table.

        Returns
        -------
        n : `int`
            Mirrors the table's ``n_sources``: 1 for tables extracted
            from a single graph source, the actual count for merged
            tables, and 0 for bare dataclass constructions.
        """
        return self._n_sources

    def as_table(self) -> QuantumRuntimeTable:
        """Return the analyzer's backing table itself.

        Returns the same frozen table object the analyzer was
        constructed with (not a copy; derived views such as
        ``dim_lookup`` are shared).  Suitable for serialization
        (``to_parquet``) or hand-off to other processes.

        Returns
        -------
        table : `QuantumRuntimeTable`
            The analyzer's backing table (frozen value object).
        """
        return self._table

    @property
    def table(self) -> QuantumRuntimeTable:
        """The backing runtime table.

        Returns
        -------
        table : `QuantumRuntimeTable`
            Arrow-backed runtime table with quantum details.
        """
        return self._table

    @property
    def dim_lookup(self) -> dict[bytes, dict[str, str]]:
        """Captured dimension lookup keyed by 16-byte quantum id.

        Returns
        -------
        dims : `dict` [ `bytes`, `dict` [ `str`, `str` ] ]
            Mapping from ``quantum_id.bytes`` to ``{dimension: value}``;
            the backing table's derived dimension view.
        """
        return self._table.dim_lookup

    def _filtered_table(
        self,
        status: str | None = None,
        task_label: str | None = None,
    ) -> QuantumRuntimeTable:
        """Filter the backing table by status name or task label.

        Parameters
        ----------
        status : `str` or `None`, optional
            Status filter (e.g., "SUCCESSFUL", "FAILED", "BLOCKED");
            matched case-insensitively against the alias table.  An
            unknown status name selects no rows (with a log warning).
        task_label : `str` or `None`, optional
            Task label filter.

        Returns
        -------
        filtered : `QuantumRuntimeTable`
            Selected rows, or the full backing table when no filters apply.
        """
        qt = self._table
        if status is None and task_label is None:
            return qt

        mask = np.ones(qt.n_rows, dtype=bool)

        if status is not None:
            status_name = str(status).upper()
            status_codes = _STATUS_FILTER_ALIASES.get(status_name)
            if status_codes is None:
                _LOG.warning(
                    "Unknown status filter %r; no quanta will match. "
                    "Valid values: %s",
                    status, ", ".join(sorted(_STATUS_FILTER_ALIASES)),
                )
                return qt.select([])
            mask = mask & np.isin(qt.status_codes, list(status_codes))

        if task_label is not None:
            mask = mask & (qt.labels() == task_label)

        return qt.select(mask)

    def summary(self, status: str | None = None, task_label: str | None = None) -> astropy.table.Table:
        """Return aggregate performance statistics grouped by task label.

        Parameters
        ----------
        status : `str` or `None`, optional
            Filter by status name.
        task_label : `str` or `None`, optional
            Filter by task label.

        Returns
        -------
        table : `astropy.table.Table`
            One row per task (sorted by task label) with columns
            ``Task``, ``quanta``, ``mean_rt``, ``p05``, ``p25``, ``p50``,
            ``p75``, ``p95``, ``max_rt``, ``min_rt``, ``std_rt``,
            ``mean_mem``, ``median_mem``, ``max_mem``, ``mean_io_pct``,
            and ``total_rt``; run times in seconds, memory in MiB.  Empty
            when no quanta match the filters.
        """
        qt = self._filtered_table(status=status, task_label=task_label)

        if qt.n_rows == 0:
            return astropy.table.Table()

        task_groups = group_by_codes(
            qt.task_label_codes, len(qt.task_labels),
        )
        labels = qt.task_labels
        rt_all = qt.run_time
        rtc_all = qt.run_time_cpu
        mem_all = qt.memory
        rows: list[dict[str, Any]] = []

        for code in sorted(task_groups, key=lambda c: labels[c]):
            mask = task_groups[code]
            tl = labels[code]
            rt = rt_all[mask]
            rtc = rtc_all[mask]
            mem = mem_all[mask]

            run_counts = len(rt)
            pcts = [5.0, 25.0, 50.0, 75.0, 95.0]
            percentile_vals = percentile(rt, pcts)

            io_percents = _io_pct_safe(rt, rtc)

            row = {
                'Task': tl,
                'quanta': int(run_counts),
                'mean_rt': float(np.mean(rt)),
                'p05': float(percentile_vals[0]),
                'p25': float(percentile_vals[1]),
                'p50': float(percentile_vals[2]),
                'p75': float(percentile_vals[3]),
                'p95': float(percentile_vals[4]),
                'max_rt': float(np.max(rt)),
                'min_rt': float(np.min(rt)),
                'std_rt': float(np.std(rt)),
                'mean_mem': float(np.mean(mem)),
                'median_mem': float(np.median(mem)),
                'max_mem': float(np.max(mem)),
                'mean_io_pct': float(np.mean(io_percents)),
                'total_rt': float(np.sum(rt)),
            }
            rows.append(row)

        return astropy.table.Table(rows)

    def top_quantities(
        self,
        metric: str = "run_time",
        n: int = 20,
        status: str | None = None,
        task_label: str | None = None,
    ) -> astropy.table.Table:
        """Return quanta ranked by a chosen metric.

        Parameters
        ----------
        metric : `str`, optional
            Metric to rank by: "run_time" or "memory".
        n : `int`, optional
            Number of top quanta to return.
        status : `str` or `None`, optional
            Filter by status name.
        task_label : `str` or `None`, optional
            Filter by task label.

        Returns
        -------
        table : `astropy.table.Table`
            Sorted table with columns: ``rank``, ``task_label``,
            ``data_id``, ``run_time``, ``memory``, ``status``,
            ``pct_of_task_mean``; empty when no quanta match the filters.

        Raises
        ------
        ValueError
            Raised if ``metric`` is not one of ``"run_time"`` or
            ``"memory"``; validated up front, before any per-task means
            are computed.
        """
        if metric not in _TOP_QUANTITIES_METRICS:
            raise ValueError(
                f"Unsupported metric {metric!r}; expected one of "
                f"{', '.join(repr(m) for m in _TOP_QUANTITIES_METRICS)}."
            )

        qt = self._filtered_table(status=status, task_label=task_label)

        if qt.n_rows == 0:
            return astropy.table.Table()

        task_groups = group_by_codes(
            qt.task_label_codes, len(qt.task_labels),
        )
        labels = qt.task_labels

        # Compute per-task mean of the *selected* metric for
        # pct_of_task_mean (memory vs memory, run_time vs run_time).
        metric_all = qt.numeric(metric)
        task_means: dict[str, float] = {}
        for code, mask in task_groups.items():
            task_means[labels[code]] = float(np.mean(metric_all[mask]))

        # Get the full dataset ranked by metric (argsort of negated values
        # is already descending, so top_indices is rank-ordered).
        sorted_indices = np.argsort(-metric_all)

        top_indices = sorted_indices[:n]

        labels_arr = qt.labels()
        data_ids = qt.data_id_list()
        status_all = qt.status_codes
        rt_all = qt.run_time
        mem_all = qt.memory

        rows: list[dict[str, Any]] = []
        for rank_idx, idx in enumerate(top_indices, start=1):
            tl = labels_arr[idx]
            # Guard zero task-mean: pct_of_task_mean is 0.0, never inf/nan.
            task_mean = task_means.get(tl, 0.0)
            if task_mean != 0.0:
                pct_mean = (float(metric_all[idx]) / task_mean) * 100.0
            else:
                pct_mean = 0.0

            status_name = INT_TO_STATUS.get(int(status_all[idx]), "UNKNOWN")

            row = {
                'rank': rank_idx,
                'task_label': tl,
                'data_id': data_ids[idx],
                'run_time': float(rt_all[idx]),
                'memory': float(mem_all[idx]),
                'status': status_name,
                'pct_of_task_mean': pct_mean,
            }
            rows.append(row)

        return astropy.table.Table(rows)

    def _row_dim_maps(self, qt: QuantumRuntimeTable) -> list[dict[str, str]]:
        """Dimension map per row: captured values, parsed-data_id fallback.

        Prefers values captured from the real DataCoordinate object at
        extraction time (``qt.dim_lookup``, keyed by full 16-byte quantum
        ids); falls back to robust parsing of the stored ``data_id``
        display string.

        Parameters
        ----------
        qt : `QuantumRuntimeTable`
            Table to derive the per-row dimension maps for.

        Returns
        -------
        maps : `list` of `dict` [ `str`, `str` ]
            One ``{dimension: value}`` dict per row, same order as ``qt``.
        """
        dims = qt.dim_lookup
        return [
            dims[qid] if qid in dims
            else parse_dimension_string(str(data_id))
            for qid, data_id in zip(
                qt.quantum_id_list(), qt.data_id_list(), strict=True,
            )
        ]

    def dimension_dist(
        self,
        dimension: str,
        task_label: str | None = None,
    ) -> dict[str, astropy.table.Table]:
        """Group quanta by a dataID dimension key.

        Parameters
        ----------
        dimension : `str`
            Dimension key to group by (e.g., "visit", "filter", "tract").
        task_label : `str` or `None`, optional
            Filter by task label. Only tasks that have the requested dimension
            will be included.

        Returns
        -------
        distributions : `dict` [ `str`, `astropy.table.Table` ]
            One table per task that has the requested dimension.

        Notes
        -----
        Logs a warning (via ``logging.warning``, not a ``UserWarning``)
        when some tasks lack the requested dimension,
        and logs a warning when a dimension has more than 1000 distinct
        group values.
        """
        qt = self._filtered_table(task_label=task_label)

        if qt.n_rows == 0:
            _LOG.warning("No data available for dimension analysis.")
            return {}

        task_groups = group_by_codes(
            qt.task_label_codes, len(qt.task_labels),
        )
        labels = qt.task_labels
        rt_all = qt.run_time
        dim_maps = self._row_dim_maps(qt)

        # Identify which tasks have the requested dimension.  Check the
        # dimension dicts captured from the real DataCoordinate objects at
        # extraction time first; fall back to the robust string parser for
        # rows without captured values (handles both the colon and equals
        # str(DataCoordinate) formats).
        tasks_with_dimension: dict[str, np.ndarray] = {}
        for code in sorted(task_groups, key=lambda c: labels[c]):
            tl = labels[code]
            mask = task_groups[code]
            has_dim = any(
                dimension in dim_maps[i] for i in np.flatnonzero(mask)
            )

            if has_dim:
                tasks_with_dimension[tl] = mask
            else:
                _LOG.warning(
                    "Task %r does not have dimension %r, excluding.",
                    tl, dimension,
                )

        if len(tasks_with_dimension) == 0:
            return {}

        result: dict[str, astropy.table.Table] = {}
        for tl, mask in tasks_with_dimension.items():
            rt = rt_all[mask]
            total_time = float(np.sum(rt))

            # Extract the dimension value per row: captured DataCoordinate
            # values first, robust string parsing as fallback.
            dim_values = [
                dim_maps[row_i].get(dimension, "unknown")
                for row_i in np.flatnonzero(mask)
            ]

            dim_values_arr = np.array(dim_values, dtype=str)

            # Group by dimension value
            unique_vals = np.unique(dim_values_arr)

            if len(unique_vals) > 1000:
                _LOG.warning(
                    "Dimension %r has %d distinct values for task %r. "
                    "Consider filtering with --task.",
                    dimension, len(unique_vals), tl,
                )

            table_rows: list[dict[str, Any]] = []
            for val in unique_vals:
                val_mask = dim_values_arr == val
                val_subset_rt = rt[val_mask]
                val_total = float(np.sum(val_subset_rt))
                pct = (val_total / total_time) * 100 if total_time > 0 else 0.0

                table_rows.append({
                    'group_key': val,
                    'quanta': int(np.sum(val_mask)),
                    'mean_rt': float(np.mean(val_subset_rt)),
                    'median_rt': float(np.median(val_subset_rt)),
                    'max_rt': float(np.max(val_subset_rt)),
                    'std_rt': float(np.std(val_subset_rt)),
                    'total_time_sum': val_total,
                    'pct_of_total_run_time': pct,
                })

            if table_rows:
                result[tl] = astropy.table.Table(table_rows)

        return result

    def bottleneck(
        self,
        method: str = "iqr",
        top_n: int = 20,
        status: str | None = None,
        task_label: str | None = None,
    ) -> dict[str, astropy.table.Table]:
        """Identify performance bottlenecks at task and quantum level.

        Parameters
        ----------
        method : `str`, optional
            Outlier detection method: "iqr" or "zscore".
        top_n : `int`, optional
            Number of top outlier quanta to return.
        status : `str` or `None`, optional
            Filter by status name.
        task_label : `str` or `None`, optional
            Filter by task label.

        Returns
        -------
        result : `dict` [ `str`, `astropy.table.Table` ]
            Dictionary with keys:
            - ``task_table``: Task-level analysis (Task, prep_pct, init_pct,
              run_pct, mean_cpu_efficiency, bottleneck_type)
            - ``outlier_table``: Per-quantum outlier detection
              (Task, quantum_id, data_id, run_time, outlier_reason,
              outlier_magnitude), ranked by extremity from the task median
              (``max(mag, 1/mag)``, descending) before truncation to
              ``top_n``, so extreme FAST outliers are not dropped.
        """
        qt = self._filtered_table(status=status, task_label=task_label)

        if qt.n_rows == 0:
            return {
                'task_table': astropy.table.Table(),
                'outlier_table': astropy.table.Table(),
            }

        task_groups = group_by_codes(
            qt.task_label_codes, len(qt.task_labels),
        )
        labels = qt.task_labels
        rt_all = qt.run_time
        rtc_all = qt.run_time_cpu
        prep_all = qt.prep_time
        init_all = qt.init_time

        # Task-level analysis
        task_rows: list[dict[str, Any]] = []
        for code, mask in task_groups.items():
            tl = labels[code]
            rt = rt_all[mask]
            rtc = rtc_all[mask]
            prep = prep_all[mask]
            init = init_all[mask]

            total = np.sum(prep) + np.sum(init) + np.sum(rt)
            if total == 0:
                prep_pct = init_pct = run_pct = 0.0
            else:
                prep_pct = float(np.sum(prep) / total * 100)
                init_pct = float(np.sum(init) / total * 100)
                run_pct = float(np.sum(rt) / total * 100)

            cpu_eff = cpu_efficiency(rt, rtc)

            task_rows.append({
                'Task': tl,
                'prep_pct': prep_pct,
                'init_pct': init_pct,
                'run_pct': run_pct,
                'mean_cpu_efficiency': float(np.mean(cpu_eff)),
                'bottleneck_type': 'BALANCED',  # Will update below
            })

        # Compute median run_pct across tasks for I/O_BOUND classification
        if task_rows:
            med_run_pct = float(np.median([tr['run_pct'] for tr in task_rows]))
            for tr in task_rows:
                if tr['mean_cpu_efficiency'] < 0.5 and tr['run_pct'] > med_run_pct:
                    tr['bottleneck_type'] = 'I/O_BOUND'
                elif (tr['prep_pct'] + tr['init_pct']) > 30:
                    tr['bottleneck_type'] = 'OVERHEAD'

        task_table = astropy.table.Table(task_rows) if task_rows else astropy.table.Table()

        # Per-quantum outlier detection.  Each entry is an
        # ``(extremity, row)`` pair: extremity is the internal sort key
        # (symmetric distance from the median) and is deliberately *not*
        # stored on the row, so the reported ``outlier_magnitude`` column
        # stays the raw ``run_time / median`` ratio.
        outlier_rows: list[tuple[float, dict[str, Any]]] = []

        qid_matrix = qt.quantum_id_matrix
        data_ids = qt.data_id_list()

        for code, mask in task_groups.items():
            tl = labels[code]
            rt = rt_all[mask]

            if len(rt) < 2:
                continue

            # Subset-relative indices must be mapped back to global row
            # indices before indexing the full columns.
            global_indices = np.flatnonzero(mask)

            if method == "iqr":
                q1, q3, lower, upper = iqr_outlier_thresholds(rt, method="iqr")
                outlier_mask = rt > upper
            else:
                mean_s, std_s, lower, upper = zscore_outlier_thresholds(rt, threshold=2.0)
                if std_s > 0:
                    zscores = np.abs((rt - mean_s) / std_s)
                    outlier_mask = zscores > 2.0
                else:
                    outlier_mask = np.zeros(len(rt), dtype=bool)

            outlier_indices = np.where(outlier_mask)[0]

            if len(outlier_indices) == 0:
                continue

            median_rt = float(np.median(rt))

            for sub_idx in outlier_indices:
                idx = global_indices[sub_idx]
                magnitude = (
                    rt_all[idx] / median_rt if median_rt > 0 else 0.0
                )
                # Format quantum_id back to string UUID from the exact
                # 16-byte row of the id matrix.
                raw_bytes = qid_matrix[idx].tobytes()
                if raw_bytes.strip(b"\x00"):
                    qid_str = str(uuid.UUID(bytes=raw_bytes))
                else:
                    qid_str = "unknown"

                if method == "iqr":
                    # IQR only flags values above the upper fence.
                    outlier_reason = "SLOW"
                else:
                    # z-score flags both tails; record the direction.
                    outlier_reason = (
                        "SLOW" if rt_all[idx] >= median_rt else "FAST"
                    )

                extremity = _outlier_extremity(float(magnitude), median_rt)

                outlier_rows.append((extremity, {
                    'Task': tl,
                    'quantum_id': qid_str,
                    'data_id': data_ids[idx],
                    'run_time': float(rt_all[idx]),
                    'outlier_reason': outlier_reason,
                    'outlier_magnitude': magnitude,
                }))

        # Rank outliers by extremity from the median (descending) before
        # truncating.  Using max(mag, 1/mag) keeps extreme FAST outliers
        # (mag < 1) from being buried and silently dropped by [:top_n];
        # for SLOW outliers (mag >= 1) this is identical to the raw
        # magnitude.  Ties keep insertion order (sort is stable).
        outlier_rows.sort(key=lambda item: item[0], reverse=True)

        top_rows = [row for _, row in outlier_rows[:top_n]]
        outlier_table = astropy.table.Table(top_rows) if top_rows else astropy.table.Table()

        return {
            'task_table': task_table,
            'outlier_table': outlier_table,
        }


def _extract_dimension_value(data_id_str: str, dimension: str) -> str:
    """Extract the value of a dimension from a DataCoordinate string.

    This is the string-only fallback path; dimension analysis prefers
    values captured from real `DataCoordinate` objects at extraction time
    (via ``.mapping``).  Robustly handles the real ``str(DataCoordinate)``
    colon format (``"{visit: 522, band: 'r'}"`` — braces, quoted string
    values) as well as the equals formats ``"{visit=522, band='r'}"``
    and ``"visit=1,filter=g"``.

    Parameters
    ----------
    data_id_str : `str`
        String representation of a DataCoordinate.
    dimension : `str`
        The dimension key to extract.

    Returns
    -------
    value : `str`
        The dimension value, or "unknown" if not found.
    """
    return parse_dimension_string(data_id_str).get(dimension, "unknown")
