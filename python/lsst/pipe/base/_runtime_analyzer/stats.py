"""Numpy aggregation utilities for quantum resource usage analysis.

This module provides helper functions for computing statistics,
grouping data, detecting outliers, and calculating efficiency metrics
on quantum resource usage arrays.
"""

from __future__ import annotations

__all__ = [
    "cpu_efficiency",
    "group_by_codes",
    "io_pct",
    "iqr_outlier_thresholds",
    "percentile",
    "zscore_outlier_thresholds",
]

import numpy as np


def percentile(array: np.ndarray, percentiles: list[float] | tuple[float, ...] | None = None) -> np.ndarray:
    """Compute specified percentiles of a 1-D numeric array.

    Parameters
    ----------
    array : `~numpy.ndarray`
        1-D array of numeric values.
    percentiles : `list` of `float`, `tuple` of `float`, or `None`, optional
        Percentile values in [0, 100].  ``None`` (default) selects
        ``[5, 25, 50, 75, 95]``.

    Returns
    -------
    result : `~numpy.ndarray`
        Percentile values corresponding to the requested percentiles,
        in the order requested.
    """
    if percentiles is None:
        percentiles = [5, 25, 50, 75, 95]
    return np.percentile(np.asarray(array, dtype=np.float64), percentiles)


def group_by_codes(codes: np.ndarray, n_codes: int) -> dict[int, np.ndarray]:
    """Group rows by small-integer category codes.

    Designed for Arrow dictionary code columns (the
    ``QuantumRuntimeTable.task_label_codes`` view): codes index a dictionary of
    ``n_codes`` values, and grouping is one vectorized comparison per
    present code.

    Parameters
    ----------
    codes : `~numpy.ndarray`
        Integer category code per row.
    n_codes : `int`
        Number of distinct codes in the dictionary; codes outside
        ``[0, n_codes)`` never produce a group.

    Returns
    -------
    result : `dict` [ `int`, `~numpy.ndarray` ]
        Mapping from each code that appears at least once to a boolean
        mask of that code's rows (same length as ``codes``).  Keys are
        emitted in ascending code order.
    """
    codes = np.asarray(codes)
    result: dict[int, np.ndarray] = {}
    for code in range(int(n_codes)):
        mask = codes == code
        if mask.any():
            result[code] = mask
    return result


def iqr_outlier_thresholds(data: np.ndarray, method: str = "iqr") -> tuple[float, float, float, float]:
    """Compute outlier thresholds using interquartile range method.

    Returns Q1, Q3, lower_fence, and upper_fence. The fences are set at
    Q1 - 1.5*IQR and Q3 + 1.5*IQR respectively.

    Parameters
    ----------
    data : `~numpy.ndarray`
        1-D array of numeric values.
    method : `str`, optional
        Outlier detection method. Only "iqr" is accepted (default).

    Returns
    -------
    q1 : `float`
        25th percentile.
    q3 : `float`
        75th percentile.
    lower_fence : `float`
        Lower fence Q1 - 1.5 * IQR.
    upper_fence : `float`
        Upper fence Q3 + 1.5 * IQR.

    Raises
    ------
    ValueError
        Raised if ``method`` is not ``"iqr"``.
    """
    if method == "iqr":
        q1, q3 = np.percentile(np.asarray(data, dtype=np.float64), [25, 75])
        iqr = q3 - q1
        return float(q1), float(q3), float(q1 - 1.5 * iqr), float(q3 + 1.5 * iqr)
    else:
        raise ValueError(f"Unknown method: {method!r}")


def zscore_outlier_thresholds(data: np.ndarray, threshold: float = 2.0) -> tuple[float, float, float, float]:
    """Compute outlier thresholds using z-score method.

    Returns mean, std, lower_fence, and upper_fence. The fences are set at
    mean - threshold * std and mean + threshold * std.

    Parameters
    ----------
    data : `~numpy.ndarray`
        1-D array of numeric values.
    threshold : `float`, optional
        Number of standard deviations for the fence boundary. Default is 2.0.

    Returns
    -------
    mean : `float`
        Mean of the data.
    std : `float`
        Standard deviation of the data.
    lower_fence : `float`
        Lower fence: mean - threshold * std.
    upper_fence : `float`
        Upper fence: mean + threshold * std.
    """
    arr = np.asarray(data, dtype=np.float64)
    mean = float(np.mean(arr))
    std = float(np.std(arr))
    if std == 0.0:
        return mean, std, mean, mean
    return mean, std, float(mean - threshold * std), float(mean + threshold * std)


def io_pct(run_time: np.ndarray, run_time_cpu: np.ndarray) -> np.ndarray:
    """Compute the percentage of run time that is not CPU time.

    This measures the I/O bound fraction of execution time.

    Parameters
    ----------
    run_time : `~numpy.ndarray`
        Wall-clock run time per quantum.
    run_time_cpu : `~numpy.ndarray`
        CPU time per quantum.

    Returns
    -------
    result : `~numpy.ndarray`
        I/O percentage per row (``(run_time - run_time_cpu) / run_time
        * 100``); ``0.0`` wherever ``run_time`` is not positive.
        Negative where CPU time exceeds wall-clock time.
    """
    rt = np.asarray(run_time, dtype=np.float64)
    rtc = np.asarray(run_time_cpu, dtype=np.float64)
    total = rt - rtc
    result = np.zeros_like(rt)
    with np.errstate(divide='ignore', invalid='ignore'):
        mask = rt > 0
        result[mask] = total[mask] / rt[mask] * 100.0
    return result


def cpu_efficiency(run_time: np.ndarray, run_time_cpu: np.ndarray) -> np.ndarray:
    """Compute the ratio of CPU time to wall-clock run time.

    Parameters
    ----------
    run_time : `~numpy.ndarray`
        Wall-clock run time per quantum.
    run_time_cpu : `~numpy.ndarray`
        CPU time per quantum.

    Returns
    -------
    result : `~numpy.ndarray`
        ``run_time_cpu / run_time`` per row; ``1.0`` wherever
        ``run_time`` is not positive.  Values above ``1.0`` indicate
        CPU time exceeding wall-clock time.
    """
    rt = np.asarray(run_time, dtype=np.float64)
    rtc = np.asarray(run_time_cpu, dtype=np.float64)
    result = np.ones_like(rt)
    with np.errstate(divide='ignore', invalid='ignore'):
        mask = rt > 0
        result[mask] = rtc[mask] / rt[mask]
    return result
