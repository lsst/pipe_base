"""Tests for the stats module (numpy aggregation utilities)."""

from __future__ import annotations

import numpy as np
import pytest

from lsst.pipe.base._runtime_analyzer.stats import (
    cpu_efficiency,
    group_by_codes,
    io_pct,
    iqr_outlier_thresholds,
    percentile,
    zscore_outlier_thresholds,
)


class TestPercentile:
    """Tests for ``percentile``."""

    def test_default_percentiles(self) -> None:
        arr = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])
        result = percentile(arr)
        assert len(result) == 5
        assert result[0] <= 3.0  # p5
        assert result[2] >= 4.0  # p25
        assert result[4] >= 8.0  # p95

    def test_with_tuple(self) -> None:
        arr = np.array([1.0, 2.0, 3.0])
        result = percentile(arr, (25, 75))
        assert len(result) == 2
        assert result[0] < result[1]


class TestGroupByCodes:
    """Tests for ``group_by_codes``."""

    def test_basic_grouping(self) -> None:
        codes = np.array([1, 0, 1, 0, 1], dtype=np.int32)
        groups = group_by_codes(codes, 2)
        assert set(groups.keys()) == {0, 1}
        # Verify masks have correct length and count
        assert len(groups[0]) == 5
        assert len(groups[1]) == 5
        assert np.sum(groups[0]) == 2
        assert np.sum(groups[1]) == 3

    def test_single_group(self) -> None:
        codes = np.array([3, 3, 3], dtype=np.int32)
        groups = group_by_codes(codes, 4)
        assert list(groups.keys()) == [3]
        assert len(groups[3]) == 3
        assert np.sum(groups[3]) == 3

    def test_absent_codes_are_skipped(self) -> None:
        codes = np.array([0, 2], dtype=np.int32)
        groups = group_by_codes(codes, 5)
        assert list(groups.keys()) == [0, 2]

    def test_out_of_range_codes_never_group(self) -> None:
        codes = np.array([0, 7], dtype=np.int32)
        groups = group_by_codes(codes, 3)
        assert list(groups.keys()) == [0]


class TestIQRThresholds:
    """Tests for ``iqr_outlier_thresholds``."""

    def test_known_iqr(self) -> None:
        data = np.arange(1, 101, dtype=float)  # 1..100
        q1, q3, lower, upper = iqr_outlier_thresholds(data)
        assert q1 == pytest.approx(25.25, abs=1.0)
        assert q3 == pytest.approx(75.75, abs=1.0)
        assert lower < q1
        assert upper > q3
        assert upper > lower

    def test_raises_unknown_method(self) -> None:
        with pytest.raises(ValueError, match="Unknown method"):
            iqr_outlier_thresholds(np.array([1.0, 2.0]), method="bad")


class TestZscoreThresholds:
    """Tests for ``zscore_outlier_thresholds``."""

    def test_known_zscore(self) -> None:
        data = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        mean, std, lower, upper = zscore_outlier_thresholds(data)
        assert mean == pytest.approx(3.0)
        assert std == pytest.approx(1.4142, abs=0.01)
        assert lower == pytest.approx(3.0 - 2.0 * std)
        assert upper == pytest.approx(3.0 + 2.0 * std)

    def test_zero_std(self) -> None:
        data = np.array([5.0, 5.0, 5.0])
        mean, std, lower, upper = zscore_outlier_thresholds(data)
        assert mean == 5.0
        assert std == 0.0
        assert lower == 5.0
        assert upper == 5.0

    def test_custom_threshold(self) -> None:
        data = np.array([1.0, 2.0, 3.0])
        mean, std, lower, upper = zscore_outlier_thresholds(data, threshold=1.0)
        assert lower == pytest.approx(mean - std)
        assert upper == pytest.approx(mean + std)


class TestIoPct:
    """Tests for ``io_pct``."""

    def test_fully_cpu_bound(self) -> None:
        rt = np.array([10.0, 20.0])
        rtc = np.array([10.0, 20.0])
        result = io_pct(rt, rtc)
        assert np.allclose(result, [0.0, 0.0])

    def test_half_io(self) -> None:
        rt = np.array([10.0])
        rtc = np.array([5.0])
        result = io_pct(rt, rtc)
        assert result[0] == pytest.approx(50.0)

    def test_zero_run_time(self) -> None:
        rt = np.array([0.0])
        rtc = np.array([0.0])
        result = io_pct(rt, rtc)
        assert result[0] == 0.0


class TestCpuEfficiency:
    """Tests for ``cpu_efficiency``."""

    def test_fully_cpu_bound(self) -> None:
        rt = np.array([10.0, 20.0])
        rtc = np.array([10.0, 20.0])
        result = cpu_efficiency(rt, rtc)
        assert np.allclose(result, [1.0, 1.0])

    def test_half_efficiency(self) -> None:
        rt = np.array([10.0])
        rtc = np.array([5.0])
        result = cpu_efficiency(rt, rtc)
        assert result[0] == pytest.approx(0.5)

    def test_zero_run_time(self) -> None:
        rt = np.array([0.0])
        rtc = np.array([0.0])
        result = cpu_efficiency(rt, rtc)
        assert result[0] == 1.0
