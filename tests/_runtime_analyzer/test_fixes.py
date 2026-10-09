"""Tests for the bug fixes and edge cases in the core module."""

from __future__ import annotations

import numpy as np
import pytest

from lsst.pipe.base._runtime_analyzer.stats import cpu_efficiency, io_pct

pytest.importorskip("pyarrow")  # provided by the [runtime] extra

from .support import MockAnalyzer  # noqa: E402


class TestSingleQuantumSummary:
    """Bug #1: summary() should not crash on single-quantum tasks."""

    def test_single_quantum_per_task(self) -> None:
        analyzer = MockAnalyzer(n_quanta=3, n_tasks=3)
        result = analyzer.summary()
        # Each task has exactly 1 quantum
        assert len(result) == 3
        # All percentile columns should be present and valid
        for col in ['p05', 'p25', 'p50', 'p75', 'p95']:
            assert col in result.colnames


class TestIoPctNoWarning:
    """Bug #10: io_pct should not emit RuntimeWarning on zero run_time."""

    def test_no_warning_on_zero(self) -> None:
        import warnings
        with warnings.catch_warnings():
            warnings.filterwarnings('error', category=RuntimeWarning)
            result = io_pct(np.array([10.0, 0.0]), np.array([8.0, 0.0]))
            assert result[0] == pytest.approx(20.0)
            assert result[1] == 0.0


class TestCpuEfficiencyNoWarning:
    """Bug #10: cpu_efficiency must not emit RuntimeWarning on zero
    run_time.
    """

    def test_no_warning_on_zero(self) -> None:
        import warnings
        with warnings.catch_warnings():
            warnings.filterwarnings('error', category=RuntimeWarning)
            result = cpu_efficiency(np.array([10.0, 0.0]), np.array([8.0, 0.0]))
            assert result[0] == pytest.approx(0.8)
            assert result[1] == 1.0
