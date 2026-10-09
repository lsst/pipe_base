"""Tests for format module."""

from __future__ import annotations

import pytest

from lsst.pipe.base._runtime_analyzer.tracker.format import (
    format_alerts,
    format_comparison_table,
    format_run_list,
    format_trend_table,
)

pytest.importorskip("pyarrow")  # provided by the [runtime] extra

from ..support import (  # noqa: E402
    comparison_row as _comparison,
)


class TestFormatComparisonTable:
    """Tests for ``format_comparison_table``."""

    @pytest.mark.parametrize(("from_run", "to_run", "comparisons",
                              "added", "removed", "lower_expected",
                              "expected"), [
        ({"label": "v15"}, {"label": "v16"}, [],
         None, None, [], ["No shared tasks"]),
        ({"label": "v15"}, {"label": "v16"},
         [_comparison(cohens_d=0.5, p_value=0.08, status="uncertain")],
         ["dark_cal"], ["preprocess"],
         ["added", "removed"], ["dark_cal", "preprocess"]),
    ], ids=["no-shared-tasks", "added-and-removed"])
    def test_comparison_table_output(self, from_run, to_run, comparisons,
                                     added, removed, lower_expected,
                                     expected):
        """The table renders its rows, header, and add/remove notices
        (empty comparison list renders the 'No shared tasks' notice).
        """
        result = format_comparison_table(
            from_run, to_run, comparisons,
            added_tasks=added, removed_tasks=removed,
        )
        for needle in lower_expected:
            assert needle in result.lower()
        for needle in expected:
            assert needle in result

    @pytest.mark.parametrize(("comparisons", "arrow"), [
        ([_comparison(task_label="task_a", metric_from=50.0,
                      metric_to=60.0, delta=10.0, delta_pct=20.0,
                      cohens_d=0.5, p_value=0.03)], "\u2191"),
        ([_comparison(task_label="task_a", metric_from=50.0,
                      metric_to=40.0, delta=-10.0, delta_pct=-20.0,
                      cohens_d=-0.5, p_value=0.03, status="stable")],
         "\u2193"),
    ], ids=["positive-delta-up-arrow", "negative-delta-down-arrow"])
    def test_delta_arrow_symbol(self, comparisons, arrow):
        """Positive deltas render an upward arrow, negative a downward one."""
        result = format_comparison_table({}, {}, comparisons)
        assert arrow in result


class TestFormatRunList:
    """Tests for ``format_run_list``."""

    @pytest.mark.parametrize(("runs", "expected"), [
        ([], ["No recorded runs"]),
        ([{"run_id": "r1", "label": "v16.0", "timestamp": 1700000000},
          {"run_id": "r2", "label": "v15.0", "timestamp": 1699000000}],
         ["v16.0", "v15.0"]),
    ], ids=["empty", "with-runs"])
    def test_run_list_output(self, runs, expected):
        """Empty input says so; populated input lists every run label."""
        result = format_run_list(runs)
        for needle in expected:
            assert needle in result


class TestFormatTrendTable:
    """Tests for ``format_trend_table``."""

    @pytest.mark.parametrize(("trend", "expected"), [
        ({"run_data": [], "slope": 0.0, "r_squared": 0.0, "p_value": 1.0,
          "intercept": 0.0, "n_runs": 0}, ["Need at least 3"]),
        ({"run_data": [
            {"label": "v15", "timestamp": 1700000000, "value": 45.0},
            {"label": "v16", "timestamp": 1710000000, "value": 52.0},
            {"label": "v17", "timestamp": 1720000000, "value": 50.0},
         ],
          "slope": 1.5, "intercept": -100.0,
          "r_squared": 0.85, "p_value": 0.03, "n_runs": 3},
         ["calibrate", "v15", "v17"]),
    ], ids=["insufficient-data", "with-data"])
    def test_trend_table_output(self, trend, expected):
        """Too-few-runs trends report the requirement; full trends list
        task and run labels.
        """
        result = format_trend_table("calibrate", "p50", trend)
        for needle in expected:
            assert needle in result


class TestFormatAlerts:
    """Tests for ``format_alerts``."""

    def test_no_alerts(self):
        result = format_alerts([])
        assert "No significant changes detected" in result

    def test_with_alerts(self):
        alerts = [
            {
                "task_label": "calibrate", "from_run": "v15", "to_run": "v16",
                "delta_pct": 15.0, "cohens_d": 0.85, "p_value": 0.001,
                "status": "significant",
                "from_run_id": "r1", "to_run_id": "r2",
            },
            {
                "task_label": "coadd", "from_run": "v15", "to_run": "v16",
                "delta_pct": -5.0, "cohens_d": -0.2, "p_value": 0.08,
                "status": "stable",
                "from_run_id": "r1", "to_run_id": "r2",
            },
        ]
        result = format_alerts(alerts)
        assert "calibrate" in result
        assert "coadd" in result
        # Verify sorted by Cohen's d descending (largest effect first)
        assert result.index("calibrate") < result.index("coadd")
