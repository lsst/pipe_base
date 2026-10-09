"""Tests for history/comparison module."""

from __future__ import annotations

import time

import pytest

from lsst.pipe.base._runtime_analyzer.tracker.database import record_run
from lsst.pipe.base._runtime_analyzer.tracker.history import (
    _auto_compare,
    check_alerts,
    compare_runs,
    find_task_changes,
    get_trend,
)

pytest.importorskip("pyarrow")  # provided by the [runtime] extra

from ..support import (  # noqa: E402
    history_record as _record,
)
from ..support import (
    history_task_row as _row,
)


@pytest.fixture
def two_runs(db_path):
    """Create two runs with different performance levels."""
    summary1 = [
        _row("calibrate"),
        _row("coadd", quanta=50, mean_rt=80.0, p05=60.0, p25=70.0,
             p50=80.0, p75=90.0, p95=100.0, max_rt=120.0, min_rt=50.0,
             std_rt=12.0, mean_mem=200.0, median_mem=190.0, max_mem=300.0,
             mean_io_pct=0.15, total_rt=4000.0),
    ]

    summary2 = [
        _row("calibrate", mean_rt=52.0, p05=36.0, p25=46.0, p50=52.0,
             p75=58.0, p95=68.0, max_rt=80.0, min_rt=30.0, std_rt=9.0,
             mean_mem=105.0, median_mem=100.0, max_mem=155.0,
             mean_io_pct=0.11, total_rt=5200.0),
        _row("coadd", quanta=55, mean_rt=78.0, p05=58.0, p25=68.0,
             p50=78.0, p75=88.0, p95=98.0, max_rt=115.0, min_rt=48.0,
             std_rt=11.0, mean_mem=195.0, median_mem=185.0, max_mem=290.0,
             mean_io_pct=0.14, total_rt=4290.0),
        # New task only in run 2
        _row("dark_cal", quanta=20, mean_rt=15.0, p05=10.0,
             p25=12.0, p50=15.0, p75=18.0, p95=20.0, max_rt=25.0,
             min_rt=8.0, std_rt=3.5, mean_mem=50.0, median_mem=48.0,
             max_mem=60.0, mean_io_pct=0.05, total_rt=300.0),
    ]

    record_run(
        label="run_v15", run_id="v15", repo="test_repo",
        collection="test_coll", graph_hash="hash15",
        analyzer_version="0.1.0", summary_table=summary1,
        raw_quanta_data=None,
    )
    time.sleep(0.01)
    record_run(
        label="run_v16", run_id="v16", repo="test_repo",
        collection="test_coll", graph_hash="hash16",
        analyzer_version="0.1.0", summary_table=summary2,
        raw_quanta_data=None,
    )
    return "run_v15", "run_v16"


class TestCompareRuns:
    """Tests for ``compare_runs``."""

    def test_returns_comparisons(self, two_runs):
        from_, to = two_runs
        comparisons = compare_runs(from_, to)
        assert len(comparisons) == 2  # only shared tasks

    def test_shared_task_values(self, two_runs):
        from_, to = two_runs
        comparisons = compare_runs(from_, to)
        calibrate = [c for c in comparisons
                     if c["task_label"] == "calibrate"][0]
        assert calibrate["metric_from"] == 45.0
        assert calibrate["metric_to"] == 52.0

    def test_delta_pct_positive(self, two_runs):
        from_, to = two_runs
        comparisons = compare_runs(from_, to)
        calibrate = [c for c in comparisons
                     if c["task_label"] == "calibrate"][0]
        assert calibrate["delta_pct"] > 0  # calibrate got slower

    def test_nonexistent_label(self, two_runs):
        with pytest.raises(ValueError, match="No run found"):
            compare_runs("nonexistent", "run_v16")

    def test_absolute_diff_mode(self, two_runs):
        from_, to = two_runs
        comparisons = compare_runs(from_, to, diff_mode="absolute")
        calibrate = [c for c in comparisons
                     if c["task_label"] == "calibrate"][0]
        assert abs(calibrate["delta"] - 7.0) < 0.01


class TestFindTaskChanges:
    """Tests for ``find_task_changes``."""

    def test_added_and_shared(self, two_runs):
        from_, to = two_runs
        added, removed, both = find_task_changes(from_, to)
        assert "dark_cal" in added
        assert "calibrate" in both
        assert "coadd" in both

    def test_no_removed(self, two_runs):
        from_, to = two_runs
        added, removed, both = find_task_changes(from_, to)
        assert len(removed) == 0


class TestGetTrend:
    """Tests for ``get_trend``."""

    def test_insufficient_runs(self, db_path):
        trend = get_trend("calibrate")
        assert trend["n_runs"] < 3

    def test_trend_three_runs(self, db_path):
        """Three runs with a rising p50 yield n_runs=3 and a positive
        slope (performance getting slower).
        """
        _record("t1", [_row("calibrate", mean_rt=40.0, p05=25.0,
                            p25=35.0, p50=40.0, p75=45.0, p95=55.0,
                            max_rt=65.0, min_rt=20.0, std_rt=7.0,
                            mean_mem=90.0, median_mem=85.0,
                            max_mem=140.0, mean_io_pct=0.09,
                            total_rt=4000.0)], graph_hash="g1")
        _record("t2", [_row("calibrate")], graph_hash="g2")
        _record("t3", [_row("calibrate", mean_rt=50.0, p05=35.0,
                            p25=42.0, p50=50.0, p75=55.0, p95=65.0,
                            max_rt=75.0, min_rt=28.0, std_rt=9.0,
                            mean_mem=105.0, median_mem=100.0,
                            max_mem=155.0, mean_io_pct=0.11,
                            total_rt=5000.0)], graph_hash="g3")
        trend = get_trend("calibrate")
        assert trend["n_runs"] == 3
        assert trend["slope"] > 0  # performance getting slower

    def test_flat_trend(self, db_path):
        """Five runs at an identical p50 yield a (near-)zero slope."""
        flat = _row("flat_task", mean_rt=30.0, p05=20.0, p25=25.0,
                    p50=30.0, p75=35.0, p95=40.0, max_rt=50.0,
                    min_rt=15.0, std_rt=5.0, mean_mem=60.0,
                    median_mem=55.0, max_mem=80.0, mean_io_pct=0.05,
                    total_rt=3000.0)
        for i in range(1, 6):
            _record(f"f{i}", [dict(flat)], graph_hash=f"g{i}")

        trend = get_trend("flat_task")
        assert trend["n_runs"] == 5
        assert abs(trend["slope"]) < 0.01


class TestCheckAlerts:
    """Tests for ``check_alerts``."""

    def test_no_alerts_with_stable_runs(self, db_path):
        alerts = check_alerts()
        # With small differences, might or might not fire
        assert isinstance(alerts, list)

    def test_alerts_with_different_values(self, db_path):
        """A ~1% p50 move is a small change: check_alerts returns a
        list (and must not raise).
        """
        _record("a1", [_row("calibrate", mean_rt=40.0, p05=25.0,
                            p25=35.0, p50=40.0, p75=45.0, p95=55.0,
                            max_rt=65.0, min_rt=20.0, std_rt=7.0,
                            mean_mem=90.0, median_mem=85.0,
                            max_mem=140.0, mean_io_pct=0.09,
                            total_rt=4000.0)], graph_hash="g1")
        _record("a2", [_row("calibrate", mean_rt=40.5, p05=25.0,
                            p25=35.0, p50=40.5, p75=45.5, p95=55.5,
                            max_rt=65.5, min_rt=20.0, std_rt=7.0,
                            mean_mem=90.0, median_mem=85.0,
                            max_mem=140.0, mean_io_pct=0.09,
                            total_rt=4050.0)], graph_hash="g2")
        alerts = check_alerts()
        # Small change should not trigger alert
        assert isinstance(alerts, list)


def _assert_no_calibrate_alert(alerts):
    """No calibrate alert fired."""
    assert [a for a in alerts if a["task_label"] == "calibrate"] == []


def _assert_calibrate_alert_with_5pct_delta(alerts):
    """Assert the calibrate alert fired with a ~5 % delta_pct."""
    cal = [a for a in alerts if a["task_label"] == "calibrate"]
    assert cal
    assert abs(cal[0]["delta_pct"] - 5.0) < 0.01


class TestCheckAlertsMetricAlignment:
    """TRK-3: significance/delta must track the requested metric."""

    def test_alert_carries_metric_selection(self, db_path):
        _record("m1", [_row("calibrate", mean_rt=45.0, std_rt=8.0,
                            p50=45.0)])
        _record("m2", [_row("calibrate", mean_rt=52.0, std_rt=9.0,
                            p50=52.0)])
        alerts = check_alerts(metric="p50")
        cal = [a for a in alerts if a["task_label"] == "calibrate"]
        assert cal, "significant p50 regression should fire"
        assert cal[0]["metric"] == "p50"
        assert cal[0]["metric_from"] == 45.0
        assert cal[0]["metric_to"] == 52.0
        assert cal[0]["significance_source"] == "summary"

    def test_delta_threshold_gate(self, db_path):
        """A ~5 % p50 move with tiny summary std is highly significant,
        so the delta_pct gate alone decides: the default 10 % gate
        suppresses it, a lowered 2 % gate lets it through.  Both gate
        outcomes are checked against one recorded pair (identical
        invocation shape apart from the gate kwarg).
        """
        _record("d1", [_row("calibrate", mean_rt=10.0, std_rt=0.1,
                            p50=10.0)])
        _record("d2", [_row("calibrate", mean_rt=10.5, std_rt=0.1,
                            p50=10.5)])
        _assert_no_calibrate_alert(check_alerts(metric="p50"))
        _assert_calibrate_alert_with_5pct_delta(
            check_alerts(metric="p50", delta_threshold_pct=2.0))

    def test_metric_delta_zero_suppresses_even_if_mean_differs(
            self, db_path):
        # Same p95 across runs -> the requested metric did not move, so no
        # alert even though mean_rt differs a lot (metric alignment).
        _record("q1", [_row("calibrate", mean_rt=40.0, std_rt=1.0,
                            p95=60.0)])
        _record("q2", [_row("calibrate", mean_rt=80.0, std_rt=1.0,
                            p95=60.0)])
        alerts = check_alerts(metric="p95")
        assert [a for a in alerts if a["task_label"] == "calibrate"] == []


class TestCheckAlertsRawData:
    """TRK-3: raw per-quantum data drives the significance test."""

    def test_raw_source_used_when_available(self, db_path):
        raw1 = [{"task_label": "calibrate", "quantum_id": b"a%d" % i,
                 "run_time": 45.0 + (i % 5)} for i in range(20)]
        raw2 = [{"task_label": "calibrate", "quantum_id": b"b%d" % i,
                 "run_time": 60.0 + (i % 5)} for i in range(20)]
        _record("r1", [_row("calibrate", mean_rt=45.0, std_rt=8.0,
                            p50=45.0)], raw_quanta_data=raw1)
        _record("r2", [_row("calibrate", mean_rt=60.0, std_rt=8.0,
                            p50=60.0)], raw_quanta_data=raw2)
        alerts = check_alerts(metric="p50")
        cal = [a for a in alerts if a["task_label"] == "calibrate"]
        assert cal
        assert cal[0]["significance_source"] == "raw"
        assert cal[0]["p_value"] < 0.05


class TestMetricZeroSentinel:
    """Tmin1: a legitimate 0.0 metric must NOT be conflated with an absent
    column and silently replaced by ``p50``.
    """

    def test_p95_zero_preserved_in_alert_not_replaced_by_p50(self,
                                                             db_path):
        # Older run: p95=50. Newer run: p95 collapses to a legitimate 0.0.
        # mean/std differ enough for the summary Welch test to fire.
        _record("z1", [_row("calibrate", mean_rt=50.0, std_rt=1.0,
                            p50=50.0, p95=50.0)])
        _record("z2", [_row("calibrate", mean_rt=80.0, std_rt=1.0,
                            p50=80.0, p95=0.0)])
        alerts = check_alerts(metric="p95")
        cal = [a for a in alerts if a["task_label"] == "calibrate"]
        assert cal, "significant p95 regression should fire"
        # The reported metric_to is the *real* p95 (0.0), NOT p50 (80.0).
        assert cal[0]["metric"] == "p95"
        assert cal[0]["metric_from"] == 50.0
        assert cal[0]["metric_to"] == 0.0
        # metric_from == 0 is guarded, so a zero baseline never divides.
        assert cal[0]["delta_pct"] == -100.0

    def test_zero_baseline_yields_zero_delta_not_error(self, db_path):
        # metric_from legitimately 0.0 (older run) -> delta guarded to 0.0.
        _record("z1", [_row("calibrate", mean_rt=1.0, std_rt=1.0,
                            p50=0.0, p95=0.0)])
        _record("z2", [_row("calibrate", mean_rt=1.5, std_rt=1.0,
                            p50=0.0, p95=0.0)])
        # Must not raise ZeroDivisionError; both p95 values are 0.0.
        alerts = check_alerts(metric="p95")
        cal = [a for a in alerts if a["task_label"] == "calibrate"]
        # A 0 -> 0 metric moved 0%, below the delta gate -> suppressed,
        # but crucially metric_from/metric_to stay 0.0 (not p50 fallback).
        for a in cal:
            assert a["metric_from"] == 0.0
            assert a["metric_to"] == 0.0
            assert a["delta_pct"] == 0.0


class TestCompareRunsZeroMetric:
    """Tmin3: removing the ``0.0 or row.get(...)`` obfuscation preserves a
    legitimate zero metric value in compare_runs (no behavior change).
    """

    def test_zero_metric_preserved_in_compare(self, two_runs):
        from_, to = two_runs
        # Add a run whose p95 is legitimately 0.0 with a non-zero p50.
        _record("zero_run", [_row("calibrate", mean_rt=45.0,
                                  std_rt=8.0, p50=45.0, p95=0.0)],
                repo="test_repo", collection="test_coll",
                graph_hash="hz")
        comparisons = compare_runs(from_, "zero_run", metric="p95")
        cal = [c for c in comparisons if c["task_label"] == "calibrate"][0]
        # 0.0 p95 must survive; the old code would report p50 (45.0) here.
        assert cal["metric_to"] == 0.0
        assert cal["metric_from"] == 60.0  # run_v15 p95

    def test_missing_metric_falls_back_to_p50(self, db_path):
        # When the metric column is genuinely absent/None, p50 is used.
        _record("fb1", [_row("calibrate", mean_rt=10.0, p50=10.0)])
        _record("fb2", [_row("calibrate", mean_rt=20.0, p50=20.0)])
        # metric "p99" is not a real column -> fall back to p50.
        comparisons = compare_runs("fb1", "fb2", metric="p99")
        cal = [c for c in comparisons if c["task_label"] == "calibrate"][0]
        assert cal["metric_from"] == 10.0
        assert cal["metric_to"] == 20.0


class TestCheckAlertsFromRunLabel:
    """Tmin2: an unmatched ``from_run_label`` must raise, not silently scan
    the whole history from index 0.
    """

    def test_unmatched_from_run_label_raises(self, db_path):
        _record("h1", [_row("calibrate", mean_rt=10.0, std_rt=1.0,
                            p50=10.0)])
        _record("h2", [_row("calibrate", mean_rt=50.0, std_rt=1.0,
                            p50=50.0)])
        with pytest.raises(ValueError, match="No run found"):
            check_alerts(from_run_label="does-not-exist")

    def test_matched_from_run_label_restricts_scan(self, db_path):
        # Three runs, each step a large significant regression.
        for label, val in (("h1", 10.0), ("h2", 50.0), ("h3", 90.0)):
            _record(label, [_row("calibrate", mean_rt=val, std_rt=1.0,
                                 p50=val)])

        # Unrestricted: both (h3 vs h2) and (h2 vs h1) are scanned.
        all_alerts = check_alerts(metric="p50")
        to_runs_unrestricted = {a["to_run"] for a in all_alerts
                                if a["task_label"] == "calibrate"}
        assert to_runs_unrestricted == {"h3", "h2"}

        # Restricted to start from h2: only (h2 vs h1) is scanned; the
        # (h3 vs h2) pair must be excluded.
        restricted = check_alerts(from_run_label="h2", metric="p50")
        to_runs_restricted = {a["to_run"] for a in restricted
                              if a["task_label"] == "calibrate"}
        assert to_runs_restricted == {"h2"}


class TestAutoCompare:
    """TRK-4: _auto_compare renders and returns a comparison table."""

    def test_returns_comparisons_and_prints(self, db_path, capsys):
        _record("auto1", [_row("calibrate", mean_rt=45.0, p50=45.0)],
                graph_hash="g1")
        _record("auto2", [_row("calibrate", mean_rt=52.0, p50=52.0)],
                graph_hash="g2")
        comparisons = _auto_compare("auto2")
        assert comparisons is not None
        assert len(comparisons) == 1
        assert comparisons[0]["task_label"] == "calibrate"
        out = capsys.readouterr().out
        assert "Auto-comparison" in out
        # The comparison row is printed.
        assert "calibrate" in out

    def test_first_run_returns_none(self, db_path):
        _record("solo", [_row("calibrate")])
        assert _auto_compare("solo") is None
