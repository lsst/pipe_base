"""Tests for plot module."""

from __future__ import annotations

import pytest

pytest.importorskip("matplotlib")  # provided by the [runtime] extra

import matplotlib  # noqa: E402

# Headless backend: the tracker plot functions themselves call
# ``matplotlib.use("Agg")`` per call, so pinning it at import time is
# consistent with production behaviour.
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from lsst.pipe.base._runtime_analyzer.tracker.plot import (  # noqa: E402
    get_tracker_plot_names,
    plot_delta_bar,
    plot_heatmap,
    plot_trend,
    save_tracker_plot,
)


def _trend_kwargs():
    return dict(task_label="calibrate", metric="p50",
                run_labels=["v15", "v16", "v17"],
                values=[45.0, 52.0, 50.0], timestamps=[1000, 1100, 1200])


def _heatmap_kwargs():
    return dict(task_labels=["calibrate", "coadd"],
                run_labels=["v15", "v16"],
                values_2d=[[45.0, 52.0], [80.0, 78.0]])


def _delta_kwargs():
    return dict(task_labels=["calibrate", "coadd"],
                delta_pcts=[12.0, -2.5])


def _check_trend_lines(fig):
    """Assert the trend axes carry at least one line (regression/scatter)."""
    axes = fig.get_axes()
    assert len(axes) > 0
    lines = axes[0].get_lines()
    # Should have at least 1 line (regression line or scatter)
    assert len(lines) >= 1


def _check_heatmap_ytick_labels(fig):
    """Every task appears as a ytick label of the heatmap."""
    axes = fig.get_axes()
    assert len(axes) > 0
    ytick_labels = [t.get_text() for t in axes[0].get_yticklabels()]
    assert "a" in ytick_labels
    assert "b" in ytick_labels
    assert "c" in ytick_labels


def _check_delta_bar_colors(fig):
    """Positive deltas are red-ish, negative ones green-ish."""
    axes = fig.get_axes()
    bars = axes[0].patches
    assert len(bars) == 3
    # Positive (slower) should be red-ish
    from matplotlib.colors import to_rgba
    colors = [to_rgba(bar.get_facecolor())[:3] for bar in bars]
    # First bar positive: should have significant red
    assert colors[0][0] > colors[0][1]  # red > green
    # Second bar negative: should be green
    assert colors[1][1] > colors[1][0]  # green > red


def _big_heatmap_kwargs():
    runs = [f'run_{i}' for i in range(100)]
    tasks = [f'task_{i}' for i in range(200)]
    vals = [[float((i + j) % 50) for j in range(len(runs))]
            for i in range(len(tasks))]
    return dict(task_labels=tasks, run_labels=runs, values_2d=vals)


class TestTrackerPlots:
    """Smoke tests for the tracker plot functions."""

    @pytest.mark.parametrize("func_kwargs", [
        (plot_trend, _trend_kwargs()),
        (plot_heatmap, _heatmap_kwargs()),
        (plot_delta_bar, _delta_kwargs()),
    ], ids=["trend", "heatmap", "delta"])
    def test_returns_figure(self, func_kwargs):
        """Every tracker plot function returns a closed-clean Figure."""
        func, kwargs = func_kwargs
        fig = func(**kwargs)
        assert fig is not None
        assert isinstance(fig, matplotlib.figure.Figure)
        plt.close(fig)

    @pytest.mark.parametrize(("func", "kwargs", "check"), [
        (plot_trend,
         dict(task_label="calibrate", metric="p50",
              run_labels=["v1", "v2", "v3"], values=[10.0, 20.0, 30.0],
              timestamps=[1, 2, 3]), _check_trend_lines),
        (plot_heatmap,
         dict(task_labels=["a", "b", "c"], run_labels=["x", "y", "z", "w"],
              values_2d=[[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]),
         _check_heatmap_ytick_labels),
        (plot_delta_bar,
         dict(task_labels=["slower", "faster", "stable"],
              delta_pcts=[+10.0, -5.0, 0.0]), _check_delta_bar_colors),
    ], ids=["trend-lines", "heatmap-ytick-labels", "delta-bar-colors"])
    def test_renders_expected_content(self, func, kwargs, check):
        """Each plot type draws its characteristic artists with data."""
        fig = func(**kwargs)
        try:
            check(fig)
        finally:
            plt.close(fig)

    @pytest.mark.parametrize(("func", "kwargs"), [
        (plot_trend, dict(task_label="calibrate", metric="p50",
                          run_labels=["v1"], values=[45.0],
                          timestamps=[1000])),
        (plot_trend, dict(task_label="calibrate", metric="p50",
                          run_labels=[], values=[], timestamps=[])),
        (plot_trend, dict(task_label="calibrate", metric="p50",
                          run_labels=["v1", "v2", "v3", "v4"],
                          values=[45.0, float("nan"), 50.0, 52.0],
                          timestamps=[1, 2, 3, 4])),
        (plot_heatmap, dict(task_labels=["a", "b"], run_labels=["v1"],
                            values_2d=[[10.0], [20.0]])),
        (plot_delta_bar, dict(task_labels=[], delta_pcts=[])),
        (plot_delta_bar, dict(task_labels=["a", "b"],
                              delta_pcts=[5.0, float("nan")])),
    ], ids=["trend-single-run", "trend-empty", "trend-nan",
            "heatmap-single-run", "delta-empty", "delta-nan"])
    def test_degenerate_inputs_never_crash(self, func, kwargs):
        """TRK-6: <2 runs, empty inputs and NaN values still return a
        Figure.
        """
        fig = func(**kwargs)
        assert fig is not None
        assert isinstance(fig, matplotlib.figure.Figure)
        plt.close(fig)

    @pytest.mark.parametrize(("func", "kwargs"), [
        (plot_heatmap, _big_heatmap_kwargs()),
    ], ids=["heatmap"])
    def test_figure_size_capped(self, func, kwargs):
        """Figure sizes stay bounded for large task/run counts."""
        fig = func(**kwargs)
        try:
            assert fig.get_size_inches()[0] <= 30.1
            assert fig.get_size_inches()[1] <= 30.1
        finally:
            plt.close(fig)


class TestHeatmapColorLimits:
    """Color limits must be driven by finite values only (deterministic)."""

    def test_heatmap_with_nan(self):
        fig = plot_heatmap(
            task_labels=["a", "b"], run_labels=["v1", "v2"],
            values_2d=[[10.0, float("nan")], [20.0, 30.0]],
        )
        assert fig is not None
        pm = fig.get_axes()[0].collections[0]
        assert pm.get_clim() == (10.0, 30.0)
        plt.close(fig)

    def test_heatmap_flat_values_deterministic_clim(self):
        fig = plot_heatmap(
            task_labels=["a", "b"], run_labels=["v1", "v2"],
            values_2d=[[7.0, 7.0], [7.0, 7.0]],
        )
        pm = fig.get_axes()[0].collections[0]
        vmin, vmax = pm.get_clim()
        # Degenerate range is widened so the colormap is well-defined.
        assert vmax > vmin
        plt.close(fig)


class TestPlotMisc:
    """Tests for tracker plot helpers."""

    def test_get_tracker_plot_names(self):
        names = get_tracker_plot_names()
        assert names == ["trend", "heatmap", "delta"]

    def test_save_tracker_plot_creates_file(self, tmp_path):
        fig = plt.Figure()
        ax = fig.add_subplot(111)
        ax.plot([1, 2, 3], [1, 4, 9])

        # Override the plots dir for testing
        import lsst.pipe.base._runtime_analyzer.tracker.plot as plot_mod
        original_dir = plot_mod._PLOTS_DIR
        plot_mod._PLOTS_DIR = tmp_path

        try:
            path = save_tracker_plot(fig, "test_plot")
            assert path.endswith(".png")
            import os
            assert os.path.exists(path)
            assert path == str(tmp_path / "test_plot.png")
        finally:
            plot_mod._PLOTS_DIR = original_dir
