"""Tests for the plot module.

Redundancy policy (P2 merge): each (plot function, input scenario) runs
exactly once through :data:`_PLOT_SCENARIOS`; every aspect once asserted
for that scenario (PNG bytes, ax-parameter acceptance, axes count,
per-axis content, y-scale, size caps, label rotation) is asserted
together.  Distinct input scenarios stay as separate param ids; unit
guards with bespoke assertions (facet-axes shape, brace-format parsing,
determinism, figure leaks, legend/label caps) remain standalone.
"""

from __future__ import annotations

import io

import numpy as np
import pytest

pytest.importorskip("pyarrow")  # provided by the [runtime] extra
pytest.importorskip("matplotlib")  # also provided by the [runtime] extra

from lsst.pipe.base._runtime_analyzer import plot as plot_module  # noqa: E402
from lsst.pipe.base._runtime_analyzer.core import _extract_dimension_value  # noqa: E402
from lsst.pipe.base._runtime_analyzer.plot import (  # noqa: E402
    _create_facet_axes,
    get_available_plots,
    get_plot_by_name,
    plot_dimension_dist,
    plot_histogram,
    plot_overview,
    plot_percentile_curve,
    plot_scatter_memory_vs_time,
    plot_task_boxplot,
    plot_time_stacked_bar,
    plot_top_n_bar,
)

from .support import (  # noqa: E402
    MockAnalyzer,
    brace_format_analyzer,
    long_id_analyzer,
    range_analyzer,
)


def _save_to_bytesio(fig: object) -> bytes:
    """Save a matplotlib figure to BytesIO and return the bytes.

    Parameters
    ----------
    fig : `matplotlib.figure.Figure`
        Figure to save.

    Returns
    -------
    data : `bytes`
        PNG-encoded figure bytes.
    """
    import matplotlib.pyplot as plt
    buf = io.BytesIO()
    fig.savefig(buf, format='png')
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


class TestAvailablePlots:
    """Registry of available plot names and lookup by name."""

    def test_plot_names(self) -> None:
        plots = get_available_plots()
        assert 'box' in plots
        assert 'scatter' in plots
        assert 'overview' in plots
        assert len(plots) == 8

    def test_get_by_name(self) -> None:
        fig = get_plot_by_name('box')
        assert fig is not None
        assert callable(fig)

    def test_invalid_name(self) -> None:
        fig = get_plot_by_name('nonexistent')
        assert fig is None


# ---------------------------------------------------------------------------
# Scenario engine: one run per (plot function, input scenario), asserting
# the union of every aspect previously asserted for that scenario (see
# ``test_plot_scenario`` for the recognised case keys).  Every scenario
# additionally asserts a non-empty PNG (the smoke aspect
# ``test_render_png`` used to assert alone).
# ---------------------------------------------------------------------------


def _analyzer_for(case: dict):
    """Build the analyzer a scenario case describes."""
    if "range" in case:
        return range_analyzer(case["range"])
    return MockAnalyzer(**case["mock"])


def _box(func=plot_task_boxplot, **case) -> dict:
    """One scenario case (``plot_task_boxplot`` is the default function)."""
    return {"func": func, **case}


_PLOT_SCENARIOS = [
    # -- plot_task_boxplot ------------------------------------------------
    _box(id="boxplot-basic", mock=dict(n_quanta=50, n_tasks=3),
         ax="new", artist="patches"),
    _box(id="boxplot-no-data", mock=dict(n_quanta=0, n_tasks=0)),
    # Sole tasks= invocation; covers plot/_style.py:111-114 — keep.
    _box(id="boxplot-subset-tasks", mock=dict(n_quanta=30, n_tasks=5),
         kwargs=dict(tasks=['TaskA', 'TaskB'])),
    _box(id="boxplot-labels-dense-90deg", mock=dict(n_quanta=720,
                                                    n_tasks=180),
         rotations={90.0}),
    _box(id="boxplot-labels-spacious-45deg", mock=dict(n_quanta=340,
                                                       n_tasks=34),
         rotations={45.0}),
    # -- y-scale selection (boxplot over hand-picked run times) ----------
    _box(id="boxplot-y-auto-symlog-zeros",
         range=[5000.0] + [0.0] * 10 + [1.0] * 10, y_scale='symlog'),
    _box(id="boxplot-y-linear-override", range=[5000.0] + [1.0] * 30,
         kwargs=dict(y_scale='linear'), y_scale='linear'),
    _box(id="boxplot-y-log-override", range=[1.0, 2.0, 3.0, 4.0],
         kwargs=dict(y_scale='log'), y_scale='log'),
    # -- plot_top_n_bar ----------------------------------------------------
    _box(func=plot_top_n_bar, id="top-n-bar-n10",
         mock=dict(n_quanta=50, n_tasks=3), kwargs=dict(n=10),
         ax="new", artist="patches"),
    _box(func=plot_top_n_bar, id="top-n-bar-color-by-task",
         mock=dict(n_quanta=50, n_tasks=5),
         kwargs=dict(n=10, color_by='task')),
    # -- plot_scatter_memory_vs_time --------------------------------------
    _box(func=plot_scatter_memory_vs_time, id="scatter-highlight",
         mock=dict(n_quanta=100, n_tasks=3),
         kwargs=dict(highlight_top_n=20)),
    # -- plot_time_stacked_bar --------------------------------------------
    _box(func=plot_time_stacked_bar, id="stacked-bar-basic",
         mock=dict(n_quanta=50, n_tasks=5), ax="new", artist="patches"),
    _box(func=plot_time_stacked_bar, id="stacked-bar-height-capped",
         mock=dict(n_quanta=600, n_tasks=150), max_figheight=30.1),
    # -- plot_percentile_curve --------------------------------------------
    # No-ax case keeps the own-figure path (own fig + multi-column legend).
    _box(func=plot_percentile_curve, id="percentile-curve-basic",
         mock=dict(n_quanta=50, n_tasks=3)),
    _box(func=plot_percentile_curve, id="percentile-curve-accepts-ax",
         mock=dict(n_quanta=50, n_tasks=3), ax="new", artist="lines"),
    # -- plot_dimension_dist ----------------------------------------------
    _box(func=plot_dimension_dist, id="boxplot-multi-column",
         mock=dict(n_quanta=30, n_tasks=5, dim_cardinality=4),
         kwargs=dict(dimension='visit', plot_type='boxplot', nrows=2),
         n_axes=5, per_axis=("patches",)),
    _box(func=plot_dimension_dist, id="histogram-multi-column",
         mock=dict(n_quanta=30, n_tasks=5, dim_cardinality=4),
         kwargs=dict(dimension='visit', plot_type='histogram', nrows=2),
         n_axes=5, per_axis=("patches",)),
    _box(func=plot_dimension_dist, id="boxplot-nrows-larger-than-tasks",
         mock=dict(n_quanta=20, n_tasks=2, dim_cardinality=4),
         kwargs=dict(dimension='visit', plot_type='boxplot'),
         n_axes=2, per_axis=("patches",)),
    _box(func=plot_dimension_dist, id="dimension-dist-facet-grid-capped",
         mock=dict(n_quanta=300, n_tasks=100),
         kwargs=dict(dimension='visit'), max_inches=(28.1, 40.1)),
    # -- plot_histogram ----------------------------------------------------
    _box(func=plot_histogram, id="histogram-nrows2-accepts-ax",
         mock=dict(n_quanta=50, n_tasks=3), kwargs=dict(nrows=2),
         ax="none"),
    _box(func=plot_histogram, id="single-task-plot-histogram",
         mock=dict(n_quanta=12, n_tasks=1, dim_cardinality=3),
         n_axes=1, per_axis=("patches",)),
    # -- plot_overview -----------------------------------------------------
    _box(func=plot_overview, id="overview-column-2",
         mock=dict(n_quanta=50, n_tasks=3), kwargs=dict(column=2),
         n_axes=4, per_axis_any=True),
    _box(func=plot_overview, id="overview-column-1",
         mock=dict(n_quanta=50, n_tasks=3), kwargs=dict(column=1),
         n_axes=2, per_axis_any=True),
]

_PLOT_SCENARIO_IDS = [case["id"] for case in _PLOT_SCENARIOS]


class TestPlotScenarios:
    """One run per (function, scenario): the union of every aspect the
    split per-aspect tests used to assert (non-empty PNG, ax-parameter
    acceptance/usage, axes count, per-axis content, y-scale choice,
    canvas-size caps, adaptive label rotation).
    """

    @pytest.mark.parametrize("case", _PLOT_SCENARIOS, ids=_PLOT_SCENARIO_IDS)
    def test_plot_scenario(self, case) -> None:
        """Run one scenario case and assert its registered aspects.

        Case keys: ``func`` plot entry point; ``mock``/``range`` analyzer
        source (MockAnalyzer kwargs / run_times); ``kwargs`` plot kwargs;
        ``ax="new"`` draw into a live ``plt.gca()`` (figure must be that
        axes' figure, ``artist`` populated on the given axes);
        ``ax="none"`` multi-subplot functions accept an ignored ax=None;
        ``n_axes`` expected ``len(fig.axes)``; ``per_axis`` attribute
        non-empty on every axis; ``per_axis_any`` every axis carries
        lines/patches/collections/texts; ``y_scale`` expected y-scale of
        axes[0]; ``max_figheight``/``max_inches`` canvas guards;
        ``rotations`` exact xticklabel rotation set of axes[0]; and always
        a non-empty PNG.
        """
        import matplotlib.pyplot as plt

        analyzer = _analyzer_for(case)
        kwargs = dict(case.get("kwargs", {}))
        ax = None
        ax_mode = case.get("ax")
        if ax_mode == "new":
            # Single-axes functions draw into the given axes.
            ax = plt.gca()
            kwargs["ax"] = ax
        elif ax_mode == "none":
            # Multi-subplot functions accept ax but ignore it (compat).
            kwargs["ax"] = None

        fig = case["func"](analyzer, **kwargs)
        try:
            if ax_mode == "new":
                assert fig is ax.figure
                assert len(getattr(ax, case["artist"])) > 0
            if "n_axes" in case:
                assert len(fig.axes) == case["n_axes"], \
                    f"Expected {case['n_axes']} axes, got {len(fig.axes)}"
            for attr in case.get("per_axis", ()):
                for ax in fig.axes:
                    assert len(getattr(ax, attr)) > 0
            if case.get("per_axis_any"):
                for i, ax in enumerate(fig.axes):
                    artists = (
                        len(ax.lines) + len(ax.patches)
                        + len(ax.collections) + len(ax.texts)
                    )
                    assert artists > 0, \
                        f"Subplot {i} ({ax.get_title()}) has no artists"
            if "y_scale" in case:
                assert fig.axes[0].get_yscale() == case["y_scale"]
            if "max_figheight" in case:
                assert fig.get_figheight() <= case["max_figheight"]
            if "rotations" in case:
                rotations = {t.get_rotation()
                             for t in fig.axes[0].get_xticklabels()}
                assert rotations == case["rotations"]
            if "max_inches" in case:
                width, height = fig.get_size_inches()
                max_w, max_h = case["max_inches"]
                assert width <= max_w
                assert height <= max_h
            data = _save_to_bytesio(fig)
            assert len(data) > 0
        finally:
            plt.close(fig)


class TestCreateFacetAxes:
    """PLOT-1 regression: _create_facet_axes must always return a flat
    1-D array of exactly len(tasks) Axes.
    """

    @pytest.mark.parametrize(("tasks", "nrows", "n_axes"), [
        (['TaskA'], 4, 1),
        (['A', 'B', 'C', 'D', 'E'], 2, 5),
        (['A', 'B', 'C', 'D'], 2, 4),
        (['A', 'B'], 0, 2),
    ], ids=["single-task-flat", "multi-column-exact-panels",
            "exact-grid-no-surplus", "nrows-zero-guard"])
    def test_facet_axes_flat_exact_count(self, tasks, nrows, n_axes) -> None:
        """Flat 1-D array with exactly one panel per task; surplus grid
        cells (e.g. 5 tasks / 2 rows -> 6 cells) removed from the figure
        and every returned axis belonging to it.
        """
        import matplotlib.pyplot as plt

        fig, axes = _create_facet_axes(tasks, nrows=nrows)
        try:
            assert isinstance(axes, np.ndarray)
            assert axes.ndim == 1
            assert len(axes) == n_axes
            assert len(fig.axes) == n_axes
            # All returned axes belong to the figure (not the deleted ones).
            assert all(ax.get_figure() is fig for ax in axes)
            assert hasattr(axes[0], 'boxplot')
        finally:
            plt.close(fig)

    def test_zero_tasks_blank_figure(self) -> None:
        """Zero tasks must not crash plt.subplots(0) — blank fig instead."""
        import matplotlib.pyplot as plt

        fig, axes = _create_facet_axes([], nrows=4)
        try:
            assert len(axes) == 0
            texts = [t.get_text() for t in fig.texts]
            assert 'No data available' in texts
        finally:
            plt.close(fig)


class TestDimensionDistFacets:
    """PLOT-1 regressions for plot_dimension_dist facet indexing (the
    panel-count/content aspects of these scenarios now ride the shared
    scenario engine above).
    """

    def test_single_task_boxplot(self) -> None:
        """Exactly one task must not crash (axes was an ndarray before)."""
        analyzer = MockAnalyzer(n_quanta=12, n_tasks=1, dim_cardinality=3)
        fig = plot_dimension_dist(analyzer, dimension='visit',
                                  plot_type='boxplot')
        assert len(fig.axes) == 1
        # Boxes actually drawn (patch_artist=True -> one patch per box).
        assert len(fig.axes[0].patches) > 0
        labels = [t.get_text() for t in fig.axes[0].get_xticklabels()]
        assert '1' in labels and 'unknown' not in labels
        assert len(_save_to_bytesio(fig)) > 0

    def test_zero_tasks_blank_fig(self) -> None:
        """Zero tasks -> blank figure with 'No data available', no crash."""
        analyzer = MockAnalyzer(n_quanta=0, n_tasks=0)
        fig = plot_dimension_dist(analyzer, dimension='visit')
        texts = [t.get_text() for ax in fig.axes for t in ax.texts]
        texts += [t.get_text() for t in fig.texts]
        assert 'No data available' in texts
        assert len(_save_to_bytesio(fig)) > 0


class TestDimensionDistBraceFormat:
    """PLOT-2 regression: real str(DataCoordinate) brace-format ids must
    parse so boxes/bars are drawn from real dimension values.
    """

    def test_boxplot_brace_ids(self) -> None:
        analyzer = brace_format_analyzer()
        fig = plot_dimension_dist(analyzer, dimension='visit',
                                  plot_type='boxplot')
        try:
            assert len(fig.axes) == 2  # TaskA + TaskB
            for ax in fig.axes:
                labels = [t.get_text() for t in ax.get_xticklabels()]
                # Old broken parser returned 'unknown' for every brace id.
                assert 'unknown' not in labels
                assert any('52' in lab or '53' in lab for lab in labels)
                assert len(ax.patches) > 0  # boxes drawn, not blank
        finally:
            _save_to_bytesio(fig)

    def test_histogram_brace_numeric(self) -> None:
        analyzer = brace_format_analyzer()
        fig = plot_dimension_dist(analyzer, dimension='visit',
                                  plot_type='histogram')
        try:
            for ax in fig.axes:
                xs = sorted(p.get_x() for p in ax.patches)
                # Deterministic decade bins at exact decade starts.
                assert xs == [520.0, 530.0]
        finally:
            _save_to_bytesio(fig)

    def test_histogram_brace_nonnumeric(self) -> None:
        """Non-numeric dimension (band) falls back to per-value counts."""
        analyzer = brace_format_analyzer()
        fig = plot_dimension_dist(analyzer, dimension='band',
                                  plot_type='histogram')
        try:
            for ax in fig.axes:
                assert len(ax.patches) == 2  # 'g' and 'r'
                assert sorted(int(p.get_height()) for p in ax.patches) == [4, 5]
        finally:
            _save_to_bytesio(fig)


class TestHistogramDeterminism:
    """PLOT-3: decade binning must be deterministic (no np.random jitter)."""

    def test_repeated_calls_identical_bars(self) -> None:
        import matplotlib.pyplot as plt

        analyzer = MockAnalyzer(n_quanta=40, n_tasks=2, dim_cardinality=8)

        def bar_geometry(fig):
            return [[(p.get_x(), p.get_width(), p.get_height())
                     for p in ax.patches] for ax in fig.axes]

        fig1 = plot_dimension_dist(analyzer, dimension='visit',
                                   plot_type='histogram')
        bars1 = bar_geometry(fig1)
        plt.close(fig1)

        fig2 = plot_dimension_dist(analyzer, dimension='visit',
                                   plot_type='histogram')
        bars2 = bar_geometry(fig2)
        plt.close(fig2)

        assert bars1 == bars2
        assert all(len(panel) > 0 for panel in bars1)
        # Honest count bars: integer counts summing to the numeric sample size.
        total = sum(h for panel in bars1 for (_, _, h) in panel)
        assert total == 40

    def test_no_random_in_plot_module(self) -> None:
        """plot.py source must not reference np.random (determinism)."""
        import inspect

        src = inspect.getsource(plot_module)
        assert 'np.random' not in src


class TestExtractDimensionValue:
    """plot.py reuses core's robust parser for dimension values.

    The parser's input formats (legacy comma + real str(DataCoordinate)
    brace) are exhaustively tabled in test_core.py
    (TestDimensionDist::test_extract_dimension_value_formats); only the
    no-duplication contract lives here.
    """

    def test_plot_module_uses_core_parser(self) -> None:
        """plot.py must not define its own parser duplicate."""
        assert plot_module._extract_dimension_value is _extract_dimension_value
        # The brace parser lives in runtime_table; plot must not carry or
        # duplicate it (only the dimension extractor may be imported).
        assert not hasattr(plot_module, 'parse_dimension_string')
        assert not hasattr(plot_module, '_split_top_level')
        assert not hasattr(plot_module, '_extract_dim_val')


class TestOverviewFigureManagement:
    """plot_overview figure bookkeeping (the subplot-content/column-grid
    aspects of both column scenarios ride the scenario engine above).
    """

    def test_overview_no_leaked_figures(self) -> None:
        """Calling plot_overview leaves no orphan matplotlib figures open."""
        import matplotlib.pyplot as plt

        initial_figs = len(plt.get_fignums())

        analyzer = MockAnalyzer(n_quanta=50, n_tasks=3)
        plot_overview(analyzer, column=2)

        # The overview returns a fig, so there should be exactly 1 new figure
        assert len(plt.get_fignums()) == initial_figs + 1

        plt.close('all')  # cleanup


class TestScaleGuards:
    """Remaining scale guards with bespoke assertions (the height-cap,
    rotation and facet-grid scenarios ride the scenario engine above).
    """

    def test_bar_labels_length_capped(self) -> None:
        analyzer = long_id_analyzer(n_tasks=3)
        fig = plot_top_n_bar(analyzer, n=10)
        labels = [t.get_text() for t in fig.axes[0].get_yticklabels()]
        assert len(labels) == 10
        assert all(len(lbl) <= 72 for lbl in labels)
        _save_to_bytesio(fig)

    def test_scatter_legend_multicolumn(self) -> None:
        analyzer = MockAnalyzer(n_quanta=400, n_tasks=60)
        fig = plot_scatter_memory_vs_time(analyzer)
        legend = fig.axes[0].get_legend()
        assert legend is not None
        # Private attribute is the only accessor; matplotlib has no public
        # Legend.get_ncols().
        ncol = legend._ncols
        n_tasks = len(np.unique(analyzer.table.labels()))
        assert ncol >= max(1, n_tasks // 24)
        _save_to_bytesio(fig)
