"""Tests for CLI tracked-run subcommands."""

from __future__ import annotations

from unittest.mock import patch

import pytest

pytest.importorskip("click")  # provided by the [runtime] extra
pytest.importorskip("pyarrow")  # provided by the [runtime] extra

from click.testing import CliRunner  # noqa: E402

from lsst.pipe.base._runtime_analyzer.cli import main  # noqa: E402

from ..support import (  # noqa: E402
    boom,
    captured_graph_loads,
    db_run_row,
    make_task_analyzer,
    write_task_cache,
)


@pytest.fixture
def runner():
    """Return a ``click.testing.CliRunner`` bound to the CLI ``main``."""
    return CliRunner()


class TestTrackedRunCliSurface:
    """Help output and usage errors for the ``tracked-run`` group."""

    @pytest.mark.parametrize(("argv", "expected"), [
        (["tracked-run", "record", "--help"], ["--label", "--raw"]),
        (["tracked-run", "list", "--help"], ["--limit"]),
        (["tracked-run", "compare", "--help"],
         ["--from", "--to", "--metric"]),
        (["tracked-run", "trend", "--help"], ["--task"]),
        (["tracked-run", "--help"],
         ["record", "list", "compare", "trend", "alerts"]),
    ], ids=["record-help", "list-help", "compare-help", "trend-help",
            "group-help-lists-subcommands"])
    def test_help_output(self, runner, argv, expected):
        """Each subcommand renders its help listing the documented
        options (and the group lists all subcommands).
        """
        result = runner.invoke(main, argv)
        assert result.exit_code == 0
        for needle in expected:
            assert needle in result.output

    @pytest.mark.parametrize("argv", [
        ["tracked-run", "record", "test.qg"],
    ], ids=["record-without-label"])
    def test_requires_arguments(self, runner, argv, db_path):
        """Missing required arguments exit non-zero (and never crash
        silently without output).
        """
        result = runner.invoke(main, argv)
        assert result.exit_code != 0
        # The error should carry message output
        assert result.output is not None

    def test_empty_list(self, runner, db_path):
        result = runner.invoke(main, ["tracked-run", "list"])
        assert result.exit_code == 0
        # Should show "No recorded runs" or empty table
        assert result.output is not None

    def test_group_in_main_help(self, runner):
        result = runner.invoke(main, ["--help"])
        assert "tracked-run" in result.output


class TestTrackedRunRecordFromCache:
    """Task 5.6: tracked-run record from positional .parquet caches."""

    def test_record_from_cached_table(self, runner, db_path, tmp_path):

        cache = tmp_path / "run.parquet"
        assert write_task_cache(
            cache,
            [('TaskA', 10.0), ('TaskA', 20.0), ('TaskB', 30.0)],
        ) == 3

        with patch('lsst.pipe.base._runtime_analyzer.cli.extract_merged_runtime_table',
                   side_effect=boom):
            result = runner.invoke(
                main, ["tracked-run", "record", "-l", "cache-v1", str(cache)]
            )

        assert result.exit_code == 0, f"record crashed: {result.exception}"
        assert "Saved run 'cache-v1' (3 quanta, 2 tasks)" in result.output

        # Table-derived input records repo/collection as NULL (consistent
        # with positional-file behavior).
        assert db_run_row(db_path, "cache-v1") == (None, None)

    def test_record_cached_table_auto_comparison_fires(self, runner,
                                                       db_path, tmp_path):

        cache_v1 = tmp_path / "v1.parquet"
        assert write_task_cache(
            cache_v1, [('TaskA', 100.0), ('TaskB', 50.0)]
        ) == 2
        cache_v2 = tmp_path / "v2.parquet"
        assert write_task_cache(
            cache_v2, [('TaskA', 150.0), ('TaskB', 40.0)]
        ) == 2

        result1 = runner.invoke(
            main, ["tracked-run", "record", "-l", "cache-v1", str(cache_v1)]
        )
        assert result1.exit_code == 0, f"record v1 crashed: {result1.exception}"
        # First run: no comparison yet, only the confirmation.
        assert "Saved run 'cache-v1'" in result1.output
        assert "Auto-comparison" not in result1.output

        result2 = runner.invoke(
            main, ["tracked-run", "record", "-l", "cache-v2", str(cache_v2)]
        )
        assert result2.exit_code == 0, f"record v2 crashed: {result2.exception}"
        assert "Saved run 'cache-v2' (2 quanta, 2 tasks)" in result2.output
        # Auto-comparison against the previous run fires.
        assert "Auto-comparison with most recent run: 'cache-v1'" in \
            result2.output

        assert db_run_row(db_path, "cache-v1") == (None, None)
        assert db_run_row(db_path, "cache-v2") == (None, None)


class TestTrackedRunRecordOrderedFlags:
    """Task 3.5: ordered source flags on ``tracked-run record``."""

    @pytest.mark.parametrize(("argv", "label", "rows", "expected_captured",
                              "expected_row"), [
        (['tracked-run', 'record', '-l', 'mix',
          '-r', '/repo', '-c', 'collA', '--graph', 'extra.qg',
          '-r', '/repo2', '-c', 'collB'],
         'mix',
         [('TaskA', 10.0), ('TaskB', 20.0)],
         [(('/repo', 'collA'), ('extra.qg', None), ('/repo2', 'collB'))],
         ('/repo', 'collA')),
        (['-r', 'groupR', '-c', 'gcoll',
          'tracked-run', 'record', '-l', 'override',
          '-r', 'recR', '-c', 'rcoll'],
         'override',
         [('TaskA', 10.0)],
         [(('recR', 'rcoll'),)],
         ('recR', 'rcoll')),
        (['-r', 'groupR', '-c', 'gcoll',
          'tracked-run', 'record', '-l', 'fallback'],
         'fallback',
         [('TaskA', 10.0)],
         [(('groupR', 'gcoll'),)],
         ('groupR', 'gcoll')),
    ], ids=["ordered-mixed-first-butler-wins",
            "record-level-overrides-group",
            "group-level-flags-fallback"])
    def test_record_source_flags(self, runner, db_path, argv, label, rows,
                                 expected_captured, expected_row):
        """Record-level ordered flags load in exact order with the
        stored repo/collection from the FIRST butler entry; record-level
        flags replace the group list, and with none given the group
        ordered list is used (documented group-scope usage).
        """
        analyzer = make_task_analyzer(rows)

        with captured_graph_loads(analyzer) as captured:
            result = runner.invoke(main, argv)

        assert result.exit_code == 0, f"record crashed: {result.exception}"
        # butler + graph share one bulk call, pairs in typed order.
        assert captured == expected_captured
        assert db_run_row(db_path, label) == expected_row

    def test_record_cache_only_records_null(self, runner, db_path,
                                            tmp_path):
        """``--table``-only record stores NULL repo/collection and never
        touches the graph reader.
        """
        cache = tmp_path / "run.parquet"
        assert write_task_cache(cache, [('TaskA', 10.0)]) == 1

        with patch('lsst.pipe.base._runtime_analyzer.cli.extract_merged_runtime_table',
                   side_effect=AssertionError(
                       "graph load attempted for --table-only record"
                   )):
            result = runner.invoke(
                main,
                ['tracked-run', 'record', '-l', 'cache-flag',
                 '-T', str(cache)],
            )

        assert result.exit_code == 0, f"record crashed: {result.exception}"
        assert db_run_row(db_path, 'cache-flag') == (None, None)
