"""Integration tests for the CLI module."""

from __future__ import annotations

import json
import uuid
from contextlib import ExitStack
from importlib.util import find_spec
from pathlib import Path
from unittest import mock

import numpy as np
import pytest

pytest.importorskip("pyarrow")  # provided by the [runtime] extra

from lsst.pipe.base._runtime_analyzer.runtime_table import QuantumRuntimeTable  # noqa: E402

from .support import (  # noqa: E402
    boom,
    captured_graph_loads,
    fake_from_args,
    graph_node,
    invoke_cli,
    make_cli_ctx,
    override_graph_loader,
    record_scenario_cli,
)

_HAS_CLICK = find_spec("click") is not None


class TestCliHelp:
    """Tests for CLI help output."""

    @pytest.mark.skipif(not _HAS_CLICK, reason="click not installed")
    @pytest.mark.parametrize(("args", "expected"), [
        (['--help'],
         ['summary', 'top-quantities', 'dimension', 'bottleneck']),
        (['help-plots'], ['box', 'scatter']),
        (['help-tables'], ['summary', 'bottleneck']),
    ], ids=["main-help", "help-plots", "help-tables"])
    def test_help_output(self, args, expected) -> None:
        """Help entry points exit 0 and list their documented commands;
        main-help also covers the single-positional-arg help contract.
        """
        result = invoke_cli(args)
        assert result.exit_code == 0
        for substr in expected:
            assert substr in result.output


class TestMultiSourceCLI:
    """Tests for the multiple source input modes."""

    @pytest.mark.skipif(not _HAS_CLICK, reason="click not installed")
    def test_no_input_rejected(self) -> None:
        """No sources at all must fail."""
        result = invoke_cli(['summary'])
        assert result.exit_code != 0
        assert 'Must provide' in result.output


class TestBottleneckCommand:
    """CLI-1: the bottleneck command must render outlier tables cleanly."""

    @pytest.mark.skipif(not _HAS_CLICK, reason="click not installed")
    @pytest.mark.parametrize(("rows", "cli_args", "expect_header"), [
        ([('TaskA', 10.0)] * 9 + [('TaskA', 1000.0)]
         + [('TaskB', 20.0)] * 10, [], True),
        ([('TaskA', 10.0)] * 9 + [('TaskA', 1000.0)],
         ['--outlier-method', 'zscore'], False),
    ], ids=["iqr-default", "zscore-method"])
    def test_bottleneck_renders_outliers(self, make_task_analyzer, rows,
                                         cli_args, expect_header) -> None:
        """Regression: a NameError (undefined header_str/n_show) crashed
        the bottleneck command whenever the outlier table was non-empty
        (both the default IQR method and zscore render outliers).
        """
        # TaskA has nine quanta at 10 s and one clear outlier at 1000 s;
        # IQR of the nine flat values is 0, so the 1000 s quantum is an
        # outlier.  TaskB is flat and yields no outliers.
        analyzer = make_task_analyzer(rows)

        with mock.patch('lsst.pipe.base._runtime_analyzer.cli._load_analyzer',
                        return_value=analyzer):
            result = invoke_cli(['bottleneck'] + cli_args)

        assert result.exit_code == 0, f"bottleneck crashed: {result.exception}"
        if expect_header:
            assert 'Task Bottleneck Analysis:' in result.output
            assert 'Top 1 Outlier Quanta:' in result.output
        # The outlier row itself must be rendered.
        outlier_section = result.output.split('Outlier Quanta:')[-1]
        assert 'TaskA' in outlier_section
        assert 'SLOW' in outlier_section


class TestSourceProducerUnification:
    """CLI-4: all graph/Butler source shapes route through
    extract_merged_runtime_table (the merged-table producer).
    """

    @pytest.mark.parametrize(("ctx_kwargs", "expected_call"), [
        (dict(repo='myrepo', collections=('coll1', 'coll2', 'coll3')),
         [('myrepo', 'coll1'), ('myrepo', 'coll2'), ('myrepo', 'coll3')]),
        (dict(graph=('a.qg', 'b.qg')), [('a.qg', None), ('b.qg', None)]),
    ], ids=["repo-multiple-collections", "files-only"])
    def test_source_shapes_route_through_producer(
        self, make_task_analyzer, ctx_kwargs, expected_call
    ) -> None:
        from lsst.pipe.base._runtime_analyzer import cli

        table = make_task_analyzer([('TaskA', 1.0)]).table
        ctx = make_cli_ctx(**ctx_kwargs)
        with mock.patch.object(
            cli, 'extract_merged_runtime_table', return_value=table
        ) as m:
            analyzer = cli._load_analyzer(ctx)
        assert analyzer.table is table
        m.assert_called_once_with(expected_call)

    @pytest.mark.parametrize(("ctx_kwargs", "match_msg"), [
        (dict(graph=('a.qg',), repo='myrepo', collections=('coll1',)),
         'Cannot mix'),
        (dict(repo='myrepo'), 'Must provide'),
    ], ids=["mixed-graph-and-repo", "repo-without-collections"])
    def test_invalid_source_combinations_rejected(self, ctx_kwargs,
                                                  match_msg) -> None:
        import click

        from lsst.pipe.base._runtime_analyzer import cli

        ctx = make_cli_ctx(**ctx_kwargs)
        with mock.patch.object(cli, 'extract_merged_runtime_table') as m:
            with pytest.raises(click.UsageError, match=match_msg):
                cli._load_analyzer(ctx)
        m.assert_not_called()

    def test_load_failure_wrapped_as_usage_error(self) -> None:
        import click

        from lsst.pipe.base._runtime_analyzer import cli

        ctx = make_cli_ctx(repo='myrepo', collections=('coll1',))
        with mock.patch.object(cli, 'extract_merged_runtime_table',
                               side_effect=RuntimeError('boom')):
            with pytest.raises(click.UsageError,
                               match='Failed to load Butler collections: boom'):
                cli._load_analyzer(ctx)

    def test_no_butler_import_in_cli(self) -> None:
        """CLI-4: cli.py delegates graph loading to the runtime_table
        producers (graph opening stays in extract_merged_runtime_table).
        """
        import inspect

        from lsst.pipe.base._runtime_analyzer import cli

        source = inspect.getsource(cli)
        assert 'ProvenanceQuantumGraph.from_args' not in source


class TestRecordCommand:
    """CLI-2/CLI-3/CLI-5: tracked-run record persistence values.

    One scenario table; every aspect of an invocation (recorded source
    fields, producer call order, confirmation output, structural
    graph_hash, real package version) is asserted together — the
    graph_hash/package-version aspects share the single-collection
    invocation and no longer run as separate tests.
    """

    @pytest.mark.skipif(not _HAS_CLICK, reason="click not installed")
    @pytest.mark.parametrize(
        ("rows", "runner_args", "expected_recorded", "expected_load_call",
         "expected_output", "check_identity"), [
            ([('TaskA', 1.0), ('TaskB', 2.0)],
             ['--repo', 'R', '--collection', 'coll1',
              'tracked-run', 'record', '--label', 'L'],
             {'collection': 'coll1', 'repo': 'R', 'label': 'L'},
             [('R', 'coll1')], 'Saved run', True),
            ([('TaskA', 1.0)],
             ['--repo', 'R', '-c', 'collA', '-c', 'collB',
              'tracked-run', 'record', '-l', 'L'],
             {'repo': 'R', 'collection': 'collA'},
             [('R', 'collA'), ('R', 'collB')], None, False),
            ([('TaskA', 1.0)],
             ['tracked-run', 'record', '-l', 'L', 'a.qg'],
             {'repo': None, 'collection': None},
             [('a.qg', None)], None, False),
        ],
        ids=["single-collection", "multiple-collections-first-wins",
             "positional-files-no-collection"])
    def test_record_persists_source_fields(
        self, make_task_analyzer, rows, runner_args, expected_recorded,
        expected_load_call, expected_output, check_identity
    ) -> None:
        """CLI-2 regression: a single --collection must be recorded, not
        None; with multiple butler sources the FIRST butler entry's
        repo/collection are recorded (design D3); positional-file runs
        record repo=None and collection=None.  Sources must reach the
        producer in order (single unified path).  For the canonical
        single-collection invocation, CLI-3 (graph_hash = hash_graph
        (analyzer), structural and non-empty) and CLI-5 (analyzer_version
        from lsst.pipe.base._runtime_analyzer.__version__
        ) are asserted on the same recorded kwargs.
        """
        import hashlib

        import lsst.pipe.base._runtime_analyzer as ra
        from lsst.pipe.base._runtime_analyzer.tracker.database import hash_graph

        analyzer = make_task_analyzer(rows)
        result, recorded, load_mock = record_scenario_cli(
            analyzer, runner_args,
        )
        assert result.exit_code == 0, f"record crashed: {result.exception}"
        for key, value in expected_recorded.items():
            if value is None:
                assert recorded[key] is None
            else:
                assert recorded[key] == value
        load_mock.assert_called_once_with(expected_load_call)
        if expected_output is not None:
            assert expected_output in result.output
        if check_identity:
            assert recorded['graph_hash'] == hash_graph(analyzer)
            assert recorded['graph_hash'] != hashlib.sha256(b'').hexdigest()
            assert hasattr(ra, '__version__')
            assert recorded['analyzer_version'] == ra.__version__

    @pytest.mark.skipif(not _HAS_CLICK, reason="click not installed")
    @pytest.mark.parametrize(("record_args", "out_name"), [
        (['--raw'], 'raw_cache.parquet'),
    ], ids=["raw-keeps-source-labels"])
    def test_record_writes_cache_with_source_labels(
        self, make_task_analyzer, tmp_path: Path, record_args, out_name
    ) -> None:
        """tracked-run record must write the --save-intermediate-table
        cache after a successful record (same writer/metadata as the
        group-level loader); --raw must not leak per-quantum task labels
        into the cache metadata sources.
        """
        analyzer = make_task_analyzer([('TaskA', 1.0), ('TaskB', 2.0)])
        out = str(tmp_path / out_name)

        result, _recorded, _load_mock = record_scenario_cli(
            analyzer,
            ['--save-intermediate-table', out,
             '--repo', 'R', '-c', 'coll',
             'tracked-run', 'record', '-l', 'L'] + record_args)

        assert result.exit_code == 0, f"record crashed: {result.exception}"
        assert Path(out).exists(), f"cache not written: {result.output}"
        assert f"Saved intermediate table: {out} (2 rows)" in result.output
        loaded = QuantumRuntimeTable.from_parquet(out)
        assert loaded.n_rows == 2
        assert json.loads(
            loaded.to_arrow().schema.metadata[b'sources']
        ) == ['R:coll']


class TestHashGraphCliContract:
    """CLI-3: hash_graph(analyzer) fingerprints task-label counts."""

    @pytest.mark.parametrize(("rows_a", "rows_b", "expect_equal"), [
        ([('TaskA', 1.0), ('TaskB', 2.0)],
         [('TaskA', 1.0), ('TaskC', 2.0)], False),
        ([('TaskA', 1.0), ('TaskB', 5.0)],
         [('TaskA', 9.0), ('TaskB', 500.0)], True),
    ], ids=["different-task-sets-differ", "identical-structure-matches"])
    def test_hash_fingerprints_structure(self, make_task_analyzer, rows_a,
                                         rows_b, expect_equal) -> None:
        """Identical task/quantum structure hashes identically even when
        built as separate analyzer objects (fresh UUIDs); different task
        sets or counts must differ.
        """
        from lsst.pipe.base._runtime_analyzer.tracker.database import hash_graph

        a = make_task_analyzer(rows_a)
        b = make_task_analyzer(rows_b)
        assert (hash_graph(a) == hash_graph(b)) is expect_equal


class TestPlotOutputDirMessage:
    """CLI-5: the plot command must state the resolved output directory."""

    @pytest.mark.skipif(not _HAS_CLICK, reason="click not installed")
    def test_plot_reports_resolved_dir(self, tmp_path: Path,
                                       make_task_analyzer) -> None:
        analyzer = make_task_analyzer([('TaskA', 1.0), ('TaskA', 2.0)])
        fig = mock.MagicMock()

        outdir = tmp_path / 'myplots'
        with mock.patch('lsst.pipe.base._runtime_analyzer.cli._load_analyzer',
                        return_value=analyzer), \
                mock.patch('lsst.pipe.base._runtime_analyzer.cli.get_plot_by_name',
                           return_value=mock.MagicMock(return_value=fig)):
            result = invoke_cli(
                ['plot', '--plots', 'box', '-o', str(outdir)])

        assert result.exit_code == 0, f"plot crashed: {result.exception}"
        assert f"Writing plots to: {outdir.resolve()}" in result.output
        assert 'Saved plot' in result.output


class TestExportParquetNativeDtypes:
    """CLI-6: export_parquet must not flatten everything to object dtype."""

    def test_numeric_and_bytes_dtypes_preserved(self, tmp_path: Path) -> None:
        import astropy.table
        import pyarrow as pa
        import pyarrow.parquet as pq

        from lsst.pipe.base._runtime_analyzer.console import export_parquet

        table = astropy.table.Table({
            'int_col': np.array([1, 2, 3], dtype=np.int64),
            'float_col': np.array([1.5, np.nan, 3.5], dtype=np.float32),
            'str_col': np.array(['alpha', 'beta', 'gamma']),
            'bytes_col': np.array([b'\x01\x02', b'\x03\x04', b'\x05\x06'],
                                  dtype='S2'),
        })
        dest = tmp_path / 'native.parquet'
        export_parquet(table, dest)

        arrow = pq.read_table(dest)
        assert arrow.num_rows == 3
        # Numeric columns keep native numeric types (not binary/str).
        assert pa.types.is_integer(arrow.schema.field('int_col').type)
        assert pa.types.is_floating(arrow.schema.field('float_col').type)
        assert arrow.column('int_col').to_pylist() == [1, 2, 3]
        # NaN in a native float column survives as NaN.
        assert np.isnan(arrow.column('float_col').to_pylist()[1])
        assert arrow.column('str_col').to_pylist() == [
            'alpha', 'beta', 'gamma']
        # Fixed-width bytes columns round-trip as Parquet binary (bytes).
        assert arrow.column('bytes_col').to_pylist() == [
            b'\x01\x02', b'\x03\x04', b'\x05\x06']

    def test_masked_values_become_nulls(self, tmp_path: Path) -> None:
        import astropy.table
        import pyarrow.parquet as pq

        from lsst.pipe.base._runtime_analyzer.console import export_parquet

        table = astropy.table.Table()
        table['m'] = astropy.table.MaskedColumn([10, 20, 30],
                                                mask=[False, True, False])
        dest = tmp_path / 'masked.parquet'
        export_parquet(table, dest)

        values = pq.read_table(dest).column('m').to_pylist()
        assert values[0] == 10
        assert values[2] == 30
        # Masked value must be a null, NOT astropy's 999999 sentinel.
        assert values[1] is None
        assert 999999 not in values


def _check_summary_renders_cache(result, cache, out):
    """Assert a cached positional feeds the summary table with no graph."""
    assert result.exit_code == 0, f"summary crashed: {result.exception}"
    assert 'Loaded 1' in result.output
    assert '3 total quanta' in result.output
    assert 'TaskA' in result.output
    assert 'TaskB' in result.output


def _check_dimension_groups_from_dims(result, cache, out):
    """`dimension --by visit` groups via the captured dims_json column."""
    assert result.exit_code == 0, f"dimension crashed: {result.exception}"
    assert 'Loaded 1 table source(s), 2 total quanta.' in result.output
    assert 'Task: TaskA' in result.output
    group_rows = [ln for ln in result.output.splitlines()
                  if ln.startswith("  ") and " | " in ln]
    # One row per distinct visit value: exactly 2 group rows.
    assert len(group_rows) == 2
    group_keys = sorted(ln.split("|")[0].strip() for ln in group_rows)
    assert group_keys == ['10', '20']


def _check_record_saves_cache_from_table(result, cache, out):
    """Record from a cached table: table-mode header, save flag works."""
    assert result.exit_code == 0, f"record crashed: {result.exception}"
    assert 'Loaded 1 table source(s), 2 total quanta.' in result.output
    assert Path(out).exists(), f"cache not written: {result.output}"
    assert f"Saved intermediate table: {out} (2 rows)" in result.output
    loaded = QuantumRuntimeTable.from_parquet(out)
    assert loaded.n_rows == 2
    assert json.loads(
        loaded.to_arrow().schema.metadata[b'sources']
    ) == [cache]


# Every cached-input guard case explodes on ANY graph/Butler access.
_BOOM_PATCHES = [
    ('lsst.pipe.base._runtime_analyzer.cli.extract_merged_runtime_table',
     {'side_effect': boom}),
    ('lsst.pipe.base.quantum_graph.ProvenanceQuantumGraph.from_args',
     {'side_effect': boom}),
]


@pytest.mark.skipif(not _HAS_CLICK, reason="click not installed")
class TestCachedInputGuardMatrix:
    """Guard matrix: positional cached-table inputs must load through the
    Parquet reader only.  Every graph/Butler seam is patched with ``boom``
    (plus record-run stubs where needed) and the subcommand must still
    succeed with cache-derived output — any graph/Butler access raises
    before the assertions run.
    """

    # data_ids in the "dimension" case are unparseable for "visit": the
    # CLI can only group correctly via the captured dims_json column.
    @pytest.mark.parametrize("case", [
        dict(
            cache_name='run.parquet',
            cache_spec=[
                ('TaskA', 10.0, "{visit=1, band='r'}", 1),
                ('TaskA', 20.0, "{visit=2, band='r'}", 2),
                ('TaskB', 30.0, "{visit=1, band='r'}", 3),
            ],
            dims=None,
            cache_sources=("run.qg",),
            args=lambda cache, out: ['summary', cache],
            check=_check_summary_renders_cache,
        ),
        dict(
            cache_name='run.parquet',
            cache_spec=[
                ('TaskA', 10.0, "opaque-a", 1),
                ('TaskA', 30.0, "opaque-b", 2),
            ],
            dims={
                uuid.UUID(int=1).bytes: {"visit": "10"},
                uuid.UUID(int=2).bytes: {"visit": "20"},
            },
            cache_sources=None,
            args=lambda cache, out: ['dimension', '--by', 'visit', cache],
            check=_check_dimension_groups_from_dims,
        ),
        dict(
            cache_name='in.parquet',
            cache_spec=[
                ('TaskA', 10.0, "{visit=1}", 1),
                ('TaskB', 20.0, "{visit=2}", 2),
            ],
            dims=None,
            cache_sources=None,
            args=lambda cache, out: ['--save-intermediate-table', out,
                                     'tracked-run', 'record', '-l', 'L',
                                     cache],
            patches=_BOOM_PATCHES[:1] + [
                ('lsst.pipe.base._runtime_analyzer.cli.record_run',
                 {'side_effect': lambda **kwargs:
                  {'quanta': 2, 'tasks': 2}}),
                ('lsst.pipe.base._runtime_analyzer.tracker.history._auto_compare', {}),
            ],
            check=_check_record_saves_cache_from_table,
        ),
    ], ids=["summary-positional-parquet", "dimension-dims-from-cache",
            "record-tracked-run-from-cache"])
    def test_cached_inputs_never_touch_graph_or_butler(self, tmp_path,
                                                       write_cache,
                                                       case) -> None:
        cache = write_cache(case['cache_name'], case['cache_spec'],
                            dims=case['dims'],
                            sources=case['cache_sources'])
        out = str(tmp_path / 'out.parquet')
        with ExitStack() as stack:
            for target, kwargs in case.get('patches', _BOOM_PATCHES):
                stack.enter_context(mock.patch(target, **kwargs))
            result = invoke_cli(case['args'](cache, out))
        case['check'](result, cache, out)


@pytest.mark.skipif(not _HAS_CLICK, reason="click not installed")
class TestParquetTableMode:
    """Task 5.5: positional .parquet dispatch, preprocess, and the
    --save-intermediate-table flag.
    """

    def test_multiple_positional_caches_first_wins(self, tmp_path,
                                                   write_cache):
        cache_a = write_cache('a.parquet', [
            ('TaskT', 10.0, "{visit=10, band='r'}", 1),
            ('TaskT', 20.0, "{visit=20, band='g'}", 2),
        ], sources=("run.qg",))
        cache_b = write_cache('b.parquet', [
            # Shared key with a.parquet (must be shadowed) + unique row.
            ('TaskT', 999.0, "{visit=10, band='r'}", 3),
            ('TaskT', 30.0, "{visit=30, band='i'}", 4),
        ], sources=("b.qg",))

        result = invoke_cli(['summary', cache_a, cache_b])

        assert result.exit_code == 0, f"summary crashed: {result.exception}"
        assert 'Loaded 2' in result.output
        # 4 rows, shared (task_label, data_id) collapses to 1 -> 3 quanta.
        assert '3 total quanta' in result.output

    def test_mixed_positionals_merge_left_to_right(self, tmp_path,
                                                   write_cache):
        from lsst.pipe.base.quantum_graph import ProvenanceQuantumGraph

        primary = str(tmp_path / 'primary.qg')
        cached = write_cache('cached.parquet', [
            # Shared key with the graph node (graph is left-most -> wins).
            ('TaskT', 999.0, "{visit=10, band='r'}", 3),
            ('TaskT', 30.0, "{visit=30, band='i'}", 4),
        ], sources=("run.qg",))

        graph_nodes = [
            graph_node(1, 'TaskT', 10.0, "{visit=10, band='r'}"),
            graph_node(2, 'TaskT', 20.0, "{visit=20, band='g'}"),
        ]
        calls: list[dict] = []
        from_args = fake_from_args({primary: graph_nodes}, calls=calls)

        with mock.patch.object(ProvenanceQuantumGraph, 'from_args',
                               side_effect=from_args):
            result = invoke_cli(['summary', primary, cached])

        assert result.exit_code == 0, f"summary crashed: {result.exception}"
        assert 'Loaded 2' in result.output
        # 2 graph + 2 table rows, shared key collapses -> 3 quanta.
        assert '3 total quanta' in result.output
        # Mixed loads use the neutral "source(s)" wording (design D3).
        assert 'Loaded 2 source(s), 3 total quanta.' in result.output
        assert 'graph source' not in result.output
        assert 'table source' not in result.output
        # The graph positional opened exactly the primary path.
        assert [c['path'] for c in calls] == [primary]

        # Precedence proof: cache the merged result and inspect run_times.
        # The graph (left-most) row at 10 s must shadow the cached 999 s.
        merged = str(tmp_path / 'merged.parquet')
        with mock.patch.object(ProvenanceQuantumGraph, 'from_args',
                               side_effect=from_args):
            result = invoke_cli(
                ['preprocess', '-o', merged, primary, cached])
        assert result.exit_code == 0, f"preprocess crashed: {result.exception}"

        merged_qt = QuantumRuntimeTable.from_parquet(merged)
        run_times = sorted(float(rt) for rt in merged_qt.run_time)
        assert run_times == [10.0, 20.0, 30.0]
        qids = {uuid.UUID(bytes=q).int
                for q in merged_qt.quantum_id_list()}
        assert qids == {1, 2, 4}
        assert 3 not in qids
        assert json.loads(
            merged_qt.to_arrow().schema.metadata[b'sources']
        ) == [primary, cached]

    @pytest.mark.parametrize(("invoke_args", "expected_error"), [
        (lambda cache: ['--repo', 'R', 'summary', cache], 'Cannot mix'),
        (lambda cache: ['preprocess', cache], '--output'),
    ], ids=["table-positional-with-repo", "preprocess-requires-output"])
    def test_usage_errors(self, write_cache, invoke_args,
                          expected_error) -> None:
        """A table positional combined with --repo is rejected, and
        preprocess without --output fails.
        """
        cache = write_cache('run.parquet', [
            ('TaskA', 10.0, "{visit=1}", 1),
        ], sources=("run.qg",))

        result = invoke_cli(invoke_args(cache))
        assert result.exit_code != 0
        assert expected_error in result.output

    @pytest.mark.parametrize(
        ("rows", "args_for", "extra_outputs", "check_fingerprint"), [
            ([('TaskA', 10.0), ('TaskA', 20.0), ('TaskB', 30.0)],
             lambda out: ['--repo', 'R', '-c', 'coll',
                          'preprocess', '-o', out],
             ['Loaded 1 source(s), 3 quanta.', 'Fingerprint:'], True),
            ([('TaskA', 10.0), ('TaskB', 20.0)],
             lambda out: ['--repo', 'R', '-c', 'coll',
                          '--save-intermediate-table', out, 'summary'],
             ['Loaded 1 graph source(s), 2 total quanta.'], False),
        ], ids=["preprocess-output", "save-intermediate-table"])
    def test_graph_load_saves_cache_with_metadata(
        self, make_task_analyzer, tmp_path, rows, args_for, extra_outputs,
        check_fingerprint
    ) -> None:
        """Both save paths (preprocess --output and the group-level
        --save-intermediate-table flag) write the exact merged table plus
        fingerprint/sources metadata.
        """
        from lsst.pipe.base._runtime_analyzer import cli
        from lsst.pipe.base._runtime_analyzer.tracker.database import hash_graph

        analyzer = make_task_analyzer(rows)
        out = str(tmp_path / 'out.parquet')

        with mock.patch.object(cli, 'extract_merged_runtime_table',
                               return_value=analyzer.table):
            result = invoke_cli(args_for(out))

        assert result.exit_code == 0, f"load crashed: {result.exception}"
        assert Path(out).exists()
        assert (f"Saved intermediate table: {out} ({len(rows)} rows)"
                in result.output)
        for extra in extra_outputs:
            assert extra in result.output
        if check_fingerprint:
            assert hash_graph(analyzer) in result.output

        loaded = QuantumRuntimeTable.from_parquet(out)
        assert loaded.to_arrow().equals(analyzer.table.to_arrow())
        meta = loaded.to_arrow().schema.metadata
        assert json.loads(meta[b'sources']) == ['R:coll']
        assert meta[b'fingerprint'].decode() == hash_graph(analyzer)

    def test_save_flag_table_input_copies_through(self, tmp_path,
                                                  write_cache):
        a = write_cache('a.parquet', [
            ('TaskA', 10.0, "{visit=1}", 1),
            ('TaskB', 20.0, "{visit=2}", 2),
        ], sources=("run.qg",))
        b = str(tmp_path / 'b.parquet')

        result = invoke_cli(['--save-intermediate-table', b,
                             'summary', a])

        assert result.exit_code == 0, f"summary crashed: {result.exception}"
        assert Path(b).exists()
        assert f"Saved intermediate table: {b} (2 rows)" in result.output
        assert QuantumRuntimeTable.from_parquet(b).n_rows == 2


@pytest.mark.skipif(not _HAS_CLICK, reason="click not installed")
class TestLoadHeaderWording:
    """Post-load header wording follows the input mix (runtime-cli spec,
    scenario "Cached table as positional argument"): table-only loads say
    "table source(s)", graph/Butler loads keep the exact "graph source(s)"
    wording, mixed lists use the neutral "source(s)".
    """

    def test_table_mode_header(self, tmp_path: Path, write_cache) -> None:
        cache = write_cache('run.parquet', [
            ('TaskA', 10.0, "{visit=1, band='r'}", 1),
            ('TaskA', 20.0, "{visit=2, band='r'}", 2),
            ('TaskB', 30.0, "{visit=1, band='r'}", 3),
        ])

        result = invoke_cli(['summary', cache])
        assert result.exit_code == 0, f"summary crashed: {result.exception}"
        assert 'Loaded 1 table source(s), 3 total quanta.' in result.output
        assert 'graph source' not in result.output

    def test_graph_mode_header_unchanged(self,
                                         make_task_analyzer) -> None:
        """Graph-mode (Butler) header must remain byte-identical."""
        from lsst.pipe.base._runtime_analyzer import cli

        analyzer = make_task_analyzer([('TaskA', 10.0), ('TaskB', 20.0)])
        with mock.patch.object(cli, 'extract_merged_runtime_table',
                               return_value=analyzer.table):
            result = invoke_cli(
                ['--repo', 'R', '-c', 'coll', 'summary'])

        assert result.exit_code == 0, f"summary crashed: {result.exception}"
        assert 'Loaded 1 graph source(s), 2 total quanta.' in result.output

    def test_graph_file_mode_header_unchanged(self) -> None:
        """Positional graph-file loads also keep the exact graph wording."""
        from lsst.pipe.base.quantum_graph import ProvenanceQuantumGraph

        graph_path = str(Path('run.qg'))
        from_args = fake_from_args({
            graph_path: [graph_node(1, 'TaskT', 10.0, "{visit=10}")],
        })

        with mock.patch.object(ProvenanceQuantumGraph, 'from_args',
                               side_effect=from_args):
            result = invoke_cli(['summary', graph_path])

        assert result.exit_code == 0, f"summary crashed: {result.exception}"
        assert 'Loaded 1 graph source(s), 1 total quanta.' in result.output


@pytest.mark.skipif(not _HAS_CLICK, reason="click not installed")
class TestCliExtensionDispatch:
    """CLI-level extension dispatch: unknown extensions load as graphs."""

    def test_unknown_extension_positional_loads_as_graph(self,
                                                         tmp_path: Path) -> None:
        """A .fits-named and an extension-less positional both dispatch to
        the graph loader (the else-branch stays the default).
        """
        from lsst.pipe.base.quantum_graph import ProvenanceQuantumGraph

        fits_path = str(tmp_path / 'run.fits')
        noext_path = str(tmp_path / 'rungraph')
        one_node = [graph_node(1, 'TaskX', 5.0, "{visit=7}")]
        calls: list[dict] = []
        from_args = fake_from_args(
            {fits_path: one_node, noext_path: one_node}, calls=calls)

        with mock.patch.object(ProvenanceQuantumGraph, 'from_args',
                               side_effect=from_args):
            result = invoke_cli(['summary', fits_path, noext_path])

        assert result.exit_code == 0, f"summary crashed: {result.exception}"
        # Both unknown-extension positionals went through the graph loader.
        assert [c["path"] for c in calls] == [fits_path, noext_path]
        assert 'Loaded 2 graph source(s), 1 total quanta.' in result.output
        assert 'table source' not in result.output


# ---- Ordered, interleavable source flags (add-ordered-flag-sources) ----


@pytest.mark.skipif(not _HAS_CLICK, reason="click not installed")
class TestOrderedSourceFlags:
    """Tasks 3.1-3.4: ordered source flags (--graph/-g, --table/-T,
    --repo/-r + --collection/-c) load in command-line encounter order,
    pair-close repo/collection, keep positionals mutually exclusive, and
    merge first-wins across kinds.
    """

    def test_source_flags_load_in_command_line_order(self, tmp_path,
                                                     write_cache,
                                                     cache_table,
                                                     make_task_analyzer):
        """3.1: ``--graph a.qg -r /repo -c coll1 -T t.parquet summary``
        loads graph, butler, table in exactly that order.
        """
        from lsst.pipe.base._runtime_analyzer import cli
        from lsst.pipe.base._runtime_analyzer.runtime_table import QuantumRuntimeTable

        cache = write_cache('t.parquet', [
            ('TaskA', 10.0, "{visit=1}", 1),
        ])

        calls: list = []
        analyzer = make_task_analyzer([('TaskA', 1.0)])

        def record_load(sources):
            calls.append(('graph/butler', tuple(sources)))
            return analyzer.table

        def record_table(path):
            calls.append(('table', str(path)))
            return cache_table([('TaskA', 20.0, "{visit=2}", 2)])

        with mock.patch.object(cli, 'extract_merged_runtime_table',
                               side_effect=record_load), \
                mock.patch.object(QuantumRuntimeTable, 'from_parquet',
                                  side_effect=record_table):
            result = invoke_cli(
                ['--graph', 'a.qg', '-r', '/repo', '-c', 'coll1',
                 '-T', str(cache), 'summary'])

        assert result.exit_code == 0, f"summary crashed: {result.exception}"
        assert calls == [
            ('graph/butler', (('a.qg', None),)),
            ('graph/butler', (('/repo', 'coll1'),)),
            ('table', str(cache)),
        ]
        assert 'Loaded 3 source(s), 2 total quanta.' in result.output

    def test_repo_collection_pair_close(self, make_task_analyzer):
        """3.2: pair-close binds collections to the nearest preceding
        repo, and flags interleave in exact command-line order:
        ``-r R1 -c A --graph g.qg -r R2 -c B`` loads
        [butler R1:A, graph g.qg, butler R2:B] in that order.
        """
        analyzer = make_task_analyzer([('TaskA', 1.0)], n_sources=3)

        with captured_graph_loads(analyzer) as captured:
            result = invoke_cli(
                ['-r', 'R1', '-c', 'A', '--graph', 'g.qg',
                 '-r', 'R2', '-c', 'B', 'summary'])

        assert result.exit_code == 0, f"summary crashed: {result.exception}"
        # Butler + graph share the unified graph/butler reader: one bulk
        # call whose pair list is in exact command-line order (pair-close
        # keeps R1 open across --graph; the second --repo rebinds R2:B).
        assert captured == [
            (('R1', 'A'), ('g.qg', None), ('R2', 'B')),
        ]
        assert 'Loaded 3 graph source(s), 1 total quanta.' in result.output

    @pytest.mark.parametrize(("invoke_args", "expected_error"), [
        (['-c', 'orphan', 'summary'],
         '--collection requires a preceding --repo'),
        (['--graph', 'a.qg', 'summary', 'b.qg'], 'Cannot mix'),
    ], ids=["orphan-collection", "positional-plus-flags"])
    def test_usage_errors(self, invoke_args, expected_error) -> None:
        """3.2/3.3: ``-c orphan`` with no preceding ``--repo`` and mixes
        of positional files with source flags fail at parse time.
        """
        result = invoke_cli(invoke_args)
        assert result.exit_code != 0
        assert expected_error in result.output

    def test_legacy_repo_multi_collections_still_two_butler_sources(
        self, make_task_analyzer
    ):
        """3.2: ``-r R -c A -c B`` loads two butler sources in order
        via the single unified bulk path.
        """
        from lsst.pipe.base._runtime_analyzer import cli

        analyzer = make_task_analyzer([('TaskA', 1.0)], n_sources=2)
        with mock.patch.object(cli, 'extract_merged_runtime_table',
                               return_value=analyzer.table) as m:
            result = invoke_cli(['-r', 'R', '-c', 'collA', '-c', 'collB',
                                 'summary'])

        assert result.exit_code == 0, f"summary crashed: {result.exception}"
        m.assert_called_once_with([('R', 'collA'), ('R', 'collB')])
        assert 'Loaded 2 graph source(s), 1 total quanta.' in result.output

    def test_positional_only_and_flags_only_still_work(self):
        """3.3: graph-only positional with no flags loads through the
        bulk graph path.
        """
        node_map = {
            'a.qg': [graph_node(1, 'TaskA', 5.0, "{visit=1}")],
            'b.qg': [graph_node(2, 'TaskB', 7.0, "{visit=2}")],
        }

        with override_graph_loader(node_map):
            result = invoke_cli(['summary', 'a.qg', 'b.qg'])

        assert result.exit_code == 0, f"summary crashed: {result.exception}"
        assert 'Loaded 2 graph source(s), 2 total quanta.' in result.output

    def test_cross_kind_first_wins_merge_and_neutral_header(self, tmp_path,
                                                            write_cache):
        """3.4: overlapping graph + butler + table sources merge
        first-wins by (task_label, data_id) in flag order; the header
        uses the neutral 'source(s)' wording for the mix.
        """
        # data_ids shared across kinds collide; unique ids add rows.
        graph_rows = [
            graph_node(1, 'TaskX', 10.0, "{visit=1}"),
            graph_node(2, 'TaskX', 20.0, "{visit=2}"),
        ]
        butler_rows = [
            # Same (task_label, data_id) as the graph -> shadowed.
            graph_node(3, 'TaskX', 999.0, "{visit=1}"),
            graph_node(4, 'TaskY', 40.0, "{visit=3}"),
        ]
        # Shared with graph + butler -> last, shadowed.
        cache = write_cache('t.parquet', [
            ('TaskX', 888.0, "{visit=1}", 5),
            ('TaskX', 50.0, "{visit=4}", 6),
        ])

        node_map = {
            'base.qg': graph_rows,
            'R:coll': butler_rows,
        }
        with override_graph_loader(node_map):
            result = invoke_cli(
                ['--graph', 'base.qg', '-r', 'R', '-c', 'coll',
                 '-T', str(cache), 'preprocess', '-o',
                 str(tmp_path / 'merged.parquet')])

        assert result.exit_code == 0, f"preprocess crashed: {result.exception}"
        assert 'Loaded 3 source(s), 4 quanta.' in result.output
        # Mix is table + graph/butler -> neutral wording, never biased.
        assert 'table source' not in result.output
        assert 'graph source' not in result.output

        merged_qt = QuantumRuntimeTable.from_parquet(
            str(tmp_path / 'merged.parquet'))
        run_times = sorted(float(rt) for rt in merged_qt.run_time)
        # 10.0 (graph visit=1) shadows 999.0 (butler) and 888.0 (table).
        assert run_times == [10.0, 20.0, 40.0, 50.0]
        assert json.loads(
            merged_qt.to_arrow().schema.metadata[b'sources']
        ) == ['base.qg', 'R:coll', str(cache)]

    def test_parquet_under_graph_flag_loads_via_graph_reader(
        self, tmp_path, make_task_analyzer, cache_table,
    ):
        """3.7: a .parquet file passed to --graph honors the literal flag
        kind (graph reader), while positional .parquet dispatches to
        the table reader.
        """
        from lsst.pipe.base._runtime_analyzer import cli
        from lsst.pipe.base._runtime_analyzer.runtime_table import QuantumRuntimeTable

        cache = tmp_path / 'as_graph.parquet'
        cache_table([
            ('TaskA', 10.0, "{visit=1}", 1),
        ]).to_parquet(str(cache))

        analyzer = make_task_analyzer([('TaskA', 1.0)])

        # --graph with a .parquet path: extension is irrelevant, the
        # entry reaches the merged-table producer as ('path', None).
        with mock.patch.object(cli, 'extract_merged_runtime_table',
                               return_value=analyzer.table) as la, \
                mock.patch.object(QuantumRuntimeTable, 'from_parquet') as lq:
            result = invoke_cli(['--graph', str(cache), 'summary'])

        assert result.exit_code == 0, f"summary crashed: {result.exception}"
        la.assert_called_once_with([(str(cache), None)])
        lq.assert_not_called()

        # Positional .parquet: extension dispatch picks the table
        # reader.
        with mock.patch.object(cli, 'extract_merged_runtime_table',
                               side_effect=AssertionError(
                                   "graph load attempted for table positional"
                               )) as la, \
                mock.patch.object(cli.QuantumRuntimeTable, 'from_parquets',
                                  return_value=analyzer.table) as ltab:
            result = invoke_cli(['summary', str(cache)])

        assert result.exit_code == 0, f"summary crashed: {result.exception}"
        la.assert_not_called()
        ltab.assert_called_once_with([str(cache)])
