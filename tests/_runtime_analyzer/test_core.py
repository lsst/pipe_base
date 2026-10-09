"""Tests for the core module (data extraction and analysis)."""

from __future__ import annotations

import json
import logging
import unittest.mock as mock
import uuid
from dataclasses import FrozenInstanceError

import numpy as np
import pytest

from lsst.pipe.base import QuantumAttemptStatus

pytest.importorskip("pyarrow")  # provided by the [runtime] extra

from lsst.pipe.base._runtime_analyzer.core import (  # noqa: E402
    QuantumRuntimeAnalyzer,
    _extract_dimension_value,
    _outlier_extremity,
    extract_merged_runtime_table,
    extract_runtime_table,
)
from lsst.pipe.base._runtime_analyzer.dimensions import (  # noqa: E402
    _split_top_level,
    parse_dimension_string,
)
from lsst.pipe.base._runtime_analyzer.runtime_table import (  # noqa: E402
    RUNTIME_SCHEMA,
    STATUS_MAP,
    QuantumRuntimeTable,
    _node_to_row,
)

from .support import (  # noqa: E402
    FakeDataId,
    _assert_table_dicts_identical,
    _assert_table_identical,
    fake_from_args,
    make_analyzer,
    make_qg,
    make_ru,
    make_table,
    merge_node,
    status_rows,
)
from .support import (
    row as _row,
)


class TestAnalyzerConstruction:
    """Analyzer construction: producer extraction, table adoption,
    n_loaded/n_expected bookkeeping.
    """

    def test_basic_init(self) -> None:
        qg = make_qg({'Calibrate': []})
        analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(qg))
        assert analyzer.n_expected == 0

    def test_n_expected(self) -> None:
        qg = make_qg({
            'Calibrate': [{'id': uuid.uuid4(), 'resource_usage': make_ru()}],
            'Coadd': [{'id': uuid.uuid4(), 'resource_usage': make_ru()}] * 3,
        })
        analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(qg))
        assert analyzer.n_expected == 4

    def test_single_quantum(self) -> None:
        ru = make_ru(memory=2e9, prep_time=10.0, init_time=5.0, run_time=100.0, run_time_cpu=80.0)
        qg = make_qg({
            'Calibrate': [{'id': uuid.UUID(int=1), 'resource_usage': ru,
                           'status': 'SUCCESSFUL', 'data_id': 'visit=1,filter=g'}],
        })
        analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(qg))
        qt = analyzer.table
        assert qt.n_rows == 1
        assert qt.labels()[0] == 'Calibrate'
        assert float(qt.memory[0]) == pytest.approx(2e9 / (1024 * 1024))  # MiB
        assert float(qt.run_time[0]) == 100.0

    def test_multiple_quanta(self) -> None:
        quanta = [
            {'id': uuid.UUID(int=i+1), 'resource_usage': make_ru(run_time=i*10.0, run_time_cpu=i*8.0),
             'status': 'SUCCESSFUL'}
            for i in range(10)
        ]
        qg = make_qg({'TaskA': quanta})
        analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(qg))
        assert analyzer.table.n_rows == 10

    def test_excludes_none_resource_usage(self) -> None:
        qg = make_qg({
            'Calibrate': [
                {'id': uuid.UUID(int=1), 'resource_usage': make_ru()},
                {'id': uuid.UUID(int=2), 'resource_usage': None},
            ],
        })
        analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(qg))
        assert analyzer.table.n_rows == 1

    def test_schema(self) -> None:
        from lsst.pipe.base._runtime_analyzer.runtime_table import RUNTIME_SCHEMA
        qg = make_qg({'Calibrate': [{'id': uuid.UUID(int=1), 'resource_usage': make_ru()}]})
        analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(qg))
        assert analyzer.table.to_arrow().schema == RUNTIME_SCHEMA

    def test_single_graph_n_loaded(self) -> None:
        qg = make_qg({
            'T': [{'id': uuid.UUID(int=i), 'resource_usage': make_ru()} for i in range(5)],
        })
        analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(qg))
        assert analyzer.n_loaded == 5

    def test_merged_n_loaded(self, make_analyzer) -> None:
        analyzer = make_analyzer(
            [_row("T", 1.0, data_id="{visit=1, band='g'}", quantum_int=0)],
            n_sources=2,
        )
        assert analyzer.n_loaded == 1
        assert analyzer.n_sources == 2

    def test_n_empty_merged(self, make_analyzer) -> None:
        analyzer = make_analyzer([])
        assert analyzer.n_loaded == 0

    def test_parity_with_analyzer(self) -> None:
        qg = make_qg({
            'Calibrate': [
                {'id': uuid.UUID(int=i),
                 'resource_usage': make_ru(run_time=10.0 + i)}
                for i in range(3)
            ],
            'Coadd': [
                {'id': uuid.UUID(int=10 + i),
                 'resource_usage': make_ru(run_time=100.0 + i)}
                for i in range(2)
            ],
        })
        table = extract_runtime_table(qg)
        analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(qg))

        assert table.to_arrow().schema == RUNTIME_SCHEMA
        assert table.to_arrow().equals(analyzer.table.to_arrow())
        assert table.dim_lookup == analyzer.dim_lookup
        assert table.n_expected == analyzer.n_expected == 5
        assert table.n_sources == 1
        assert table.sources == ()

    def test_extract_excludes_none_resource_usage(self) -> None:
        qg = make_qg({
            'Calibrate': [
                {'id': uuid.UUID(int=1), 'resource_usage': make_ru()},
                {'id': uuid.UUID(int=2), 'resource_usage': None},
            ],
        })
        table = extract_runtime_table(qg)
        # The BLOCKED-style (resource_usage=None) quantum yields no row.
        assert table.n_rows == 1
        assert uuid.UUID(bytes=table.quantum_id_list()[0]).int == 1
        assert set(table.dim_lookup) == {uuid.UUID(int=1).bytes}
        # n_expected comes from the header (both quanta expected).
        assert table.n_expected == 2

    def test_empty_graph(self) -> None:
        qg = make_qg({'T': []})
        table = extract_runtime_table(qg)
        assert table.n_rows == 0
        assert table.to_arrow().schema == RUNTIME_SCHEMA
        assert table.dim_lookup == {}
        assert table.n_expected == 0
        assert table.n_sources == 1
        assert table.sources == ()

    def test_frozen_container_mutable_contents(self) -> None:
        table = make_table(
            [], {}, n_expected=3, n_sources=1, sources=("a.qg",)
        )
        # The container is frozen: fields cannot be reassigned.
        with pytest.raises(FrozenInstanceError):
            table.n_expected = 5
        # ... but the contents are mutable by design.
        table.dim_lookup[b"\x00" * 16] = {"visit": "1"}
        assert table.dim_lookup == {b"\x00" * 16: {"visit": "1"}}

    def test_init_adopts_table_eagerly(self) -> None:
        qg = make_qg({
            'T': [{'id': uuid.UUID(int=1), 'resource_usage': make_ru()}],
        })
        table = extract_runtime_table(qg)
        analyzer = QuantumRuntimeAnalyzer(table)
        # __init__ adopts the table directly: the analyzer holds the
        # table itself, and n_expected is a table bookkeeping field.
        assert analyzer.table is table
        assert analyzer.n_expected == 1
        assert table.n_rows == 1

    def test_ctor_shares_table_dim_lookup(self) -> None:
        table = make_table(
            [], {uuid.UUID(int=1).bytes: {"band": "r"}},
        )
        analyzer = QuantumRuntimeAnalyzer(table)
        assert analyzer.dim_lookup == table.dim_lookup
        # The table is the single source of truth: the analyzer reads
        # through to the table's derived view (no hidden private copy).
        assert analyzer.dim_lookup is table.dim_lookup

    def test_from_parquets_zero_n_expected(
        self, tmp_path,
    ) -> None:
        rows = [
            _row("calibrate", 10.0, data_id="{visit=8001, band='r'}",
                 quantum_int=101),
        ]
        dim_lookup = {uuid.UUID(int=101).bytes: {"visit": "8001", "band": "r"}}
        cache_path = str(tmp_path / "run.parquet")
        cache_qt = make_table(rows, dim_lookup, sources=("run.qg",))
        cache_qt.to_parquet(cache_path)

        analyzer = QuantumRuntimeAnalyzer(
            QuantumRuntimeTable.from_parquets([cache_path])
        )
        # Cache loads never report an expected count (spec D4 matrix).
        assert analyzer.n_expected == 0
        assert analyzer.n_loaded == 1


class TestFilters:
    """Row selection: task/status filtering with the explicit
    status-alias table (CORE-4 spec).
    """

    def test_filter_by_task(self) -> None:
        quanta = [
            {'id': uuid.UUID(int=1), 'resource_usage': make_ru()},
            {'id': uuid.UUID(int=2), 'resource_usage': make_ru()},
            {'id': uuid.UUID(int=3), 'resource_usage': make_ru()},
        ]
        # Use different task labels in mock by adding status field
        for i, q in enumerate(quanta):
            q['status'] = 'SUCCESSFUL' if i != 1 else 'FAILED'
        qg = make_qg({'TaskA': quanta[:2], 'TaskB': quanta[2:]})
        analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(qg))

        # Filtering reads the table backing the analyzer.
        filtered = analyzer._filtered_table(task_label='TaskA')
        assert filtered.n_rows == 2
        assert all(tl == 'TaskA' for tl in filtered.labels())

    def test_filter_by_status(self) -> None:
        quanta = [
            {'id': uuid.UUID(int=i+1), 'resource_usage': make_ru(), 'status': 'SUCCESSFUL'}
            for i in range(2)
        ]
        quanta.append({'id': uuid.UUID(int=100), 'resource_usage': make_ru(), 'status': 'FAILED'})
        qg = make_qg({'TaskA': quanta})
        analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(qg))

        filtered = analyzer._filtered_table(status='FAILED')
        assert filtered.n_rows == 1
        assert int(filtered.status_codes[0]) == STATUS_MAP['FAILED']

    @pytest.mark.parametrize("filt,expected", [
        ("failed", {STATUS_MAP["FAILED"], STATUS_MAP["ABORTED"]}),
        # One alias spelling kept; other alias rows shared a canonical's
        # frozenset.
        ("fail", {STATUS_MAP["FAILED"], STATUS_MAP["ABORTED"]}),
        ("aborted", {STATUS_MAP["ABORTED"]}),
        ("aborted_success", {STATUS_MAP["ABORTED_SUCCESS"]}),
        ("successful", {STATUS_MAP["SUCCESSFUL"]}),
        ("blocked", {STATUS_MAP["BLOCKED"]}),
        ("unknown", {STATUS_MAP["UNKNOWN"]}),
    ], ids=["failed", "fail", "aborted", "aborted-success", "successful",
            "blocked", "unknown"])
    def test_status_alias_table(self, make_analyzer, filt,
                                expected) -> None:
        analyzer = make_analyzer(status_rows())
        filtered = analyzer._filtered_table(status=filt)
        codes = {int(c) for c in filtered.status_codes}
        assert codes == expected

    def test_failed_includes_aborted_not_aborted_success(self, make_analyzer) -> None:
        analyzer = make_analyzer(status_rows())
        codes = {int(c)
                 for c in analyzer._filtered_table(status="failed").status_codes}
        assert STATUS_MAP["ABORTED_SUCCESS"] not in codes

    def test_unrecognized_filter_empty_and_warns(self, caplog, make_analyzer) -> None:
        analyzer = make_analyzer(status_rows())
        with caplog.at_level(logging.WARNING, logger="lsst.pipe.base._runtime_analyzer.core"):
            filtered = analyzer._filtered_table(
                status="definitely_not_a_status"
            )
        assert filtered.n_rows == 0
        assert any("Unknown status filter" in rec.message for rec in caplog.records)


class TestSummary:
    """Per-task summary table aggregation."""

    def test_summary_basic(self) -> None:
        quanta = []
        for i in range(5):
            quanta.append({
                'id': uuid.UUID(int=i+1),
                'resource_usage': make_ru(run_time=100.0 + i*10, run_time_cpu=80.0 + i*8),
            })
        qg = make_qg({'Calibrate': quanta})
        analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(qg))
        result = analyzer.summary()
        assert len(result) == 1
        assert result['Task'][0] == 'Calibrate'
        assert result['quanta'][0] == 5
        assert 'mean_rt' in result.colnames

    def test_summary_multiple_tasks(self) -> None:
        qg = make_qg({
            'Calibrate': [
                {'id': uuid.UUID(int=i+1),
                 'resource_usage': make_ru(run_time=50.0+i*5)}
                for i in range(3)
            ],
            'Coadd': [
                {'id': uuid.UUID(int=i+10),
                 'resource_usage': make_ru(run_time=200.0+i*10)}
                for i in range(2)
            ],
        })
        analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(qg))
        result = analyzer.summary()
        assert len(result) == 2
        tasks = set(result['Task'])
        assert 'Calibrate' in tasks
        assert 'Coadd' in tasks


class TestTopQuantities:
    """top_quantities ranking, metric validation, guards and the
    pct_of_task_mean metric alignment (CORE-3).
    """

    def test_top_n(self) -> None:
        quanta = []
        for i in range(10):
            quanta.append({
                'id': uuid.UUID(int=i+1),
                'resource_usage': make_ru(run_time=100.0 * (i+1)),
            })
        qg = make_qg({'TaskA': quanta})
        analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(qg))
        result = analyzer.top_quantities(n=5)
        assert len(result) == 5
        assert result['rank'][0] == 1
        # The ranked run_time column must be monotonically non-increasing
        # and start at the global maximum (100.0 * 10 = 1000.0).
        rts = [float(rt) for rt in result['run_time']]
        assert rts[0] == pytest.approx(1000.0)
        assert all(a >= b for a, b in zip(rts, rts[1:]))
        assert rts == [1000.0, 900.0, 800.0, 700.0, 600.0]

    def test_invalid_metric_raises_value_error(self, make_analyzer) -> None:
        rows = [_row("TaskA", 10.0, memory=100.0, quantum_int=1),
                _row("TaskA", 20.0, memory=200.0, quantum_int=2)]
        analyzer = make_analyzer(rows)

        # A numeric schema field that is *not* supported must raise
        # ValueError (not silently rank by run_time or compute task means).
        with pytest.raises(ValueError, match="Unsupported metric"):
            analyzer.top_quantities(metric="prep_time")
        # An entirely bogus name behaves the same.
        with pytest.raises(ValueError, match="Unsupported metric"):
            analyzer.top_quantities(metric="bogus_metric")
        # Supported metrics work.
        assert len(analyzer.top_quantities(metric="run_time", n=2)) == 2
        assert len(analyzer.top_quantities(metric="memory", n=2)) == 2

    def test_pct_of_task_mean_zero_mean_guard(self, make_analyzer) -> None:
        """A zero task mean yields pct_of_task_mean 0.0, never inf/nan."""
        rows = [_row("TaskZero", 0.0, memory=0.0, quantum_int=i)
                for i in range(1, 4)]
        analyzer = make_analyzer(rows)

        for metric in ("run_time", "memory"):
            table = analyzer.top_quantities(metric=metric, n=3)
            assert len(table) == 3
            for row in table:
                pct = float(row["pct_of_task_mean"])
                assert pct == 0.0
                assert np.isfinite(pct)

    def test_pct_of_task_mean_uses_metric_mean(self, make_analyzer) -> None:
        rows = [
            _row("TaskA", 10.0, memory=100.0, quantum_int=1),
            _row("TaskA", 20.0, memory=200.0, quantum_int=2),
            _row("TaskB", 999.0, memory=50.0, quantum_int=3),
            _row("TaskB", 1.0, memory=150.0, quantum_int=4),
        ]
        analyzer = make_analyzer(rows)

        table = analyzer.top_quantities(metric="memory", n=4)
        task_mem_means = {"TaskA": 150.0, "TaskB": 100.0}
        for row in table:
            tl = str(row["task_label"])
            expected = float(row["memory"]) / task_mem_means[tl] * 100.0
            assert float(row["pct_of_task_mean"]) == pytest.approx(expected)

        # Spot-check concrete values: 200/150*100 and 150/100*100.
        by_key = {(str(r["task_label"]), float(r["memory"])): float(r["pct_of_task_mean"])
                  for r in table}
        assert by_key[("TaskA", 200.0)] == pytest.approx(133.3333, rel=1e-4)
        assert by_key[("TaskB", 150.0)] == pytest.approx(150.0, rel=1e-4)


class TestDimensionDist:
    """dimension_dist grouping plus the dimension-string parser
    stack (extract/parse/split) shared with plots.
    """

    def test_dimension_dist_basic(self) -> None:
        quanta = [
            {'id': uuid.UUID(int=1), 'resource_usage': make_ru(run_time=10), 'data_id': 'visit=1,filter=g'},
            {'id': uuid.UUID(int=2), 'resource_usage': make_ru(run_time=20), 'data_id': 'visit=1,filter=g'},
            {'id': uuid.UUID(int=3), 'resource_usage': make_ru(run_time=30), 'data_id': 'visit=2,filter=g'},
        ]
        qg = make_qg({'Calibrate': quanta})
        analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(qg))
        result = analyzer.dimension_dist('visit')
        assert 'Calibrate' in result
        assert len(result['Calibrate']) == 2  # visit=1 and visit=2

    def test_dimension_dist_zero_total_guard(self, make_analyzer) -> None:
        """Zero total run_time gives pct 0.0, never inf/nan."""
        rows = [
            _row("Calibrate", 0.0, data_id="{visit=1, band='g'}", quantum_int=1),
            _row("Calibrate", 0.0, data_id="{visit=2, band='g'}", quantum_int=2),
        ]
        analyzer = make_analyzer(rows)
        result = analyzer.dimension_dist('visit')
        assert 'Calibrate' in result
        for row in result['Calibrate']:
            pct = float(row['pct_of_total_run_time'])
            assert pct == 0.0
            assert np.isfinite(pct)

    @pytest.mark.parametrize(("data_id", "dimension", "expected"), [
        ('visit=1,filter=g', 'visit', '1'),
        ('filter=g,pixel=1', 'visit', 'unknown'),
        ('tract=123,patch=a3', 'tract', '123'),
        ("{visit=522, band='r'}", 'visit', '522'),
        ("{visit=522, band='r'}", 'band', 'r'),
        ("{visit=522, band='r'}", 'tract', 'unknown'),
    ], ids=["legacy-simple", "legacy-missing", "legacy-special-chars",
            "brace-visit", "brace-band", "brace-missing-dim"])
    def test_extract_dimension_value_formats(self, data_id, dimension,
                                             expected) -> None:
        """The robust parser extracts dimension values from the legacy
        comma format *and* the real str(DataCoordinate) brace format
        (stripping braces/quotes); a missing dimension yields 'unknown'.
        """
        assert _extract_dimension_value(data_id, dimension) == expected

    @pytest.mark.parametrize(("dim_str", "expected"), [
        # Colon form is the REAL str(DataCoordinate) format.
        ("{visit=1, band='g'}", {"visit": "1", "band": "g"}),
        ("visit=1,filter=g", {"visit": "1", "filter": "g"}),
        ("", {}),
        ("{instrument: 'INSTR', visit: 8001}",
         {"instrument": "INSTR", "visit": "8001"}),
        # Sequence values canonicalize to "1,2" so the string-fallback
        # path yields the SAME group key as the captured mapping path.
        ("{detector: [1, 2], visit: 1}", {"detector": "1,2", "visit": "1"}),
        ("{band='a,b', visit=1}", {"band": "a,b", "visit": "1"}),
    ], ids=["equals-brace", "legacy-comma", "empty", "colon-format",
            "sequence-normalized", "quoted-comma"])
    def test_parse_dimension_string(self, dim_str, expected) -> None:
        """parse_dimension_string handles brace/legacy/colon formats,
        normalizes sequence values, keeps quoted commas intact, and maps
        empty input to an empty dict.
        """
        assert parse_dimension_string(dim_str) == expected

    @pytest.mark.parametrize(("dim_str", "expected"), [
        ("visit=1, band='g'", ["visit=1", "band='g'"]),
        # Commas inside brackets stay in one part.
        ("detector=[1,2,3], band='r'", ["detector=[1,2,3]", "band='r'"]),
        # Quoted values containing a comma must stay in one part.
        ("band='a,b', visit=1", ["band='a,b'", "visit=1"]),
    ], ids=["plain", "bracket-commas", "single-quoted-comma"])
    def test_split_top_level(self, dim_str, expected) -> None:
        """_split_top_level must respect brackets *and* quotes."""
        assert _split_top_level(dim_str) == expected

    def test_format_dimension_value_ndarray(self) -> None:
        from lsst.pipe.base._runtime_analyzer.runtime_table import (
            _format_dimension_value,
        )

        assert _format_dimension_value(np.array([3, 1, 2])) == "1,2,3"

    def test_dimension_dist_real_format_parsed(self, make_analyzer) -> None:
        # No captured dim_lookup: the robust string parser must do the work.
        rows = [
            _row("Calibrate", 10.0, data_id="{visit=1, band='g'}", quantum_int=1),
            _row("Calibrate", 20.0, data_id="{visit=2, band='r'}", quantum_int=2),
            _row("Calibrate", 30.0, data_id="{visit=1, band='r'}", quantum_int=3),
        ]
        analyzer = make_analyzer(rows)

        visit = analyzer.dimension_dist("visit")
        assert "Calibrate" in visit
        groups = {str(r["group_key"]): int(r["quanta"]) for r in visit["Calibrate"]}
        assert groups == {"1": 2, "2": 1}

        band = analyzer.dimension_dist("band")
        band_groups = {str(r["group_key"]): int(r["quanta"]) for r in band["Calibrate"]}
        assert band_groups == {"g": 1, "r": 2}

    def test_dimension_dist_captured_values(self) -> None:
        # Dimension values captured from a real DataCoordinate-like object
        # must be used even when the stored display string is unparseable.
        class _Coord:
            def __init__(self, dims):
                self._dims = dict(dims)

            def items(self):
                return self._dims.items()

            def __str__(self):
                return "UNPARSEABLE_COORD"

        specs = [
            {"visit": 1, "band": "g"},
            {"visit": 2, "band": "r"},
            {"visit": 1, "band": "r"},
        ]
        quanta = [
            {
                "id": uuid.UUID(int=200 + i),
                "resource_usage": make_ru(
                    memory=1e9, prep_time=1.0, init_time=1.0,
                    run_time=10.0 + i, run_time_cpu=5.0,
                ),
                "data_id": _Coord(dims),
            }
            for i, dims in enumerate(specs)
        ]
        qg = make_qg({"Calibrate": quanta})

        analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(qg))
        # Confirm the extraction captured dims but the display strings are
        # intentionally unparseable.
        assert analyzer.table.data_id_list() == ["UNPARSEABLE_COORD"] * 3

        band = analyzer.dimension_dist("band")
        band_groups = {str(r["group_key"]): int(r["quanta"]) for r in band["Calibrate"]}
        assert band_groups == {"g": 1, "r": 2}
        assert "unknown" not in band_groups

    def test_dimension_dist_has_dim_missing_excluded(self, make_analyzer) -> None:
        rows = [
            _row("Calibrate", 10.0, data_id="{visit=1, band='g'}", quantum_int=1),
            _row("Calibrate", 20.0, data_id="{visit=2, band='r'}", quantum_int=2),
        ]
        analyzer = make_analyzer(rows)
        # No quantum has the 'tract' dimension -> task excluded -> empty.
        assert analyzer.dimension_dist("tract") == {}


class TestBottleneck:
    """bottleneck(): task-type classification and outlier
    identification/indexing (CORE-1, CORE-2, CORE-6).
    """

    def test_bottleneck_basic(self) -> None:
        quanta = [
            {'id': uuid.UUID(int=i+1),
             'resource_usage': make_ru(
                 run_time=100, run_time_cpu=20, prep_time=50,
                 init_time=50)}
            for i in range(5)
        ]
        qg = make_qg({'TaskA': quanta})
        analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(qg))
        result = analyzer.bottleneck()
        assert 'task_table' in result
        assert 'outlier_table' in result
        assert len(result['task_table']) == 1

    def test_bottleneck_io_bound(self) -> None:
        # Create a task with low CPU efficiency (I/O-bound) and high run_pct
        quanta = []
        # Task1: I/O bound - low cpu efficiency, high run_pct
        for i in range(3):
            quanta.append({
                'id': uuid.UUID(int=i+1),
                'resource_usage': make_ru(run_time=100, run_time_cpu=10, prep_time=1, init_time=1),
            })
        # Task2: normal
        for i in range(3):
            quanta.append({
                'id': uuid.UUID(int=i+10),
                'resource_usage': make_ru(run_time=50, run_time_cpu=45, prep_time=2, init_time=2),
            })
        qg = make_qg({'Task1': quanta[:3], 'Task2': quanta[3:]})
        analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(qg))
        result = analyzer.bottleneck()
        task_table = result['task_table']
        task_types = {row['Task']: row['bottleneck_type'] for row in task_table}
        assert task_types.get('Task1') == 'I/O_BOUND'

    def test_interleaved_outlier_reports_correct_row(self, make_analyzer) -> None:
        # Two tasks interleaved; the single outlier belongs to TaskB which
        # appears *later* in the array.  The reported row must be that
        # TaskB outlier (global index 1).
        layout = [
            ("TaskA", 10.0), ("TaskB", 5000.0), ("TaskA", 11.0),
            ("TaskB", 10.0), ("TaskA", 12.0), ("TaskB", 11.0),
            ("TaskA", 13.0), ("TaskB", 12.0), ("TaskA", 14.0),
            ("TaskB", 13.0),
        ]
        rows = []
        for gi, (tl, rt) in enumerate(layout):
            band = "r" if tl == "TaskB" else "g"
            rows.append(_row(
                tl, rt,
                status="SUCCESSFUL",
                data_id=f"{{visit={gi}, band='{band}'}}",
                quantum_int=1000 + gi,
            ))
        analyzer = make_analyzer(rows)

        outlier_table = analyzer.bottleneck(method="iqr")["outlier_table"]
        assert len(outlier_table) == 1
        row = outlier_table[0]
        assert str(row["Task"]) == "TaskB"
        assert float(row["run_time"]) == pytest.approx(5000.0)
        # The outlier is global array index 1 (quantum_int 1001); the buggy
        # implementation reported quantum_int 1000 (global index 0, a TaskA).
        assert str(row["quantum_id"]) == str(uuid.UUID(int=1001))
        assert str(row["data_id"]) == "{visit=1, band='r'}"

    def test_outliers_sorted_by_magnitude_descending(self, make_analyzer) -> None:
        # A-CORE-1 potency: the alphabetically-later task (TaskY) holds the
        # *larger* outlier, and it is also inserted last.  Insertion order
        # therefore contradicts the desired descending order: without the
        # magnitude-descending sort the test fails.
        layout = [
            ("TaskX", 10.0), ("TaskY", 10.0), ("TaskX", 11.0),
            ("TaskY", 11.0), ("TaskX", 12.0), ("TaskY", 12.0),
            ("TaskX", 13.0), ("TaskY", 13.0), ("TaskX", 500.0),
            ("TaskY", 1000.0),
        ]
        rows = [_row(tl, rt, quantum_int=500 + gi) for gi, (tl, rt) in enumerate(layout)]
        analyzer = make_analyzer(rows)

        outlier_table = analyzer.bottleneck(method="iqr")["outlier_table"]
        mags = [float(m) for m in outlier_table["outlier_magnitude"]]
        assert len(outlier_table) == 2
        assert mags == sorted(mags, reverse=True)
        # Insertion order was TaskX(500) then TaskY(1000); the sort must
        # have swapped them: largest magnitude first is TaskY 1000/median(12).
        assert str(outlier_table[0]["Task"]) == "TaskY"
        assert float(outlier_table[0]["run_time"]) == pytest.approx(1000.0)
        assert str(outlier_table[1]["Task"]) == "TaskX"
        assert float(outlier_table[1]["run_time"]) == pytest.approx(500.0)
        # Row identity follows the sort, not insertion order.
        assert uuid.UUID(str(outlier_table[0]["quantum_id"])).int == 509
        assert uuid.UUID(str(outlier_table[1]["quantum_id"])).int == 508

    def test_outliers_top_n_survivor(self, make_analyzer) -> None:
        # A-CORE-1: with top_n=1 the survivor must be the largest-magnitude
        # outlier (TaskY 1000), which was inserted *last*.
        layout = [
            ("TaskX", 10.0), ("TaskY", 10.0), ("TaskX", 11.0),
            ("TaskY", 11.0), ("TaskX", 12.0), ("TaskY", 12.0),
            ("TaskX", 13.0), ("TaskY", 13.0), ("TaskX", 500.0),
            ("TaskY", 1000.0),
        ]
        rows = [_row(tl, rt, quantum_int=500 + gi) for gi, (tl, rt) in enumerate(layout)]
        analyzer = make_analyzer(rows)

        outlier_table = analyzer.bottleneck(method="iqr", top_n=1)["outlier_table"]
        assert len(outlier_table) == 1
        assert str(outlier_table[0]["Task"]) == "TaskY"
        assert float(outlier_table[0]["run_time"]) == pytest.approx(1000.0)
        assert uuid.UUID(str(outlier_table[0]["quantum_id"])).int == 509

    def test_zscore_direction_labels(self, make_analyzer) -> None:
        """CORE-6: z-score outliers below the task median are labelled FAST."""
        rows = []
        for _ in range(50):
            rows.append(_row("TaskZ", 100.0, quantum_int=len(rows) + 1))
        for _ in range(2):
            rows.append(_row("TaskZ", 130.0, quantum_int=len(rows) + 1))
        for _ in range(2):
            rows.append(_row("TaskZ", 70.0, quantum_int=len(rows) + 1))
        analyzer = make_analyzer(rows)
        median_rt = float(np.median(analyzer.table.run_time))
        outlier_table = analyzer.bottleneck(method="zscore", top_n=50)["outlier_table"]
        assert len(outlier_table) >= 2
        reasons = [str(r["outlier_reason"]) for r in outlier_table]
        assert "FAST" in reasons
        assert "SLOW" in reasons
        # Direction invariant: reason is SLOW iff run_time >= median.
        for row in outlier_table:
            expected = "SLOW" if float(row["run_time"]) >= median_rt else "FAST"
            assert str(row["outlier_reason"]) == expected

    def test_zscore_extreme_fast_survives_truncation(self, make_analyzer) -> None:
        """A-CORE-2: extreme FAST outliers (magnitude < 1) must not be
        buried below SLOW outliers and dropped by [:top_n].

        TaskA has a SLOW outlier at 2.5x its median (extremity 2.5);
        TaskB has a FAST outlier at 0.1x its median (extremity 10).  With
        top_n=1 the FAST row must survive, and the raw magnitude column
        must report the un-transformed ratio 0.1.
        """
        rows = []
        # TaskA: 40 normal rows + one SLOW outlier (z ~ 6.3, 2.5x median).
        for _ in range(40):
            rows.append(_row("TaskA", 100.0, quantum_int=len(rows) + 1))
        rows.append(_row("TaskA", 250.0, quantum_int=len(rows) + 1))
        # TaskB: 40 normal rows + one FAST outlier (z ~ -6.3, 0.1x median).
        for _ in range(40):
            rows.append(_row("TaskB", 100.0, quantum_int=len(rows) + 1))
        rows.append(_row("TaskB", 10.0, quantum_int=len(rows) + 1))
        analyzer = make_analyzer(rows)

        # Sanity: both deviant rows are z-score outliers.
        labels = np.array(analyzer.table.labels())
        for tl in ("TaskA", "TaskB"):
            sub = analyzer.table.run_time[labels == tl].astype(np.float64)
            mean, std = float(np.mean(sub)), float(np.std(sub))
            deviant = float(sub[-1])
            assert abs((deviant - mean) / std) > 2.0

        # top_n=1: the more extreme FAST row wins despite magnitude 0.1.
        table1 = analyzer.bottleneck(method="zscore", top_n=1)["outlier_table"]
        assert len(table1) == 1
        assert str(table1[0]["Task"]) == "TaskB"
        assert str(table1[0]["outlier_reason"]) == "FAST"
        assert float(table1[0]["run_time"]) == pytest.approx(10.0)
        # Raw magnitude keeps the raw ratio (run_time / median = 10/100).
        assert float(table1[0]["outlier_magnitude"]) == pytest.approx(0.1)

        # top_n=2: both outliers present, FAST first (extremity 10 > 2.5).
        table2 = analyzer.bottleneck(method="zscore", top_n=2)["outlier_table"]
        assert len(table2) == 2
        assert [str(r["Task"]) for r in table2] == ["TaskB", "TaskA"]
        mags = [float(r["outlier_magnitude"]) for r in table2]
        assert mags[0] == pytest.approx(0.1)
        assert mags[1] == pytest.approx(2.5)
        extremities = [_outlier_extremity(m, 100.0) for m in mags]
        assert extremities == sorted(extremities, reverse=True)

    def test_outlier_extremity_helpers(self) -> None:
        """Symmetric extremity key for the bottleneck sort."""
        assert _outlier_extremity(10.0, 100.0) == pytest.approx(10.0)
        assert _outlier_extremity(0.1, 100.0) == pytest.approx(10.0)
        assert _outlier_extremity(1.0, 100.0) == pytest.approx(1.0)
        assert _outlier_extremity(2.5, 100.0) == pytest.approx(2.5)
        # Zero run_time against a positive median: maximally extreme FAST.
        assert _outlier_extremity(0.0, 100.0) == float("inf")
        # No positive median (or negative magnitude): no information.
        assert _outlier_extremity(0.0, 0.0) == 0.0
        assert _outlier_extremity(-1.0, 100.0) == 0.0

    def test_iqr_reason_is_always_slow(self, make_analyzer) -> None:
        layout = [
            ("TaskA", 10.0), ("TaskA", 11.0), ("TaskA", 12.0),
            ("TaskA", 13.0), ("TaskA", 1000.0),
        ]
        rows = [_row(tl, rt, quantum_int=gi + 1) for gi, (tl, rt) in enumerate(layout)]
        analyzer = make_analyzer(rows)
        outlier_table = analyzer.bottleneck(method="iqr")["outlier_table"]
        assert len(outlier_table) == 1
        assert str(outlier_table[0]["outlier_reason"]) == "SLOW"


class TestTableProducers:
    """The row/record producers behind QuantumRuntimeTable:
    node->row mapping (status codes), from_rows, schema types,
    Arrow accessors, Parquet serialization, module identity.
    """

    def test_empty_rows(self) -> None:
        assert QuantumRuntimeTable.from_rows([]).n_rows == 0

    def test_populated_rows(self) -> None:
        row = ('TaskA', b'\x00' * 16, 1, 100.0, 1.0, 1.0, 10.0, 8.0, 'did')
        qt = QuantumRuntimeTable.from_rows([row])
        assert qt.n_rows == 1
        assert qt.task_labels == ('TaskA',)

    def test_dims_embedded(self) -> None:
        row = ('TaskA', b'\x00' * 16, 1, 100.0, 1.0, 1.0, 10.0, 8.0, 'did')
        qt = QuantumRuntimeTable.from_rows(
            [row], {b'\x00' * 16: {'visit': '1'}},
        )
        assert qt.dim_lookup[b'\x00' * 16] == {'visit': '1'}

    def test_valid_node(self) -> None:
        qid = uuid.UUID(int=42)
        node_data = {
            'task_label': 'Calibrate',
            'status': 'SUCCESSFUL',
            'data_id': 'visit=1,filter=g',
            'resource_usage': make_ru(run_time=100.0),
        }
        row = _node_to_row(qid, node_data)
        assert row is not None
        assert row[0] == 'Calibrate'
        assert row[6] == 100.0

    def test_none_resource_usage_returns_none(self) -> None:
        qid = uuid.UUID(int=42)
        node_data = {
            'task_label': 'Calibrate',
            'status': 'BLOCKED',
            'data_id': 'visit=1,filter=g',
            'resource_usage': None,
        }
        assert _node_to_row(qid, node_data) is None

    @pytest.mark.parametrize(("status", "expected"), [
        (None, STATUS_MAP["UNKNOWN"]),
        ("ABORTED", STATUS_MAP["ABORTED"]),
        # Raw ints map via INT_TO_STATUS; unmapped ints fall back to
        # UNKNOWN (never FAILED).
        (1, STATUS_MAP["SUCCESSFUL"]),
        (0, STATUS_MAP["BLOCKED"]),
        (-1, STATUS_MAP["FAILED"]),
        (99, STATUS_MAP["UNKNOWN"]),
    ], ids=["none-status", "known-string",
            "raw-int-successful", "raw-int-blocked", "raw-int-failed",
            "raw-int-unmapped"])
    def test_node_to_row_status_codes(self, status, expected) -> None:
        """CORE-4: _node_to_row maps status names and raw ints through the
        inverse table; unmapped / None status maps to UNKNOWN, not FAILED.
        """
        row = _node_to_row(uuid.UUID(int=1), {
            "task_label": "T", "status": status,
            "data_id": "{band='g'}", "resource_usage": make_ru(),
        })
        assert row is not None
        assert row[2] == expected

    def test_enum_status_aborted_maps_via_name(self) -> None:
        """A real lsst.pipe.base.QuantumAttemptStatus enum must map by name."""
        assert int(QuantumAttemptStatus.ABORTED.value) == -4  # sanity
        ru = make_ru()
        row = _node_to_row(uuid.UUID(int=1), {
            "task_label": "T", "status": QuantumAttemptStatus.ABORTED,
            "data_id": "{band='g'}", "resource_usage": ru,
        })
        assert row is not None
        assert row[2] == -4
        assert row[2] == STATUS_MAP["ABORTED"]

    def test_enum_status_through_extraction(self) -> None:
        """QuantumAttemptStatus enums survive extraction and filtering."""
        qg = make_qg({
            "TaskE": [
                {"id": uuid.UUID(int=1),
                 "resource_usage": make_ru(),
                 "status": QuantumAttemptStatus.ABORTED},
                {"id": uuid.UUID(int=2),
                 "resource_usage": make_ru(),
                 "status": QuantumAttemptStatus.SUCCESSFUL},
            ],
        })
        analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(qg))
        qt = analyzer.table
        assert int(qt.status_codes[0]) == -4
        assert int(qt.status_codes[1]) == 1
        aborted = analyzer._filtered_table(status="aborted")
        assert aborted.n_rows == 1
        assert uuid.UUID(bytes=aborted.quantum_id_list()[0]).int == 1

    def test_string_field_types(self) -> None:
        import pyarrow as pa
        assert (RUNTIME_SCHEMA.field("task_label").type
                == pa.dictionary(pa.int32(), pa.string()))
        assert RUNTIME_SCHEMA.field("data_id").type == pa.string()

    def test_numeric_types_unchanged(self) -> None:
        # tracker/plot depend on these numeric field types.
        import pyarrow as pa
        assert RUNTIME_SCHEMA.field("status").type == pa.int8()
        assert RUNTIME_SCHEMA.field("memory").type == pa.float32()
        assert RUNTIME_SCHEMA.field("run_time").type == pa.float32()
        assert RUNTIME_SCHEMA.field("quantum_id").type == pa.binary(16)

    def test_long_task_label_not_truncated(self) -> None:
        # Arrow string columns have no fixed width: long dotted pipeline
        # task module paths (and long data IDs) are stored intact.
        long_label = "lsst.pipe.tasks." + "a" * 300
        row = _row(long_label, 10.0, quantum_int=1)
        qt = QuantumRuntimeTable.from_rows([row])
        assert qt.labels()[0] == long_label

    @staticmethod
    def _arrow_table() -> QuantumRuntimeTable:
        data = [
            (np.str_("TaskB"), uuid.UUID(int=1).bytes, 1,
             10.0, 1.0, 1.0, 100.0, 50.0, np.str_("{visit=10}")),
            (np.str_("TaskA"), uuid.UUID(int=2).bytes, 1,
             20.0, 1.0, 1.0, 200.0, 60.0, np.str_("{visit=20}")),
            (np.str_("TaskB"), uuid.UUID(int=3).bytes, 3,
             30.0, 1.0, 1.0, 300.0, 70.0, np.str_("{visit=30}")),
        ]
        dims = {
            uuid.UUID(int=1).bytes: {"visit": "10", "band": "r"},
            uuid.UUID(int=3).bytes: {"visit": "20", "band": "g"},
        }
        return QuantumRuntimeTable.from_rows(data, dims)

    def test_numeric_accessors(self) -> None:
        qt = self._arrow_table()
        assert qt.n_rows == 3
        assert np.array_equal(qt.run_time, np.float32([100, 200, 300]))
        assert np.array_equal(qt.memory, np.float32([10, 20, 30]))
        assert np.array_equal(qt.status_codes, np.int8([1, 1, 3]))
        assert np.array_equal(qt.numeric("run_time_cpu"),
                              np.float32([50, 60, 70]))

    def test_task_label_dictionary_order(self) -> None:
        qt = self._arrow_table()
        # Dictionary values are byte-sorted (np.unique) and codes index them.
        assert qt.task_labels == ("TaskA", "TaskB")
        assert np.array_equal(qt.task_label_codes, np.int32([1, 0, 1]))
        assert list(qt.labels()) == ["TaskB", "TaskA", "TaskB"]

    def test_quantum_id_accessors(self) -> None:
        qt = self._arrow_table()
        matrix = qt.quantum_id_matrix
        assert matrix.shape == (3, 16)
        assert matrix.dtype == np.uint8
        assert bytes(matrix[1].tobytes()) == uuid.UUID(int=2).bytes
        assert qt.quantum_id_list() == [
            uuid.UUID(int=1).bytes, uuid.UUID(int=2).bytes,
            uuid.UUID(int=3).bytes,
        ]

    def test_select_mask_and_indices(self) -> None:
        qt = self._arrow_table()
        for picked in ([True, False, True], [0, 2]):
            sub = qt.select(picked)
            assert sub.n_rows == 2
            assert np.array_equal(sub.run_time, np.float32([100, 300]))
            # Dictionary encoding survives selection.
            assert np.array_equal(sub.task_label_codes, np.int32([1, 1]))

    def test_single_chunk_invariant(self) -> None:
        qt = self._arrow_table()
        merged = QuantumRuntimeTable.merge_first_wins(qt, self._arrow_table())
        for table in (qt.to_arrow(), qt.select([0]).to_arrow(),
                      merged.to_arrow()):
            assert all(col.num_chunks == 1 for col in table.itercolumns())

    def test_merge_first_wins_disjoint_and_shadowed(self) -> None:
        qt = self._arrow_table()
        other_rows = [
            (np.str_("TaskA"), uuid.UUID(int=2).bytes, 1,
             99.0, 1.0, 1.0, 999.0, 99.0, np.str_("{visit=20}")),
            (np.str_("TaskC"), uuid.UUID(int=4).bytes, 1,
             40.0, 1.0, 1.0, 400.0, 40.0, np.str_("{visit=40}")),
        ]
        other = QuantumRuntimeTable.from_rows(
            other_rows,
            {uuid.UUID(int=4).bytes: {"visit": "9"}},
        )
        merged = QuantumRuntimeTable.merge_first_wins(qt, other)
        assert merged.n_rows == 4
        # First-seen (table) order; the shadowed TaskA row from `other` is
        # dropped and the winner keeps source 1's run_time and dims.
        assert list(merged.labels()) == ["TaskB", "TaskA", "TaskB", "TaskC"]
        assert np.array_equal(merged.run_time,
                              np.float32([100, 200, 300, 400]))
        assert uuid.UUID(int=2).bytes not in merged.dim_lookup
        assert merged.dim_lookup[uuid.UUID(int=4).bytes] == {"visit": "9"}
        assert merged.dim_lookup[uuid.UUID(int=1).bytes] == {
            "visit": "10", "band": "r",
        }

    @staticmethod
    def _parquet_table() -> QuantumRuntimeTable:
        rows = [
            _row("calibrate", 10.0, data_id="{visit=8001, band='r'}",
                 quantum_int=101),
            _row("coadd", 50.0, data_id="{band='g'}", quantum_int=201),
        ]
        dim_lookup = {
            uuid.UUID(int=101).bytes: {"visit": "8001", "band": "r"},
            uuid.UUID(int=201).bytes: {"band": "g"},
        }
        return make_table(
            rows, dim_lookup, n_expected=3, n_sources=1, sources=("a.qg",)
        )

    def test_to_parquet_defaults_and_round_trip(self, tmp_path) -> None:
        table = self._parquet_table()
        dest = str(tmp_path / "t.parquet")
        assert table.to_parquet(dest, fingerprint="fp-1") == 2

        back = QuantumRuntimeTable.from_parquet(dest)
        assert back.to_arrow().equals(table.to_arrow())
        assert back.dim_lookup == table.dim_lookup
        # Parquet caches store rows only: n_expected resets to 0.
        assert back.n_expected == 0
        assert back.n_sources == 1
        assert back.sources == (dest,)

        # Metadata sources default to the table's own sources tuple.
        meta = QuantumRuntimeTable.from_parquet(dest).to_arrow().schema.metadata
        assert json.loads(meta[b"sources"].decode()) == ["a.qg"]
        assert meta[b"fingerprint"].decode() == "fp-1"

    def test_to_parquet_sources_override(self, tmp_path) -> None:
        table = self._parquet_table()
        dest = str(tmp_path / "t.parquet")
        table.to_parquet(dest, sources=("repo:run", "extra.qg"))

        meta = QuantumRuntimeTable.from_parquet(dest).to_arrow().schema.metadata
        assert json.loads(meta[b"sources"].decode()) == [
            "repo:run", "extra.qg",
        ]

    def test_as_table_round_trips_through_analyzer(self, tmp_path) -> None:
        table = self._parquet_table()
        analyzer = QuantumRuntimeAnalyzer(table)
        snapshot = analyzer.as_table()

        # The frozen backing table IS the snapshot (single source of truth).
        assert snapshot is table
        assert snapshot.n_expected == table.n_expected == 3
        assert snapshot.n_sources == 1
        assert snapshot.sources == ("a.qg",)

        dest = str(tmp_path / "rt.parquet")
        assert snapshot.to_parquet(dest) == 2
        reloaded = QuantumRuntimeAnalyzer(
            QuantumRuntimeTable.from_parquets([dest])
        )
        _assert_table_identical(reloaded.summary(), analyzer.summary())
        _assert_table_dicts_identical(
            reloaded.dimension_dist("band"), analyzer.dimension_dist("band")
        )

    def test_canonical_module_identity(self) -> None:
        import lsst.pipe.base._runtime_analyzer.runtime_table as runtime_table

        assert runtime_table.__file__.endswith("runtime_table.py")
        # Canonical names resolve on the canonical module itself.
        assert runtime_table.QuantumRuntimeTable is QuantumRuntimeTable
        assert runtime_table.RUNTIME_SCHEMA is RUNTIME_SCHEMA
        # The Parquet engine and extraction producers are defined here.
        for func in (runtime_table.extract_runtime_table,
                     runtime_table.extract_merged_runtime_table,
                     QuantumRuntimeTable.to_parquet,
                     QuantumRuntimeTable.from_parquet):
            assert func.__module__ == "lsst.pipe.base._runtime_analyzer.runtime_table"
        import lsst.pipe.base._runtime_analyzer.console as console_module

        # The console module keeps display/export helpers only; the
        # QuantumRuntimeTable type and its Parquet cache format live solely in
        # runtime_table and are NOT re-exported here.
        assert not hasattr(console_module, "QuantumRuntimeTable")

    def test_module_level_imports_are_stdlib_and_numpy_only(self) -> None:
        import ast
        import pathlib

        import lsst.pipe.base._runtime_analyzer.runtime_table as _rt_module

        path = pathlib.Path(_rt_module.__file__)
        tree = ast.parse(path.read_text())
        top: set[str] = set()

        def collect(body: list[ast.stmt]) -> None:
            # Module level only (deferred calls allowed), but the
            # try-guarded pyarrow import is still module scope.
            for node in body:
                if isinstance(node, ast.Import):
                    top.update(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom):
                    top.add(node.module or "__future__")
                elif isinstance(node, ast.Try):
                    collect(node.body)

        collect(tree.body)
        assert top == {"__future__", "collections.abc", "dataclasses",
                       "datetime", "json", "pathlib", "numpy", "typing",
                       "uuid", "pyarrow", "pyarrow.parquet"}
        # No runtime_analyzer-internal module-level imports: the file stays
        # a drop-in upstream candidate.
        assert not [m for m in top if m.startswith("lsst.")]


def _merge_graphs() -> dict:
    """Two-source graph node map (``a.qg``/``b.qg``) for the merged-table
    tests: one (task_label, data_id) key is shared via equal-by-value
    ``FakeDataId`` stand-ins (the first source must win there, keeping its
    own dims), the other quanta are unique (ids 1, 2 / 3, 4).
    """
    shared_src1 = FakeDataId(visit=10, band="r")
    # Equal-by-value but distinct instance in the second source.
    shared_src2 = FakeDataId(visit=10, band="r")
    assert shared_src1 == shared_src2 and shared_src1 is not shared_src2
    return {
        "a.qg": [merge_node(1, shared_src1, 10.0),
                 merge_node(2, FakeDataId(visit=20, band="g"), 20.0)],
        "b.qg": [merge_node(3, shared_src2, 999.0),
                 merge_node(4, FakeDataId(visit=30, band="i"), 30.0)],
    }


class TestMergesAndMetadata:
    """Multi-source producers: merged-table first-wins semantics,
    source metadata/n_expected bookkeeping, cache source recording.
    """

    def test_extract_merged_signature(self) -> None:
        """extract_merged_runtime_table accepts a list of
        (path, collection) tuples.
        """
        import inspect

        from lsst.pipe.base._runtime_analyzer.runtime_table import (
            extract_merged_runtime_table,
        )
        sig = inspect.signature(extract_merged_runtime_table)
        params = list(sig.parameters.keys())
        assert params == ['sources']

    def test_two_source_merge_first_wins(self) -> None:
        from lsst.pipe.base.quantum_graph import ProvenanceQuantumGraph

        graphs = _merge_graphs()
        calls: list[dict] = []

        with mock.patch.object(ProvenanceQuantumGraph, "from_args",
                               side_effect=fake_from_args(
                                   graphs, header_task="TaskT",
                                   calls=calls)):
            analyzer = QuantumRuntimeAnalyzer(
                extract_merged_runtime_table([("a.qg", None),
                                              ("b.qg", None)])
            )

        # from_args called once per source, in order, with datasets=().
        assert [c["path"] for c in calls] == ["a.qg", "b.qg"]
        assert [c["collection"] for c in calls] == [None, None]
        assert [c["datasets"] for c in calls] == [(), ()]

        assert analyzer.n_sources == 2
        # 4 nodes, shared key collapses to 1 -> 3 quanta.
        assert analyzer.n_loaded == 3

        qt = analyzer.table
        by_qid = {uuid.UUID(bytes=q).int: i
                  for i, q in enumerate(qt.quantum_id_list())}
        assert set(by_qid) == {1, 2, 4}

        # First source wins on the shared (task_label, data_id) key: the
        # surviving row is source 1's (quantum 1, run_time 10), and the
        # shadowed source-2 quantum 3 is absent.
        assert float(qt.run_time[by_qid[1]]) == pytest.approx(10.0)
        assert 3 not in by_qid

        # Unique-from-second kept.
        assert float(qt.run_time[by_qid[4]]) == pytest.approx(30.0)

        # Captured dimensions are present in the merged analyzer, keyed by
        # the *winning* quantum ids.
        assert analyzer.dim_lookup[uuid.UUID(int=1).bytes] == {
            "band": "r", "visit": "10",
        }
        assert analyzer.dim_lookup[uuid.UUID(int=2).bytes] == {
            "band": "g", "visit": "20",
        }
        assert analyzer.dim_lookup[uuid.UUID(int=4).bytes] == {
            "band": "i", "visit": "30",
        }
        assert uuid.UUID(int=3).bytes not in analyzer.dim_lookup

        # The merged dimensions are usable by dimension_dist.
        dist = analyzer.dimension_dist("visit")
        groups = {str(r["group_key"]): int(r["quanta"]) for r in dist["TaskT"]}
        assert groups == {"10": 1, "20": 1, "30": 1}

    @staticmethod
    def _fake_from_args():
        """Build a ProvenanceQuantumGraph.from_args stand-in over the
        two-source merge graphs (first source wins on the shared key).
        """
        return fake_from_args(_merge_graphs(), header_task="TaskT")

    def test_merge_first_wins_and_metadata(self) -> None:
        from lsst.pipe.base.quantum_graph import ProvenanceQuantumGraph

        with mock.patch.object(ProvenanceQuantumGraph, "from_args",
                               side_effect=TestMergesAndMetadata._fake_from_args()):
            table = extract_merged_runtime_table([("a.qg", None), ("b.qg", None)])

        assert table.n_sources == 2
        assert table.sources == ("a.qg", "b.qg")
        # Multi-source merges report no expected count: the header sum
        # (2 + 2) would double-count quanta shared across sources.
        assert table.n_expected == 0

        by_qid = {uuid.UUID(bytes=q).int: i
                  for i, q in enumerate(table.quantum_id_list())}
        # First source wins on the shared key; shadowed quantum 3 absent.
        assert set(by_qid) == {1, 2, 4}
        assert float(table.run_time[by_qid[1]]) == pytest.approx(10.0)
        assert float(table.run_time[by_qid[4]]) == pytest.approx(30.0)

        # dim_lookup keyed by quantum_id.bytes, winner's dims only.
        assert set(table.dim_lookup) == {uuid.UUID(int=i).bytes
                                         for i in (1, 2, 4)}
        assert table.dim_lookup[uuid.UUID(int=1).bytes] == {
            "band": "r", "visit": "10",
        }

    def test_single_source_keeps_header_count(self) -> None:
        from lsst.pipe.base.quantum_graph import ProvenanceQuantumGraph

        with mock.patch.object(ProvenanceQuantumGraph, "from_args",
                               side_effect=TestMergesAndMetadata._fake_from_args()):
            analyzer = QuantumRuntimeAnalyzer(
                extract_merged_runtime_table([("a.qg", None)])
            )

        # Single source: the a.qg header count ({"TaskT": 2}) is carried
        # through untouched, like extract_runtime_table reports it.
        assert analyzer.n_expected == 2
        assert analyzer.n_loaded == 2
        assert analyzer.n_sources == 1

    def test_empty_sources(self) -> None:
        table = extract_merged_runtime_table([])
        assert table.n_rows == 0
        assert table.to_arrow().schema == RUNTIME_SCHEMA
        assert table.dim_lookup == {}
        assert table.n_expected == 0
        assert table.n_sources == 0
        assert table.sources == ()
        analyzer = QuantumRuntimeAnalyzer(extract_merged_runtime_table([]))
        assert analyzer.n_loaded == 0
        assert analyzer.n_expected == 0

    def test_blocked_first_source_later_source_wins(self) -> None:
        """A resource_usage=None node claims no key, so a later source's
        runnable quantum for the same (task_label, data_id) prevails.
        """
        from lsst.pipe.base.quantum_graph import ProvenanceQuantumGraph

        shared_a = FakeDataId(visit=10, band="r")
        shared_b = FakeDataId(visit=10, band="r")
        blocked = merge_node(5, shared_a, 1.0)
        blocked[1]["resource_usage"] = None
        graphs = {
            "a.qg": [blocked],
            "b.qg": [merge_node(6, shared_b, 12.0)],
        }

        with mock.patch.object(ProvenanceQuantumGraph, "from_args",
                               side_effect=fake_from_args(
                                   graphs, header_task="TaskT")):
            table = extract_merged_runtime_table([("a.qg", None), ("b.qg", None)])

        by_qid = {uuid.UUID(bytes=q).int: i
                  for i, q in enumerate(table.quantum_id_list())}
        assert set(by_qid) == {6}
        assert float(table.run_time[by_qid[6]]) == pytest.approx(12.0)

    def test_from_parquets_records_source_paths(self, tmp_path) -> None:
        """from_parquets records the per-file cache paths as sources."""
        path_a = str(tmp_path / "a.parquet")
        path_b = str(tmp_path / "b.parquet")
        make_table(
            [_row("TaskT", 10.0, data_id="{visit=1}", quantum_int=1)],
        ).to_parquet(path_a)
        make_table(
            [_row("TaskT", 20.0, data_id="{visit=2}", quantum_int=2)],
        ).to_parquet(path_b)

        qt = QuantumRuntimeTable.from_parquets([path_a, path_b])
        assert qt.sources == (path_a, path_b)
        assert qt.n_sources == 2
        assert qt.n_rows == 2
        assert sorted(qt.run_time.tolist()) == [10.0, 20.0]

    def test_analyzer_from_cache_matches_graph(self, tmp_path) -> None:
        """Round-tripping an analyzer through a cache table is lossless.

        Spec scenario: summary(), top_quantities(), dimension_dist(), and
        bottleneck() produce tables identical to those of the original
        analyzer.  The test_core fixtures are mock graphs, so the original
        here is constructed directly from a QuantumRuntimeTable (with captured
        dim_lookup), mirroring the from_parquets cache-load output shape.
        """
        calibrate = [
            _row("calibrate", 10.0, memory=100.0, status="SUCCESSFUL",
                 data_id="{visit=8001, band='r'}", quantum_int=101),
            _row("calibrate", 20.0, memory=200.0, status="FAILED",
                 data_id="{visit=8002, band='r'}", quantum_int=102),
            _row("calibrate", 30.0, memory=300.0, status="SUCCESSFUL",
                 data_id="{visit=8003, band='r'}", quantum_int=103),
            _row("calibrate", 40.0, memory=400.0, status="ABORTED",
                 data_id="{visit=8004, band='r'}", quantum_int=104),
            _row("calibrate", 1000.0, memory=700.0, status="SUCCESSFUL",
                 data_id="{visit=8007, band='r'}", quantum_int=105),
        ]
        coadd = [
            _row("coadd", 50.0, memory=600.0, status="SUCCESSFUL",
                 data_id="{band='r'}", quantum_int=201),
            _row("coadd", 60.0, memory=100.0, status="SUCCESSFUL",
                 data_id="{band='g'}", quantum_int=202),
            _row("coadd", 70.0, memory=900.0, status="BLOCKED",
                 data_id="{band='u'}", quantum_int=203),
        ]
        rows = calibrate + coadd

        dim_lookup: dict[bytes, dict[str, str]] = {}
        for i in range(101, 106):
            dim_lookup[uuid.UUID(int=i).bytes] = {"visit": str(8000 + i - 100),
                                                  "band": "r"}
        # coadd dims carry band only.
        for i, band in zip((201, 202, 203), ("r", "g", "u")):
            dim_lookup[uuid.UUID(int=i).bytes] = {"band": band}

        original = make_analyzer(rows, dim_lookup)

        cache_path = str(tmp_path / "run.parquet")
        cache_qt = make_table(rows, dim_lookup, sources=("run.qg",))
        cache_qt.to_parquet(cache_path)

        cached = QuantumRuntimeAnalyzer(
            QuantumRuntimeTable.from_parquets([cache_path])
        )

        # Same merged table in the same order.
        assert cached.table.to_arrow().equals(
            make_table(rows, dim_lookup).to_arrow()
        )
        assert cached.n_loaded == len(rows)
        assert cached.n_sources == 1

        _assert_table_identical(cached.summary(), original.summary())
        for metric in ("run_time", "memory"):
            for n in (3, 8, 100):
                _assert_table_identical(
                    cached.top_quantities(metric=metric, n=n),
                    original.top_quantities(metric=metric, n=n),
                )
        for status in (None, "failed", "successful"):
            _assert_table_identical(
                cached.summary(status=status), original.summary(status=status)
            )

        _assert_table_dicts_identical(
            cached.dimension_dist("band"), original.dimension_dist("band")
        )
        _assert_table_dicts_identical(
            cached.dimension_dist("visit"), original.dimension_dist("visit")
        )

        for method in ("iqr", "zscore"):
            _assert_table_dicts_identical(
                cached.bottleneck(method=method),
                original.bottleneck(method=method),
            )
        # The seeded calibrate outlier (visit=8007, 1000 s) must survive.
        outliers = cached.bottleneck()["outlier_table"]
        assert str(outliers[0]["Task"]) == "calibrate"
        assert float(outliers[0]["run_time"]) == pytest.approx(1000.0)
