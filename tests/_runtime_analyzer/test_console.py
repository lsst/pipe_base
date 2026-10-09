"""Tests for the console module (format_table, export_csv, export_parquet)
and the Parquet cache engine owned by runtime_table.
"""

from __future__ import annotations

import csv
import datetime
import json
import uuid
from pathlib import Path

import astropy.table
import numpy as np
import pytest

pytest.importorskip("pyarrow")  # provided by the [runtime] extra

from lsst.pipe.base._runtime_analyzer.console import (  # noqa: E402
    _convert_numpy_types,
    export_csv,
    export_parquet,
    format_table,
)
from lsst.pipe.base._runtime_analyzer.runtime_table import (  # noqa: E402
    RUNTIME_SCHEMA,
    QuantumRuntimeTable,
)

from .support import (  # noqa: E402
    make_table,
    quantum_row,
    schema_probe_table,
)


class TestFormatTable:
    """Tests for ``format_table``."""

    def test_basic_format(self) -> None:
        table = astropy.table.Table({'Task': ['A', 'B'], 'Count': [10, 20]})
        result = format_table(table)
        assert isinstance(result, str)
        assert 'A' in result
        assert 'B' in result

    def test_empty_table(self) -> None:
        table = astropy.table.Table()
        result = format_table(table)
        assert isinstance(result, str)


class TestConvertNumpyTypes:
    """Tests for ``_convert_numpy_types``."""

    def test_integer(self) -> None:
        assert _convert_numpy_types(np.int64(42)) == 42

    def test_float(self) -> None:
        val = _convert_numpy_types(np.float64(3.14))
        assert val == pytest.approx(3.14)

    def test_nan(self) -> None:
        val = _convert_numpy_types(np.float64(np.nan))
        assert val is None

    def test_boolean(self) -> None:
        assert _convert_numpy_types(np.bool_(True)) is True

    def test_array(self) -> None:
        result = _convert_numpy_types(np.array([1, 2, 3]))
        assert result == [1, 2, 3]

    def test_bytes(self) -> None:
        result = _convert_numpy_types(b'hello')
        assert result == '68656c6c6f'  # hex encoding

    def test_native_python(self) -> None:
        assert _convert_numpy_types(42) == 42
        assert _convert_numpy_types('hello') == 'hello'
        assert _convert_numpy_types(None) is None


class TestExportCsv:
    """Tests for ``export_csv``."""

    def test_simple_export(self, tmp_path: Path) -> None:
        table = astropy.table.Table({
            'Task': ['A', 'B'],
            'Time': [10.5, 20.0],
        })
        dest = tmp_path / 'test.csv'
        export_csv(table, str(dest))
        assert dest.exists()

        with open(dest) as f:
            reader = csv.reader(f)
            rows = list(reader)
        assert rows[0] == ['Task', 'Time']
        assert rows[1][0] == 'A'
        assert rows[2][0] == 'B'

    def test_empty_table(self, tmp_path: Path) -> None:
        table = astropy.table.Table()
        dest = tmp_path / 'empty.csv'
        export_csv(table, str(dest))
        assert dest.exists()

    def test_with_integers(self, tmp_path: Path) -> None:
        table = astropy.table.Table({
            'Task': ['A'],
            'Count': [42],
        })
        dest = tmp_path / 'ints.csv'
        export_csv(table, str(dest))
        with open(dest) as f:
            reader = csv.reader(f)
            rows = list(reader)
        assert '42' in rows[1]

    def test_with_bytes(self, tmp_path: Path) -> None:
        table = astropy.table.Table({
            'ID': [b'\x01\x02\x03\x04'],
        })
        dest = tmp_path / 'bytes.csv'
        export_csv(table, str(dest))
        assert dest.exists()
        # astropy converts bytes to a string, so export holds raw string


class TestExportParquet:
    """Tests for ``export_parquet`` and the Parquet round-trip helpers."""

    def test_basic_export(self, tmp_path: Path) -> None:
        table = astropy.table.Table({
            'Task': ['A', 'B'],
            'Time': [10.5, 20.0],
            'Count': [1, 2],
        })
        dest = tmp_path / 'test.parquet'
        export_parquet(table, str(dest))
        assert dest.exists()

        import pyarrow.parquet as pq
        arrow = pq.read_table(str(dest))
        assert arrow.num_rows == 2
        assert arrow.column('Task').to_pylist() == ['A', 'B']


class TestParquetCache:
    """Round-trip fidelity of the Parquet runtime table cache."""

    def test_to_parquet_from_parquet_roundtrip(self, tmp_path: Path) -> None:
        from lsst.pipe.base.version import __version__

        # One quantum_id deliberately ends in a 0x00 byte: naive scalar
        # bytes conversion would silently truncate it.
        trailing_nul_id = uuid.UUID(int=1 << 120)
        qids = [uuid.UUID(int=1), uuid.UUID(int=2), trailing_nul_id]
        rows = [
            quantum_row("calibrate", qids[0], 10.25, memory=100.5,
                        data_id="{visit=8001, band='r'}"),
            quantum_row("calibrate", qids[1], 20.5, memory=200.0,
                        status="FAILED", data_id="{visit=8002, band='r'}"),
            quantum_row("coadd", qids[2], 1000.75, memory=700.25,
                        status="ABORTED", data_id="{band='u'}"),
        ]
        dim_lookup = {
            qids[0].bytes: {"visit": "8001", "band": "r"},
            qids[1].bytes: {"visit": "8002", "band": "r"},
            qids[2].bytes: {"band": "u"},
        }
        qt = make_table(
            rows, dim_lookup, sources=("repo:run1", "extra.qg"),
        )
        dest = tmp_path / "cache.parquet"

        n = qt.to_parquet(dest, fingerprint="fp-abc")
        assert n == len(rows)
        assert dest.exists()

        loaded = QuantumRuntimeTable.from_parquet(dest)
        arrow = loaded.to_arrow()

        # Arrow fidelity: exact schema and values, including the exact
        # 16-byte binary ids with trailing NULs.
        assert arrow.schema == RUNTIME_SCHEMA
        assert arrow.equals(qt.to_arrow())
        assert arrow.column("quantum_id").to_pylist() == [
            q.bytes for q in qids
        ]
        assert loaded.run_time.dtype == np.float32
        assert loaded.run_time.tolist() == [10.25, 20.5, 1000.75]
        assert loaded.memory.dtype == np.float32
        assert list(loaded.labels()) == ["calibrate", "calibrate", "coadd"]
        assert loaded.data_id_list() == [
            "{visit=8001, band='r'}", "{visit=8002, band='r'}", "{band='u'}",
        ]

        assert loaded.dim_lookup == dim_lookup

        meta = arrow.schema.metadata
        assert meta[b"schema_version"].decode() == "1"
        assert (meta[b"runtime_analyzer_version"].decode() == __version__)
        assert json.loads(meta[b"sources"]) == ["repo:run1", "extra.qg"]
        assert meta[b"n_rows"].decode() == str(len(rows))
        # created_at is UTC ISO-8601.
        parsed = datetime.datetime.fromisoformat(
            meta[b"created_at"].decode()
        )
        assert parsed.tzinfo is not None
        assert meta[b"fingerprint"].decode() == "fp-abc"

    def test_trailing_nul_quantum_id_roundtrip_and_analysis(
        self, tmp_path: Path
    ) -> None:
        """Regression: ids ending in 0x00 must round-trip through the cache
        and remain usable downstream.  ``bytes()`` on an 'S16' scalar strips
        trailing NULs, which broke ``_dim_lookup`` lookups (silent fallback
        to data_id parsing) and made ``uuid.UUID(bytes=...)`` in
        ``bottleneck`` raise.  dimension_dist() and bottleneck() must both
        work on a cache whose quantum_id ends in 0x00.
        """
        from lsst.pipe.base._runtime_analyzer.core import QuantumRuntimeAnalyzer

        nul_qid = uuid.UUID(int=1 << 120)  # 0x01 followed by fifteen 0x00
        qid2 = uuid.UUID(int=2)
        qid3 = uuid.UUID(int=3)
        qid4 = uuid.UUID(int=4)
        # data_ids are deliberately unparseable for "visit" so the analysis
        # can only succeed via the captured dims_json / full-width keys.
        rows = [
            quantum_row("coadd", nul_qid, 1000.0, data_id="opaque-a"),
            quantum_row("coadd", qid2, 10.0, data_id="opaque-b"),
            quantum_row("coadd", qid3, 11.0, data_id="opaque-c"),
            quantum_row("coadd", qid4, 12.0, data_id="opaque-d"),
        ]
        dim_lookup = {
            nul_qid.bytes: {"visit": "42"},
            qid2.bytes: {"visit": "7"},
            qid3.bytes: {"visit": "8"},
            qid4.bytes: {"visit": "9"},
        }
        dest = tmp_path / "nulid.parquet"
        qt = make_table(rows, dim_lookup)
        assert qt.to_parquet(dest) == len(rows)

        analyzer = QuantumRuntimeAnalyzer(
            QuantumRuntimeTable.from_parquets([str(dest)])
        )

        dist = analyzer.dimension_dist("visit")
        assert "coadd" in dist
        groups = {str(r["group_key"]): int(r["quanta"]) for r in dist["coadd"]}
        assert groups == {"42": 1, "7": 1, "8": 1, "9": 1}

        # The 1000 s outlier carries the trailing-NUL id; bottleneck
        # UUID-formats the truncated id.
        result = analyzer.bottleneck()
        outliers = list(result["outlier_table"]["quantum_id"])
        assert outliers == [str(nul_qid)]
        assert uuid.UUID(outliers[0]).bytes == nul_qid.bytes

    def test_fingerprint_none_stored_as_empty(self, write_cache) -> None:
        dest = write_cache("nofp.parquet", [("T", 1.0, "{band='g'}", 7)])
        meta = QuantumRuntimeTable.from_parquet(dest).to_arrow().schema.metadata
        assert meta[b"fingerprint"].decode() == ""
        assert json.loads(meta[b"sources"]) == []

    def test_empty_table_roundtrip(self, tmp_path: Path) -> None:
        qt = QuantumRuntimeTable.from_rows([])
        dest = tmp_path / "empty.parquet"
        assert qt.to_parquet(dest) == 0
        loaded = QuantumRuntimeTable.from_parquet(dest)
        assert loaded.to_arrow().schema == RUNTIME_SCHEMA
        assert loaded.n_rows == 0
        assert loaded.dim_lookup == {}
        meta = loaded.to_arrow().schema.metadata
        assert meta[b"n_rows"].decode() == "0"

    def test_from_parquet_rejects_schema_version(self, tmp_path: Path) -> None:
        import pyarrow.parquet as pq

        table = schema_probe_table()
        dest = tmp_path / "future.parquet"

        table = table.replace_schema_metadata({"schema_version": "99"})
        pq.write_table(table, str(dest), compression="snappy")
        with pytest.raises(ValueError, match="99") as excinfo:
            QuantumRuntimeTable.from_parquet(dest)
        assert "schema_version" in str(excinfo.value)

        # A minor bump within the supported major version is accepted.
        table = table.replace_schema_metadata({"schema_version": "1.4"})
        pq.write_table(table, str(dest), compression="snappy")
        loaded = QuantumRuntimeTable.from_parquet(dest)
        assert loaded.to_arrow().schema == RUNTIME_SCHEMA
        # Empty per-row dim maps carry no information: omitted.
        assert loaded.dim_lookup == {}
        meta = loaded.to_arrow().schema.metadata
        assert meta[b"schema_version"].decode() == "1.4"

    def test_from_parquet_requires_schema_version(self, tmp_path: Path) -> None:
        """schema_version is required: files lacking it are rejected, only
        files WITH a recognized major version pass.
        """
        import pyarrow.parquet as pq

        table = schema_probe_table()
        dest = tmp_path / "noversion.parquet"

        # Metadata present but without the schema_version key at all.
        table = table.replace_schema_metadata(
            {"runtime_analyzer_version": "0.0.0", "sources": "[]"}
        )
        pq.write_table(table, str(dest), compression="snappy")
        with pytest.raises(ValueError, match="schema_version") as excinfo:
            QuantumRuntimeTable.from_parquet(dest)
        assert "lacks schema_version" in str(excinfo.value)

        # No metadata whatsoever: also rejected.
        table = table.replace_schema_metadata(None)
        pq.write_table(table, str(dest), compression="snappy")
        with pytest.raises(ValueError, match="schema_version"):
            QuantumRuntimeTable.from_parquet(dest)

    @pytest.mark.parametrize(
        "null_column",
        ["status", "task_label"],
    )
    def test_from_parquet_rejects_nulls(
        self, tmp_path: Path, null_column: str
    ) -> None:
        """Parquet nulls in required columns (hand-edited file) must raise
        ValueError naming the column, not be cast into garbage values.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        columns: dict[str, pa.Array] = {
            "task_label": pa.array(["T", "T"], type=pa.string()),
            "quantum_id": pa.array(
                [uuid.UUID(int=1).bytes, uuid.UUID(int=2).bytes],
                type=pa.binary(16),
            ),
            "status": pa.array([1, 1], type=pa.int8()),
            "memory": pa.array([100.0, 200.0], type=pa.float32()),
            "prep_time": pa.array([1.0, 1.0], type=pa.float32()),
            "init_time": pa.array([1.0, 1.0], type=pa.float32()),
            "run_time": pa.array([10.0, 20.0], type=pa.float32()),
            "run_time_cpu": pa.array([5.0, 10.0], type=pa.float32()),
            "data_id": pa.array(["{}", "{}"], type=pa.string()),
            "dims": pa.array([{}, {}],
                             type=pa.map_(pa.string(), pa.string())),
        }
        nullable = {
            "status": pa.array([1, None], type=pa.int8()),
            "task_label": pa.array(["T", None], type=pa.string()),
        }
        columns[null_column] = nullable[null_column]

        schema = pa.schema(
            [(name, columns[name].type) for name in columns]
        )
        table = pa.Table.from_arrays(
            [columns[name] for name in columns], schema=schema
        )
        # Valid schema_version so the null check is the one that trips.
        table = table.replace_schema_metadata({"schema_version": "1"})
        dest = tmp_path / "nulls.parquet"
        pq.write_table(table, str(dest), compression="snappy")

        with pytest.raises(ValueError, match=null_column) as excinfo:
            QuantumRuntimeTable.from_parquet(dest)
        assert "null" in str(excinfo.value)
