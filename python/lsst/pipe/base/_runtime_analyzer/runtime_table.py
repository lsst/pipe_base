"""Standalone runtime table: row contract, Arrow table value type,
graph -> table extraction, and the Parquet cache engine.

Defines ``ROW_FIELDS`` (the per-quantum scalar columns — every
``RUNTIME_SCHEMA`` field except ``dims`` — in tuple order),
``RUNTIME_SCHEMA`` (the single Arrow schema shared by the in-memory
representation, the Parquet cache format, and the upstream table
contract), ``QuantumRuntimeTable`` (the immutable Arrow-backed value container
bundling an Arrow table with the per-quantum dimension view and extraction
bookkeeping), the graph -> table producers
(``extract_runtime_table``/``extract_merged_runtime_table``, which
duck-type the pipe_base node objects they read), and the Parquet cache
engine (``QuantumRuntimeTable.to_parquet``/``from_parquet``/
``QuantumRuntimeTable.from_parquets``), which serializes that layout with a
self-describing metadata block.

This module is the seam for moving runtime table functionality upstream to
pipe_base: it depends only on the standard library, numpy, and pyarrow (an
optional dependency of the pipe_base ``[runtime]`` extra, guarded at import
time); ``ProvenanceQuantumGraph`` is imported
lazily inside the functions that need it, and the data-ID string parser
lives in the dependency-free :mod:`lsst.pipe.base._runtime_analyzer.dimensions`
module (which imports nothing from here) and is likewise imported where
used — nothing here imports the rest of ``runtime_analyzer`` at module
level.
"""

from __future__ import annotations

__all__ = [
    "INT_TO_STATUS",
    "ROW_FIELDS",
    "RUNTIME_SCHEMA",
    "STATUS_MAP",
    "QuantumRuntimeTable",
    "extract_merged_runtime_table",
    "extract_runtime_table",
]

import dataclasses
import json
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "Runtime-table caching requires 'pyarrow >= 20'. Install "
        "pipe_base with the [runtime] extra."
    ) from exc


# Width of the raw UUID byte string carried by every quantum row.
_QUANTUM_ID_BYTES = 16

# Arrow map type of the per-quantum dimensions column.
_DIMS_TYPE = pa.map_(pa.string(), pa.string())

# Arrow schema: the single source of truth for the field set and its
# order.  The memory representation, the Parquet cache schema, and the
# upstream table contract are one schema.  String fields are unbounded
# (Arrow ``string``): pipe_base task labels (full dotted pipeline task
# module paths) and ``str(DataCoordinate)`` data IDs with many dimensions
# can be arbitrarily long.
RUNTIME_SCHEMA = pa.schema([
    ("task_label", pa.dictionary(pa.int32(), pa.string())),
    ("quantum_id", pa.binary(_QUANTUM_ID_BYTES)),  # raw UUID bytes
    ("status", pa.int8()),                  # QuantumAttemptStatus code
    ("memory", pa.float32()),               # MiB
    ("prep_time", pa.float32()),            # seconds
    ("init_time", pa.float32()),            # seconds
    ("run_time", pa.float32()),             # seconds
    ("run_time_cpu", pa.float32()),         # seconds
    ("data_id", pa.string()),               # str(DataCoordinate) display
    ("dims", _DIMS_TYPE),
])

# The scalar quantum fields in canonical tuple order (the layout
# ``_node_to_row`` emits and ``_rows_to_arrow`` consumes): every schema
# field except ``dims``, whose per-row maps are supplied out-of-band via
# the ``dim_lookup`` argument.
ROW_FIELDS: tuple[str, ...] = tuple(
    f.name for f in RUNTIME_SCHEMA if f.name != "dims"
)

_CACHE_SCHEMA_VERSION = "1"
_CACHE_SCHEMA_MAJOR_VERSION = 1


# ---------------------------------------------------------------------------
# Arrow bridge helpers
# ---------------------------------------------------------------------------


def _task_label_column(labels: Sequence[str]) -> pa.Array:
    """Dictionary-encode task labels with deterministically sorted uniques.

    ``np.unique`` gives deterministically sorted uniques, matching the
    sorted-unique task grouping that the analysis layer relies on.

    Parameters
    ----------
    labels : `list` of `str`
        Task label per row (may repeat).

    Returns
    -------
    col : `pyarrow.Array`
        ``dictionary<int32, string>`` column.
    """
    uniques, codes = np.unique(
        np.asarray(labels, dtype=object) if labels
        else np.asarray([], dtype=object),
        return_inverse=True,
    )
    return pa.DictionaryArray.from_arrays(
        pa.array(np.asarray(codes, dtype=np.int32)),
        pa.array([str(u) for u in uniques.tolist()], type=pa.string()),
    )


def _rows_to_arrow(
    rows: list[tuple[Any, ...]],
    dim_lookup: dict[bytes, dict[str, str]] | None = None,
) -> pa.Table:
    """Build a `RUNTIME_SCHEMA` Arrow table from ``ROW_FIELDS`` tuples.

    Transposes the raw Python row tuples straight into Arrow columns in a
    single pass, building the typed columns directly.

    Parameters
    ----------
    rows : `list` of `tuple`
        Tuples ordered as ``ROW_FIELDS``.
    dim_lookup : `dict` [ `bytes`, `dict` [ `str`, `str` ] ], optional
        Per-quantum dimension maps keyed by exact 16-byte quantum ids.

    Returns
    -------
    table : `pyarrow.Table`
    """
    dim_lookup = dim_lookup or {}
    if not rows:
        return pa.Table.from_arrays(
            [pa.array([], type=f.type) for f in RUNTIME_SCHEMA],
            schema=RUNTIME_SCHEMA,
        )
    # Transpose once; the tuples are consumed in place by the column
    # builders below (no per-column list copies).
    (labels, qids, status, memory, prep_time, init_time, run_time,
     run_time_cpu, data_ids) = zip(*rows)

    # Arrow reads (never retains) the mapping objects during construction,
    # so the source dim maps pass straight through.
    if dim_lookup:
        dims_values = [dim_lookup.get(bytes(q)) or {} for q in qids]
    else:
        dims_values = [{}] * len(rows)

    return pa.Table.from_arrays(
        [
            _task_label_column(labels),
            pa.array(qids, type=pa.binary(_QUANTUM_ID_BYTES)),
            pa.array(status, type=pa.int8()),
            pa.array(memory, type=pa.float32()),
            pa.array(prep_time, type=pa.float32()),
            pa.array(init_time, type=pa.float32()),
            pa.array(run_time, type=pa.float32()),
            pa.array(run_time_cpu, type=pa.float32()),
            pa.array(data_ids, type=pa.string()),
            pa.array(dims_values, type=_DIMS_TYPE),
        ],
        schema=RUNTIME_SCHEMA,
    )


def _arrow_column(table: pa.Table, name: str) -> pa.Array:
    """Return the single (or empty) chunk of a column."""
    chunked = table.column(name)
    if chunked.num_chunks == 0:
        return pa.array([], type=table.schema.field(name).type)
    return chunked.chunk(0)


@dataclass(frozen=True, eq=False)
class QuantumRuntimeTable:
    """Immutable Arrow-backed container for an extracted runtime table.

    The canonical view, returned by ``to_arrow()``, is a single-chunk
    ``pyarrow.Table`` with ``RUNTIME_SCHEMA`` (the memory layout IS the
    Parquet cache schema):
    dictionary-encoded ``task_label``, ``fixed_size_binary(16)``
    ``quantum_id``, int8 ``status``, float32 metrics, utf8 ``data_id``, and
    a ``map<string, string>`` ``dims`` column holding the normalized
    dimension decomposition per row.  Analysis and plotting consume the
    typed accessors below.

    Instances are frozen and compare by identity (the dataclass uses
    ``eq=False``).  Accessor results (NumPy arrays, lists, and the
    ``dim_lookup`` dict) are cached, shared objects: treat them as
    read-only, because mutation is visible to every later consumer of this
    object.  Mutating ``dim_lookup`` additionally does not rewrite the
    underlying ``dims`` column.

    Attributes
    ----------
    arrow : `pyarrow.Table`
        Arrow store with the ``RUNTIME_SCHEMA`` layout (may hold multiple
        chunks; ``to_arrow()`` returns the single-chunk canonical view).
    n_expected : `int`
        Expected quantum count summed from the graph header(s); ``0`` when
        not derivable.
    n_sources : `int`
        Number of graph sources the table was extracted from.
    sources : `tuple` of `str`
        Source paths recorded at extraction time.
    _cache : `dict` [ `str`, `Any` ]
        Internal memo for lazily materialized derived views (the combined
        Arrow table, NumPy column/label/id views, and dimension lookups).
        An implementation detail; never affects equality (identity-based)
        or ``repr``.
    """

    arrow: pa.Table
    n_expected: int = 0
    n_sources: int = 0
    sources: tuple[str, ...] = ()
    _cache: dict[str, Any] = field(
        init=False, default_factory=dict, repr=False
    )

    # -- constructors -------------------------------------------------------

    @classmethod
    def from_arrow(
        cls,
        table: pa.Table,
        *,
        n_expected: int = 0,
        n_sources: int = 1,
        sources: Sequence[str] = (),
    ) -> QuantumRuntimeTable:
        """Build a table from a ``RUNTIME_SCHEMA``-layout Arrow table.

        Task labels are re-dictionary-encoded with sorted uniques so that
        ``task_labels``/``task_label_codes`` are deterministic regardless of
        the input column's encoding.

        Parameters
        ----------
        table : `pyarrow.Table`
            Table carrying at least the ``RUNTIME_SCHEMA`` column names.
        n_expected : `int`, optional
            Expected quantum count from the graph header(s).  Default is
            0 (not derivable).
        n_sources : `int`, optional
            Number of sources the table was extracted from.  Default is 1.
        sources : `~collections.abc.Sequence` of `str`, optional
            Source paths recorded at extraction time.  Default is empty.

        Returns
        -------
        qt : `QuantumRuntimeTable`
        """
        table = table.select([f.name for f in RUNTIME_SCHEMA])
        labels = [str(x) for x in table.column("task_label").to_pylist()]
        table = table.set_column(
            table.schema.get_field_index("task_label"),
            RUNTIME_SCHEMA.field("task_label"),
            _task_label_column(labels),
        )
        return cls(
            n_expected=n_expected,
            n_sources=n_sources,
            sources=tuple(sources),
            arrow=table,
        )

    @classmethod
    def from_rows(
        cls,
        rows: Sequence[tuple[Any, ...]],
        dim_lookup: dict[bytes, dict[str, str]] | None = None,
        *,
        n_expected: int = 0,
        n_sources: int = 1,
        sources: Sequence[str] = (),
    ) -> QuantumRuntimeTable:
        """Build a table from ``ROW_FIELDS``-ordered row tuples.

        Parameters
        ----------
        rows : `~collections.abc.Sequence` of `tuple`
            Rows ordered as ``ROW_FIELDS``: ``(task_label: str,
            quantum_id: bytes (16), status: int, memory: float,
            prep_time: float, init_time: float, run_time: float,
            run_time_cpu: float, data_id: str)``.
        dim_lookup : `dict` [ `bytes`, `dict` [ `str`, `str` ] ], optional
            Per-quantum dimension maps keyed by exact 16-byte quantum ids,
            embedded into the ``dims`` column.
        n_expected : `int`, optional
            Expected quantum count from the graph header(s).  Default is
            0 (not derivable).
        n_sources : `int`, optional
            Number of sources the table was extracted from.  Default is 1.
        sources : `~collections.abc.Sequence` of `str`, optional
            Source paths recorded at extraction time.  Default is empty.

        Returns
        -------
        qt : `QuantumRuntimeTable`
        """
        return cls(
            arrow=_rows_to_arrow(list(rows), dim_lookup),
            n_expected=n_expected,
            n_sources=n_sources,
            sources=tuple(sources),
        )

    # -- canonical Arrow view ----------------------------------------------

    def _get_arrow(self) -> pa.Table:
        table = self._cache.get("arrow")
        if table is None:
            table = self.arrow.combine_chunks()
            self._cache["arrow"] = table
        return table

    def to_arrow(self) -> pa.Table:
        """Return the canonical single-chunk Arrow view of this table.

        Returns
        -------
        table : `pyarrow.Table`
            Table with ``RUNTIME_SCHEMA``.
        """
        return self._get_arrow()

    # -- dimension view ------------------------------------------------------

    @property
    def dim_lookup(self) -> dict[bytes, dict[str, str]]:
        """Per-quantum dimension map keyed by 16-byte quantum ids.

        Lazily materialized from the Arrow ``dims`` column; the same dict
        object (mutable, cached) is returned on every call.
        """
        d = self._cache.get("dims")
        if d is None:
            t = self._get_arrow()
            d = {}
            if t.num_rows:
                matrix = self.quantum_id_matrix
                for i, m in enumerate(_arrow_column(t, "dims").to_pylist()):
                    # Empty maps carry no information: leave the quantum out
                    # so callers can treat an empty lookup as "no captured
                    # dimensions" and fall back to data-id parsing.
                    if m:
                        d[bytes(matrix[i].tobytes())] = dict(m)
            self._cache["dims"] = d
        return d

    # -- scalar accessors ----------------------------------------------------

    @property
    def n_rows(self) -> int:
        """Number of quantum rows."""
        return self._get_arrow().num_rows

    def numeric(self, col: str) -> np.ndarray:
        """Return a numpy view of a fixed-width numeric column.

        Parameters
        ----------
        col : `str`
            One of ``status``, ``memory``, ``prep_time``, ``init_time``,
            ``run_time``, ``run_time_cpu``.

        Returns
        -------
        values : `~numpy.ndarray`
            Contiguous array sharing the Arrow buffer; the fixed-width
            columns are null-free, so no conversion copy occurs.
        """
        return np.asarray(_arrow_column(self._get_arrow(), col))

    @property
    def run_time(self) -> np.ndarray:
        """``float32`` run-time seconds per row."""
        return self.numeric("run_time")

    @property
    def memory(self) -> np.ndarray:
        """``float32`` peak memory (MiB) per row."""
        return self.numeric("memory")

    @property
    def prep_time(self) -> np.ndarray:
        """``float32`` prep time (s) per row."""
        return self.numeric("prep_time")

    @property
    def init_time(self) -> np.ndarray:
        """``float32`` init time (s) per row."""
        return self.numeric("init_time")

    @property
    def run_time_cpu(self) -> np.ndarray:
        """``float32`` CPU run-time seconds per row."""
        return self.numeric("run_time_cpu")

    @property
    def status_codes(self) -> np.ndarray:
        """``int8`` status codes per row.

        Values are ``lsst.pipe.base.QuantumAttemptStatus`` member codes:
        ``-4`` ABORTED, ``-3`` UNKNOWN, ``-2`` ABORTED_SUCCESS, ``-1``
        FAILED, ``0`` BLOCKED, ``1`` SUCCESSFUL.
        """
        return self.numeric("status")

    # -- label / id accessors ------------------------------------------------

    @property
    def task_label_codes(self) -> np.ndarray:
        """``int32`` dictionary codes for ``task_label``."""
        return np.asarray(_arrow_column(self._get_arrow(),
                                        "task_label").indices)

    @property
    def task_labels(self) -> tuple[str, ...]:
        """Unique task labels (the ``task_label`` dictionary values).

        Tables built via ``from_arrow``/``from_rows``/``from_parquet``
        re-encode labels so the values are sorted; ``select`` keeps the
        source encoding (codes survive) and ``merge_first_wins`` unifies
        the inputs' dictionaries, so those paths need not be globally
        sorted.  Sort explicitly where group order matters.
        """
        return tuple(
            str(v) for v in _arrow_column(self._get_arrow(),
                                          "task_label").dictionary.to_pylist()
        )

    def labels(self) -> np.ndarray:
        """Task label string per row (cached).

        Returns
        -------
        labels : `~numpy.ndarray`
            Task label per row, in table order (empty when the table has
            no rows).
        """
        arr = self._cache.get("labels")
        if arr is None:
            arr = np.array(
                _arrow_column(self._get_arrow(), "task_label").to_pylist()
            )
            self._cache["labels"] = arr
        return arr

    def data_id_list(self) -> list[str]:
        """Stringified ``DataCoordinate`` per row (cached).

        Returns
        -------
        data_ids : `list` of `str`
            Display string per row, in table order (empty for an empty
            table).
        """
        out = self._cache.get("data_ids")
        if out is None:
            out = list(_arrow_column(self._get_arrow(),
                                     "data_id").to_pylist())
            self._cache["data_ids"] = out
        return out

    @property
    def quantum_id_matrix(self) -> np.ndarray:
        """Read-only ``(n, 16)`` uint8 view of the exact quantum ids."""
        m = self._cache.get("id_matrix")
        if m is None:
            arr = _arrow_column(self._get_arrow(), "quantum_id")
            buf = arr.buffers()[1]
            size = len(buf) if buf is not None else 0
            m = np.frombuffer(
                buf if size else b"", dtype=np.uint8,
            ).reshape(
                size // _QUANTUM_ID_BYTES, _QUANTUM_ID_BYTES
            )
            self._cache["id_matrix"] = m
        return m

    def quantum_id_list(self) -> list[bytes]:
        """Exact 16-byte quantum id per row (cached).

        Returns
        -------
        ids : `list` of `bytes`
            16-byte quantum id per row, in table order (empty for an
            empty table).
        """
        out = self._cache.get("ids")
        if out is None:
            matrix = self.quantum_id_matrix
            out = [bytes(matrix[i].tobytes()) for i in range(matrix.shape[0])]
            self._cache["ids"] = out
        return out

    # -- selection / merging --------------------------------------------------

    def select(self, sel: np.ndarray | Sequence[int]) -> QuantumRuntimeTable:
        """Return a new table with the selected rows.

        Parameters
        ----------
        sel : `~numpy.ndarray` or sequence of `int`
            Boolean mask (same length as ``n_rows``) or row indices.

        Returns
        -------
        table : `QuantumRuntimeTable`
            New table carrying the same bookkeeping metadata.
        """
        t = self._get_arrow()
        sel_arr = np.asarray(sel)
        if sel_arr.dtype == np.bool_:
            filtered = t.filter(
                pa.chunked_array([pa.array(sel_arr, type=pa.bool_())])
            )
        else:
            filtered = t.take(pa.array(sel_arr.astype(np.int64)))
        return type(self)(
            n_expected=self.n_expected,
            n_sources=self.n_sources,
            sources=self.sources,
            arrow=filtered,
        )

    @classmethod
    def merge_first_wins(
        cls,
        *tables: QuantumRuntimeTable,
        n_expected: int = 0,
        n_sources: int | None = None,
        sources: Sequence[str] | None = None,
    ) -> QuantumRuntimeTable:
        """Merge tables, first-wins on ``(task_label, data_id)``.

        Rows are scanned in table order (earliest table highest
        precedence), and the merged output preserves that first-seen row
        order.

        Parameters
        ----------
        *tables : `QuantumRuntimeTable`
            Ordered sources; earlier entries win on key collisions.
        n_expected : `int`, optional
            Expected quantum count for the merged table.  Default is 0.
        n_sources : `int`, optional
            Source count to record.  Default is the number of input
            tables.
        sources : `~collections.abc.Sequence` of `str`, optional
            Source paths to record.  Default is the concatenation of the
            input tables' ``sources``.

        Returns
        -------
        merged : `QuantumRuntimeTable`
        """
        label_cols = [t.labels() for t in tables]
        id_cols = [t.data_id_list() for t in tables]
        seen: dict[tuple[str, str], tuple[int, int]] = {}
        for ti, t in enumerate(tables):
            labs, ids = label_cols[ti], id_cols[ti]
            for r in range(t.n_rows):
                seen.setdefault((labs[r], ids[r]), (ti, r))

        picks: list[list[int]] = [[] for _ in tables]
        for ti, t in enumerate(tables):
            labs, ids = label_cols[ti], id_cols[ti]
            for r in range(t.n_rows):
                if seen.get((labs[r], ids[r])) == (ti, r):
                    picks[ti].append(r)

        parts = [
            t.to_arrow().take(pa.array(p, type=pa.int64())) if p
            else t.to_arrow().slice(0, 0)
            for t, p in zip(tables, picks, strict=True)
        ]
        combined = (
            pa.concat_tables(parts, unify=True)
            if parts else _rows_to_arrow([])
        )
        if sources is None:
            sources = tuple(s for t in tables for s in t.sources)
        return cls(
            n_expected=n_expected,
            n_sources=len(tables) if n_sources is None else n_sources,
            sources=tuple(sources),
            arrow=combined,
        )

    # -- Parquet cache --------------------------------------------------------

    def to_parquet(
        self,
        path: str | Path,
        *,
        sources: Sequence[str] | None = None,
        fingerprint: str | None = None,
    ) -> int:
        """Serialize this table to a Parquet cache file.

        Writes the canonical Arrow view plus a self-describing metadata
        block (schema version, package version, sources, row count,
        creation timestamp, fingerprint); the file can be read back with
        :meth:`from_parquet`, or merged with sibling caches via
        :meth:`from_parquets`.

        Parameters
        ----------
        path : `str` or `~pathlib.Path`
            Destination Parquet file path.
        sources : `~collections.abc.Sequence` of `str`, optional
            Source labels recorded in the file metadata.  Defaults to this
            table's ``sources`` tuple.
        fingerprint : `str` or ``None``, optional
            Run fingerprint for staleness diagnosis.  Default is ``None``
            (stored as an empty string).

        Returns
        -------
        n_rows : `int`
            Number of rows written.
        """
        from lsst.pipe.base.version import __version__

        table = self.to_arrow()
        metadata = {
            "schema_version": _CACHE_SCHEMA_VERSION,
            "runtime_analyzer_version": __version__,
            "sources": json.dumps(
                [str(s) for s in
                 (self.sources if sources is None else tuple(sources))]
            ),
            "n_rows": str(table.num_rows),
            "created_at": datetime.now(UTC).isoformat(),
            "fingerprint": "" if fingerprint is None else str(fingerprint),
        }
        table = table.replace_schema_metadata(pa.KeyValueMetadata(metadata))
        pq.write_table(table, str(Path(path)), compression="snappy")
        return int(table.num_rows)

    @classmethod
    def from_parquet(cls, path: str | Path) -> QuantumRuntimeTable:
        """Rebuild a table from a Parquet cache file.

        Inverse of :meth:`to_parquet`; reads only the Parquet file.
        ``n_expected`` is ``0`` and ``sources`` is ``(path,)``.

        Parameters
        ----------
        path : `str` or `~pathlib.Path`
            Path to a cache file written by :meth:`to_parquet`.

        Returns
        -------
        table : `QuantumRuntimeTable`
            The reconstructed table.

        Raises
        ------
        FileNotFoundError
            Raised if the cache file does not exist.
        ValueError
            Raised if the file lacks ``schema_version`` metadata, its major
            version is unsupported, a required column is missing, or a
            required column contains Parquet nulls.
        """
        path = Path(path)
        table = pq.read_table(str(path))

        raw = table.schema.metadata or {}
        raw_version = raw.get(b"schema_version", raw.get("schema_version"))
        schema_version = (raw_version.decode()
                          if isinstance(raw_version, bytes)
                          else raw_version)
        if schema_version is None:
            raise ValueError(
                f"Cache table {path!s} lacks schema_version metadata; only "
                f"runtime_analyzer runtime-table caches with schema major "
                f"version {_CACHE_SCHEMA_MAJOR_VERSION} are supported."
            )
        major_text = str(schema_version).split(".", 1)[0]
        try:
            major = int(major_text)
        except ValueError:
            major = -1
        if major != _CACHE_SCHEMA_MAJOR_VERSION:
            raise ValueError(
                f"Unsupported cache schema_version {schema_version!r} in "
                f"{path!s}; this version of runtime_analyzer supports "
                f"schema major version {_CACHE_SCHEMA_MAJOR_VERSION} only."
            )

        missing = [name for name in RUNTIME_SCHEMA.names
                   if name not in table.column_names]
        if missing:
            raise ValueError(
                f"Cache table {path!s} is missing required column(s): "
                f"{', '.join(missing)}."
            )

        for name in RUNTIME_SCHEMA.names:
            null_count = table.column(name).null_count
            if null_count:
                raise ValueError(
                    f"Cache table {path!s} has {null_count} null value(s) "
                    f"in required column {name!r}; refusing to load a "
                    f"corrupted cache file."
                )

        return QuantumRuntimeTable.from_arrow(
            table, n_sources=1, sources=(str(path),)
        )

    @classmethod
    def from_parquets(
        cls, paths: Sequence[str | Path]
    ) -> QuantumRuntimeTable:
        """Rebuild one table from multiple Parquet cache files.

        Reads each path with :meth:`from_parquet` and merges the rows
        with the same *first-wins* ``(task_label, data_id)`` precedence
        as :meth:`merge_first_wins`: the first file's row for a given
        key wins and quanta unique to later files are kept.
        ``dim_lookup`` entries are recorded for each surviving winner row
        only, taken from that winner's own cache file, so each surviving
        quantum keeps the dimensions of its winning row.  The merged
        ``n_expected`` is ``0`` (cache files store no expected count).

        Parameters
        ----------
        paths : `~collections.abc.Sequence` of `str` or `~pathlib.Path`
            Ordered cache Parquet paths (as written by :meth:`to_parquet`).
            The first entry has highest precedence.

        Returns
        -------
        table : `QuantumRuntimeTable`
            One merged table with the concatenated per-file ``sources``
            (the cache paths), ``n_sources=len(paths)``, and — for an
            empty ``paths`` sequence — the empty ``RUNTIME_SCHEMA``
            layout.

        Raises
        ------
        FileNotFoundError
            Raised if a cache file does not exist (propagated from
            :meth:`from_parquet`).
        ValueError
            Raised if a cache file lacks ``schema_version`` metadata, its
            major version is unsupported, a required column is missing,
            or a required column contains Parquet nulls (propagated from
            :meth:`from_parquet`; the per-column messages name the
            offending file).
        """
        tables = [cls.from_parquet(path) for path in paths]
        # Default merge metadata: sources concatenated from the per-file
        # (str(path),) tuples, n_sources=len(paths), n_expected=0.
        return cls.merge_first_wins(*tables)


# ---------------------------------------------------------------------------
# Graph -> table extraction (producer side).  These functions duck-type the
# pipe_base node objects (QuantumResourceUsage, QuantumAttemptStatus,
# DataCoordinate) that they are designed to read, and `ProvenanceQuantumGraph`
# itself is imported lazily — the module's only module-level third-party
# dependencies are numpy and pyarrow (an optional [runtime]-extra
# dependency, guarded at import).
# ---------------------------------------------------------------------------

# QuantumAttemptStatus member .value codes keyed by member name (the enum
# is not an IntEnum, so codes must be mapped explicitly).
STATUS_MAP: dict[str, int] = {
    'UNKNOWN': -3,
    'ABORTED_SUCCESS': -2,
    'FAILED': -1,
    'BLOCKED': 0,
    'SUCCESSFUL': 1,
    'ABORTED': -4,
}


# Invert the map for looking up names
INT_TO_STATUS = {v: k for k, v in STATUS_MAP.items()}


def _node_to_row(
    quantum_id: uuid.UUID,
    node_data: dict,
) -> tuple | None:
    """Extract a ``ROW_FIELDS``-ordered row tuple from a graph node.

    Parameters
    ----------
    quantum_id : `uuid.UUID`
        The quantum node key (unique id from the xgraph).
    node_data : `dict`
        Node data dict from ``quantum_only_xgraph`` nodes.

    Returns
    -------
    row : `tuple` or ``None``
        A row tuple ordered as ``ROW_FIELDS``, or ``None`` if the node
        lacks usable resource usage.
    """
    ru = node_data.get("resource_usage")
    if ru is None:
        return None

    task_label = node_data.get("task_label", "Unknown")
    status = node_data.get("status", None)
    # Unmapped or missing status is UNKNOWN, not FAILED.
    if status is None:
        status_code = STATUS_MAP['UNKNOWN']
    elif hasattr(status, 'name'):
        # Enum (e.g. lsst.pipe.base.QuantumAttemptStatus) or named value.
        status_code = STATUS_MAP.get(status.name, STATUS_MAP['UNKNOWN'])
    elif isinstance(status, int):
        # Raw int status code (e.g. from a hand-built node dict): keep it
        # if INT_TO_STATUS recognizes it, else fall back to UNKNOWN.
        status_code = int(status) if int(status) in INT_TO_STATUS else STATUS_MAP['UNKNOWN']
    else:
        # Plain string status name.
        status_code = STATUS_MAP.get(str(status), STATUS_MAP['UNKNOWN'])

    data_id = node_data.get("data_id")
    data_id_str = str(data_id) if data_id else ""

    row = (
        task_label,
        quantum_id.bytes,
        status_code,
        float(ru.memory) / (1024 * 1024),
        float(ru.prep_time),
        float(ru.init_time),
        float(ru.run_time),
        float(ru.run_time_cpu),
        data_id_str,
    )
    return row


def _format_dimension_value(value: Any) -> str:
    """Format a single DataCoordinate dimension value as a string.

    Parameters
    ----------
    value : `Any`
        A dimension value (e.g., an `lsst.daf.butler.DimensionValue`, an
        `int`, or a sequence of values).

    Returns
    -------
    formatted : `str`
        String representation; sequences are joined with ",".
    """
    if isinstance(value, str):
        return value
    if isinstance(value, np.ndarray):
        return _join_sorted(value.tolist())
    if isinstance(value, (list, tuple, set, frozenset)):
        return _join_sorted(value)
    return str(value)


def _join_sorted(values: Any) -> str:
    """Join sequence values with "," in sorted order.

    Falls back to string sort when element types are not mutually
    comparable (mixed ints and strings raise ``TypeError`` under
    ``sorted``).
    """
    try:
        ordered = sorted(values)
    except TypeError:
        ordered = sorted(str(v) for v in values)
    return ",".join(str(v) for v in ordered)


def _node_dimensions(node_data: dict) -> dict[str, str]:
    """Capture dimension -> value pairs for a quantum graph node.

    Prefers the real ``lsst.daf.butler.DataCoordinate`` object stored on the
    node.  A real ``DataCoordinate`` has no ``items()`` method; iterate its
    ``.mapping`` view (a ``Mapping`` exposing ``items()``) instead.  Falls back
    to ``items()`` for plain mapping-like mocks, then to robust parsing of the
    ``str(data_id)`` representation (colon format ``"{visit: 1, band: 'g'}"``).

    Parameters
    ----------
    node_data : `dict`
        Node data dict from ``quantum_only_xgraph`` nodes.

    Returns
    -------
    dimensions : `dict` [ `str`, `str` ]
        Mapping from dimension key to stringified value.  May be empty if
        the node has no data_id.
    """
    data_id = node_data.get("data_id")
    if data_id is None:
        return {}

    # Real DataCoordinate: iterate .mapping (has items(); coord does not).
    mapping = getattr(data_id, "mapping", None)
    if mapping is not None:
        try:
            captured = {
                str(k): _format_dimension_value(v) for k, v in mapping.items()
            }
            if captured:
                return captured
        except (AttributeError, TypeError):
            # A Mock.mapping.items() may not be a real mapping; fall through.
            pass

    items = getattr(data_id, "items", None)
    if callable(items):
        try:
            return {str(k): _format_dimension_value(v) for k, v in items()}
        except TypeError:
            # Mocks and non-mapping objects raise here; fall back to parsing.
            pass

    from .dimensions import parse_dimension_string

    return parse_dimension_string(str(data_id))


def _header_n_expected(qg: Any) -> int:
    """Sum a graph header's per-task quanta counts, defensively.

    Parameters
    ----------
    qg : `object`
        A ``ProvenanceQuantumGraph``-like object exposing
        ``header.n_task_quanta``.

    Returns
    -------
    n : `int`
        ``sum(qg.header.n_task_quanta.values())`` when the header exposes
        a mapping of per-task quanta counts; ``0`` otherwise (partially
        built or header-less graph objects), matching the
        ``QuantumRuntimeTable`` "not derivable" convention.
    """
    n_task_quanta = getattr(getattr(qg, "header", None), "n_task_quanta", None)
    if isinstance(n_task_quanta, Mapping):
        return sum(n_task_quanta.values())
    return 0


def extract_runtime_table(qg: Any) -> QuantumRuntimeTable:
    """Extract a runtime table from an already-loaded quantum graph.

    Performs a single pass over ``qg.quantum_only_xgraph`` nodes, building
    the Arrow runtime table and the dimension lookup.  No
    files, Butler, or ``ProvenanceQuantumGraph.from_args`` access happens
    here — the graph must already be loaded.

    Parameters
    ----------
    qg : `object`
        An already-loaded ``ProvenanceQuantumGraph``-like object exposing
        ``quantum_only_xgraph`` and ``header``.

    Returns
    -------
    table : `QuantumRuntimeTable`
        Table with one row per quantum that has usable resource usage, a
        ``dims`` column captured from each node's data ID, ``n_expected``
        summed from the graph header, ``n_sources=1``, and empty
        ``sources``.
    """
    xgraph = qg.quantum_only_xgraph

    rows: list[tuple[Any, ...]] = []
    dim_lookup: dict[bytes, dict[str, str]] = {}
    for quantum_id, node_data in xgraph.nodes(data=True):
        row = _node_to_row(quantum_id, node_data)
        if row is not None:
            rows.append(row)
            dim_lookup[quantum_id.bytes] = _node_dimensions(node_data)

    return QuantumRuntimeTable(
        n_expected=_header_n_expected(qg),
        n_sources=1,
        sources=(),
        arrow=_rows_to_arrow(rows, dim_lookup),
    )


def extract_merged_runtime_table(
    sources: list[tuple[str, str | None]],
) -> QuantumRuntimeTable:
    """Extract a merged runtime table from an ordered list of graph sources.

    Each source is opened via ``ProvenanceQuantumGraph.from_args`` (the
    only place in the package that opens graphs for analysis) and its
    nodes are merged with *first-wins* precedence on
    ``(task_label, data_id)`` (keyed by the formatted data-ID string, the
    same key as :meth:`QuantumRuntimeTable.merge_first_wins`): the first source
    with a usable (resource-usage-bearing) row for a given key prevails,
    and quanta unique to later sources are included.  Exactly one source
    takes a fast path that skips the merge machinery entirely.

    Parameters
    ----------
    sources : `list` of `(str, str or None)`
        Ordered list of graph sources.  Each entry is a ``(path, collection)``
        tuple where ``collection`` is ``None`` for file-based sources and a
        string for Butler collections.  The first entry has highest
        precedence.

    Returns
    -------
    table : `QuantumRuntimeTable`
        Merged table with the combined dimension view (captured per winner
        row in the ``dims`` column), ``n_sources`` equal to
        ``len(sources)``, and ``sources`` set to the source paths.
        ``n_expected`` is the header count for a single source, and ``0``
        when merging multiple sources, whose header sums would
        double-count shared quanta.
    """
    from lsst.pipe.base.quantum_graph import ProvenanceQuantumGraph

    if len(sources) == 1:
        # Single source: no merging to do; stamp the source path onto the
        # freshly extracted table.
        path, collection = sources[0]
        with ProvenanceQuantumGraph.from_args(
            path, collection=collection, datasets=()
        ) as (
            qg,
            _butler,
        ):
            table = extract_runtime_table(qg)
        return dataclasses.replace(table, sources=(str(path),))

    tables: list[QuantumRuntimeTable] = []
    for path, collection in sources:
        with ProvenanceQuantumGraph.from_args(
            path, collection=collection, datasets=()
        ) as (
            qg,
            _butler,
        ):
            tables.append(extract_runtime_table(qg))

    # Header counts are not summed across sources: the sum would
    # double-count quanta shared between sources, so merged multi-source
    # tables report ``n_expected=0`` (the merge default).
    return QuantumRuntimeTable.merge_first_wins(
        *tables,
        sources=tuple(str(path) for path, _ in sources),
    )
