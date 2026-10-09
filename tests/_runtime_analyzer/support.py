"""Shared construction helpers for the runtime_analyzer test suites.

Single home for every test-data builder used by
``tests/_runtime_analyzer`` and
``tests/_runtime_analyzer/tracker``: row tuples,
``QuantumRuntimeTable``/``QuantumRuntimeAnalyzer`` factories, Parquet
cache writers, mock quantum graphs and resource-usage objects, click
CLI invokers, tracker DB record helpers and plot fixtures.

The module is intentionally *free* of pytest fixture machinery: every
public helper is a plain function (or class) importable from both test
packages; conftest modules wrap them in thin fixtures.

Builders that previously lived as near-twins with subtly different
defaults are kept as separate functions (``row`` vs ``quantum_row``,
``db_task_row`` vs ``history_task_row``) rather than merged, so call
sites keep byte-identical data.
"""

from __future__ import annotations

import datetime
import random
import time
import uuid
from collections.abc import Callable, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest import mock

import numpy as np
import pytest

pytest.importorskip("pyarrow")  # provided by the [runtime] extra

from lsst.pipe.base._runtime_analyzer.core import QuantumRuntimeAnalyzer  # noqa: E402
from lsst.pipe.base._runtime_analyzer.runtime_table import (  # noqa: E402
    STATUS_MAP,
    QuantumRuntimeTable,
)
from lsst.pipe.base._runtime_analyzer.tracker.database import record_run  # noqa: E402
from lsst.pipe.base.resource_usage import QuantumResourceUsage  # noqa: E402

# Reference instant and MiB factor used by ``real_usage`` (mirrors the
# integration test module constants).
_START = datetime.datetime(2026, 1, 1, 12, 0, 0)
_MIB = 1024 * 1024


# ---------------------------------------------------------------------------
# Row tuple builders (ROW_FIELDS order consumed by from_rows).
# ---------------------------------------------------------------------------


def row(
    task_label: str,
    run_time: float,
    *,
    memory: float = 100.0,
    status: str = "SUCCESSFUL",
    data_id: str = "{band='g'}",
    quantum_int: int = 1,
    run_time_cpu: float | None = None,
    prep_time: float = 1.0,
    init_time: float = 1.0,
) -> tuple:
    """Build a single ROW_FIELDS-ordered row tuple with defaults.

    The quantum id is ``uuid.UUID(int=quantum_int)``; ``run_time_cpu``
    defaults to half of ``run_time``.
    """
    if run_time_cpu is None:
        run_time_cpu = run_time * 0.5
    qid = uuid.UUID(int=quantum_int)
    return (
        task_label,
        qid.bytes,
        STATUS_MAP[status],
        memory,
        prep_time,
        init_time,
        run_time,
        run_time_cpu,
        data_id,
    )


def quantum_row(
    task_label: str,
    qid: uuid.UUID,
    run_time: float,
    *,
    memory: float = 100.0,
    status: str = "SUCCESSFUL",
    data_id: str = "{band='g'}",
    prep_time: float = 1.0,
    init_time: float = 2.0,
    run_time_cpu: float | None = None,
) -> tuple:
    """ROW_FIELDS-ordered row tuple from an explicit ``uuid.UUID`` id.

    Cache-round-trip twin of :func:`row`, kept separate because its
    ``init_time`` default (2.0) differs.
    """
    if run_time_cpu is None:
        run_time_cpu = run_time * 0.5
    return (
        task_label,
        qid.bytes,
        STATUS_MAP[status],
        memory,
        prep_time,
        init_time,
        run_time,
        run_time_cpu,
        data_id,
    )


def status_rows() -> list[tuple]:
    """One row per mapped status for the filter-semantics tests."""
    return [
        row("T", 10.0, status="FAILED", quantum_int=1),
        row("T", 10.0, status="ABORTED", quantum_int=2),
        row("T", 10.0, status="ABORTED_SUCCESS", quantum_int=3),
        row("T", 10.0, status="SUCCESSFUL", quantum_int=4),
        row("T", 10.0, status="BLOCKED", quantum_int=5),
        row("T", 10.0, status="UNKNOWN", quantum_int=6),
    ]


# ---------------------------------------------------------------------------
# Table / analyzer factories.
# ---------------------------------------------------------------------------


def make_table(
    rows: Sequence[tuple[Any, ...]],
    dim_lookup: dict[bytes, dict[str, str]] | None = None,
    *,
    n_expected: int = 0,
    n_sources: int = 1,
    sources: Sequence[str] = (),
) -> QuantumRuntimeTable:
    """Build a ``QuantumRuntimeTable`` via ``from_rows``."""
    return QuantumRuntimeTable.from_rows(
        rows,
        dim_lookup,
        n_expected=n_expected,
        n_sources=n_sources,
        sources=sources,
    )


def make_analyzer(
    rows: list[tuple],
    dim_lookup: dict[bytes, dict[str, str]] | None = None,
    n_sources: int = 1,
    n_expected: int = 0,
) -> QuantumRuntimeAnalyzer:
    """Build a real analyzer backed by a ``from_rows`` table."""
    return QuantumRuntimeAnalyzer(
        QuantumRuntimeTable.from_rows(
            rows, dim_lookup, n_expected=n_expected,
            n_sources=n_sources,
        )
    )


def make_task_analyzer(
    rows: list[tuple[str, float]],
    n_sources: int = 1,
) -> QuantumRuntimeAnalyzer:
    """Build an analyzer from ``(task_label, run_time)`` pairs.

    Each row gets a fresh uuid4 ``quantum_id``, SUCCESSFUL status and
    plausible memory/CPU values, so structurally identical call
    arguments never share quantum ids.
    """
    data = [
        (task_label, uuid.uuid4().bytes, 1, 128.0, 1.0, 1.0, run_time,
         run_time * 0.9, "{}")
        for task_label, run_time in rows
    ]
    return QuantumRuntimeAnalyzer(
        QuantumRuntimeTable.from_rows(data, n_sources=n_sources)
    )


def cache_table(
    spec: list[tuple[str, float, str, int]],
    dim_lookup: dict[bytes, dict[str, str]] | None = None,
) -> QuantumRuntimeTable:
    """Build a table from ``(task_label, run_time, data_id,
    quantum_int)`` cache spec rows.
    """
    rows = [
        (task_label, uuid.UUID(int=quantum_int).bytes, 1, 128.0, 1.0,
         1.0, run_time, run_time * 0.9, data_id)
        for task_label, run_time, data_id, quantum_int in spec
    ]
    return QuantumRuntimeTable.from_rows(rows, dim_lookup)


def write_cache(
    tmp_path: Path,
    name: str,
    spec: list[tuple[str, float, str, int]],
    dims: dict[bytes, dict[str, str]] | None = None,
    sources: tuple[str, ...] | None = None,
) -> str:
    """Write a Parquet cache from spec rows into ``tmp_path``.

    Saves the table built from ``spec`` to ``tmp_path / name``
    (asserting the written row count) and returns the path as a
    ``str``.  ``sources=None`` keeps the table's own empty ``sources``.
    """
    path = tmp_path / name
    kwargs: dict[str, Any] = {} if sources is None else {"sources": sources}
    n = cache_table(spec, dims).to_parquet(str(path), **kwargs)
    assert n == len(spec)
    return str(path)


def write_task_cache(path: str | Path, rows: list[tuple[str, float]]) -> int:
    """Write a per-quantum Parquet cache from ``(task, run_time)`` rows;
    return the number of rows written.

    Rows get fresh uuid4 ids and unique ``{visit=N}`` data ids so they
    never collide on the ``(task_label, data_id)`` merge key; the file
    metadata records the destination file name as its source.
    """
    table_rows = [
        (task_label, uuid.uuid4().bytes, 1, 128.0, 1.0, 1.0, run_time,
         run_time * 0.9, f"{{visit={i}}}")
        for i, (task_label, run_time) in enumerate(rows)
    ]
    qt = QuantumRuntimeTable.from_rows(table_rows, sources=(Path(path).name,))
    return qt.to_parquet(str(path))


# ---------------------------------------------------------------------------
# Mock quantum graphs / resource usage.
# ---------------------------------------------------------------------------


def make_qg(
    task_data: dict[str, list[dict]],
    n_task_quanta: dict[str, int] | None = None,
) -> mock.Mock:
    """Create a mock ProvenanceQuantumGraph for testing.

    ``task_data`` maps task_label to quantum attribute dicts (``id``,
    ``status``, ``data_id``, ``resource_usage``); ``n_task_quanta``
    overrides the header mapping (default: label -> len(quanta)).
    """
    # Build quantum nodes for quantum_only_xgraph
    nodes = []
    for task_label, quanta_list in task_data.items():
        for qattrs in quanta_list:
            qid = qattrs.get('id', uuid.uuid4())
            node = {
                'task_label': task_label,
                'status': qattrs.get('status', 'SUCCESSFUL'),
                'data_id': qattrs.get('data_id', 'visit=1,filter=g'),
            }
            ru = qattrs.get('resource_usage')
            if ru is not None:
                node['resource_usage'] = ru
            nodes.append((qid, node))

    # Create a callable mock that returns the node iterator
    xgraph = mock.Mock()
    xgraph.nodes = mock.MagicMock(side_effect=lambda **kwargs: iter(nodes))

    qg = mock.Mock()
    qg.quantum_only_xgraph = xgraph
    qg.header = mock.Mock()
    if n_task_quanta is not None:
        qg.header.n_task_quanta = n_task_quanta
    else:
        qg.header.n_task_quanta = {tl: len(qu) for tl, qu in task_data.items()}

    return qg


def make_ru(
    memory: float = 1e9,
    prep_time: float = 10.0,
    init_time: float = 5.0,
    run_time: float = 100.0,
    run_time_cpu: float = 80.0,
) -> mock.Mock:
    """Create a mock QuantumResourceUsage object."""
    ru = mock.Mock()
    ru.memory = memory
    ru.prep_time = prep_time
    ru.init_time = init_time
    ru.run_time = run_time
    ru.run_time_cpu = run_time_cpu
    return ru


def fake_usage(run_time: float) -> mock.Mock:
    """Mock QuantumResourceUsage with 1 s prep/init and CPU at half the
    wall time (memory 1e9).
    """
    ru = mock.Mock()
    ru.memory = 1e9
    ru.prep_time = 1.0
    ru.init_time = 1.0
    ru.run_time = run_time
    ru.run_time_cpu = run_time * 0.5
    return ru


def real_usage(
    run_time: float,
    memory_mib: float,
    *,
    run_time_cpu: float | None = None,
    prep_time: float = 1.0,
    init_time: float = 2.0,
) -> QuantumResourceUsage:
    """Build a real ``QuantumResourceUsage`` from hand-picked numbers."""
    if run_time_cpu is None:
        run_time_cpu = run_time * 0.8
    return QuantumResourceUsage(
        memory=memory_mib * _MIB,  # bytes, as in real graphs
        start=_START,
        prep_time=prep_time,
        init_time=init_time,
        run_time=run_time,
        run_time_cpu=run_time_cpu,
    )


def graph_node(
    qid_int: int,
    task_label: str,
    run_time: float,
    data_id: str,
) -> tuple:
    """SUCCESSFUL ``(quantum_uuid, node_data)`` tuple with plausible
    usage for the fake ``from_args`` loader.
    """
    return (uuid.UUID(int=qid_int), {
        'task_label': task_label,
        'status': 'SUCCESSFUL',
        'data_id': data_id,
        'resource_usage': fake_usage(run_time),
    })


def merge_node(
    qid_int: int,
    data_id: Any,
    run_time: float,
) -> tuple:
    """One graph node tuple with plausible resource usage for merge
    tests (fixed ``TaskT``/SUCCESSFUL; ``data_id`` may be any object,
    e.g. a value-equal DataCoordinate stand-in).
    """
    ru = mock.Mock()
    ru.memory = 1e9
    ru.prep_time = 1.0
    ru.init_time = 1.0
    ru.run_time = run_time
    ru.run_time_cpu = run_time * 0.5
    return uuid.UUID(int=qid_int), {
        "task_label": "TaskT",
        "status": "SUCCESSFUL",
        "data_id": data_id,
        "resource_usage": ru,
    }


class _FakeDataId:
    """Minimal DataCoordinate stand-in with *value*-based equality/hash.

    Assigning ``__hash__``/``__eq__`` on a Mock instance is inert (special
    methods are looked up on the type), so the merge-key usage here needs a
    real class: two distinct-but-equal instances must collide in the
    ``extract_merged_runtime_table`` merge map.
    """

    def __init__(self, **dims) -> None:
        self._dims = dict(sorted(dims.items()))

    def items(self):
        return self._dims.items()

    def __hash__(self) -> int:
        return hash(tuple(self._dims.items()))

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _FakeDataId) and self._dims == other._dims

    def __repr__(self) -> str:
        return "{" + ", ".join(f"{k}={v!r}" for k, v in self._dims.items()) + "}"


FakeDataId = _FakeDataId


def boom(*args: Any, **kwargs: Any) -> None:
    """Explode if a graph/Butler seam is touched for cached input."""
    raise AssertionError(
        "graph/Butler load attempted for cached-table input"
    )


def fake_from_args(
    node_map: dict[str, list[tuple]],
    *,
    header_task: str | None = None,
    calls: list[dict] | None = None,
) -> Callable:
    """Build a ``ProvenanceQuantumGraph.from_args`` stand-in.

    ``node_map`` maps a source string (path, or ``"repo:collection"``
    for Butler entries) to the node tuples that source yields; the
    stack-context yields ``(qg, butler)`` exactly like the real
    ``from_args``.  ``header_task`` sets
    ``n_task_quanta={header_task: len(nodes)}``; ``calls`` records
    ``{"path", "collection", "datasets"}`` per invocation.
    """

    @contextmanager
    def _from_args(path, collection=None, datasets=None):
        key = str(path) if collection is None \
            else f"{path}:{collection}"
        if calls is not None:
            calls.append({"path": path, "collection": collection,
                          "datasets": datasets})
        nodes = node_map[key]
        xg = mock.Mock()
        xg.nodes = mock.MagicMock(side_effect=lambda **kw: iter(nodes))
        qg = mock.Mock()
        qg.quantum_only_xgraph = xg
        if header_task is not None:
            qg.header = mock.Mock()
            qg.header.n_task_quanta = {header_task: len(nodes)}
        yield qg, mock.Mock()

    return _from_args


def override_graph_loader(node_map: dict[str, list[tuple]]) -> mock._patch:
    """Patch ``ProvenanceQuantumGraph.from_args`` with ``node_map``.

    ``extract_merged_runtime_table`` imports ``ProvenanceQuantumGraph``
    lazily, so patching the class attribute is required.
    """
    from lsst.pipe.base.quantum_graph import ProvenanceQuantumGraph

    return mock.patch.object(
        ProvenanceQuantumGraph, 'from_args',
        side_effect=fake_from_args(node_map),
    )


def make_cli_ctx(
    graph: Sequence[str] = (),
    repo: str | None = None,
    collections: Sequence[str] = (),
) -> mock.Mock:
    """Build a minimal stand-in for the click context."""
    ctx = mock.Mock()
    ctx.obj = {
        'graph': tuple(graph),
        'repo': repo,
        'collection': collections[0] if collections else None,
        'task': None,
        '_collections': tuple(collections),
    }
    return ctx


def invoke_cli(args: Sequence[str], runner: Any | None = None):
    """Invoke the runtime-analyzer CLI's ``main`` group via click's
    ``CliRunner`` (a fresh runner is created unless one is given).

    Returns a ``click.testing.Result``.
    """
    from click.testing import CliRunner

    from lsst.pipe.base._runtime_analyzer.cli import main

    if runner is None:
        runner = CliRunner()
    return runner.invoke(main, args)


@contextmanager
def captured_graph_loads(analyzer):
    """Patch ``cli.extract_merged_runtime_table`` to capture its calls.

    The producer returns ``analyzer.table`` and each invocation's
    ``sources`` argument is appended (as a tuple) to the yielded list,
    so tests can assert load order without duplicating the patch
    boilerplate per test.

    Parameters
    ----------
    analyzer : `QuantumRuntimeAnalyzer`
        Analyzer whose ``table`` every (graph/Butler) load returns.

    Yields
    ------
    captured : `list` of `tuple`
        One entry per producer call: the ordered ``(path, collection)``
        pairs the CLI passed.
    """
    from lsst.pipe.base._runtime_analyzer import cli

    captured: list = []

    def record_load(sources):
        captured.append(tuple(sources))
        return analyzer.table

    with mock.patch.object(cli, 'extract_merged_runtime_table',
                           side_effect=record_load):
        yield captured


def record_scenario_cli(
    analyzer,
    args: Sequence[str],
    runner: Any | None = None,
):
    """Invoke ``tracked-run record`` with load/save/auto-compare stubbed.

    ``extract_merged_runtime_table`` returns ``analyzer.table``,
    ``record_run`` captures its kwargs (and reports
    ``{'quanta': analyzer.n_loaded, 'tasks': 1}``), and the
    ``_auto_compare`` seam is neutralised, so the CLI runs its record
    path end-to-end without a graph, Butler or database.

    Parameters
    ----------
    analyzer : `QuantumRuntimeAnalyzer`
        Backing analyzer for the recorded run.
    args : `sequence` of `str`
        Full CLI argument list (group flags + ``tracked-run record ...``).
    runner : object, optional
        Click test runner (a fresh one is created when `None`).

    Returns
    -------
    result : `click.testing.Result`
        The CLI invocation result.
    recorded : `dict`
        Keyword arguments handed to ``record_run``.
    load_mock : `unittest.mock.MagicMock`
        The patched producer, for call assertions.
    """
    from lsst.pipe.base._runtime_analyzer import cli

    recorded: dict = {}

    def fake_record_run(**kwargs):
        recorded.update(kwargs)
        return {'quanta': int(analyzer.n_loaded), 'tasks': 1}

    with mock.patch.object(cli, 'extract_merged_runtime_table',
                           return_value=analyzer.table) as load_mock, \
            mock.patch('lsst.pipe.base._runtime_analyzer.cli.record_run',
                       side_effect=fake_record_run), \
            mock.patch('lsst.pipe.base._runtime_analyzer.tracker.history.'
                       '_auto_compare'):
        result = invoke_cli(args, runner=runner)
    return result, recorded, load_mock


# ---------------------------------------------------------------------------
# Exact-comparison assertion helpers.
# ---------------------------------------------------------------------------


def _assert_table_identical(actual, expected) -> None:
    """Assert two astropy Tables are *exactly* identical.

    Exact equality (not approx) is intentional: both sides are computed from
    bit-identical float32 inputs, so any divergence indicates the cache
    round-trip altered the data.
    """
    import astropy.table

    assert isinstance(actual, astropy.table.Table)
    assert isinstance(expected, astropy.table.Table)
    assert list(actual.colnames) == list(expected.colnames)
    assert len(actual) == len(expected)
    for col in actual.colnames:
        a = np.asarray(actual[col])
        b = np.asarray(expected[col])
        assert np.array_equal(a, b), f"column {col!r} differs:\n{a}\n{b}"


def _assert_table_dicts_identical(actual: dict, expected: dict) -> None:
    assert set(actual.keys()) == set(expected.keys())
    for key in expected:
        _assert_table_identical(actual[key], expected[key])


def schema_probe_table():
    """Build the canonical single-row Arrow runtime table used by the
    schema-version guard tests (metadata-free; callers stamp their own
    ``schema_version`` before writing).
    """
    import pyarrow as pa

    schema = pa.schema([
        ("task_label", pa.string()),
        ("quantum_id", pa.binary(16)),
        ("status", pa.int8()),
        ("memory", pa.float32()),
        ("prep_time", pa.float32()),
        ("init_time", pa.float32()),
        ("run_time", pa.float32()),
        ("run_time_cpu", pa.float32()),
        ("data_id", pa.string()),
        ("dims", pa.map_(pa.string(), pa.string())),
    ])
    return pa.Table.from_arrays(
        [
            pa.array(["T"], type=pa.string()),
            pa.array([b"\x00" * 16], type=pa.binary(16)),
            pa.array([1], type=pa.int8()),
            pa.array([1.0], type=pa.float32()),
            pa.array([1.0], type=pa.float32()),
            pa.array([1.0], type=pa.float32()),
            pa.array([1.0], type=pa.float32()),
            pa.array([1.0], type=pa.float32()),
            pa.array(["{band='g'}"], type=pa.string()),
            pa.array([{}], type=pa.map_(pa.string(), pa.string())),
        ],
        schema=schema,
    )


# ---------------------------------------------------------------------------
# Mock analyzers (synthetic-table backed) used by plot/bug regression tests.
# ---------------------------------------------------------------------------


class MockAnalyzer:
    """Mock QuantumRuntimeAnalyzer backed by a synthetic `QuantumRuntimeTable`.

    Generates deterministic pseudo-random quanta and delegates every
    analyzer call to a real :class:`QuantumRuntimeAnalyzer` built from the
    table, so plots and assertions run against the genuine analysis code
    with synthetic data.  ``rng_seed`` defaults to 42.
    """

    def __init__(
        self,
        n_quanta: int = 50,
        n_tasks: int = 3,
        dim_cardinality: int = 5,
        rng_seed: int | None = None,
    ) -> None:
        self._rng = random.Random(rng_seed) if rng_seed is not None else random.Random(42)
        rows, dim_lookup = self._generate_rows(
            n_quanta, n_tasks, dim_cardinality,
        )
        self._table = QuantumRuntimeTable.from_rows(
            rows, dim_lookup, n_expected=len(rows),
        )
        self._analyzer = QuantumRuntimeAnalyzer(self._table)

    @property
    def table(self) -> QuantumRuntimeTable:
        return self._table

    def __getattr__(self, name: str):
        """Delegate analyzer API (summary, bottleneck, n_loaded, ...) to
        the real analyzer backing the synthetic table.
        """
        return getattr(self._analyzer, name)

    def _generate_rows(
        self,
        n_quanta: int,
        n_tasks: int,
        dim_cardinality: int,
    ) -> tuple[list[tuple], dict[bytes, dict[str, str]]]:
        """Generate synthetic ``(row, dims)`` data."""
        if n_quanta <= 0 or n_tasks <= 0:
            return [], {}

        # chr(65 + i) would spill into non-ASCII control characters for
        # n_tasks > 26; fall back to plain numeric names past 'Z'.
        task_names = [f'Task{chr(65 + i)}' if i < 26 else f'Task{i}'
                      for i in range(n_tasks)]

        rows: list[tuple] = []
        dim_lookup: dict[bytes, dict[str, str]] = {}
        per_task = n_quanta // n_tasks
        counter = 0
        for ti, tl in enumerate(task_names):
            for qi in range(per_task + (1 if ti < n_quanta % n_tasks else 0)):
                qid_int = counter
                counter += 1
                visit = self._rng.randint(1, dim_cardinality)
                filt = self._rng.choice(['g', 'r', 'i', 'z', 'y'])
                # Real str(DataCoordinate) uses colon format with quoted
                # string values; match it to avoid mock/real format drift.
                data_id_str = f'{{visit: {visit}, filter: {filt!r}}}'
                rt = self._rng.uniform(10, 200)
                rtc = rt * self._rng.uniform(0.3, 0.95)
                qid_bytes = qid_int.to_bytes(16, 'big')[:16]
                rows.append((
                    tl,
                    qid_bytes,
                    1,  # SUCCESSFUL status
                    self._rng.uniform(100, 5000),  # MiB
                    self._rng.uniform(1, 30),  # prep
                    self._rng.uniform(1, 20),  # init
                    rt,
                    rtc,
                    data_id_str,
                ))
                dim_lookup[qid_bytes] = {'visit': str(visit), 'filter': filt}
        return rows, dim_lookup


# ---------------------------------------------------------------------------
# Tracker DB mock shapes (hash_graph probing) and record helpers.
# ---------------------------------------------------------------------------


class FakeProvenanceGraph:
    """Mimics pipe_base ProvenanceQuantumGraph.quanta_by_task."""

    def __init__(self, quanta_by_task):
        self.quanta_by_task = quanta_by_task


class FakeTable:
    """Mimics a QuantumRuntimeTable's ``labels()`` accessor."""

    def __init__(self, labels):
        self._labels = list(labels)

    def labels(self):
        return self._labels


class FakeAnalyzer:
    """Mimics an analyzer exposing a QuantumRuntimeTable-like ``table``."""

    def __init__(self, labels):
        self.table = FakeTable(labels)


def db_task_row(task_label: str = "task_a", **over: Any) -> dict:
    """One summary-table row with the classic 10-quantum ``task_a``
    field set; ``over`` overrides individual fields.
    """
    row_ = {
        "task_label": task_label, "quanta": 10, "mean_rt": 45.0,
        "p05": 30.0, "p25": 40.0, "p50": 45.0, "p75": 50.0,
        "p95": 60.0, "max_rt": 70.0, "min_rt": 25.0, "std_rt": 8.0,
        "mean_mem": 100.0, "median_mem": 95.0, "max_mem": 150.0,
        "mean_io_pct": 0.1, "total_rt": 450.0,
    }
    row_.update(over)
    return row_


def db_record(label, run_id, summary_table=(), raw_quanta_data=None,
              repo=None, collection=None, graph_hash="gh",
              analyzer_version="0.1.0"):
    """``record_run`` with the boilerplate defaults used across the CRUD
    tests (same semantics as the direct keyword calls).
    """
    return record_run(
        label=label, run_id=run_id, repo=repo, collection=collection,
        graph_hash=graph_hash, analyzer_version=analyzer_version,
        summary_table=summary_table, raw_quanta_data=raw_quanta_data,
    )


def db_run_row(db: str, label: str):
    """Return the stored ``(repo, collection)`` pair for ``label`` from
    the tracker DB at ``db`` (``None`` when the label is absent).

    Parameters
    ----------
    db : `str`
        Path to the SQLite tracker database (the ``db_path`` fixture).
    label : `str`
        Run label to look up in the ``runs`` table.

    Returns
    -------
    row : `tuple` or `None`
        ``(repo, collection)`` as stored, or ``None`` when no run with
        ``label`` exists.
    """
    import sqlite3

    conn = sqlite3.connect(db)
    try:
        return conn.execute(
            "SELECT repo, collection FROM runs WHERE label = ?", (label,)
        ).fetchone()
    finally:
        conn.close()


def raw_quantum_rows() -> list[dict]:
    """Return the two-row calibrate ``raw_quanta_data`` list shared by the
    raw-storage and staleness tests.
    """
    return [
        {"task_label": "calibrate", "quantum_id": b"q1", "status": 1,
         "memory": 10.0, "prep_time": 1.0, "init_time": 2.0,
         "run_time": 45.0, "run_time_cpu": 40.0, "data_id": "{v=1}"},
        {"task_label": "calibrate", "quantum_id": b"q2", "status": 1,
         "memory": 12.0, "prep_time": 1.0, "init_time": 2.0,
         "run_time": 55.0, "run_time_cpu": 41.0, "data_id": "{v=2}"},
    ]


def two_quantum_summary(p50: float) -> list[dict]:
    """Return a one-task ``calibrate`` summary (2 quanta) whose percentile
    fields are derived from ``p50`` (raw-staleness update tests).
    """
    return [{
        "task_label": "calibrate", "quanta": 2, "mean_rt": p50,
        "p05": p50 - 5.0, "p25": p50 - 2.0, "p50": p50,
        "p75": p50 + 2.0, "p95": p50 + 5.0, "max_rt": p50 + 5.0,
        "min_rt": p50 - 5.0, "std_rt": 5.0, "mean_mem": 11.0,
        "median_mem": 11.0, "max_mem": 12.0, "mean_io_pct": 0.1,
        "total_rt": p50 * 2,
    }]


def history_task_row(task_label: str, **over: Any) -> dict:
    """One summary-table row with the 100-quantum history-test field
    set; ``over`` overrides individual fields.
    """
    base = {
        "task_label": task_label, "quanta": 100, "mean_rt": 45.0,
        "p05": 30.0, "p25": 40.0, "p50": 45.0, "p75": 50.0,
        "p95": 60.0, "max_rt": 70.0, "min_rt": 25.0, "std_rt": 8.0,
        "mean_mem": 100.0, "median_mem": 95.0, "max_mem": 150.0,
        "mean_io_pct": 0.1, "total_rt": 4500.0,
    }
    base.update(over)
    return base


def history_record(label, rows, graph_hash="g", repo=None, collection=None,
                   raw_quanta_data=None):
    """``record_run`` (``run_id=label``) with the boilerplate the history
    tests repeat, plus the 10 ms gap that keeps run timestamps strictly
    ordered.
    """
    record_run(
        label=label, run_id=label, repo=repo, collection=collection,
        graph_hash=graph_hash, analyzer_version="0.1.0",
        summary_table=rows, raw_quanta_data=raw_quanta_data,
    )
    time.sleep(0.01)


def comparison_row(**over: Any) -> dict:
    """Return a ``compare_runs``-shaped comparison row (default: a
    calibrate change); ``over`` overrides individual fields.
    """
    row_ = {
        "task_label": "calibrate",
        "metric_from": 45.0,
        "metric_to": 52.0,
        "delta": 7.0,
        "delta_pct": 15.6,
        "cohens_d": 0.85,
        "p_value": 0.01,
        "status": "significant",
    }
    row_.update(over)
    return row_


# ---------------------------------------------------------------------------
# Plot-fixture analyzers.
# ---------------------------------------------------------------------------


class _TableAnalyzer:
    """Expose a bare ``QuantumRuntimeTable`` as ``.table`` (plot fixture)."""

    def __init__(self, table: QuantumRuntimeTable) -> None:
        self._table = table

    @property
    def table(self) -> QuantumRuntimeTable:
        return self._table


def brace_format_analyzer(n_per_task: int = 9):
    """Fake analyzer whose data_id strings use the real
    ``str(DataCoordinate)`` brace format (e.g. ``"{visit=522,
    band='r'}"``) over two tasks x ``n_per_task`` quanta.
    """
    visits = [520, 521, 522, 523, 530, 531]
    bands = ['r', 'g']
    rows = []
    qid = 0
    for ti, task in enumerate(['TaskA', 'TaskB']):
        for k in range(n_per_task):
            visit = visits[k % len(visits)]
            band = bands[k % 2]
            qid += 1
            rows.append((
                task,
                qid.to_bytes(16, 'big'),
                1,  # SUCCESSFUL
                1000.0 + k,  # memory MiB
                5.0,  # prep_time
                2.0,  # init_time
                50.0 + ti * 10.0 + k,  # run_time
                40.0 + k,  # run_time_cpu
                f"{{visit={visit}, band='{band}'}}",
            ))
    table = QuantumRuntimeTable.from_rows(rows)
    return _TableAnalyzer(table)


def long_id_analyzer(n_tasks: int = 3, id_len: int = 200):
    """Fake analyzer with maximally long (``id_len``-wide) ``data_id``
    strings, 5 quanta per task.
    """
    rows = []
    qid = 0
    for ti in range(n_tasks):
        for k in range(5):
            qid += 1
            rows.append((
                f'task_{ti}',
                qid.to_bytes(16, 'big'),
                1,
                1000.0 + k,
                5.0,
                2.0,
                50.0 + ti * 10.0 + k,
                40.0 + k,
                '{' + 'x' * id_len + '}',
            ))
    table = QuantumRuntimeTable.from_rows(rows)
    return _TableAnalyzer(table)


def range_analyzer(run_times: list[float]):
    """Fake analyzer with one task holding the given run times."""
    rows = []
    for qi, rt in enumerate(run_times):
        rows.append((
            'wide_task',
            (qi + 1).to_bytes(16, 'big'),
            1,
            1000.0,
            0.0,
            0.0,
            rt,
            rt,
            "{visit: 1}",
        ))
    table = QuantumRuntimeTable.from_rows(rows)
    return _TableAnalyzer(table)
