"""Database layer for the runtime tracker.

Handles SQLite database connections, schema creation, CRUD operations,
and config file loading.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import time
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypedDict

if TYPE_CHECKING:
    from astropy.table import Table


class RunRecord(TypedDict):
    """One row of the ``runs`` table (see :func:`get_run`)."""

    run_id: str
    label: str
    repo: str | None
    collection: str | None
    graph_hash: str | None
    timestamp: float
    analyzer_version: str | None


class TaskSummaryRow(TypedDict):
    """One row of the ``task_summary`` table (see
    :func:`get_task_summary`).  Metric columns are nullable because the
    schema accepts NULLs from partial summary tables.
    """

    task_label: str
    quanta: int | None
    mean_rt: float | None
    p05: float | None
    p25: float | None
    p50: float | None
    p75: float | None
    p95: float | None
    max_rt: float | None
    min_rt: float | None
    std_rt: float | None
    mean_mem: float | None
    median_mem: float | None
    max_mem: float | None
    mean_io_pct: float | None
    total_rt: float | None


class SummaryRow(TaskSummaryRow):
    """A ``task_summary`` row joined with its run identity (see
    :func:`get_all_task_summaries`).
    """

    run_id: str
    label: str


class RecordResult(TypedDict):
    """Result dict returned by :func:`record_run` / :func:`update_run`."""

    run_id: str
    label: str
    quanta: int
    tasks: int


def load_config() -> dict[str, str]:
    """Load tracker configuration from YAML config file.

    Reads ``~/.config/lsst/pipe-base/_runtime_analyzer/config.yaml`` and
    parses simple ``key: value`` lines. Only the ``db_path`` key is used.
    Falls back to an empty dict on any error.

    Returns
    -------
    config : `dict`
        Parsed configuration (currently just ``{"db_path": str}`` or {}).
    """
    config_path = (
        Path.home() / ".config" / "lsst" / "pipe-base" / "_runtime_analyzer"
        / "config.yaml"
    )
    if not config_path.exists():
        return {}
    try:
        config: dict[str, str] = {}
        with open(config_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if ":" in line:
                    key, _, value = line.partition(":")
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    if key in ("db_path",):
                        config[key] = value
        return config
    except Exception:
        return {}


DEFAULT_DB_PATH = os.path.join(
    os.path.expanduser("~"), ".local", "share", "runtime-tracker.db"
)


def get_db_path() -> str:
    """Return the resolved database path.

    Uses ``db_path`` from config if available, otherwise falls back
    to the default location.

    Returns
    -------
    db_path : `str`
        Absolute path to the SQLite database file.
    """
    config = load_config()
    db_path = config.get("db_path", DEFAULT_DB_PATH)
    # Resolve relative paths
    if not os.path.isabs(db_path):
        db_path = os.path.abspath(db_path)
    return db_path


def _init_db(conn: sqlite3.Connection) -> None:
    """Execute CREATE TABLE statements for all tracker tables.

    Parameters
    ----------
    conn : `sqlite3.Connection`
        Open database connection.
    """
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            repo TEXT,
            collection TEXT,
            graph_hash TEXT,
            timestamp REAL NOT NULL,
            analyzer_version TEXT
        );

        CREATE TABLE IF NOT EXISTS task_summary (
            run_id TEXT NOT NULL,
            task_label TEXT NOT NULL,
            quanta INTEGER,
            mean_rt REAL,
            p05 REAL,
            p25 REAL,
            p50 REAL,
            p75 REAL,
            p95 REAL,
            max_rt REAL,
            min_rt REAL,
            std_rt REAL,
            mean_mem REAL,
            median_mem REAL,
            max_mem REAL,
            mean_io_pct REAL,
            total_rt REAL,
            PRIMARY KEY (run_id, task_label),
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS quanta_raw (
            run_id TEXT NOT NULL,
            task_label TEXT NOT NULL,
            quantum_id BLOB,
            status INTEGER,
            memory REAL,
            prep_time REAL,
            init_time REAL,
            run_time REAL,
            run_time_cpu REAL,
            data_id TEXT,
            PRIMARY KEY (run_id, quantum_id),
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );
    """)


def _setup_indexes(conn: sqlite3.Connection) -> None:
    """Create indexes for common query patterns.

    Parameters
    ----------
    conn : `sqlite3.Connection`
        Open database connection.
    """
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_runs_label ON runs(label);
        CREATE INDEX IF NOT EXISTS idx_task_summary_run_id ON task_summary(run_id);
        CREATE INDEX IF NOT EXISTS idx_task_summary_task_label ON task_summary(task_label);
        CREATE INDEX IF NOT EXISTS idx_quanta_raw_run_id ON quanta_raw(run_id);
    """)


def create_connection() -> sqlite3.Connection:
    """Open a SQLite connection with WAL mode and foreign keys enabled.

    Creates the database file and all tables if they do not already exist.

    Returns
    -------
    conn : `sqlite3.Connection`
        Configured database connection.
    """
    db_path = get_db_path()
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    _init_db(conn)
    _setup_indexes(conn)
    return conn


def hash_graph(qg: Any) -> str:
    """Compute a SHA-256 hash from a quantum graph structure.

    The hash is derived from the task list and quantum counts per task,
    providing a fingerprint of the graph's logical structure.

    Parameters
    ----------
    qg : object
        A pipe_base ``ProvenanceQuantumGraph`` (exposing ``quanta_by_task``)
        or an analyzer-like object with a ``table`` (`QuantumRuntimeTable`).
        See :func:`_get_graph_tasks` for the accepted shapes.

    Returns
    -------
    hash_str : `str`
        Hex-encoded SHA-256 digest.
    """
    hasher = hashlib.sha256()
    tasks = _get_graph_tasks(qg)

    sorted_tasks = sorted(tasks.keys())
    for tl in sorted_tasks:
        quanta_list = tasks[tl]
        if isinstance(quanta_list, list):
            count = len(quanta_list)
        elif isinstance(quanta_list, dict):
            count = len(quanta_list)
        else:
            count = quanta_list if isinstance(quanta_list, int) else 0
        hasher.update(f"{tl}:{count}".encode())

    return hasher.hexdigest()


def _get_graph_tasks(qg: Any) -> dict[str, Any]:
    """Extract a ``{task_label: quantum_count}`` mapping from a graph-like.

    The input may be one of two shapes, probed in priority order:

    1. A pipe_base ``ProvenanceQuantumGraph`` exposing ``quanta_by_task``
       (``{task_label: set[uuid]}``).
    2. An analyzer-like object with a ``table`` (`QuantumRuntimeTable`)
       carrying per-row task labels (counts are taken directly,
       version-proof).

    Parameters
    ----------
    qg : object
        Quantum-graph-like or analyzer-like object.

    Returns
    -------
    counts : `dict`
        Mapping of task label to number of quanta.

    Raises
    ------
    TypeError
        Raised if ``qg`` exposes neither ``quanta_by_task`` nor a
        ``QuantumRuntimeTable``-like ``table`` attribute.
    """
    # (a) pipe_base ProvenanceQuantumGraph.
    if hasattr(qg, "quanta_by_task"):
        raw = qg.quanta_by_task
        return {
            k: len(v) if hasattr(v, "__len__") else v
            for k, v in raw.items()
        }

    # (b) analyzer/table-like object exposing a QuantumRuntimeTable `table`.
    table = getattr(qg, "table", None)
    if table is not None and hasattr(table, "labels"):
        counts: dict[str, int] = {}
        for tl in table.labels():
            key = str(tl)
            counts[key] = counts.get(key, 0) + 1
        return counts

    raise TypeError(
        "hash_graph/_get_graph_tasks accept only a pipe_base "
        "ProvenanceQuantumGraph (exposing 'quanta_by_task') or an "
        "analyzer-like object exposing a QuantumRuntimeTable 'table'; got "
        f"{type(qg).__name__!r}"
    )


# Canonical ``task_summary`` columns and the accepted source-column aliases
# from which each may be read. The first alias is the canonical name.
_TASK_SUMMARY_FIELDS: list[tuple[str, tuple[str, ...]]] = [
    ("task_label", ("task_label", "Task")),
    ("quanta", ("quanta",)),
    ("mean_rt", ("mean_rt",)),
    ("p05", ("p05",)),
    ("p25", ("p25",)),
    ("p50", ("p50",)),
    ("p75", ("p75",)),
    ("p95", ("p95",)),
    ("max_rt", ("max_rt",)),
    ("min_rt", ("min_rt",)),
    ("std_rt", ("std_rt",)),
    ("mean_mem", ("mean_mem",)),
    ("median_mem", ("median_mem",)),
    ("max_mem", ("max_mem",)),
    ("mean_io_pct", ("mean_io_pct",)),
    ("total_rt", ("total_rt",)),
]


def _extract_row_field(
    row: Any,
    key: str,
    fallback_keys: Iterable[str],
    default: Any,
) -> Any:
    """Extract a single named field from a row.

    Access is always *by column name*, never by positional index. Supported
    row shapes: plain ``dict`` (``row.get``) and ``astropy.table.Row`` /
    ``astropy.table.Table`` rows (name-indexed ``row[name]`` lookup).
    ``fallback_keys`` supplies additional alias names tried, in order, after
    ``key``.

    Parameters
    ----------
    row : `dict` or object
        A single summary/quanta row.
    key : `str`
        Preferred column name.
    fallback_keys : `iterable` of `str`
        Alias column names tried, in order, when ``key`` is absent.
    default : object
        Value returned when no name resolves.

    Returns
    -------
    value : object
        The resolved value, or ``default``.
    """
    names = [key] + [k for k in fallback_keys if k != key]

    def _native(val: Any) -> Any:
        # ``sqlite3`` binds numpy integer scalars (np.int64/np.int8, which
        # are NOT ``int`` subclasses) as BLOBs on Python 3.12+, so callers
        # that later compare the stored value numerically (cohens_d,
        # thresholds) would see bytes.  Unwrap any numpy scalar to its
        # native Python equivalent; plain int/float/str/bytes pass through
        # untouched (bytes quantum ids stay bytes).
        if val is not None and not isinstance(
            val, (int, float, str, bytes)
        ) and hasattr(val, "item"):
            try:
                return val.item()
            except Exception:  # pragma: no cover - exotic scalar types
                return val
        return val

    if isinstance(row, dict):
        for name in names:
            val = row.get(name)
            if val is not None:
                return _native(val)
        return default

    # astropy Row / any name-indexable mapping.
    for name in names:
        try:
            val = row[name]
        except (KeyError, IndexError, TypeError, ValueError):
            continue
        if val is not None:
            return _native(val)
    return default


def _normalize_summary_rows(
    summary_table: Table | list[Mapping[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Normalize a summary table into a list of canonical dicts.

    Handles both ``astropy.table.Table`` input (whose task column is named
    ``Task``) and plain ``list[dict]`` input (whose task column is
    ``task_label``). Normalization is performed once, by column name, before
    any insert. See :data:`_TASK_SUMMARY_FIELDS` for the alias mapping.

    Parameters
    ----------
    summary_table : `astropy.table.Table` or `list` of `Mapping` or `None`
        Summary data.

    Returns
    -------
    rows : `list` of `dict`
        Rows with canonical keys: ``task_label``, ``quanta``, the runtime and
        memory statistics.
    """
    defaults = {"task_label": "", "quanta": 0}
    rows: list[dict[str, Any]] = []
    if summary_table is None:
        return rows
    for row in summary_table:
        out: dict[str, Any] = {}
        for canon, aliases in _TASK_SUMMARY_FIELDS:
            out[canon] = _extract_row_field(row, canon, aliases, defaults.get(canon))
        rows.append(out)
    return rows


def record_run(
    label: str,
    run_id: str,
    repo: str | None,
    collection: str | None,
    graph_hash: str | None,
    analyzer_version: str | None,
    summary_table: Table | list[Mapping[str, Any]] | None,
    raw_quanta_data: Any = None,
) -> RecordResult:
    """Persist a run and its task metrics to the database.

    If a run with the same ``label`` already exists, its existing
    ``run_id`` is reused and its summary/raw rows are replaced (a new
    ``run_id`` argument is ignored in that case); a graph-hash mismatch
    with the stored run warns to stderr.

    Parameters
    ----------
    label : `str`
        Human-readable label for the run.
    run_id : `str`
        Unique identifier for the run (ignored when ``label`` already
        exists, which keeps its stored ``run_id``).
    repo : `str` or `None`
        Butler repository path or alias.
    collection : `str` or `None`
        Butler collection name.
    graph_hash : `str` or `None`
        SHA-256 hash of the graph structure.
    analyzer_version : `str` or `None`
        Version string of the runtime-analyzer.
    summary_table : `astropy.table.Table` or `list` of `dict`
        Table from ``QuantumRuntimeAnalyzer.summary()`` with columns:
        task_label, quanta, mean_rt, p05, p25, p50, p75, p95, max_rt,
        min_rt, std_rt, mean_mem, median_mem, max_mem, mean_io_pct, total_rt.
    raw_quanta_data : sequence of mapping, optional
        Per-quantum rows (mapping-like, e.g. the dicts built by the CLI
        ``--raw`` path).  If passed, rows are also stored in ``quanta_raw``.

    Returns
    -------
    result : `dict`
        Summary dict with keys: ``run_id``, ``label``, ``quanta``, ``tasks``.
    """
    conn = create_connection()
    try:
        now = time.time()

        # Check if label already exists
        existing = conn.execute(
            "SELECT run_id, graph_hash FROM runs WHERE label = ?", (label,)
        ).fetchone()

        is_update = existing is not None
        actual_run_id = run_id
        if is_update:
            actual_run_id = existing[0]
            if existing[1] and existing[1] != graph_hash:
                os.write(2, (f"Warning: Graph hash mismatch for label '{label}'.\n").encode())

        # Insert or update run metadata
        if is_update:
            conn.execute(
                """UPDATE runs
                   SET run_id=?, repo=?, collection=?, graph_hash=?,
                       timestamp=?, analyzer_version=?
                   WHERE label=?""",
                (actual_run_id, repo, collection, graph_hash, now,
                 analyzer_version, label),
            )
        else:
            conn.execute(
                """INSERT INTO runs (run_id, label, repo, collection,
                    graph_hash, timestamp, analyzer_version)
                   VALUES (?,?,?,?,?,?,?)""",
                (actual_run_id, label, repo, collection, graph_hash,
                 now, analyzer_version),
            )

        # Delete-then-insert for an idempotent update. Both deletes run
        # unconditionally for this run_id: task_summary is always replaced, and
        # quanta_raw must ALSO always be cleared — even when no new raw data
        # is supplied. Otherwise a label UPDATE that reuses the existing
        # run_id but omits ``--raw`` would leave the *previous* run's quanta
        # rows behind, and ``check_alerts`` would then compute a
        # ``"raw"``-significance from stale quanta against the new summary,
        # producing wrong p-values. On a fresh (non-update) insert there are
        # no rows for the run_id yet, so the delete is a harmless no-op.
        conn.execute("DELETE FROM task_summary WHERE run_id = ?", (actual_run_id,))
        conn.execute("DELETE FROM quanta_raw WHERE run_id = ?", (actual_run_id,))

        # Normalize the summary table ONCE, by column name, so that both
        # astropy Table input (task column named "Task") and plain list[dict]
        # input (task column named "task_label") are handled explicitly.
        summary_rows = _normalize_summary_rows(summary_table)

        total_quanta = 0
        for row in summary_rows:
            quanta = row["quanta"]
            total_quanta += int(quanta) if quanta else 0

        # Insert summary data with executemany.
        if summary_rows:
            conn.executemany(
                """INSERT INTO task_summary (run_id, task_label, quanta,
                    mean_rt, p05, p25, p50, p75, p95, max_rt, min_rt,
                    std_rt, mean_mem, median_mem, max_mem, mean_io_pct, total_rt)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [
                    (actual_run_id, row["task_label"], row["quanta"],
                     row["mean_rt"], row["p05"], row["p25"], row["p50"],
                     row["p75"], row["p95"], row["max_rt"], row["min_rt"],
                     row["std_rt"], row["mean_mem"], row["median_mem"],
                     row["max_mem"], row["mean_io_pct"], row["total_rt"])
                    for row in summary_rows
                ],
            )

        # Insert raw quanta data if provided (executemany).
        if raw_quanta_data is not None and len(raw_quanta_data) > 0:
            quanta_params = []
            for qrow in raw_quanta_data:
                quantum_id = _extract_row_field(qrow, "quantum_id", [], b"")
                status = _extract_row_field(qrow, "status", [], 0)
                memory = _extract_row_field(qrow, "memory", [], 0.0)
                prep_time = _extract_row_field(qrow, "prep_time", [], 0.0)
                init_time = _extract_row_field(qrow, "init_time", [], 0.0)
                run_time = _extract_row_field(qrow, "run_time", [], 0.0)
                run_time_cpu = _extract_row_field(qrow, "run_time_cpu", [], 0.0)
                data_id = str(_extract_row_field(qrow, "data_id", [], ""))
                task_label = _extract_row_field(qrow, "task_label", [], "")
                quanta_params.append(
                    (actual_run_id, task_label, quantum_id, status, memory,
                     prep_time, init_time, run_time, run_time_cpu, data_id)
                )
            conn.executemany(
                """INSERT INTO quanta_raw (run_id, task_label,
                    quantum_id, status, memory, prep_time, init_time,
                    run_time, run_time_cpu, data_id)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                quanta_params,
            )

        conn.commit()

        return {
            "run_id": actual_run_id,
            "label": label,
            "quanta": total_quanta,
            "tasks": len(summary_rows),
        }
    finally:
        conn.close()


def get_run(run_id: str) -> RunRecord | None:
    """Return a run dict with metadata, or None.

    Parameters
    ----------
    run_id : `str`
        The run identifier.

    Returns
    -------
    run : `RunRecord` or `None`
        Dict with keys ``run_id``, ``label``, ``repo``, ``collection``,
        ``graph_hash``, ``timestamp``, and ``analyzer_version``; ``None``
        if no run has this id.
    """
    conn = create_connection()
    try:
        row = conn.execute(
            "SELECT run_id, label, repo, collection, graph_hash, timestamp, analyzer_version "
            "FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None:
            return None
        return {
            "run_id": row[0],
            "label": row[1],
            "repo": row[2],
            "collection": row[3],
            "graph_hash": row[4],
            "timestamp": row[5],
            "analyzer_version": row[6],
        }
    finally:
        conn.close()


def get_run_by_label(label: str) -> RunRecord | None:
    """Return a run dict by label, or None.

    Parameters
    ----------
    label : `str`
        The human-readable label.

    Returns
    -------
    run : `RunRecord` or `None`
        Dict with the same keys as :func:`get_run`; ``None`` if no run
        carries this label.
    """
    conn = create_connection()
    try:
        row = conn.execute(
            "SELECT run_id, label, repo, collection, graph_hash, timestamp, analyzer_version "
            "FROM runs WHERE label = ?", (label,)
        ).fetchone()
        if row is None:
            return None
        return {
            "run_id": row[0],
            "label": row[1],
            "repo": row[2],
            "collection": row[3],
            "graph_hash": row[4],
            "timestamp": row[5],
            "analyzer_version": row[6],
        }
    finally:
        conn.close()


def get_runs(limit: int = 20, task_filter: str | None = None) -> list[RunRecord]:
    """Return a list of run dicts ordered by timestamp descending.

    Parameters
    ----------
    limit : `int`, optional
        Maximum number of runs to return. Default 20.
    task_filter : `str` or `None`, optional
        If provided, only include runs containing a task_label matching
        this substring.

    Returns
    -------
    runs : `list` of `RunRecord`
        Run dicts with the same keys as :func:`get_run`, newest first.
    """
    conn = create_connection()
    try:
        if task_filter:
            subquery = """
                SELECT DISTINCT t.run_id FROM task_summary t
                WHERE t.task_label LIKE ?
            """
            rows = conn.execute(
                f"SELECT run_id, label, repo, collection, graph_hash, timestamp, analyzer_version "
                f"FROM runs WHERE run_id IN ({subquery}) "
                f"ORDER BY timestamp DESC LIMIT ?",
                (f"%{task_filter}%", limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT run_id, label, repo, collection, graph_hash, timestamp, analyzer_version "
                "FROM runs ORDER BY timestamp DESC LIMIT ?",
                (limit,),
            ).fetchall()

        result: list[RunRecord] = []
        for row in rows:
            result.append({
                "run_id": row[0],
                "label": row[1],
                "repo": row[2],
                "collection": row[3],
                "graph_hash": row[4],
                "timestamp": row[5],
                "analyzer_version": row[6],
            })
        return result
    finally:
        conn.close()


def get_task_summary(run_id: str) -> dict[str, TaskSummaryRow]:
    """Return a dict mapping task_label to a metric row dict.

    Parameters
    ----------
    run_id : `str`
        The run identifier.

    Returns
    -------
    summary : `dict` [ `str`, `TaskSummaryRow` ]
        Mapping of task_label to a metric row keyed by the
        ``task_summary`` columns (``quanta``, ``mean_rt``, the
        percentiles, memory stats, ``mean_io_pct``, ``total_rt``, and
        ``task_label``).
    """
    conn = create_connection()
    try:
        rows = conn.execute(
            "SELECT task_label, quanta, mean_rt, p05, p25, p50, p75, p95, "
            "max_rt, min_rt, std_rt, mean_mem, median_mem, max_mem, "
            "mean_io_pct, total_rt "
            "FROM task_summary WHERE run_id = ?",
            (run_id,),
        ).fetchall()
        result: dict[str, TaskSummaryRow] = {}
        for row in rows:
            result[row[0]] = {
                "task_label": row[0],
                "quanta": row[1],
                "mean_rt": row[2],
                "p05": row[3],
                "p25": row[4],
                "p50": row[5],
                "p75": row[6],
                "p95": row[7],
                "max_rt": row[8],
                "min_rt": row[9],
                "std_rt": row[10],
                "mean_mem": row[11],
                "median_mem": row[12],
                "max_mem": row[13],
                "mean_io_pct": row[14],
                "total_rt": row[15],
            }
        return result
    finally:
        conn.close()


# Columns that may be requested from the ``quanta_raw`` table. Whitelisted so
# the name can be safely interpolated into a SELECT statement.
_RAW_NUMERIC_COLUMNS = frozenset(
    {"memory", "prep_time", "init_time", "run_time", "run_time_cpu"}
)


def get_raw_quantum_values(
    run_id: str,
    task_label: str,
    column: str = "run_time",
) -> list[float]:
    """Return per-quantum values for a task in a run from ``quanta_raw``.

    Parameters
    ----------
    run_id : `str`
        The run identifier.
    task_label : `str`
        The task whose quanta should be selected.
    column : `str`, optional
        Numeric column to read. One of ``run_time``, ``run_time_cpu``,
        ``memory``, ``prep_time``, ``init_time``. Default ``"run_time"``.

    Returns
    -------
    values : `list` of `float`
        The requested metric for each stored quantum (empty if the run has
        no raw data recorded, e.g. recorded without ``--raw``).

    Raises
    ------
    ValueError
        Raised if ``column`` is not one of the whitelisted numeric
        ``quanta_raw`` columns.
    """
    if column not in _RAW_NUMERIC_COLUMNS:
        raise ValueError(
            f"Unknown raw column {column!r}; expected one of "
            f"{sorted(_RAW_NUMERIC_COLUMNS)}"
        )
    conn = create_connection()
    try:
        rows = conn.execute(
            f"SELECT {column} FROM quanta_raw "
            f"WHERE run_id = ? AND task_label = ?",
            (run_id, task_label),
        ).fetchall()
        values: list[float] = []
        for row in rows:
            try:
                values.append(float(row[0]))
            except (TypeError, ValueError):
                continue
        return values
    finally:
        conn.close()


def get_all_task_summaries(
    task_filter: str | None = None,
) -> list[SummaryRow]:
    """Return all runs' task summaries, optionally filtered by task.

    Parameters
    ----------
    task_filter : `str` or `None`, optional
        If provided, only include task summaries matching this substring.

    Returns
    -------
    summaries : `list` of `SummaryRow`
        One dict per task-summary row (newest run first) with keys
        ``run_id``, ``label``, ``task_label``, ``quanta``, the runtime and
        memory statistics, and ``total_rt``.
    """
    conn = create_connection()
    try:
        where_clause = "WHERE 1=1"
        params = []
        if task_filter:
            where_clause += " AND t.task_label LIKE ?"
            params.append(f"%{task_filter}%")

        rows = conn.execute(
            f"SELECT t.run_id, r.label, t.task_label, t.quanta, t.mean_rt, "
            f"t.p05, t.p25, t.p50, t.p75, t.p95, t.max_rt, t.min_rt, "
            f"t.std_rt, t.mean_mem, t.median_mem, t.max_mem, "
            f"t.mean_io_pct, t.total_rt "
            f"FROM task_summary t JOIN runs r ON t.run_id = r.run_id "
            f"{where_clause} "
            f"ORDER BY r.timestamp DESC",
            params,
        ).fetchall()

        result: list[SummaryRow] = []
        for row in rows:
            result.append({
                "run_id": row[0],
                "label": row[1],
                "task_label": row[2],
                "quanta": row[3],
                "mean_rt": row[4],
                "p05": row[5],
                "p25": row[6],
                "p50": row[7],
                "p75": row[8],
                "p95": row[9],
                "max_rt": row[10],
                "min_rt": row[11],
                "std_rt": row[12],
                "mean_mem": row[13],
                "median_mem": row[14],
                "max_mem": row[15],
                "mean_io_pct": row[16],
                "total_rt": row[17],
            })
        return result
    finally:
        conn.close()


def update_run(
    label: str,
    summary_table: Table | list[Mapping[str, Any]] | None,
    raw_quanta_data: Any = None,
) -> RecordResult:
    """Update metrics for an existing label, preserving its run_id.

    The stored ``repo``, ``collection``, ``graph_hash``, and
    ``analyzer_version`` are carried over verbatim; the task summary and
    raw-quanta rows are replaced via :func:`record_run`.

    Parameters
    ----------
    label : `str`
        The run label to update.
    summary_table : `astropy.table.Table` or `list` of `dict`
        New summary data.
    raw_quanta_data : sequence of mapping, optional
        New per-quantum rows; when omitted, the stored raw rows are
        cleared.

    Returns
    -------
    result : `dict`
        The updated run summary (see :func:`record_run`).

    Raises
    ------
    ValueError
        Raised if no run exists with the given ``label``.
    """
    run = get_run_by_label(label)
    if run is None:
        raise ValueError(f"No run found with label {label!r}")
    return record_run(
        label=label,
        run_id=run["run_id"],
        repo=run["repo"],
        collection=run["collection"],
        graph_hash=run["graph_hash"],
        analyzer_version=run["analyzer_version"],
        summary_table=summary_table,
        raw_quanta_data=raw_quanta_data,
    )
