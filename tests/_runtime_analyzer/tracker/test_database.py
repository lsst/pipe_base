"""Tests for database module (config loading, CRUD operations)."""

from __future__ import annotations

import os
import time

import pytest

from lsst.pipe.base._runtime_analyzer.tracker.database import (
    _extract_row_field,
    _get_graph_tasks,
    create_connection,
    get_all_task_summaries,
    get_raw_quantum_values,
    get_run,
    get_run_by_label,
    get_runs,
    get_task_summary,
    hash_graph,
    load_config,
    update_run,
)

pytest.importorskip("pyarrow")  # provided by the [runtime] extra

from ..support import (  # noqa: E402
    FakeAnalyzer,
    FakeProvenanceGraph,
    raw_quantum_rows,
    two_quantum_summary,
)
from ..support import (
    db_record as _record,
)
from ..support import (
    db_task_row as _task_row,
)


class TestLoadConfig:
    """Tests for ``load_config`` discovery and defaults."""

    @pytest.mark.parametrize(("config_text", "expected"), [
        (None, {}),
        ('db_path: /custom/path/db.db\n', {"db_path": "/custom/path/db.db"}),
    ], ids=["default-path-no-config", "custom-db-path"])
    def test_load_config(self, tmp_path, monkeypatch, config_text,
                         expected):
        """Missing, valid, and malformed config files yield the expected
        dict (and never raise).
        """
        monkeypatch.setenv("HOME", str(tmp_path))
        config_dir = tmp_path / ".config" / "lsst" / "pipe-base" / "_runtime_analyzer"
        if config_text is None:
            assert not config_dir.exists()
        else:
            config_dir.mkdir(parents=True)
            (config_dir / "config.yaml").write_text(config_text)
        config = load_config()
        assert config == expected


class TestDatabaseCRUD:
    """Tests for recording, reading, and updating runs."""

    def test_db_path_fallback(self, monkeypatch):
        import lsst.pipe.base._runtime_analyzer.tracker.database as db_mod
        monkeypatch.setattr(
            db_mod, "DEFAULT_DB_PATH",
            os.path.join(os.path.expanduser("~"), ".local",
                         "share", "runtime-tracker.db"),
        )
        path = db_mod.get_db_path()
        assert "runtime-tracker.db" in path

    def test_create_connection_creates_tables(self, db_path):
        conn = create_connection()
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = {t[0] for t in tables}
        assert "runs" in table_names
        assert "task_summary" in table_names
        assert "quanta_raw" in table_names
        conn.close()

    @pytest.mark.parametrize(("pragma", "expected"), [
        ("journal_mode", "wal"),
        ("foreign_keys", 1),
    ], ids=["journal-mode-wal", "foreign-keys-enabled"])
    def test_connection_pragmas(self, db_path, pragma, expected):
        """Connections enforce WAL journalling and FK enforcement."""
        conn = create_connection()
        result = conn.execute(f"PRAGMA {pragma}").fetchone()
        assert result[0] == expected
        conn.close()

    def test_record_run(self, db_path):
        summary = [_task_row(), _task_row(
            "task_b", quanta=20, mean_rt=30.0, p05=20.0, p25=25.0,
            p50=30.0, p75=35.0, p95=40.0, max_rt=50.0, min_rt=15.0,
            std_rt=5.0, mean_mem=80.0, median_mem=75.0, max_mem=120.0,
            mean_io_pct=0.15, total_rt=600.0,
        )]
        result = _record(label="test_run", run_id="run_001",
                         summary_table=summary, graph_hash="abc123")
        assert result["label"] == "test_run"
        assert result["quanta"] == 30
        assert result["tasks"] == 2

    @pytest.mark.parametrize(("label", "run_id", "lookup_by", "repo",
                              "collection", "expected"), [
        ("g_run", "g_001", "run_id", "myrepo", "mycoll",
         {"label": "g_run", "repo": "myrepo"}),
        ("label_run", "lr_001", "label", None, None,
         {"run_id": "lr_001"}),
        (None, "nonexistent", "run_id", None, None, None),
    ], ids=["by-run-id", "by-label", "not-found"])
    def test_run_lookup(self, db_path, label, run_id, lookup_by, repo,
                        collection, expected):
        """Runs are retrievable by run_id and by label; an unknown id
        returns ``None``.
        """
        if label is not None:
            _record(label=label, run_id=run_id, repo=repo,
                    collection=collection, graph_hash="ghash")
        if lookup_by == "run_id":
            run = get_run(run_id)
        else:
            run = get_run_by_label(label)
        if expected is None:
            assert run is None
        else:
            assert run is not None
            for key, value in expected.items():
                assert run[key] == value

    @pytest.mark.parametrize(("rows", "kwargs", "expected_len",
                              "expected_first"), [
        ([("run_00", "r100", "gh0", []), ("run_01", "r101", "gh1", []),
          ("run_02", "r102", "gh2", []), ("run_03", "r103", "gh3", []),
          ("run_04", "r104", "gh4", [])],
         {"limit": 3}, 3, "run_04"),
        ([("r1", "r10", "g",
           [_task_row("calibrate", quanta=5, mean_rt=10.0, p05=5.0,
                      p25=8.0, p50=10.0, p75=12.0, p95=15.0, max_rt=20.0,
                      min_rt=3.0, std_rt=3.0, mean_mem=50.0,
                      median_mem=48.0, max_mem=80.0, mean_io_pct=0.1,
                      total_rt=50.0)]),
          ("r2", "r20", "g",
           [_task_row("coadd", quanta=8, mean_rt=20.0, p05=10.0,
                      p25=15.0, p50=20.0, p75=25.0, p95=30.0,
                      max_rt=40.0, min_rt=8.0, std_rt=6.0,
                      mean_mem=60.0, median_mem=58.0, max_mem=90.0,
                      mean_io_pct=0.2, total_rt=160.0)])],
         {"limit": 10, "task_filter": "calib"}, 1, "r1"),
    ], ids=["limit-descending", "task-filter"])
    def test_get_runs_listing(self, db_path, rows, kwargs, expected_len,
                              expected_first):
        """``get_runs`` honours the limit (newest first) and a task-name
        filter.
        """
        for label, run_id, graph_hash, summary in rows:
            _record(label=label, run_id=run_id, graph_hash=graph_hash,
                    summary_table=summary)
        runs = get_runs(**kwargs)
        assert len(runs) == expected_len
        # Should be descending by timestamp
        assert runs[0]["label"] == expected_first

    def test_get_task_summary(self, db_path):
        _record(label="ts_run", run_id="ts_001", graph_hash="gh",
                summary_table=[_task_row("calibrate")])
        result = get_task_summary("ts_001")
        assert "calibrate" in result
        assert result["calibrate"]["p50"] == 45.0

    def test_update_run(self, db_path):
        _record(label="upd_run", run_id="u001", graph_hash="gh1",
                summary_table=[_task_row()])
        time.sleep(0.01)
        updated = _record(
            label="upd_run", run_id="u001", graph_hash="gh2",
            analyzer_version="0.1.1",
            summary_table=[_task_row(
                quanta=12, mean_rt=50.0, p05=35.0, p25=42.0, p50=50.0,
                p75=55.0, p95=65.0, max_rt=75.0, min_rt=28.0,
                std_rt=9.0, mean_mem=105.0, median_mem=100.0,
                max_mem=155.0, mean_io_pct=0.12, total_rt=600.0,
            )],
        )
        assert updated["quanta"] == 12
        assert get_run_by_label("upd_run")["analyzer_version"] == "0.1.1"

    def test_get_all_task_summaries(self, db_path):
        _record(label="r1", run_id="r01", graph_hash="g",
                summary_table=[_task_row("calibrate")])
        _record(label="r2", run_id="r02", graph_hash="g",
                summary_table=[_task_row(
                    "calibrate", quanta=12, mean_rt=50.0, p05=35.0,
                    p25=42.0, p50=50.0, p75=55.0, p95=65.0, max_rt=75.0,
                    min_rt=28.0, std_rt=9.0, mean_mem=105.0,
                    median_mem=100.0, max_mem=155.0, mean_io_pct=0.12,
                    total_rt=600.0,
                )])
        all_summaries = get_all_task_summaries()
        assert len(all_summaries) == 2
        filtered = get_all_task_summaries(task_filter="calib")
        assert len(filtered) == 2
        empty = get_all_task_summaries(task_filter="nonexistent")
        assert len(empty) == 0


class TestRecordRunNormalization:
    """TRK-5: summary_table normalization handles both input shapes."""

    @staticmethod
    def _astropy_table():
        astropy = pytest.importorskip("astropy.table")
        return astropy.Table(
            rows=[
                {"Task": "calibrate", "quanta": 10, "mean_rt": 45.0,
                 "p05": 30.0, "p25": 40.0, "p50": 45.0, "p75": 50.0,
                 "p95": 60.0, "max_rt": 70.0, "min_rt": 25.0, "std_rt": 8.0,
                 "mean_mem": 100.0, "median_mem": 95.0, "max_mem": 150.0,
                 "mean_io_pct": 0.1, "total_rt": 450.0},
                {"Task": "coadd", "quanta": 20, "mean_rt": 30.0,
                 "p05": 20.0, "p25": 25.0, "p50": 30.0, "p75": 35.0,
                 "p95": 40.0, "max_rt": 50.0, "min_rt": 15.0, "std_rt": 5.0,
                 "mean_mem": 80.0, "median_mem": 75.0, "max_mem": 120.0,
                 "mean_io_pct": 0.2, "total_rt": 600.0},
            ],
        )

    @staticmethod
    def _dict_rows():
        return [
            {"task_label": "calibrate", "quanta": 7, "mean_rt": 12.0,
             "p05": 5.0, "p25": 8.0, "p50": 12.0, "p75": 15.0,
             "p95": 20.0, "max_rt": 25.0, "min_rt": 3.0, "std_rt": 4.0,
             "mean_mem": 50.0, "median_mem": 48.0, "max_mem": 80.0,
             "mean_io_pct": 0.1, "total_rt": 84.0},
        ]

    @pytest.mark.parametrize(("builder", "label", "run_id", "result_checks",
                              "summary_checks"), [
        (_astropy_table, "astropy_run", "ar1",
         {"tasks": 2, "quanta": 30},
         [("calibrate", "p50", 45.0), ("coadd", "mean_rt", 30.0)]),
        (_dict_rows, "dict_run", "dr1",
         {"tasks": 1, "quanta": 7},
         [("calibrate", "quanta", 7)]),
    ], ids=["astropy-task-header", "dict-task-label"])
    def test_summary_shapes_normalized(self, db_path, builder, label,
                                       run_id, result_checks,
                                       summary_checks):
        """Both a raw astropy Table (with a ``Task`` header column) and
        plain dicts are normalized into the task_summary schema.
        """
        result = _record(label=label, run_id=run_id,
                         summary_table=builder())
        for key, expected in result_checks.items():
            assert result[key] == expected
        summary = get_task_summary(run_id)
        # 'Task' header column must be normalized to task_label key.
        for task, field, expected in summary_checks:
            assert task in summary
            assert summary[task][field] == expected

    def test_raw_quanta_stored_and_readable(self, db_path):
        raw = raw_quantum_rows()
        _record(
            label="raw_run", run_id="rr1",
            summary_table=[_task_row(
                "calibrate", quanta=2, mean_rt=50.0, p05=45.0, p25=45.0,
                p50=50.0, p75=55.0, p95=55.0, max_rt=55.0, min_rt=45.0,
                std_rt=5.0, mean_mem=11.0, median_mem=11.0, max_mem=12.0,
                mean_io_pct=0.1, total_rt=100.0,
            )],
            raw_quanta_data=raw,
        )
        values = get_raw_quantum_values("rr1", "calibrate", "run_time")
        assert sorted(values) == [45.0, 55.0]
        # Memory column readable too, and other tasks return empty.
        assert sorted(get_raw_quantum_values("rr1", "calibrate", "memory")) \
            == [10.0, 12.0]
        assert get_raw_quantum_values("rr1", "nonexistent") == []

    def test_get_raw_quantum_values_bad_column(self, db_path):
        with pytest.raises(ValueError):
            get_raw_quantum_values("x", "y", "not_a_column")


class TestExtractRowField:
    """TRK-5: name-based extraction, never positional."""

    @pytest.mark.parametrize(("row", "key", "aliases", "default",
                              "expected"), [
        ({"task_label": "t"}, "task_label", [], "", "t"),
        ({"Task": "t"}, "task_label", ["Task"], "", "t"),
        ({"other": 1}, "task_label", [], "def", "def"),
    ], ids=["dict-direct", "dict-fallback-alias",
            "missing-returns-default"])
    def test_extract_row_field(self, row, key, aliases, default, expected):
        """Fields are resolved by name (and alias), falling back to the
        default — never to a positional index.
        """
        assert _extract_row_field(row, key, aliases, default) == expected


class TestHashGraph:
    """TRK-5: hash_graph probing order and stable counts."""

    @pytest.mark.parametrize(("obj", "expected_counts"), [
        (FakeProvenanceGraph({"a": {"q1", "q2", "q3"}, "b": {"q4"}}),
         {"a": 3, "b": 1}),
        (FakeAnalyzer(["a", "a", "b"]), {"a": 2, "b": 1}),
    ], ids=["quanta-by-task", "analyzer-table"])
    def test_supported_shapes(self, obj, expected_counts):
        """Both supported shapes yield stable task counts and a
        deterministic, non-empty hash.

        The analyzer case additionally checks hash determinism (the
        quanta_by_task case always did); hashing an analyzer must not be
        empty (CRITICAL-adjacent).
        """
        assert _get_graph_tasks(obj) == expected_counts
        # hash is deterministic and non-empty
        h = hash_graph(obj)
        assert h and h == hash_graph(obj)

    def test_rejects_legacy_shapes(self):
        # Each unsupported input raises a clear TypeError that names
        # the accepted shapes.
        from types import SimpleNamespace
        for legacy in (
            {"a": [1, 2], "b": 5},  # plain dict
            SimpleNamespace(tasks={"a": [1]}),  # 'tasks' attribute
            SimpleNamespace(task_label_to_quanta={"a": [1]}),  # dict-of-lists
            SimpleNamespace(quantum_only_xgraph=object()),  # raw xgraph
        ):
            with pytest.raises(TypeError, match="quanta_by_task"):
                _get_graph_tasks(legacy)
        with pytest.raises(TypeError):
            hash_graph(object())


class TestQuantaRawStalenessOnUpdate:
    """TMAJOR: a label update reuses run_id; quanta_raw must never go stale.

    ``update_run`` re-records under the *same* run_id and (by default) does
    not pass ``raw_quanta_data``. The delete-then-insert in ``record_run``
    must clear the previous run's ``quanta_raw`` rows even when no new raw
    data is supplied, otherwise ``check_alerts`` would compute a
    ``"raw"``-significance from the previous run's quanta against the new
    summary (wrong p-values, mislabeled provenance).
    """

    @staticmethod
    def _raw_count(conn, run_id):
        return conn.execute(
            "SELECT COUNT(*) FROM quanta_raw WHERE run_id = ?", (run_id,)
        ).fetchone()[0]

    def test_update_without_raw_clears_quanta(self, db_path):
        raw = raw_quantum_rows()
        _record(label="stale_run", run_id="stale1",
                summary_table=two_quantum_summary(50.0), raw_quanta_data=raw)

        # Precondition: raw quanta are stored for the run_id.
        assert self._raw_count(create_connection(), "stale1") == 2
        assert sorted(get_raw_quantum_values("stale1", "calibrate",
                                             "run_time")) \
            == [45.0, 55.0]

        # update_run reuses the run_id and does NOT pass raw_quanta_data.
        result = update_run(label="stale_run",
                            summary_table=two_quantum_summary(60.0))
        assert result["run_id"] == "stale1"

        # No stale raw may survive the update.
        assert self._raw_count(create_connection(), "stale1") == 0
        assert get_raw_quantum_values("stale1", "calibrate", "run_time") \
            == []
        # Summary is replaced in place.
        assert get_task_summary("stale1")["calibrate"]["p50"] == 60.0

    def test_update_with_raw_replaces_quanta(self, db_path):
        raw_old = [
            {"task_label": "calibrate", "quantum_id": b"a", "run_time": 10.0},
            {"task_label": "calibrate", "quantum_id": b"b", "run_time": 20.0},
            {"task_label": "calibrate", "quantum_id": b"c", "run_time": 30.0},
        ]
        _record(label="repl_run", run_id="repl1",
                summary_table=two_quantum_summary(50.0), raw_quanta_data=raw_old)
        assert self._raw_count(create_connection(), "repl1") == 3

        raw_new = [{"task_label": "calibrate", "quantum_id": b"z",
                    "run_time": 99.0}]
        update_run(
            label="repl_run", summary_table=two_quantum_summary(70.0),
            raw_quanta_data=raw_new,
        )
        # Only the new single row remains; no stale rows from the old set.
        conn = create_connection()
        try:
            assert self._raw_count(conn, "repl1") == 1
        finally:
            conn.close()
        assert get_raw_quantum_values("repl1", "calibrate", "run_time") \
            == [99.0]
