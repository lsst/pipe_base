"""Integration tests: ``QuantumRuntimeAnalyzer`` against REAL pipe_base
provenance graphs.

These tests build *real* ``lsst.pipe.base`` objects
end-to-end:

* real ``Pipeline`` → ``PipelineGraph`` (``to_graph(
  visualization_only=True)``),
* real ``ProvenanceQuantumGraph`` populated through
  ``ProvenanceQuantumModel._add_to_graph`` (so every node carries a real
  ``lsst.daf.butler.DataCoordinate``, a real ``QuantumAttemptStatus`` enum
  member and a real ``QuantumResourceUsage`` row), and
* a real ``ProvenanceQuantumGraphWriter`` → file → ``ProvenanceQuantumGraph``
  / ``ProvenanceQuantumGraphReader`` round-trip (seeded from a real
  ``QuantumGraph`` built by ``lsst.pipe.base.tests.simpleQGraph``).

The dimension-analysis tests are the crux: they exercise ``dimension_dist``
against real ``DataCoordinate`` objects — the exact path that hand-rolled
``"visit=1,filter=g"`` mock strings cannot reproduce.
"""

from __future__ import annotations

import logging
import os
import tempfile
import uuid
from contextlib import ExitStack

import pytest

import lsst.pipe.base as pb
import lsst.pipe.base.connectionTypes as cT
from lsst.daf.butler import DataCoordinate
from lsst.pipe.base import QuantumAttemptStatus, QuantumGraph
from lsst.pipe.base.log_on_close import LogOnClose
from lsst.pipe.base.quantum_graph import (
    ProvenanceQuantumGraph,
    ProvenanceQuantumGraphWriter,
)
from lsst.pipe.base.quantum_graph._common import HeaderModel
from lsst.pipe.base.quantum_graph._predicted import PredictedQuantumGraphComponents
from lsst.pipe.base.quantum_graph._provenance import (
    ProvenanceQuantumAttemptModel,
    ProvenanceQuantumModel,
    ProvenanceQuantumScanData,
    ProvenanceQuantumScanStatus,
)
from lsst.pipe.base.resource_usage import QuantumResourceUsage
from lsst.pipe.base.tests import simpleQGraph

pytest.importorskip("pyarrow")  # provided by the [runtime] extra

from lsst.pipe.base._runtime_analyzer.core import (  # noqa: E402
    QuantumRuntimeAnalyzer,
    extract_merged_runtime_table,
    extract_runtime_table,
)

from .support import real_usage as _usage  # noqa: E402

# --------------------------------------------------------------------------
# Real PipelineTask definitions used to build real PipelineGraphs.
# --------------------------------------------------------------------------


class CalibrateConnections(
    pb.PipelineTaskConnections,
    dimensions=("instrument", "visit", "band"),
):
    """Connections for the synthetic calibrate task."""

    input = cT.Input(
        doc="raw",
        name="raw",
        dimensions=["instrument", "visit"],
        storageClass="StructuredData",
    )
    output = cT.Output(
        doc="calexp",
        name="calexp",
        dimensions=["instrument", "visit", "band"],
        storageClass="Measurement",
    )


class CalibrateConfig(pb.PipelineTaskConfig, pipelineConnections=CalibrateConnections):
    """Config for the synthetic calibrate task."""

    pass


class CalibrateTask(pb.PipelineTask):
    """Pass-through calibrate task for synthetic graph runs."""

    ConfigClass = CalibrateConfig
    _DefaultName = "calibrateTask"

    def run(self, input):
        return pb.Struct(output=input)


class CoaddConnections(
    pb.PipelineTaskConnections,
    dimensions=("instrument", "band"),
):
    """Connections for the synthetic coadd task."""

    input = cT.Input(
        doc="calexp",
        name="calexp",
        dimensions=["instrument", "visit", "band"],
        storageClass="Measurement",
    )
    output = cT.Output(
        doc="deep coadd",
        name="deepCoadd",
        dimensions=["instrument", "band"],
        storageClass="Struct",
    )


class CoaddConfig(pb.PipelineTaskConfig, pipelineConnections=CoaddConnections):
    """Config for the synthetic coadd task."""

    pass


class CoaddTask(pb.PipelineTask):
    """Pass-through coadd task for synthetic graph runs."""

    ConfigClass = CoaddConfig
    _DefaultName = "coaddTask"

    def run(self, input):
        return pb.Struct(output=input)


def _make_pipeline_graph():
    """Build a real, resolved ``PipelineGraph`` (no registry needed)."""
    pipeline = pb.Pipeline("integration test pipeline")
    pipeline.addTask(CalibrateTask, "calibrate")
    pipeline.addTask(CoaddTask, "coadd")
    return pipeline.to_graph(visualization_only=True)


def _full_values(task_node, data_id: dict) -> list:
    """Order ``data_id`` values as ``chain(required, implied)`` for
    ``DataCoordinate.from_full_values``.
    """
    dims = task_node.dimensions
    return [data_id[name] for name in dims.required] + [data_id[name] for name in dims.implied]


def _seed_quantum(
    qg: ProvenanceQuantumGraph,
    task_label: str,
    data_id: dict,
    status: QuantumAttemptStatus,
    usage: QuantumResourceUsage,
) -> uuid.UUID:
    """Add one quantum with a real attempt to a real provenance graph."""
    task_node = qg.pipeline_graph.tasks[task_label]
    quantum_id = uuid.uuid4()
    model = ProvenanceQuantumModel(
        quantum_id=quantum_id,
        task_label=task_label,
        data_coordinate=_full_values(task_node, data_id),
        attempts=[
            ProvenanceQuantumAttemptModel(attempt=0, status=status, resource_usage=usage),
        ],
    )
    model._add_to_graph(qg)
    return quantum_id


# Seeded run data for the in-memory graph: hand-computable numbers.
# calibrate uses 7 quanta so the z-score method (threshold fixed at 2.0 in
# core.py) can flag an outlier: the maximum attainable population z-score is
# (n-1)/sqrt(n), i.e. ~1.79 for n=5 and ~2.45 for these 7 values.
_CALIBRATE_SEED = [
    # (visit, status, run_time, memory_MiB)
    (8001, QuantumAttemptStatus.SUCCESSFUL, 10.0, 100.0),
    (8002, QuantumAttemptStatus.SUCCESSFUL, 20.0, 200.0),
    (8003, QuantumAttemptStatus.FAILED, 30.0, 300.0),
    (8004, QuantumAttemptStatus.ABORTED, 40.0, 400.0),
    (8005, QuantumAttemptStatus.SUCCESSFUL, 50.0, 450.0),
    (8006, QuantumAttemptStatus.SUCCESSFUL, 60.0, 550.0),
    (8007, QuantumAttemptStatus.SUCCESSFUL, 1000.0, 700.0),  # seeded outlier
]
_COADD_SEED = [
    ("r", QuantumAttemptStatus.SUCCESSFUL, 50.0, 600.0),
    ("g", QuantumAttemptStatus.SUCCESSFUL, 60.0, 100.0),
    ("u", QuantumAttemptStatus.SUCCESSFUL, 70.0, 900.0),
]


@pytest.fixture
def real_graph():
    """Build an in-memory ``ProvenanceQuantumGraph`` with 10 seeded quanta."""
    pg = _make_pipeline_graph()
    header = HeaderModel(
        graph_type="provenance",
        output_run="test_run",
        n_quanta=len(_CALIBRATE_SEED) + len(_COADD_SEED),
        n_task_quanta={
            "calibrate": len(_CALIBRATE_SEED),
            "coadd": len(_COADD_SEED),
        },
    )
    qg = ProvenanceQuantumGraph(header, pg)
    ids: dict[str, uuid.UUID] = {}
    for visit, status, rt, mem in _CALIBRATE_SEED:
        qid = _seed_quantum(
            qg,
            "calibrate",
            {
                "instrument": "INSTR",
                "visit": visit,
                "band": "r",
                "day_obs": 20260101 + (visit - 8001),
                "physical_filter": "r_SDSS09i",
            },
            status,
            _usage(rt, mem),
        )
        ids[f"calibrate_{visit}"] = qid
    for band, status, rt, mem in _COADD_SEED:
        qid = _seed_quantum(
            qg,
            "coadd",
            {"instrument": "INSTR", "band": band, "physical_filter": f"{band}_f"},
            status,
            _usage(rt, mem),
        )
        ids[f"coadd_{band}"] = qid
    qg._integration_ids = ids  # type: ignore[attr-defined]
    return qg


# --------------------------------------------------------------------------
# (a) extraction counts + real object types.
# --------------------------------------------------------------------------


def test_real_graph_node_types_and_counts(real_graph):
    """Analyzer loads exactly the seeded quanta, backed by real objects."""
    nodes = dict(real_graph.quantum_only_xgraph.nodes(data=True))
    n_seeded = len(_CALIBRATE_SEED) + len(_COADD_SEED)
    assert len(nodes) == n_seeded
    for nd in nodes.values():
        assert isinstance(nd["data_id"], DataCoordinate)
        assert isinstance(nd["status"], QuantumAttemptStatus)
        assert isinstance(nd["resource_usage"], QuantumResourceUsage)

    analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(real_graph))
    assert analyzer.n_expected == n_seeded
    assert analyzer.n_loaded == n_seeded
    qt = analyzer.table
    assert qt.n_rows == n_seeded
    # Nothing filtered out, both tasks present.
    assert set(qt.labels().tolist()) == {"calibrate", "coadd"}


# --------------------------------------------------------------------------
# (b) summary(): columns + hand-computed per-task aggregates.
# --------------------------------------------------------------------------


def test_summary_columns_and_hand_computed_means(real_graph):
    """summary() exposes the documented columns and exact seeded means."""
    analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(real_graph))
    table = analyzer.summary()

    for col in (
        "Task",
        "quanta",
        "mean_rt",
        "p05",
        "p25",
        "p50",
        "p75",
        "p95",
        "max_rt",
        "min_rt",
        "std_rt",
        "mean_mem",
        "median_mem",
        "max_mem",
        "mean_io_pct",
        "total_rt",
    ):
        assert col in table.colnames, f"missing summary column {col!r}"

    assert list(table["Task"]) == ["calibrate", "coadd"]

    cal = table[table["Task"] == "calibrate"][0]
    cal_rts = [s[2] for s in _CALIBRATE_SEED]  # 10..60 + 1000
    assert cal["quanta"] == len(cal_rts)
    assert cal["mean_rt"] == pytest.approx(sum(cal_rts) / len(cal_rts))  # 1210/7
    assert cal["total_rt"] == pytest.approx(sum(cal_rts))  # 1210.0
    assert cal["max_rt"] == pytest.approx(1000.0)
    assert cal["min_rt"] == pytest.approx(10.0)
    assert cal["p50"] == pytest.approx(40.0)
    assert cal["mean_mem"] == pytest.approx(sum(s[3] for s in _CALIBRATE_SEED) / len(cal_rts))  # 2700/7
    assert cal["median_mem"] == pytest.approx(400.0)
    assert cal["max_mem"] == pytest.approx(700.0)
    # run_time_cpu is exactly 0.8*run_time for every seeded quantum,
    # so the I/O percentage is 20% for every row.
    assert cal["mean_io_pct"] == pytest.approx(20.0)

    co = table[table["Task"] == "coadd"][0]
    assert co["quanta"] == 3
    assert co["mean_rt"] == pytest.approx(60.0)
    assert co["mean_mem"] == pytest.approx((600.0 + 100.0 + 900.0) / 3)  # 533.33..
    assert co["median_mem"] == pytest.approx(600.0)


# --------------------------------------------------------------------------
# (c) dimension_dist('visit') — the crux (real DataCoordinate parsing).
# --------------------------------------------------------------------------


def test_dimension_dist_visit_real_datacoordinate(real_graph):
    """dimension_dist('visit') must group the calibrate quanta by visit.

    This is the check that would have failed pre-CORE-2.  It is the crux of
    the integration suite: it drives the real ``DataCoordinate`` colon format
    (``str(coord)`` is ``"{instrument: 'INSTR', visit: 8001}"`` and the coord
    exposes ``.mapping``, not ``items()``) end-to-end, which is exactly the
    mock-vs-stack drift the earlier string-format mocks could not catch.
    """
    analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(real_graph))
    dists = analyzer.dimension_dist("visit")

    # Only 'calibrate' has the visit dimension; 'coadd' must be excluded.
    assert set(dists.keys()) == {"calibrate"}

    table = dists["calibrate"]
    assert set(table.colnames) == {
        "group_key",
        "quanta",
        "mean_rt",
        "median_rt",
        "max_rt",
        "std_rt",
        "total_time_sum",
        "pct_of_total_run_time",
    }
    groups = {str(row["group_key"]): row for row in table}
    expected_visits = {str(s[0]) for s in _CALIBRATE_SEED}
    assert set(groups.keys()) == expected_visits
    for visit, _status, rt, _mem in _CALIBRATE_SEED:
        row = groups[str(visit)]
        assert row["quanta"] == 1
        assert row["mean_rt"] == pytest.approx(rt)
    total = sum(s[2] for s in _CALIBRATE_SEED)  # 1210.0
    assert groups["8007"]["pct_of_total_run_time"] == pytest.approx(1000.0 / total * 100)


def test_dimension_dist_band_spans_both_tasks(real_graph):
    """dimension_dist('band') groups across both seeded tasks."""
    analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(real_graph))
    dists = analyzer.dimension_dist("band")
    assert set(dists.keys()) == {"calibrate", "coadd"}
    assert {str(k) for k in dists["coadd"]["group_key"]} == {"r", "g", "u"}
    assert {str(k) for k in dists["calibrate"]["group_key"]} == {"r"}


# --------------------------------------------------------------------------
# (d) bottleneck(): seeded outlier is reported with task + data_id.
# --------------------------------------------------------------------------


def test_bottleneck_finds_seeded_outlier(real_graph):
    """The visit=8007 calibrate quantum (1000 s) is THE IQR outlier."""
    analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(real_graph))
    result = analyzer.bottleneck(method="iqr")

    assert set(result.keys()) == {"task_table", "outlier_table"}
    outliers = result["outlier_table"]
    # calibrate rts [10..60, 1000]: q1=25, q3=55 → upper fence=100
    # → only 1000 flagged.
    # coadd rts [50,60,70]: upper fence=85 → none flagged.
    assert len(outliers) == 1
    row = outliers[0]
    assert row["Task"] == "calibrate"
    assert row["quantum_id"] == str(real_graph._integration_ids["calibrate_8007"])
    assert "visit: 8007" in str(row["data_id"]) or "visit=8007" in str(row["data_id"])
    assert row["run_time"] == pytest.approx(1000.0)
    assert row["outlier_reason"] == "SLOW"
    # median of [10..60, 1000] is 40 → magnitude 25.
    assert row["outlier_magnitude"] == pytest.approx(1000.0 / 40.0)

    # z-score method finds the same outlier on real data.
    zresult = analyzer.bottleneck(method="zscore")
    zrows = zresult["outlier_table"]
    assert len(zrows) >= 1
    assert zrows[0]["quantum_id"] == str(real_graph._integration_ids["calibrate_8007"])

    # Task-level table covers both tasks.
    assert set(result["task_table"]["Task"]) == {"calibrate", "coadd"}


# --------------------------------------------------------------------------
# (e) top_quantities(): ranking + pct_of_task_mean uses the selected metric.
# --------------------------------------------------------------------------


def test_top_quantities_run_time_ranking(real_graph):
    """metric='run_time' ranks by run time with run_time-vs-run_time pct."""
    analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(real_graph))
    top = analyzer.top_quantities(metric="run_time", n=10)
    rts = [float(rt) for rt in top["run_time"]]
    assert rts == sorted(rts, reverse=True)
    assert float(top["run_time"][0]) == pytest.approx(1000.0)
    assert top["task_label"][0] == "calibrate"
    # pct vs the calibrate run_time mean (1210/7).
    assert top["pct_of_task_mean"][0] == pytest.approx(1000.0 / (1210.0 / 7.0) * 100)
    assert top["rank"][-1] == 10

    with pytest.raises(ValueError, match="Unsupported metric"):
        analyzer.top_quantities(metric="prep_time")


# --------------------------------------------------------------------------
# File write/read round-trip via ProvenanceQuantumGraphWriter/Reader.
# --------------------------------------------------------------------------


def _write_provenance_roundtrip(
    root: str, seed_spec: list[tuple[int, QuantumAttemptStatus, float, float]],
) -> str:
    """Build a real .qgraph with simpleQGraph, convert to predicted components,
    and write a provenance graph file with seeded per-quantum attempts.

    ``seed_spec`` provides (index, status, run_time, memory_MiB) for each
    predicted quantum in task order (task0, task1, ...).
    """
    butler, old_qg = simpleQGraph.makeSimpleQGraph(root=root, nQuanta=len(seed_spec))
    assert isinstance(old_qg, QuantumGraph)
    components = PredictedQuantumGraphComponents.from_old_quantum_graph(old_qg)
    assert len(components.quantum_datasets) == len(seed_spec)

    prov_path = os.path.join(root, "provenance.qg")
    # Bind seeds to task labels deterministically (task0, task1, ... in
    # topological task order).
    labels = sorted({predicted.task_label for predicted in components.quantum_datasets.values()})
    spec_map = dict(zip(labels, seed_spec))
    with ExitStack() as stack:
        writer = ProvenanceQuantumGraphWriter(
            prov_path,
            exit_stack=stack,
            log_on_close=LogOnClose(logging.getLogger("integration").log),
            predicted=components,
        )
        writer.write_overall_inputs()
        writer.write_packages()
        writer.write_init_outputs(assume_existence=True)
        for quantum_id, predicted in components.quantum_datasets.items():
            _label, status, rt, mem = spec_map[predicted.task_label]
            model = ProvenanceQuantumModel.from_predicted(predicted)
            model.attempts = [
                ProvenanceQuantumAttemptModel(attempt=0, status=status, resource_usage=_usage(rt, mem)),
            ]
            existing = {ref.dataset_id for refs in predicted.outputs.values() for ref in refs}
            writer.write_scan_data(
                ProvenanceQuantumScanData(
                    quantum_id,
                    status=ProvenanceQuantumScanStatus.SUCCESSFUL,
                    existing_outputs=existing,
                    quantum=model.model_dump_json().encode(),
                )
            )
    return prov_path


def test_writer_reader_round_trip_end_to_end():
    """Write a real provenance .qg, read it back, analyze it, exact numbers."""
    seed_spec = [
        (0, QuantumAttemptStatus.SUCCESSFUL, 10.0, 100.0),
        (1, QuantumAttemptStatus.ABORTED, 400.0, 200.0),
    ]
    with tempfile.TemporaryDirectory() as root:
        prov_path = _write_provenance_roundtrip(root, seed_spec)
        assert os.path.getsize(prov_path) > 0

        # Full analyzer over the on-disk graph (single source).
        analyzer = QuantumRuntimeAnalyzer(
            extract_merged_runtime_table([(prov_path, None)])
        )
        assert analyzer.n_loaded == 2
        assert analyzer.n_sources == 1

        # And via the direct from_args/extract path too.
        with ProvenanceQuantumGraph.from_args(prov_path, datasets=()) as (qg, _):
            direct = QuantumRuntimeAnalyzer(extract_runtime_table(qg))
            assert direct.n_expected == 2
            assert direct.n_loaded == 2
            loaded_nodes = list(qg.quantum_only_xgraph.nodes(data=True))
            assert len(loaded_nodes) == 2
            for _qid, nd in loaded_nodes:
                assert isinstance(nd["data_id"], DataCoordinate)
                assert isinstance(nd["status"], QuantumAttemptStatus)
                assert isinstance(nd["resource_usage"], QuantumResourceUsage)

        table = analyzer.summary()
        assert list(table["Task"]) == ["task0", "task1"]
        assert table["mean_rt"][0] == pytest.approx(10.0)
        assert table["mean_rt"][1] == pytest.approx(400.0)
        assert table["mean_mem"][0] == pytest.approx(100.0)
        assert table["mean_mem"][1] == pytest.approx(200.0)

        # Spec status semantics survive serialization: ABORTED counts as
        # "failed"; "aborted" matches it; ABORTED_SUCCESS matches nothing.
        failed = analyzer.summary(status="failed")
        assert list(failed["Task"]) == ["task1"]
        assert failed["quanta"][0] == 1
        assert len(analyzer.summary(status="aborted_success")) == 0

        # One quantum per task: no outliers possible, but the table keys
        # must exist (regression guard for empty-table paths).
        bn = analyzer.bottleneck()
        assert len(bn["outlier_table"]) == 0
        assert set(bn["task_table"]["Task"]) == {"task0", "task1"}

        top = analyzer.top_quantities(metric="run_time")
        assert top["task_label"][0] == "task1"
        assert top["status"][0] == "ABORTED"
        # pct_of_task_mean is metric-vs-metric: 400/400 = 100%.
        assert top["pct_of_task_mean"][0] == pytest.approx(100.0)


def test_writer_reader_round_trip_dimension_dist():
    """dimension_dist('detector') on the round-tripped real graph.

    Nodes come back through ``ProvenanceQuantumGraphReader`` as real
    ``DataCoordinate`` objects ({instrument: 'INSTR', detector: 0}); the
    analyzer must group by detector value 0.
    """
    seed_spec = [
        (0, QuantumAttemptStatus.SUCCESSFUL, 10.0, 100.0),
        (1, QuantumAttemptStatus.SUCCESSFUL, 20.0, 200.0),
    ]
    with tempfile.TemporaryDirectory() as root:
        prov_path = _write_provenance_roundtrip(root, seed_spec)
        analyzer = QuantumRuntimeAnalyzer(
            extract_merged_runtime_table([(prov_path, None)])
        )
        dists = analyzer.dimension_dist("detector")
        assert set(dists.keys()) == {"task0", "task1"}
        for table in dists.values():
            assert {str(k) for k in table["group_key"]} == {"0"}
            assert table["quanta"][0] == 1
