# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

import dataclasses
import itertools
import os
import tempfile
import time
import unittest.mock
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

import astropy.table
import click.testing
import numpy as np
import pydantic
from click.testing import CliRunner, Result

import lsst.utils.tests
from lsst.daf.butler import Butler, ButlerLogRecords, QuantumBackedButler
from lsst.pex.config import Config
from lsst.pipe.base import (
    AlgorithmError,
    QuantumAttemptStatus,
    QuantumSuccessCaveats,
    TaskMetadata,
)
from lsst.pipe.base import automatic_connection_constants as acc
from lsst.pipe.base.cli.cmd.commands import (
    aggregate_graph as aggregate_graph_cli,
)
from lsst.pipe.base.cli.cmd.commands import (
    provenance_report as provenance_report_cli,
)
from lsst.pipe.base.graph_walker import GraphWalker
from lsst.pipe.base.pipeline_graph import Edge
from lsst.pipe.base.quantum_graph import (
    FORMAT_VERSION,
    PredictedDatasetInfo,
    PredictedQuantumGraph,
    PredictedQuantumInfo,
    ProvenanceDatasetInfo,
    ProvenanceQuantumGraph,
    ProvenanceQuantumInfo,
    ProvenanceQuantumReport,
    ProvenanceReport,
    ProvenanceTaskMetadataModel,
)
from lsst.pipe.base.quantum_graph.aggregator import AggregatorConfig, FatalWorkerError, aggregate_graph
from lsst.pipe.base.quantum_graph.ingest_graph import ingest_graph
from lsst.pipe.base.resource_usage import QuantumResourceUsage
from lsst.pipe.base.single_quantum_executor import SingleQuantumExecutor
from lsst.pipe.base.tests.mocks import (
    DirectButlerRepo,
    DynamicConnectionConfig,
    DynamicTestPipelineTaskConfig,
)
from lsst.pipe.base.tests.util import patch_deterministic_uuid4
from lsst.utils.packages import Packages


@dataclasses.dataclass
class PrepInfo:
    """Struct of objects used in an aggregator test."""

    butler: Butler
    butler_path: str
    predicted: PredictedQuantumGraph
    predicted_path: str
    config: AggregatorConfig


class AggregatorTestCase(unittest.TestCase):
    """Unit tests for `lsst.pipe.base.quantum_graph.aggregator`."""

    @staticmethod
    @contextmanager
    def make_test_repo() -> Iterator[PrepInfo]:
        """Make a test data repository and predicted quantum graph.

        Returns
        -------
        prep_info : `PrepInfo`
            Objects used in aggregator tests.

        Notes
        -----
        The pipeline graph used by this task looks like this:

            ■  calibrate: {detector, visit}
          ╭─┤
          ■ │  consolidate: {visit}
            │
            ■  resample: {patch, visit}
            │
            ■  coadd: {band, patch}

        The data can be visualized via::

            python -m lsst.daf.butler.tests.registry_data.spatial

        One of the 'calibrate' quanta (visit=2, detector=2) is configured to
        fail with `lsst.pipe.base.AnnotatedPartialOutputsError`.  This lets us
        test both success-with-caveats and failures, depending on how we
        configure the executor.  This ``{visit: 2, detector: 2}`` data ID is
        the only one that overlaps ``{tract: 1, patch: 1}`` and
        ``{tract: 0, patch: 5}``, so it should chain to the 'resample' and
        'coadd' tasks, too.
        """
        with patch_deterministic_uuid4(100):
            with DirectButlerRepo.make_temporary("base.yaml", "spatial.yaml") as (helper, root):
                calibrate_config = DynamicTestPipelineTaskConfig()
                calibrate_config.fail_exception = "lsst.pipe.base.AnnotatedPartialOutputsError"
                calibrate_config.fail_condition = "visit=2 AND detector=2"
                helper.add_task(
                    "calibrate",
                    config=calibrate_config,
                    dimensions=["visit", "detector"],
                    inputs={
                        "input_image": DynamicConnectionConfig(
                            dataset_type_name="raw",
                            dimensions=["visit", "detector"],
                        )
                    },
                    prerequisite_inputs={
                        "refcat": DynamicConnectionConfig(
                            dataset_type_name="references",
                            dimensions=["htm7"],
                            multiple=True,
                        )
                    },
                    init_outputs={
                        "output_schema": DynamicConnectionConfig(
                            dataset_type_name="source_schema",
                        )
                    },
                    outputs={
                        "output_image": DynamicConnectionConfig(
                            dataset_type_name="image",
                            dimensions=["visit", "detector"],
                        ),
                        "output_table": DynamicConnectionConfig(
                            dataset_type_name="source_detector",
                            dimensions=["visit", "detector"],
                        ),
                    },
                )
                helper.add_task(
                    "consolidate",
                    dimensions=["visit"],
                    init_inputs={
                        "input_schema": DynamicConnectionConfig(
                            dataset_type_name="source_schema",
                        )
                    },
                    inputs={
                        "input_table": DynamicConnectionConfig(
                            dataset_type_name="source_detector",
                            dimensions=["visit", "detector"],
                            multiple=True,
                        )
                    },
                    outputs={
                        "output_table": DynamicConnectionConfig(
                            dataset_type_name="source",
                            dimensions=["visit"],
                        )
                    },
                )
                helper.add_task(
                    "resample",
                    dimensions=["patch", "visit"],
                    inputs={
                        "input_image": DynamicConnectionConfig(
                            dataset_type_name="image",
                            dimensions=["visit", "detector"],
                            multiple=True,
                        )
                    },
                    outputs={
                        "output_image": DynamicConnectionConfig(
                            dataset_type_name="warp",
                            dimensions=["patch", "visit"],
                        )
                    },
                )
                helper.add_task(
                    "coadd",
                    dimensions=["patch", "band"],
                    inputs={
                        "input_image": DynamicConnectionConfig(
                            dataset_type_name="warp",
                            dimensions=["patch", "visit"],
                            multiple=True,
                        )
                    },
                    outputs={
                        "output_image": DynamicConnectionConfig(
                            dataset_type_name="coadd",
                            dimensions=["patch", "band"],
                        ),
                    },
                )
                pqgc = helper.make_quantum_graph_builder().finish(output="out_chain")
                # We use the butler root for various QG files just because it's
                # a convenient temporary directory.
                predicted_path = os.path.join(root, "predicted.qg")
                pqgc.write(predicted_path)
                config = AggregatorConfig(
                    output_path=os.path.join(root, "provenance.qg"),
                    # Set these small to see logic paths that otherwise only
                    # affect large graphs.
                    ingest_batch_size=10,
                    zstd_dict_size=256,
                    zstd_dict_n_inputs=16,
                )
                yield PrepInfo(
                    butler=helper.butler.clone(collections="out_chain"),
                    butler_path=root,
                    predicted=pqgc.assemble(),
                    predicted_path=predicted_path,
                    config=config,
                )

    def iter_graph_execution(
        self,
        repo: str,
        qg: PredictedQuantumGraph,
        raise_on_partial_outputs: bool,
        is_retry: bool = False,
    ) -> Iterator[uuid.UUID]:
        """Return an iterator that executes and yields quanta one by one.

        Parameters
        ----------
        repo : `str`
            Butler repository path or alias.
        qg : `lsst.pipe.base.quantum_graph.PredictedQuantumGraph`
            Predicted quantum graph.  Must have datastore records attached,
            since execution uses a quantum-backed butler.
        raise_on_partial_outputs : `bool`
            Whether to raise on `lsst.pipe.base.AnnotatedPartialOutputsError`
            or treat it as a success with caveats.
        is_retry : `bool`, optional
            If `True`, this is a retry attempt and hence some outputs may
            already be present; skip successes and reprocess failures.

        Returns
        -------
        quanta : `~collections.abc.Iterator` [`uuid.UUID`]
            An iterator over all executed quantum IDs (not blocked ones).
        """
        qbb = qg.make_init_qbb(repo)
        self.enterContext(qbb)
        qg.init_output_run(qbb)
        sqe = SingleQuantumExecutor(
            limited_butler_factory=lambda quantum: QuantumBackedButler.initialize(
                repo,
                quantum,
                qg.pipeline_graph.universe,
            ),
            assume_no_existing_outputs=not is_retry,
            skip_existing=is_retry,
            clobber_outputs=is_retry,
            raise_on_partial_outputs=raise_on_partial_outputs,
        )
        qg.build_execution_quanta()
        xgraph = qg.quantum_only_xgraph
        walker = GraphWalker[uuid.UUID](xgraph.copy())
        for ready in walker:
            for quantum_id in ready:
                info = xgraph.nodes[quantum_id]
                try:
                    sqe.execute(info["pipeline_node"], info["quantum"], quantum_id)
                except AlgorithmError:
                    walker.fail(quantum_id)
                else:
                    walker.finish(quantum_id)
                yield quantum_id

    def check_provenance_graph(
        self,
        pred: PredictedQuantumGraph,
        butler: Butler,
        expect_failure: bool,
        start_time: float,
        expect_failures_retried: bool = False,
    ) -> ProvenanceQuantumGraph:
        """Run a batter of tests on a provenance quantum graph produced by
        scanning the graph created by `make_test_repo`.

        Parameters
        ----------
        pred: `lsst.pipe.base.quantum_graph.PredictedQuantumGraph`
            Predicted quantum graph.
        prov_reader : \
                `lsst.pipe.base.quantum_graph.ProvenanceQuantumGraphReader`
            Reader for the provenance quantum graph.
        butler : `lsst.daf.butler.Butler`
            Client for the data repository.
        expect_failure : `bool`
            Whether to expect one quantum of 'calibrate' to fail (`True`) or
            succeed without writing anything (`False`).
        start_time : `float`
            A POSIX timestamp that strictly precedes the start time of any
            quantum's execution.
        expect_failures_retried : `bool`, optional
            If `True`, expect an initial attempt with failures prior to the
            most recent attempt.

        Returns
        -------
        prov : `ProvenanceQuantumGraph`
            The full provenance quantum graph.
        """
        prov: ProvenanceQuantumGraph = butler.get("run_provenance")
        self.maxDiff = None
        self.assertEqual(prov.header.version, FORMAT_VERSION)
        self.assertEqual(
            list(butler.collections.get_info(prov.header.output).children),
            [prov.header.output_run]
            + list(butler.collections.query(prov.header.inputs, flatten_chains=True)),
        )
        self.assertEqual(pred.quanta_by_task.keys(), prov.quanta_by_task.keys())
        for task_label in pred.quanta_by_task:
            self.assertEqual(pred.quanta_by_task[task_label], prov.quanta_by_task[task_label])
        self.assertEqual(pred.datasets_by_type.keys() - {"packages"}, prov.datasets_by_type.keys())
        for dataset_type_name in prov.datasets_by_type:
            self.assertEqual(
                pred.datasets_by_type[dataset_type_name], prov.datasets_by_type[dataset_type_name]
            )
        self.assertEqual(prov.init_quanta.keys(), pred.quanta_by_task.keys())
        for quantum_id in pred:
            # Check consistency between the predicted and provenance quantum
            # node attributes.
            pred_qinfo: PredictedQuantumInfo = pred.bipartite_xgraph.nodes[quantum_id]
            prov_qinfo: ProvenanceQuantumInfo = prov.bipartite_xgraph.nodes[quantum_id]
            self.assertEqual(pred_qinfo["task_label"], prov_qinfo["task_label"])
            self.assertEqual(pred_qinfo["data_id"], prov_qinfo["data_id"])
            msg = f"{pred_qinfo['task_label']}@{pred_qinfo['data_id']}"
            # Check consistency between the predicted and provenance dataset
            # node attributes and edges.  Also gather existence information for
            # use later.
            existence: dict[str, list[bool]] = {}
            pipeline_edges: list[Edge]
            for dataset_id, _, pipeline_edges in pred.bipartite_xgraph.in_edges(
                quantum_id, data="pipeline_edges"
            ):
                self.assertTrue(prov.bipartite_xgraph.has_predecessor(quantum_id, dataset_id))
                for edge in pipeline_edges:
                    existence.setdefault(edge.connection_name, []).append(
                        self.check_dataset(dataset_id, pred, prov, butler)
                    )
            for _, dataset_id, pipeline_edges in pred.bipartite_xgraph.out_edges(
                quantum_id, data="pipeline_edges"
            ):
                self.assertTrue(prov.bipartite_xgraph.has_successor(quantum_id, dataset_id))
                for edge in pipeline_edges:
                    existence.setdefault(edge.connection_name, []).append(
                        self.check_dataset(dataset_id, pred, prov, butler)
                    )
            # Check quantum status and dataset existence against the known
            # structure of the graph and where failures/caveats occur.
            match (pred_qinfo["task_label"], dict(pred_qinfo["data_id"].required)):
                case "calibrate", {"visit": 2, "detector": 2}:
                    # This is the quantum that can directly raise.
                    self._expect_all_exist(existence["input_image"], msg=msg)
                    self._expect_all_exist(existence["refcat"], msg=msg)
                    self._expect_none_exist(existence["output_image"], msg=msg)
                    self._expect_none_exist(existence["output_table"], msg=msg)
                    if expect_failure:
                        self._expect_failure(prov_qinfo, existence, msg=msg)
                    else:
                        self._expect_successful(
                            prov_qinfo,
                            existence,
                            caveats=(
                                QuantumSuccessCaveats.PARTIAL_OUTPUTS_ERROR
                                | QuantumSuccessCaveats.ALL_OUTPUTS_MISSING
                                | QuantumSuccessCaveats.ANY_OUTPUTS_MISSING
                            ),
                            exception_type="lsst.pipe.base.tests.mocks.MockAlgorithmError",
                            msg=msg,
                        )
                    if expect_failures_retried:
                        self.assertEqual(len(prov_qinfo["attempts"]), 2)
                        self.assertEqual(
                            prov_qinfo["attempts"][0].exception.type_name,
                            "lsst.pipe.base.tests.mocks.MockAlgorithmError",
                        )
                    else:
                        self.assertEqual(len(prov_qinfo["attempts"]), 1)
                case "consolidate", {"visit": 2}:
                    # This quantum will succeed (with one predicted input
                    # missing) or be blocked.
                    self._expect_one_missing(existence["input_table"], msg=msg)
                    if expect_failure:
                        self._expect_blocked(prov_qinfo, existence, msg=msg)
                    else:
                        self._expect_successful(prov_qinfo, existence, msg=msg)
                    self.assertEqual(
                        len(prov_qinfo["attempts"]), expect_failures_retried or not expect_failure
                    )
                case (
                    "resample" | "coadd",
                    {"tract": 1, "patch": 1} | {"tract": 0, "patch": 5},
                ):
                    # These quanta will be blocked by an upstream failure or do
                    # chained caveats, since they won't have enough inputs to
                    # run.
                    if expect_failure:
                        self._expect_blocked(prov_qinfo, existence, msg=msg)
                    else:
                        self._expect_successful(
                            prov_qinfo,
                            existence,
                            caveats=(
                                QuantumSuccessCaveats.ADJUST_QUANTUM_RAISED
                                | QuantumSuccessCaveats.NO_WORK
                                | QuantumSuccessCaveats.ALL_OUTPUTS_MISSING
                                | QuantumSuccessCaveats.ANY_OUTPUTS_MISSING
                            ),
                            msg=msg,
                        )
                    self.assertEqual(
                        len(prov_qinfo["attempts"]), expect_failures_retried or not expect_failure
                    )
                case (
                    "resample",
                    {"tract": 0, "patch": 4, "visit": 2} | {"tract": 1, "patch": 0, "visit": 2},
                ):
                    # This will succeed or be blocked, with one input missing
                    # regardless.
                    self._expect_one_missing(existence["input_image"], msg=msg)
                    if expect_failure:
                        self._expect_blocked(prov_qinfo, existence, msg=msg)
                    else:
                        self._expect_successful(prov_qinfo, existence, msg=msg)
                    self.assertEqual(
                        len(prov_qinfo["attempts"]), expect_failures_retried or not expect_failure
                    )
                case (
                    "coadd",
                    {"tract": 0, "patch": 4, "band": "r"} | {"tract": 1, "patch": 0, "band": "r"},
                ):
                    # This will succeed with no inputs missing or be blocked
                    # with one input missing.
                    if expect_failure:
                        self._expect_one_missing(existence["input_image"], msg=msg)
                        self._expect_blocked(prov_qinfo, existence, msg=msg)
                    else:
                        self._expect_all_exist(existence["input_image"], msg=msg)
                        self._expect_successful(prov_qinfo, existence, msg=msg)
                    self.assertEqual(
                        len(prov_qinfo["attempts"]), expect_failures_retried or not expect_failure
                    )
                case _:
                    # All other quanta should succeed and have all inputs
                    # present.
                    for connection_name in prov_qinfo["pipeline_node"].inputs.keys():
                        self._expect_all_exist(existence[connection_name], msg=msg)
                    self._expect_successful(prov_qinfo, existence, msg=msg)
                    self.assertEqual(len(prov_qinfo["attempts"]), 1)
            self.check_metadata(
                quantum_id,
                prov,
                butler,
                expect_ingested=(prov_qinfo["status"] is QuantumAttemptStatus.SUCCESSFUL),
            )
            self.check_log(
                quantum_id,
                prov,
                butler,
                expect_ingested=(
                    prov_qinfo["status"]
                    in (
                        QuantumAttemptStatus.SUCCESSFUL,
                        QuantumAttemptStatus.FAILED,
                    )
                ),
            )
        self.check_resource_usage_table(prov, expect_failure=expect_failure, start_time=start_time)
        self.check_packages(butler)
        self.check_configs(butler, prov)
        self.check_quantum_table(prov, expect_failure=expect_failure)
        self.check_exception_table(prov, expect_failure=expect_failure)
        self.check_report(prov, expect_failure=expect_failure)
        return prov

    def _expect_all_exist(self, existence: list[bool], msg: str) -> None:
        self.assertTrue(all(existence), msg=msg)

    def _expect_none_exist(self, existence: list[bool], msg: str) -> None:
        self.assertFalse(any(existence), msg=msg)

    def _expect_one_missing(self, existence: list[bool], msg: str) -> None:
        self.assertEqual(existence.count(False), 1, msg=msg)

    def _expect_successful(
        self,
        info: ProvenanceQuantumInfo,
        existence: dict[str, list[bool]],
        caveats: QuantumSuccessCaveats = QuantumSuccessCaveats.NO_CAVEATS,
        exception_type: str | None = None,
        *,
        msg: str,
    ) -> None:
        self.assertEqual(info["status"], QuantumAttemptStatus.SUCCESSFUL, msg=msg)
        self.assertEqual(info["caveats"], caveats, msg=msg)
        if exception_type is None:
            self.assertIsNone(info["exception"], msg=msg)
        else:
            assert info["exception"] is not None
            self.assertEqual(info["exception"].type_name, exception_type, msg=msg)
        self._expect_all_exist(existence[acc.LOG_OUTPUT_CONNECTION_NAME], msg=msg)
        self._expect_all_exist(existence[acc.METADATA_OUTPUT_CONNECTION_NAME], msg=msg)
        if not (caveats & QuantumSuccessCaveats.ANY_OUTPUTS_MISSING):
            for connection_name in info["pipeline_node"].outputs.keys():
                self._expect_all_exist(existence[connection_name], msg=msg)
        if caveats & QuantumSuccessCaveats.ALL_OUTPUTS_MISSING:
            for connection_name in info["pipeline_node"].outputs.keys():
                self._expect_none_exist(existence[connection_name], msg=msg)
        self.assertIsNotNone(info["resource_usage"], msg=msg)
        self.assertGreater(info["resource_usage"].total_time, 0, msg=msg)
        self.assertGreater(info["resource_usage"].memory, 0, msg=msg)

    def _expect_failure(
        self, info: ProvenanceQuantumInfo, existence: dict[str, list[bool]], msg: str
    ) -> None:
        self.assertEqual(info["status"], QuantumAttemptStatus.FAILED, msg=msg)
        self.assertEqual(info["exception"].type_name, "lsst.pipe.base.tests.mocks.MockAlgorithmError")
        self._expect_all_exist(existence[acc.LOG_OUTPUT_CONNECTION_NAME], msg=msg)
        self._expect_none_exist(existence[acc.METADATA_OUTPUT_CONNECTION_NAME], msg=msg)
        for connection_name in info["pipeline_node"].outputs.keys():
            self._expect_none_exist(existence[connection_name], msg=msg)

    def _expect_blocked(
        self,
        info: ProvenanceQuantumInfo,
        existence: dict[str, list[bool]],
        msg: str,
    ) -> None:
        self.assertEqual(info["status"], QuantumAttemptStatus.BLOCKED, msg=msg)
        self.assertEqual(info["attempts"], [])
        self._expect_none_exist(existence[acc.LOG_OUTPUT_CONNECTION_NAME], msg=msg)
        self._expect_none_exist(existence[acc.METADATA_OUTPUT_CONNECTION_NAME], msg=msg)
        for connection_name in info["pipeline_node"].outputs.keys():
            self._expect_none_exist(existence[connection_name], msg=msg)

    def check_dataset(
        self,
        dataset_id: uuid.UUID,
        pred: PredictedQuantumGraph,
        prov: ProvenanceQuantumGraph,
        butler: Butler,
    ) -> bool:
        """Check a provenance dataset for consistency with its predicted
        counterpart.

        Parameters
        ----------
        dataset_id : `uuid.UUID`
            Unique ID for the dataset.
        pred: `lsst.pipe.base.quantum_graph.PredictedQuantumGraph`
            Predicted quantum graph.
        prov : `lsst.pipe.base.quantum_graph.ProvenanceQuantumGraph`
            Provenance quantum graph.
        butler : `lsst.daf.butler.Butler`
            Client for the data repository.

        Returns
        -------
        exists : `bool`
            Whether the dataset was marked as existing in the provenance
            quantum graph.
        """
        pred_info: PredictedDatasetInfo = pred.bipartite_xgraph.nodes[dataset_id]
        prov_info: ProvenanceDatasetInfo = prov.bipartite_xgraph.nodes[dataset_id]
        self.assertEqual(pred_info["dataset_type_name"], prov_info["dataset_type_name"])
        self.assertEqual(pred_info["data_id"], prov_info["data_id"])
        self.assertEqual(pred_info["run"], prov_info["run"])
        exists = prov_info["produced"]
        dataset_type_name = prov_info["dataset_type_name"]
        # We can remove this guard when we ingest QG-backed metadata and logs.
        if not dataset_type_name.endswith("_metadata") and not dataset_type_name.endswith("_log"):
            self.assertEqual(
                butler.get_dataset(dataset_id) is not None,
                exists,
                msg=(
                    f"Ingest/existence inconsistency for {dataset_type_name}"
                    f"@{prov_info['data_id']}/{dataset_id}]"
                ),
            )
        return exists

    def check_metadata(
        self, quantum_id: uuid.UUID, prov: ProvenanceQuantumGraph, butler: Butler, expect_ingested: bool
    ) -> None:
        """Check reading a metadata dataset from the butler, and check that the
        original metadata file has been deleted.

        Parameters
        ----------
        quantum_id : `uuid.UUID`
            Unique ID for the quantum this metadata belongs to.
        prov : `lsst.pipe.base.quantum_graph.ProvenanceQuantumGraph`
            Provenance quantum graph.
        butler : `lsst.daf.butler.Butler`
            Client for the data repository.
        expect_ingested : `bool`
            Whether the metadata dataset should have been ingested.
        """
        dataset_id = prov.bipartite_xgraph.nodes[quantum_id]["metadata_id"]
        ref = butler.get_dataset(dataset_id)
        if not expect_ingested:
            self.assertIsNone(ref)
            return
        assert ref is not None
        metadata = butler.get(ref)
        self.assertIsInstance(metadata, TaskMetadata)
        graph_path = butler.getURI("run_provenance")
        self.assertEqual(butler.getURI(ref), graph_path)
        # We now delete the metadata dataset, in order let us get the original
        # location from the butler and check that there's nothing there.  Note
        # that this doesn't actually delete the file because the butler knows
        # it's shared with other datasets.
        butler.pruneDatasets([ref], disassociate=True, unstore=True, purge=True)
        original_path = butler.getURI(ref, predict=True)
        self.assertTrue(graph_path.exists())
        self.assertNotEqual(graph_path, original_path)
        self.assertFalse(original_path.exists())

    def check_log(
        self, quantum_id: uuid.UUID, prov: ProvenanceQuantumGraph, butler: Butler, expect_ingested: bool
    ) -> None:
        """Check reading a log dataset from the butler, and check that the
        original log file has been deleted.

        Parameters
        ----------
        quantum_id : `uuid.UUID`
            Unique ID for the quantum this log belongs to.
        prov : `lsst.pipe.base.quantum_graph.ProvenanceQuantumGraph`
            Provenance quantum graph.
        butler : `lsst.daf.butler.Butler`
            Client for the data repository.
        expect_ingested : `bool`
            Whether the metadata dataset should have been ingested.
        """
        dataset_id = prov.bipartite_xgraph.nodes[quantum_id]["log_id"]
        ref = butler.get_dataset(dataset_id)
        if not expect_ingested:
            self.assertIsNone(ref)
            return
        assert ref is not None
        log = butler.get(ref)
        self.assertIsInstance(log, ButlerLogRecords)
        graph_path = butler.getURI("run_provenance")
        self.assertEqual(butler.getURI(ref), graph_path)
        # We now delete the log dataset, in order let us get the original
        # location from the butler and check that there's nothing there.  Note
        # that this doesn't actually delete the file because the butler knows
        # it's shared with other datasets.
        butler.pruneDatasets([ref], disassociate=True, unstore=True, purge=True)
        original_path = butler.getURI(ref, predict=True)
        self.assertTrue(graph_path.exists())
        self.assertNotEqual(graph_path, original_path)
        self.assertFalse(original_path.exists())

    def check_configs(self, butler: Butler, prov: ProvenanceQuantumGraph) -> None:
        for task_node in prov.pipeline_graph.tasks.values():
            config = butler.get(task_node.init.config_output.dataset_type_name)
            self.assertIsInstance(config, Config)

    def check_packages(self, butler: Butler) -> None:
        """Check fetching package versions from the provenance graph.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            Client for the data repository.
        """
        packages = butler.get("run_provenance.packages")
        self.assertIsInstance(packages, Packages)
        self.assertIn("pipe_base", packages)

    def check_resource_usage_table(
        self, prov: ProvenanceQuantumGraph, expect_failure: bool, start_time: float
    ) -> None:
        """Check building a resource usage table from the provenance graph.

        Parameters
        ----------
        prov : `lsst.pipe.base.quantum_graph.ProvenanceQuantumGraph`
            Reader for the provenance quantum graph.
        expect_failure : `bool`
            Whether to expect one quantum of 'calibrate' to fail (`True`) or
            succeed without writing anything (`False`).
        start_time : `float`
            A POSIX timestamp that strictly precedes the start time of any
            quantum's execution.
        """
        tbl = prov.make_task_resource_usage_table("calibrate", include_data_ids=True)
        self.assertEqual(len(tbl), prov.header.n_task_quanta["calibrate"])
        self.assertCountEqual(
            tbl.colnames,
            ["quantum_id"]
            + list(prov.pipeline_graph.tasks["calibrate"].dimensions.names)
            + list(QuantumResourceUsage.model_fields),
        )
        # Check that quantum start times are bounded by the before-execution
        # start_time and now.  This makes sure we didn't get any timezone
        # shenanigans.
        end_time = time.time()
        for quantum_start_time in tbl["start"]:
            self.assertGreater(quantum_start_time, start_time)
            self.assertLess(quantum_start_time, end_time)
        self.assertTrue(np.all(tbl["init_time"] >= 0.0))
        self.assertTrue(np.all(tbl["prep_time"] > 0.0))
        self.assertTrue(np.all(tbl["run_time"] >= 0.0))

    def check_quantum_table(self, prov: ProvenanceQuantumGraph, expect_failure: bool) -> None:
        """Check `ProvenanceQuantumGraph.make_quantum_table`.

        Parameters
        ----------
        prov : `lsst.pipe.base.quantum_graph.ProvenanceQuantumGraph`
            Reader for the provenance quantum graph.
        expect_failure : `bool`
            Whether to expect one quantum of 'calibrate' to fail (`True`) or
            succeed without writing anything (`False`).
        """
        t = prov.make_quantum_table()
        self.assertEqual(list(t["Task"]), ["calibrate", "consolidate", "resample", "coadd"])
        self.assertEqual(t["TOTAL"][0], 8)
        self.assertEqual(t["EXPECTED"][0], 8)
        self.assertEqual(t["Blocked"][0], 0)
        self.assertEqual(t["TOTAL"][1], 2)
        self.assertEqual(t["EXPECTED"][1], 2)
        self.assertEqual(t["TOTAL"][2], 10)
        self.assertEqual(t["EXPECTED"][2], 10)
        if expect_failure:
            # calibrate
            self.assertEqual(t["Successful"][0], 7)
            self.assertEqual(t["Caveats"][0], "")
            self.assertEqual(t["Failed"][0], 1)
            # consolidate
            self.assertEqual(t["Successful"][1], 1)
            self.assertEqual(t["Caveats"][1], "")
            self.assertEqual(t["Failed"][1], 0)
            self.assertEqual(t["Blocked"][1], 1)
            # resample
            self.assertEqual(t["Successful"][2], 6)
            self.assertEqual(t["Caveats"][2], "")
            self.assertEqual(t["Failed"][2], 0)
            self.assertEqual(t["Blocked"][2], 4)
        else:
            # calibrate
            self.assertEqual(t["Successful"][0], 8)
            self.assertEqual(t["Caveats"][0], "*P(1)")
            self.assertEqual(t["Failed"][0], 0)
            # consolidate
            self.assertEqual(t["Successful"][1], 2)
            self.assertEqual(t["Caveats"][1], "")
            self.assertEqual(t["Failed"][1], 0)
            self.assertEqual(t["Blocked"][1], 0)
            # resample
            self.assertEqual(t["Successful"][2], 10)
            self.assertEqual(t["Caveats"][2], "*A(2)")
            self.assertEqual(t["Failed"][2], 0)
            self.assertEqual(t["Blocked"][2], 0)

    def check_exception_table(self, prov: ProvenanceQuantumGraph, expect_failure: bool) -> None:
        """Check `ProvenanceQuantumGraph.make_exception_table`.

        Parameters
        ----------
        prov : `lsst.pipe.base.quantum_graph.ProvenanceQuantumGraph`
            Reader for the provenance quantum graph.
        expect_failure : `bool`
            Whether to expect one quantum of 'calibrate' to fail (`True`) or
            succeed without writing anything (`False`).
        """
        t = prov.make_exception_table()
        self.assertEqual(list(t["Task"]), ["calibrate"])
        self.assertEqual(list(t["Exception"]), ["lsst.pipe.base.tests.mocks.MockAlgorithmError"])
        self.assertEqual(list(t["Successes"]), [int(not expect_failure)])
        self.assertEqual(list(t["Failures"]), [int(expect_failure)])

    def check_report(self, prov: ProvenanceQuantumGraph, expect_failure: bool) -> None:
        """Check `ProvenanceQuantumGraph.make_report`.

        Parameters
        ----------
        prov : `lsst.pipe.base.quantum_graph.ProvenanceQuantumGraph`
            Reader for the provenance quantum graph.
        expect_failure : `bool`
            Whether to expect one quantum of 'calibrate' to fail (`True`) or
            succeed without writing anything (`False`).
        """
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as data_id_table_dir:
            report = prov.make_status_report(
                also=QuantumAttemptStatus.SUCCESSFUL, data_id_table_dir=data_id_table_dir
            )
            task_label = "calibrate"
            status_name = "FAILED" if expect_failure else "SUCCESSFUL"
            exc_type = "lsst.pipe.base.tests.mocks.MockAlgorithmError"
            self.assertEqual(report.root.keys(), {task_label})
            self.assertEqual(report.root[task_label].keys(), {status_name})
            self.assertEqual(report.root[task_label][status_name].keys(), {exc_type})
            self.assertEqual(len(report.root[task_label][status_name][exc_type]), 1)
            qr = report.root[task_label][status_name][exc_type][0]
            self.assertIsInstance(qr, ProvenanceQuantumReport)
            self.assertEqual(
                qr.data_id,
                {
                    "instrument": "Cam1",
                    "visit": 2,
                    "detector": 2,
                    "band": "r",
                    "day_obs": 20210909,
                    "physical_filter": "Cam1-R1",
                },
            )
            tbl = astropy.table.Table.read(
                os.path.join(data_id_table_dir, task_label, status_name, f"{exc_type}.ecsv")
            )
            self.assertCountEqual(tbl.colnames, ["instrument", "visit", "detector"])
            self.assertEqual(len(tbl), 1)
            self.assertEqual(list(tbl["instrument"]), ["Cam1"])
            self.assertEqual(list(tbl["detector"]), [2])
            self.assertEqual(list(tbl["visit"]), [2])

    def test_all_successful(self) -> None:
        """Test running a full graph with no failures, and then scanning the
        results with incomplete=False.
        """
        with self.make_test_repo() as prep:
            prep.config.incomplete = False
            start_time = time.time()
            attempted_quanta = list(
                self.iter_graph_execution(prep.butler_path, prep.predicted, raise_on_partial_outputs=False)
            )
            self.assertCountEqual(attempted_quanta, prep.predicted.quantum_only_xgraph.nodes.keys())
            aggregate_graph(prep.predicted_path, prep.butler_path, prep.config)
            ingest_graph(prep.butler_path, prep.config.output_path, transfer="move", batch_size=10)
            prov = self.check_provenance_graph(
                prep.predicted,
                prep.butler,
                expect_failure=False,
                start_time=start_time,
            )
            self.check_no_original_dirs(prep.butler_path, prov.header.output_run)
            for i, quantum_id in enumerate(attempted_quanta):
                qinfo: ProvenanceQuantumInfo = prov.quantum_only_xgraph.nodes[quantum_id]
                self.assertEqual(qinfo["attempts"][-1].previous_process_quanta, attempted_quanta[:i])

    def test_all_successful_two_phase(self) -> None:
        """Test running some of a graph with no failures, scanning with
        incomplete=True, then finishing the graph and scanning again.
        """
        with self.make_test_repo() as prep:
            start_time = time.time()
            execution_iter = self.iter_graph_execution(
                prep.butler_path, prep.predicted, raise_on_partial_outputs=False
            )
            attempted_quanta = list(itertools.islice(execution_iter, 9))
            self.assertEqual(len(attempted_quanta), 9)
            # Run the scanner while telling it execution is incomplete, so it
            # just abandons incomplete quanta and doesn't write the provenance
            # QG.
            prep.config.incomplete = True
            aggregate_graph(prep.predicted_path, prep.butler_path, prep.config)
            self.assertFalse(os.path.exists(cast(str, prep.config.output_path)))
            # Finish executing the quanta.
            attempted_quanta.extend(execution_iter)
            # Scan again, and write the provenance QG.
            prep.config.incomplete = False
            aggregate_graph(prep.predicted_path, prep.butler_path, prep.config)
            ingest_graph(prep.butler_path, prep.config.output_path, transfer="move", batch_size=10)
            prov = self.check_provenance_graph(
                prep.predicted,
                prep.butler,
                expect_failure=False,
                start_time=start_time,
            )
            for i, quantum_id in enumerate(attempted_quanta):
                qinfo: ProvenanceQuantumInfo = prov.quantum_only_xgraph.nodes[quantum_id]
                self.assertEqual(qinfo["attempts"][-1].previous_process_quanta, attempted_quanta[:i])

    def test_some_failed(self) -> None:
        """Test running a full graph with some failures, and then scanning the
        results with incomplete=False.
        """
        with self.make_test_repo() as prep:
            prep.config.incomplete = False
            start_time = time.time()
            attempted_quanta = list(
                self.iter_graph_execution(prep.butler_path, prep.predicted, raise_on_partial_outputs=True)
            )
            aggregate_graph(prep.predicted_path, prep.butler_path, prep.config)
            ingest_graph(prep.butler_path, prep.config.output_path, transfer="move", batch_size=10)
            prov = self.check_provenance_graph(
                prep.predicted,
                prep.butler,
                expect_failure=True,
                start_time=start_time,
            )
            self.check_no_original_dirs(prep.butler_path, prov.header.output_run)
            for i, quantum_id in enumerate(attempted_quanta):
                qinfo: ProvenanceQuantumInfo = prov.quantum_only_xgraph.nodes[quantum_id]
                self.assertEqual(qinfo["attempts"][-1].previous_process_quanta, attempted_quanta[:i])

    def test_some_failed_two_phase(self) -> None:
        """Test running a full graph with some failures, then scanning the
        results with incomplete=True, then scanning again with
        incomplete=False.
        """
        with self.make_test_repo() as prep:
            start_time = time.time()
            attempted_quanta = list(
                self.iter_graph_execution(prep.butler_path, prep.predicted, raise_on_partial_outputs=True)
            )
            prep.config.incomplete = True
            aggregate_graph(prep.predicted_path, prep.butler_path, prep.config)
            prep.config.incomplete = False
            aggregate_graph(prep.predicted_path, prep.butler_path, prep.config)
            ingest_graph(prep.butler_path, prep.config.output_path, transfer="move", batch_size=10)
            prov = self.check_provenance_graph(
                prep.predicted,
                prep.butler,
                expect_failure=True,
                start_time=start_time,
            )
            for i, quantum_id in enumerate(attempted_quanta):
                qinfo: ProvenanceQuantumInfo = prov.quantum_only_xgraph.nodes[quantum_id]
                self.assertEqual(qinfo["attempts"][-1].previous_process_quanta, attempted_quanta[:i])

    def test_retry(self) -> None:
        """Test running a full graph with some failures, rerunning the quanta
        that failed or were blocked in the first attempt, and then scanning
        for provenance.
        """
        with self.make_test_repo() as prep:
            start_time = time.time()
            attempted_quanta_1 = list(
                self.iter_graph_execution(prep.butler_path, prep.predicted, raise_on_partial_outputs=True)
            )
            attempted_quanta_2 = list(
                self.iter_graph_execution(
                    prep.butler_path, prep.predicted, raise_on_partial_outputs=False, is_retry=True
                )
            )
            aggregate_graph(prep.predicted_path, prep.butler_path, prep.config)
            ingest_graph(prep.butler_path, prep.config.output_path, transfer="move", batch_size=10)
            prov = self.check_provenance_graph(
                prep.predicted,
                prep.butler,
                expect_failure=False,
                start_time=start_time,
                expect_failures_retried=True,
            )
            for i, quantum_id in enumerate(attempted_quanta_1):
                qinfo: ProvenanceQuantumInfo = prov.quantum_only_xgraph.nodes[quantum_id]
                self.assertEqual(qinfo["attempts"][0].previous_process_quanta, attempted_quanta_1[:i])
            expected: list[uuid.UUID] = []
            for quantum_id in attempted_quanta_2:
                qinfo: ProvenanceQuantumInfo = prov.quantum_only_xgraph.nodes[quantum_id]
                if (
                    quantum_id in attempted_quanta_1
                    and qinfo["attempts"][0].status is QuantumAttemptStatus.SUCCESSFUL
                ):
                    # These weren't actually attempted twice, since they
                    # were already successful in the first round.
                    self.assertEqual(len(qinfo["attempts"]), 1)
                else:
                    self.assertEqual(qinfo["attempts"][-1].previous_process_quanta, expected)
                    expected.append(quantum_id)

    def test_promise_ingest_graph(self) -> None:
        """Test running with promise_ingest_graph=True."""
        with self.make_test_repo() as prep:
            prep.config.incomplete = False
            prep.config.promise_ingest_graph = True
            start_time = time.time()
            attempted_quanta = list(
                self.iter_graph_execution(prep.butler_path, prep.predicted, raise_on_partial_outputs=True)
            )
            aggregate_graph(prep.predicted_path, prep.butler_path, prep.config)
            self.assertFalse(prep.butler.query_datasets("calibrate_metadata", explain=False))
            self.assertFalse(prep.butler.query_datasets("consolidate_log", explain=False))
            self.assertFalse(prep.butler.query_datasets("resample_config", explain=False))
            ingest_graph(prep.butler_path, prep.config.output_path, transfer="move", batch_size=10)
            prov = self.check_provenance_graph(
                prep.predicted,
                prep.butler,
                expect_failure=True,
                start_time=start_time,
            )
            self.check_no_original_dirs(prep.butler_path, prov.header.output_run)
            for i, quantum_id in enumerate(attempted_quanta):
                qinfo: ProvenanceQuantumInfo = prov.quantum_only_xgraph.nodes[quantum_id]
                self.assertEqual(qinfo["attempts"][-1].previous_process_quanta, attempted_quanta[:i])

    def test_worker_failures(self) -> None:
        """Test that if failures occur on (multiple) workers we shut down
        gracefully instead of hanging.
        """
        with self.make_test_repo() as prep:
            with self.assertRaises(FatalWorkerError):
                aggregate_graph(prep.predicted_path, "nonexistent", prep.config)

    def test_aggregate_graph_cli_overrides(self) -> None:
        """Test that command-line options override config attributes as
        expected.
        """

        def mock_run(predicted_path: str, butler_path: str, config: AggregatorConfig) -> None:
            print(config.model_dump_json(indent=2))

        def check(result: Result, **kwargs: Any) -> None:
            self.assertEqual(result.exit_code, 0, msg=result.output)
            self.assertEqual(
                result.output.strip(), AggregatorConfig(**kwargs).model_dump_json(indent=2).strip()
            )

        self.maxDiff = None
        runner = CliRunner()
        with unittest.mock.patch("lsst.pipe.base.quantum_graph.aggregator.aggregate_graph", mock_run):
            check(runner.invoke(aggregate_graph_cli, ("pg", "repo")))
            check(runner.invoke(aggregate_graph_cli, ("pg", "repo", "--output", "out")), output_path="out")
            check(runner.invoke(aggregate_graph_cli, ("pg", "repo", "-j", "4")), n_processes=4)
            check(
                runner.invoke(aggregate_graph_cli, ("pg", "repo", "--incomplete")),
                incomplete=True,
            )
            check(runner.invoke(aggregate_graph_cli, ("pg", "repo", "--dry-run")), dry_run=True)
            check(
                runner.invoke(aggregate_graph_cli, ("pg", "repo", "--interactive-status")),
                interactive_status=True,
            )
            check(
                runner.invoke(aggregate_graph_cli, ("pg", "repo", "--log-status-interval", "120")),
                log_status_interval=120,
            )
            check(
                runner.invoke(aggregate_graph_cli, ("pg", "repo", "--no-register-dataset-types")),
                register_dataset_types=False,
            )
            check(
                runner.invoke(aggregate_graph_cli, ("pg", "repo", "--no-update-output-chain")),
                update_output_chain=False,
            )
            check(
                runner.invoke(aggregate_graph_cli, ("pg", "repo", "--worker-log-dir", "wlogs")),
                worker_log_dir="wlogs",
            )
            check(
                runner.invoke(aggregate_graph_cli, ("pg", "repo", "--worker-log-level", "DEBUG")),
                worker_log_level="DEBUG",
            )
            check(
                runner.invoke(aggregate_graph_cli, ("pg", "repo", "--zstd-level", "11")),
                zstd_level=11,
            )
            check(
                runner.invoke(aggregate_graph_cli, ("pg", "repo", "--zstd-dict-size", "143")),
                zstd_dict_size=143,
            )
            check(
                runner.invoke(aggregate_graph_cli, ("pg", "repo", "--zstd-dict-n-inputs", "2")),
                zstd_dict_n_inputs=2,
            )
            check(
                runner.invoke(aggregate_graph_cli, ("pg", "repo", "--mock-storage-classes")),
                mock_storage_classes=True,
            )

    def check_provenance_report(self, result: click.testing.Result, root: str) -> None:
        self.maxDiff = None
        self.assertEqual(result.exit_code, 0, msg=result.output)
        self.assertEqual(
            result.output,
            "    Task    Caveats Failed Blocked Successful TOTAL EXPECTED\n"
            "----------- ------- ------ ------- ---------- ----- --------\n"
            "  calibrate              1       0          7     8        8\n"
            "consolidate              0       1          1     2        2\n"
            "   resample              0       4          6    10       10\n"
            "      coadd              0       4          6    10       10\n"
            "\n"
            "   Task                     Exception                   Successes Failures\n"
            "--------- --------------------------------------------- --------- --------\n"
            "calibrate lsst.pipe.base.tests.mocks.MockAlgorithmError         0        1\n"
            "\n",
        )
        with open(os.path.join(root, "report.json")) as report_file:
            report = ProvenanceReport.model_validate_json(report_file.read())
            self.assertEqual(report.root.keys(), {"calibrate"})
        self.assertEqual(report.root["calibrate"].keys(), {"FAILED"})
        self.assertEqual(
            report.root["calibrate"]["FAILED"].keys(), {"lsst.pipe.base.tests.mocks.MockAlgorithmError"}
        )
        self.assertEqual(
            len(report.root["calibrate"]["FAILED"]["lsst.pipe.base.tests.mocks.MockAlgorithmError"]), 1
        )
        self.assertEqual(
            list(os.walk(os.path.join(root, "data_ids"))),
            [
                (os.path.join(root, "data_ids"), ["calibrate"], []),
                (os.path.join(root, "data_ids", "calibrate"), ["FAILED"], []),
                (
                    os.path.join(root, "data_ids", "calibrate", "FAILED"),
                    [],
                    ["lsst.pipe.base.tests.mocks.MockAlgorithmError.ecsv"],
                ),
            ],
        )

    def check_no_original_dirs(self, butler_path: str, output_run: str) -> None:
        """Check that there are no config/log/metadata directories in
        the butler's directory for this output run.
        """
        root = os.path.join(butler_path, output_run)
        for subdir in os.listdir(root):
            if subdir.endswith("_config") or subdir.endswith("_metadata") or subdir.endswith("_log"):
                raise AssertionError(f"Directory {os.path.join(root, subdir)} still exists.")

    def test_provenance_report_content(self) -> None:
        """Test the provenance-report CLI command."""
        with self.make_test_repo() as prep:
            prep.config.incomplete = False
            prep.config.promise_ingest_graph = True
            for _ in self.iter_graph_execution(
                prep.butler_path, prep.predicted, raise_on_partial_outputs=True
            ):
                pass
            aggregate_graph(prep.predicted_path, prep.butler_path, prep.config)
            self.assertFalse(prep.butler.query_datasets("calibrate_metadata", explain=False))
            self.assertFalse(prep.butler.query_datasets("consolidate_log", explain=False))
            self.assertFalse(prep.butler.query_datasets("resample_config", explain=False))

            # First test on a provenance graph file that has not been ingested.
            runner = CliRunner()
            report_root = os.path.join(prep.butler_path, "uningested")
            result = runner.invoke(
                provenance_report_cli,
                (
                    cast(str, prep.config.output_path),
                    "--status-report",
                    os.path.join(report_root, "report.json"),
                    "--data-id-table-dir",
                    os.path.join(report_root, "data_ids"),
                ),
            )
            self.check_provenance_report(result, report_root)

            ingest_graph(prep.butler_path, prep.config.output_path, transfer="move", batch_size=10)

            runner = CliRunner()
            report_root = os.path.join(prep.butler_path, "ingested")
            result = runner.invoke(
                provenance_report_cli,
                (
                    prep.butler_path,
                    *prep.butler.collections.defaults,
                    "--status-report",
                    os.path.join(report_root, "report.json"),
                    "--data-id-table-dir",
                    os.path.join(report_root, "data_ids"),
                ),
            )
            self.check_provenance_report(result, os.path.join(report_root))

    def test_provenance_report_cli_overrides(self) -> None:
        """Test the provenance-report CLI command with a mocked
        implementation.
        """

        class MakeManyReportsArgs(pydantic.BaseModel):
            status_report_file: str | None = None
            print_quantum_table: bool = True
            print_exception_table: bool = True
            states: list[QuantumAttemptStatus] = pydantic.Field(
                default_factory=lambda: [
                    QuantumAttemptStatus.FAILED,
                    QuantumAttemptStatus.ABORTED,
                    QuantumAttemptStatus.ABORTED_SUCCESS,
                ]
            )
            with_caveats: QuantumSuccessCaveats | None = None
            data_id_table_dir: str | None = None

            @pydantic.model_validator(mode="after")
            def _sort_states(self) -> MakeManyReportsArgs:
                self.states.sort(key=lambda e: e.value)
                return self

        class MockProvenanceQuantumGraph(pydantic.BaseModel):
            repo_or_filename: str
            collection: str | None
            quanta: list[uuid.UUID] | None = None
            datasets: list[uuid.UUID] | None = None
            writeable: bool = False
            make_many_reports_args: MakeManyReportsArgs | None = None

            @classmethod
            @contextmanager
            def from_args(
                cls, repo_or_filename: str, /, **kwargs: Any
            ) -> Iterator[tuple[MockProvenanceQuantumGraph, None]]:
                yield cls(repo_or_filename=repo_or_filename, **kwargs), None

            def make_many_reports(self, **kwargs: Any) -> None:
                self.make_many_reports_args = MakeManyReportsArgs(**kwargs)
                print(self.model_dump_json(indent=2))

        def check(
            result: Result, repo_or_filename: str, collection: str | None = None, **kwargs: Any
        ) -> None:
            self.assertEqual(result.exit_code, 0, msg=result.output)
            self.assertEqual(
                result.output.strip(),
                MockProvenanceQuantumGraph(
                    repo_or_filename=repo_or_filename,
                    collection=collection,
                    datasets=[],
                    writeable=False,
                    make_many_reports_args=MakeManyReportsArgs(**kwargs),
                ).model_dump_json(indent=2),
            )

        self.maxDiff = None
        runner = CliRunner()
        with unittest.mock.patch(
            "lsst.pipe.base.quantum_graph.ProvenanceQuantumGraph", MockProvenanceQuantumGraph
        ):
            check(
                runner.invoke(provenance_report_cli, ("repo", "collection1")),
                repo_or_filename="repo",
                collection="collection1",
            )
            check(
                runner.invoke(provenance_report_cli, ("filename1",)),
                repo_or_filename="filename1",
                collection=None,
            )
            check(
                runner.invoke(provenance_report_cli, ("repo", "collection1", "--no-quantum-table")),
                repo_or_filename="repo",
                collection="collection1",
                print_quantum_table=False,
            )
            check(
                runner.invoke(provenance_report_cli, ("repo", "collection1", "--no-exception-table")),
                repo_or_filename="repo",
                collection="collection1",
                print_exception_table=False,
            )
            check(
                runner.invoke(provenance_report_cli, ("repo", "collection1", "--status-report", "filename2")),
                repo_or_filename="repo",
                collection="collection1",
                status_report_file="filename2",
            )
            check(
                runner.invoke(provenance_report_cli, ("repo", "collection1", "--state", "SUCCESSFUL")),
                repo_or_filename="repo",
                collection="collection1",
                states=[
                    QuantumAttemptStatus.SUCCESSFUL,
                    QuantumAttemptStatus.FAILED,
                    QuantumAttemptStatus.ABORTED,
                    QuantumAttemptStatus.ABORTED_SUCCESS,
                ],
            )
            check(
                runner.invoke(provenance_report_cli, ("repo", "collection1", "--no-state", "FAILED")),
                repo_or_filename="repo",
                collection="collection1",
                states=[
                    QuantumAttemptStatus.ABORTED,
                    QuantumAttemptStatus.ABORTED_SUCCESS,
                ],
            )
            check(
                runner.invoke(
                    provenance_report_cli, ("repo", "collection1", "--caveat", "PARTIAL_OUTPUTS_ERROR")
                ),
                repo_or_filename="repo",
                collection="collection1",
                with_caveats=QuantumSuccessCaveats.PARTIAL_OUTPUTS_ERROR,
                states=[
                    QuantumAttemptStatus.SUCCESSFUL,
                    QuantumAttemptStatus.FAILED,
                    QuantumAttemptStatus.ABORTED,
                    QuantumAttemptStatus.ABORTED_SUCCESS,
                ],
            )
            check(
                runner.invoke(provenance_report_cli, ("repo", "collection1", "--data-id-table-dir", "dir1")),
                repo_or_filename="repo",
                collection="collection1",
                data_id_table_dir="dir1",
            )

    def test_bad_metadata_readable(self) -> None:
        """Test that consolidated metadata accidentally written with floats
        transformed to JSON null are now readable.
        """
        with open(os.path.join(os.path.dirname(__file__), "data", "DM-54057.json")) as stream:
            data = stream.read()
        prov_md = ProvenanceTaskMetadataModel.model_validate_json(data)
        self.assertTrue(np.isnan(prov_md.attempts[0]["calibrateImage:psf_measure_psf"]["spatialFitChi2"]))


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
