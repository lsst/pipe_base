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

import dataclasses
import itertools
import os
import time
import unittest.mock
import uuid
from collections.abc import Iterator
from contextlib import contextmanager

import numpy as np
from click.testing import CliRunner, Result

import lsst.utils.tests
from lsst.daf.butler import Butler, ButlerLogRecords, DataCoordinate, DatasetRef, QuantumBackedButler
from lsst.pipe.base import AlgorithmError, QuantumSuccessCaveats, TaskMetadata
from lsst.pipe.base import automatic_connection_constants as acc
from lsst.pipe.base.cli.cmd.commands import aggregate_graph as aggregate_graph_cli
from lsst.pipe.base.graph_walker import GraphWalker
from lsst.pipe.base.pipeline_graph import Edge, TaskNode
from lsst.pipe.base.quantum_graph import (
    PredictedDatasetInfo,
    PredictedQuantumGraph,
    PredictedQuantumInfo,
    ProvenanceDatasetInfo,
    ProvenanceQuantumGraph,
    ProvenanceQuantumGraphReader,
    ProvenanceQuantumInfo,
)
from lsst.pipe.base.quantum_graph.aggregator import AggregatorConfig, FatalWorkerError, aggregate_graph
from lsst.pipe.base.quantum_provenance_graph import QuantumRunStatus
from lsst.pipe.base.resource_usage import QuantumResourceUsage
from lsst.pipe.base.single_quantum_executor import SingleQuantumExecutor
from lsst.pipe.base.tests.mocks import (
    DirectButlerRepo,
    DynamicConnectionConfig,
    DynamicTestPipelineTaskConfig,
)
from lsst.pipe.base.tests.util import patch_deterministic_uuid4
from lsst.resources import ResourcePath
from lsst.utils.packages import Packages


@dataclasses.dataclass
class PrepInfo:
    """Struct of objects used in an aggregator test."""

    butler: Butler
    butler_path: str
    predicted: PredictedQuantumGraph
    predicted_path: str
    config: AggregatorConfig
    start_time: float = dataclasses.field(default_factory=time.time)


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
                helper.butler.collections.defaults = ["out_chain"]
                yield PrepInfo(
                    butler=helper.butler,
                    butler_path=root,
                    predicted=pqgc.assemble(),
                    predicted_path=predicted_path,
                    config=config,
                )

    def iter_graph_execution(
        self, repo: ResourcePath, qg: PredictedQuantumGraph, raise_on_partial_outputs: bool
    ) -> Iterator[uuid.UUID]:
        """Return an iterator that executes and yields quanta one by one.

        Parameters
        ----------
        repo : `lsst.resources.ResourcePath`
            Butler repository path.
        qg : `lsst.pipe.base.quantum_graph.PredictedQuantumGraph`
            Predicted quantum graph.  Must have datastore records attached,
            since execution uses a quantum-backed butler.
        raise_on_partial_outputs : `bool`
            Whether to raise on `lsst.pipe.base.AnnotatedPartialOutputsError`
            or treat it as a success with caveats.

        Returns
        -------
        quanta : `~collections.abc.Iterator` [`uuid.UUID`]
            An iterator over successful quantum IDs.  Failed and blocked quanta
            are not included.
        """
        qg.init_output_run(qg.make_init_qbb(repo))
        sqe = SingleQuantumExecutor(
            limited_butler_factory=lambda quantum: QuantumBackedButler.initialize(
                repo,
                quantum,
                qg.pipeline_graph.universe,
            ),
            assume_no_existing_outputs=True,
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

    def check(
        self,
        prep: PrepInfo,
        prov_reader: ProvenanceQuantumGraphReader | None,
        expect_failure: bool,
    ) -> None:
        """Run a batter of tests on a provenance quantum graph produced by
        scanning the graph created by `make_test_repo`.

        Parameters
        ----------
        prep : `PrepInfo`
            Information used to set up the test.
        prov_reader : `None` or \
                `lsst.pipe.base.quantum_graph.ProvenanceQuantumGraphReader`
            Reader for the provenance quantum graph, or `None`, to only check
            that datasets were ingested into the central butler repository.
        expect_failure : `bool`
            Whether to expect one quantum of 'calibrate' to fail (`True`) or
            succeed without writing anything (`False`).
        """
        prov: ProvenanceQuantumGraph | None = None
        if prov_reader is not None:
            prov_reader.read_full_graph()
            prov = prov_reader.graph
            self.assertEqual(prep.predicted.quanta_by_task.keys(), prov.quanta_by_task.keys())
            for task_label in prep.predicted.quanta_by_task:
                self.assertEqual(prep.predicted.quanta_by_task[task_label], prov.quanta_by_task[task_label])
            self.assertEqual(
                prep.predicted.datasets_by_type.keys() - {"packages"}, prov.datasets_by_type.keys()
            )
            for dataset_type_name in prov.datasets_by_type:
                self.assertEqual(
                    prep.predicted.datasets_by_type[dataset_type_name],
                    prov.datasets_by_type[dataset_type_name],
                )
            self.assertEqual(prov.init_quanta.keys(), prep.predicted.quanta_by_task.keys())
        self.maxDiff = None
        self.assertEqual(
            list(prep.butler.collections.get_info(prep.predicted.header.output).children),
            [prep.predicted.header.output_run]
            + list(prep.butler.collections.query(prep.predicted.header.inputs, flatten_chains=True)),
        )

        for quantum_id in prep.predicted:
            # Check consistency between the predicted and provenance quantum
            # node attributes.
            pred_qinfo: PredictedQuantumInfo = prep.predicted.bipartite_xgraph.nodes[quantum_id]
            task_node = pred_qinfo["pipeline_node"]
            msg = f"{pred_qinfo['task_label']}@{pred_qinfo['data_id']}"
            prov_qinfo: ProvenanceQuantumInfo | None = None
            if prov is not None:
                prov_qinfo = prov.bipartite_xgraph.nodes[quantum_id]
                self.assertEqual(pred_qinfo["task_label"], prov_qinfo["task_label"])
                self.assertEqual(pred_qinfo["data_id"], prov_qinfo["data_id"])
            # Check consistency between the predicted and provenance dataset
            # node attributes and edges.  Also gather existence information for
            # use later.
            existence: dict[str, list[bool]] = {}
            pipeline_edges: list[Edge]
            for dataset_id, _, pipeline_edges in prep.predicted.bipartite_xgraph.in_edges(
                quantum_id, data="pipeline_edges"
            ):
                if prov is not None:
                    self.assertTrue(prov.bipartite_xgraph.has_predecessor(quantum_id, dataset_id))
                for edge in pipeline_edges:
                    existence.setdefault(edge.connection_name, []).append(
                        self.check_dataset(dataset_id, prep, prov)
                    )
            for _, dataset_id, pipeline_edges in prep.predicted.bipartite_xgraph.out_edges(
                quantum_id, data="pipeline_edges"
            ):
                if prov is not None:
                    self.assertTrue(prov.bipartite_xgraph.has_successor(quantum_id, dataset_id))
                for edge in pipeline_edges:
                    existence.setdefault(edge.connection_name, []).append(
                        self.check_dataset(dataset_id, prep, prov)
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
                        self._expect_failure(prov_qinfo, task_node, existence, msg=msg)
                    else:
                        self._expect_successful(
                            prov_qinfo,
                            task_node,
                            existence,
                            caveats=(
                                QuantumSuccessCaveats.PARTIAL_OUTPUTS_ERROR
                                | QuantumSuccessCaveats.ALL_OUTPUTS_MISSING
                                | QuantumSuccessCaveats.ANY_OUTPUTS_MISSING
                            ),
                            exception_type="lsst.pipe.base.tests.mocks.MockAlgorithmError",
                            msg=msg,
                        )
                case "consolidate", {"visit": 2}:
                    # This quantum will succeed (with one predicted input
                    # missing) or be blocked.
                    self._expect_one_missing(existence["input_table"], msg=msg)
                    if expect_failure:
                        self._expect_blocked(prov_qinfo, task_node, existence, msg=msg)
                    else:
                        self._expect_successful(prov_qinfo, task_node, existence, msg=msg)
                case (
                    "resample" | "coadd",
                    {"tract": 1, "patch": 1} | {"tract": 0, "patch": 5},
                ):
                    # These quanta will be blocked by an upstream failure or do
                    # chained caveats, since they won't have enough inputs to
                    # run.
                    if expect_failure:
                        self._expect_blocked(prov_qinfo, task_node, existence, msg=msg)
                    else:
                        self._expect_successful(
                            prov_qinfo,
                            task_node,
                            existence,
                            caveats=(
                                QuantumSuccessCaveats.ADJUST_QUANTUM_RAISED
                                | QuantumSuccessCaveats.NO_WORK
                                | QuantumSuccessCaveats.ALL_OUTPUTS_MISSING
                                | QuantumSuccessCaveats.ANY_OUTPUTS_MISSING
                            ),
                            msg=msg,
                        )
                case (
                    "resample",
                    {"tract": 0, "patch": 4, "visit": 2} | {"tract": 1, "patch": 0, "visit": 2},
                ):
                    # This will succeed or be blocked, with one input missing
                    # regardless.
                    self._expect_one_missing(existence["input_image"], msg=msg)
                    if expect_failure:
                        self._expect_blocked(prov_qinfo, task_node, existence, msg=msg)
                    else:
                        self._expect_successful(prov_qinfo, task_node, existence, msg=msg)
                case (
                    "coadd",
                    {"tract": 0, "patch": 4, "band": "r"} | {"tract": 1, "patch": 0, "band": "r"},
                ):
                    # This will succeed with no inputs missing or be blocked
                    # with one input missing.
                    if expect_failure:
                        self._expect_one_missing(existence["input_image"], msg=msg)
                        self._expect_blocked(prov_qinfo, task_node, existence, msg=msg)
                    else:
                        self._expect_all_exist(existence["input_image"], msg=msg)
                        self._expect_successful(prov_qinfo, task_node, existence, msg=msg)
                case _:
                    # All other quanta should succeed and have all inputs
                    # present.
                    for connection_name in task_node.inputs.keys():
                        self._expect_all_exist(existence[connection_name], msg=msg)
                    self._expect_successful(prov_qinfo, task_node, existence, msg=msg)
        # Spot-check datasets that may be backed by the provenance QG.
        # To test that deletes occurred, we need to ask the QBB where it would
        # write the original files, since the real Butler will refuse to
        # predict the location of a dataset that actually exists.
        qbb = QuantumBackedButler.from_predicted(
            prep.butler_path,
            predicted_inputs=[],
            predicted_outputs=[],
            dimensions=prep.butler.dimensions,
            datastore_records={},
        )
        data_id = DataCoordinate.standardize(
            visit=1, detector=1, instrument="Cam1", universe=prep.butler.dimensions
        )
        quantum_id = prep.predicted.quanta_by_task["calibrate"][data_id]
        self.check_metadata(quantum_id, prep, prov_reader, qbb)
        self.check_log(
            quantum_id,
            prep,
            prov_reader,
            qbb,
            ingested=prep.config.ingest_logs or not prep.config.ingest_provenance,
        )
        self.check_configs(
            prep, prov_reader, qbb, ingested=prep.config.ingest_configs or not prep.config.ingest_provenance
        )
        self.check_packages(
            prep, prov_reader, qbb, ingested=prep.config.ingest_packages or not prep.config.ingest_provenance
        )
        if prov is not None:
            self.check_resource_usage_table(prov, expect_failure=expect_failure, start_time=prep.start_time)
            self.check_quantum_table(prov, expect_failure=expect_failure)
            self.check_exception_table(prov, expect_failure=expect_failure)

    def _expect_all_exist(self, existence: list[bool], msg: str) -> None:
        self.assertTrue(all(existence), msg=msg)

    def _expect_none_exist(self, existence: list[bool], msg: str) -> None:
        self.assertFalse(any(existence), msg=msg)

    def _expect_one_missing(self, existence: list[bool], msg: str) -> None:
        self.assertEqual(existence.count(False), 1, msg=msg)

    def _expect_successful(
        self,
        info: ProvenanceQuantumInfo | None,
        task_node: TaskNode,
        existence: dict[str, list[bool]],
        caveats: QuantumSuccessCaveats = QuantumSuccessCaveats.NO_CAVEATS,
        exception_type: str | None = None,
        *,
        msg: str,
    ) -> None:
        self._expect_all_exist(existence[acc.LOG_OUTPUT_CONNECTION_NAME], msg=msg)
        self._expect_all_exist(existence[acc.METADATA_OUTPUT_CONNECTION_NAME], msg=msg)
        if not (caveats & QuantumSuccessCaveats.ANY_OUTPUTS_MISSING):
            for connection_name in task_node.outputs.keys():
                self._expect_all_exist(existence[connection_name], msg=msg)
        if caveats & QuantumSuccessCaveats.ALL_OUTPUTS_MISSING:
            for connection_name in task_node.outputs.keys():
                self._expect_none_exist(existence[connection_name], msg=msg)
        if info is not None:
            self.assertEqual(info["status"], QuantumRunStatus.SUCCESSFUL, msg=msg)
            self.assertEqual(info["caveats"], caveats, msg=msg)
            if exception_type is None:
                self.assertIsNone(info["exception"], msg=msg)
            else:
                assert info["exception"] is not None
                self.assertEqual(info["exception"].type_name, exception_type, msg=msg)
            self.assertIsNotNone(info["resource_usage"], msg=msg)
            self.assertGreater(info["resource_usage"].total_time, 0, msg=msg)
            self.assertGreater(info["resource_usage"].memory, 0, msg=msg)

    def _expect_failure(
        self,
        info: ProvenanceQuantumInfo | None,
        task_node: TaskNode,
        existence: dict[str, list[bool]],
        msg: str,
    ) -> None:
        self._expect_all_exist(existence[acc.LOG_OUTPUT_CONNECTION_NAME], msg=msg)
        self._expect_none_exist(existence[acc.METADATA_OUTPUT_CONNECTION_NAME], msg=msg)
        for connection_name in task_node.outputs.keys():
            self._expect_none_exist(existence[connection_name], msg=msg)
        if info is not None:
            self.assertEqual(info["status"], QuantumRunStatus.FAILED, msg=msg)

    def _expect_blocked(
        self,
        info: ProvenanceQuantumInfo | None,
        task_node: TaskNode,
        existence: dict[str, list[bool]],
        msg: str,
    ) -> None:
        self._expect_none_exist(existence[acc.LOG_OUTPUT_CONNECTION_NAME], msg=msg)
        self._expect_none_exist(existence[acc.METADATA_OUTPUT_CONNECTION_NAME], msg=msg)
        for connection_name in task_node.outputs.keys():
            self._expect_none_exist(existence[connection_name], msg=msg)
        if info is not None:
            self.assertEqual(info["status"], QuantumRunStatus.BLOCKED, msg=msg)

    def check_dataset(
        self,
        dataset_id: uuid.UUID,
        prep: PrepInfo,
        prov: ProvenanceQuantumGraph | None,
    ) -> bool:
        """Check a provenance dataset for consistency with its predicted
        counterpart.

        Parameters
        ----------
        dataset_id : `uuid.UUID`
            Unique ID for the dataset.
        prep: `PrepInfo`
            Information used to set up the test.
        prov : `lsst.pipe.base.quantum_graph.ProvenanceQuantumGraph` or `None`
            Provenance quantum graph.

        Returns
        -------
        exists : `bool`
            Whether the dataset was marked as existing in the provenance
            quantum graph.
        """
        pred_info: PredictedDatasetInfo = prep.predicted.bipartite_xgraph.nodes[dataset_id]
        dataset_type_name = pred_info["dataset_type_name"]
        exists = prep.butler.get_dataset(dataset_id) is not None
        if prov is not None:
            prov_info: ProvenanceDatasetInfo = prov.bipartite_xgraph.nodes[dataset_id]
            self.assertEqual(pred_info["dataset_type_name"], prov_info["dataset_type_name"])
            self.assertEqual(pred_info["data_id"], prov_info["data_id"])
            self.assertEqual(pred_info["run"], prov_info["run"])
            if in_edges := prep.predicted.bipartite_xgraph.in_edges(dataset_id, data="pipeline_edges"):
                ((_, _, producing_edges),) = in_edges
                if producing_edges[0].connection_name == acc.LOG_OUTPUT_CONNECTION_NAME and (
                    prep.config.ingest_provenance and not prep.config.ingest_logs
                ):
                    self.assertFalse(exists, "Log should not have been been ingested.")
                    exists = prov_info["exists"]  # as of the end of execution, that is.
            self.assertEqual(
                prov_info["exists"],
                exists,
                msg=(
                    f"Provenance vs. butler existence inconsistency for {dataset_type_name}"
                    f"@{prov_info['data_id']}/{dataset_id}]"
                ),
            )
        return exists

    def check_metadata(
        self,
        quantum_id: uuid.UUID,
        prep: PrepInfo,
        prov_reader: ProvenanceQuantumGraphReader | None,
        qbb: QuantumBackedButler,
    ) -> None:
        """Check reading a metadata dataset from the provenance reader, and
        check that the original metadata file was deleted if it was included in
        the provenance quantum graph.

        Parameters
        ----------
        quantum_id : `uuid.UUID`
            Unique ID for the quantum this metadata belongs to.
        prep: `PrepInfo`
            Information used to set up the test.
        prov_reader : `None` or \
                `lsst.pipe.base.quantum_graph.ProvenanceQuantumGraphReader`
            Reader for the provenance quantum graph.  `None` if the provenance
            graph was not written
        qbb : `lsst.daf.butler.QuantumBackedButler`
            Limited butler that can predict paths used during execution.
        """
        # Look up the dataset ID.
        for _, dataset_id, pipeline_edges in prep.predicted.bipartite_xgraph.out_edges(
            quantum_id, data="pipeline_edges"
        ):
            if pipeline_edges[0].connection_name == acc.METADATA_OUTPUT_CONNECTION_NAME:
                break
        else:
            raise AssertionError("No metadata connection found.")
        ref = prep.butler.get_dataset(dataset_id)
        assert ref is not None, "Metadata should always be ingested."
        if prov_reader is not None:
            # Try reading metadata through the quantum ID.
            (metadata1,) = prov_reader.fetch_metadata([quantum_id]).values()
            self.assertIsInstance(metadata1, TaskMetadata)
            # Also try reading metadata through the dataset ID.
            (metadata2,) = prov_reader.fetch_metadata([dataset_id]).values()
            self.assertEqual(metadata1, metadata2)
            # If we wrote a provenance QG, we should have deleted the original
            # file.
            ref = ref.expanded(prep.butler.registry.expandDataId(ref.dataId))
            self.assertFalse(qbb.stored(ref))

    def check_log(
        self,
        quantum_id: uuid.UUID,
        prep: PrepInfo,
        prov_reader: ProvenanceQuantumGraphReader | None,
        qbb: QuantumBackedButler,
        ingested: bool,
    ) -> None:
        """Check reading a log dataset from the provenance reader, and check
        that the original log file was deleted if it was included in the
        provenance quantum graph.

        Parameters
        ----------
        quantum_id : `uuid.UUID`
            Unique ID for the quantum this metadata belongs to.
        prep: `PrepInfo`
            Information used to set up the test.
        prov_reader : `None` or \
                `lsst.pipe.base.quantum_graph.ProvenanceQuantumGraphReader`
            Reader for the provenance quantum graph.  `None` if the provenance
            graph was not written
        qbb : `lsst.daf.butler.QuantumBackedButler`
            Limited butler that can predict paths used during execution.
        ingested : `bool`
            Whether log datasets should have been ingested as their own dataset
            types.
        """
        # Look up the dataset ID.
        for _, dataset_id, pipeline_edges in prep.predicted.bipartite_xgraph.out_edges(
            quantum_id, data="pipeline_edges"
        ):
            if pipeline_edges[0].connection_name == acc.LOG_OUTPUT_CONNECTION_NAME:
                break
        else:
            raise AssertionError("No log connection found.")
        ref = prep.butler.get_dataset(dataset_id)
        if ingested:
            self.assertIsNotNone(ref)
        else:
            info: PredictedDatasetInfo = prep.predicted.bipartite_xgraph.nodes[dataset_id]
            self.assertIsNone(ref)
            ref = DatasetRef(info["pipeline_node"].dataset_type, info["data_id"], info["run"], id=dataset_id)
        if prov_reader is not None:
            # Try reading log through the quantum ID.
            (log1,) = prov_reader.fetch_logs([quantum_id]).values()
            self.assertIsInstance(log1, ButlerLogRecords)
            # Also try reading log through the dataset ID.
            (log2,) = prov_reader.fetch_logs([dataset_id]).values()
            self.assertEqual(log1, log2)
            # If we wrote a provenance QG, we should have deleted the original
            # file.
            ref = ref.expanded(prep.butler.registry.expandDataId(ref.dataId))
            self.assertFalse(qbb.stored(ref))

    def check_configs(
        self,
        prep: PrepInfo,
        prov_reader: ProvenanceQuantumGraphReader | None,
        qbb: QuantumBackedButler,
        ingested: bool,
    ) -> None:
        """Check reading a config dataset from the provenance reader,
        and check that the original config was deleted if it was included in
        the provenance quantum graph.

        Parameters
        ----------
        prep: `PrepInfo`
            Information used to set up the test.
        prov_reader : `None` or \
                `lsst.pipe.base.quantum_graph.ProvenanceQuantumGraphReader`
            Reader for the provenance quantum graph.  `None` if the provenance
            graph was not written
        qbb : `lsst.daf.butler.QuantumBackedButler`
            Limited butler that can predict paths used during execution.
        ingested : `bool`
            Whether log datasets should have been ingested as their own dataset
            types.
        """
        dataset_type_name = acc.CONFIG_INIT_OUTPUT_TEMPLATE.format(label="calibrate")
        ref = prep.butler.find_dataset(dataset_type_name, {})
        if ingested:
            self.assertIsNotNone(ref)
        else:
            self.assertIsNone(ref)
            ref = prep.predicted.get_init_outputs("calibrate")[acc.CONFIG_INIT_OUTPUT_CONNECTION_NAME]
        if prov_reader is not None:
            self.assertEqual(
                prep.predicted.pipeline_graph.tasks["calibrate"].get_config_str(),
                prov_reader.pipeline_graph.tasks["calibrate"].get_config_str(),
            )
            # If we wrote a provenance QG, we should have deleted the original
            # file.
            self.assertFalse(qbb.stored(ref))

    def check_packages(
        self,
        prep: PrepInfo,
        prov_reader: ProvenanceQuantumGraphReader | None,
        qbb: QuantumBackedButler,
        ingested: bool,
    ) -> None:
        """Check fetching package versions from the provenance graph.

        Parameters
        ----------
        prep: `PrepInfo`
            Information used to set up the test.
        prov_reader : `None` or \
                `lsst.pipe.base.quantum_graph.ProvenanceQuantumGraphReader`
            Reader for the provenance quantum graph.  `None` if the provenance
            graph was not written
        qbb : `lsst.daf.butler.QuantumBackedButler`
            Limited butler that can predict paths used during execution.
        ingested : `bool`
            Whether log datasets should have been ingested as their own dataset
            types.
        """
        ref = prep.butler.find_dataset("packages", {})
        if ingested:
            self.assertIsNotNone(ref)
        else:
            self.assertIsNone(ref)
            ref = prep.predicted.get_init_outputs("")[acc.PACKAGES_INIT_OUTPUT_NAME]
        if prov_reader is not None:
            packages = prov_reader.fetch_packages()
            self.assertIsInstance(packages, Packages)
            self.assertIn("pipe_base", packages)
            # If we wrote a provenance QG, we should have deleted the original
            # file.
            self.assertFalse(qbb.stored(ref))

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
        if expect_failure:
            self.assertEqual(len(tbl), prov.header.n_task_quanta["calibrate"] - 1)
        else:
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
        self.assertTrue(np.all(t["Unknown"] == 0))
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
        if expect_failure:
            self.assertEqual(len(t), 0)
        else:
            self.assertEqual(list(t["Task"]), ["calibrate"])
            self.assertEqual(list(t["Exception"]), ["lsst.pipe.base.tests.mocks.MockAlgorithmError"])
            self.assertEqual(list(t["Count"]), [1])

    def test_all_successful_no_ingest_provenance(self) -> None:
        """Test running a full graph with no failures, and then scanning the
        results with ingest_provenance=False.
        """
        with self.make_test_repo() as prep:
            prep.config.ingest_provenance = False
            executed_quanta = list(
                self.iter_graph_execution(prep.butler_path, prep.predicted, raise_on_partial_outputs=False)
            )
            self.assertCountEqual(executed_quanta, prep.predicted.quantum_only_xgraph.nodes.keys())
            aggregate_graph(prep.predicted_path, prep.butler_path, prep.config)
            self.check(prep, None, expect_failure=False)

    def test_all_successful_ingest_provenance(self) -> None:
        """Test running a full graph with no failures, and then scanning the
        results with ingest_provenance=True.
        """
        with self.make_test_repo() as prep:
            prep.config.ingest_provenance = True
            executed_quanta = list(
                self.iter_graph_execution(prep.butler_path, prep.predicted, raise_on_partial_outputs=False)
            )
            self.assertCountEqual(executed_quanta, prep.predicted.quantum_only_xgraph.nodes.keys())
            aggregate_graph(prep.predicted_path, prep.butler_path, prep.config)
            with ProvenanceQuantumGraphReader.open(prep.config.output_path) as reader:
                self.check(prep, reader, expect_failure=False)

    def test_all_successful_two_phase_no_ingest_provenance(self) -> None:
        """Test running some of a graph with no failures, scanning with
        incomplete=True, then finishing the graph and scanning again, with
        ingest_provenance=False.
        """
        with self.make_test_repo() as prep:
            execution_iter = self.iter_graph_execution(
                prep.butler_path, prep.predicted, raise_on_partial_outputs=False
            )
            executed_quanta = list(itertools.islice(execution_iter, 9))
            self.assertEqual(len(executed_quanta), 9)
            # Run the aggregator while telling it to assume failures might
            # change, so it just waits for incomplete quanta to finish (and
            # then times out).
            prep.config.incomplete = True
            prep.config.ingest_provenance = True
            aggregate_graph(prep.predicted_path, prep.butler_path, prep.config)
            self.assertFalse(os.path.exists(prep.config.output_path))
            # Finish executing the quanta.
            executed_quanta.extend(execution_iter)
            # Scan again, and write the provenance QG.
            prep.config.incomplete = False
            # Run the aggregator again.
            aggregate_graph(prep.predicted_path, prep.butler_path, prep.config)
            self.check(prep, None, expect_failure=False)

    def test_all_successful_two_phase_ingest_provenance(self) -> None:
        """Test running some of a graph with no failures, scanning with
        incomplete=True, then finishing the graph and scanning again, all with
        ingest_provenance=True.
        """
        with self.make_test_repo() as prep:
            execution_iter = self.iter_graph_execution(
                prep.butler_path, prep.predicted, raise_on_partial_outputs=False
            )
            executed_quanta = list(itertools.islice(execution_iter, 9))
            self.assertEqual(len(executed_quanta), 9)
            # Run the aggregator while telling it to assume failures might
            # change, so it just waits for incomplete quanta to finish (and
            # then times out).
            prep.config.incomplete = True
            prep.config.ingest_provenance = True
            aggregate_graph(prep.predicted_path, prep.butler_path, prep.config)
            self.assertFalse(os.path.exists(prep.config.output_path))
            # Finish executing the quanta.
            executed_quanta.extend(execution_iter)
            # Scan again, and write the provenance QG.
            prep.config.incomplete = False
            # Run the aggregator again.
            aggregate_graph(prep.predicted_path, prep.butler_path, prep.config)
            with ProvenanceQuantumGraphReader.open(prep.config.output_path) as reader:
                self.check(prep, reader, expect_failure=False)

    def test_some_failed_no_ingest_provenance(self) -> None:
        """Test running a full graph with some failures, and then scanning the
        results with ingest_provenance=True.
        """
        with self.make_test_repo() as prep:
            prep.config.incomplete = False
            for _ in self.iter_graph_execution(
                prep.butler_path, prep.predicted, raise_on_partial_outputs=True
            ):
                pass
            aggregate_graph(prep.predicted_path, prep.butler_path, prep.config)
            self.check(prep, None, expect_failure=True)

    def test_some_failed_ingest_provenance(self) -> None:
        """Test running a full graph with some failures, and then scanning the
        results with ingest_provenance=True.
        """
        with self.make_test_repo() as prep:
            prep.config.incomplete = False
            prep.config.ingest_provenance = True
            for _ in self.iter_graph_execution(
                prep.butler_path, prep.predicted, raise_on_partial_outputs=True
            ):
                pass
            aggregate_graph(prep.predicted_path, prep.butler_path, prep.config)
            with ProvenanceQuantumGraphReader.open(prep.config.output_path) as reader:
                self.check(prep, reader, expect_failure=True)

    def test_some_failed_ingest_minimal_provenance(self) -> None:
        """Test running a full graph with some failures, and then scanning the
        results with ingest_provenance=True, but disable ingestion of logs,
        configs, and packages (since those will be available via the provenance
        QG).
        """
        with self.make_test_repo() as prep:
            prep.config.incomplete = False
            prep.config.ingest_provenance = True
            prep.config.ingest_logs = False
            prep.config.ingest_configs = False
            prep.config.ingest_packages = False
            for _ in self.iter_graph_execution(
                prep.butler_path, prep.predicted, raise_on_partial_outputs=True
            ):
                pass
            aggregate_graph(prep.predicted_path, prep.butler_path, prep.config)
            with ProvenanceQuantumGraphReader.open(prep.config.output_path) as reader:
                self.check(prep, reader, expect_failure=True)

    def test_some_failed_two_phase_no_ingest_provenance(self) -> None:
        """Test running a full graph with some failures, then scanning the
        results with incomplete=True, then scanning again with
        incomplete=False, all with ingest_provenance=False.
        """
        with self.make_test_repo() as prep:
            prep.config.ingest_provenance = False
            for _ in self.iter_graph_execution(
                prep.butler_path, prep.predicted, raise_on_partial_outputs=True
            ):
                pass
            prep.config.incomplete = True
            aggregate_graph(prep.predicted_path, prep.butler_path, prep.config)
            self.assertFalse(os.path.exists(prep.config.output_path))
            prep.config.incomplete = False
            aggregate_graph(prep.predicted_path, prep.butler_path, prep.config)
            self.check(prep, None, expect_failure=True)

    def test_some_failed_two_phase_ingest_provenance(self) -> None:
        """Test running a full graph with some failures, then scanning the
        results with incomplete=True, then scanning again with
        incomplete=False, all with ingest_provenance=True.
        """
        with self.make_test_repo() as prep:
            prep.config.ingest_provenance = True
            for _ in self.iter_graph_execution(
                prep.butler_path, prep.predicted, raise_on_partial_outputs=True
            ):
                pass
            prep.config.incomplete = True
            aggregate_graph(prep.predicted_path, prep.butler_path, prep.config)
            self.assertFalse(os.path.exists(prep.config.output_path))
            prep.config.incomplete = False
            aggregate_graph(prep.predicted_path, prep.butler_path, prep.config)
            with ProvenanceQuantumGraphReader.open(prep.config.output_path) as reader:
                self.check(prep, reader, expect_failure=True)

    def test_worker_failures(self) -> None:
        """Test that if failures occur on (multiple) workers we shut down
        gracefully instead of hanging.
        """
        with self.make_test_repo() as prep:
            with self.assertRaises(FatalWorkerError):
                aggregate_graph(prep.predicted_path, "nonexistent", prep.config)

    def test_cli_overrides(self) -> None:
        """Test that command-line options override config attributes as
        expected.
        """

        def mock_run(predicted_path: str, butler_path: str, config: AggregatorConfig) -> None:
            print(config.model_dump_json(indent=2))

        def check(result: Result, **kwargs: object) -> None:
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


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
