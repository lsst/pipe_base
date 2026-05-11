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

"""Tests of things related to the GraphBuilder class."""

import io
import logging
import tempfile
import unittest

import numpy

import lsst.utils.tests
from lsst.daf.butler import Butler, DatasetType, ButlerLogRecords
from lsst.daf.butler.registry import UserExpressionError
from lsst.pipe.base import PipelineGraph, QuantumGraph, TaskMetadata
from lsst.pipe.base.all_dimensions_quantum_graph_builder import (
    AllDimensionsQuantumGraphBuilder,
    DatasetQueryConstraintVariant,
)
from lsst.pipe.base.quantum_graph_builder import OutputExistsError
from lsst.pipe.base.tests import simpleQGraph
from lsst.pipe.base.tests.mocks import (
    DynamicConnectionConfig,
    DynamicTestPipelineTask,
    DynamicTestPipelineTaskConfig,
    MockDataset,
    MockStorageClass,
)
from lsst.utils.tests import temporaryDirectory

_LOG = logging.getLogger(__name__)


class GraphBuilderTestCase(unittest.TestCase):
    """Test graph building."""

    def _assertGraph(self, graph: QuantumGraph) -> None:
        """Check basic structure of the graph."""
        for taskDef in graph.iterTaskGraph():
            refs = graph.initOutputRefs(taskDef)
            # task has one initOutput, second ref is for config dataset
            self.assertEqual(len(refs), 2)

        self.assertEqual(len(list(graph.inputQuanta)), 1)

        # This includes only "packages" dataset for now.
        refs = graph.globalInitOutputRefs()
        self.assertEqual(len(refs), 1)

    def testDefault(self):
        """Simple test to verify makeSimpleQGraph can be used to make a Quantum
        Graph.
        """
        with temporaryDirectory() as root:
            # makeSimpleQGraph calls GraphBuilder.
            butler, qgraph = simpleQGraph.makeSimpleQGraph(root=root)
            self.enterContext(butler)
            # by default makeSimpleQGraph makes a graph with 5 nodes
            self.assertEqual(len(qgraph), 5)
            self._assertGraph(qgraph)
            constraint = DatasetQueryConstraintVariant.OFF
            _, qgraph2 = simpleQGraph.makeSimpleQGraph(
                butler=butler, datasetQueryConstraint=constraint, callPopulateButler=False
            )
            # When all outputs are random resolved refs, direct comparison
            # of graphs does not work because IDs are different. Can only
            # verify the number of quanta in the graph without doing something
            # terribly complicated.
            self.assertEqual(len(qgraph2), 5)
            constraint = DatasetQueryConstraintVariant.fromExpression("add_dataset0")
            _, qgraph3 = simpleQGraph.makeSimpleQGraph(
                butler=butler, datasetQueryConstraint=constraint, callPopulateButler=False
            )
            self.assertEqual(len(qgraph3), 5)

    def test_empty_qg(self):
        """Test that making an empty QG doesn't raise exceptions."""
        config = DynamicTestPipelineTaskConfig()
        config.inputs["i"] = DynamicConnectionConfig(
            dataset_type_name="input",
            storage_class="StructuredDataDict",
            dimensions=["detector"],
        )
        config.init_inputs["ii"] = DynamicConnectionConfig(
            dataset_type_name="init_input",
            storage_class="StructuredDataDict",
        )
        config.outputs["o"] = DynamicConnectionConfig(
            dataset_type_name="output",
            storage_class="StructuredDataDict",
            dimensions=["detector"],
        )
        config.init_outputs["io"] = DynamicConnectionConfig(
            dataset_type_name="init_output",
            storage_class="StructuredDataDict",
        )
        pipeline_graph = PipelineGraph()
        pipeline_graph.add_task("a", DynamicTestPipelineTask, config)
        with temporaryDirectory() as repo_path:
            Butler.makeRepo(repo_path)
            butler = Butler.from_config(repo_path, writeable=True, run="test_empty_qg")
            self.enterContext(butler)
            MockStorageClass.get_or_register_mock("StructuredDataDict")
            butler.registry.registerDatasetType(
                DatasetType(
                    "input",
                    dimensions=butler.dimensions.conform(["detector"]),
                    storageClass="_mock_StructuredDataDict",
                )
            )
            init_input_dataset_type = DatasetType(
                "init_input",
                dimensions=butler.dimensions.empty,
                storageClass="_mock_StructuredDataDict",
            )
            butler.registry.registerDatasetType(init_input_dataset_type)
            # Init-input initially exists, but input does not (hence empty QG).
            butler.put(
                MockDataset(
                    dataset_id=None,
                    dataset_type=init_input_dataset_type.to_simple(),
                    data_id={},
                    run=butler.run,
                ),
                "init_input",
            )
            # Attempt to make QG; should just be empty, with no exceptions.
            self.assertFalse(AllDimensionsQuantumGraphBuilder(pipeline_graph, butler).build())
            # Initialize the output run, try again, with same expected result.
            pipeline_graph.register_dataset_types(butler)
            pipeline_graph.init_output_run(butler)
            self.assertFalse(AllDimensionsQuantumGraphBuilder(pipeline_graph, butler).build())

    # Inconsistent governor dimensions are no longer an error, so this test
    # fails with the new query system. We should probably check instead that
    # logging includes an explanation for the empty QG, but it might not
    # because explain_no_results isn't good enough yet.
    @unittest.expectedFailure
    def testAddInstrumentMismatch(self):
        """Verify that a RuntimeError is raised if the instrument in the user
        query does not match the instrument in the pipeline.
        """
        with temporaryDirectory() as root:
            pipeline = simpleQGraph.makeSimplePipeline(
                nQuanta=5, instrument="lsst.pipe.base.tests.simpleQGraph.SimpleInstrument"
            )
            with self.assertRaises(UserExpressionError):
                butler, _ = simpleQGraph.makeSimpleQGraph(
                    root=root, pipeline=pipeline, userQuery="instrument = 'foo'"
                )
                self.enterContext(butler)

    def testUserQueryBind(self):
        """Verify that bind values work for user query."""
        pipeline = simpleQGraph.makeSimplePipeline(
            nQuanta=5, instrument="lsst.pipe.base.tests.simpleQGraph.SimpleInstrument"
        )
        instr = simpleQGraph.SimpleInstrument.getName()
        # With a literal in the user query
        with temporaryDirectory() as root:
            butler, _ = simpleQGraph.makeSimpleQGraph(
                root=root, pipeline=pipeline, userQuery=f"instrument = '{instr}'"
            )
            self.enterContext(butler)
        # With a bind for the user query
        with temporaryDirectory() as root:
            butler, _ = simpleQGraph.makeSimpleQGraph(
                root=root, pipeline=pipeline, userQuery="instrument = :instr", bind={"instr": instr}
            )
            self.enterContext(butler)

    def test_datastore_records(self):
        """Test for generating datastore records."""
        with temporaryDirectory() as root:
            # need FileDatastore for this tests
            butler, qgraph1 = simpleQGraph.makeSimpleQGraph(
                root=root, inMemory=False, makeDatastoreRecords=True
            )
            self.enterContext(butler)

            # save and reload
            buffer = io.BytesIO()
            qgraph1.save(buffer)
            buffer.seek(0)
            qgraph2 = QuantumGraph.load(buffer, universe=butler.dimensions)
            del buffer

            for qgraph in (qgraph1, qgraph2):
                self.assertEqual(len(qgraph), 5)
                for i, qnode in enumerate(qgraph):
                    quantum = qnode.quantum
                    self.assertIsNotNone(quantum.datastore_records)
                    # only the first quantum has a pre-existing input
                    if i == 0:
                        datastore_name = "FileDatastore@<butlerRoot>"
                        self.assertEqual(set(quantum.datastore_records.keys()), {datastore_name})
                        records_data = quantum.datastore_records[datastore_name]
                        records = dict(records_data.records)
                        self.assertEqual(len(records), 1)
                        _, records = records.popitem()
                        records = records["file_datastore_records"]
                        self.assertEqual(
                            [record.path for record in records],
                            ["test/add_dataset0/add_dataset0_INSTR_det0_test.pickle"],
                        )
                    else:
                        self.assertEqual(quantum.datastore_records, {})


class SkipExistingInTestCase(unittest.TestCase):
    """Tests for the skip_existing_in behavior of QuantumGraphBuilder."""

    def setUp(self):
        repodir = tempfile.TemporaryDirectory()
        self.addCleanup(tempfile.TemporaryDirectory.cleanup, repodir)
        pipeline = simpleQGraph.makeSimplePipeline(nQuanta=1)
        butler, _ = simpleQGraph.makeSimpleQGraph(root=repodir.name, pipeline=pipeline, nQuanta=1)
        self.enterContext(butler)
        self.butler = butler
        self.pipeline_graph = pipeline.to_graph()
        self.butler.registry.registerRun("run")

    def test_not_skipped_without_skip_existing_in(self):
        """Without skip_existing_in, a quantum is never skipped even if
        metadata exists in an input collection.
        """
        self.butler.put(
            TaskMetadata(),
            "task0_metadata",
            run="run",
            instrument="INSTR",
            detector=0,
        )

        qgraph = AllDimensionsQuantumGraphBuilder(
            self.pipeline_graph, self.butler, input_collections=["test"], output_run="new_run"
        ).build()
        self.assertEqual(len(qgraph), 1)

    def test_skipped_when_metadata_exists(self):
        """With skip_existing_in, a quantum is skipped when its metadata
        dataset is present in the specified collections.
        """
        self.butler.put(
            TaskMetadata(),
            "task0_metadata",
            run="run",
            instrument="INSTR",
            detector=0,
        )
        # Init-outputs required, otherwise InitInputMissingError.
        self.butler.put(numpy.array([0.0]), "add_init_output1", run="run")
        self.butler.put(simpleQGraph.AddTaskConfig(), "task0_config", run="run")

        qgraph = AllDimensionsQuantumGraphBuilder(
            self.pipeline_graph,
            self.butler,
            skip_existing_in=["run"],
            input_collections=["test"],
            output_run="new_run",
        ).build()
        self.assertEqual(len(qgraph), 0)

    def test_not_skipped_when_metadata_absent(self):
        """With skip_existing_in, a quantum is not skipped when its metadata
        dataset is absent from the specified collections.
        """
        # No metadata put — run exists but is empty.

        qgraph = AllDimensionsQuantumGraphBuilder(
            self.pipeline_graph,
            self.butler,
            skip_existing_in=["run"],
            input_collections=["test"],
            output_run="new_run",
        ).build()
        self.assertEqual(len(qgraph), 1)


class IgnoreMetadataForTestCase(unittest.TestCase):
    """Tests for QuantumGraphBuilder.ignore_metadata_for."""

    def setUp(self):
        repodir = tempfile.TemporaryDirectory()
        self.addCleanup(tempfile.TemporaryDirectory.cleanup, repodir)
        pipeline = simpleQGraph.makeSimplePipeline(nQuanta=1)
        butler, _ = simpleQGraph.makeSimpleQGraph(root=repodir.name, pipeline=pipeline, nQuanta=1)
        self.enterContext(butler)
        self.butler = butler
        self.pipeline_graph = pipeline.to_graph()
        # Simulate a prior run and put a metadata.
        self.butler.registry.registerRun("run")
        self.butler.put(
            TaskMetadata(),
            "task0_metadata",
            run="run",
            instrument="INSTR",
            detector=0,
        )

    def test_not_skipped_when_outputs_missing(self):
        """With ignore_metadata_for, quantum is not skipped when science
        outputs are absent from skip_existing_in, even if metadata is present.

        A scenario is that an upstream pipeline ran and wrote
        metadata but did not retain output datasets.
        """
        qgraph = AllDimensionsQuantumGraphBuilder(
            self.pipeline_graph,
            self.butler,
            skip_existing_in=["run"],
            ignore_metadata_for=["task0"],
            input_collections=["test"],
            output_run="new_run",
        ).build()
        # Outputs are missing so the quantum is not skipped.
        self.assertEqual(len(qgraph), 1)

    def test_skips_when_all_outputs_present(self):
        """With ignore_metadata_for, quantum is skipped when all science
        outputs are present in skip_existing_in.
        """
        self.butler.put(numpy.array([0.0]), "add_dataset1", run="run", instrument="INSTR", detector=0)
        self.butler.put(numpy.array([0.0]), "add2_dataset1", run="run", instrument="INSTR", detector=0)
        self.butler.put(ButlerLogRecords.from_records([]), "task0_log", run="run", instrument="INSTR", detector=0)
        # Init-outputs required when all quanta are skipped.
        self.butler.put(numpy.array([0.0]), "add_init_output1", run="run")
        self.butler.put(simpleQGraph.AddTaskConfig(), "task0_config", run="run")

        qgraph = AllDimensionsQuantumGraphBuilder(
            self.pipeline_graph,
            self.butler,
            skip_existing_in=["run"],
            ignore_metadata_for=["task0"],
            input_collections=["test"],
            output_run="new_run",
        ).build()
        # All outputs found, so quantum should be skipped.
        self.assertEqual(len(qgraph), 0)

    def test_output_exists_error_when_partial_outputs(self):
        """With ignore_metadata_for, OutputExistsError is raised when some but
        not all science outputs exist in the output run and clobber is off.

        Because not all outputs are present, the task is not skipped.  The
        partial output already in the run is then "in the way" when the builder
        tries to plan writing it again.
        """
        self.butler.put(numpy.array([0.0]), "add_dataset1", run="run", instrument="INSTR", detector=0)
        # add2_dataset1 absent → not all outputs present → task not skipped

        with self.assertRaises(OutputExistsError):
            AllDimensionsQuantumGraphBuilder(
                self.pipeline_graph,
                self.butler,
                skip_existing_in=["run"],
                ignore_metadata_for=["task0"],
                input_collections=["test"],
                # Use the same run so that partial output is in the way.
                output_run="run",
            ).build()

    def test_partial_outputs_clobber(self):
        """With ignore_metadata_for and clobber=True, partial outputs in the
        output run are discarded and the task re-runs normally.
        """
        self.butler.put(numpy.array([0.0]), "add_dataset1", run="run", instrument="INSTR", detector=0)
        # add2_dataset1 absent → not all outputs present → task not skipped
        # clobber=True → add_dataset1 discarded from graph, task re-runs

        qgraph = AllDimensionsQuantumGraphBuilder(
            self.pipeline_graph,
            self.butler,
            skip_existing_in=["run"],
            ignore_metadata_for=["task0"],
            input_collections=["test"],
            output_run="run",
            clobber=True,
        ).build()
        self.assertEqual(len(qgraph), 1)


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
