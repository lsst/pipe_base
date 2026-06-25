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
import unittest

import lsst.utils.tests
from lsst.daf.butler import Butler, DataCoordinate, DatasetType
from lsst.daf.butler.registry import UserExpressionError
from lsst.pipe.base import PipelineGraph, QuantumGraph
from lsst.pipe.base.all_dimensions_quantum_graph_builder import (
    AllDimensionsQuantumGraphBuilder,
    DatasetQueryConstraintVariant,
)
from lsst.pipe.base.tests import simpleQGraph
from lsst.pipe.base.tests.mocks import (
    DynamicConnectionConfig,
    DynamicTestPipelineTask,
    DynamicTestPipelineTaskConfig,
    InMemoryRepo,
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
        self.helper = InMemoryRepo()
        self.enterContext(self.helper)
        self.helper.add_task()
        self.helper.make_quantum_graph_builder(output_run="new_run")
        self.helper.butler.collections.register("prior_run")
        self._task_node = self.helper.pipeline_graph.tasks["task_auto1"]
        self._empty_data_id = DataCoordinate.make_empty(self.helper.butler.dimensions)

    def _insert(self, *names, run="prior_run"):
        """Register datasets with empty data IDs into a run collection."""
        for name in names:
            dt = self.helper.pipeline_graph.dataset_types[name].dataset_type
            self.helper.butler.registry.insertDatasets(dt, [self._empty_data_id], run=run)

    def _build(self, *, output_run="new_run", **kwargs):
        return AllDimensionsQuantumGraphBuilder(
            self.helper.pipeline_graph,
            self.helper.butler,
            input_collections=[self.helper.input_chain],
            output_run=output_run,
            **kwargs,
        ).build(attach_datastore_records=False)

    def test_not_skipped_without_skip_existing_in(self):
        """Without skip_existing_in, a quantum is never skipped even if
        metadata exists in an input collection.
        """
        self._insert(self._task_node.metadata_output.parent_dataset_type_name)
        qgraph = self._build()
        self.assertEqual(len(qgraph), 1)

    def test_skipped_when_metadata_exists(self):
        """With skip_existing_in, a quantum is skipped when its metadata
        dataset is present in the specified collections.
        """
        self._insert(self._task_node.metadata_output.parent_dataset_type_name)
        # Init-outputs required, otherwise InitInputMissingError.
        for edge in self._task_node.init.iter_all_outputs():
            self._insert(edge.parent_dataset_type_name)
        qgraph = self._build(skip_existing_in=["prior_run"])
        self.assertEqual(len(qgraph), 0)

    def test_not_skipped_when_metadata_absent(self):
        """With skip_existing_in, a quantum is not skipped when its metadata
        dataset is absent from the specified collections.
        """
        qgraph = self._build(skip_existing_in=["prior_run"])
        self.assertEqual(len(qgraph), 1)


class RetainedDatasetTypesTestCase(unittest.TestCase):
    """Tests for QuantumGraphBuilder.retained_dataset_types.

    dataset_auto0 -> task_auto1 -> dataset_auto1 -> task_auto2
    """

    def setUp(self):
        self.helper = InMemoryRepo()
        self.enterContext(self.helper)
        self.helper.add_task()
        self.helper.add_task()
        self.helper.make_quantum_graph_builder(output_run="new_run")
        self.helper.butler.collections.register("prior_run")
        self._task1 = self.helper.pipeline_graph.tasks["task_auto1"]
        self._task2 = self.helper.pipeline_graph.tasks["task_auto2"]
        self._empty_data_id = DataCoordinate.make_empty(self.helper.butler.dimensions)

    def _insert(self, *names, run="prior_run"):
        """Register datasets with empty data IDs into a run collection."""
        for name in names:
            dt = self.helper.pipeline_graph.dataset_types[name].dataset_type
            self.helper.butler.registry.insertDatasets(dt, [self._empty_data_id], run=run)

    def _build(self, *, output_run="new_run", **kwargs):
        return AllDimensionsQuantumGraphBuilder(
            self.helper.pipeline_graph,
            self.helper.butler,
            input_collections=[self.helper.input_chain],
            output_run=output_run,
            **kwargs,
        ).build(attach_datastore_records=False)

    def test_raises_without_skip_existing_in(self):
        """retained_dataset_types invalid without skip_existing_in."""
        with self.assertRaises(ValueError):
            self._build(retained_dataset_types=["dataset_auto1"])

    def test_ancestor_unskipped_when_output_not_retained(self):
        """task1 ran (metadata present) but did not retain its output;
        task2 must run.  Because dataset_auto1 is not retained, task1
        is unskipped to regenerate it.
        """
        # task1 succeeded previously, but dataset_auto1 not retained.
        self._insert(self._task1.metadata_output.parent_dataset_type_name)
        qgraph = self._build(
            skip_existing_in=["prior_run"],
            retained_dataset_types=["*_metadata"],
        )
        # Both tasks run: task1 regenerate dataset_auto1 for task2.
        self.assertEqual(len(qgraph), 2)

    def test_ancestor_not_unskipped_when_output_retained(self):
        """When the intermediate output is declared retained and is present in
        skip_existing_in, unskipping stops there and task1 remains skipped.
        """
        # task1 metadata and its output dataset_auto1 both present in
        # prior_run.
        self._insert(self._task1.metadata_output.parent_dataset_type_name)
        self._insert("dataset_auto1")
        for edge in self._task1.init.iter_all_outputs():
            self._insert(edge.parent_dataset_type_name)
        qgraph = self._build(
            skip_existing_in=["prior_run"],
            retained_dataset_types=["dataset_auto1", "*_metadata"],
        )
        # Only task2 runs.
        self.assertEqual(len(qgraph), 1)

    def test_both_skipped_when_both_have_metadata(self):
        """When both tasks have metadata, both remain skipped regardless of
        which outputs are not retained.
        """
        self._insert(self._task1.metadata_output.parent_dataset_type_name)
        self._insert(self._task2.metadata_output.parent_dataset_type_name)
        for edge in self._task1.init.iter_all_outputs():
            self._insert(edge.parent_dataset_type_name)
        for edge in self._task2.init.iter_all_outputs():
            self._insert(edge.parent_dataset_type_name)
        qgraph = self._build(
            skip_existing_in=["prior_run"],
            retained_dataset_types=["*_metadata"],
        )
        self.assertEqual(len(qgraph), 0)

    def test_unrecognised_pattern_warns(self):
        """Literal names and wildcard patterns that match nothing in the
        pipeline emit a WARNING log message.
        """
        with self.assertLogs("lsst.pipe.base.quantum_graph_builder", level="WARNING") as cm:
            self._build(
                skip_existing_in=["prior_run"],
                retained_dataset_types=["no_such_dataset_type", "no_such_*"],
            )
        self.assertTrue(any("no_such_dataset_type" in msg for msg in cm.output))
        self.assertTrue(any("no_such_*" in msg for msg in cm.output))

    def test_no_unskipping_when_all_retained(self):
        """'*' matches all dataset types; no ancestor unskipping occurs,
        equivalent to not providing retained_dataset_types.
        """
        # task1 ran; metadata and dataset_auto1 present.
        self._insert(self._task1.metadata_output.parent_dataset_type_name)
        self._insert("dataset_auto1")
        for edge in self._task1.init.iter_all_outputs():
            self._insert(edge.parent_dataset_type_name)
        qgraph = self._build(
            skip_existing_in=["prior_run"],
            retained_dataset_types=["*"],
        )
        # All types retained -> no unskipping -> task1 stays skipped,
        # only task2 runs.
        self.assertEqual(len(qgraph), 1)


class RetainedDatasetTypesThreeTaskTestCase(unittest.TestCase):
    """Tests for retained_dataset_types with a 3-task chain.

    Pipeline: dataset_auto0 -> task_auto1 -> dataset_auto1
                            -> task_auto2 -> dataset_auto2
                            -> task_auto3 -> dataset_auto3
    """

    def setUp(self):
        self.helper = InMemoryRepo()
        self.enterContext(self.helper)
        self.helper.add_task()
        self.helper.add_task()
        self.helper.add_task()
        self.helper.make_quantum_graph_builder(output_run="new_run")
        self.helper.butler.collections.register("prior_run")
        self._task1 = self.helper.pipeline_graph.tasks["task_auto1"]
        self._task2 = self.helper.pipeline_graph.tasks["task_auto2"]
        self._task3 = self.helper.pipeline_graph.tasks["task_auto3"]
        self._empty_data_id = DataCoordinate.make_empty(self.helper.butler.dimensions)

    def _insert(self, *names, run="prior_run"):
        """Register datasets with empty data IDs into a run collection."""
        for name in names:
            dt = self.helper.pipeline_graph.dataset_types[name].dataset_type
            self.helper.butler.registry.insertDatasets(dt, [self._empty_data_id], run=run)

    def _build(self, *, output_run="new_run", **kwargs):
        return AllDimensionsQuantumGraphBuilder(
            self.helper.pipeline_graph,
            self.helper.butler,
            input_collections=[self.helper.input_chain],
            output_run=output_run,
            **kwargs,
        ).build(attach_datastore_records=False)

    def test_unskipping_stops_at_retained_intermediate(self):
        """task2's output is retained and present in skip_existing_in.
        Only task3 runs, task1 and task2 remain skipped.
        """
        self._insert(self._task1.metadata_output.parent_dataset_type_name)
        self._insert(self._task2.metadata_output.parent_dataset_type_name)
        self._insert("dataset_auto2")
        for edge in self._task1.init.iter_all_outputs():
            self._insert(edge.parent_dataset_type_name)
        for edge in self._task2.init.iter_all_outputs():
            self._insert(edge.parent_dataset_type_name)
        qgraph = self._build(
            skip_existing_in=["prior_run"],
            retained_dataset_types=["dataset_auto2", "*_metadata"],
        )
        self.assertEqual(len(qgraph), 1)

    def test_full_chain_unskipped_when_none_retained(self):
        """task3 needs to run.  Unskipping walks back through
        dataset_auto2 (not retained) to unskip task2, then through
        dataset_auto1 (not retained) to unskip task1.  All three tasks
        run to regenerate the non-retained datasets.
        """
        self._insert(self._task1.metadata_output.parent_dataset_type_name)
        self._insert(self._task2.metadata_output.parent_dataset_type_name)
        qgraph = self._build(
            skip_existing_in=["prior_run"],
            retained_dataset_types=["*_metadata"],
        )
        self.assertEqual(len(qgraph), 3)


class PruneUnanchoredQuantaTestCase(unittest.TestCase):
    """Tests for the prune_unanchored_quanta behavior of QuantumGraphBuilder.

    Pipeline: auto0 -> source -> auto1 -> anchor -> auto2

    All tasks are dimensionless so each is one quantum.
    """

    def setUp(self):
        self.helper = InMemoryRepo()
        self.enterContext(self.helper)
        self.helper.add_task("source")
        self.helper.add_task("anchor")
        self.helper.make_quantum_graph_builder(output_run="output_run")

    def _build(self, **kwargs):
        return AllDimensionsQuantumGraphBuilder(
            self.helper.pipeline_graph,
            self.helper.butler,
            input_collections=[self.helper.input_chain],
            output_run="output_run",
            **kwargs,
        ).build(attach_datastore_records=False)

    def test_no_effect_without_parameter(self):
        """Without prune_unanchored_quanta, all quanta are kept."""
        qg = self._build()
        self.assertEqual(len(qg), 2)

    def test_no_pruning_when_anchor_reachable(self):
        """Anchor reachable from source quantum: nothing is pruned."""
        qg = self._build(prune_unanchored_quanta=("source", "anchor"))
        self.assertEqual(len(qg), 2)

    def test_all_pruned_when_anchor_label_absent(self):
        """Anchor is absent: all source quanta and task removed."""
        qg = self._build(prune_unanchored_quanta=("source", "no_such_task"))
        self.assertEqual(len(qg), 0)
        self.assertNotIn("source", {td.label for td in qg.iterTaskGraph()})

    def test_noop_when_source_label_absent(self):
        """source_label not in pipeline: nothing happens."""
        qg = self._build(prune_unanchored_quanta=("no_such_task", "anchor"))
        self.assertEqual(len(qg), 2)


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
