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

import itertools
import tempfile
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from typing import ClassVar

import lsst.utils.tests
from lsst.daf.butler import (
    Butler,
    DatasetRef,
    DatasetType,
    MissingDatasetTypeError,
    QuantumBackedButler,
    SerializedDatasetType,
    StorageClassFactory,
)
from lsst.daf.butler.registry import ConflictingDefinitionError
from lsst.pipe.base import QuantumGraph
from lsst.pipe.base.all_dimensions_quantum_graph_builder import AllDimensionsQuantumGraphBuilder
from lsst.pipe.base.pipeline_graph import PipelineGraph
from lsst.pipe.base.tests.mocks import (
    DynamicConnectionConfig,
    DynamicTestPipelineTask,
    DynamicTestPipelineTaskConfig,
    MockDataset,
)


def _have_example_storage_classes() -> bool:
    """Check whether some storage classes work as expected.

    Given that these have registered converters, it shouldn't actually be
    necessary to import those types in order to determine that they're
    convertible, but the storage class machinery is implemented such that types
    that can't be imported can't be converted, and while that's inconvenient
    here it's totally fine in non-testing scenarios where you only care about a
    storage class if you can actually use it.
    """
    getter = StorageClassFactory().getStorageClass
    return (
        getter("ArrowTable").can_convert(getter("ArrowAstropy"))
        and getter("ArrowAstropy").can_convert(getter("ArrowTable"))
        and getter("ArrowTable").can_convert(getter("DataFrame"))
        and getter("DataFrame").can_convert(getter("ArrowTable"))
    )


class InitOutputRunTestCase(unittest.TestCase):
    """Tests for the init_output_run methods of PipelineGraph and
    QuantumGraph.
    """

    INPUT_COLLECTION: ClassVar[str] = "overall_inputs"

    @contextmanager
    def make_butler(self) -> Iterator[Butler]:
        """Wrap a temporary local butler repository in a context manager."""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
            Butler.makeRepo(root)
            butler = Butler.from_config(root, writeable=True)
            yield butler

    @contextmanager
    def prep_butler(self, pipeline_graph: PipelineGraph) -> Iterator[Butler]:
        """Create a temporary local butler repository with the dataset types
        and input datasets needed by a pipeline graph.

        This also resolves the pipeline graph and checks dataset types
        immediately after they are registered, providing test coverage for the
        methods that do that.
        """
        with self.make_butler() as butler:
            butler.collections.register(self.INPUT_COLLECTION)
            pipeline_graph.resolve(butler.registry)
            with self.assertRaises(MissingDatasetTypeError):
                pipeline_graph.check_dataset_type_registrations(butler)
            pipeline_graph.register_dataset_types(butler)
            pipeline_graph.check_dataset_type_registrations(butler)
            for _, dataset_type_node in pipeline_graph.iter_overall_inputs():
                butler.put(
                    MockDataset(
                        dataset_id=None,
                        dataset_type=SerializedDatasetType(
                            name=dataset_type_node.name,
                            dimensions=[],
                            storageClass=dataset_type_node.storage_class_name,
                        ),
                        data_id={},
                        run=self.INPUT_COLLECTION,
                    ),
                    dataset_type_node.name,
                    run=self.INPUT_COLLECTION,
                )
            yield butler

    def find_init_output_refs(
        self, pipeline_graph: PipelineGraph, butler: Butler
    ) -> dict[str, list[DatasetRef]]:
        """Find the init-output datasets of a pipeline graph in a butler
        repository.

        Parameters
        ----------
        pipeline_graph : `PipelineGraph`
            Pipeline graph.
        butler : `Butler`
            Full butler client.

        Returns
        -------
        init_output_refs : `dict`
            Dataset references, keyed by task label.  Storage classes will
            match the data repository definitions of the dataset types.  The
            special 'packages' dataset type will be included under a '*' key.
        """
        init_output_refs: dict[str, list[DatasetRef]] = {}
        for task_node in pipeline_graph.tasks.values():
            init_output_refs_for_task: list[DatasetRef] = []
            for write_edge in task_node.init.iter_all_outputs():
                ref = butler.find_dataset(write_edge.dataset_type_name)
                # Check that the ref we got back uses the dataset type node's
                # definition of the dataset type (including storage class).
                self.assertEqual(
                    ref.datasetType, pipeline_graph.dataset_types[write_edge.dataset_type_name].dataset_type
                )
                # Remember the version of the ref that has the task's storage
                # class, in case they differ.
                init_output_refs_for_task.append(write_edge.adapt_dataset_ref(ref))
            init_output_refs[task_node.label] = init_output_refs_for_task
        init_output_refs["*"] = [butler.find_dataset(pipeline_graph.packages_dataset_type)]
        return init_output_refs

    def get_quantum_graph_init_output_refs(self, quantum_graph: QuantumGraph) -> dict[str, list[DatasetRef]]:
        """Extract dataset references from a QuantumGraph into the same form
        as returned by `find_init_output_refs`.
        """
        init_output_refs: dict[str, list[DatasetRef]] = {}
        for task_label in quantum_graph.pipeline_graph.tasks:
            init_output_refs[task_label] = quantum_graph.get_init_output_refs(task_label)
        init_output_refs["*"] = list(quantum_graph.globalInitOutputRefs())
        return init_output_refs

    def assert_init_output_refs_equal(
        self, a: dict[str, list[DatasetRef]], b: dict[str, list[DatasetRef]]
    ) -> None:
        """Check that two dictionaries of the form returned by
        `find_init_output_refs` are equal.
        """
        self.assertEqual(a.keys(), b.keys())
        for task_label, init_output_refs_for_task in a.items():
            self.assertCountEqual(init_output_refs_for_task, b[task_label])

    def check_qbb_consistency(
        self, init_output_refs: dict[str, list[DatasetRef]], qbb: QuantumBackedButler
    ) -> None:
        """Check that a quantum-backed butler sees all of the given datasets.

        Parameters
        ----------
        init_output_refs : `dict`
            Dataset references, keyed by task label.  Storage classes should
            match the data repository definitions of the dataset types.  The
            special 'packages' dataset type should be included under a '*' key.
        qbb : `lsst.daf.butler.QuantumBackedButler`
            A quantum-backed butler.
        """
        for task_label, init_output_refs_for_task in init_output_refs.items():
            for ref, stored_in in qbb.stored_many(init_output_refs_for_task).items():
                self.assertTrue(
                    stored_in, msg=f"Init-input {ref} of task {task_label} not stored according to QBB."
                )

    def init_with_pipeline_graph_first(
        self, pipeline_graph: PipelineGraph, butler: Butler, run: str
    ) -> QuantumGraph:
        """Test the init_output_run methods of PipelineGraph and QuantumGraph,
        using the former to actually write init-outputs (with later attempts
        correctly failing or doing nothing, depending on parameters).
        """
        butler = butler.clone(run=run, collections=[self.INPUT_COLLECTION, run])
        pipeline_graph.init_output_run(butler)
        init_output_refs = self.find_init_output_refs(pipeline_graph, butler)
        # Build a QG with the init outputs already in place.
        quantum_graph_builder = AllDimensionsQuantumGraphBuilder(
            pipeline_graph,
            butler,
            skip_existing_in=[run],
            output_run=run,
            input_collections=[self.INPUT_COLLECTION],
        )
        quantum_graph = quantum_graph_builder.build(
            metadata={"output_run": run}, attach_datastore_records=True
        )
        # Check that the QG refs are the same as the ones that were present
        # already.
        self.assert_init_output_refs_equal(
            self.get_quantum_graph_init_output_refs(quantum_graph),
            init_output_refs,
        )
        # Initialize with the pipeline graph, should be a no-op.
        pipeline_graph.init_output_run(butler)
        self.assert_init_output_refs_equal(
            self.find_init_output_refs(pipeline_graph, butler),
            init_output_refs,
        )
        # Initialize again with the QG; should be a no-op.
        quantum_graph.init_output_run(butler)
        self.assert_init_output_refs_equal(
            self.find_init_output_refs(pipeline_graph, butler),
            init_output_refs,
        )
        # Initialize again with the QG but tell it to expect an empty run.
        with self.assertRaises(ConflictingDefinitionError):
            quantum_graph.init_output_run(butler, existing=False)
        with self.assertRaises(ConflictingDefinitionError):
            quantum_graph.write_configs(butler, compare_existing=False)
        with self.assertRaises(ConflictingDefinitionError):
            quantum_graph.write_packages(butler, compare_existing=False)
        with self.assertRaises(ConflictingDefinitionError):
            quantum_graph.write_init_outputs(butler, skip_existing=False)
        # Make a QBB, check that it can see the init outputs.
        qbb = quantum_graph.make_init_qbb(butler._config)
        self.check_qbb_consistency(init_output_refs, qbb)
        # Use QBB to initialize again, should be a no-op.
        quantum_graph.init_output_run(qbb)
        self.check_qbb_consistency(init_output_refs, qbb)
        # Use QBB to initialize again but tell it to expect an empty run.
        with self.assertRaises(ConflictingDefinitionError):
            quantum_graph.init_output_run(qbb, existing=False)
        return quantum_graph

    def init_with_quantum_graph_first(
        self, pipeline_graph: PipelineGraph, butler: Butler, run: str
    ) -> QuantumGraph:
        """Test the init_output_run methods of PipelineGraph and QuantumGraph,
        using the latter to actually write init-outputs (with later attempts
        correctly failing or doing nothing, depending on parameters).
        """
        butler = butler.clone(run=run, collections=[self.INPUT_COLLECTION, run])
        # Build a QG.
        quantum_graph_builder = AllDimensionsQuantumGraphBuilder(
            pipeline_graph,
            butler,
            input_collections=[self.INPUT_COLLECTION],
        )
        quantum_graph = quantum_graph_builder.build(
            metadata={"output_run": run}, attach_datastore_records=True
        )
        # Initialize with the QG.
        quantum_graph.init_output_run(butler)
        # Check that the QG refs are the same as the ones we find in the repo.
        init_output_refs = self.find_init_output_refs(pipeline_graph, butler)
        self.assert_init_output_refs_equal(
            self.get_quantum_graph_init_output_refs(quantum_graph),
            init_output_refs,
        )
        # Initialize again with the QG; should be a no-op.
        quantum_graph.init_output_run(butler)
        self.assert_init_output_refs_equal(
            self.find_init_output_refs(pipeline_graph, butler),
            init_output_refs,
        )
        # Initialize again with the QG but tell it to expect an empty run.
        with self.assertRaises(ConflictingDefinitionError):
            quantum_graph.init_output_run(butler, existing=False)
        with self.assertRaises(ConflictingDefinitionError):
            quantum_graph.write_configs(butler, compare_existing=False)
        with self.assertRaises(ConflictingDefinitionError):
            quantum_graph.write_packages(butler, compare_existing=False)
        with self.assertRaises(ConflictingDefinitionError):
            quantum_graph.write_init_outputs(butler, skip_existing=False)
        # Initialize with the pipeline graph, should be a no-op.
        pipeline_graph.init_output_run(butler)
        # Make a QBB, check that it can see the init outputs.
        qbb = quantum_graph.make_init_qbb(butler._config)
        self.check_qbb_consistency(init_output_refs, qbb)
        # Use QBB to initialize again, should be a no-op.
        quantum_graph.init_output_run(qbb)
        self.check_qbb_consistency(init_output_refs, qbb)
        # Use QBB to initialize again but tell it to expect an empty run.
        with self.assertRaises(ConflictingDefinitionError):
            quantum_graph.init_output_run(qbb, existing=False)
        return quantum_graph

    def init_with_qbb_first(self, pipeline_graph: PipelineGraph, butler: Butler, run: str) -> QuantumGraph:
        """Test the init_output_run methods of PipelineGraph and QuantumGraph,
        using the latter a quantum-backed butler to actually write init-outputs
        (with later attempts correctly failing or doing nothing, depending on
        parameters).
        """
        butler = butler.clone(run=run, collections=[self.INPUT_COLLECTION, run])
        # Build a QG.
        quantum_graph_builder = AllDimensionsQuantumGraphBuilder(
            pipeline_graph,
            butler,
            input_collections=[self.INPUT_COLLECTION],
        )
        quantum_graph = quantum_graph_builder.build(
            metadata={"output_run": run}, attach_datastore_records=True
        )
        # Make a quantum-backed butler and use it to initialize the run.
        qbb = quantum_graph.make_init_qbb(butler._config)
        quantum_graph.init_output_run(qbb)
        init_output_refs = self.get_quantum_graph_init_output_refs(quantum_graph)
        self.check_qbb_consistency(init_output_refs, qbb)
        # Use QBB to initialize again, should be a no-op.
        self.check_qbb_consistency(init_output_refs, qbb)
        # Use QBB to initialize again but tell it to expect an empty run.
        with self.assertRaises(ConflictingDefinitionError):
            quantum_graph.init_output_run(qbb, existing=False)
        # Transferring datasets back to the main butler (i.e. insert DB entries
        # for them).
        butler.transfer_from(qbb, itertools.chain.from_iterable(init_output_refs.values()))
        # Check that the QG refs are the same as the ones we find in the repo.
        self.assert_init_output_refs_equal(
            self.find_init_output_refs(pipeline_graph, butler),
            init_output_refs,
        )
        # Initialize again with the QG; should be a no-op.
        quantum_graph.init_output_run(butler)
        self.assert_init_output_refs_equal(
            self.find_init_output_refs(pipeline_graph, butler),
            init_output_refs,
        )
        # Initialize again with the QG but tell it to expect an empty run.
        with self.assertRaises(ConflictingDefinitionError):
            quantum_graph.init_output_run(butler, existing=False)
        # Initialize with the pipeline graph, should be a no-op.
        pipeline_graph.init_output_run(butler)
        return quantum_graph

    def test_two_tasks_no_conversions(self) -> None:
        """Test a two-task pipeline with an overall init-input, an overall
        init-output, and an init-intermediate.
        """
        a_config = DynamicTestPipelineTaskConfig()
        a_config.inputs["i"] = DynamicConnectionConfig(dataset_type_name="input_runtime")
        a_config.outputs["o"] = DynamicConnectionConfig(dataset_type_name="intermediate_runtime")
        a_config.init_inputs["ii"] = DynamicConnectionConfig(dataset_type_name="input_init")
        a_config.init_outputs["io"] = DynamicConnectionConfig(dataset_type_name="intermediate_init")
        b_config = DynamicTestPipelineTaskConfig()
        b_config.inputs["i"] = DynamicConnectionConfig(dataset_type_name="intermediate_runtime")
        b_config.outputs["o"] = DynamicConnectionConfig(dataset_type_name="output_runtime")
        b_config.init_inputs["ii"] = DynamicConnectionConfig(dataset_type_name="intermediate_init")
        b_config.init_outputs["io"] = DynamicConnectionConfig(dataset_type_name="output_init")
        pipeline_graph = PipelineGraph()
        pipeline_graph.add_task("a", DynamicTestPipelineTask, a_config)
        pipeline_graph.add_task("b", DynamicTestPipelineTask, b_config)
        with self.prep_butler(pipeline_graph) as butler:
            self.init_with_pipeline_graph_first(pipeline_graph, butler, "run1")
            self.assertEqual(butler.get("a_config", collections="run1"), a_config)
            self.assertEqual(butler.get("b_config", collections="run1"), b_config)
            self.init_with_quantum_graph_first(pipeline_graph, butler, "run2")
            self.assertEqual(butler.get("a_config", collections="run2"), a_config)
            self.assertEqual(butler.get("b_config", collections="run2"), b_config)
            self.init_with_qbb_first(pipeline_graph, butler, "run3")
            self.assertEqual(butler.get("a_config", collections="run3"), a_config)
            self.assertEqual(butler.get("b_config", collections="run3"), b_config)

    def test_optional_input_unregistered(self) -> None:
        """Test that an optional input dataset type that is not registered is
        not considered an error.
        """
        a_config = DynamicTestPipelineTaskConfig()
        a_config.inputs["i"] = DynamicConnectionConfig(dataset_type_name="input_runtime", minimum=0)
        a_config.outputs["o"] = DynamicConnectionConfig(dataset_type_name="output_runtime")
        pipeline_graph = PipelineGraph()
        pipeline_graph.add_task("a", DynamicTestPipelineTask, a_config)
        with self.make_butler() as butler:
            pipeline_graph.resolve(butler.registry)
            butler.registry.registerDatasetType(pipeline_graph.dataset_types["a_config"].dataset_type)
            butler.registry.registerDatasetType(pipeline_graph.dataset_types["a_log"].dataset_type)
            butler.registry.registerDatasetType(pipeline_graph.dataset_types["a_metadata"].dataset_type)
            butler.registry.registerDatasetType(pipeline_graph.dataset_types["output_runtime"].dataset_type)
            pipeline_graph.check_dataset_type_registrations(butler, include_packages=False)

    def test_registration_changed(self) -> None:
        """Test that we get an error when dataset type registrations in a data
        repository change between the time a pipeline graph is resolved (e.g.
        at QG generation) and when dataset types are checked later (e.g. during
        execution).
        """
        a_config = DynamicTestPipelineTaskConfig()
        a_config.inputs["i"] = DynamicConnectionConfig(dataset_type_name="input_runtime")
        a_config.outputs["o"] = DynamicConnectionConfig(dataset_type_name="output_runtime")
        pipeline_graph = PipelineGraph()
        pipeline_graph.add_task("a", DynamicTestPipelineTask, a_config)
        with self.make_butler() as butler:
            pipeline_graph.resolve(butler.registry)
            pipeline_graph.register_dataset_types(butler)
            butler.registry.removeDatasetType("input_runtime")
            butler.registry.registerDatasetType(
                DatasetType("input_runtime", {"instrument"}, "StructuredDataList", universe=butler.dimensions)
            )
            with self.assertRaises(ConflictingDefinitionError):
                pipeline_graph.check_dataset_type_registrations(butler)

    @unittest.skipUnless(
        _have_example_storage_classes(), "Arrow/Astropy/Pandas storage classes are not available."
    )
    def test_init_intermediate_component(self) -> None:
        """Test init_output_run with an init-intermediate that is written as
        a composite and read as a component.
        """
        a_config = DynamicTestPipelineTaskConfig()
        a_config.inputs["i"] = DynamicConnectionConfig(dataset_type_name="input_runtime")
        a_config.outputs["o"] = DynamicConnectionConfig(dataset_type_name="intermediate_runtime")
        a_config.init_outputs["io"] = DynamicConnectionConfig(
            dataset_type_name="intermediate_init", storage_class="ArrowTable"
        )
        b_config = DynamicTestPipelineTaskConfig()
        b_config.inputs["i"] = DynamicConnectionConfig(dataset_type_name="intermediate_runtime")
        b_config.outputs["o"] = DynamicConnectionConfig(dataset_type_name="output_runtime")
        b_config.init_inputs["ii"] = DynamicConnectionConfig(
            dataset_type_name="intermediate_init.schema", storage_class="ArrowSchema"
        )
        pipeline_graph = PipelineGraph()
        pipeline_graph.add_task("a", DynamicTestPipelineTask, a_config)
        pipeline_graph.add_task("b", DynamicTestPipelineTask, b_config)
        with self.prep_butler(pipeline_graph) as butler:
            self.init_with_pipeline_graph_first(pipeline_graph, butler, "run1")
            self.assertEqual(butler.get("a_config", collections="run1"), a_config)
            self.assertEqual(butler.get("b_config", collections="run1"), b_config)
            self.init_with_quantum_graph_first(pipeline_graph, butler, "run2")
            self.assertEqual(butler.get("a_config", collections="run2"), a_config)
            self.assertEqual(butler.get("b_config", collections="run2"), b_config)
            self.init_with_qbb_first(pipeline_graph, butler, "run3")
            self.assertEqual(butler.get("a_config", collections="run3"), a_config)
            self.assertEqual(butler.get("b_config", collections="run3"), b_config)

    def test_no_get_init_input_callback(self) -> None:
        """Test calling PipelineGraph.instantiate_tasks with no get_init_input
        callback when one is necessary.
        """
        a_config = DynamicTestPipelineTaskConfig()
        a_config.inputs["i"] = DynamicConnectionConfig(dataset_type_name="input_runtime")
        a_config.outputs["o"] = DynamicConnectionConfig(dataset_type_name="output_runtime")
        a_config.init_inputs["ii"] = DynamicConnectionConfig(dataset_type_name="input_init")
        pipeline_graph = PipelineGraph()
        pipeline_graph.add_task("a", DynamicTestPipelineTask, a_config)
        with self.make_butler() as butler:
            pipeline_graph.resolve(butler.registry)
        with self.assertRaises(ValueError):
            pipeline_graph.instantiate_tasks()

    def test_multiple_init_input_consumers(self) -> None:
        """Test init_output_run when there are two tasks consuming the same
        init-input.
        """
        a_config = DynamicTestPipelineTaskConfig()
        a_config.inputs["i"] = DynamicConnectionConfig(dataset_type_name="input_runtime")
        a_config.outputs["o"] = DynamicConnectionConfig(dataset_type_name="intermediate_runtime")
        a_config.init_inputs["ii"] = DynamicConnectionConfig(dataset_type_name="input_init")
        a_config.init_outputs["io"] = DynamicConnectionConfig(dataset_type_name="output_init")
        b_config = DynamicTestPipelineTaskConfig()
        b_config.inputs["i"] = DynamicConnectionConfig(dataset_type_name="intermediate_runtime")
        b_config.outputs["o"] = DynamicConnectionConfig(dataset_type_name="output_runtime")
        b_config.init_inputs["ii"] = DynamicConnectionConfig(dataset_type_name="input_init")
        pipeline_graph = PipelineGraph()
        pipeline_graph.add_task("a", DynamicTestPipelineTask, a_config)
        pipeline_graph.add_task("b", DynamicTestPipelineTask, b_config)
        with self.prep_butler(pipeline_graph) as butler:
            self.init_with_pipeline_graph_first(pipeline_graph, butler, "run1")
            self.assertEqual(butler.get("a_config", collections="run1"), a_config)
            self.assertEqual(butler.get("b_config", collections="run1"), b_config)
            self.init_with_quantum_graph_first(pipeline_graph, butler, "run2")
            self.assertEqual(butler.get("a_config", collections="run2"), a_config)
            self.assertEqual(butler.get("b_config", collections="run2"), b_config)
            self.init_with_qbb_first(pipeline_graph, butler, "run3")
            self.assertEqual(butler.get("a_config", collections="run3"), a_config)
            self.assertEqual(butler.get("b_config", collections="run3"), b_config)

    def test_config_change(self) -> None:
        """Test init_output_run when there is an existing config that is
        inconsistent with the one in the pipeline graph.
        """
        a_config = DynamicTestPipelineTaskConfig()
        a_config.inputs["i"] = DynamicConnectionConfig(dataset_type_name="input_runtime")
        a_config.outputs["o"] = DynamicConnectionConfig(dataset_type_name="output_runtime")
        pipeline_graph = PipelineGraph()
        pipeline_graph.add_task("a", DynamicTestPipelineTask, a_config)
        with self.prep_butler(pipeline_graph) as butler:
            butler.collections.register("run1")
            butler.put(DynamicTestPipelineTaskConfig(), "a_config", run="run1")
            with self.assertRaises(ConflictingDefinitionError):
                pipeline_graph.init_output_run(
                    butler.clone(run="run1", collections=[self.INPUT_COLLECTION, "run1"])
                )
            quantum_graph_builder = AllDimensionsQuantumGraphBuilder(
                pipeline_graph,
                butler,
                skip_existing_in=["run1"],
                output_run="run1",
                input_collections=[self.INPUT_COLLECTION],
            )
            quantum_graph = quantum_graph_builder.build(
                metadata={"output_run": "run1"}, attach_datastore_records=True
            )
            with self.assertRaises(ConflictingDefinitionError):
                quantum_graph.init_output_run(
                    butler.clone(run="run1", collections=[self.INPUT_COLLECTION, "run1"])
                )


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
