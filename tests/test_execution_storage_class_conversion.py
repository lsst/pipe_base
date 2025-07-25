# This file is part of ctrl_mpexec.
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

import os
import shutil
import tempfile
import unittest

import lsst.daf.butler
import lsst.utils.tests
from lsst.ctrl.mpexec import SimplePipelineExecutor
from lsst.pipe.base import PipelineGraph
from lsst.pipe.base.pipeline_graph import IncompatibleDatasetTypeError
from lsst.pipe.base.tests.mocks import (
    DynamicConnectionConfig,
    DynamicTestPipelineTask,
    DynamicTestPipelineTaskConfig,
    MockStorageClass,
    get_mock_name,
)

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class TestExecutionStorageClassConversion(lsst.utils.tests.TestCase):
    """Test storage class conversions during execution.

    Task connection declarations should always define which storage class they
    see, while data repository registrations should always define what is
    stored.

    This test uses mock storage classes for intermediate and output datasets,
    which let us load the dataset to see what storage class the task saw when
    it was running.  These storage class names need to be wrapped in
    get_mock_name calls to get what the butler actually sees.  Overall input
    datasets are not declared with mock datasets, so we can `put` them directly
    in test code.
    """

    def setUp(self):
        self.path = tempfile.mkdtemp()
        # standalone parameter forces the returned config to also include
        # the information from the search paths.
        config = lsst.daf.butler.Butler.makeRepo(
            self.path, standalone=True, searchPaths=[os.path.join(TESTDIR, "config")]
        )
        self.butler = SimplePipelineExecutor.prep_butler(config, [], "fake")
        self.butler.registry.registerDatasetType(
            lsst.daf.butler.DatasetType(
                "input",
                dimensions=self.butler.dimensions.empty,
                storageClass="StructuredDataDict",
            )
        )
        self.butler.put({"zero": 0}, "input")
        MockStorageClass.get_or_register_mock("StructuredDataDict")
        MockStorageClass.get_or_register_mock("TaskMetadataLike")

    def tearDown(self):
        shutil.rmtree(self.path, ignore_errors=True)

    def _make_config(
        self,
        input_storage_class="StructuredDataDict",
        output_storage_class="StructuredDataDict",
        input_name="input",
        output_name="output",
    ):
        """Create configuration for a test task with a single input and single
        output of the given storage classes and dataset type names.
        """
        config = DynamicTestPipelineTaskConfig()
        config.inputs["i"] = DynamicConnectionConfig(
            dataset_type_name=input_name,
            storage_class=input_storage_class,
            # Since the overall input is special, we only use a mock storage
            # class for it when there's a storage class conversion.
            mock_storage_class=(input_name != "input" or (input_storage_class != "StructuredDataDict")),
        )
        config.outputs["o"] = DynamicConnectionConfig(
            dataset_type_name=output_name, storage_class=output_storage_class
        )
        return config

    def _make_executor(
        self,
        a_i_storage_class="StructuredDataDict",
        a_o_storage_class="StructuredDataDict",
        b_i_storage_class="StructuredDataDict",
        b_o_storage_class="StructuredDataDict",
    ):
        """Configure a SimplePipelineExecutor with tasks with the given
        storage classes as inputs and outputs.

        This sets up a simple pipeline with two tasks ('a' and 'b') where the
        second task's only input is the first task's only output.
        """
        config_a = self._make_config(a_i_storage_class, a_o_storage_class, output_name="intermediate")
        config_b = self._make_config(b_i_storage_class, b_o_storage_class, input_name="intermediate")
        pipeline_graph = PipelineGraph()
        pipeline_graph.add_task("a", DynamicTestPipelineTask, config_a)
        pipeline_graph.add_task("b", DynamicTestPipelineTask, config_b)
        executor = SimplePipelineExecutor.from_pipeline_graph(pipeline_graph, butler=self.butler)
        return executor

    def _assert_datasets(
        self,
        a_i_storage_class="StructuredDataDict",
        a_o_storage_class="StructuredDataDict",
        b_i_storage_class="StructuredDataDict",
        b_o_storage_class="StructuredDataDict",
        stored_intermediate_storage_class="StructuredDataDict",
        stored_output_storage_class="StructuredDataDict",
        butler: lsst.daf.butler.Butler | None = None,
    ):
        """Check that a butler repository's contents are consistent with
        running a pipeline created by _make_executor.
        """
        if butler is None:
            butler = self.butler
        # Read input and output datasets from butler, inspect their storage
        # classes directly.
        stored_intermediate = butler.get("intermediate")
        stored_output = butler.get("output")
        self.assertEqual(
            butler.get_dataset_type("intermediate").storageClass_name,
            get_mock_name(stored_intermediate_storage_class),
        )
        self.assertEqual(stored_output.storage_class, get_mock_name(stored_output_storage_class))
        self.assertEqual(
            butler.get_dataset_type("output").storageClass_name,
            get_mock_name(stored_output_storage_class),
        )
        # Since we didn't tell the butler to convert storage classes on read,
        # they'll remember their last conversion (on write).
        if a_o_storage_class != stored_intermediate_storage_class:
            self.assertEqual(
                stored_intermediate.converted_from.storage_class,
                get_mock_name(a_o_storage_class),
            )
        else:
            self.assertIsNone(stored_intermediate.converted_from)
        if b_o_storage_class != stored_output_storage_class:
            self.assertEqual(
                stored_output.converted_from.storage_class,
                get_mock_name(b_o_storage_class),
            )
        else:
            self.assertIsNone(stored_output.converted_from)
        # Extract the inputs as seen by the tasks from those stored outputs.
        quantum_a = stored_intermediate.quantum
        quantum_b = stored_output.quantum
        b_input = quantum_b.inputs["i"][0]
        a_input = quantum_a.inputs["i"][0]
        if a_i_storage_class == "StructuredDataDict":
            self.assertIsNone(a_input.converted_from, None)
        else:
            self.assertEqual(a_input.original_type, "dict")
        self.assertEqual(b_input.storage_class, get_mock_name(b_i_storage_class))

    def test_no_conversions(self):
        """Test execution with no storage class conversions as a baseline."""
        executor = self._make_executor()
        quanta = executor.run(register_dataset_types=True, save_versions=False)
        self.assertEqual(len(quanta), 2)
        self._assert_datasets()

    def test_intermediate_registration_differs(self):
        """Test execution where an intermediate is registered to be different
        from both the producing and consuming task.
        """
        self.butler.registry.registerDatasetType(
            lsst.daf.butler.DatasetType(
                "intermediate",
                dimensions=self.butler.dimensions.empty,
                storageClass=get_mock_name("TaskMetadataLike"),
            )
        )
        executor = self._make_executor()
        quanta = executor.run(register_dataset_types=True, save_versions=False)
        self.assertEqual(len(quanta), 2)
        self._assert_datasets(stored_intermediate_storage_class="TaskMetadataLike")

    def test_intermediate_producer_differs(self):
        """Test execution where an intermediate is registered to be consistent
        with the consumer but different from its producer.
        """
        self.butler.registry.registerDatasetType(
            lsst.daf.butler.DatasetType(
                "intermediate",
                dimensions=self.butler.dimensions.empty,
                storageClass=get_mock_name("TaskMetadataLike"),
            )
        )
        executor = self._make_executor(b_i_storage_class="TaskMetadataLike")
        quanta = executor.run(register_dataset_types=True, save_versions=False)
        self.assertEqual(len(quanta), 2)
        self._assert_datasets(
            stored_intermediate_storage_class="TaskMetadataLike", b_i_storage_class="TaskMetadataLike"
        )

    def test_intermediate_consumer_differs(self):
        """Test execution where an intermediate is registered to be consistent
        with its producer but different from its consumer.
        """
        executor = self._make_executor(a_o_storage_class="TaskMetadataLike")
        quanta = executor.run(register_dataset_types=True, save_versions=False)
        self.assertEqual(len(quanta), 2)
        self._assert_datasets(
            stored_intermediate_storage_class="TaskMetadataLike", a_o_storage_class="TaskMetadataLike"
        )

    def test_output_differs(self):
        """Test execution where an overall output is registered to be
        different from the producing task.
        """
        self.butler.registry.registerDatasetType(
            lsst.daf.butler.DatasetType(
                "output",
                dimensions=self.butler.dimensions.empty,
                storageClass=get_mock_name("TaskMetadataLike"),
            )
        )
        executor = self._make_executor()
        quanta = executor.run(register_dataset_types=True, save_versions=False)
        self.assertEqual(len(quanta), 2)
        self._assert_datasets(stored_output_storage_class="TaskMetadataLike")

    def test_input_differs(self):
        """Test execution where an overall input's storage class is different
        from the consuming task.
        """
        executor = self._make_executor(a_i_storage_class="TaskMetadataLike")
        quanta = executor.run(register_dataset_types=True, save_versions=False)
        self.assertEqual(len(quanta), 2)
        self._assert_datasets(a_i_storage_class="TaskMetadataLike")

    def test_input_differs_use_local_butler(self):
        """Test execution where an overall input's storage class is different
        from the consuming task, and we use a local butler.
        """
        executor = self._make_executor(a_i_storage_class="TaskMetadataLike")
        with tempfile.TemporaryDirectory() as tempdir:
            root = os.path.join(tempdir, "butler_root")
            executor.use_local_butler(root)
            quanta = executor.run(register_dataset_types=True, save_versions=False)
            self.assertEqual(len(quanta), 2)
            self._assert_datasets(a_i_storage_class="TaskMetadataLike", butler=executor.butler)

    def test_incompatible(self):
        """Test that we cannot make a QG if the registry and pipeline have
        incompatible storage classes for a dataset type.
        """
        # Incompatible output dataset type.
        self.butler.registry.registerDatasetType(
            lsst.daf.butler.DatasetType(
                "output",
                dimensions=self.butler.dimensions.empty,
                storageClass="StructuredDataList",
            )
        )
        with self.assertRaisesRegex(
            IncompatibleDatasetTypeError, "Incompatible definition.*StructuredDataDict.*StructuredDataList.*"
        ):
            self._make_executor()

    def test_registry_changed(self):
        """Run pipeline, but change registry dataset types between making the
        QG and executing it.

        This only fails with full-butler execution; we don't have a way to
        prevent it with QBB.
        """
        executor = self._make_executor()
        self.butler.registry.registerDatasetType(
            lsst.daf.butler.DatasetType(
                "output",
                dimensions=self.butler.dimensions.empty,
                storageClass="TaskMetadataLike",  # even compatible is not okay
            )
        )
        with self.assertRaisesRegex(
            lsst.daf.butler.registry.ConflictingDefinitionError,
            ".*_mock_StructuredDataDict.*is inconsistent with.*TaskMetadataLike.*",
        ):
            executor.run(register_dataset_types=True, save_versions=False)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    """Generic tests for file leaks."""


def setup_module(module):
    """Set up the module for pytest.

    Parameters
    ----------
    module : `~types.ModuleType`
        Module to set up.
    """
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
