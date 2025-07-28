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

import os
import shutil
import tempfile
import unittest

import lsst.daf.butler
import lsst.pipe.base.quantum_provenance_graph as qpg
import lsst.utils.tests
from lsst.pipe.base import PipelineGraph, QuantumSuccessCaveats, RepeatableQuantumError
from lsst.pipe.base.simple_pipeline_executor import SimplePipelineExecutor
from lsst.pipe.base.tests.mocks import (
    DynamicConnectionConfig,
    DynamicTestPipelineTask,
    DynamicTestPipelineTaskConfig,
    MockStorageClass,
    get_mock_name,
)

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class SimplePipelineExecutorTests(lsst.utils.tests.TestCase):
    """Test the SimplePipelineExecutor API.

    Because SimplePipelineExecutor is the easiest way to run simple pipelines
    in tests, this has also become a home for tests of execution edge cases
    that don't have a clear home in other test files.
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

    def test_from_task_class(self):
        """Test executing a single quantum with an executor created by the
        `from_task_class` factory method, and the
        `SimplePipelineExecutor.as_generator` method.
        """
        config = DynamicTestPipelineTaskConfig()
        config.inputs["i"] = DynamicConnectionConfig(
            dataset_type_name="input", storage_class="StructuredDataDict", mock_storage_class=False
        )
        config.outputs["o"] = DynamicConnectionConfig(
            dataset_type_name="output", storage_class="StructuredDataDict"
        )
        executor = SimplePipelineExecutor.from_task_class(
            DynamicTestPipelineTask,
            config=config,
            butler=self.butler,
            label="a",
        )
        (quantum,) = executor.as_generator(register_dataset_types=True)
        self.assertEqual(self.butler.get("output").storage_class, get_mock_name("StructuredDataDict"))

    def test_metadata_input(self):
        """Test two tasks where the output uses metadata from input."""
        config_a = DynamicTestPipelineTaskConfig()
        config_a.inputs["i"] = DynamicConnectionConfig(
            dataset_type_name="input", storage_class="StructuredDataDict", mock_storage_class=False
        )
        config_b = DynamicTestPipelineTaskConfig()
        config_b.outputs["o"] = DynamicConnectionConfig(
            dataset_type_name="output", storage_class="StructuredDataDict"
        )
        config_b.inputs["in_metadata"] = DynamicConnectionConfig(
            dataset_type_name="a_metadata", storage_class="TaskMetadata"
        )
        pipeline_graph = PipelineGraph()
        pipeline_graph.add_task("a", DynamicTestPipelineTask, config_a)
        pipeline_graph.add_task("b", DynamicTestPipelineTask, config_b)
        executor = SimplePipelineExecutor.from_pipeline_graph(pipeline_graph, butler=self.butler)
        quanta = executor.run(register_dataset_types=True, save_versions=False)
        self.assertEqual(len(quanta), 2)
        output = self.butler.get("output")
        self.assertEqual(output.quantum.inputs["in_metadata"][0].original_type, "lsst.pipe.base.TaskMetadata")

    def test_optional_intermediate(self):
        """Test a pipeline task with an optional regular input that is produced
        by another task.
        """
        config_a = DynamicTestPipelineTaskConfig()
        config_a.inputs["i"] = DynamicConnectionConfig(
            dataset_type_name="input", storage_class="StructuredDataDict", mock_storage_class=False
        )
        config_a.fail_exception = "lsst.pipe.base.NoWorkFound"
        config_a.fail_condition = "1=1"  # butler query expression that is true
        config_a.outputs["o"] = DynamicConnectionConfig(
            dataset_type_name="intermediate", storage_class="StructuredDataDict"
        )
        config_b = DynamicTestPipelineTaskConfig()
        config_b.inputs["i"] = DynamicConnectionConfig(
            dataset_type_name="intermediate", storage_class="StructuredDataDict", minimum=0
        )
        config_b.outputs["o"] = DynamicConnectionConfig(
            dataset_type_name="output", storage_class="StructuredDataDict"
        )
        pipeline_graph = PipelineGraph()
        pipeline_graph.add_task("a", DynamicTestPipelineTask, config_a)
        pipeline_graph.add_task("b", DynamicTestPipelineTask, config_b)
        executor = SimplePipelineExecutor.from_pipeline_graph(pipeline_graph, butler=self.butler)
        quanta = executor.run(register_dataset_types=True, save_versions=False)
        self.assertEqual(len(quanta), 2)
        # Both quanta ran successfully (NoWorkFound is a success).
        self.assertTrue(self.butler.exists("a_metadata"))
        self.assertTrue(self.butler.exists("b_metadata"))
        # The intermediate dataset was not written, but the final output was.
        self.assertFalse(self.butler.exists("intermediate"))
        self.assertTrue(self.butler.exists("output"))

    def test_optional_input(self):
        """Test a pipeline task with an optional regular input that is an
        overall input to the pipeline.
        """
        config_a = DynamicTestPipelineTaskConfig()
        config_a.inputs["i1"] = DynamicConnectionConfig(
            dataset_type_name="input", storage_class="StructuredDataDict", mock_storage_class=False
        )
        config_a.outputs["i2"] = DynamicConnectionConfig(
            dataset_type_name="input_2",
            storage_class="StructuredDataDict",  # will never exist
        )
        config_a.outputs["o"] = DynamicConnectionConfig(
            dataset_type_name="output", storage_class="StructuredDataDict"
        )
        pipeline_graph = PipelineGraph()
        pipeline_graph.add_task("a", DynamicTestPipelineTask, config_a)
        executor = SimplePipelineExecutor.from_pipeline_graph(pipeline_graph, butler=self.butler)
        quanta = executor.run(register_dataset_types=True, save_versions=False)
        self.assertEqual(len(quanta), 1)
        # The quanta ran successfully.
        self.assertTrue(self.butler.exists("a_metadata"))
        # The final output was written.
        self.assertTrue(self.butler.exists("output"))

    def test_from_pipeline_file(self) -> None:
        """Test executing a two quanta from different configurations of the
        same task, with an executor created by the `from_pipeline_filename`
        factory method, and the `SimplePipelineExecutor.run` method.
        """
        filename = os.path.join(TESTDIR, "pipelines", "pipeline_simple.yaml")
        executor = SimplePipelineExecutor.from_pipeline_filename(filename, butler=self.butler)
        self._test_pipeline_file(executor)

    def test_use_local_butler(self) -> None:
        """Test generating a local butler repository from a pipeline, then
        running that pipeline using the local butler.
        """
        filename = os.path.join(TESTDIR, "pipelines", "pipeline_simple.yaml")
        executor = SimplePipelineExecutor.from_pipeline_filename(
            filename, butler=self.butler, output="u/someone/pipeline"
        )
        with tempfile.TemporaryDirectory() as tempdir:
            root = os.path.join(tempdir, "butler_root")
            executor.use_local_butler(root)
            self._test_pipeline_file(executor)

    def _test_pipeline_file(self, executor: SimplePipelineExecutor) -> None:
        quanta = executor.run(register_dataset_types=True, save_versions=False)
        self.assertEqual(len(quanta), 2)
        self.assertEqual(
            executor.butler.get("intermediate").storage_class, get_mock_name("StructuredDataDict")
        )
        self.assertEqual(executor.butler.get("output").storage_class, get_mock_name("StructuredDataDict"))

    def test_partial_outputs_success(self):
        """Test executing two quanta where the first raises
        `lsst.pipe.base.AnnotatedPartialOutputsError` and its output is an
        optional input to the second, while configuring the executor to
        consider this a success.
        """
        config_a = DynamicTestPipelineTaskConfig()
        config_a.inputs["i"] = DynamicConnectionConfig(
            dataset_type_name="input", storage_class="StructuredDataDict", mock_storage_class=False
        )
        config_a.fail_exception = "lsst.pipe.base.AnnotatedPartialOutputsError"
        config_a.fail_condition = "1=1"  # butler query expression that is true
        config_a.outputs["o"] = DynamicConnectionConfig(
            dataset_type_name="intermediate", storage_class="StructuredDataDict"
        )
        config_b = DynamicTestPipelineTaskConfig()
        config_b.inputs["i"] = DynamicConnectionConfig(
            dataset_type_name="intermediate", storage_class="StructuredDataDict", minimum=0
        )
        config_b.outputs["o"] = DynamicConnectionConfig(
            dataset_type_name="output", storage_class="StructuredDataDict"
        )
        pipeline_graph = PipelineGraph()
        pipeline_graph.add_task("a", DynamicTestPipelineTask, config_a)
        pipeline_graph.add_task("b", DynamicTestPipelineTask, config_b)
        # Consider the partial a success and proceed.
        executor = SimplePipelineExecutor.from_pipeline_graph(
            pipeline_graph, butler=self.butler, raise_on_partial_outputs=False
        )
        (_, _) = executor.as_generator(register_dataset_types=True)
        self.assertFalse(self.butler.exists("intermediate"))
        self.assertEqual(self.butler.get("output").storage_class, get_mock_name("StructuredDataDict"))
        prov = qpg.QuantumProvenanceGraph(self.butler, [executor.quantum_graph], read_caveats="exhaustive")
        (quantum_key_a,) = prov.quanta["a"]
        quantum_info_a = prov.get_quantum_info(quantum_key_a)
        _, quantum_run_a = qpg.QuantumRun.find_final(quantum_info_a)
        self.assertEqual(
            quantum_run_a.caveats,
            QuantumSuccessCaveats.ALL_OUTPUTS_MISSING
            | QuantumSuccessCaveats.ANY_OUTPUTS_MISSING
            | QuantumSuccessCaveats.PARTIAL_OUTPUTS_ERROR,
        )
        self.assertEqual(
            quantum_run_a.exception.type_name,
            "lsst.pipe.base.tests.mocks.MockAlgorithmError",
        )
        self.assertEqual(
            quantum_run_a.exception.metadata,
            {"badness": 12},
        )
        (quantum_key_b,) = prov.quanta["b"]
        quantum_info_b = prov.get_quantum_info(quantum_key_b)
        _, quantum_run_b = qpg.QuantumRun.find_final(quantum_info_b)
        self.assertEqual(quantum_run_b.caveats, QuantumSuccessCaveats.NO_CAVEATS)
        prov_summary = prov.to_summary(self.butler)
        # One partial-outputs case, with an empty data ID:
        self.assertEqual(prov_summary.tasks["a"].caveats, {"*P": [{}]})
        self.assertEqual(
            prov_summary.tasks["a"].exceptions.keys(), {"lsst.pipe.base.tests.mocks.MockAlgorithmError"}
        )
        self.assertEqual(
            prov_summary.tasks["a"]
            .exceptions["lsst.pipe.base.tests.mocks.MockAlgorithmError"][0]
            .exception.metadata,
            {"badness": 12},
        )
        # No caveats for the second task, since it didn't need the first task's
        # output anyway.
        self.assertEqual(prov_summary.tasks["b"].caveats, {})
        self.assertEqual(prov_summary.tasks["b"].exceptions, {})
        # Check table forms for summaries of the same information.
        quantum_table = prov_summary.make_quantum_table()
        self.assertEqual(list(quantum_table["Task"]), ["a", "b"])
        self.assertEqual(list(quantum_table["Unknown"]), [0, 0])
        self.assertEqual(list(quantum_table["Successful"]), [1, 1])
        self.assertEqual(list(quantum_table["Caveats"]), ["*P(1)", ""])
        self.assertEqual(list(quantum_table["Blocked"]), [0, 0])
        self.assertEqual(list(quantum_table["Failed"]), [0, 0])
        self.assertEqual(list(quantum_table["Wonky"]), [0, 0])
        self.assertEqual(list(quantum_table["TOTAL"]), [1, 1])
        self.assertEqual(list(quantum_table["EXPECTED"]), [1, 1])
        dataset_table = prov_summary.make_dataset_table()
        self.assertEqual(
            list(dataset_table["Dataset"]),
            ["intermediate", "a_metadata", "a_log", "output", "b_metadata", "b_log"],
        )
        self.assertEqual(list(dataset_table["Visible"]), [0, 1, 1, 1, 1, 1])
        self.assertEqual(list(dataset_table["Shadowed"]), [0, 0, 0, 0, 0, 0])
        self.assertEqual(list(dataset_table["Predicted Only"]), [1, 0, 0, 0, 0, 0])
        self.assertEqual(list(dataset_table["Unsuccessful"]), [0, 0, 0, 0, 0, 0])
        self.assertEqual(list(dataset_table["Cursed"]), [0, 0, 0, 0, 0, 0])
        self.assertEqual(list(dataset_table["TOTAL"]), [1, 1, 1, 1, 1, 1])
        self.assertEqual(list(dataset_table["EXPECTED"]), [1, 1, 1, 1, 1, 1])
        exception_table = prov_summary.make_exception_table()
        self.assertEqual(list(exception_table["Task"]), ["a"])
        self.assertEqual(
            list(exception_table["Exception"]), ["lsst.pipe.base.tests.mocks.MockAlgorithmError"]
        )
        self.assertEqual(list(exception_table["Count"]), [1])
        bad_quantum_tables = prov_summary.make_bad_quantum_tables()
        self.assertEqual(bad_quantum_tables.keys(), {"a"})
        self.assertEqual(list(bad_quantum_tables["a"]["Status(Caveats)"]), ["SUCCESSFUL(P)"])
        self.assertEqual(list(bad_quantum_tables["a"]["Exception"]), ["MockAlgorithmError"])
        self.assertFalse(prov_summary.make_bad_dataset_tables())

    def test_no_work_found(self):
        """Test executing two quanta where the first raises
        `NoWorkFound` in `runQuantum`, leading the next to raise `NoWorkFound`
        in `adjustQuantum`.
        """
        config_a = DynamicTestPipelineTaskConfig()
        config_a.inputs["i"] = DynamicConnectionConfig(
            dataset_type_name="input", storage_class="StructuredDataDict", mock_storage_class=False
        )
        config_a.fail_exception = "lsst.pipe.base.NoWorkFound"
        config_a.fail_condition = "1=1"  # butler query expression that is true
        config_a.outputs["o"] = DynamicConnectionConfig(
            dataset_type_name="intermediate", storage_class="StructuredDataDict"
        )
        config_b = DynamicTestPipelineTaskConfig()
        config_b.inputs["i"] = DynamicConnectionConfig(
            dataset_type_name="intermediate", storage_class="StructuredDataDict"
        )
        config_b.outputs["o"] = DynamicConnectionConfig(
            dataset_type_name="output", storage_class="StructuredDataDict"
        )
        pipeline_graph = PipelineGraph()
        pipeline_graph.add_task("a", DynamicTestPipelineTask, config_a)
        pipeline_graph.add_task("b", DynamicTestPipelineTask, config_b)
        # Consider the partial a success and proceed.
        executor = SimplePipelineExecutor.from_pipeline_graph(
            pipeline_graph, butler=self.butler, raise_on_partial_outputs=False
        )
        (_, _) = executor.as_generator(register_dataset_types=True)
        prov = qpg.QuantumProvenanceGraph()
        prov.assemble_quantum_provenance_graph(self.butler, [executor.quantum_graph])
        (quantum_key_a,) = prov.quanta["a"]
        quantum_info_a = prov.get_quantum_info(quantum_key_a)
        _, quantum_run_a = qpg.QuantumRun.find_final(quantum_info_a)
        self.assertEqual(
            quantum_run_a.caveats,
            QuantumSuccessCaveats.ALL_OUTPUTS_MISSING
            | QuantumSuccessCaveats.ANY_OUTPUTS_MISSING
            | QuantumSuccessCaveats.NO_WORK,
        )
        (quantum_key_b,) = prov.quanta["b"]
        quantum_info_b = prov.get_quantum_info(quantum_key_b)
        _, quantum_run_b = qpg.QuantumRun.find_final(quantum_info_b)
        self.assertEqual(
            quantum_run_b.caveats,
            QuantumSuccessCaveats.ALL_OUTPUTS_MISSING
            | QuantumSuccessCaveats.ANY_OUTPUTS_MISSING
            | QuantumSuccessCaveats.ADJUST_QUANTUM_RAISED
            | QuantumSuccessCaveats.NO_WORK,
        )
        prov_summary = prov.to_summary(self.butler)
        # One NoWorkFound, raised by runQuantum, with an empty data ID:
        self.assertEqual(prov_summary.tasks["a"].caveats, {"*N": [{}]})
        self.assertEqual(prov_summary.tasks["a"].exceptions, {})
        # One NoWorkFound, raised by adjustQuantum, with an empty data ID.
        self.assertEqual(prov_summary.tasks["b"].caveats, {"*A": [{}]})
        self.assertEqual(prov_summary.tasks["b"].exceptions, {})
        # Check table forms for summaries of the same information.
        quantum_table = prov_summary.make_quantum_table()
        self.assertEqual(list(quantum_table["Task"]), ["a", "b"])
        self.assertEqual(list(quantum_table["Unknown"]), [0, 0])
        self.assertEqual(list(quantum_table["Successful"]), [1, 1])
        self.assertEqual(list(quantum_table["Caveats"]), ["*N(1)", "*A(1)"])
        self.assertEqual(list(quantum_table["Blocked"]), [0, 0])
        self.assertEqual(list(quantum_table["Failed"]), [0, 0])
        self.assertEqual(list(quantum_table["Wonky"]), [0, 0])
        self.assertEqual(list(quantum_table["TOTAL"]), [1, 1])
        self.assertEqual(list(quantum_table["EXPECTED"]), [1, 1])
        dataset_table = prov_summary.make_dataset_table()
        self.assertEqual(
            list(dataset_table["Dataset"]),
            ["intermediate", "a_metadata", "a_log", "output", "b_metadata", "b_log"],
        )
        self.assertEqual(list(dataset_table["Visible"]), [0, 1, 1, 0, 1, 1])
        self.assertEqual(list(dataset_table["Shadowed"]), [0, 0, 0, 0, 0, 0])
        self.assertEqual(list(dataset_table["Predicted Only"]), [1, 0, 0, 1, 0, 0])
        self.assertEqual(list(dataset_table["Unsuccessful"]), [0, 0, 0, 0, 0, 0])
        self.assertEqual(list(dataset_table["Cursed"]), [0, 0, 0, 0, 0, 0])
        self.assertEqual(list(dataset_table["TOTAL"]), [1, 1, 1, 1, 1, 1])
        self.assertEqual(list(dataset_table["EXPECTED"]), [1, 1, 1, 1, 1, 1])
        self.assertFalse(prov_summary.make_exception_table())
        self.assertFalse(prov_summary.make_bad_quantum_tables())
        self.assertFalse(prov_summary.make_bad_dataset_tables())

    def test_partial_outputs_failure(self):
        """Test executing two quanta where the first raises
        `lsst.pipe.base.AnnotatedPartialOutputsError` and its output is an
        optional input to the second, while configuring the executor to
        consider this a failure.
        """
        config_a = DynamicTestPipelineTaskConfig()
        config_a.inputs["i"] = DynamicConnectionConfig(
            dataset_type_name="input", storage_class="StructuredDataDict", mock_storage_class=False
        )
        config_a.fail_exception = "lsst.pipe.base.AnnotatedPartialOutputsError"
        config_a.fail_condition = "1=1"  # butler query expression that is true
        config_a.outputs["o"] = DynamicConnectionConfig(
            dataset_type_name="intermediate", storage_class="StructuredDataDict"
        )
        config_b = DynamicTestPipelineTaskConfig()
        config_b.inputs["i"] = DynamicConnectionConfig(
            dataset_type_name="intermediate", storage_class="StructuredDataDict", minimum=0
        )
        config_b.outputs["o"] = DynamicConnectionConfig(
            dataset_type_name="output", storage_class="StructuredDataDict"
        )
        pipeline_graph = PipelineGraph()
        pipeline_graph.add_task("a", DynamicTestPipelineTask, config_a)
        pipeline_graph.add_task("b", DynamicTestPipelineTask, config_b)
        executor = SimplePipelineExecutor.from_pipeline_graph(
            pipeline_graph,
            butler=self.butler,
            raise_on_partial_outputs=True,
        )
        # The executor should raise the chained exception
        # (RepeatableQuantumError, since that's what the mocking system in
        # pipe_base uses here), not AnnotatedPartialOutputsError.
        with self.assertRaises(RepeatableQuantumError):
            executor.run(register_dataset_types=True)
        self.assertFalse(self.butler.exists("intermediate"))
        self.assertFalse(self.butler.exists("output"))
        prov = qpg.QuantumProvenanceGraph()
        prov.assemble_quantum_provenance_graph(self.butler, [executor.quantum_graph])
        (quantum_key_a,) = prov.quanta["a"]
        quantum_info_a = prov.get_quantum_info(quantum_key_a)
        _, quantum_run_a = qpg.QuantumRun.find_final(quantum_info_a)
        self.assertEqual(quantum_run_a.status, qpg.QuantumRunStatus.FAILED)
        self.assertIsNone(quantum_run_a.caveats)
        self.assertIsNone(quantum_run_a.exception)
        (quantum_key_b,) = prov.quanta["b"]
        quantum_info_b = prov.get_quantum_info(quantum_key_b)
        self.assertEqual(quantum_info_b["status"], qpg.QuantumInfoStatus.BLOCKED)
        prov_summary = prov.to_summary(self.butler)
        # One partial-outputs failure case for the first task.
        self.assertEqual(prov_summary.tasks["a"].n_failed, 1)
        # No direct failures, but one blocked for the second
        self.assertEqual(prov_summary.tasks["b"].n_failed, 0)
        self.assertEqual(prov_summary.tasks["b"].n_blocked, 1)
        # Check table forms for summaries of the same information.
        quantum_table = prov_summary.make_quantum_table()
        self.assertEqual(list(quantum_table["Task"]), ["a", "b"])
        self.assertEqual(list(quantum_table["Unknown"]), [0, 0])
        self.assertEqual(list(quantum_table["Successful"]), [0, 0])
        self.assertEqual(list(quantum_table["Caveats"]), ["", ""])
        self.assertEqual(list(quantum_table["Blocked"]), [0, 1])
        self.assertEqual(list(quantum_table["Failed"]), [1, 0])
        self.assertEqual(list(quantum_table["Wonky"]), [0, 0])
        self.assertEqual(list(quantum_table["TOTAL"]), [1, 1])
        self.assertEqual(list(quantum_table["EXPECTED"]), [1, 1])
        dataset_table = prov_summary.make_dataset_table()
        self.assertEqual(
            list(dataset_table["Dataset"]),
            ["intermediate", "a_metadata", "a_log", "output", "b_metadata", "b_log"],
        )
        # Note that a_log is UNSUCCESSFUL, not VISIBLE, despite being present
        # in butler because those categories are mutually exclusive and we
        # don't want to consider any outputs of failed quanta to be successful.
        self.assertEqual(list(dataset_table["Visible"]), [0, 0, 0, 0, 0, 0])
        self.assertEqual(list(dataset_table["Shadowed"]), [0, 0, 0, 0, 0, 0])
        self.assertEqual(list(dataset_table["Predicted Only"]), [0, 0, 0, 0, 0, 0])
        self.assertEqual(list(dataset_table["Unsuccessful"]), [1, 1, 1, 1, 1, 1])
        self.assertEqual(list(dataset_table["Cursed"]), [0, 0, 0, 0, 0, 0])
        self.assertEqual(list(dataset_table["TOTAL"]), [1, 1, 1, 1, 1, 1])
        self.assertEqual(list(dataset_table["EXPECTED"]), [1, 1, 1, 1, 1, 1])
        self.assertFalse(prov_summary.make_exception_table())
        bad_quantum_tables = prov_summary.make_bad_quantum_tables()
        self.assertEqual(bad_quantum_tables.keys(), {"a"})
        self.assertEqual(list(bad_quantum_tables["a"]["Status(Caveats)"]), ["FAILED"])
        self.assertEqual(list(bad_quantum_tables["a"]["Exception"]), [""])
        self.assertFalse(prov_summary.make_bad_dataset_tables())

    def test_existence_check_skips(self):
        """Test that pre-execution existence checks are not performed for
        overall-input datasets, as this those checks could otherwise mask
        repository configuration problems or downtime as NoWorkFound cases.
        """
        # First we configure and execute task A, which is just a way to get a
        # MockDataset in the repo for us to play with; the important test can't
        # use the non-mock 'input' dataset because the mock runQuantum only
        # actually reads MockDatasets.
        config_a = DynamicTestPipelineTaskConfig()
        config_a.inputs["i"] = DynamicConnectionConfig(
            dataset_type_name="input", storage_class="StructuredDataDict", mock_storage_class=False
        )
        config_a.outputs["o"] = DynamicConnectionConfig(
            dataset_type_name="intermediate", storage_class="StructuredDataDict"
        )
        executor_a = SimplePipelineExecutor.from_task_class(
            DynamicTestPipelineTask,
            config=config_a,
            butler=self.butler,
            label="a",
        )
        executor_a.run(register_dataset_types=True)
        # Now we can do the real test.
        config_b = DynamicTestPipelineTaskConfig()
        config_b.inputs["i"] = DynamicConnectionConfig(
            dataset_type_name="intermediate", storage_class="StructuredDataDict", minimum=0
        )
        config_b.outputs["o"] = DynamicConnectionConfig(
            dataset_type_name="output", storage_class="StructuredDataDict"
        )
        butler = self.butler.clone(run="new_run")
        executor_b = SimplePipelineExecutor.from_task_class(
            DynamicTestPipelineTask,
            config=config_b,
            butler=butler,
            label="b",
            attach_datastore_records=True,
        )
        # Delete the input dataset after the QG has already been built.
        intermediate_refs = butler.query_datasets("intermediate")
        self.assertEqual(len(intermediate_refs), 1)
        butler.pruneDatasets(intermediate_refs, purge=True, unstore=True)
        with self.assertRaises(FileNotFoundError):
            # We should get an exception rather than NoWorkFound, because for
            # this single-task pipeline, the missing dataset is an
            # overall-input (name notwithstanding).
            executor_b.run(register_dataset_types=True)


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
