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


import os
import tempfile
import unittest

import lsst.daf.butler
import lsst.daf.butler.tests as butlerTests
import lsst.pex.config
import lsst.utils.tests
from lsst.daf.butler.registry import RegistryDefaults
from lsst.pipe.base import (
    Instrument,
    Pipeline,
    PipelineGraph,
    QuantumAttemptStatus,
    QuantumGraph,
    QuantumSuccessCaveats,
    TaskMetadata,
)
from lsst.pipe.base.all_dimensions_quantum_graph_builder import AllDimensionsQuantumGraphBuilder
from lsst.pipe.base.automatic_connection_constants import (
    PACKAGES_INIT_OUTPUT_NAME,
    PROVENANCE_DATASET_TYPE_NAME,
    PROVENANCE_STORAGE_CLASS,
)
from lsst.pipe.base.quantum_graph_builder import OutputExistsError
from lsst.pipe.base.separable_pipeline_executor import SeparablePipelineExecutor
from lsst.pipe.base.tests.mocks import (
    DirectButlerRepo,
    DynamicTestPipelineTaskConfig,
)
from lsst.resources import ResourcePath
from lsst.utils.packages import Packages

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class SeparablePipelineExecutorTests(lsst.utils.tests.TestCase):
    """Test the SeparablePipelineExecutor API with a trivial task."""

    pipeline_file = os.path.join(TESTDIR, "pipelines", "pipeline_separable.yaml")

    def setUp(self):
        repodir = tempfile.TemporaryDirectory()
        # TemporaryDirectory warns on leaks; addCleanup also keeps it from
        # getting garbage-collected.
        self.addCleanup(tempfile.TemporaryDirectory.cleanup, repodir)

        # standalone parameter forces the returned config to also include
        # the information from the search paths.
        config = lsst.daf.butler.Butler.makeRepo(
            repodir.name, standalone=True, searchPaths=[os.path.join(TESTDIR, "config")]
        )
        butler = lsst.daf.butler.Butler.from_config(config, writeable=True)
        self.enterContext(butler)
        output = "fake"
        output_run = f"{output}/{Instrument.makeCollectionTimestamp()}"
        butler.registry.registerCollection(output_run, lsst.daf.butler.CollectionType.RUN)
        butler.registry.registerCollection(output, lsst.daf.butler.CollectionType.CHAINED)
        butler.registry.setCollectionChain(output, [output_run])
        butler.registry.defaults = RegistryDefaults(collections=[output], run=output_run)
        self.butler = butler

        butlerTests.addDatasetType(self.butler, "input", set(), "StructuredDataDict")
        butlerTests.addDatasetType(self.butler, "intermediate", set(), "StructuredDataDict")
        butlerTests.addDatasetType(self.butler, "a_log", set(), "ButlerLogRecords")
        butlerTests.addDatasetType(self.butler, "a_metadata", set(), "TaskMetadata")
        butlerTests.addDatasetType(self.butler, "a_config", set(), "Config")
        provenance_dataset_type = butlerTests.addDatasetType(
            self.butler, PROVENANCE_DATASET_TYPE_NAME, set(), PROVENANCE_STORAGE_CLASS
        )
        self.provenance_ref = lsst.daf.butler.DatasetRef(
            provenance_dataset_type,
            lsst.daf.butler.DataCoordinate.make_empty(self.butler.dimensions),
            run=butler.run,
        )

    def build_empty_quantum_graph(self) -> None:
        pipeline_graph = PipelineGraph(universe=self.butler.dimensions)
        pipeline_graph.resolve(self.butler.registry)
        builder = AllDimensionsQuantumGraphBuilder(pipeline_graph, self.butler)
        return builder.finish(attach_datastore_records=False).assemble()

    def check_provenance_fullgraph(self):
        provenance_qg = self.butler.get(self.provenance_ref)
        empty_data_id = lsst.daf.butler.DataCoordinate.make_empty(self.butler.dimensions)
        self.assertCountEqual(provenance_qg.quanta_by_task.keys(), {"a", "b"})
        self.assertCountEqual(
            provenance_qg.datasets_by_type.keys(),
            {
                "input",
                "intermediate",
                "output",
                "a_config",
                "b_config",
                "a_metadata",
                "b_metadata",
                "a_log",
                "b_log",
            },
        )
        a_id = provenance_qg.quanta_by_task["a"][empty_data_id]
        b_id = provenance_qg.quanta_by_task["b"][empty_data_id]
        input_id = provenance_qg.datasets_by_type["input"][empty_data_id]
        intermediate_id = provenance_qg.datasets_by_type["intermediate"][empty_data_id]
        output_id = provenance_qg.datasets_by_type["output"][empty_data_id]
        a_metadata_id = provenance_qg.datasets_by_type["a_metadata"][empty_data_id]
        a_log_id = provenance_qg.datasets_by_type["a_log"][empty_data_id]
        b_metadata_id = provenance_qg.datasets_by_type["b_metadata"][empty_data_id]
        b_log_id = provenance_qg.datasets_by_type["b_log"][empty_data_id]
        self.assertEqual(
            provenance_qg.bipartite_xgraph.nodes[a_id]["status"], QuantumAttemptStatus.SUCCESSFUL
        )
        self.assertEqual(
            provenance_qg.bipartite_xgraph.nodes[b_id]["status"], QuantumAttemptStatus.SUCCESSFUL
        )
        self.assertEqual(list(provenance_qg.bipartite_xgraph.predecessors(a_id)), [input_id])
        self.assertEqual(
            list(provenance_qg.bipartite_xgraph.successors(a_id)), [intermediate_id, a_metadata_id, a_log_id]
        )
        self.assertEqual(list(provenance_qg.bipartite_xgraph.predecessors(b_id)), [intermediate_id])
        self.assertEqual(
            list(provenance_qg.bipartite_xgraph.successors(b_id)), [output_id, b_metadata_id, b_log_id]
        )
        for datasets_by_data_id in provenance_qg.datasets_by_type.values():
            for dataset_id in datasets_by_data_id.values():
                self.assertTrue(provenance_qg.bipartite_xgraph.nodes[dataset_id]["produced"])
        logs_pqg = self.butler.get(
            self.provenance_ref.makeComponentRef("logs"),
            parameters={"quanta": (a_id,), "datasets": (b_log_id,)},
        )
        self.assertEqual(list(logs_pqg[a_id][-1]), list(self.butler.get("a_log", empty_data_id)))
        self.assertEqual(list(logs_pqg[b_log_id][-1]), list(self.butler.get("b_log", empty_data_id)))
        metadata_pqg = self.butler.get(
            self.provenance_ref.makeComponentRef("metadata"),
            parameters={"quanta": (b_id,), "datasets": (a_metadata_id,)},
        )
        self.assertEqual(metadata_pqg[a_metadata_id][-1], self.butler.get("a_metadata", empty_data_id))
        self.assertEqual(metadata_pqg[b_id][-1], self.butler.get("b_metadata", empty_data_id))
        self.assertIsInstance(self.butler.get("run_provenance.packages"), Packages)

    def check_provenance_emptygraph(self):
        provenance_qg = self.butler.get(self.provenance_ref)
        self.assertFalse(provenance_qg.bipartite_xgraph)
        self.assertFalse(any(provenance_qg.quanta_by_task.values()))
        self.assertFalse(any(provenance_qg.datasets_by_type.values()))
        self.assertIsInstance(self.butler.get("run_provenance.packages"), Packages)

    def test_pre_execute_qgraph_old(self):
        # Too hard to make a quantum graph from scratch.
        executor = SeparablePipelineExecutor(self.butler)
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        graph = executor.make_quantum_graph(pipeline)

        butlerTests.addDatasetType(self.butler, "output", set(), "StructuredDataDict")
        butlerTests.addDatasetType(self.butler, "b_config", set(), "Config")
        butlerTests.addDatasetType(self.butler, "b_log", set(), "ButlerLogRecords")
        butlerTests.addDatasetType(self.butler, "b_metadata", set(), "TaskMetadata")
        butlerTests.addDatasetType(self.butler, PACKAGES_INIT_OUTPUT_NAME, set(), "Packages")

        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=False,
            save_init_outputs=False,
            save_versions=False,
        )
        self.assertFalse(self.butler.exists("a_config", {}, collections=[self.butler.run]))
        self.assertFalse(self.butler.exists(PACKAGES_INIT_OUTPUT_NAME, {}))

    def test_pre_execute_qgraph(self):
        # Too hard to make a quantum graph from scratch.
        executor = SeparablePipelineExecutor(self.butler)
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        graph = executor.build_quantum_graph(pipeline)

        butlerTests.addDatasetType(self.butler, "output", set(), "StructuredDataDict")
        butlerTests.addDatasetType(self.butler, "b_config", set(), "Config")
        butlerTests.addDatasetType(self.butler, "b_log", set(), "ButlerLogRecords")
        butlerTests.addDatasetType(self.butler, "b_metadata", set(), "TaskMetadata")
        butlerTests.addDatasetType(self.butler, PACKAGES_INIT_OUTPUT_NAME, set(), "Packages")

        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=False,
            save_init_outputs=False,
            save_versions=False,
        )
        self.assertFalse(self.butler.exists("a_config", {}, collections=[self.butler.run]))
        self.assertFalse(self.butler.exists(PACKAGES_INIT_OUTPUT_NAME, {}))

    def test_pre_execute_qgraph_unconnected_old(self):
        # Unconnected graph; see
        # test_make_quantum_graph_nowhere_skippartial_clobber.
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=True,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        graph = executor.make_quantum_graph(pipeline)

        butlerTests.addDatasetType(self.butler, "output", set(), "StructuredDataDict")
        butlerTests.addDatasetType(self.butler, "b_config", set(), "Config")
        butlerTests.addDatasetType(self.butler, "b_log", set(), "ButlerLogRecords")
        butlerTests.addDatasetType(self.butler, "b_metadata", set(), "TaskMetadata")
        butlerTests.addDatasetType(self.butler, PACKAGES_INIT_OUTPUT_NAME, set(), "Packages")

        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=False,
            save_init_outputs=False,
            save_versions=False,
        )
        self.assertFalse(self.butler.exists("a_config", {}, collections=[self.butler.run]))
        self.assertFalse(self.butler.exists(PACKAGES_INIT_OUTPUT_NAME, {}))

    def test_pre_execute_qgraph_unconnected(self):
        # Unconnected graph; see
        # test_make_quantum_graph_nowhere_skippartial_clobber.
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=True,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        graph = executor.build_quantum_graph(pipeline)

        butlerTests.addDatasetType(self.butler, "output", set(), "StructuredDataDict")
        butlerTests.addDatasetType(self.butler, "b_config", set(), "Config")
        butlerTests.addDatasetType(self.butler, "b_log", set(), "ButlerLogRecords")
        butlerTests.addDatasetType(self.butler, "b_metadata", set(), "TaskMetadata")
        butlerTests.addDatasetType(self.butler, PACKAGES_INIT_OUTPUT_NAME, set(), "Packages")

        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=False,
            save_init_outputs=False,
            save_versions=False,
        )
        self.assertFalse(self.butler.exists("a_config", {}, collections=[self.butler.run]))
        self.assertFalse(self.butler.exists(PACKAGES_INIT_OUTPUT_NAME, {}))

    def test_pre_execute_qgraph_empty_old(self):
        executor = SeparablePipelineExecutor(self.butler)
        graph = QuantumGraph({}, universe=self.butler.dimensions)

        butlerTests.addDatasetType(self.butler, "output", set(), "StructuredDataDict")
        butlerTests.addDatasetType(self.butler, "b_config", set(), "Config")
        butlerTests.addDatasetType(self.butler, "b_log", set(), "ButlerLogRecords")
        butlerTests.addDatasetType(self.butler, "b_metadata", set(), "TaskMetadata")
        butlerTests.addDatasetType(self.butler, PACKAGES_INIT_OUTPUT_NAME, set(), "Packages")

        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=False,
            save_init_outputs=False,
            save_versions=False,
        )
        self.assertFalse(self.butler.exists("a_config", {}, collections=[self.butler.run]))
        self.assertFalse(self.butler.exists(PACKAGES_INIT_OUTPUT_NAME, {}))

    def test_pre_execute_qgraph_empty(self):
        executor = SeparablePipelineExecutor(self.butler)
        graph = self.build_empty_quantum_graph()

        butlerTests.addDatasetType(self.butler, "output", set(), "StructuredDataDict")
        butlerTests.addDatasetType(self.butler, "b_config", set(), "Config")
        butlerTests.addDatasetType(self.butler, "b_log", set(), "ButlerLogRecords")
        butlerTests.addDatasetType(self.butler, "b_metadata", set(), "TaskMetadata")
        butlerTests.addDatasetType(self.butler, PACKAGES_INIT_OUTPUT_NAME, set(), "Packages")

        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=False,
            save_init_outputs=False,
            save_versions=False,
        )
        self.assertFalse(self.butler.exists("a_config", {}, collections=[self.butler.run]))
        self.assertFalse(self.butler.exists(PACKAGES_INIT_OUTPUT_NAME, {}))

    def test_pre_execute_qgraph_register_old(self):
        executor = SeparablePipelineExecutor(self.butler)
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        graph = executor.make_quantum_graph(pipeline)

        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=True,
            save_init_outputs=False,
            save_versions=False,
        )
        self.assertEqual({d.name for d in self.butler.registry.queryDatasetTypes("output")}, {"output"})
        self.assertEqual(
            {d.name for d in self.butler.registry.queryDatasetTypes("b_*")},
            {"b_config", "b_log", "b_metadata"},
        )
        self.assertFalse(self.butler.exists("a_config", {}, collections=[self.butler.run]))
        self.assertFalse(self.butler.exists(PACKAGES_INIT_OUTPUT_NAME, {}))

    def test_pre_execute_qgraph_register(self):
        executor = SeparablePipelineExecutor(self.butler)
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        graph = executor.build_quantum_graph(pipeline)

        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=True,
            save_init_outputs=False,
            save_versions=False,
        )
        self.assertEqual({d.name for d in self.butler.registry.queryDatasetTypes("output")}, {"output"})
        self.assertEqual(
            {d.name for d in self.butler.registry.queryDatasetTypes("b_*")},
            {"b_config", "b_log", "b_metadata"},
        )
        self.assertFalse(self.butler.exists("a_config", {}, collections=[self.butler.run]))
        self.assertFalse(self.butler.exists(PACKAGES_INIT_OUTPUT_NAME, {}))

    def test_pre_execute_qgraph_init_outputs_old(self):
        # Too hard to make a quantum graph from scratch.
        executor = SeparablePipelineExecutor(self.butler)
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        graph = executor.make_quantum_graph(pipeline)

        butlerTests.addDatasetType(self.butler, "output", set(), "StructuredDataDict")
        butlerTests.addDatasetType(self.butler, "b_config", set(), "Config")
        butlerTests.addDatasetType(self.butler, "b_log", set(), "ButlerLogRecords")
        butlerTests.addDatasetType(self.butler, "b_metadata", set(), "TaskMetadata")
        butlerTests.addDatasetType(self.butler, PACKAGES_INIT_OUTPUT_NAME, set(), "Packages")

        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=False,
            save_init_outputs=True,
            save_versions=False,
        )
        self.assertTrue(self.butler.exists("a_config", {}, collections=[self.butler.run]))
        self.assertFalse(self.butler.exists(PACKAGES_INIT_OUTPUT_NAME, {}))

    def test_pre_execute_qgraph_init_outputs(self):
        # Too hard to make a quantum graph from scratch.
        executor = SeparablePipelineExecutor(self.butler)
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        graph = executor.build_quantum_graph(pipeline)

        butlerTests.addDatasetType(self.butler, "output", set(), "StructuredDataDict")
        butlerTests.addDatasetType(self.butler, "b_config", set(), "Config")
        butlerTests.addDatasetType(self.butler, "b_log", set(), "ButlerLogRecords")
        butlerTests.addDatasetType(self.butler, "b_metadata", set(), "TaskMetadata")
        butlerTests.addDatasetType(self.butler, PACKAGES_INIT_OUTPUT_NAME, set(), "Packages")

        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=False,
            save_init_outputs=True,
            save_versions=False,
        )
        self.assertTrue(self.butler.exists("a_config", {}, collections=[self.butler.run]))
        self.assertFalse(self.butler.exists(PACKAGES_INIT_OUTPUT_NAME, {}))

    def test_pre_execute_qgraph_versions_old(self):
        executor = SeparablePipelineExecutor(self.butler)
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        graph = executor.make_quantum_graph(pipeline)

        butlerTests.addDatasetType(self.butler, "output", set(), "StructuredDataDict")
        butlerTests.addDatasetType(self.butler, "b_config", set(), "Config")
        butlerTests.addDatasetType(self.butler, "b_log", set(), "ButlerLogRecords")
        butlerTests.addDatasetType(self.butler, "b_metadata", set(), "TaskMetadata")
        butlerTests.addDatasetType(self.butler, PACKAGES_INIT_OUTPUT_NAME, set(), "Packages")

        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=False,
            save_init_outputs=True,
            save_versions=True,
        )
        self.assertTrue(self.butler.exists("a_config", {}, collections=[self.butler.run]))
        self.assertTrue(self.butler.exists(PACKAGES_INIT_OUTPUT_NAME, {}))

    def test_pre_execute_qgraph_versions(self):
        executor = SeparablePipelineExecutor(self.butler)
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        graph = executor.build_quantum_graph(pipeline)

        butlerTests.addDatasetType(self.butler, "output", set(), "StructuredDataDict")
        butlerTests.addDatasetType(self.butler, "b_config", set(), "Config")
        butlerTests.addDatasetType(self.butler, "b_log", set(), "ButlerLogRecords")
        butlerTests.addDatasetType(self.butler, "b_metadata", set(), "TaskMetadata")
        butlerTests.addDatasetType(self.butler, PACKAGES_INIT_OUTPUT_NAME, set(), "Packages")

        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=False,
            save_init_outputs=True,
            save_versions=True,
        )
        self.assertTrue(self.butler.exists("a_config", {}, collections=[self.butler.run]))
        self.assertTrue(self.butler.exists(PACKAGES_INIT_OUTPUT_NAME, {}))

    def test_init_badinput(self):
        with lsst.daf.butler.Butler.from_config(butler=self.butler, collections=[], run="foo") as butler:
            with self.assertRaises(ValueError):
                SeparablePipelineExecutor(butler)

    def test_init_badoutput(self):
        with lsst.daf.butler.Butler.from_config(butler=self.butler, collections=["foo"]) as butler:
            with self.assertRaises(ValueError):
                SeparablePipelineExecutor(butler)

    def test_make_pipeline_full(self):
        executor = SeparablePipelineExecutor(self.butler)
        for uri in [
            self.pipeline_file,
            ResourcePath(self.pipeline_file),
            ResourcePath(self.pipeline_file).geturl(),
        ]:
            pipeline_graph = executor.make_pipeline(uri).to_graph()
            self.assertEqual(set(pipeline_graph.tasks), {"a", "b"})

    def test_make_pipeline_subset(self):
        executor = SeparablePipelineExecutor(self.butler)
        path = self.pipeline_file + "#a"
        for uri in [
            path,
            ResourcePath(path),
            ResourcePath(path).geturl(),
        ]:
            pipeline_graph = executor.make_pipeline(uri).to_graph()
            self.assertEqual(set(pipeline_graph.tasks), {"a"})

    def test_build_quantum_graph_nowhere_noskip_noclobber(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=False)
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")

        graph = executor.make_quantum_graph(pipeline)
        self.assertTrue(graph.isConnected)
        self.assertEqual(len(graph), 2)
        self.assertEqual({q.taskDef.label for q in graph.inputQuanta}, {"a"})
        self.assertEqual({q.taskDef.label for q in graph.outputQuanta}, {"b"})

    def test_make_quantum_graph_nowhere_noskip_noclobber(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=False)
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")

        graph = executor.build_quantum_graph(pipeline)
        self.assertEqual(len(graph), 2)
        self.assertEqual(graph.quanta_by_task.keys(), {"a", "b"})

    def test_make_quantum_graph_nowhere_noskip_noclobber_conflict(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=False)
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "a_log")
        self.butler.put(TaskMetadata(), "a_metadata")

        with self.assertRaises(OutputExistsError):
            executor.build_quantum_graph(pipeline)

    # TODO: need more complex task and Butler to test
    # make_quantum_graph(where=...)

    def test_build_quantum_graph_nowhere_skipnone_noclobber(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=False,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")

        graph = executor.make_quantum_graph(pipeline)
        self.assertTrue(graph.isConnected)
        self.assertEqual(len(graph), 2)
        self.assertEqual({q.taskDef.label for q in graph.inputQuanta}, {"a"})
        self.assertEqual({q.taskDef.label for q in graph.outputQuanta}, {"b"})

    def test_make_quantum_graph_nowhere_skipnone_noclobber(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=False,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        graph = executor.build_quantum_graph(pipeline)
        self.assertEqual(len(graph), 2)
        self.assertEqual(graph.quanta_by_task.keys(), {"a", "b"})

    def test_build_quantum_graph_nowhere_skiptotal_noclobber(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=False,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "a_log")
        self.butler.put(TaskMetadata(), "a_metadata")
        self.butler.put(lsst.pex.config.Config(), "a_config")

        graph = executor.make_quantum_graph(pipeline)
        self.assertTrue(graph.isConnected)
        self.assertEqual(len(graph), 1)
        self.assertEqual({q.taskDef.label for q in graph.inputQuanta}, {"b"})
        self.assertEqual({q.taskDef.label for q in graph.outputQuanta}, {"b"})

    def test_make_quantum_graph_nowhere_skiptotal_noclobber(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=False,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "a_log")
        self.butler.put(TaskMetadata(), "a_metadata")
        self.butler.put(lsst.pex.config.Config(), "a_config")

        graph = executor.build_quantum_graph(pipeline)
        self.assertEqual(len(graph), 1)
        self.assertEqual(graph.header.n_task_quanta["a"], 0)
        self.assertEqual(graph.header.n_task_quanta["b"], 1)

    def test_make_quantum_graph_nowhere_skippartial_noclobber(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=False,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")

        with self.assertRaises(OutputExistsError):
            executor.build_quantum_graph(pipeline)

    def test_build_quantum_graph_nowhere_noskip_clobber(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=True)
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")

        graph = executor.make_quantum_graph(pipeline)
        self.assertTrue(graph.isConnected)
        self.assertEqual(len(graph), 2)
        self.assertEqual({q.taskDef.label for q in graph.inputQuanta}, {"a"})
        self.assertEqual({q.taskDef.label for q in graph.outputQuanta}, {"b"})

    def test_make_quantum_graph_nowhere_noskip_clobber(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=True)
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        graph = executor.build_quantum_graph(pipeline)
        self.assertEqual(len(graph), 2)
        self.assertEqual(graph.quanta_by_task.keys(), {"a", "b"})

    def test_build_quantum_graph_nowhere_noskip_clobber_conflict(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=True)
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "a_log")
        self.butler.put(TaskMetadata(), "a_metadata")

        graph = executor.make_quantum_graph(pipeline)
        self.assertTrue(graph.isConnected)
        self.assertEqual(len(graph), 2)
        self.assertEqual({q.taskDef.label for q in graph.inputQuanta}, {"a"})
        self.assertEqual({q.taskDef.label for q in graph.outputQuanta}, {"b"})

    def test_make_quantum_graph_nowhere_noskip_clobber_conflict(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=True)
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "a_log")
        self.butler.put(TaskMetadata(), "a_metadata")
        graph = executor.build_quantum_graph(pipeline)
        self.assertEqual(len(graph), 2)
        self.assertEqual(graph.quanta_by_task.keys(), {"a", "b"})

    def test_build_quantum_graph_nowhere_skipnone_clobber(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=True,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")

        graph = executor.make_quantum_graph(pipeline)
        self.assertTrue(graph.isConnected)
        self.assertEqual(len(graph), 2)
        self.assertEqual({q.taskDef.label for q in graph.inputQuanta}, {"a"})
        self.assertEqual({q.taskDef.label for q in graph.outputQuanta}, {"b"})

    def test_make_quantum_graph_nowhere_skipnone_clobber(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=True,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        graph = executor.build_quantum_graph(pipeline)
        self.assertEqual(len(graph), 2)
        self.assertEqual(graph.quanta_by_task.keys(), {"a", "b"})

    def test_make_quantum_graph_nowhere_skiptotal_clobber(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=True,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "a_log")
        self.butler.put(TaskMetadata(), "a_metadata")
        self.butler.put(lsst.pex.config.Config(), "a_config")

        graph = executor.make_quantum_graph(pipeline)
        self.assertTrue(graph.isConnected)
        self.assertEqual(len(graph), 1)
        self.assertEqual({q.taskDef.label for q in graph.inputQuanta}, {"b"})
        self.assertEqual({q.taskDef.label for q in graph.outputQuanta}, {"b"})

    def test_build_quantum_graph_nowhere_skiptotal_clobber(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=True,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "a_log")
        self.butler.put(TaskMetadata(), "a_metadata")
        self.butler.put(lsst.pex.config.Config(), "a_config")

        graph = executor.make_quantum_graph(pipeline)
        self.assertTrue(graph.isConnected)
        self.assertEqual(len(graph), 1)
        self.assertEqual({q.taskDef.label for q in graph.inputQuanta}, {"b"})
        self.assertEqual({q.taskDef.label for q in graph.outputQuanta}, {"b"})

    def test_make_quantum_graph_nowhere_skippartial_clobber(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=True,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        graph = executor.build_quantum_graph(pipeline)
        self.assertEqual(len(graph), 2)
        self.assertEqual(graph.quanta_by_task.keys(), {"a", "b"})

    def test_make_quantum_graph_noinput(self):
        executor = SeparablePipelineExecutor(self.butler)
        pipeline = Pipeline.fromFile(self.pipeline_file)

        graph = executor.make_quantum_graph(pipeline)
        self.assertEqual(len(graph), 0)

    def test_build_quantum_graph_noinput(self):
        executor = SeparablePipelineExecutor(self.butler)
        pipeline = Pipeline.fromFile(self.pipeline_file)

        graph = executor.build_quantum_graph(pipeline)
        self.assertEqual(len(graph), 0)

    def test_make_quantum_graph_alloutput_skip(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=[self.butler.run])
        pipeline = Pipeline.fromFile(self.pipeline_file)

        butlerTests.addDatasetType(self.butler, "output", set(), "StructuredDataDict")
        butlerTests.addDatasetType(self.butler, "b_log", set(), "ButlerLogRecords")
        butlerTests.addDatasetType(self.butler, "b_metadata", set(), "TaskMetadata")
        butlerTests.addDatasetType(self.butler, "b_config", set(), "Config")

        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "a_log")
        self.butler.put(TaskMetadata(), "a_metadata")
        self.butler.put({"zero": 0}, "output")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "b_log")
        self.butler.put(TaskMetadata(), "b_metadata")
        self.butler.put(lsst.pex.config.Config(), "a_config")
        self.butler.put(lsst.pex.config.Config(), "b_config")

        graph = executor.make_quantum_graph(pipeline)
        self.assertEqual(len(graph), 0)

    def test_build_quantum_graph_alloutput_skip(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=[self.butler.run])
        pipeline = Pipeline.fromFile(self.pipeline_file)

        butlerTests.addDatasetType(self.butler, "output", set(), "StructuredDataDict")
        butlerTests.addDatasetType(self.butler, "b_log", set(), "ButlerLogRecords")
        butlerTests.addDatasetType(self.butler, "b_metadata", set(), "TaskMetadata")
        butlerTests.addDatasetType(self.butler, "b_config", set(), "Config")

        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "a_log")
        self.butler.put(TaskMetadata(), "a_metadata")
        self.butler.put({"zero": 0}, "output")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "b_log")
        self.butler.put(TaskMetadata(), "b_metadata")
        self.butler.put(lsst.pex.config.Config(), "a_config")
        self.butler.put(lsst.pex.config.Config(), "b_config")

        graph = executor.build_quantum_graph(pipeline)
        self.assertEqual(len(graph), 0)

    def test_run_pipeline_noskip_noclobber_fullgraph(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=False)
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        graph = executor.make_quantum_graph(pipeline)
        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=True,
            save_init_outputs=True,
            save_versions=False,
        )

        executor.run_pipeline(graph, provenance_dataset_ref=self.provenance_ref)
        self.butler.registry.refresh()
        self.assertEqual(self.butler.get("intermediate"), {"zero": 0, "one": 1})
        self.assertEqual(self.butler.get("output"), {"zero": 0, "one": 1, "two": 2})
        self.check_provenance_fullgraph()

    def test_run_pipeline_noskip_noclobber_fullgraph_old(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=False)
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        graph = executor.build_quantum_graph(pipeline)
        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=True,
            save_init_outputs=True,
            save_versions=False,
        )

        executor.run_pipeline(graph, provenance_dataset_ref=self.provenance_ref)
        self.butler.registry.refresh()
        self.assertEqual(self.butler.get("intermediate"), {"zero": 0, "one": 1})
        self.assertEqual(self.butler.get("output"), {"zero": 0, "one": 1, "two": 2})
        self.check_provenance_fullgraph()

    def test_run_pipeline_noskip_noclobber_emptygraph_old(self):
        old_repo_size = self.butler.registry.queryDatasets(...).count()

        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=False)
        graph = QuantumGraph({}, universe=self.butler.dimensions)
        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=True,
            save_init_outputs=True,
            save_versions=False,
        )

        executor.run_pipeline(graph, provenance_dataset_ref=self.provenance_ref)
        self.butler.registry.refresh()
        # Empty graph execution should do nothing but write provenance.
        self.assertEqual(self.butler.registry.queryDatasets(...).count(), old_repo_size + 1)
        self.check_provenance_emptygraph()

    def test_run_pipeline_noskip_noclobber_emptygraph(self):
        old_repo_size = self.butler.registry.queryDatasets(...).count()

        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=False)
        graph = self.build_empty_quantum_graph()
        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=True,
            save_init_outputs=True,
            save_versions=False,
        )

        executor.run_pipeline(graph, provenance_dataset_ref=self.provenance_ref)
        self.butler.registry.refresh()
        # Empty graph execution should do nothing but write provenance.
        self.assertEqual(self.butler.registry.queryDatasets(...).count(), old_repo_size + 1)
        self.check_provenance_emptygraph()

    def test_run_pipeline_skipnone_noclobber_old(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=False,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        graph = executor.make_quantum_graph(pipeline)
        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=True,
            save_init_outputs=True,
            save_versions=False,
        )

        executor.run_pipeline(graph)
        self.butler.registry.refresh()
        self.assertEqual(self.butler.get("intermediate"), {"zero": 0, "one": 1})
        self.assertEqual(self.butler.get("output"), {"zero": 0, "one": 1, "two": 2})

    def test_run_pipeline_skipnone_noclobber(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=False,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        graph = executor.build_quantum_graph(pipeline)
        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=True,
            save_init_outputs=True,
            save_versions=False,
        )

        executor.run_pipeline(graph)
        self.butler.registry.refresh()
        self.assertEqual(self.butler.get("intermediate"), {"zero": 0, "one": 1})
        self.assertEqual(self.butler.get("output"), {"zero": 0, "one": 1, "two": 2})

    def test_run_pipeline_skiptotal_noclobber_old(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=False,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "a_log")
        self.butler.put(TaskMetadata(), "a_metadata")
        self.butler.put(lsst.pex.config.Config(), "a_config")
        graph = executor.make_quantum_graph(pipeline)
        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=True,
            save_init_outputs=True,
            save_versions=False,
        )

        executor.run_pipeline(graph)
        self.butler.registry.refresh()
        self.assertEqual(self.butler.get("output"), {"zero": 0, "two": 2})

    def test_run_pipeline_skiptotal_noclobber(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=False,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "a_log")
        self.butler.put(TaskMetadata(), "a_metadata")
        self.butler.put(lsst.pex.config.Config(), "a_config")
        graph = executor.build_quantum_graph(pipeline)
        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=True,
            save_init_outputs=True,
            save_versions=False,
        )

        executor.run_pipeline(graph)
        self.butler.registry.refresh()
        self.assertEqual(self.butler.get("output"), {"zero": 0, "two": 2})

    def test_run_pipeline_noskip_clobber_connected_old(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=True)
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        graph = executor.make_quantum_graph(pipeline)
        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=True,
            save_init_outputs=True,
            save_versions=False,
        )

        executor.run_pipeline(graph, provenance_dataset_ref=self.provenance_ref)
        self.butler.registry.refresh()
        self.assertEqual(self.butler.get("intermediate"), {"zero": 0, "one": 1})
        self.assertEqual(self.butler.get("output"), {"zero": 0, "one": 1, "two": 2})
        self.check_provenance_fullgraph()

    def test_run_pipeline_noskip_clobber_connected(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=True)
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        graph = executor.build_quantum_graph(pipeline)
        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=True,
            save_init_outputs=True,
            save_versions=False,
        )

        executor.run_pipeline(graph, provenance_dataset_ref=self.provenance_ref)
        self.butler.registry.refresh()
        self.assertEqual(self.butler.get("intermediate"), {"zero": 0, "one": 1})
        self.assertEqual(self.butler.get("output"), {"zero": 0, "one": 1, "two": 2})
        self.check_provenance_fullgraph()

    def test_run_pipeline_noskip_clobber_unconnected_old(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=True)
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "a_log")
        self.butler.put(TaskMetadata(), "a_metadata")
        graph = executor.make_quantum_graph(pipeline)
        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=True,
            save_init_outputs=True,
            save_versions=False,
        )

        executor.run_pipeline(graph, provenance_dataset_ref=self.provenance_ref)
        self.butler.registry.refresh()
        self.assertEqual(self.butler.get("intermediate"), {"zero": 0, "one": 1})
        # The value of output is undefined; it depends on which task ran first.
        self.assertTrue(self.butler.exists("output", {}))

    def test_run_pipeline_noskip_clobber_unconnected(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=True)
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "a_log")
        self.butler.put(TaskMetadata(), "a_metadata")
        graph = executor.build_quantum_graph(pipeline)
        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=True,
            save_init_outputs=True,
            save_versions=False,
        )

        executor.run_pipeline(graph, provenance_dataset_ref=self.provenance_ref)
        self.butler.registry.refresh()
        self.assertEqual(self.butler.get("intermediate"), {"zero": 0, "one": 1})
        # The value of output is undefined; it depends on which task ran first.
        self.assertTrue(self.butler.exists("output", {}))
        self.check_provenance_fullgraph()

    def test_run_pipeline_skipnone_clobber_old(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=True,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        graph = executor.make_quantum_graph(pipeline)
        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=True,
            save_init_outputs=True,
            save_versions=False,
        )

        executor.run_pipeline(graph)
        self.butler.registry.refresh()
        self.assertEqual(self.butler.get("intermediate"), {"zero": 0, "one": 1})
        self.assertEqual(self.butler.get("output"), {"zero": 0, "one": 1, "two": 2})

    def test_run_pipeline_skipnone_clobber(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=True,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        graph = executor.build_quantum_graph(pipeline)
        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=True,
            save_init_outputs=True,
            save_versions=False,
        )

        executor.run_pipeline(graph)
        self.butler.registry.refresh()
        self.assertEqual(self.butler.get("intermediate"), {"zero": 0, "one": 1})
        self.assertEqual(self.butler.get("output"), {"zero": 0, "one": 1, "two": 2})

    def test_run_pipeline_skiptotal_clobber_connected_old(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=True,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "a_log")
        self.butler.put(TaskMetadata(), "a_metadata")
        self.butler.put(lsst.pex.config.Config(), "a_config")
        graph = executor.make_quantum_graph(pipeline)
        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=True,
            save_init_outputs=True,
            save_versions=False,
        )

        executor.run_pipeline(graph)
        self.butler.registry.refresh()
        self.assertEqual(self.butler.get("output"), {"zero": 0, "two": 2})

    def test_run_pipeline_skiptotal_clobber_connected(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=True,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "a_log")
        self.butler.put(TaskMetadata(), "a_metadata")
        self.butler.put(lsst.pex.config.Config(), "a_config")
        graph = executor.build_quantum_graph(pipeline)
        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=True,
            save_init_outputs=True,
            save_versions=False,
        )

        executor.run_pipeline(graph)
        self.butler.registry.refresh()
        self.assertEqual(self.butler.get("output"), {"zero": 0, "two": 2})

    def test_run_pipeline_skippartial_clobber_unconnected_old(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=True,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        graph = executor.make_quantum_graph(pipeline)
        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=True,
            save_init_outputs=True,
            save_versions=False,
        )

        executor.run_pipeline(graph)
        self.butler.registry.refresh()
        self.assertEqual(self.butler.get("intermediate"), {"zero": 0, "one": 1})
        # The value of output is undefined; it depends on which task ran first.
        self.assertTrue(self.butler.exists("output", {}))

    def test_run_pipeline_skippartial_clobber_unconnected(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=True,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)
        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        graph = executor.build_quantum_graph(pipeline)
        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=True,
            save_init_outputs=True,
            save_versions=False,
        )
        executor.run_pipeline(graph)
        self.butler.registry.refresh()
        self.assertEqual(self.butler.get("intermediate"), {"zero": 0, "one": 1})
        # The value of output is undefined; it depends on which task ran first.
        self.assertTrue(self.butler.exists("output", {}))


class SeparablePipelineExecutorMockTests(lsst.utils.tests.TestCase):
    """Additional tests for SeparablePipelineExecutor API that use
    the lsst.pipe.base.tests.mocks system to define complex pipelines.
    """

    def test_no_work_chain_provenance(self):
        """Test provenance recording when a NoWorkFound error chains to
        downstream tasks during execution.
        """
        # 'base.yaml' adds an instrument, 'Cam1', with four detectors and
        # two physical filters.
        with DirectButlerRepo.make_temporary("base.yaml") as (helper, _):
            helper.add_task("a", dimensions=["detector"])
            b_config = DynamicTestPipelineTaskConfig()
            b_config.fail_exception = "lsst.pipe.base.NoWorkFound"
            b_config.fail_condition = "detector=2"
            helper.add_task("b", dimensions=["detector"], config=b_config)
            helper.add_task("c", dimensions=["detector"])
            qg = helper.make_quantum_graph()
            helper.butler.collections.register(qg.header.output_run)
            qg.init_output_run(helper.butler, existing=False)
            provenance_type = lsst.daf.butler.DatasetType(
                PROVENANCE_DATASET_TYPE_NAME,
                helper.butler.dimensions.empty,
                PROVENANCE_STORAGE_CLASS,
            )
            helper.butler.registry.registerDatasetType(provenance_type)
            provenance_ref = lsst.daf.butler.DatasetRef(
                provenance_type,
                lsst.daf.butler.DataCoordinate.make_empty(helper.butler.dimensions),
                run=qg.header.output_run,
            )
            executor = SeparablePipelineExecutor(
                helper.butler.clone(collections=qg.header.inputs, run=qg.header.output_run)
            )
            executor.run_pipeline(qg, provenance_dataset_ref=provenance_ref)
            provenance_graph = helper.butler.get(provenance_ref)
            self.assertEqual(len(provenance_graph.quanta_by_task), 3)
            self.assertEqual(len(provenance_graph.quanta_by_task["a"]), 4)
            self.assertEqual(len(provenance_graph.quanta_by_task["b"]), 4)
            self.assertEqual(len(provenance_graph.quanta_by_task["c"]), 4)
            xgraph = provenance_graph.quantum_only_xgraph
            for quantum_id in provenance_graph.quanta_by_task["a"].values():
                self.assertEqual(xgraph.nodes[quantum_id]["status"], QuantumAttemptStatus.SUCCESSFUL)
                self.assertEqual(xgraph.nodes[quantum_id]["caveats"], QuantumSuccessCaveats.NO_CAVEATS)
            for data_id, quantum_id in provenance_graph.quanta_by_task["b"].items():
                self.assertEqual(xgraph.nodes[quantum_id]["status"], QuantumAttemptStatus.SUCCESSFUL)
                if data_id["detector"] == 2:
                    self.assertTrue(xgraph.nodes[quantum_id]["caveats"] & QuantumSuccessCaveats.NO_WORK)
                else:
                    self.assertEqual(xgraph.nodes[quantum_id]["caveats"], QuantumSuccessCaveats.NO_CAVEATS)
            for data_id, quantum_id in provenance_graph.quanta_by_task["c"].items():
                self.assertEqual(xgraph.nodes[quantum_id]["status"], QuantumAttemptStatus.SUCCESSFUL)
                if data_id["detector"] == 2:
                    self.assertTrue(
                        xgraph.nodes[quantum_id]["caveats"] & QuantumSuccessCaveats.ADJUST_QUANTUM_RAISED
                    )
                else:
                    self.assertEqual(xgraph.nodes[quantum_id]["caveats"], QuantumSuccessCaveats.NO_CAVEATS)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    """Generic test for file leaks."""


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
