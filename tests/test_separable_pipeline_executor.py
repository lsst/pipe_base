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


import os
import tempfile
import unittest

import lsst.daf.butler
import lsst.daf.butler.tests as butlerTests
import lsst.pex.config
import lsst.utils.tests
from lsst.ctrl.mpexec import SeparablePipelineExecutor
from lsst.pipe.base import Instrument, Pipeline, TaskMetadata
from lsst.pipe.base.automatic_connection_constants import PACKAGES_INIT_OUTPUT_NAME
from lsst.pipe.base.quantum_graph_builder import OutputExistsError
from lsst.resources import ResourcePath

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class SeparablePipelineExecutorTests(lsst.utils.tests.TestCase):
    """Test the SeparablePipelineExecutor API with a trivial task."""

    pipeline_file = os.path.join(TESTDIR, "pipeline_separable.yaml")

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
        output = "fake"
        output_run = f"{output}/{Instrument.makeCollectionTimestamp()}"
        butler.registry.registerCollection(output_run, lsst.daf.butler.CollectionType.RUN)
        butler.registry.registerCollection(output, lsst.daf.butler.CollectionType.CHAINED)
        butler.registry.setCollectionChain(output, [output_run])
        self.butler = lsst.daf.butler.Butler.from_config(butler=butler, collections=[output], run=output_run)

        butlerTests.addDatasetType(self.butler, "input", set(), "StructuredDataDict")
        butlerTests.addDatasetType(self.butler, "intermediate", set(), "StructuredDataDict")
        butlerTests.addDatasetType(self.butler, "a_log", set(), "ButlerLogRecords")
        butlerTests.addDatasetType(self.butler, "a_metadata", set(), "TaskMetadata")
        butlerTests.addDatasetType(self.butler, "a_config", set(), "Config")

    def test_pre_execute_qgraph(self):
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

    def test_pre_execute_qgraph_empty(self):
        executor = SeparablePipelineExecutor(self.butler)
        graph = lsst.pipe.base.QuantumGraph({}, universe=self.butler.dimensions)

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

    def test_pre_execute_qgraph_register(self):
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

    def test_pre_execute_qgraph_init_outputs(self):
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

    def test_pre_execute_qgraph_versions(self):
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

    def test_init_badinput(self):
        butler = lsst.daf.butler.Butler.from_config(butler=self.butler, collections=[], run="foo")

        with self.assertRaises(ValueError):
            SeparablePipelineExecutor(butler)

    def test_init_badoutput(self):
        butler = lsst.daf.butler.Butler.from_config(butler=self.butler, collections=["foo"])

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

    def test_make_quantum_graph_nowhere_noskip_noclobber(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=False)
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")

        graph = executor.make_quantum_graph(pipeline)
        self.assertTrue(graph.isConnected)
        self.assertEqual(len(graph), 2)
        self.assertEqual({q.taskDef.label for q in graph.inputQuanta}, {"a"})
        self.assertEqual({q.taskDef.label for q in graph.outputQuanta}, {"b"})

    def test_make_quantum_graph_nowhere_noskip_noclobber_conflict(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=False)
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")
        self.butler.put(lsst.daf.butler.ButlerLogRecords.from_records([]), "a_log")
        self.butler.put(TaskMetadata(), "a_metadata")

        with self.assertRaises(OutputExistsError):
            executor.make_quantum_graph(pipeline)

    # TODO: need more complex task and Butler to test
    # make_quantum_graph(where=...)

    def test_make_quantum_graph_nowhere_skipnone_noclobber(self):
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

        graph = executor.make_quantum_graph(pipeline)
        self.assertTrue(graph.isConnected)
        self.assertEqual(len(graph), 1)
        self.assertEqual({q.taskDef.label for q in graph.inputQuanta}, {"b"})
        self.assertEqual({q.taskDef.label for q in graph.outputQuanta}, {"b"})

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
            executor.make_quantum_graph(pipeline)

    def test_make_quantum_graph_nowhere_noskip_clobber(self):
        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=True)
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")

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

        graph = executor.make_quantum_graph(pipeline)
        self.assertTrue(graph.isConnected)
        self.assertEqual(len(graph), 2)
        self.assertEqual({q.taskDef.label for q in graph.inputQuanta}, {"a"})
        self.assertEqual({q.taskDef.label for q in graph.outputQuanta}, {"b"})

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

    def test_make_quantum_graph_nowhere_skippartial_clobber(self):
        executor = SeparablePipelineExecutor(
            self.butler,
            skip_existing_in=[self.butler.run],
            clobber_output=True,
        )
        pipeline = Pipeline.fromFile(self.pipeline_file)

        self.butler.put({"zero": 0}, "input")
        self.butler.put({"zero": 0}, "intermediate")

        graph = executor.make_quantum_graph(pipeline)
        self.assertTrue(graph.isConnected)
        self.assertEqual(len(graph), 2)
        self.assertEqual({q.taskDef.label for q in graph.inputQuanta}, {"a"})
        self.assertEqual({q.taskDef.label for q in graph.outputQuanta}, {"b"})

    def test_make_quantum_graph_noinput(self):
        executor = SeparablePipelineExecutor(self.butler)
        pipeline = Pipeline.fromFile(self.pipeline_file)

        graph = executor.make_quantum_graph(pipeline)
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

        executor.run_pipeline(graph)
        self.butler.registry.refresh()
        self.assertEqual(self.butler.get("intermediate"), {"zero": 0, "one": 1})
        self.assertEqual(self.butler.get("output"), {"zero": 0, "one": 1, "two": 2})

    def test_run_pipeline_noskip_noclobber_emptygraph(self):
        old_repo_size = self.butler.registry.queryDatasets(...).count()

        executor = SeparablePipelineExecutor(self.butler, skip_existing_in=None, clobber_output=False)
        graph = lsst.pipe.base.QuantumGraph({}, universe=self.butler.dimensions)
        executor.pre_execute_qgraph(
            graph,
            register_dataset_types=True,
            save_init_outputs=True,
            save_versions=False,
        )

        executor.run_pipeline(graph)
        self.butler.registry.refresh()
        # Empty graph execution should do nothing.
        self.assertEqual(self.butler.registry.queryDatasets(...).count(), old_repo_size)

    def test_run_pipeline_skipnone_noclobber(self):
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

    def test_run_pipeline_noskip_clobber_connected(self):
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

        executor.run_pipeline(graph)
        self.butler.registry.refresh()
        self.assertEqual(self.butler.get("intermediate"), {"zero": 0, "one": 1})
        self.assertEqual(self.butler.get("output"), {"zero": 0, "one": 1, "two": 2})

    def test_run_pipeline_noskip_clobber_unconnected(self):
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

        executor.run_pipeline(graph)
        self.butler.registry.refresh()
        self.assertEqual(self.butler.get("intermediate"), {"zero": 0, "one": 1})
        # The value of output is undefined; it depends on which task ran first.
        self.assertTrue(self.butler.exists("output", {}))

    def test_run_pipeline_skipnone_clobber(self):
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

    def test_run_pipeline_skippartial_clobber_unconnected(self):
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
