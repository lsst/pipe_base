# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Simple unit test for Pipeline.
"""

import os
import textwrap
import unittest

import lsst.utils.tests
import yaml
from lsst.pipe.base import LabelSpecifier, Pipeline, PipelineDatasetTypes, TaskDef
from lsst.pipe.base.tests.simpleQGraph import AddTask, makeSimplePipeline


class PipelineTestCase(unittest.TestCase):
    """A test case for TaskDef and Pipeline."""

    def testTaskDef(self):
        """Tests for TaskDef structure"""
        task1 = TaskDef(taskClass=AddTask, config=AddTask.ConfigClass())
        self.assertIn("Add", task1.taskName)
        self.assertIsInstance(task1.config, AddTask.ConfigClass)
        self.assertIsNotNone(task1.taskClass)
        self.assertEqual(task1.label, "add_task")

    def testEmpty(self):
        """Creating empty pipeline"""
        pipeline = Pipeline("test")
        self.assertEqual(len(pipeline), 0)

    def testInitial(self):
        """Testing constructor with initial data"""
        pipeline = makeSimplePipeline(2)
        self.assertEqual(len(pipeline), 2)
        expandedPipeline = list(pipeline.toExpandedPipeline())
        self.assertEqual(expandedPipeline[0].taskName, "lsst.pipe.base.tests.simpleQGraph.AddTask")
        self.assertEqual(expandedPipeline[1].taskName, "lsst.pipe.base.tests.simpleQGraph.AddTask")
        self.assertEqual(expandedPipeline[0].taskClass, AddTask)
        self.assertEqual(expandedPipeline[1].taskClass, AddTask)
        self.assertEqual(expandedPipeline[0].label, "task0")
        self.assertEqual(expandedPipeline[1].label, "task1")

    def testParameters(self):
        """Test that parameters can be set and used to format"""
        pipeline_str = textwrap.dedent(
            """
            description: Test Pipeline
            parameters:
               testValue: 5
            tasks:
              add:
                class: test_pipeline.AddTask
                config:
                  addend: parameters.testValue
        """
        )
        # verify that parameters are used in expanding a pipeline
        pipeline = Pipeline.fromString(pipeline_str)
        expandedPipeline = list(pipeline.toExpandedPipeline())
        self.assertEqual(expandedPipeline[0].config.addend, 5)

        # verify that a parameter can be overridden on the "command line"
        pipeline.addConfigOverride("parameters", "testValue", 14)
        expandedPipeline = list(pipeline.toExpandedPipeline())
        self.assertEqual(expandedPipeline[0].config.addend, 14)

        # verify that a non existing parameter cant be overridden
        with self.assertRaises(ValueError):
            pipeline.addConfigOverride("parameters", "missingValue", 17)

        # verify that parameters does not support files or python overrides
        with self.assertRaises(ValueError):
            pipeline.addConfigFile("parameters", "fakeFile")
        with self.assertRaises(ValueError):
            pipeline.addConfigPython("parameters", "fakePythonString")

    def testSerialization(self):
        pipeline = makeSimplePipeline(2)
        dump = str(pipeline)
        load = Pipeline.fromString(dump)
        self.assertEqual(pipeline, load)

    def test_initOutputNames(self):
        """Test for PipelineDatasetTypes.initOutputNames method."""
        pipeline = makeSimplePipeline(3)
        dsType = set(PipelineDatasetTypes.initOutputNames(pipeline))
        expected = {
            "packages",
            "add_init_output1",
            "add_init_output2",
            "add_init_output3",
            "task0_config",
            "task1_config",
            "task2_config",
        }
        self.assertEqual(dsType, expected)

    def test_relative_config_file_overrides(self):
        """Test that config file overrides with relative paths are interpreted
        as relative to the location of the pipeline file.
        """
        p1 = makeSimplePipeline(3)
        p1.addConfigFile("task1", "../a.py")
        p2 = makeSimplePipeline(3)
        p2.addConfigFile("task2", "b.py")
        config = AddTask.ConfigClass()
        config.addend = 1
        with lsst.utils.tests.temporaryDirectory() as root:
            pipeline_dir = os.path.join(root, "p")
            os.makedirs(pipeline_dir)
            p1.write_to_uri(os.path.join(pipeline_dir, "p1.yaml"))
            p2.write_to_uri(os.path.join(pipeline_dir, "p2.yaml"))
            config.save(os.path.join(root, "a.py"))
            config.save(os.path.join(pipeline_dir, "b.py"))
            # Directory structure is now
            # <root>/
            #   p/
            #     p1.yaml
            #     p2.yaml
            #     b.py
            #   a.py
            p1_loaded = Pipeline.from_uri(f"file://{pipeline_dir}/p1.yaml")
            p2_loaded = Pipeline.from_uri(os.path.join(pipeline_dir, "p2.yaml"))
            self.assertEqual(p1_loaded["task0"].config.addend, 3)
            self.assertEqual(p1_loaded["task1"].config.addend, 1)
            self.assertEqual(p1_loaded["task2"].config.addend, 3)
            self.assertEqual(p2_loaded["task0"].config.addend, 3)
            self.assertEqual(p2_loaded["task1"].config.addend, 3)
            self.assertEqual(p2_loaded["task2"].config.addend, 1)

    def test_expanded_io(self):
        """Test writing and reading a pipeline with expand=True."""
        original_pipeline_str = textwrap.dedent(
            """
            description: Test Pipeline
            parameters:
              testValue: 5
            tasks:
              # Note that topological ordering is addB, then a tie between
              # addA and addC.
              addC:
                class: lsst.pipe.base.tests.simpleQGraph.AddTask
                config:
                  connections.in_tmpl: _1
                  connections.out_tmpl: _2c
              addB:
                class: lsst.pipe.base.tests.simpleQGraph.AddTask
                config:
                  addend: parameters.testValue
                  connections.in_tmpl: _0
                  connections.out_tmpl: _1
              addA:
                class: lsst.pipe.base.tests.simpleQGraph.AddTask
                config:
                  addend: 8
                  connections.in_tmpl: _1
                  connections.out_tmpl: _2a
            subsets:
              things: [addC, addB]
            contracts:
              addB.connections.out_tmpl == addA.connections.in_tmpl
        """
        )
        original_pipeline = Pipeline.fromString(original_pipeline_str)

        def check(pipeline):
            """Check that a pipeline has the test content defined above.

            This does not include checks for contracts (no public API for
            that) or the presence or absence of parameters or instruments,
            because an expanded pipeline should not have those anymore.
            """
            expanded = list(pipeline)
            self.assertEqual([t.label for t in expanded], ["addB", "addA", "addC"])
            self.assertEqual(
                [t.taskName for t in expanded],
                ["lsst.pipe.base.tests.simpleQGraph.AddTask"] * 3,
            )
            self.assertEqual([t.config.addend for t in expanded], [5, 8, 3])
            subset = pipeline.subsetFromLabels(LabelSpecifier(labels={"things"}))
            self.assertEqual(len(subset), 2)

        check(original_pipeline)

        with lsst.utils.tests.temporaryDirectory() as root:
            with self.assertRaises(RuntimeError):
                original_pipeline.write_to_uri(os.path.join(root, "foo.yaml"), expand=True)
            # Expansion writes to directories, not yaml files.
            # But if we don't have a .yaml extension, we assume the caller
            # wants a directory, even without a trailing slash.
            original_pipeline.write_to_uri(os.path.join(root, "test_pipeline"), expand=True)
            self.assertTrue(os.path.isdir(os.path.join(root, "test_pipeline")))
            self.assertTrue(os.path.isdir(os.path.join(root, "test_pipeline/config")))
            self.assertTrue(os.path.isfile(os.path.join(root, "test_pipeline/pipeline.yaml")))
            self.assertTrue(os.path.isfile(os.path.join(root, "test_pipeline/config/addA.py")))
            self.assertTrue(os.path.isfile(os.path.join(root, "test_pipeline/config/addB.py")))
            self.assertTrue(os.path.isfile(os.path.join(root, "test_pipeline/config/addC.py")))
            with open(os.path.join(root, "test_pipeline/pipeline.yaml"), "r") as buffer:
                primitives = yaml.load(buffer)
            self.assertEqual(primitives.keys(), {"description", "tasks", "subsets", "contracts"})
            self.assertEqual(len(primitives["contracts"]), 1)
            self.assertEqual(primitives["subsets"], {"things": {"subset": ["addB", "addC"]}})
            read_pipeline_1 = Pipeline.from_uri(os.path.join(root, "test_pipeline"))
            read_pipeline_2 = Pipeline.from_uri(os.path.join(root, "test_pipeline/pipeline.yaml"))
            check(read_pipeline_1)
            check(read_pipeline_2)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
