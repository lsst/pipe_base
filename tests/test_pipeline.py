# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Simple unit test for Pipeline.
"""

import pickle
import textwrap
import unittest

import lsst.utils.tests
from lsst.pipe.base import LabelSpecifier, Pipeline, TaskDef
from lsst.pipe.base.pipelineIR import LabeledSubset
from lsst.pipe.base.tests.simpleQGraph import AddTask, makeSimplePipeline


class PipelineTestCase(unittest.TestCase):
    """A test case for TaskDef and Pipeline."""

    def testTaskDef(self):
        """Tests for TaskDef structure."""
        task1 = TaskDef(taskClass=AddTask, config=AddTask.ConfigClass())
        self.assertIn("Add", task1.taskName)
        self.assertIsInstance(task1.config, AddTask.ConfigClass)
        self.assertIsNotNone(task1.taskClass)
        self.assertEqual(task1.label, "add_task")
        task1a = pickle.loads(pickle.dumps(task1))
        self.assertEqual(task1, task1a)

    def testEmpty(self):
        """Creating empty pipeline."""
        pipeline = Pipeline("test")
        self.assertEqual(len(pipeline), 0)

    def testInitial(self):
        """Testing constructor with initial data."""
        pipeline = makeSimplePipeline(2)
        self.assertEqual(len(pipeline), 2)
        pipeline_graph = pipeline.to_graph()
        pipeline_graph.sort()
        task_nodes = list(pipeline_graph.tasks.values())
        self.assertEqual(task_nodes[0].task_class, AddTask)
        self.assertEqual(task_nodes[1].task_class, AddTask)
        self.assertEqual(task_nodes[0].label, "task0")
        self.assertEqual(task_nodes[1].label, "task1")
        self.assertEqual(pipeline.task_labels, {"task0", "task1"})

    def testModifySubset(self):
        pipeline = makeSimplePipeline(2)

        # Test adding labels.
        with self.assertRaises(ValueError):
            pipeline.addLabelToSubset("test", "new_label")
        pipeline._pipelineIR.labeled_subsets["test"] = LabeledSubset("test", set(), None)
        with self.assertRaises(ValueError):
            pipeline.addLabelToSubset("test", "missing_label")
        pipeline.addLabelToSubset("test", "task0")
        self.assertEqual(pipeline._pipelineIR.labeled_subsets["test"].subset, {"task0"})

        # Test removing labels.
        with self.assertRaises(ValueError):
            pipeline.addLabelToSubset("missing_subset", "task0")
        with self.assertRaises(ValueError):
            pipeline.addLabelToSubset("test", "missing_label")
        pipeline.removeLabelFromSubset("test", "task0")
        self.assertEqual(pipeline._pipelineIR.labeled_subsets["test"].subset, set())

        # Test creating new labeled subsets.
        with self.assertRaises(ValueError):
            # missing task label
            pipeline.addLabeledSubset("newSubset", "test description", {"missing_task_label"})
        with self.assertRaises(ValueError):
            # duplicate labeled subset
            pipeline.addLabeledSubset("test", "test description", {"missing_task_label"})

        taskLabels = {"task0", "task1"}
        pipeline.addLabeledSubset("newSubset", "test description", taskLabels)

        # verify using the subset property interface
        self.assertEqual(pipeline.subsets["newSubset"], taskLabels)

        # Test removing labeled subsets
        with self.assertRaises(ValueError):
            pipeline.removeLabeledSubset("missing_subset")

        pipeline.removeLabeledSubset("newSubset")
        self.assertNotIn("newSubset", pipeline.subsets.keys())

        pipeline.addLabeledSubset("testSubset", "Test subset description", taskLabels)
        taskSubset = {"task0"}
        pipelineDrop = pipeline.subsetFromLabels(LabelSpecifier(taskSubset), pipeline.PipelineSubsetCtrl.DROP)
        pipelineEdit = pipeline.subsetFromLabels(LabelSpecifier(taskSubset), pipeline.PipelineSubsetCtrl.EDIT)

        # Test subsetting from labels
        self.assertNotIn(taskLabels - taskSubset, set(pipelineDrop.task_labels))
        self.assertNotIn("testSubset", pipelineDrop.subsets.keys())
        self.assertNotIn(taskLabels - taskSubset, set(pipelineEdit.task_labels))
        self.assertIn("testSubset", pipelineEdit.subsets.keys())
        self.assertEqual(pipelineEdit.subsets["testSubset"], taskSubset)

    def testMergingPipelines(self):
        pipeline1 = makeSimplePipeline(2)
        pipeline2 = makeSimplePipeline(4)
        pipeline2.removeTask("task0")
        pipeline2.removeTask("task1")

        pipeline1.mergePipeline(pipeline2)
        self.assertEqual(pipeline1._pipelineIR.tasks.keys(), {"task0", "task1", "task2", "task3"})

    def testFindingSubset(self):
        pipeline = makeSimplePipeline(2)
        pipeline._pipelineIR.labeled_subsets["test1"] = LabeledSubset("test1", set(), None)
        pipeline._pipelineIR.labeled_subsets["test2"] = LabeledSubset("test2", set(), None)
        pipeline._pipelineIR.labeled_subsets["test3"] = LabeledSubset("test3", set(), None)

        pipeline.addLabelToSubset("test1", "task0")
        pipeline.addLabelToSubset("test3", "task0")

        with self.assertRaises(ValueError):
            pipeline.findSubsetsWithLabel("missing_label")

        self.assertEqual(pipeline.findSubsetsWithLabel("task0"), {"test1", "test3"})

    def testParameters(self):
        """Test that parameters can be set and used to format."""
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
        pipeline_graph = pipeline.to_graph()
        self.assertEqual(pipeline_graph.tasks["add"].config.addend, 5)

        # verify that a parameter can be overridden on the "command line"
        pipeline.addConfigOverride("parameters", "testValue", 14)
        pipeline_graph = pipeline.to_graph()
        self.assertEqual(pipeline_graph.tasks["add"].config.addend, 14)

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


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    """Run file leak tests."""


def setup_module(module):
    """Configure pytest."""
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
