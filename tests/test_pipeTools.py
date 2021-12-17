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

import unittest

import lsst.pipe.base.connectionTypes as cT
import lsst.utils.tests
from lsst.pipe.base import Pipeline, PipelineTask, PipelineTaskConfig, PipelineTaskConnections, pipeTools


class ExamplePipelineTaskConnections(PipelineTaskConnections, dimensions=["Visit", "Detector"]):
    input1 = cT.Input(
        name="", dimensions=["Visit", "Detector"], storageClass="example", doc="Input for this task"
    )
    input2 = cT.Input(
        name="", dimensions=["Visit", "Detector"], storageClass="example", doc="Input for this task"
    )
    output1 = cT.Output(
        name="", dimensions=["Visit", "Detector"], storageClass="example", doc="Output for this task"
    )
    output2 = cT.Output(
        name="", dimensions=["Visit", "Detector"], storageClass="example", doc="Output for this task"
    )

    def __init__(self, *, config=None):
        super().__init__(config=config)
        if not config.connections.input2:
            self.inputs.remove("input2")
        if not config.connections.output2:
            self.outputs.remove("output2")


class ExamplePipelineTaskConfig(PipelineTaskConfig, pipelineConnections=ExamplePipelineTaskConnections):
    pass


def _makeConfig(inputName, outputName, pipeline, label):
    """Factory method for config instances

    inputName and outputName can be either string or tuple of strings
    with two items max.
    """
    if isinstance(inputName, tuple):
        pipeline.addConfigOverride(label, "connections.input1", inputName[0])
        pipeline.addConfigOverride(label, "connections.input2", inputName[1] if len(inputName) > 1 else "")
    else:
        pipeline.addConfigOverride(label, "connections.input1", inputName)

    if isinstance(outputName, tuple):
        pipeline.addConfigOverride(label, "connections.output1", outputName[0])
        pipeline.addConfigOverride(label, "connections.output2", outputName[1] if len(outputName) > 1 else "")
    else:
        pipeline.addConfigOverride(label, "connections.output1", outputName)


class ExamplePipelineTask(PipelineTask):
    ConfigClass = ExamplePipelineTaskConfig


def _makePipeline(tasks):
    """Generate Pipeline instance.

    Parameters
    ----------
    tasks : list of tuples
        Each tuple in the list has 3 or 4 items:
        - input DatasetType name(s), string or tuple of strings
        - output DatasetType name(s), string or tuple of strings
        - task label, string
        - optional task class object, can be None

    Returns
    -------
    Pipeline instance
    """
    pipe = Pipeline("test pipeline")
    for task in tasks:
        inputs = task[0]
        outputs = task[1]
        label = task[2]
        klass = task[3] if len(task) > 3 else ExamplePipelineTask
        pipe.addTask(klass, label)
        _makeConfig(inputs, outputs, pipe, label)
    return list(pipe.toExpandedPipeline())


class PipelineToolsTestCase(unittest.TestCase):
    """A test case for pipelineTools"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testIsOrdered(self):
        """Tests for pipeTools.isPipelineOrdered method"""
        pipeline = _makePipeline([("A", "B", "task1"), ("B", "C", "task2")])
        self.assertTrue(pipeTools.isPipelineOrdered(pipeline))

        pipeline = _makePipeline(
            [("A", ("B", "C"), "task1"), ("B", "D", "task2"), ("C", "E", "task3"), (("D", "E"), "F", "task4")]
        )
        self.assertTrue(pipeTools.isPipelineOrdered(pipeline))

        pipeline = _makePipeline(
            [("A", ("B", "C"), "task1"), ("C", "E", "task2"), ("B", "D", "task3"), (("D", "E"), "F", "task4")]
        )
        self.assertTrue(pipeTools.isPipelineOrdered(pipeline))

    def testIsOrderedExceptions(self):
        """Tests for pipeTools.isPipelineOrdered method exceptions"""
        # two producers should throw ValueError
        with self.assertRaises(pipeTools.DuplicateOutputError):
            _makePipeline(
                [
                    ("A", "B", "task1"),
                    ("B", "C", "task2"),
                    ("A", "C", "task3"),
                ]
            )

    def testOrderPipeline(self):
        """Tests for pipeTools.orderPipeline method"""
        pipeline = _makePipeline([("A", "B", "task1"), ("B", "C", "task2")])
        pipeline = pipeTools.orderPipeline(pipeline)
        self.assertEqual(len(pipeline), 2)
        self.assertEqual(pipeline[0].label, "task1")
        self.assertEqual(pipeline[1].label, "task2")

        pipeline = _makePipeline([("B", "C", "task2"), ("A", "B", "task1")])
        pipeline = pipeTools.orderPipeline(pipeline)
        self.assertEqual(len(pipeline), 2)
        self.assertEqual(pipeline[0].label, "task1")
        self.assertEqual(pipeline[1].label, "task2")

        pipeline = _makePipeline(
            [("A", ("B", "C"), "task1"), ("B", "D", "task2"), ("C", "E", "task3"), (("D", "E"), "F", "task4")]
        )
        pipeline = pipeTools.orderPipeline(pipeline)
        self.assertEqual(len(pipeline), 4)
        self.assertEqual(pipeline[0].label, "task1")
        self.assertEqual(pipeline[1].label, "task2")
        self.assertEqual(pipeline[2].label, "task3")
        self.assertEqual(pipeline[3].label, "task4")

        pipeline = _makePipeline(
            [("A", ("B", "C"), "task1"), ("C", "E", "task3"), ("B", "D", "task2"), (("D", "E"), "F", "task4")]
        )
        pipeline = pipeTools.orderPipeline(pipeline)
        self.assertEqual(len(pipeline), 4)
        self.assertEqual(pipeline[0].label, "task1")
        self.assertEqual(pipeline[1].label, "task2")
        self.assertEqual(pipeline[2].label, "task3")
        self.assertEqual(pipeline[3].label, "task4")

        pipeline = _makePipeline(
            [(("D", "E"), "F", "task4"), ("B", "D", "task2"), ("C", "E", "task3"), ("A", ("B", "C"), "task1")]
        )
        pipeline = pipeTools.orderPipeline(pipeline)
        self.assertEqual(len(pipeline), 4)
        self.assertEqual(pipeline[0].label, "task1")
        self.assertEqual(pipeline[1].label, "task2")
        self.assertEqual(pipeline[2].label, "task3")
        self.assertEqual(pipeline[3].label, "task4")

        pipeline = _makePipeline(
            [(("D", "E"), "F", "task4"), ("C", "E", "task3"), ("B", "D", "task2"), ("A", ("B", "C"), "task1")]
        )
        pipeline = pipeTools.orderPipeline(pipeline)
        self.assertEqual(len(pipeline), 4)
        self.assertEqual(pipeline[0].label, "task1")
        self.assertEqual(pipeline[1].label, "task2")
        self.assertEqual(pipeline[2].label, "task3")
        self.assertEqual(pipeline[3].label, "task4")

    def testOrderPipelineExceptions(self):
        """Tests for pipeTools.orderPipeline method exceptions"""
        with self.assertRaises(pipeTools.DuplicateOutputError):
            _makePipeline(
                [
                    ("A", "B", "task1"),
                    ("B", "C", "task2"),
                    ("A", "C", "task3"),
                ]
            )

        # cycle in a graph should throw ValueError
        with self.assertRaises(pipeTools.PipelineDataCycleError):
            _makePipeline([("A", ("A", "B"), "task1")])

        # another kind of cycle in a graph
        with self.assertRaises(pipeTools.PipelineDataCycleError):
            _makePipeline(
                [("A", "B", "task1"), ("B", "C", "task2"), ("C", "D", "task3"), ("D", "A", "task4")]
            )


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
