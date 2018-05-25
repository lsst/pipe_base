#
# LSST Data Management System
# Copyright 2016-2018 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

"""Simple unit test for SuperTask.
"""

from __future__ import absolute_import, division, print_function

import unittest
from builtins import object

import lsst.utils.tests
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase
from lsst.pipe.supertask import (GraphBuilder, Pipeline, SuperTask, TaskDef,
                                 SuperTaskConfig, InputDatasetConfig,
                                 OutputDatasetConfig)
from lsst.pipe.supertask.examples.exampleStorageClass import ExampleStorageClass  # noqa: F401


class OneToOneTaskConfig(SuperTaskConfig):
    input = pexConfig.ConfigField(dtype=InputDatasetConfig,
                                  doc="Input dataset type for this task")
    output = pexConfig.ConfigField(dtype=OutputDatasetConfig,
                                   doc="Output dataset type for this task")

    def setDefaults(self):
        # set units of a quantum, this task uses per-visit quanta and it
        # expects dataset units to be the same
        self.quantum.units = ["Camera", "Visit"]
        self.quantum.sql = None

        # default config for input dataset type
        self.input.name = "input"
        self.input.units = ["Camera", "Visit"]
        self.input.storageClass = "example"

        # default config for output dataset type
        self.output.name = "output"
        self.output.units = ["Camera", "Visit"]
        self.output.storageClass = "example"


class OneToOneTask(SuperTask):
    ConfigClass = OneToOneTaskConfig
    _DefaultName = "1to1_task"

    def run(self, input, output):
        output = [val + self.config.addend for val in input]
        return pipeBase.Struct(output=output)


class TaskOne(OneToOneTask):
    _DefaultName = "task_one"


class TaskTwo(OneToOneTask):
    _DefaultName = "task_two"


class TaskFactoryMock(object):
    def loadTaskClass(self, taskName):
        if taskName == "TaskOne":
            return TaskOne, "TaskOne"
        elif taskName == "TaskTwo":
            return TaskTwo, "TaskTwo"

    def makeTask(self, taskClass, config, overrides):
        if config is None:
            config = taskClass.ConfigClass()
            if overrides:
                overrides.applyTo(config)
        return taskClass(config=config)


class RegistryMock(object):
    pass


class GraphBuilderTestCase(unittest.TestCase):
    """A test case for GraphBuilder class
    """

    def _makePipeline(self):
        config1 = OneToOneTaskConfig()
        config2 = OneToOneTaskConfig()
        config2.input.name = config1.output.name
        config2.output.name = "output2"

        tasks = [TaskDef("TaskOne", config1, TaskOne),
                 TaskDef("TaskTwo", config2, TaskTwo)]
        return Pipeline(tasks)

    def _checkQuantum(self, datasets, DSClass, expectedValues):
        self.assertEqual(len(datasets), 1)
        for key, values in datasets.items():
            self.assertIs(key, DSClass)
            self.assertEqual(len(values), 1)
            for val in values:
                self.assertTrue(val.number.value in expectedValues)

    def test_buildIOClasses(self):
        """Test for buildIOClasses() implementation.
        """
        taskFactory = TaskFactoryMock()
        registry = RegistryMock()
        userQuery = None
        gbuilder = GraphBuilder(taskFactory, registry, userQuery)

        tasks = self._makePipeline()
        inputs, outputs = gbuilder.buildIODatasets([task for task in tasks])

        self.assertIsInstance(inputs, set)
        self.assertIsInstance(outputs, set)
        self.assertEqual([x.name for x in inputs], ["input"])
        self.assertEqual(set(x.name for x in outputs), set(["output", "output2"]))

    def test_makeGraph(self):
        """Test for makeGraph() implementation.
        """
        taskFactory = TaskFactoryMock()
        registry = RegistryMock()
        userQuery = None
        gbuilder = GraphBuilder(taskFactory, registry, userQuery)

        pipeline = self._makePipeline()
        graph = gbuilder.makeGraph(pipeline)

        # TODO: temporary until we implement makeGraph()
        self.assertIs(graph, None)
#         self.assertEqual(len(graph), 2)
#         taskDef = graph[0].taskDef
#         quanta = graph[0].quanta
#         self.assertEqual(len(quanta), 10)
#         self.assertEqual(taskDef.taskName, "TaskOne")
#         self.assertEqual(taskDef.taskClass, TaskOne)
#         for quantum in quanta:
#             self._checkQuantum(quantum.inputs, Dataset1, range(10))
#             self._checkQuantum(quantum.outputs, Dataset2, range(10))
#
#         taskDef = graph[1].taskDef
#         quanta = graph[1].quanta
#         self.assertEqual(len(quanta), 10)
#         self.assertEqual(taskDef.taskName, "TaskTwo")
#         self.assertEqual(taskDef.taskClass, TaskTwo)
#         for quantum in quanta:
#             self._checkQuantum(quantum.inputs, Dataset2, range(10))
#             self._checkQuantum(quantum.outputs, Dataset3, range(10))

    def test_makeGraphSelect(self):
        """Test for makeGraph() implementation with subset of data.
        """
        userQuery = "1 = 1"
        taskFactory = TaskFactoryMock()
        registry = RegistryMock()
        pipeline = self._makePipeline()

        gbuilder = GraphBuilder(taskFactory, registry, userQuery)

        graph = gbuilder.makeGraph(pipeline)

        # TODO: temporary until we implement makeGraph()
        self.assertIs(graph, None)
#
#         self.assertEqual(len(graph), 2)
#         taskDef = graph[0].taskDef
#         quanta = graph[0].quanta
#         self.assertEqual(taskDef.taskName, "TaskOne")
#         self.assertEqual(taskDef.taskClass, TaskOne)
#         self.assertEqual(len(quanta), 3)
#         for quantum in quanta:
#             self._checkQuantum(quantum.inputs, Dataset1, [1, 5, 9])
#             self._checkQuantum(quantum.outputs, Dataset2, [1, 5, 9])
#
#         taskDef = graph[1].taskDef
#         quanta = graph[1].quanta
#         self.assertEqual(taskDef.taskName, "TaskTwo")
#         self.assertEqual(taskDef.taskClass, TaskTwo)
#         self.assertEqual(len(quanta), 3)
#         for quantum in quanta:
#             self._checkQuantum(quantum.inputs, Dataset2, [1, 5, 9])
#             self._checkQuantum(quantum.outputs, Dataset3, [1, 5, 9])


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
