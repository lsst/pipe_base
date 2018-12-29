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

"""Simple unit test for GraphBuilder class.
"""

from __future__ import absolute_import, division, print_function

import unittest

import lsst.utils.tests
from lsst.daf.butler import (Registry, RegistryConfig, SchemaConfig,
                             DatasetOriginInfoDef)
from lsst.pipe.base import (Struct, PipelineTask, PipelineTaskConfig,
                            InputDatasetField, OutputDatasetField,
                            InitInputDatasetField, InitOutputDatasetField,
                            GraphBuilder, Pipeline, TaskDef)
from lsst.pipe.base.graphBuilder import _TaskDatasetTypes


class OneToOneTaskConfig(PipelineTaskConfig):
    input = InputDatasetField(name="input",
                              dimensions=["Instrument", "Visit"],
                              storageClass="example",
                              scalar=True,
                              doc="Input dataset type for this task")
    output = OutputDatasetField(name="output",
                                dimensions=["Instrument", "Visit"],
                                storageClass="example",
                                scalar=True,
                                doc="Output dataset type for this task")
    initInput = InitInputDatasetField(name="initInput",
                                      storageClass="example",
                                      doc="InitInput test")
    initOutput = InitOutputDatasetField(name="initOutput",
                                        storageClass="example",
                                        doc="InitOutput test")

    def setDefaults(self):
        # set dimensions of a quantum, this task uses per-visit quanta and it
        # expects dataset dimensions to be the same
        self.quantum.dimensions = ["Instrument", "Visit"]


class VisitToPatchTaskConfig(PipelineTaskConfig):
    input = InputDatasetField(name="input",
                              dimensions=["Instrument", "Visit"],
                              storageClass="example",
                              scalar=False,
                              doc="Input dataset type for this task")
    output = OutputDatasetField(name="output",
                                dimensions=["SkyMap", "Tract", "Patch"],
                                storageClass="example",
                                scalar=True,
                                doc="Output dataset type for this task")

    def setDefaults(self):
        # set dimensions of a quantum, this task uses per-visit quanta and it
        # expects dataset dimensions to be the same
        self.quantum.dimensions = ["SkyMap", "Tract", "Patch"]


class TaskOne(PipelineTask):
    ConfigClass = OneToOneTaskConfig
    _DefaultName = "task_one"

    def run(self, input):
        output = []
        return Struct(output=output)


class TaskTwo(PipelineTask):
    ConfigClass = VisitToPatchTaskConfig
    _DefaultName = "task_two"

    def run(self, input):
        output = []
        return Struct(output=output)


class TaskFactoryMock:
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


@unittest.skip("Registry missing, Will be fixed in DM-16833")
class GraphBuilderTestCase(unittest.TestCase):
    """A test case for GraphBuilder class
    """

    def _makePipeline(self):
        config1 = OneToOneTaskConfig()
        config2 = VisitToPatchTaskConfig()
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

    def test_makeFullIODatasetTypes(self):
        """Test for _makeFullIODatasetTypes() implementation.
        """
        taskFactory = TaskFactoryMock()
        reg = Registry.fromConfig(RegistryConfig(), SchemaConfig())
        gbuilder = GraphBuilder(taskFactory, reg)

        # build a pipeline
        tasks = self._makePipeline()

        # collect inputs/outputs from each task
        taskDatasets = []
        for taskDef in tasks:
            taskClass = taskDef.taskClass
            taskInputs = taskClass.getInputDatasetTypes(taskDef.config) or {}
            taskInputs = [dsTypeDescr.datasetType for dsTypeDescr in taskInputs.values()]
            taskOutputs = taskClass.getOutputDatasetTypes(taskDef.config) or {}
            taskOutputs = [dsTypeDescr.datasetType for dsTypeDescr in taskOutputs.values()]

            taskInitInputs = taskClass.getInitInputDatasetTypes(taskDef.config) or {}
            taskInitInputs = [dsTypeDescr.datasetType for dsTypeDescr in taskInitInputs.values()]

            taskInitOutputs = taskClass.getInitOutputDatasetTypes(taskDef.config) or {}
            taskInitOutputs = [dsTypeDescr.datasetType for dsTypeDescr in taskInitOutputs.values()]
            taskDatasets.append(_TaskDatasetTypes(taskDef=taskDef,
                                                  inputs=taskInputs,
                                                  outputs=taskOutputs,
                                                  initInputs=taskInitInputs,
                                                  initOutputs=taskInitOutputs))

        # make inputs and outputs from per-task dataset types
        inputs, outputs, initInputs, initOutputs = gbuilder._makeFullIODatasetTypes(taskDatasets)

        self.assertIsInstance(inputs, set)
        self.assertIsInstance(outputs, set)
        self.assertIsInstance(initInputs, set)
        self.assertIsInstance(initOutputs, set)
        self.assertEqual([x.name for x in inputs], ["input"])
        self.assertEqual(set(x.name for x in outputs), set(["output", "output2"]))
        self.assertEqual([x.name for x in initInputs], ['initInput'])
        self.assertEqual([x.name for x in initOutputs], ['initOutputs'])

    def test_makeGraph(self):
        """Test for makeGraph() implementation.
        """
        taskFactory = TaskFactoryMock()
        reg = Registry.fromConfig(RegistryConfig(), SchemaConfig())
        gbuilder = GraphBuilder(taskFactory, reg)

        pipeline = self._makePipeline()
        collection = ""
        userQuery = None
        coll = DatasetOriginInfoDef([collection], collection)
        graph = gbuilder.makeGraph(pipeline, coll, userQuery)

        self.assertEqual(len(graph), 2)
        taskDef = graph[0].taskDef
        self.assertEqual(taskDef.taskName, "TaskOne")
        self.assertEqual(taskDef.taskClass, TaskOne)
        # TODO: temporary until we add some content to regitry
        # quanta = graph[0].quanta
        # self.assertEqual(len(quanta), 10)
        # for quantum in quanta:
        #     self._checkQuantum(quantum.inputs, Dataset1, range(10))
        #     self._checkQuantum(quantum.outputs, Dataset2, range(10))

        taskDef = graph[1].taskDef
        self.assertEqual(taskDef.taskName, "TaskTwo")
        self.assertEqual(taskDef.taskClass, TaskTwo)
        # TODO: temporary until we add some content to regitry
        # quanta = graph[1].quanta
        # self.assertEqual(len(quanta), 10)
        # for quantum in quanta:
        #     self._checkQuantum(quantum.inputs, Dataset2, range(10))
        #     self._checkQuantum(quantum.outputs, Dataset3, range(10))

    def test_makeGraphSelect(self):
        """Test for makeGraph() implementation with subset of data.
        """
        taskFactory = TaskFactoryMock()
        reg = Registry.fromConfig(RegistryConfig(), SchemaConfig())
        gbuilder = GraphBuilder(taskFactory, reg)

        pipeline = self._makePipeline()
        collection = ""
        userQuery = "1 = 1"
        coll = DatasetOriginInfoDef([collection], collection)
        graph = gbuilder.makeGraph(pipeline, coll, userQuery)

        self.assertEqual(len(graph), 2)
        taskDef = graph[0].taskDef
        self.assertEqual(taskDef.taskName, "TaskOne")
        self.assertEqual(taskDef.taskClass, TaskOne)
        # TODO: temporary until we implement makeGraph()
        # quanta = graph[0].quanta
        # self.assertEqual(len(quanta), 3)
        # for quantum in quanta:
        #     self._checkQuantum(quantum.inputs, Dataset1, [1, 5, 9])
        #     self._checkQuantum(quantum.outputs, Dataset2, [1, 5, 9])

        taskDef = graph[1].taskDef
        self.assertEqual(taskDef.taskName, "TaskTwo")
        self.assertEqual(taskDef.taskClass, TaskTwo)
        # TODO: temporary until we implement makeGraph()
        # quanta = graph[1].quanta
        # self.assertEqual(len(quanta), 3)
        # for quantum in quanta:
        #     self._checkQuantum(quantum.inputs, Dataset2, [1, 5, 9])
        #     self._checkQuantum(quantum.outputs, Dataset3, [1, 5, 9])


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
