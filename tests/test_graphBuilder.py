#
# LSST Data Management System
# Copyright 2016-2017 LSST Corporation.
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
from lsst.daf.persistence import DataId
from lsst.log import Log
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase
from lsst.pipe.supertask import GraphBuilder, Pipeline, Quantum, SuperTask, TaskDef
from lsst.obs.base import repodb
from lsst.obs.base.repodb.graph import RepoGraph


class IntTestUnit(repodb.Unit):
    """Special Unit class for testing
    """
    value = repodb.IntField()
    unique = (value,)

    def getDataIdValue(self):
        return self.id


Dataset1 = repodb.Dataset.subclass("ds1", number=IntTestUnit)
Dataset2 = repodb.Dataset.subclass("ds2", number=IntTestUnit)
Dataset3 = repodb.Dataset.subclass("ds3", number=IntTestUnit)


class ButlerMock(object):
    """Mock version of butler, only usable for this test
    """
    def __init__(self):
        self.datasets = {}

    def get(self, datasetType, dataId=None, immediate=True, **rest):
        dataId = DataId(dataId)
        dsdata = self.datasets.get(datasetType)
        if dsdata:
            key = dataId['number']
            return dsdata.get(key)
        return None

    def put(self, obj, datasetType, dataId={}, doBackup=False, **rest):
        dataId = DataId(dataId)
        dataId.update(**rest)
        dsdata = self.datasets.setdefault(datasetType, {})
        key = dataId['number']
        dsdata[key] = obj

class OneToOneTask(SuperTask):
    ConfigClass = pexConfig.Config

    def __init__(self, InputDataSet, OutputDataset, **kwargs):
        SuperTask.__init__(self, **kwargs)
        self.InputDataset = InputDataSet
        self.OutputDataset = OutputDataset

    def defineQuanta(self, repoGraph, butler):
        quanta = []
        for inputDs in repoGraph.datasets[self.InputDataset]:
            outputDs = repoGraph.addDataset(self.OutputDataset, number=inputDs.number)
            quanta.append(Quantum(inputs={self.InputDataset: set([inputDs])},
                                  outputs={self.OutputDataset: set([outputDs])}))
        return quanta

    def runQuantum(self, quantum, butler):
        dataset, = quantum.inputs[self.InputDataset]
        value = dataset.get(butler)
        struct = self.run(value)
        outputCatalogDataset, = quantum.outputs[self.OutputDataset]
        outputCatalogDataset.put(butler, struct.val)

    def run(self, val):
        return pipeBase.Struct(val=val + 100)

    def getDatasetClasses(self):
        return ({self.InputDataset.name: self.InputDataset},
                {self.OutputDataset.name: self.OutputDataset})


class TaskOne(OneToOneTask):
    _DefaultName = "task_one"
    def __init__(self, **kwargs):
        OneToOneTask.__init__(self, Dataset1, Dataset2, **kwargs)


class TaskTwo(OneToOneTask):
    _DefaultName = "task_two"
    def __init__(self, **kwargs):
        OneToOneTask.__init__(self, Dataset2, Dataset3, **kwargs)


class TaskFactoryMock(object):
    def loadTaskClass(self, taskName):
        if taskName == "TaskOne":
            return TaskOne, "TaskOne"
        elif taskName == "TaskTwo":
            return TaskTwo, "TaskTwo"

    def makeTask(self, taskClass, config, overrides, butler):
        if config is None:
            config = taskClass.ConfigClass()
            if overrides:
                overrides.applyTo(config)
        return taskClass(config=config, butler=butler)


class RepoDbMock(object):

    def makeGraph(self, UnitClasses=(), where=None,
                  NeededDatasets=(), FutureDatasets=()):
        datasets = {}
        for DS in NeededDatasets:
            datasets[DS] = set()
        for DS in FutureDatasets:
            datasets[DS] = set()

        units = self._makeUnits()
        repoGraph = RepoGraph(units=units, datasets=datasets)

        # for "query" we support a list of numbers
        if where is not None:
            where = set([int(x) for x in where.split()])
        for unit in units[IntTestUnit]:
            if where is None or unit.value in where:
                for DS in NeededDatasets:
                    repoGraph.addDataset(DS, number=unit)

        return repoGraph

    def _makeUnits(self):
        """Make units universe.
        """
        units = set()
        for i in range(10):
            units.add(IntTestUnit(id=i, value=i))
        return {IntTestUnit: units}


class GraphBuilderTestCase(unittest.TestCase):
    """A test case for GraphBuilder class
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def _makePipeline(self):
        return Pipeline([TaskDef("TaskOne", pexConfig.Config(), TaskOne),
                         TaskDef("TaskTwo", pexConfig.Config(), TaskTwo)])

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
        butler = ButlerMock()
        repoQuery = ""
        taskFactory = TaskFactoryMock()
        repoDb = RepoDbMock()

        gbuilder = GraphBuilder(taskFactory, butler, repoDb, repoQuery)

        tasks = [TaskOne(), TaskTwo()]
        inputClasses, outputClasses = gbuilder.buildIOClasses(tasks)

        self.assertIsInstance(inputClasses, set)
        self.assertIsInstance(outputClasses, set)
        self.assertEqual(inputClasses, set([Dataset1]))
        self.assertEqual(outputClasses, set([Dataset2, Dataset3]))

    def test_makeGraph(self):
        """Test for makeGraph() implementation.
        """
        butler = ButlerMock()
        repoQuery = None
        taskFactory = TaskFactoryMock()
        repoDb = RepoDbMock()
        pipeline = self._makePipeline()

        gbuilder = GraphBuilder(taskFactory, butler, repoDb, repoQuery)

        graph = gbuilder.makeGraph(pipeline)

        self.assertEqual(len(graph), 2)
        taskDef = graph[0].taskDef
        quanta = graph[0].quanta
        self.assertEqual(len(quanta), 10)
        self.assertEqual(taskDef.taskName, "TaskOne")
        self.assertEqual(taskDef.taskClass, TaskOne)
        for quantum in quanta:
            self._checkQuantum(quantum.inputs, Dataset1, range(10))
            self._checkQuantum(quantum.outputs, Dataset2, range(10))

        taskDef = graph[1].taskDef
        quanta = graph[1].quanta
        self.assertEqual(len(quanta), 10)
        self.assertEqual(taskDef.taskName, "TaskTwo")
        self.assertEqual(taskDef.taskClass, TaskTwo)
        for quantum in quanta:
            self._checkQuantum(quantum.inputs, Dataset2, range(10))
            self._checkQuantum(quantum.outputs, Dataset3, range(10))

    def test_makeGraphSelect(self):
        """Test for makeGraph() implementation with subset of data.
        """
        butler = ButlerMock()
        repoQuery = "1 5 9"
        taskFactory = TaskFactoryMock()
        repoDb = RepoDbMock()
        pipeline = self._makePipeline()

        gbuilder = GraphBuilder(taskFactory, butler, repoDb, repoQuery)

        graph = gbuilder.makeGraph(pipeline)

        self.assertEqual(len(graph), 2)
        taskDef = graph[0].taskDef
        quanta = graph[0].quanta
        self.assertEqual(taskDef.taskName, "TaskOne")
        self.assertEqual(taskDef.taskClass, TaskOne)
        self.assertEqual(len(quanta), 3)
        for quantum in quanta:
            self._checkQuantum(quantum.inputs, Dataset1, [1, 5, 9])
            self._checkQuantum(quantum.outputs, Dataset2, [1, 5, 9])
            
        taskDef = graph[1].taskDef
        quanta = graph[1].quanta
        self.assertEqual(taskDef.taskName, "TaskTwo")
        self.assertEqual(taskDef.taskClass, TaskTwo)
        self.assertEqual(len(quanta), 3)
        for quantum in quanta:
            self._checkQuantum(quantum.inputs, Dataset2, [1, 5, 9])
            self._checkQuantum(quantum.outputs, Dataset3, [1, 5, 9])


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()

if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
