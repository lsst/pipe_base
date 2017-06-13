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

import lsst.utils.tests
from lsst.daf.persistence import DataId
from lsst.log import Log
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase
from lsst.pipe.supertask import Quantum, SuperTask
from lsst.obs.base import repodb
from lsst.obs.base.repodb.graph import RepoGraph


class TestIntUnit(repodb.Unit):
    """Special Unit class for testing
    """
    value = repodb.IntField()
    unique = (value,)

    def getDataIdValue(self):
        return self.id

class ButlerMock():
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


class AddConfig(pexConfig.Config):
    addend = pexConfig.Field(doc="amount to add", dtype=int, default=3)
    inputCatalog = pexConfig.Field(dtype=str, default="add_input",
                                   doc="Name of the catalog with input data.")
    outputCatalog = pexConfig.Field(dtype=str, default="add_output",
                                    doc="Name of the catalog for output data.")

class AddTask(SuperTask):
    ConfigClass = AddConfig
    _DefaultName = "add_task"

    def __init__(self, **kwargs):
        SuperTask.__init__(self, **kwargs)
        self.InputCatalog = repodb.Dataset.subclass(self.config.inputCatalog,
                                                    number=TestIntUnit)
        self.OutputCatalog = repodb.Dataset.subclass(self.config.outputCatalog,
                                                     number=TestIntUnit)

    def defineQuanta(self, repoGraph, butler):
        quanta = []
        for inputDs in repoGraph.datasets[self.InputCatalog]:
            print('inputDs:', inputDs)
            outputDs = repoGraph.addDataset(self.OutputCatalog,
                                            number=inputDs.number)
            quanta.append(Quantum(inputs={self.InputCatalog: set([inputDs])},
                                  outputs={self.OutputCatalog: set([outputDs])}))
        return quanta

    def runQuantum(self, quantum, butler):
        dataset, = quantum.inputs[self.InputCatalog]
        value = dataset.get(butler)
        struct = self.run(value)
        outputCatalogDataset, = quantum.outputs[self.OutputCatalog]
        outputCatalogDataset.put(butler, struct.val)

    def run(self, val):
        self.metadata.add("add", self.config.addend)
        return pipeBase.Struct(
            val=val + self.config.addend,
        )

    def getDatasetClasses(self):
        return ({self.InputCatalog.name: self.InputCatalog},
                {self.OutputCatalog.name: self.OutputCatalog})


class SuperTaskTestCase(unittest.TestCase):
    """A test case for SuperTask
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def _makeUnits(self):
        """Make units universe.
        """
        units = set()
        for i in range(1000):
            units.add(TestIntUnit(id=i, value=i))
        return {TestIntUnit: units}

    def _makeRepoGraph(self, task, task2=None):
        """Create initial RepoGraph with units and all empty datasets
        """
        datasets = {task.InputCatalog: set(),
                    task.OutputCatalog: set()}
        if task2:
            datasets[task2.InputCatalog] = set()
            datasets[task2.OutputCatalog] = set()
        units = self._makeUnits()
        return RepoGraph(units=units, datasets=datasets)

    def testDefineQuanta(self):
        """Test for AddTask.defineQuanta() implementation.
        """
        butler = ButlerMock()
        task = AddTask()

        # one input dataset
        repoGraph = self._makeRepoGraph(task)
        repoGraph.addDataset(task.InputCatalog, number=TestIntUnit(id=5, value=5))

        print('testing with 1 dataset')
        quanta = task.defineQuanta(repoGraph, butler)
        self.assertEqual(len(quanta), 1)
        self.assertEqual(len(quanta[0].inputs), 1)
        self.assertEqual(len(quanta[0].outputs), 1)
        for key, val in quanta[0].inputs.items():
            self.assertEqual(len(val), 1)
        for key, val in quanta[0].outputs.items():
            self.assertEqual(len(val), 1)

        # three input datasets
        repoGraph = self._makeRepoGraph(task)
        repoGraph.addDataset(task.InputCatalog, number=TestIntUnit(id=5, value=5))
        repoGraph.addDataset(task.InputCatalog, number=TestIntUnit(id=10, value=10))
        repoGraph.addDataset(task.InputCatalog, number=TestIntUnit(id=100, value=100))

        print('testing with 3 datasets')
        quanta = task.defineQuanta(repoGraph, butler)
        self.assertEqual(len(quanta), 3)
        for key, val in quanta[0].inputs.items():
            self.assertEqual(len(val), 1)
        for key, val in quanta[0].outputs.items():
            self.assertEqual(len(val), 1)

        # many input datasets
        repoGraph = self._makeRepoGraph(task)
        for i in range(10):
            repoGraph.addDataset(task.InputCatalog, number=TestIntUnit(id=i, value=i))

        print('testing with many datasets')
        quanta = task.defineQuanta(repoGraph, butler)
        self.assertEqual(len(quanta), 10)

    def testRunQuantum(self):
        """Test for AddTask.runQuantum() implementation.
        """
        butler = ButlerMock()
        task = AddTask()

        # one input dataset
        repoGraph = self._makeRepoGraph(task)
        ds = repoGraph.addDataset(task.InputCatalog, number=TestIntUnit(id=5, value=5))
        ds.put(butler, ds.number.value)

        quanta = task.defineQuanta(repoGraph, butler)
        self.assertEqual(len(quanta), 1)

        for quantum in quanta:
            task.runQuantum(quantum, butler)

        dsdata = butler.datasets[task.OutputCatalog.name]
        self.assertEqual(len(dsdata), 1)
        self.assertEqual(dsdata[ds.number.id], ds.number.value + 3)

    def testChain2(self):
        """Test for two-task chain.
        """
        butler = ButlerMock()
        task1 = AddTask()
        config2 = AddConfig()
        config2.addend = 200
        config2.inputCatalog = task1.config.outputCatalog
        config2.outputCatalog = "add_output_2"
        task2 = AddTask(config=config2)

        # one input dataset
        repoGraph = self._makeRepoGraph(task1, task2)
        ds = repoGraph.addDataset(task1.InputCatalog, number=TestIntUnit(id=5, value=5))
        ds.put(butler, ds.number.value)

        quanta1 = task1.defineQuanta(repoGraph, butler)
        quanta2 = task2.defineQuanta(repoGraph, butler)

        self.assertEqual(len(quanta1), 1)
        self.assertEqual(len(quanta1[0].inputs), 1)
        self.assertEqual(len(quanta1[0].outputs), 1)
        self.assertEqual(len(quanta2), 1)
        self.assertEqual(len(quanta2[0].inputs), 1)
        self.assertEqual(len(quanta2[0].outputs), 1)

        for quantum in quanta1:
            task1.runQuantum(quantum, butler)
        for quantum in quanta2:
            task2.runQuantum(quantum, butler)

        dsdata = butler.datasets[task1.OutputCatalog.name]
        self.assertEqual(len(dsdata), 1)
        self.assertEqual(dsdata[ds.number.id], ds.number.value + 3)

        dsdata = butler.datasets[task2.OutputCatalog.name]
        self.assertEqual(len(dsdata), 1)
        self.assertEqual(dsdata[ds.number.id], ds.number.value + 3 + 200)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()

if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
