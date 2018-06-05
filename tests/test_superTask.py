#
# LSST Data Management System
# Copyright 2016-2017 AURA/LSST.
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

"""Simple unit test for supertask.SuperTask.
"""

from __future__ import absolute_import, division, print_function

import unittest

import lsst.utils.tests
from lsst.daf.butler import DatasetRef, Quantum, Run
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase
from lsst.pipe import supertask
from lsst.pipe.supertask.examples.exampleStorageClass import ExampleStorageClass  # noqa: F401


class ButlerMock():
    """Mock version of butler, only usable for this test
    """
    def __init__(self):
        self.datasets = {}

    @staticmethod
    def key(dataId):
        """Make a dict key out of dataId.
        """
        key = (dataId["camera"], dataId["visit"])
        return tuple(key)

    def get(self, dsTypeName, dataId, parameters=None):
        key = self.key(dataId)
#         print("butler.get: name={} key={}".format(ref.datasetType.name, key))
        dsdata = self.datasets.get(dsTypeName)
        if dsdata:
            return dsdata.get(key)
        return None

    def put(self, inMemoryDataset, dsTypeName, dataId, producer=None):
        key = self.key(dataId)
#         print("butler.put: {} -> name={} key={}".format(inMemoryDataset, ref.datasetType.name, key))
        dsdata = self.datasets.setdefault(dsTypeName, {})
        dsdata[key] = inMemoryDataset


class AddConfig(supertask.SuperTaskConfig):
    addend = pexConfig.Field(doc="amount to add", dtype=int, default=3)
    input = pexConfig.ConfigField(dtype=supertask.InputDatasetConfig,
                                  doc="Input dataset type for this task")
    output = pexConfig.ConfigField(dtype=supertask.OutputDatasetConfig,
                                   doc="Output dataset type for this task")

    def setDefaults(self):
        # set units of a quantum, this task uses per-visit quanta and it
        # expects dataset units to be the same
        self.quantum.units = ["Camera", "Visit"]
        self.quantum.sql = None

        # default config for input dataset type
        self.input.name = "add_input"
        self.input.units = ["Camera", "Visit"]
        self.input.storageClass = "example"

        # default config for output dataset type
        self.output.name = "add_output"
        self.output.units = ["Camera", "Visit"]
        self.output.storageClass = "example"


class AddTask(supertask.SuperTask):
    ConfigClass = AddConfig
    _DefaultName = "add_task"

    def run(self, input, output):
        self.metadata.add("add", self.config.addend)
        output = [val + self.config.addend for val in input]
        return pipeBase.Struct(output=output)


class SuperTaskTestCase(unittest.TestCase):
    """A test case for supertask.SuperTask
    """

    def _makeDSRefVisit(self, dstype, visitId):
            return DatasetRef(datasetType=dstype,
                              dataId=dict(camera="X",
                                          visit=visitId,
                                          physical_filter='a',
                                          abstract_filter='b'))

    def _makeQuanta(self, config):
        """Create set of Quanta
        """
        run = Run(collection=1, environment=None, pipeline=None)

        dstype0 = supertask.SuperTask.makeDatasetType(config.input)
        dstype1 = supertask.SuperTask.makeDatasetType(config.output)

        quanta = []
        for visit in range(100):
            quantum = Quantum(run=run, task=None)
            quantum.addPredictedInput(self._makeDSRefVisit(dstype0, visit))
            quantum.addOutput(self._makeDSRefVisit(dstype1, visit))
            quanta.append(quantum)

        return quanta

    def testRunQuantum(self):
        """Test for AddTask.runQuantum() implementation.
        """
        butler = ButlerMock()
        task = AddTask(config=AddConfig())

        # make all quanta
        quanta = self._makeQuanta(task.config)

        # add input data to butler
        dstype0 = supertask.SuperTask.makeDatasetType(task.config.input)
        for i, quantum in enumerate(quanta):
            ref = quantum.predictedInputs[dstype0.name][0]
            butler.put(100 + i, dstype0.name, ref.dataId)

        # run task on each quanta
        for quantum in quanta:
            task.runQuantum(quantum, butler)

        # look at the output produced by the task
        outputName = task.config.output.name
        dsdata = butler.datasets[outputName]
        self.assertEqual(len(dsdata), len(quanta))
        for i, quantum in enumerate(quanta):
            ref = quantum.outputs[outputName][0]
            self.assertEqual(dsdata[butler.key(ref.dataId)], 100 + i + 3)

    def testChain2(self):
        """Test for two-task chain.
        """
        butler = ButlerMock()
        task1 = AddTask(config=AddConfig())
        config2 = AddConfig()
        config2.addend = 200
        config2.input.name = task1.config.output.name
        config2.output.name = "add_output_2"
        task2 = AddTask(config=config2)

        # make all quanta
        quanta1 = self._makeQuanta(task1.config)
        quanta2 = self._makeQuanta(task2.config)

        # add input data to butler
        dstype0 = supertask.SuperTask.makeDatasetType(task1.config.input)
        for i, quantum in enumerate(quanta1):
            ref = quantum.predictedInputs[dstype0.name][0]
            butler.put(100 + i, dstype0.name, ref.dataId)

        # run task on each quanta
        for quantum in quanta1:
            task1.runQuantum(quantum, butler)
        for quantum in quanta2:
            task2.runQuantum(quantum, butler)

        # look at the output produced by the task
        outputName = task1.config.output.name
        dsdata = butler.datasets[outputName]
        self.assertEqual(len(dsdata), len(quanta1))
        for i, quantum in enumerate(quanta1):
            ref = quantum.outputs[outputName][0]
            self.assertEqual(dsdata[butler.key(ref.dataId)], 100 + i + 3)

        outputName = task2.config.output.name
        dsdata = butler.datasets[outputName]
        self.assertEqual(len(dsdata), len(quanta2))
        for i, quantum in enumerate(quanta2):
            ref = quantum.outputs[outputName][0]
            self.assertEqual(dsdata[butler.key(ref.dataId)], 100 + i + 3 + 200)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
