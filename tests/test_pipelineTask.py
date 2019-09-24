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

"""Simple unit test for PipelineTask.
"""

import unittest
from types import SimpleNamespace

import lsst.utils.tests
from lsst.daf.butler import DatasetRef, Quantum, Run, DimensionUniverse, DataCoordinate
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase


class ButlerMock():
    """Mock version of butler, only usable for this test
    """
    def __init__(self):
        self.datasets = {}
        self.registry = SimpleNamespace(dimensions=DimensionUniverse())

    def get(self, datasetRefOrType, dataId=None):
        if isinstance(datasetRefOrType, DatasetRef):
            dataId = datasetRefOrType.dataId
            dsTypeName = datasetRefOrType.datasetType.name
        else:
            dsTypeName = datasetRefOrType
        key = dataId
        dsdata = self.datasets.get(dsTypeName)
        if dsdata:
            return dsdata.get(key)
        return None

    def put(self, inMemoryDataset, dsRef, producer=None):
        key = dsRef.dataId
        if isinstance(dsRef.datasetType, str):
            name = dsRef.datasetType
        else:
            name = dsRef.datasetType.name
        dsdata = self.datasets.setdefault(name, {})
        dsdata[key] = inMemoryDataset


class AddConnections(pipeBase.PipelineTaskConnections, dimensions=["instrument", "visit"]):
    input = pipeBase.connectionTypes.Input(name="add_input",
                                           dimensions=["instrument", "visit", "detector",
                                                       "physical_filter", "abstract_filter"],
                                           storageClass="Catalog",
                                           doc="Input dataset type for this task")
    output = pipeBase.connectionTypes.Output(name="add_output",
                                             dimensions=["instrument", "visit", "detector",
                                                         "physical_filter", "abstract_filter"],
                                             storageClass="Catalog",
                                             doc="Output dataset type for this task")


class AddConfig(pipeBase.PipelineTaskConfig, pipelineConnections=AddConnections):
    addend = pexConfig.Field(doc="amount to add", dtype=int, default=3)


# example task which overrides run() method
class AddTask(pipeBase.PipelineTask):
    ConfigClass = AddConfig
    _DefaultName = "add_task"

    def run(self, input):
        self.metadata.add("add", self.config.addend)
        output = input + self.config.addend
        return pipeBase.Struct(output=output)


# example task which overrides adaptArgsAndRun() method
class AddTask2(pipeBase.PipelineTask):
    ConfigClass = AddConfig
    _DefaultName = "add_task"

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        self.metadata.add("add", self.config.addend)
        inputs = butlerQC.get(inputRefs)
        outputs = inputs['input'] + self.config.addend
        butlerQC.put(pipeBase.Struct(output=outputs), outputRefs)


class PipelineTaskTestCase(unittest.TestCase):
    """A test case for PipelineTask
    """

    def _makeDSRefVisit(self, dstype, visitId, universe):
        return DatasetRef(
            datasetType=dstype,
            dataId=DataCoordinate.standardize(
                detector="X",
                visit=visitId,
                physical_filter='a',
                abstract_filter='b',
                instrument='TestInstrument',
                universe=universe
            )
        )

    def _makeQuanta(self, config):
        """Create set of Quanta
        """
        universe = DimensionUniverse()
        run = Run(collection=1, environment=None, pipeline=None)
        connections = config.connections.ConnectionsClass(config=config)

        dstype0 = connections.input.makeDatasetType(universe)
        dstype1 = connections.output.makeDatasetType(universe)

        quanta = []
        for visit in range(100):
            quantum = Quantum(run=run)
            quantum.addPredictedInput(self._makeDSRefVisit(dstype0, visit, universe))
            quantum.addOutput(self._makeDSRefVisit(dstype1, visit, universe))
            quanta.append(quantum)

        return quanta

    def testRunQuantum(self):
        """Test for AddTask.runQuantum() implementation.
        """
        butler = ButlerMock()
        task = AddTask(config=AddConfig())
        connections = task.config.connections.ConnectionsClass(config=task.config)

        # make all quanta
        quanta = self._makeQuanta(task.config)

        # add input data to butler
        dstype0 = connections.input.makeDatasetType(butler.registry.dimensions)
        for i, quantum in enumerate(quanta):
            ref = quantum.predictedInputs[dstype0.name][0]
            butler.put(100 + i, pipeBase.Struct(datasetType=dstype0.name, dataId=ref.dataId))

        # run task on each quanta
        for quantum in quanta:
            butlerQC = pipeBase.ButlerQuantumContext(butler, quantum)
            inputRefs, outputRefs = connections.buildDatasetRefs(quantum)
            task.runQuantum(butlerQC, inputRefs, outputRefs)

        # look at the output produced by the task
        outputName = connections.output.name
        dsdata = butler.datasets[outputName]
        self.assertEqual(len(dsdata), len(quanta))
        for i, quantum in enumerate(quanta):
            ref = quantum.outputs[outputName][0]
            self.assertEqual(dsdata[ref.dataId], 100 + i + 3)

    def testChain2(self):
        """Test for two-task chain.
        """
        butler = ButlerMock()
        config1 = AddConfig()
        connections1 = config1.connections.ConnectionsClass(config=config1)
        task1 = AddTask(config=config1)
        config2 = AddConfig()
        config2.addend = 200
        config2.connections.input = task1.config.connections.output
        config2.connections.output = "add_output_2"
        task2 = AddTask2(config=config2)
        connections2 = config2.connections.ConnectionsClass(config=config2)

        # make all quanta
        quanta1 = self._makeQuanta(task1.config)
        quanta2 = self._makeQuanta(task2.config)

        # add input data to butler
        task1Connections = task1.config.connections.ConnectionsClass(config=task1.config)
        dstype0 = task1Connections.input.makeDatasetType(butler.registry.dimensions)
        for i, quantum in enumerate(quanta1):
            ref = quantum.predictedInputs[dstype0.name][0]
            butler.put(100 + i, pipeBase.Struct(datasetType=dstype0.name, dataId=ref.dataId))

        # run task on each quanta
        for quantum in quanta1:
            butlerQC = pipeBase.ButlerQuantumContext(butler, quantum)
            inputRefs, outputRefs = connections1.buildDatasetRefs(quantum)
            task1.runQuantum(butlerQC, inputRefs, outputRefs)
        for quantum in quanta2:
            butlerQC = pipeBase.ButlerQuantumContext(butler, quantum)
            inputRefs, outputRefs = connections2.buildDatasetRefs(quantum)
            task2.runQuantum(butlerQC, inputRefs, outputRefs)

        # look at the output produced by the task
        outputName = task1.config.connections.output
        dsdata = butler.datasets[outputName]
        self.assertEqual(len(dsdata), len(quanta1))
        for i, quantum in enumerate(quanta1):
            ref = quantum.outputs[outputName][0]
            self.assertEqual(dsdata[ref.dataId], 100 + i + 3)

        outputName = task2.config.connections.output
        dsdata = butler.datasets[outputName]
        self.assertEqual(len(dsdata), len(quanta2))
        for i, quantum in enumerate(quanta2):
            ref = quantum.outputs[outputName][0]
            self.assertEqual(dsdata[ref.dataId], 100 + i + 3 + 200)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
