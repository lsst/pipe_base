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
from typing import Any

import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase
import lsst.utils.logging
import lsst.utils.tests
from lsst.daf.butler import (
    DataCoordinate,
    DatasetIdFactory,
    DatasetRef,
    DatasetType,
    DimensionUniverse,
    Quantum,
)


class ButlerMock:
    """Mock version of butler, only usable for this test"""

    def __init__(self) -> None:
        self.datasets: dict[str, dict[DataCoordinate, Any]] = {}
        self.registry = SimpleNamespace(dimensions=DimensionUniverse())

    def getDirect(self, ref: DatasetRef) -> Any:
        # getDirect requires resolved ref
        assert ref.id is not None
        dsdata = self.datasets.get(ref.datasetType.name)
        if dsdata:
            return dsdata.get(ref.dataId)
        return None

    def put(self, inMemoryDataset: Any, dsRef: DatasetRef, producer: Any = None):
        # put requires unresolved ref
        assert dsRef.id is None
        key = dsRef.dataId
        name = dsRef.datasetType.name
        dsdata = self.datasets.setdefault(name, {})
        dsdata[key] = inMemoryDataset

    def putDirect(self, obj: Any, ref: DatasetRef):
        # putDirect requires resolved ref
        assert ref.id is not None
        self.put(obj, ref.unresolved())


class AddConnections(pipeBase.PipelineTaskConnections, dimensions=["instrument", "visit"]):
    input = pipeBase.connectionTypes.Input(
        name="add_input",
        dimensions=["instrument", "visit", "detector", "physical_filter", "band"],
        storageClass="Catalog",
        doc="Input dataset type for this task",
    )
    output = pipeBase.connectionTypes.Output(
        name="add_output",
        dimensions=["instrument", "visit", "detector", "physical_filter", "band"],
        storageClass="Catalog",
        doc="Output dataset type for this task",
    )


class AddConfig(pipeBase.PipelineTaskConfig, pipelineConnections=AddConnections):
    addend = pexConfig.Field[int](doc="amount to add", default=3)


# example task which overrides run() method
class AddTask(pipeBase.PipelineTask):
    ConfigClass = AddConfig
    _DefaultName = "add_task"

    def run(self, input: int) -> pipeBase.Struct:
        self.metadata.add("add", self.config.addend)
        output = input + self.config.addend
        return pipeBase.Struct(output=output)


# example task which overrides adaptArgsAndRun() method
class AddTask2(pipeBase.PipelineTask):
    ConfigClass = AddConfig
    _DefaultName = "add_task"

    def runQuantum(
        self, butlerQC: pipeBase.ButlerQuantumContext, inputRefs: DatasetRef, outputRefs: DatasetRef
    ) -> None:
        self.metadata.add("add", self.config.addend)
        inputs = butlerQC.get(inputRefs)
        outputs = inputs["input"] + self.config.addend
        butlerQC.put(pipeBase.Struct(output=outputs), outputRefs)


class PipelineTaskTestCase(unittest.TestCase):
    """A test case for PipelineTask"""

    datasetIdFactory = DatasetIdFactory()

    def _resolve_ref(self, ref: DatasetRef) -> DatasetRef:
        return self.datasetIdFactory.resolveRef(ref, "test")

    def _makeDSRefVisit(
        self, dstype: DatasetType, visitId: int, universe: DimensionUniverse, resolve: bool = False
    ) -> DatasetRef:
        ref = DatasetRef(
            datasetType=dstype,
            dataId=DataCoordinate.standardize(
                detector="X",
                visit=visitId,
                physical_filter="a",
                band="b",
                instrument="TestInstrument",
                universe=universe,
            ),
        )
        if resolve:
            ref = self._resolve_ref(ref)
        return ref

    def _makeQuanta(
        self, config: pipeBase.PipelineTaskConfig, nquanta: int = 100, resolve_outputs: bool = False
    ) -> list[Quantum]:
        """Create set of Quanta"""
        universe = DimensionUniverse()
        connections = config.connections.ConnectionsClass(config=config)

        dstype0 = connections.input.makeDatasetType(universe)
        dstype1 = connections.output.makeDatasetType(universe)

        quanta = []
        for visit in range(nquanta):
            inputRef = self._makeDSRefVisit(dstype0, visit, universe, resolve=True)
            outputRef = self._makeDSRefVisit(dstype1, visit, universe, resolve=resolve_outputs)
            quantum = Quantum(
                inputs={inputRef.datasetType: [inputRef]}, outputs={outputRef.datasetType: [outputRef]}
            )
            quanta.append(quantum)

        return quanta

    def testRunQuantumFull(self):
        """Test for AddTask.runQuantum() implementation with full butler."""
        self._testRunQuantum(full_butler=True)

    def testRunQuantumLimited(self):
        """Test for AddTask.runQuantum() implementation with limited butler."""
        self._testRunQuantum(full_butler=False)

    def _testRunQuantum(self, full_butler: bool) -> None:
        """Test for AddTask.runQuantum() implementation."""

        butler = ButlerMock()
        task = AddTask(config=AddConfig())
        connections = task.config.connections.ConnectionsClass(config=task.config)

        # make all quanta
        quanta = self._makeQuanta(task.config, resolve_outputs=not full_butler)

        # add input data to butler
        dstype0 = connections.input.makeDatasetType(butler.registry.dimensions)
        for i, quantum in enumerate(quanta):
            ref = quantum.inputs[dstype0.name][0]
            butler.putDirect(100 + i, ref)

        # run task on each quanta
        checked_get = False
        for quantum in quanta:
            if full_butler:
                butlerQC = pipeBase.ButlerQuantumContext.from_full(butler, quantum)
            else:
                butlerQC = pipeBase.ButlerQuantumContext.from_limited(butler, quantum)
            inputRefs, outputRefs = connections.buildDatasetRefs(quantum)
            task.runQuantum(butlerQC, inputRefs, outputRefs)

            # Test getting of datasets in different ways.
            # (only need to do this one time)
            if not checked_get:
                # Force the periodic logger to issue messages.
                lsst.utils.logging.PeriodicLogger.LOGGING_INTERVAL = 0.0

                checked_get = True
                with self.assertLogs("lsst.pipe.base", level=lsst.utils.logging.VERBOSE) as cm:
                    input_data = butlerQC.get(inputRefs)
                self.assertIn("Completed", cm.output[-1])
                self.assertEqual(len(input_data), len(inputRefs))

                # In this test there are no multiples returned.
                refs = [ref for _, ref in inputRefs]
                with self.assertLogs("lsst.pipe.base", level=lsst.utils.logging.VERBOSE) as cm:
                    list_get = butlerQC.get(refs)

                lsst.utils.logging.PeriodicLogger.LOGGING_INTERVAL = 600.0

                self.assertIn("Completed", cm.output[-1])
                self.assertEqual(len(list_get), len(input_data))
                self.assertIsInstance(list_get[0], int)
                scalar_get = butlerQC.get(refs[0])
                self.assertEqual(scalar_get, list_get[0])

                with self.assertRaises(TypeError):
                    butlerQC.get({})

                # Output ref won't be known to this quantum.
                outputs = [ref for _, ref in outputRefs]
                with self.assertRaises(ValueError):
                    butlerQC.get(outputs[0])

        # look at the output produced by the task
        outputName = connections.output.name
        dsdata = butler.datasets[outputName]
        self.assertEqual(len(dsdata), len(quanta))
        for i, quantum in enumerate(quanta):
            ref = quantum.outputs[outputName][0]
            self.assertEqual(dsdata[ref.dataId], 100 + i + 3)

    def testChain2Full(self) -> None:
        """Test for two-task chain with full butler."""
        self._testChain2(full_butler=True)

    def testChain2Limited(self) -> None:
        """Test for two-task chain with limited butler."""
        self._testChain2(full_butler=True)

    def _testChain2(self, full_butler: bool) -> None:
        """Test for two-task chain."""
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
        quanta1 = self._makeQuanta(task1.config, resolve_outputs=not full_butler)
        quanta2 = self._makeQuanta(task2.config, resolve_outputs=not full_butler)

        # add input data to butler
        task1Connections = task1.config.connections.ConnectionsClass(config=task1.config)
        dstype0 = task1Connections.input.makeDatasetType(butler.registry.dimensions)
        for i, quantum in enumerate(quanta1):
            ref = quantum.inputs[dstype0.name][0]
            butler.putDirect(100 + i, ref)

        butler_qc_factory = (
            pipeBase.ButlerQuantumContext.from_full
            if full_butler
            else pipeBase.ButlerQuantumContext.from_limited
        )

        # run task on each quanta
        for quantum in quanta1:
            butlerQC = butler_qc_factory(butler, quantum)
            inputRefs, outputRefs = connections1.buildDatasetRefs(quantum)
            task1.runQuantum(butlerQC, inputRefs, outputRefs)
        for quantum in quanta2:
            butlerQC = butler_qc_factory(butler, quantum)
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

    def testButlerQC(self):
        """Test for ButlerQuantumContext. Full and limited share get
        implementation so only full is tested.
        """

        butler = ButlerMock()
        task = AddTask(config=AddConfig())
        connections = task.config.connections.ConnectionsClass(config=task.config)

        # make one quantum
        (quantum,) = self._makeQuanta(task.config, 1)

        # add input data to butler
        dstype0 = connections.input.makeDatasetType(butler.registry.dimensions)
        ref = quantum.inputs[dstype0.name][0]
        butler.putDirect(100, ref)

        butlerQC = pipeBase.ButlerQuantumContext.from_full(butler, quantum)

        # Pass ref as single argument or a list.
        obj = butlerQC.get(ref)
        self.assertEqual(obj, 100)
        obj = butlerQC.get([ref])
        self.assertEqual(obj, [100])

        # Pass None instead of a ref.
        obj = butlerQC.get(None)
        self.assertIsNone(obj)
        obj = butlerQC.get([None])
        self.assertEqual(obj, [None])

        # COmbine a ref and None.
        obj = butlerQC.get([ref, None])
        self.assertEqual(obj, [100, None])

        # Use refs from a QuantizedConnection.
        inputRefs, outputRefs = connections.buildDatasetRefs(quantum)
        obj = butlerQC.get(inputRefs)
        self.assertEqual(obj, {"input": 100})

        # Add few None values to a QuantizedConnection.
        inputRefs.input = [None, ref]
        inputRefs.input2 = None
        obj = butlerQC.get(inputRefs)
        self.assertEqual(obj, {"input": [None, 100], "input2": None})


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
