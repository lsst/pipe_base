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

"""Simple unit test for PipelineTask."""

import copy
import pickle
import unittest
from typing import Any

import astropy.units as u

import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase
import lsst.utils.logging
import lsst.utils.tests
from lsst.daf.butler import (
    DataCoordinate,
    DatasetProvenance,
    DatasetRef,
    DatasetType,
    DimensionUniverse,
    Quantum,
)


class ButlerMock:
    """Mock version of butler, only usable for this test."""

    def __init__(self) -> None:
        self.datasets: dict[str, dict[DataCoordinate, Any]] = {}
        self.dimensions = DimensionUniverse()

    def get(self, ref: DatasetRef) -> Any:
        dsdata = self.datasets.get(ref.datasetType.name)
        if dsdata:
            return dsdata.get(ref.dataId)
        return None

    def put(
        self,
        inMemoryDataset: Any,
        dsRef: DatasetRef,
        provenance: DatasetProvenance | None = None,
        producer: Any = None,
    ):
        key = dsRef.dataId
        name = dsRef.datasetType.name
        dsdata = self.datasets.setdefault(name, {})
        dsdata[key] = inMemoryDataset


class AddConnections(pipeBase.PipelineTaskConnections, dimensions=["instrument", "visit"]):
    """Connections for the AddTask."""

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
    """Config for the AddTask."""

    addend = pexConfig.Field[int](doc="amount to add", default=3)


class AddTask(pipeBase.PipelineTask):
    """Example task which overrides run() method."""

    ConfigClass = AddConfig
    _DefaultName = "add_task"

    def run(self, input: int) -> pipeBase.Struct:
        self.metadata.add("add", self.config.addend)
        output = input + self.config.addend
        return pipeBase.Struct(output=output)


class AddTask2(pipeBase.PipelineTask):
    """Example task which overrides runQuantum() method."""

    ConfigClass = AddConfig
    _DefaultName = "add_task_2"

    def runQuantum(
        self, butlerQC: pipeBase.QuantumContext, inputRefs: DatasetRef, outputRefs: DatasetRef
    ) -> None:
        self.metadata.add("add", self.config.addend)
        inputs = butlerQC.get(inputRefs)
        outputs = inputs["input"] + self.config.addend
        butlerQC.put(pipeBase.Struct(output=outputs), outputRefs)


class PipelineTaskTestCase(unittest.TestCase):
    """A test case for PipelineTask."""

    def _makeDSRefVisit(self, dstype: DatasetType, visitId: int) -> DatasetRef:
        dataId = DataCoordinate.standardize(
            detector="X",
            visit=visitId,
            physical_filter="a",
            band="b",
            instrument="TestInstrument",
            dimensions=dstype.dimensions,
        )
        run = "test"
        ref = DatasetRef(datasetType=dstype, dataId=dataId, run=run)
        return ref

    def _makeQuanta(
        self,
        task_node: pipeBase.pipeline_graph.TaskNode,
        pipeline_graph: pipeBase.PipelineGraph,
        nquanta: int = 100,
    ) -> list[Quantum]:
        """Create set of Quanta"""
        quanta = []
        for visit in range(nquanta):
            quantum = Quantum(
                inputs={
                    pipeline_graph.dataset_types[edge.dataset_type_name].dataset_type: [
                        self._makeDSRefVisit(
                            pipeline_graph.dataset_types[edge.dataset_type_name].dataset_type, visit
                        )
                    ]
                    for edge in task_node.inputs.values()
                },
                outputs={
                    pipeline_graph.dataset_types[edge.dataset_type_name].dataset_type: [
                        self._makeDSRefVisit(
                            pipeline_graph.dataset_types[edge.dataset_type_name].dataset_type, visit
                        )
                    ]
                    for edge in task_node.outputs.values()
                },
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
        input_name = task.config.connections.input
        output_name = task.config.connections.output

        pipeline_graph = pipeBase.PipelineGraph()
        task_node = pipeline_graph.add_task(None, type(task), task.config)
        pipeline_graph.resolve(dimensions=butler.dimensions, dataset_types={})

        # make all quanta
        quanta = self._makeQuanta(task_node, pipeline_graph)

        # add input data to butler
        dstype0 = pipeline_graph.dataset_types[input_name].dataset_type
        for i, quantum in enumerate(quanta):
            ref = quantum.inputs[dstype0.name][0]
            butler.put(100 + i, ref)

        # run task on each quanta
        checked_get = False
        for quantum in quanta:
            butlerQC = pipeBase.QuantumContext(butler, quantum)
            inputRefs, outputRefs = task_node.get_connections().buildDatasetRefs(quantum)
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
        dsdata = butler.datasets[output_name]
        self.assertEqual(len(dsdata), len(quanta))
        for i, quantum in enumerate(quanta):
            ref = quantum.outputs[output_name][0]
            self.assertEqual(dsdata[ref.dataId], 100 + i + 3)

    def testChain2Full(self) -> None:
        """Test for two-task chain with full butler."""
        self._testChain2(full_butler=True)

    def testChain2Limited(self) -> None:
        """Test for two-task chain with limited butler."""
        self._testChain2(full_butler=False)

    def _testChain2(self, full_butler: bool) -> None:
        """Test for two-task chain."""
        butler = ButlerMock()
        config1 = AddConfig()
        overall_input_name = config1.connections.input
        intermediate_name = config1.connections.output
        task1 = AddTask(config=config1)
        config2 = AddConfig()
        config2.addend = 200
        config2.connections.input = intermediate_name
        overall_output_name = "add_output_2"
        config2.connections.output = overall_output_name
        task2 = AddTask2(config=config2)

        pipeline_graph = pipeBase.PipelineGraph()
        task1_node = pipeline_graph.add_task(None, type(task1), task1.config)
        task2_node = pipeline_graph.add_task(None, type(task2), task2.config)
        pipeline_graph.resolve(dimensions=butler.dimensions, dataset_types={})

        # make all quanta
        quanta1 = self._makeQuanta(task1_node, pipeline_graph)
        quanta2 = self._makeQuanta(task2_node, pipeline_graph)

        # add input data to butler
        dstype0 = pipeline_graph.dataset_types[overall_input_name].dataset_type
        for i, quantum in enumerate(quanta1):
            ref = quantum.inputs[dstype0.name][0]
            butler.put(100 + i, ref)

        # run task on each quanta
        for quantum in quanta1:
            butlerQC = pipeBase.QuantumContext(butler, quantum)
            inputRefs, outputRefs = task1_node.get_connections().buildDatasetRefs(quantum)
            task1.runQuantum(butlerQC, inputRefs, outputRefs)
        for quantum in quanta2:
            butlerQC = pipeBase.QuantumContext(butler, quantum)
            inputRefs, outputRefs = task2_node.get_connections().buildDatasetRefs(quantum)
            task2.runQuantum(butlerQC, inputRefs, outputRefs)

        # look at the output produced by the task
        dsdata = butler.datasets[intermediate_name]
        self.assertEqual(len(dsdata), len(quanta1))
        for i, quantum in enumerate(quanta1):
            ref = quantum.outputs[intermediate_name][0]
            self.assertEqual(dsdata[ref.dataId], 100 + i + 3)

        dsdata = butler.datasets[overall_output_name]
        self.assertEqual(len(dsdata), len(quanta2))
        for i, quantum in enumerate(quanta2):
            ref = quantum.outputs[overall_output_name][0]
            self.assertEqual(dsdata[ref.dataId], 100 + i + 3 + 200)

    def testButlerQC(self):
        """Test for QuantumContext. Full and limited share get
        implementation so only full is tested.
        """
        butler = ButlerMock()
        task = AddTask(config=AddConfig())
        input_name = task.config.connections.input

        pipeline_graph = pipeBase.PipelineGraph()
        task_node = pipeline_graph.add_task(None, type(task), config=task.config)
        pipeline_graph.resolve(dimensions=butler.dimensions, dataset_types={})

        # make one quantum
        (quantum,) = self._makeQuanta(task_node, pipeline_graph, 1)

        # add input data to butler
        dstype0 = pipeline_graph.dataset_types[input_name].dataset_type
        ref = quantum.inputs[dstype0.name][0]
        butler.put(100, ref)

        butlerQC = pipeBase.QuantumContext(butler, quantum)
        self.assertEqual(butlerQC.resources.num_cores, 1)
        self.assertIsNone(butlerQC.resources.max_mem)

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
        inputRefs, outputRefs = task_node.get_connections().buildDatasetRefs(quantum)
        obj = butlerQC.get(inputRefs)
        self.assertEqual(obj, {"input": 100})

        # Add few None values to a QuantizedConnection.
        inputRefs.input = [None, ref]
        inputRefs.input2 = None
        obj = butlerQC.get(inputRefs)
        self.assertEqual(obj, {"input": [None, 100], "input2": None})

        # Set additional context.
        resources = pipeBase.ExecutionResources(num_cores=4, max_mem=5 * u.MB)
        butlerQC = pipeBase.QuantumContext(butler, quantum, resources=resources)
        self.assertEqual(butlerQC.resources.num_cores, 4)
        self.assertEqual(butlerQC.resources.max_mem, 5_000_000 * u.B)

        resources = pipeBase.ExecutionResources(max_mem=5)
        butlerQC = pipeBase.QuantumContext(butler, quantum, resources=resources)
        self.assertEqual(butlerQC.resources.num_cores, 1)
        self.assertEqual(butlerQC.resources.max_mem, 5 * u.B)

    def test_ExecutionResources(self):
        res = pipeBase.ExecutionResources()
        self.assertEqual(res.num_cores, 1)
        self.assertIsNone(res.max_mem)
        self.assertEqual(pickle.loads(pickle.dumps(res)), res)

        res = pipeBase.ExecutionResources(num_cores=4, max_mem=1 * u.MiB)
        self.assertEqual(res.num_cores, 4)
        self.assertEqual(res.max_mem.value, 1024 * 1024)
        self.assertEqual(pickle.loads(pickle.dumps(res)), res)

        res = pipeBase.ExecutionResources(max_mem=512)
        self.assertEqual(res.num_cores, 1)
        self.assertEqual(res.max_mem.value, 512)
        self.assertEqual(pickle.loads(pickle.dumps(res)), res)

        res = pipeBase.ExecutionResources(max_mem="")
        self.assertIsNone(res.max_mem)

        res = pipeBase.ExecutionResources(max_mem="32 KiB")
        self.assertEqual(res.num_cores, 1)
        self.assertEqual(res.max_mem.value, 32 * 1024)
        self.assertEqual(pickle.loads(pickle.dumps(res)), res)

        self.assertIs(res, copy.deepcopy(res))

        with self.assertRaises(AttributeError):
            res.num_cores = 4

        with self.assertRaises(u.UnitConversionError):
            pipeBase.ExecutionResources(max_mem=1 * u.m)
        with self.assertRaises(ValueError):
            pipeBase.ExecutionResources(num_cores=-32)
        with self.assertRaises(u.UnitConversionError):
            pipeBase.ExecutionResources(max_mem=1 * u.m)
        with self.assertRaises(u.UnitConversionError):
            pipeBase.ExecutionResources(max_mem=1, default_mem_units=u.m)
        with self.assertRaises(u.UnitConversionError):
            pipeBase.ExecutionResources(max_mem="32 Pa")


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    """Run file leak tests."""


def setup_module(module):
    """Configure pytest."""
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
