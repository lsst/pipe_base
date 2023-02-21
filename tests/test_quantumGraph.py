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

import os
import pickle
import random
import tempfile
import unittest
import uuid
from itertools import chain
from typing import Iterable

import lsst.pipe.base.connectionTypes as cT
import lsst.utils.tests
from lsst.daf.butler import Config, DataCoordinate, DatasetRef, DatasetType, DimensionUniverse, Quantum
from lsst.pex.config import Field
from lsst.pipe.base import (
    DatasetTypeName,
    PipelineTask,
    PipelineTaskConfig,
    PipelineTaskConnections,
    QuantumGraph,
    TaskDef,
)
from lsst.pipe.base.graph.quantumNode import QuantumNode
from lsst.utils.introspection import get_full_type_name

METADATA = {"a": [1, 2, 3]}


class Dummy1Connections(PipelineTaskConnections, dimensions=("A", "B")):
    initOutput = cT.InitOutput(name="Dummy1InitOutput", storageClass="ExposureF", doc="n/a")
    input = cT.Input(name="Dummy1Input", storageClass="ExposureF", doc="n/a", dimensions=("A", "B"))
    output = cT.Output(name="Dummy1Output", storageClass="ExposureF", doc="n/a", dimensions=("A", "B"))


class Dummy1Config(PipelineTaskConfig, pipelineConnections=Dummy1Connections):
    conf1 = Field(dtype=int, default=1, doc="dummy config")


class Dummy1PipelineTask(PipelineTask):
    ConfigClass = Dummy1Config


class Dummy2Connections(PipelineTaskConnections, dimensions=("A", "B")):
    initInput = cT.InitInput(name="Dummy1InitOutput", storageClass="ExposureF", doc="n/a")
    initOutput = cT.InitOutput(name="Dummy2InitOutput", storageClass="ExposureF", doc="n/a")
    input = cT.Input(name="Dummy1Output", storageClass="ExposureF", doc="n/a", dimensions=("A", "B"))
    output = cT.Output(name="Dummy2Output", storageClass="ExposureF", doc="n/a", dimensions=("A", "B"))


class Dummy2Config(PipelineTaskConfig, pipelineConnections=Dummy2Connections):
    conf1 = Field(dtype=int, default=1, doc="dummy config")


class Dummy2PipelineTask(PipelineTask):
    ConfigClass = Dummy2Config


class Dummy3Connections(PipelineTaskConnections, dimensions=("A", "B")):
    initInput = cT.InitInput(name="Dummy2InitOutput", storageClass="ExposureF", doc="n/a")
    initOutput = cT.InitOutput(name="Dummy3InitOutput", storageClass="ExposureF", doc="n/a")
    input = cT.Input(name="Dummy2Output", storageClass="ExposureF", doc="n/a", dimensions=("A", "B"))
    output = cT.Output(name="Dummy3Output", storageClass="ExposureF", doc="n/a", dimensions=("A", "B"))


class Dummy3Config(PipelineTaskConfig, pipelineConnections=Dummy3Connections):
    conf1 = Field(dtype=int, default=1, doc="dummy config")


class Dummy3PipelineTask(PipelineTask):
    ConfigClass = Dummy3Config


# Test if a Task that does not interact with the other Tasks works fine in
# the graph.
class Dummy4Connections(PipelineTaskConnections, dimensions=("A", "B")):
    input = cT.Input(name="Dummy4Input", storageClass="ExposureF", doc="n/a", dimensions=("A", "B"))
    output = cT.Output(name="Dummy4Output", storageClass="ExposureF", doc="n/a", dimensions=("A", "B"))


class Dummy4Config(PipelineTaskConfig, pipelineConnections=Dummy4Connections):
    conf1 = Field(dtype=int, default=1, doc="dummy config")


class Dummy4PipelineTask(PipelineTask):
    ConfigClass = Dummy4Config


class QuantumGraphTestCase(unittest.TestCase):
    """Tests the various functions of a quantum graph"""

    def setUp(self):
        self.config = Config(
            {
                "version": 1,
                "namespace": "pipe_base_test",
                "skypix": {
                    "common": "htm7",
                    "htm": {
                        "class": "lsst.sphgeom.HtmPixelization",
                        "max_level": 24,
                    },
                },
                "elements": {
                    "A": {
                        "keys": [
                            {
                                "name": "id",
                                "type": "int",
                            }
                        ],
                        "storage": {
                            "cls": "lsst.daf.butler.registry.dimensions.table.TableDimensionRecordStorage",
                        },
                    },
                    "B": {
                        "keys": [
                            {
                                "name": "id",
                                "type": "int",
                            }
                        ],
                        "storage": {
                            "cls": "lsst.daf.butler.registry.dimensions.table.TableDimensionRecordStorage",
                        },
                    },
                },
                "packers": {},
            }
        )
        universe = DimensionUniverse(config=self.config)

        def _makeDatasetType(connection):
            return DatasetType(
                connection.name,
                getattr(connection, "dimensions", ()),
                storageClass=connection.storageClass,
                universe=universe,
            )

        # need to make a mapping of TaskDef to set of quantum
        quantumMap = {}
        tasks = []
        initInputs = {}
        initOutputs = {}
        dataset_types = set()
        for task, label in (
            (Dummy1PipelineTask, "R"),
            (Dummy2PipelineTask, "S"),
            (Dummy3PipelineTask, "T"),
            (Dummy4PipelineTask, "U"),
        ):
            config = task.ConfigClass()
            taskDef = TaskDef(get_full_type_name(task), config, task, label)
            tasks.append(taskDef)
            quantumSet = set()
            connections = taskDef.connections
            if connections.initInputs:
                initInputDSType = _makeDatasetType(connections.initInput)
                initRefs = [DatasetRef(initInputDSType, DataCoordinate.makeEmpty(universe))]
                initInputs[taskDef] = initRefs
                dataset_types.add(initInputDSType)
            else:
                initRefs = None
            if connections.initOutputs:
                initOutputDSType = _makeDatasetType(connections.initOutput)
                initRefs = [DatasetRef(initOutputDSType, DataCoordinate.makeEmpty(universe))]
                initOutputs[taskDef] = initRefs
                dataset_types.add(initOutputDSType)
            inputDSType = _makeDatasetType(connections.input)
            dataset_types.add(inputDSType)
            outputDSType = _makeDatasetType(connections.output)
            dataset_types.add(outputDSType)
            for a, b in ((1, 2), (3, 4)):
                inputRefs = [
                    DatasetRef(inputDSType, DataCoordinate.standardize({"A": a, "B": b}, universe=universe))
                ]
                outputRefs = [
                    DatasetRef(outputDSType, DataCoordinate.standardize({"A": a, "B": b}, universe=universe))
                ]
                quantumSet.add(
                    Quantum(
                        taskName=task.__qualname__,
                        dataId=DataCoordinate.standardize({"A": a, "B": b}, universe=universe),
                        taskClass=task,
                        initInputs=initRefs,
                        inputs={inputDSType: inputRefs},
                        outputs={outputDSType: outputRefs},
                    )
                )
            quantumMap[taskDef] = quantumSet
        self.tasks = tasks
        self.quantumMap = quantumMap
        self.packagesDSType = DatasetType("packages", universe.empty, storageClass="Packages")
        dataset_types.add(self.packagesDSType)
        globalInitOutputs = [DatasetRef(self.packagesDSType, DataCoordinate.makeEmpty(universe))]
        self.qGraph = QuantumGraph(
            quantumMap,
            metadata=METADATA,
            universe=universe,
            initInputs=initInputs,
            initOutputs=initOutputs,
            globalInitOutputs=globalInitOutputs,
            registryDatasetTypes=dataset_types,
        )
        self.universe = universe
        self.num_dataset_types = len(dataset_types)

    def testTaskGraph(self):
        for taskDef in self.quantumMap.keys():
            self.assertIn(taskDef, self.qGraph.taskGraph)

    def testGraph(self):
        graphSet = {q.quantum for q in self.qGraph.graph}
        for quantum in chain.from_iterable(self.quantumMap.values()):
            self.assertIn(quantum, graphSet)

    def testGetQuantumNodeByNodeId(self):
        inputQuanta = tuple(self.qGraph.inputQuanta)
        node = self.qGraph.getQuantumNodeByNodeId(inputQuanta[0].nodeId)
        self.assertEqual(node, inputQuanta[0])
        wrongNode = uuid.uuid4()
        with self.assertRaises(KeyError):
            self.qGraph.getQuantumNodeByNodeId(wrongNode)

    def testPickle(self):
        stringify = pickle.dumps(self.qGraph)
        restore: QuantumGraph = pickle.loads(stringify)
        self.assertEqual(self.qGraph, restore)

    def testInputQuanta(self):
        inputs = {q.quantum for q in self.qGraph.inputQuanta}
        self.assertEqual(self.quantumMap[self.tasks[0]] | self.quantumMap[self.tasks[3]], inputs)

    def testOutputQuanta(self):
        outputs = {q.quantum for q in self.qGraph.outputQuanta}
        self.assertEqual(self.quantumMap[self.tasks[2]] | self.quantumMap[self.tasks[3]], outputs)

    def testLength(self):
        self.assertEqual(len(self.qGraph), 2 * len(self.tasks))

    def testGetQuantaForTask(self):
        for task in self.tasks:
            self.assertEqual(self.qGraph.getQuantaForTask(task), self.quantumMap[task])

    def testGetNumberOfQuantaForTask(self):
        for task in self.tasks:
            self.assertEqual(self.qGraph.getNumberOfQuantaForTask(task), len(self.quantumMap[task]))

    def testGetNodesForTask(self):
        for task in self.tasks:
            nodes: Iterable[QuantumNode] = self.qGraph.getNodesForTask(task)
            quanta_in_node = set(n.quantum for n in nodes)
            self.assertEqual(quanta_in_node, self.quantumMap[task])

    def testFindTasksWithInput(self):
        self.assertEqual(
            tuple(self.qGraph.findTasksWithInput(DatasetTypeName("Dummy1Output")))[0], self.tasks[1]
        )

    def testFindTasksWithOutput(self):
        self.assertEqual(self.qGraph.findTaskWithOutput(DatasetTypeName("Dummy1Output")), self.tasks[0])

    def testTaskWithDSType(self):
        self.assertEqual(
            set(self.qGraph.tasksWithDSType(DatasetTypeName("Dummy1Output"))), set(self.tasks[:2])
        )

    def testFindTaskDefByName(self):
        self.assertEqual(self.qGraph.findTaskDefByName(Dummy1PipelineTask.__qualname__)[0], self.tasks[0])

    def testFindTaskDefByLabel(self):
        self.assertEqual(self.qGraph.findTaskDefByLabel("R"), self.tasks[0])

    def testFindQuantaWIthDSType(self):
        self.assertEqual(
            self.qGraph.findQuantaWithDSType(DatasetTypeName("Dummy1Input")), self.quantumMap[self.tasks[0]]
        )

    def testAllDatasetTypes(self):
        allDatasetTypes = set(self.qGraph.allDatasetTypes)
        truth = set()
        for conClass in (Dummy1Connections, Dummy2Connections, Dummy3Connections, Dummy4Connections):
            for connection in conClass.allConnections.values():  # type: ignore
                if not isinstance(connection, cT.InitOutput):
                    truth.add(connection.name)
        self.assertEqual(allDatasetTypes, truth)

    def testSubset(self):
        allNodes = list(self.qGraph)
        firstNode = allNodes[0]
        subset = self.qGraph.subset(firstNode)
        self.assertEqual(len(subset), 1)
        subsetList = list(subset)
        self.assertEqual(firstNode.quantum, subsetList[0].quantum)
        self.assertEqual(self.qGraph._buildId, subset._buildId)
        self.assertEqual(len(subset.globalInitOutputRefs()), 1)
        # Depending on which task was first the list can contain different
        # number of datasets. The first task can be either Dummy1 or Dummy4.
        num_types = {"R": 4, "U": 3}
        self.assertEqual(len(subset.registryDatasetTypes()), num_types[firstNode.taskDef.label])

    def testSubsetToConnected(self):
        # False because there are two quantum chains for two distinct sets of
        # dimensions
        self.assertFalse(self.qGraph.isConnected)

        connectedGraphs = self.qGraph.subsetToConnected()
        self.assertEqual(len(connectedGraphs), 4)
        self.assertTrue(connectedGraphs[0].isConnected)
        self.assertTrue(connectedGraphs[1].isConnected)
        self.assertTrue(connectedGraphs[2].isConnected)
        self.assertTrue(connectedGraphs[3].isConnected)

        # Split out task[3] because it is expected to be on its own
        for cg in connectedGraphs:
            if self.tasks[3] in cg.taskGraph:
                self.assertEqual(len(cg), 1)
            else:
                self.assertEqual(len(cg), 3)

        self.assertNotEqual(connectedGraphs[0], connectedGraphs[1])

        count = 0
        for node in self.qGraph:
            if connectedGraphs[0].checkQuantumInGraph(node.quantum):
                count += 1
            if connectedGraphs[1].checkQuantumInGraph(node.quantum):
                count += 1
            if connectedGraphs[2].checkQuantumInGraph(node.quantum):
                count += 1
            if connectedGraphs[3].checkQuantumInGraph(node.quantum):
                count += 1
        self.assertEqual(len(self.qGraph), count)

        taskSets = {len(tg := s.taskGraph): set(tg) for s in connectedGraphs}
        for setLen, tskSet in taskSets.items():
            if setLen == 3:
                self.assertEqual(set(self.tasks[:-1]), tskSet)
            elif setLen == 1:
                self.assertEqual({self.tasks[-1]}, tskSet)
        for cg in connectedGraphs:
            if len(cg.taskGraph) == 1:
                continue
            allNodes = list(cg)
            node = cg.determineInputsToQuantumNode(allNodes[1])
            self.assertEqual(set([allNodes[0]]), node)
            node = cg.determineInputsToQuantumNode(allNodes[1])
            self.assertEqual(set([allNodes[0]]), node)

    def testDetermineOutputsOfQuantumNode(self):
        testNodes = self.qGraph.getNodesForTask(self.tasks[0])
        matchNodes = self.qGraph.getNodesForTask(self.tasks[1])
        connections = set()
        for node in testNodes:
            connections |= set(self.qGraph.determineOutputsOfQuantumNode(node))
        self.assertEqual(matchNodes, connections)

    def testDetermineConnectionsOfQuantum(self):
        testNodes = self.qGraph.getNodesForTask(self.tasks[1])
        matchNodes = self.qGraph.getNodesForTask(self.tasks[0]) | self.qGraph.getNodesForTask(self.tasks[2])
        # outputs contain nodes tested for because it is a complete graph
        matchNodes |= set(testNodes)
        connections = set()
        for node in testNodes:
            connections |= set(self.qGraph.determineConnectionsOfQuantumNode(node))
        self.assertEqual(matchNodes, connections)

    def testDetermineAnsestorsOfQuantumNode(self):
        testNodes = self.qGraph.getNodesForTask(self.tasks[1])
        matchNodes = self.qGraph.getNodesForTask(self.tasks[0])
        matchNodes |= set(testNodes)
        connections = set()
        for node in testNodes:
            connections |= set(self.qGraph.determineAncestorsOfQuantumNode(node))
        self.assertEqual(matchNodes, connections)

    def testFindCycle(self):
        self.assertFalse(self.qGraph.findCycle())

    def testSaveLoad(self):
        with tempfile.TemporaryFile(suffix=".qgraph") as tmpFile:
            self.qGraph.save(tmpFile)
            tmpFile.seek(0)
            restore = QuantumGraph.load(tmpFile, self.universe)
            self.assertEqual(self.qGraph, restore)
            # Load in just one node
            tmpFile.seek(0)
            nodeId = [n.nodeId for n in self.qGraph][0]
            restoreSub = QuantumGraph.load(tmpFile, self.universe, nodes=(nodeId,))
            self.assertEqual(len(restoreSub), 1)
            self.assertEqual(list(restoreSub)[0], restore.getQuantumNodeByNodeId(nodeId))
            self.assertEqual(len(restoreSub.globalInitOutputRefs()), 1)
            self.assertEqual(len(restoreSub.registryDatasetTypes()), self.num_dataset_types)
            # Check that InitInput and InitOutput refs are restored correctly.
            for taskDef in restore.iterTaskGraph():
                if taskDef.label in ("S", "T"):
                    refs = restore.initInputRefs(taskDef)
                    self.assertIsNotNone(refs)
                    self.assertGreater(len(refs), 0)
                if taskDef.label in ("R", "S", "T"):
                    refs = restore.initOutputRefs(taskDef)
                    self.assertIsNotNone(refs)
                    self.assertGreater(len(refs), 0)

            # Different universes.
            tmpFile.seek(0)
            different_config = self.config.copy()
            different_config["version"] = 1_000_000
            different_universe = DimensionUniverse(config=different_config)
            with self.assertLogs("lsst.daf.butler", "INFO"):
                QuantumGraph.load(tmpFile, different_universe)

            different_config["namespace"] = "incompatible"
            different_universe = DimensionUniverse(config=different_config)
            print("Trying with uni ", different_universe)
            tmpFile.seek(0)
            with self.assertRaises(RuntimeError) as cm:
                QuantumGraph.load(tmpFile, different_universe)
            self.assertIn("not compatible with", str(cm.exception))

    def testSaveLoadUri(self):
        uri = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".qgraph") as tmpFile:
                uri = tmpFile.name
                self.qGraph.saveUri(uri)
                restore = QuantumGraph.loadUri(uri)
                self.assertEqual(restore.metadata, METADATA)
                self.assertEqual(self.qGraph, restore)
                nodeNumberId = random.randint(0, len(self.qGraph) - 1)
                nodeNumber = [n.nodeId for n in self.qGraph][nodeNumberId]
                restoreSub = QuantumGraph.loadUri(
                    uri, self.universe, nodes=(nodeNumber,), graphID=self.qGraph._buildId
                )
                self.assertEqual(len(restoreSub), 1)
                self.assertEqual(list(restoreSub)[0], restore.getQuantumNodeByNodeId(nodeNumber))
                # verify that more than one node works
                nodeNumberId2 = random.randint(0, len(self.qGraph) - 1)
                # ensure it is a different node number
                while nodeNumberId2 == nodeNumberId:
                    nodeNumberId2 = random.randint(0, len(self.qGraph) - 1)
                nodeNumber2 = [n.nodeId for n in self.qGraph][nodeNumberId2]
                restoreSub = QuantumGraph.loadUri(uri, self.universe, nodes=(nodeNumber, nodeNumber2))
                self.assertEqual(len(restoreSub), 2)
                self.assertEqual(
                    set(restoreSub),
                    set(
                        (
                            restore.getQuantumNodeByNodeId(nodeNumber),
                            restore.getQuantumNodeByNodeId(nodeNumber2),
                        )
                    ),
                )
                # verify an error when requesting a non existant node number
                with self.assertRaises(ValueError):
                    QuantumGraph.loadUri(uri, self.universe, nodes=(99,))

                # verify a graphID that does not match will be an error
                with self.assertRaises(ValueError):
                    QuantumGraph.loadUri(uri, self.universe, graphID="NOTRIGHT")

        except Exception as e:
            raise e
        finally:
            if uri is not None:
                os.remove(uri)

        with self.assertRaises(TypeError):
            self.qGraph.saveUri("test.notgraph")

    def testSaveLoadNoRegistryDatasetTypes(self):
        """Test for reading quantum that is missing registry dataset types.

        This test depends on internals of QuantumGraph implementation, in
        particular that empty list of registry dataset types is not stored,
        which makes save file identical to the "old" format.
        """
        # Reset the list, this is safe as QuantumGraph itself does not use it.
        self.qGraph._registryDatasetTypes = []
        with tempfile.TemporaryFile(suffix=".qgraph") as tmpFile:
            self.qGraph.save(tmpFile)
            tmpFile.seek(0)
            restore = QuantumGraph.load(tmpFile, self.universe)
            self.assertEqual(self.qGraph, restore)
            self.assertEqual(restore.registryDatasetTypes(), [])

    def testContains(self):
        firstNode = next(iter(self.qGraph))
        self.assertIn(firstNode, self.qGraph)

    def testDimensionUniverseInSave(self):
        _, header = self.qGraph._buildSaveObject(returnHeader=True)
        # type ignore because buildSaveObject does not have method overload
        self.assertEqual(header["universe"], self.universe.dimensionConfig.toDict())  # type: ignore


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
