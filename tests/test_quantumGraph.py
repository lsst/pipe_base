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

from itertools import chain
import pickle
import tempfile
import unittest
from lsst.daf.butler import DimensionUniverse

from lsst.pipe.base import (QuantumGraph, TaskDef, PipelineTask, PipelineTaskConfig, PipelineTaskConnections,
                            DatasetTypeName, IncompatibleGraphError)
import lsst.pipe.base.connectionTypes as cT
from lsst.daf.butler import Quantum, DatasetRef, DataCoordinate, DatasetType, Config
from lsst.pex.config import Field
from lsst.pipe.base.graph.quantumNode import NodeId, BuildId
import lsst.utils.tests


class Dummy1Connections(PipelineTaskConnections, dimensions=("A", "B")):
    initOutput = cT.InitOutput(name="Dummy1InitOutput",
                               storageClass="ExposureF",
                               doc="n/a")
    input = cT.Input(name="Dummy1Input",
                     storageClass="ExposureF",
                     doc="n/a",
                     dimensions=("A", "B"))
    output = cT.Output(name="Dummy1Output",
                       storageClass="ExposureF",
                       doc="n/a",
                       dimensions=("A", "B"))


class Dummy1Config(PipelineTaskConfig, pipelineConnections=Dummy1Connections):
    conf1 = Field(dtype=int, default=1, doc="dummy config")


class Dummy1PipelineTask(PipelineTask):
    ConfigClass = Dummy1Config


class Dummy2Connections(PipelineTaskConnections, dimensions=("A", "B")):
    initInput = cT.InitInput(name="Dummy1InitOutput",
                             storageClass="ExposureF",
                             doc="n/a")
    initOutput = cT.InitOutput(name="Dummy2InitOutput",
                               storageClass="ExposureF",
                               doc="n/a")
    input = cT.Input(name="Dummy1Output",
                     storageClass="ExposureF",
                     doc="n/a",
                     dimensions=("A", "B"))
    output = cT.Output(name="Dummy2Output",
                       storageClass="ExposureF",
                       doc="n/a",
                       dimensions=("A", "B"))


class Dummy2Config(PipelineTaskConfig, pipelineConnections=Dummy2Connections):
    conf1 = Field(dtype=int, default=1, doc="dummy config")


class Dummy2PipelineTask(PipelineTask):
    ConfigClass = Dummy2Config


class Dummy3Connections(PipelineTaskConnections, dimensions=("A", "B")):
    initInput = cT.InitInput(name="Dummy2InitOutput",
                             storageClass="ExposureF",
                             doc="n/a")
    initOutput = cT.InitOutput(name="Dummy3InitOutput",
                               storageClass="ExposureF",
                               doc="n/a")
    input = cT.Input(name="Dummy2Output",
                     storageClass="ExposureF",
                     doc="n/a",
                     dimensions=("A", "B"))
    output = cT.Output(name="Dummy3Output",
                       storageClass="ExposureF",
                       doc="n/a",
                       dimensions=("A", "B"))


class Dummy3Config(PipelineTaskConfig, pipelineConnections=Dummy3Connections):
    conf1 = Field(dtype=int, default=1, doc="dummy config")


class Dummy3PipelineTask(PipelineTask):
    ConfigClass = Dummy3Config


class QuantumGraphTestCase(unittest.TestCase):
    """Tests the various functions of a quantum graph
    """
    def setUp(self):
        config = Config({"dimensions": {"version": 1,
                                        "skypix": {},
                                        "elements": {"A": {},
                                                     "B": {}}}})
        universe = DimensionUniverse(config=config)
        # need to make a mapping of TaskDef to set of quantum
        quantumMap = {}
        tasks = []
        for task, label in ((Dummy1PipelineTask, "R"), (Dummy2PipelineTask, "S"), (Dummy3PipelineTask, "T")):
            config = task.ConfigClass()
            taskDef = TaskDef(f"__main__.{task.__qualname__}", config, task, label)
            tasks.append(taskDef)
            quantumSet = set()
            connections = taskDef.connections
            for a, b in ((1, 2), (3, 4)):
                if connections.initInputs:
                    initInputDSType = DatasetType(connections.initInput.name,
                                                  tuple(),
                                                  storageClass=connections.initInput.storageClass,
                                                  universe=universe)
                    initRefs = [DatasetRef(initInputDSType,
                                           DataCoordinate.makeEmpty(universe))]
                else:
                    initRefs = None
                inputDSType = DatasetType(connections.input.name,
                                          connections.input.dimensions,
                                          storageClass=connections.input.storageClass,
                                          universe=universe,
                                          )
                inputRefs = [DatasetRef(inputDSType, DataCoordinate.standardize({"A": a, "B": b},
                                                                                universe=universe))]
                outputDSType = DatasetType(connections.output.name,
                                           connections.output.dimensions,
                                           storageClass=connections.output.storageClass,
                                           universe=universe,
                                           )
                outputRefs = [DatasetRef(outputDSType, DataCoordinate.standardize({"A": a, "B": b},
                                                                                  universe=universe))]
                quantumSet.add(
                    Quantum(taskName=task.__qualname__,
                            dataId=DataCoordinate.standardize({"A": a, "B": b}, universe=universe),
                            taskClass=task,
                            initInputs=initRefs,
                            inputs={inputDSType: inputRefs},
                            outputs={outputDSType: outputRefs}
                            )
                )
            quantumMap[taskDef] = quantumSet
        self.tasks = tasks
        self.quantumMap = quantumMap
        self.qGraph = QuantumGraph(quantumMap)
        self.universe = universe

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
        wrongNode = NodeId(15, BuildId("alternative build Id"))
        with self.assertRaises(IncompatibleGraphError):
            self.qGraph.getQuantumNodeByNodeId(wrongNode)

    def testPickle(self):
        stringify = pickle.dumps(self.qGraph)
        restore: QuantumGraph = pickle.loads(stringify)
        # This is a hack for the unit test since the qualified name will be
        # different as it will be __main__ here, but qualified to the
        # unittest module name when restored
        for saved, loaded in zip(self.qGraph._quanta.keys(),
                                 restore._quanta.keys()):
            saved.taskName = saved.taskName.split('.')[-1]
            loaded.taskName = loaded.taskName.split('.')[-1]
        self.assertEqual(self.qGraph, restore)

    def testInputQuanta(self):
        inputs = {q.quantum for q in self.qGraph.inputQuanta}
        self.assertEqual(self.quantumMap[self.tasks[0]], inputs)

    def testOutputtQuanta(self):
        outputs = {q.quantum for q in self.qGraph.outputQuanta}
        self.assertEqual(self.quantumMap[self.tasks[-1]], outputs)

    def testLength(self):
        self.assertEqual(len(self.qGraph), 6)

    def testGetQuantaForTask(self):
        for task in self.tasks:
            self.assertEqual(self.qGraph.getQuantaForTask(task), self.quantumMap[task])

    def testFindTasksWithInput(self):
        self.assertEqual(tuple(self.qGraph.findTasksWithInput(DatasetTypeName("Dummy1Output")))[0],
                         self.tasks[1])

    def testFindTasksWithOutput(self):
        self.assertEqual(self.qGraph.findTaskWithOutput(DatasetTypeName("Dummy1Output")), self.tasks[0])

    def testTaskWithDSType(self):
        self.assertEqual(set(self.qGraph.tasksWithDSType(DatasetTypeName("Dummy1Output"))),
                         set(self.tasks[:2]))

    def testFindTaskDefByName(self):
        self.assertEqual(self.qGraph.findTaskDefByName(Dummy1PipelineTask.__qualname__)[0],
                         self.tasks[0])

    def testFindTaskDefByLabel(self):
        self.assertEqual(self.qGraph.findTaskDefByLabel("R"),
                         self.tasks[0])

    def testFindQuantaWIthDSType(self):
        self.assertEqual(self.qGraph.findQuantaWithDSType(DatasetTypeName("Dummy1Input")),
                         self.quantumMap[self.tasks[0]])

    def testAllDatasetTypes(self):
        allDatasetTypes = set(self.qGraph.allDatasetTypes)
        truth = set()
        for conClass in (Dummy1Connections, Dummy2Connections, Dummy3Connections):
            for connection in conClass.allConnections.values():  # type: ignore
                truth.add(connection.name)
        self.assertEqual(allDatasetTypes, truth)

    def testSubset(self):
        allNodes = list(self.qGraph)
        subset = self.qGraph.subset(allNodes[0])
        self.assertEqual(len(subset), 1)
        subsetList = list(subset)
        self.assertEqual(allNodes[0].quantum, subsetList[0].quantum)
        self.assertEqual(self.qGraph._buildId, subset._buildId)

    def testIsConnected(self):
        # False because there are two quantum chains for two distinct sets of
        # dimensions
        self.assertFalse(self.qGraph.isConnected)
        # make a broken subset
        allNodes = list(self.qGraph)
        subset = self.qGraph.subset((allNodes[0], allNodes[1]))
        # True because we subset to only one chain of graphs
        self.assertTrue(subset.isConnected)

    def testSubsetToConnected(self):
        connectedGraphs = self.qGraph.subsetToConnected()
        self.assertEqual(len(connectedGraphs), 2)
        self.assertTrue(connectedGraphs[0].isConnected)
        self.assertTrue(connectedGraphs[1].isConnected)

        self.assertEqual(len(connectedGraphs[0]), 3)
        self.assertEqual(len(connectedGraphs[1]), 3)

        self.assertNotEqual(connectedGraphs[0], connectedGraphs[1])

        count = 0
        for node in self.qGraph:
            if connectedGraphs[0].checkQuantumInGraph(node.quantum):
                count += 1
            if connectedGraphs[1].checkQuantumInGraph(node.quantum):
                count += 1
        self.assertEqual(len(self.qGraph), count)

        self.assertEqual(self.tasks, list(connectedGraphs[0].taskGraph))
        self.assertEqual(self.tasks, list(connectedGraphs[1].taskGraph))
        allNodes = list(self.qGraph)
        node = self.qGraph.determineInputsToQuantumNode(allNodes[1])
        self.assertEqual(set([allNodes[0]]), node)
        node = self.qGraph.determineInputsToQuantumNode(allNodes[1])
        self.assertEqual(set([allNodes[0]]), node)

    def testDetermineOutputsOfQuantumNode(self):
        allNodes = list(self.qGraph)
        node = next(iter(self.qGraph.determineOutputsOfQuantumNode(allNodes[1])))
        self.assertEqual(allNodes[2], node)

    def testDetermineConnectionsOfQuantum(self):
        allNodes = list(self.qGraph)
        connections = self.qGraph.determineConnectionsOfQuantumNode(allNodes[1])
        self.assertEqual(list(connections), list(self.qGraph.subset(allNodes[:3])))

    def testDetermineAnsestorsOfQuantumNode(self):
        allNodes = list(self.qGraph)
        ansestors = self.qGraph.determineAncestorsOfQuantumNode(allNodes[2])
        self.assertEqual(list(ansestors), list(self.qGraph.subset(allNodes[:3])))

    def testFindCycle(self):
        self.assertFalse(self.qGraph.findCycle())

    def testSaveLoad(self):
        with tempfile.TemporaryFile() as tmpFile:
            self.qGraph.save(tmpFile)
            tmpFile.seek(0)
            restore = QuantumGraph.load(tmpFile, self.universe)
            # This is a hack for the unit test since the qualified name will be
            # different as it will be __main__ here, but qualified to the
            # unittest module name when restored
            for saved, loaded in zip(self.qGraph._quanta.keys(),
                                     restore._quanta.keys()):
                saved.taskName = saved.taskName.split('.')[-1]
                loaded.taskName = loaded.taskName.split('.')[-1]
            self.assertEqual(self.qGraph, restore)

    def testContains(self):
        firstNode = next(iter(self.qGraph))
        self.assertIn(firstNode, self.qGraph)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
