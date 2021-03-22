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
from __future__ import annotations
import warnings

__all__ = ("QuantumGraph", "IncompatibleGraphError")

from collections import defaultdict, deque

from itertools import chain, count
import io
import networkx as nx
from networkx.drawing.nx_agraph import write_dot
import os
import pickle
import lzma
import copy
import struct
import time
from typing import (DefaultDict, Dict, FrozenSet, Iterable, List, Mapping, Set, Generator, Optional, Tuple,
                    Union, TypeVar)

from ..connections import iterConnections
from ..pipeline import TaskDef
from lsst.daf.butler import Quantum, DatasetRef, ButlerURI, DimensionUniverse

from ._implDetails import _DatasetTracker, DatasetTypeName
from .quantumNode import QuantumNode, NodeId, BuildId
from ._loadHelpers import LoadHelper


_T = TypeVar("_T", bound="QuantumGraph")

# modify this constant any time the on disk representation of the save file
# changes, and update the load helpers to behave properly for each version.
SAVE_VERSION = 1

# String used to describe the format for the preamble bytes in a file save
# This marks a Big endian encoded format with an unsigned short, an unsigned
# long long, and an unsigned long long in the byte stream
STRUCT_FMT_STRING = '>HQQ'


# magic bytes that help determine this is a graph save
MAGIC_BYTES = b"qgraph4\xf6\xe8\xa9"


class IncompatibleGraphError(Exception):
    """Exception class to indicate that a lookup by NodeId is impossible due
    to incompatibilities
    """
    pass


class QuantumGraph:
    """QuantumGraph is a directed acyclic graph of `QuantumNode` objects

    This data structure represents a concrete workflow generated from a
    `Pipeline`.

    Parameters
    ----------
    quanta : Mapping of `TaskDef` to sets of `Quantum`
        This maps tasks (and their configs) to the sets of data they are to
        process.
    """
    def __init__(self, quanta: Mapping[TaskDef, Set[Quantum]]):
        self._buildGraphs(quanta)

    def _buildGraphs(self,
                     quanta: Mapping[TaskDef, Set[Quantum]],
                     *,
                     _quantumToNodeId: Optional[Mapping[Quantum, NodeId]] = None,
                     _buildId: Optional[BuildId] = None):
        """Builds the graph that is used to store the relation between tasks,
        and the graph that holds the relations between quanta
        """
        self._quanta = quanta
        self._buildId = _buildId if _buildId is not None else BuildId(f"{time.time()}-{os.getpid()}")
        # Data structures used to identify relations between components;
        # DatasetTypeName -> TaskDef for task,
        # and DatasetRef -> QuantumNode for the quanta
        self._datasetDict = _DatasetTracker[DatasetTypeName, TaskDef]()
        self._datasetRefDict = _DatasetTracker[DatasetRef, QuantumNode]()

        nodeNumberGenerator = count()
        self._nodeIdMap: Dict[NodeId, QuantumNode] = {}
        self._taskToQuantumNode: DefaultDict[TaskDef, Set[QuantumNode]] = defaultdict(set)
        self._count = 0
        for taskDef, quantumSet in self._quanta.items():
            connections = taskDef.connections

            # For each type of connection in the task, add a key to the
            # `_DatasetTracker` for the connections name, with a value of
            # the TaskDef in the appropriate field
            for inpt in iterConnections(connections, ("inputs", "prerequisiteInputs", "initInputs")):
                self._datasetDict.addInput(DatasetTypeName(inpt.name), taskDef)

            for output in iterConnections(connections, ("outputs", "initOutputs")):
                self._datasetDict.addOutput(DatasetTypeName(output.name), taskDef)

            # For each `Quantum` in the set of all `Quantum` for this task,
            # add a key to the `_DatasetTracker` that is a `DatasetRef` for one
            # of the individual datasets inside the `Quantum`, with a value of
            # a newly created QuantumNode to the appropriate input/output
            # field.
            self._count += len(quantumSet)
            for quantum in quantumSet:
                if _quantumToNodeId:
                    nodeId = _quantumToNodeId.get(quantum)
                    if nodeId is None:
                        raise ValueError("If _quantuMToNodeNumber is not None, all quanta must have an "
                                         "associated value in the mapping")
                else:
                    nodeId = NodeId(next(nodeNumberGenerator), self._buildId)

                inits = quantum.initInputs.values()
                inputs = quantum.inputs.values()
                value = QuantumNode(quantum, taskDef, nodeId)
                self._taskToQuantumNode[taskDef].add(value)
                self._nodeIdMap[nodeId] = value

                for dsRef in chain(inits, inputs):
                    # unfortunately, `Quantum` allows inits to be individual
                    # `DatasetRef`s or an Iterable of such, so there must
                    # be an instance check here
                    if isinstance(dsRef, Iterable):
                        for sub in dsRef:
                            self._datasetRefDict.addInput(sub, value)
                    else:
                        self._datasetRefDict.addInput(dsRef, value)
                for dsRef in chain.from_iterable(quantum.outputs.values()):
                    self._datasetRefDict.addOutput(dsRef, value)

        # Graph of task relations, used in various methods
        self._taskGraph = self._datasetDict.makeNetworkXGraph()

        # Graph of quanta relations
        self._connectedQuanta = self._datasetRefDict.makeNetworkXGraph()

    @property
    def taskGraph(self) -> nx.DiGraph:
        """Return a graph representing the relations between the tasks inside
        the quantum graph.

        Returns
        -------
        taskGraph : `networkx.Digraph`
            Internal datastructure that holds relations of `TaskDef` objects
        """
        return self._taskGraph

    @property
    def graph(self) -> nx.DiGraph:
        """Return a graph representing the relations between all the
        `QuantumNode` objects. Largely it should be preferred to iterate
        over, and use methods of this class, but sometimes direct access to
        the networkx object may be helpful

        Returns
        -------
        graph : `networkx.Digraph`
            Internal datastructure that holds relations of `QuantumNode`
            objects
        """
        return self._connectedQuanta

    @property
    def inputQuanta(self) -> Iterable[QuantumNode]:
        """Make a `list` of all `QuantumNode` objects that are 'input' nodes
        to the graph, meaning those nodes to not depend on any other nodes in
        the graph.

        Returns
        -------
        inputNodes : iterable of `QuantumNode`
            A list of nodes that are inputs to the graph
        """
        return (q for q, n in self._connectedQuanta.in_degree if n == 0)

    @property
    def outputQuanta(self) -> Iterable[QuantumNode]:
        """Make a `list` of all `QuantumNode` objects that are 'output' nodes
        to the graph, meaning those nodes have no nodes that depend them in
        the graph.

        Returns
        -------
        outputNodes : iterable of `QuantumNode`
            A list of nodes that are outputs of the graph
        """
        return [q for q, n in self._connectedQuanta.out_degree if n == 0]

    @property
    def allDatasetTypes(self) -> Tuple[DatasetTypeName, ...]:
        """Return all the `DatasetTypeName` objects that are contained inside
        the graph.

        Returns
        -------
        tuple of `DatasetTypeName`
            All the data set type names that are present in the graph
        """
        return tuple(self._datasetDict.keys())

    @property
    def isConnected(self) -> bool:
        """Return True if all of the nodes in the graph are connected, ignores
        directionality of connections.
        """
        return nx.is_weakly_connected(self._connectedQuanta)

    def getQuantumNodeByNodeId(self, nodeId: NodeId) -> QuantumNode:
        """Lookup a `QuantumNode` from an id associated with the node.

        Parameters
        ----------
        nodeId : `NodeId`
            The number associated with a node

        Returns
        -------
        node : `QuantumNode`
            The node corresponding with input number

        Raises
        ------
        IndexError
            Raised if the requested nodeId is not in the graph.
        IncompatibleGraphError
            Raised if the nodeId was built with a different graph than is not
            this instance (or a graph instance that produced this instance
            through and operation such as subset)
        """
        if nodeId.buildId != self._buildId:
            raise IncompatibleGraphError("This node was built from a different, incompatible, graph instance")
        return self._nodeIdMap[nodeId]

    def getQuantaForTask(self, taskDef: TaskDef) -> FrozenSet[Quantum]:
        """Return all the `Quantum` associated with a `TaskDef`.

        Parameters
        ----------
        taskDef : `TaskDef`
            The `TaskDef` for which `Quantum` are to be queried

        Returns
        -------
        frozenset of `Quantum`
            The `set` of `Quantum` that is associated with the specified
            `TaskDef`.
        """
        return frozenset(self._quanta[taskDef])

    def getNodesForTask(self, taskDef: TaskDef) -> FrozenSet[QuantumNode]:
        """Return all the `QuantumNodes` associated with a `TaskDef`.

        Parameters
        ----------
        taskDef : `TaskDef`
            The `TaskDef` for which `Quantum` are to be queried

        Returns
        -------
        frozenset of `QuantumNodes`
            The `frozenset` of `QuantumNodes` that is associated with the
            specified `TaskDef`.
        """
        return frozenset(self._taskToQuantumNode[taskDef])

    def findTasksWithInput(self, datasetTypeName: DatasetTypeName) -> Iterable[TaskDef]:
        """Find all tasks that have the specified dataset type name as an
        input.

        Parameters
        ----------
        datasetTypeName : `str`
            A string representing the name of a dataset type to be queried,
            can also accept a `DatasetTypeName` which is a `NewType` of str for
            type safety in static type checking.

        Returns
        -------
        tasks : iterable of `TaskDef`
            `TaskDef` objects that have the specified `DatasetTypeName` as an
            input, list will be empty if no tasks use specified
            `DatasetTypeName` as an input.

        Raises
        ------
        KeyError
            Raised if the `DatasetTypeName` is not part of the `QuantumGraph`
        """
        return (c for c in self._datasetDict.getInputs(datasetTypeName))

    def findTaskWithOutput(self, datasetTypeName: DatasetTypeName) -> Optional[TaskDef]:
        """Find all tasks that have the specified dataset type name as an
        output.

        Parameters
        ----------
        datasetTypeName : `str`
            A string representing the name of a dataset type to be queried,
            can also accept a `DatasetTypeName` which is a `NewType` of str for
            type safety in static type checking.

        Returns
        -------
        `TaskDef` or `None`
            `TaskDef` that outputs `DatasetTypeName` as an output or None if
            none of the tasks produce this `DatasetTypeName`.

        Raises
        ------
        KeyError
            Raised if the `DatasetTypeName` is not part of the `QuantumGraph`
        """
        return self._datasetDict.getOutput(datasetTypeName)

    def tasksWithDSType(self, datasetTypeName: DatasetTypeName) -> Iterable[TaskDef]:
        """Find all tasks that are associated with the specified dataset type
        name.

        Parameters
        ----------
        datasetTypeName : `str`
            A string representing the name of a dataset type to be queried,
            can also accept a `DatasetTypeName` which is a `NewType` of str for
            type safety in static type checking.

        Returns
        -------
        result : iterable of `TaskDef`
            `TaskDef` objects that are associated with the specified
            `DatasetTypeName`

        Raises
        ------
        KeyError
            Raised if the `DatasetTypeName` is not part of the `QuantumGraph`
        """
        results = self.findTasksWithInput(datasetTypeName)
        output = self.findTaskWithOutput(datasetTypeName)
        if output is not None:
            results = chain(results, (output,))
        return results

    def findTaskDefByName(self, taskName: str) -> List[TaskDef]:
        """Determine which `TaskDef` objects in this graph are associated
        with a `str` representing a task name (looks at the taskName property
        of `TaskDef` objects).

        Returns a list of `TaskDef` objects as a `PipelineTask` may appear
        multiple times in a graph with different labels.

        Parameters
        ----------
        taskName : str
            Name of a task to search for

        Returns
        -------
        result : list of `TaskDef`
            List of the `TaskDef` objects that have the name specified.
            Multiple values are returned in the case that a task is used
            multiple times with different labels.
        """
        results = []
        for task in self._quanta.keys():
            split = task.taskName.split('.')
            if split[-1] == taskName:
                results.append(task)
        return results

    def findTaskDefByLabel(self, label: str) -> Optional[TaskDef]:
        """Determine which `TaskDef` objects in this graph are associated
        with a `str` representing a tasks label.

        Parameters
        ----------
        taskName : str
            Name of a task to search for

        Returns
        -------
        result : `TaskDef`
            `TaskDef` objects that has the specified label.
        """
        for task in self._quanta.keys():
            if label == task.label:
                return task
        return None

    def findQuantaWithDSType(self, datasetTypeName: DatasetTypeName) -> Set[Quantum]:
        """Return all the `Quantum` that contain a specified `DatasetTypeName`.

        Parameters
        ----------
        datasetTypeName : `str`
            The name of the dataset type to search for as a string,
            can also accept a `DatasetTypeName` which is a `NewType` of str for
            type safety in static type checking.

        Returns
        -------
        result : `set` of `QuantumNode` objects
            A `set` of `QuantumNode`s that contain specified `DatasetTypeName`

        Raises
        ------
        KeyError
            Raised if the `DatasetTypeName` is not part of the `QuantumGraph`

        """
        tasks = self._datasetDict.getAll(datasetTypeName)
        result: Set[Quantum] = set()
        result = result.union(*(self._quanta[task] for task in tasks))
        return result

    def checkQuantumInGraph(self, quantum: Quantum) -> bool:
        """Check if specified quantum appears in the graph as part of a node.

        Parameters
        ----------
        quantum : `Quantum`
            The quantum to search for

        Returns
        -------
        `bool`
            The result of searching for the quantum
        """
        for qset in self._quanta.values():
            if quantum in qset:
                return True
        return False

    def writeDotGraph(self, output: Union[str, io.BufferedIOBase]):
        """Write out the graph as a dot graph.

        Parameters
        ----------
        output : str or `io.BufferedIOBase`
            Either a filesystem path to write to, or a file handle object
        """
        write_dot(self._connectedQuanta, output)

    def subset(self: _T, nodes: Union[QuantumNode, Iterable[QuantumNode]]) -> _T:
        """Create a new graph object that contains the subset of the nodes
        specified as input. Node number is preserved.

        Parameters
        ----------
        nodes : `QuantumNode` or iterable of `QuantumNode`

        Returns
        -------
        graph : instance of graph type
            An instance of the type from which the subset was created
        """
        if not isinstance(nodes, Iterable):
            nodes = (nodes, )
        quantumSubgraph = self._connectedQuanta.subgraph(nodes).nodes
        quantumMap = defaultdict(set)

        node: QuantumNode
        for node in quantumSubgraph:
            quantumMap[node.taskDef].add(node.quantum)
        # Create an empty graph, and then populate it with custom mapping
        newInst = type(self)({})
        newInst._buildGraphs(quantumMap, _quantumToNodeId={n.quantum: n.nodeId for n in nodes},
                             _buildId=self._buildId)
        return newInst

    def subsetToConnected(self: _T) -> Tuple[_T, ...]:
        """Generate a list of subgraphs where each is connected.

        Returns
        -------
        result : list of `QuantumGraph`
            A list of graphs that are each connected
        """
        return tuple(self.subset(connectedSet)
                     for connectedSet in nx.weakly_connected_components(self._connectedQuanta))

    def determineInputsToQuantumNode(self, node: QuantumNode) -> Set[QuantumNode]:
        """Return a set of `QuantumNode` that are direct inputs to a specified
        node.

        Parameters
        ----------
        node : `QuantumNode`
            The node of the graph for which inputs are to be determined

        Returns
        -------
        set of `QuantumNode`
            All the nodes that are direct inputs to specified node
        """
        return set(pred for pred in self._connectedQuanta.predecessors(node))

    def determineOutputsOfQuantumNode(self, node: QuantumNode) -> Set[QuantumNode]:
        """Return a set of `QuantumNode` that are direct outputs of a specified
        node.

        Parameters
        ----------
        node : `QuantumNode`
            The node of the graph for which outputs are to be determined

        Returns
        -------
        set of `QuantumNode`
            All the nodes that are direct outputs to specified node
        """
        return set(succ for succ in self._connectedQuanta.successors(node))

    def determineConnectionsOfQuantumNode(self: _T, node: QuantumNode) -> _T:
        """Return a graph of `QuantumNode` that are direct inputs and outputs
        of a specified node.

        Parameters
        ----------
        node : `QuantumNode`
            The node of the graph for which connected nodes are to be
            determined.

        Returns
        -------
        graph : graph of `QuantumNode`
            All the nodes that are directly connected to specified node
        """
        nodes = self.determineInputsToQuantumNode(node).union(self.determineOutputsOfQuantumNode(node))
        nodes.add(node)
        return self.subset(nodes)

    def determineAncestorsOfQuantumNode(self: _T, node: QuantumNode) -> _T:
        """Return a graph of the specified node and all the ancestor nodes
        directly reachable by walking edges.

        Parameters
        ----------
        node : `QuantumNode`
            The node for which all ansestors are to be determined

        Returns
        -------
        graph of `QuantumNode`
            Graph of node and all of its ansestors
        """
        predecessorNodes = nx.ancestors(self._connectedQuanta, node)
        predecessorNodes.add(node)
        return self.subset(predecessorNodes)

    def findCycle(self) -> List[Tuple[QuantumNode, QuantumNode]]:
        """Check a graph for the presense of cycles and returns the edges of
        any cycles found, or an empty list if there is no cycle.

        Returns
        -------
        result : list of tuple of `QuantumNode`, `QuantumNode`
            A list of any graph edges that form a cycle, or an empty list if
            there is no cycle. Empty list to so support if graph.find_cycle()
            syntax as an empty list is falsy.
        """
        try:
            return nx.find_cycle(self._connectedQuanta)
        except nx.NetworkXNoCycle:
            return []

    def saveUri(self, uri):
        """Save `QuantumGraph` to the specified URI.

        Parameters
        ----------
        uri : `ButlerURI` or `str`
            URI to where the graph should be saved.
        """
        buffer = self._buildSaveObject()
        butlerUri = ButlerURI(uri)
        if butlerUri.getExtension() not in (".qgraph"):
            raise TypeError(f"Can currently only save a graph in qgraph format not {uri}")
        butlerUri.write(buffer)  # type: ignore  # Ignore because bytearray is safe to use in place of bytes

    @classmethod
    def loadUri(cls, uri: Union[ButlerURI, str], universe: DimensionUniverse,
                nodes: Optional[Iterable[int]] = None,
                graphID: Optional[BuildId] = None
                ) -> QuantumGraph:
        """Read `QuantumGraph` from a URI.

        Parameters
        ----------
        uri : `ButlerURI` or `str`
            URI from where to load the graph.
        universe: `~lsst.daf.butler.DimensionUniverse`
            DimensionUniverse instance, not used by the method itself but
            needed to ensure that registry data structures are initialized.
        nodes: iterable of `int` or None
            Numbers that correspond to nodes in the graph. If specified, only
            these nodes will be loaded. Defaults to None, in which case all
            nodes will be loaded.
        graphID : `str` or `None`
            If specified this ID is verified against the loaded graph prior to
            loading any Nodes. This defaults to None in which case no
            validation is done.

        Returns
        -------
        graph : `QuantumGraph`
            Resulting QuantumGraph instance.

        Raises
        ------
        TypeError
            Raised if pickle contains instance of a type other than
            QuantumGraph.
        ValueError
            Raised if one or more of the nodes requested is not in the
            `QuantumGraph` or if graphID parameter does not match the graph
            being loaded or if the supplied uri does not point at a valid
            `QuantumGraph` save file.


        Notes
        -----
        Reading Quanta from pickle requires existence of singleton
        DimensionUniverse which is usually instantiated during Registry
        initialization. To make sure that DimensionUniverse exists this method
        accepts dummy DimensionUniverse argument.
        """
        uri = ButlerURI(uri)
        # With ButlerURI we have the choice of always using a local file
        # or reading in the bytes directly. Reading in bytes can be more
        # efficient for reasonably-sized pickle files when the resource
        # is remote. For now use the local file variant. For a local file
        # as_local() does nothing.

        if uri.getExtension() in (".pickle", ".pkl"):
            with uri.as_local() as local, open(local.ospath, "rb") as fd:
                warnings.warn("Pickle graphs are deprecated, please re-save your graph with the save method")
                qgraph = pickle.load(fd)
        elif uri.getExtension() in ('.qgraph'):
            with LoadHelper(uri) as loader:
                qgraph = loader.load(nodes, graphID)
        else:
            raise ValueError("Only know how to handle files saved as `pickle`, `pkl`, or `qgraph`")
        if not isinstance(qgraph, QuantumGraph):
            raise TypeError(f"QuantumGraph save file contains unexpected object type: {type(qgraph)}")
        return qgraph

    def save(self, file: io.IO[bytes]):
        """Save QuantumGraph to a file.

        Presently we store QuantumGraph in pickle format, this could
        potentially change in the future if better format is found.

        Parameters
        ----------
        file : `io.BufferedIOBase`
            File to write pickle data open in binary mode.
        """
        buffer = self._buildSaveObject()
        file.write(buffer)  # type: ignore # Ignore because bytearray is safe to use in place of bytes

    def _buildSaveObject(self) -> bytearray:
        # make some containers
        pickleData = deque()
        nodeMap = {}
        taskDefMap = {}
        protocol = 3

        # counter for the number of bytes processed thus far
        count = 0
        # serialize out the task Defs recording the start and end bytes of each
        # taskDef
        for taskDef in self.taskGraph:
            # compressing has very little impact on saving or load time, but
            # a large impact on on disk size, so it is worth doing
            dump = lzma.compress(pickle.dumps(taskDef, protocol=protocol))
            taskDefMap[taskDef.label] = (count, count+len(dump))
            count += len(dump)
            pickleData.append(dump)

        # Store the QauntumGraph BuildId along side the TaskDefs for
        # convenance. This will allow validating BuildIds at load time, prior
        # to loading any QuantumNodes. Name chosen for unlikely conflicts with
        # labels as this is python standard for private.
        taskDefMap['__GraphBuildID'] = self.graphID

        # serialize the nodes, recording the start and end bytes of each node
        for node in self:
            node = copy.copy(node)
            taskDef = node.taskDef
            # Explicitly overload the "frozen-ness" of nodes to normalized out
            # the taskDef, this saves a lot of space and load time. The label
            # will be used to retrive the taskDef from the taskDefMap upon load
            #
            # This strategy was chosen instead of creating a new class that
            # looked just like a QuantumNode but containing a label in place of
            # a TaskDef because it would be needlessly slow to construct a
            # bunch of new object to immediately serialize them and destroy the
            # object. This seems like an acceptable use of Python's dynamic
            # nature in a controlled way for optimization and simplicity.
            object.__setattr__(node, 'taskDef', taskDef.label)
            # compressing has very little impact on saving or load time, but
            # a large impact on on disk size, so it is worth doing
            dump = lzma.compress(pickle.dumps(node, protocol=protocol))
            pickleData.append(dump)
            nodeMap[node.nodeId.number] = (count, count+len(dump))
            count += len(dump)

        # pickle the taskDef byte map
        taskDef_pickle = pickle.dumps(taskDefMap, protocol=protocol)

        # pickle the node byte map
        map_pickle = pickle.dumps(nodeMap, protocol=protocol)

        # record the sizes as 2 unsigned long long numbers for a total of 16
        # bytes
        map_lengths = struct.pack(STRUCT_FMT_STRING, SAVE_VERSION, len(taskDef_pickle), len(map_pickle))

        # write each component of the save out in a deterministic order
        # buffer = io.BytesIO()
        # buffer.write(map_lengths)
        # buffer.write(taskDef_pickle)
        # buffer.write(map_pickle)
        buffer = bytearray()
        buffer.extend(MAGIC_BYTES)
        buffer.extend(map_lengths)
        buffer.extend(taskDef_pickle)
        buffer.extend(map_pickle)
        # Iterate over the length of pickleData, and for each element pop the
        # leftmost element off the deque and write it out. This is to save
        # memory, as the memory is added to the buffer object, it is removed
        # from from the container.
        #
        # Only this section needs to worry about memory pressue because
        # everything else written to the buffer prior to this pickle data is
        # only on the order of kilobytes to low numbers of megabytes.
        while pickleData:
            buffer.extend(pickleData.popleft())
        return buffer

    @classmethod
    def load(cls, file: io.IO[bytes], universe: DimensionUniverse,
             nodes: Optional[Iterable[int]] = None,
             graphID: Optional[BuildId] = None
             ) -> QuantumGraph:
        """Read QuantumGraph from a file that was made by `save`.

        Parameters
        ----------
        file : `io.IO` of bytes
            File with pickle data open in binary mode.
        universe: `~lsst.daf.butler.DimensionUniverse`
            DimensionUniverse instance, not used by the method itself but
            needed to ensure that registry data structures are initialized.
        nodes: iterable of `int` or None
            Numbers that correspond to nodes in the graph. If specified, only
            these nodes will be loaded. Defaults to None, in which case all
            nodes will be loaded.
        graphID : `str` or `None`
            If specified this ID is verified against the loaded graph prior to
            loading any Nodes. This defaults to None in which case no
            validation is done.

        Returns
        -------
        graph : `QuantumGraph`
            Resulting QuantumGraph instance.

        Raises
        ------
        TypeError
            Raised if pickle contains instance of a type other than
            QuantumGraph.
        ValueError
            Raised if one or more of the nodes requested is not in the
            `QuantumGraph` or if graphID parameter does not match the graph
            being loaded or if the supplied uri does not point at a valid
            `QuantumGraph` save file.

        Notes
        -----
        Reading Quanta from pickle requires existence of singleton
        DimensionUniverse which is usually instantiated during Registry
        initialization. To make sure that DimensionUniverse exists this method
        accepts dummy DimensionUniverse argument.
        """
        # Try to see if the file handle contains pickle data, this will be
        # removed in the future
        try:
            qgraph = pickle.load(file)
            warnings.warn("Pickle graphs are deprecated, please re-save your graph with the save method")
        except pickle.UnpicklingError:
            with LoadHelper(file) as loader:  # type: ignore # needed because we don't have Protocols yet
                qgraph = loader.load(nodes, graphID)
        if not isinstance(qgraph, QuantumGraph):
            raise TypeError(f"QuantumGraph pickle file has contains unexpected object type: {type(qgraph)}")
        return qgraph

    def iterTaskGraph(self) -> Generator[TaskDef, None, None]:
        """Iterate over the `taskGraph` attribute in topological order

        Yields
        ------
        taskDef : `TaskDef`
            `TaskDef` objects in topological order
        """
        yield from nx.topological_sort(self.taskGraph)

    @property
    def graphID(self):
        """Returns the ID generated by the graph at construction time
        """
        return self._buildId

    def __iter__(self) -> Generator[QuantumNode, None, None]:
        yield from nx.topological_sort(self._connectedQuanta)

    def __len__(self) -> int:
        return self._count

    def __contains__(self, node: QuantumNode) -> bool:
        return self._connectedQuanta.has_node(node)

    def __getstate__(self) -> dict:
        """Stores a compact form of the graph as a list of graph nodes, and a
        tuple of task labels and task configs. The full graph can be
        reconstructed with this information, and it preseves the ordering of
        the graph ndoes.
        """
        return {"nodesList": list(self)}

    def __setstate__(self, state: dict):
        """Reconstructs the state of the graph from the information persisted
        in getstate.
        """
        quanta: DefaultDict[TaskDef, Set[Quantum]] = defaultdict(set)
        quantumToNodeId: Dict[Quantum, NodeId] = {}
        quantumNode: QuantumNode
        for quantumNode in state['nodesList']:
            quanta[quantumNode.taskDef].add(quantumNode.quantum)
            quantumToNodeId[quantumNode.quantum] = quantumNode.nodeId
        _buildId = quantumNode.nodeId.buildId if state['nodesList'] else None  # type: ignore
        self._buildGraphs(quanta, _quantumToNodeId=quantumToNodeId, _buildId=_buildId)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, QuantumGraph):
            return False
        if len(self) != len(other):
            return False
        for node in self:
            if node not in other:
                return False
            if self.determineInputsToQuantumNode(node) != other.determineInputsToQuantumNode(node):
                return False
            if self.determineOutputsOfQuantumNode(node) != other.determineOutputsOfQuantumNode(node):
                return False
        return list(self.taskGraph) == list(other.taskGraph)
