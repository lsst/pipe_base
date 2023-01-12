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

__all__ = ("QuantumGraph", "IncompatibleGraphError")

import io
import json
import lzma
import os
import pickle
import struct
import time
import uuid
import warnings
from collections import defaultdict, deque
from itertools import chain
from types import MappingProxyType
from typing import (
    Any,
    BinaryIO,
    DefaultDict,
    Deque,
    Dict,
    FrozenSet,
    Generator,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

import networkx as nx
from lsst.daf.butler import DatasetRef, DatasetType, DimensionRecordsAccumulator, DimensionUniverse, Quantum
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils.introspection import get_full_type_name
from networkx.drawing.nx_agraph import write_dot

from ..connections import iterConnections
from ..pipeline import TaskDef
from ._implDetails import DatasetTypeName, _DatasetTracker, _pruner
from ._loadHelpers import LoadHelper
from ._versionDeserializers import DESERIALIZER_MAP
from .quantumNode import BuildId, QuantumNode

_T = TypeVar("_T", bound="QuantumGraph")

# modify this constant any time the on disk representation of the save file
# changes, and update the load helpers to behave properly for each version.
SAVE_VERSION = 3

# Strings used to describe the format for the preamble bytes in a file save
# The base is a big endian encoded unsigned short that is used to hold the
# file format version. This allows reading version bytes and determine which
# loading code should be used for the rest of the file
STRUCT_FMT_BASE = ">H"
#
# Version 1
# This marks a big endian encoded format with an unsigned short, an unsigned
# long long, and an unsigned long long in the byte stream
# Version 2
# A big endian encoded format with an unsigned long long byte stream used to
# indicate the total length of the entire header.
STRUCT_FMT_STRING = {1: ">QQ", 2: ">Q"}

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
    metadata : Optional Mapping of `str` to primitives
        This is an optional parameter of extra data to carry with the graph.
        Entries in this mapping should be able to be serialized in JSON.
    pruneRefs : iterable [ `DatasetRef` ], optional
        Set of dataset refs to exclude from a graph.
    initInputs : `Mapping`, optional
        Maps tasks to their InitInput dataset refs. Dataset refs can be either
        resolved or non-resolved. Presently the same dataset refs are included
        in each `Quantum` for the same task.
    initOutputs : `Mapping`, optional
        Maps tasks to their InitOutput dataset refs. Dataset refs can be either
        resolved or non-resolved. For intermediate resolved refs their dataset
        ID must match ``initInputs`` and Quantum ``initInputs``.
    globalInitOutputs : iterable [ `DatasetRef` ], optional
        Dataset refs for some global objects produced by pipeline. These
        objects include task configurations and package versions. Typically
        they have an empty DataId, but there is no real restriction on what
        can appear here.

    Raises
    ------
    ValueError
        Raised if the graph is pruned such that some tasks no longer have nodes
        associated with them.
    """

    def __init__(
        self,
        quanta: Mapping[TaskDef, Set[Quantum]],
        metadata: Optional[Mapping[str, Any]] = None,
        pruneRefs: Optional[Iterable[DatasetRef]] = None,
        universe: Optional[DimensionUniverse] = None,
        initInputs: Optional[Mapping[TaskDef, Iterable[DatasetRef]]] = None,
        initOutputs: Optional[Mapping[TaskDef, Iterable[DatasetRef]]] = None,
        globalInitOutputs: Optional[Iterable[DatasetRef]] = None,
    ):
        self._buildGraphs(
            quanta,
            metadata=metadata,
            pruneRefs=pruneRefs,
            universe=universe,
            initInputs=initInputs,
            initOutputs=initOutputs,
            globalInitOutputs=globalInitOutputs,
        )

    def _buildGraphs(
        self,
        quanta: Mapping[TaskDef, Set[Quantum]],
        *,
        _quantumToNodeId: Optional[Mapping[Quantum, uuid.UUID]] = None,
        _buildId: Optional[BuildId] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        pruneRefs: Optional[Iterable[DatasetRef]] = None,
        universe: Optional[DimensionUniverse] = None,
        initInputs: Optional[Mapping[TaskDef, Iterable[DatasetRef]]] = None,
        initOutputs: Optional[Mapping[TaskDef, Iterable[DatasetRef]]] = None,
        globalInitOutputs: Optional[Iterable[DatasetRef]] = None,
    ) -> None:
        """Builds the graph that is used to store the relation between tasks,
        and the graph that holds the relations between quanta
        """
        self._metadata = metadata
        self._buildId = _buildId if _buildId is not None else BuildId(f"{time.time()}-{os.getpid()}")
        # Data structures used to identify relations between components;
        # DatasetTypeName -> TaskDef for task,
        # and DatasetRef -> QuantumNode for the quanta
        self._datasetDict = _DatasetTracker[DatasetTypeName, TaskDef](createInverse=True)
        self._datasetRefDict = _DatasetTracker[DatasetRef, QuantumNode]()

        self._nodeIdMap: Dict[uuid.UUID, QuantumNode] = {}
        self._taskToQuantumNode: MutableMapping[TaskDef, Set[QuantumNode]] = defaultdict(set)
        for taskDef, quantumSet in quanta.items():
            connections = taskDef.connections

            # For each type of connection in the task, add a key to the
            # `_DatasetTracker` for the connections name, with a value of
            # the TaskDef in the appropriate field
            for inpt in iterConnections(connections, ("inputs", "prerequisiteInputs", "initInputs")):
                # Have to handle components in inputs.
                dataset_name, _, _ = inpt.name.partition(".")
                self._datasetDict.addConsumer(DatasetTypeName(dataset_name), taskDef)

            for output in iterConnections(connections, ("outputs",)):
                # Have to handle possible components in outputs.
                dataset_name, _, _ = output.name.partition(".")
                self._datasetDict.addProducer(DatasetTypeName(dataset_name), taskDef)

            # For each `Quantum` in the set of all `Quantum` for this task,
            # add a key to the `_DatasetTracker` that is a `DatasetRef` for one
            # of the individual datasets inside the `Quantum`, with a value of
            # a newly created QuantumNode to the appropriate input/output
            # field.
            for quantum in quantumSet:
                if quantum.dataId is not None:
                    if universe is None:
                        universe = quantum.dataId.universe
                    elif universe != quantum.dataId.universe:
                        raise RuntimeError(
                            "Mismatched dimension universes in QuantumGraph construction: "
                            f"{universe} != {quantum.dataId.universe}. "
                        )

                if _quantumToNodeId:
                    if (nodeId := _quantumToNodeId.get(quantum)) is None:
                        raise ValueError(
                            "If _quantuMToNodeNumber is not None, all quanta must have an "
                            "associated value in the mapping"
                        )
                else:
                    nodeId = uuid.uuid4()

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
                            if sub.isComponent():
                                sub = sub.makeCompositeRef()
                            self._datasetRefDict.addConsumer(sub, value)
                    else:
                        assert isinstance(dsRef, DatasetRef)
                        if dsRef.isComponent():
                            dsRef = dsRef.makeCompositeRef()
                        self._datasetRefDict.addConsumer(dsRef, value)
                for dsRef in chain.from_iterable(quantum.outputs.values()):
                    self._datasetRefDict.addProducer(dsRef, value)

        if pruneRefs is not None:
            # track what refs were pruned and prune the graph
            prunes: Set[QuantumNode] = set()
            _pruner(self._datasetRefDict, pruneRefs, alreadyPruned=prunes)

            # recreate the taskToQuantumNode dict removing nodes that have been
            # pruned. Keep track of task defs that now have no QuantumNodes
            emptyTasks: Set[str] = set()
            newTaskToQuantumNode: DefaultDict[TaskDef, Set[QuantumNode]] = defaultdict(set)
            # accumulate all types
            types_ = set()
            # tracker for any pruneRefs that have caused tasks to have no nodes
            # This helps the user find out what caused the issues seen.
            culprits = set()
            # Find all the types from the refs to prune
            for r in pruneRefs:
                types_.add(r.datasetType)

            # For each of the tasks, and their associated nodes, remove any
            # any nodes that were pruned. If there are no nodes associated
            # with a task, record that task, and find out if that was due to
            # a type from an input ref to prune.
            for td, taskNodes in self._taskToQuantumNode.items():
                diff = taskNodes.difference(prunes)
                if len(diff) == 0:
                    if len(taskNodes) != 0:
                        tp: DatasetType
                        for tp in types_:
                            if (tmpRefs := next(iter(taskNodes)).quantum.inputs.get(tp)) and not set(
                                tmpRefs
                            ).difference(pruneRefs):
                                culprits.add(tp.name)
                    emptyTasks.add(td.label)
                newTaskToQuantumNode[td] = diff

            # update the internal dict
            self._taskToQuantumNode = newTaskToQuantumNode

            if emptyTasks:
                raise ValueError(
                    f"{', '.join(emptyTasks)} task(s) have no nodes associated with them "
                    f"after graph pruning; {', '.join(culprits)} caused over-pruning"
                )

        # Dimension universe
        if universe is None:
            raise RuntimeError(
                "Dimension universe or at least one quantum with a data ID "
                "must be provided when constructing a QuantumGraph."
            )
        self._universe = universe

        # Graph of quanta relations
        self._connectedQuanta = self._datasetRefDict.makeNetworkXGraph()
        self._count = len(self._connectedQuanta)

        # Graph of task relations, used in various methods
        self._taskGraph = self._datasetDict.makeNetworkXGraph()

        # convert default dict into a regular to prevent accidental key
        # insertion
        self._taskToQuantumNode = dict(self._taskToQuantumNode.items())

        self._initInputRefs: Dict[TaskDef, List[DatasetRef]] = {}
        self._initOutputRefs: Dict[TaskDef, List[DatasetRef]] = {}
        self._globalInitOutputRefs: List[DatasetRef] = []
        if initInputs is not None:
            self._initInputRefs = {taskDef: list(refs) for taskDef, refs in initInputs.items()}
        if initOutputs is not None:
            self._initOutputRefs = {taskDef: list(refs) for taskDef, refs in initOutputs.items()}
        if globalInitOutputs is not None:
            self._globalInitOutputRefs = list(globalInitOutputs)

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
            All the data set type names that are present in the graph, not
            including global init-outputs.
        """
        return tuple(self._datasetDict.keys())

    @property
    def isConnected(self) -> bool:
        """Return True if all of the nodes in the graph are connected, ignores
        directionality of connections.
        """
        return nx.is_weakly_connected(self._connectedQuanta)

    def pruneGraphFromRefs(self: _T, refs: Iterable[DatasetRef]) -> _T:
        r"""Return a graph pruned of input `~lsst.daf.butler.DatasetRef`\ s
        and nodes which depend on them.

        Parameters
        ----------
        refs : `Iterable` of `DatasetRef`
            Refs which should be removed from resulting graph

        Returns
        -------
        graph : `QuantumGraph`
            A graph that has been pruned of specified refs and the nodes that
            depend on them.
        """
        newInst = object.__new__(type(self))
        quantumMap = defaultdict(set)
        for node in self:
            quantumMap[node.taskDef].add(node.quantum)

        # convert to standard dict to prevent accidental key insertion
        quantumDict: Dict[TaskDef, Set[Quantum]] = dict(quantumMap.items())

        newInst._buildGraphs(
            quantumDict,
            _quantumToNodeId={n.quantum: n.nodeId for n in self},
            metadata=self._metadata,
            pruneRefs=refs,
            universe=self._universe,
            globalInitOutputs=self._globalInitOutputRefs,
        )
        return newInst

    def getQuantumNodeByNodeId(self, nodeId: uuid.UUID) -> QuantumNode:
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
        KeyError
            Raised if the requested nodeId is not in the graph.
        """
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
        return frozenset(node.quantum for node in self._taskToQuantumNode.get(taskDef, ()))

    def getNumberOfQuantaForTask(self, taskDef: TaskDef) -> int:
        """Return all the number of `Quantum` associated with a `TaskDef`.

        Parameters
        ----------
        taskDef : `TaskDef`
            The `TaskDef` for which `Quantum` are to be queried

        Returns
        -------
        count : int
            The number of `Quantum` that are associated with the specified
            `TaskDef`.
        """
        return len(self._taskToQuantumNode.get(taskDef, ()))

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
        return (c for c in self._datasetDict.getConsumers(datasetTypeName))

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
        return self._datasetDict.getProducer(datasetTypeName)

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
        return self._datasetDict.getAll(datasetTypeName)

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
        for task in self._taskToQuantumNode.keys():
            split = task.taskName.split(".")
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
        for task in self._taskToQuantumNode.keys():
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
        result = result.union(quantum for task in tasks for quantum in self.getQuantaForTask(task))
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
        for node in self:
            if quantum == node.quantum:
                return True
        return False

    def writeDotGraph(self, output: Union[str, io.BufferedIOBase]) -> None:
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
            nodes = (nodes,)
        quantumSubgraph = self._connectedQuanta.subgraph(nodes).nodes
        quantumMap = defaultdict(set)

        node: QuantumNode
        for node in quantumSubgraph:
            quantumMap[node.taskDef].add(node.quantum)

        # convert to standard dict to prevent accidental key insertion
        quantumDict: Dict[TaskDef, Set[Quantum]] = dict(quantumMap.items())
        # Create an empty graph, and then populate it with custom mapping
        newInst = type(self)({}, universe=self._universe)
        newInst._buildGraphs(
            quantumDict,
            _quantumToNodeId={n.quantum: n.nodeId for n in nodes},
            _buildId=self._buildId,
            metadata=self._metadata,
            universe=self._universe,
            globalInitOutputs=self._globalInitOutputRefs,
        )
        return newInst

    def subsetToConnected(self: _T) -> Tuple[_T, ...]:
        """Generate a list of subgraphs where each is connected.

        Returns
        -------
        result : list of `QuantumGraph`
            A list of graphs that are each connected
        """
        return tuple(
            self.subset(connectedSet)
            for connectedSet in nx.weakly_connected_components(self._connectedQuanta)
        )

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

    def saveUri(self, uri: ResourcePathExpression) -> None:
        """Save `QuantumGraph` to the specified URI.

        Parameters
        ----------
        uri : convertible to `ResourcePath`
            URI to where the graph should be saved.
        """
        buffer = self._buildSaveObject()
        path = ResourcePath(uri)
        if path.getExtension() not in (".qgraph"):
            raise TypeError(f"Can currently only save a graph in qgraph format not {uri}")
        path.write(buffer)  # type: ignore  # Ignore because bytearray is safe to use in place of bytes

    @property
    def metadata(self) -> Optional[MappingProxyType[str, Any]]:
        """ """
        if self._metadata is None:
            return None
        return MappingProxyType(self._metadata)

    def initInputRefs(self, taskDef: TaskDef) -> Optional[List[DatasetRef]]:
        """Return DatasetRefs for a given task InitInputs.

        Parameters
        ----------
        taskDef : `TaskDef`
            Task definition structure.

        Returns
        -------
        refs : `list` [ `DatasetRef` ] or None
            DatasetRef for the task InitInput, can be `None`. This can return
            either resolved or non-resolved reference.
        """
        return self._initInputRefs.get(taskDef)

    def initOutputRefs(self, taskDef: TaskDef) -> Optional[List[DatasetRef]]:
        """Return DatasetRefs for a given task InitOutputs.

        Parameters
        ----------
        taskDef : `TaskDef`
            Task definition structure.

        Returns
        -------
        refs : `list` [ `DatasetRef` ] or None
            DatasetRefs for the task InitOutput, can be `None`. This can return
            either resolved or non-resolved reference. Resolved reference will
            match Quantum's initInputs if this is an intermediate dataset type.
        """
        return self._initOutputRefs.get(taskDef)

    def globalInitOutputRefs(self) -> List[DatasetRef]:
        """Return DatasetRefs for global InitOutputs.

        Returns
        -------
        refs : `list` [ `DatasetRef` ]
            DatasetRefs for global InitOutputs.
        """
        return self._globalInitOutputRefs

    @classmethod
    def loadUri(
        cls,
        uri: ResourcePathExpression,
        universe: Optional[DimensionUniverse] = None,
        nodes: Optional[Iterable[uuid.UUID]] = None,
        graphID: Optional[BuildId] = None,
        minimumVersion: int = 3,
    ) -> QuantumGraph:
        """Read `QuantumGraph` from a URI.

        Parameters
        ----------
        uri : convertible to `ResourcePath`
            URI from where to load the graph.
        universe: `~lsst.daf.butler.DimensionUniverse` optional
            DimensionUniverse instance, not used by the method itself but
            needed to ensure that registry data structures are initialized.
            If None it is loaded from the QuantumGraph saved structure. If
            supplied, the DimensionUniverse from the loaded `QuantumGraph`
            will be validated against the supplied argument for compatibility.
        nodes: iterable of `int` or None
            Numbers that correspond to nodes in the graph. If specified, only
            these nodes will be loaded. Defaults to None, in which case all
            nodes will be loaded.
        graphID : `str` or `None`
            If specified this ID is verified against the loaded graph prior to
            loading any Nodes. This defaults to None in which case no
            validation is done.
        minimumVersion : int
            Minimum version of a save file to load. Set to -1 to load all
            versions. Older versions may need to be loaded, and re-saved
            to upgrade them to the latest format before they can be used in
            production.

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
        RuntimeError
            Raise if Supplied DimensionUniverse is not compatible with the
            DimensionUniverse saved in the graph


        Notes
        -----
        Reading Quanta from pickle requires existence of singleton
        DimensionUniverse which is usually instantiated during Registry
        initialization. To make sure that DimensionUniverse exists this method
        accepts dummy DimensionUniverse argument.
        """
        uri = ResourcePath(uri)
        # With ResourcePath we have the choice of always using a local file
        # or reading in the bytes directly. Reading in bytes can be more
        # efficient for reasonably-sized pickle files when the resource
        # is remote. For now use the local file variant. For a local file
        # as_local() does nothing.

        if uri.getExtension() in (".pickle", ".pkl"):
            with uri.as_local() as local, open(local.ospath, "rb") as fd:
                warnings.warn("Pickle graphs are deprecated, please re-save your graph with the save method")
                qgraph = pickle.load(fd)
        elif uri.getExtension() in (".qgraph"):
            with LoadHelper(uri, minimumVersion) as loader:
                qgraph = loader.load(universe, nodes, graphID)
        else:
            raise ValueError("Only know how to handle files saved as `pickle`, `pkl`, or `qgraph`")
        if not isinstance(qgraph, QuantumGraph):
            raise TypeError(f"QuantumGraph save file contains unexpected object type: {type(qgraph)}")
        return qgraph

    @classmethod
    def readHeader(cls, uri: ResourcePathExpression, minimumVersion: int = 3) -> Optional[str]:
        """Read the header of a `QuantumGraph` pointed to by the uri parameter
        and return it as a string.

        Parameters
        ----------
        uri : convertible to `ResourcePath`
            The location of the `QuantumGraph` to load. If the argument is a
            string, it must correspond to a valid `ResourcePath` path.
        minimumVersion : int
            Minimum version of a save file to load. Set to -1 to load all
            versions. Older versions may need to be loaded, and re-saved
            to upgrade them to the latest format before they can be used in
            production.

        Returns
        -------
        header : `str` or `None`
            The header associated with the specified `QuantumGraph` it there is
            one, else `None`.

        Raises
        ------
        ValueError
            Raised if `QuantuGraph` was saved as a pickle.
            Raised if the extention of the file specified by uri is not a
            `QuantumGraph` extention.
        """
        uri = ResourcePath(uri)
        if uri.getExtension() in (".pickle", ".pkl"):
            raise ValueError("Reading a header from a pickle save is not supported")
        elif uri.getExtension() in (".qgraph"):
            return LoadHelper(uri, minimumVersion).readHeader()
        else:
            raise ValueError("Only know how to handle files saved as `qgraph`")

    def buildAndPrintHeader(self) -> None:
        """Creates a header that would be used in a save of this object and
        prints it out to standard out.
        """
        _, header = self._buildSaveObject(returnHeader=True)
        print(json.dumps(header))

    def save(self, file: BinaryIO) -> None:
        """Save QuantumGraph to a file.

        Parameters
        ----------
        file : `io.BufferedIOBase`
            File to write pickle data open in binary mode.
        """
        buffer = self._buildSaveObject()
        file.write(buffer)  # type: ignore # Ignore because bytearray is safe to use in place of bytes

    def _buildSaveObject(self, returnHeader: bool = False) -> Union[bytearray, Tuple[bytearray, Dict]]:
        # make some containers
        jsonData: Deque[bytes] = deque()
        # node map is a list because json does not accept mapping keys that
        # are not strings, so we store a list of key, value pairs that will
        # be converted to a mapping on load
        nodeMap = []
        taskDefMap = {}
        headerData: Dict[str, Any] = {}

        # Store the QauntumGraph BuildId, this will allow validating BuildIds
        # at load time, prior to loading any QuantumNodes. Name chosen for
        # unlikely conflicts.
        headerData["GraphBuildID"] = self.graphID
        headerData["Metadata"] = self._metadata

        # Store the universe this graph was created with
        universeConfig = self._universe.dimensionConfig
        headerData["universe"] = universeConfig.toDict()

        # counter for the number of bytes processed thus far
        count = 0
        # serialize out the task Defs recording the start and end bytes of each
        # taskDef
        inverseLookup = self._datasetDict.inverse
        taskDef: TaskDef
        # sort by task label to ensure serialization happens in the same order
        for taskDef in self.taskGraph:
            # compressing has very little impact on saving or load time, but
            # a large impact on on disk size, so it is worth doing
            taskDescription: Dict[str, Any] = {}
            # save the fully qualified name.
            taskDescription["taskName"] = get_full_type_name(taskDef.taskClass)
            # save the config as a text stream that will be un-persisted on the
            # other end
            stream = io.StringIO()
            taskDef.config.saveToStream(stream)
            taskDescription["config"] = stream.getvalue()
            taskDescription["label"] = taskDef.label
            if (refs := self._initInputRefs.get(taskDef)) is not None:
                taskDescription["initInputRefs"] = [ref.to_json() for ref in refs]
            if (refs := self._initOutputRefs.get(taskDef)) is not None:
                taskDescription["initOutputRefs"] = [ref.to_json() for ref in refs]

            inputs = []
            outputs = []

            # Determine the connection between all of tasks and save that in
            # the header as a list of connections and edges in each task
            # this will help in un-persisting, and possibly in a "quick view"
            # method that does not require everything to be un-persisted
            #
            # Typing returns can't be parameter dependent
            for connection in inverseLookup[taskDef]:  # type: ignore
                consumers = self._datasetDict.getConsumers(connection)
                producer = self._datasetDict.getProducer(connection)
                if taskDef in consumers:
                    # This checks if the task consumes the connection directly
                    # from the datastore or it is produced by another task
                    producerLabel = producer.label if producer is not None else "datastore"
                    inputs.append((producerLabel, connection))
                elif taskDef not in consumers and producer is taskDef:
                    # If there are no consumers for this tasks produced
                    # connection, the output will be said to be the datastore
                    # in which case the for loop will be a zero length loop
                    if not consumers:
                        outputs.append(("datastore", connection))
                    for td in consumers:
                        outputs.append((td.label, connection))

            # dump to json string, and encode that string to bytes and then
            # conpress those bytes
            dump = lzma.compress(json.dumps(taskDescription).encode())
            # record the sizing and relation information
            taskDefMap[taskDef.label] = {
                "bytes": (count, count + len(dump)),
                "inputs": inputs,
                "outputs": outputs,
            }
            count += len(dump)
            jsonData.append(dump)

        headerData["TaskDefs"] = taskDefMap

        # serialize the nodes, recording the start and end bytes of each node
        dimAccumulator = DimensionRecordsAccumulator()
        for node in self:
            # compressing has very little impact on saving or load time, but
            # a large impact on on disk size, so it is worth doing
            simpleNode = node.to_simple(accumulator=dimAccumulator)

            dump = lzma.compress(simpleNode.json().encode())
            jsonData.append(dump)
            nodeMap.append(
                (
                    str(node.nodeId),
                    {
                        "bytes": (count, count + len(dump)),
                        "inputs": [str(n.nodeId) for n in self.determineInputsToQuantumNode(node)],
                        "outputs": [str(n.nodeId) for n in self.determineOutputsOfQuantumNode(node)],
                    },
                )
            )
            count += len(dump)

        headerData["DimensionRecords"] = {
            key: value.dict() for key, value in dimAccumulator.makeSerializedDimensionRecordMapping().items()
        }

        # need to serialize this as a series of key,value tuples because of
        # a limitation on how json cant do anything but strings as keys
        headerData["Nodes"] = nodeMap

        if self._globalInitOutputRefs:
            headerData["GlobalInitOutputRefs"] = [ref.to_json() for ref in self._globalInitOutputRefs]

        # dump the headerData to json
        header_encode = lzma.compress(json.dumps(headerData).encode())

        # record the sizes as 2 unsigned long long numbers for a total of 16
        # bytes
        save_bytes = struct.pack(STRUCT_FMT_BASE, SAVE_VERSION)

        fmt_string = DESERIALIZER_MAP[SAVE_VERSION].FMT_STRING()
        map_lengths = struct.pack(fmt_string, len(header_encode))

        # write each component of the save out in a deterministic order
        # buffer = io.BytesIO()
        # buffer.write(map_lengths)
        # buffer.write(taskDef_pickle)
        # buffer.write(map_pickle)
        buffer = bytearray()
        buffer.extend(MAGIC_BYTES)
        buffer.extend(save_bytes)
        buffer.extend(map_lengths)
        buffer.extend(header_encode)
        # Iterate over the length of pickleData, and for each element pop the
        # leftmost element off the deque and write it out. This is to save
        # memory, as the memory is added to the buffer object, it is removed
        # from from the container.
        #
        # Only this section needs to worry about memory pressue because
        # everything else written to the buffer prior to this pickle data is
        # only on the order of kilobytes to low numbers of megabytes.
        while jsonData:
            buffer.extend(jsonData.popleft())
        if returnHeader:
            return buffer, headerData
        else:
            return buffer

    @classmethod
    def load(
        cls,
        file: BinaryIO,
        universe: Optional[DimensionUniverse] = None,
        nodes: Optional[Iterable[uuid.UUID]] = None,
        graphID: Optional[BuildId] = None,
        minimumVersion: int = 3,
    ) -> QuantumGraph:
        """Read QuantumGraph from a file that was made by `save`.

        Parameters
        ----------
        file : `io.IO` of bytes
            File with pickle data open in binary mode.
        universe: `~lsst.daf.butler.DimensionUniverse`, optional
            DimensionUniverse instance, not used by the method itself but
            needed to ensure that registry data structures are initialized.
            If None it is loaded from the QuantumGraph saved structure. If
            supplied, the DimensionUniverse from the loaded `QuantumGraph`
            will be validated against the supplied argument for compatibility.
        nodes: iterable of `int` or None
            Numbers that correspond to nodes in the graph. If specified, only
            these nodes will be loaded. Defaults to None, in which case all
            nodes will be loaded.
        graphID : `str` or `None`
            If specified this ID is verified against the loaded graph prior to
            loading any Nodes. This defaults to None in which case no
            validation is done.
        minimumVersion : int
            Minimum version of a save file to load. Set to -1 to load all
            versions. Older versions may need to be loaded, and re-saved
            to upgrade them to the latest format before they can be used in
            production.

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
            with LoadHelper(file, minimumVersion) as loader:
                qgraph = loader.load(universe, nodes, graphID)
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
    def graphID(self) -> BuildId:
        """Returns the ID generated by the graph at construction time"""
        return self._buildId

    @property
    def universe(self) -> DimensionUniverse:
        """Dimension universe associated with this graph."""
        return self._universe

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
        universe: Optional[DimensionUniverse] = None
        for node in self:
            dId = node.quantum.dataId
            if dId is None:
                continue
            universe = dId.graph.universe
        return {"reduced": self._buildSaveObject(), "graphId": self._buildId, "universe": universe}

    def __setstate__(self, state: dict) -> None:
        """Reconstructs the state of the graph from the information persisted
        in getstate.
        """
        buffer = io.BytesIO(state["reduced"])
        with LoadHelper(buffer, minimumVersion=3) as loader:
            qgraph = loader.load(state["universe"], graphID=state["graphId"])

        self._metadata = qgraph._metadata
        self._buildId = qgraph._buildId
        self._datasetDict = qgraph._datasetDict
        self._nodeIdMap = qgraph._nodeIdMap
        self._count = len(qgraph)
        self._taskToQuantumNode = qgraph._taskToQuantumNode
        self._taskGraph = qgraph._taskGraph
        self._connectedQuanta = qgraph._connectedQuanta
        self._initInputRefs = qgraph._initInputRefs
        self._initOutputRefs = qgraph._initOutputRefs

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
        if set(self.allDatasetTypes) != set(other.allDatasetTypes):
            return False
        return set(self.taskGraph) == set(other.taskGraph)
