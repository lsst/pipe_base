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
from __future__ import annotations

__all__ = ("IncompatibleGraphError", "QuantumGraph")

import datetime
import getpass
import io
import json
import lzma
import os
import struct
import sys
import time
import uuid
from collections import defaultdict, deque
from collections.abc import Generator, Iterable, Iterator, Mapping, MutableMapping
from itertools import chain
from types import MappingProxyType
from typing import Any, BinaryIO, TypeVar

import networkx as nx
from networkx.drawing.nx_agraph import write_dot

import lsst.utils.logging
from lsst.daf.butler import (
    Config,
    DatasetId,
    DatasetRef,
    DatasetType,
    DimensionRecordsAccumulator,
    DimensionUniverse,
    LimitedButler,
    Quantum,
    QuantumBackedButler,
)
from lsst.daf.butler.datastore.record_data import DatastoreRecordData
from lsst.daf.butler.persistence_context import PersistenceContextVars
from lsst.daf.butler.registry import ConflictingDefinitionError
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils.introspection import get_full_type_name
from lsst.utils.packages import Packages

from ..config import PipelineTaskConfig
from ..connections import iterConnections
from ..pipeline import TaskDef
from ..pipeline_graph import PipelineGraph, compare_packages, log_config_mismatch
from ._implDetails import DatasetTypeName, _DatasetTracker
from ._loadHelpers import LoadHelper
from ._versionDeserializers import DESERIALIZER_MAP
from .graphSummary import QgraphSummary, QgraphTaskSummary
from .quantumNode import BuildId, QuantumNode

_T = TypeVar("_T", bound="QuantumGraph")
_LOG = lsst.utils.logging.getLogger(__name__)

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
    to incompatibilities.
    """

    pass


class QuantumGraph:
    """QuantumGraph is a directed acyclic graph of `QuantumNode` objects.

    This data structure represents a concrete workflow generated from a
    `Pipeline`.

    Parameters
    ----------
    quanta : `~collections.abc.Mapping` [ `TaskDef`, \
            `set` [ `~lsst.daf.butler.Quantum` ] ]
        This maps tasks (and their configs) to the sets of data they are to
        process.
    metadata : Optional `~collections.abc.Mapping` of `str` to primitives
        This is an optional parameter of extra data to carry with the graph.
        Entries in this mapping should be able to be serialized in JSON.
    universe : `~lsst.daf.butler.DimensionUniverse`, optional
        The dimensions in which quanta can be defined. Need only be provided if
        no quanta have data IDs.
    initInputs : `~collections.abc.Mapping`, optional
        Maps tasks to their InitInput dataset refs. Dataset refs can be either
        resolved or non-resolved. Presently the same dataset refs are included
        in each `~lsst.daf.butler.Quantum` for the same task.
    initOutputs : `~collections.abc.Mapping`, optional
        Maps tasks to their InitOutput dataset refs. Dataset refs can be either
        resolved or non-resolved. For intermediate resolved refs their dataset
        ID must match ``initInputs`` and Quantum ``initInputs``.
    globalInitOutputs : iterable [ `~lsst.daf.butler.DatasetRef` ], optional
        Dataset refs for some global objects produced by pipeline. These
        objects include task configurations and package versions. Typically
        they have an empty DataId, but there is no real restriction on what
        can appear here.
    registryDatasetTypes : iterable [ `~lsst.daf.butler.DatasetType` ], \
            optional
        Dataset types which are used by this graph, their definitions must
        match registry. If registry does not define dataset type yet, then
        it should match one that will be created later.

    Raises
    ------
    ValueError
        Raised if the graph is pruned such that some tasks no longer have nodes
        associated with them.
    """

    def __init__(
        self,
        quanta: Mapping[TaskDef, set[Quantum]],
        metadata: Mapping[str, Any] | None = None,
        universe: DimensionUniverse | None = None,
        initInputs: Mapping[TaskDef, Iterable[DatasetRef]] | None = None,
        initOutputs: Mapping[TaskDef, Iterable[DatasetRef]] | None = None,
        globalInitOutputs: Iterable[DatasetRef] | None = None,
        registryDatasetTypes: Iterable[DatasetType] | None = None,
    ):
        self._buildGraphs(
            quanta,
            metadata=metadata,
            universe=universe,
            initInputs=initInputs,
            initOutputs=initOutputs,
            globalInitOutputs=globalInitOutputs,
            registryDatasetTypes=registryDatasetTypes,
        )

    def _buildGraphs(
        self,
        quanta: Mapping[TaskDef, set[Quantum]],
        *,
        _quantumToNodeId: Mapping[Quantum, uuid.UUID] | None = None,
        _buildId: BuildId | None = None,
        metadata: Mapping[str, Any] | None = None,
        universe: DimensionUniverse | None = None,
        initInputs: Mapping[TaskDef, Iterable[DatasetRef]] | None = None,
        initOutputs: Mapping[TaskDef, Iterable[DatasetRef]] | None = None,
        globalInitOutputs: Iterable[DatasetRef] | None = None,
        registryDatasetTypes: Iterable[DatasetType] | None = None,
    ) -> None:
        """Build the graph that is used to store the relation between tasks,
        and the graph that holds the relations between quanta
        """
        # Save packages to metadata
        self._metadata = dict(metadata) if metadata is not None else {}
        self._metadata["packages"] = Packages.fromSystem()
        self._metadata["user"] = getpass.getuser()
        self._metadata["time"] = f"{datetime.datetime.now()}"
        self._metadata["full_command"] = " ".join(sys.argv)

        self._buildId = _buildId if _buildId is not None else BuildId(f"{time.time()}-{os.getpid()}")
        # Data structure used to identify relations between
        # DatasetTypeName -> TaskDef.
        self._datasetDict = _DatasetTracker(createInverse=True)

        # Temporary graph that will have dataset UUIDs (as raw bytes) and
        # QuantumNode objects as nodes; will be collapsed down to just quanta
        # later.
        bipartite_graph = nx.DiGraph()

        self._nodeIdMap: dict[uuid.UUID, QuantumNode] = {}
        self._taskToQuantumNode: MutableMapping[TaskDef, set[QuantumNode]] = defaultdict(set)
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

                bipartite_graph.add_node(value, bipartite=0)
                for dsRef in chain(inits, inputs):
                    # unfortunately, `Quantum` allows inits to be individual
                    # `DatasetRef`s or an Iterable of such, so there must
                    # be an instance check here
                    if isinstance(dsRef, Iterable):
                        for sub in dsRef:
                            bipartite_graph.add_node(sub.id.bytes, bipartite=1)
                            bipartite_graph.add_edge(sub.id.bytes, value)
                    else:
                        assert isinstance(dsRef, DatasetRef)
                        if dsRef.isComponent():
                            dsRef = dsRef.makeCompositeRef()
                        bipartite_graph.add_node(dsRef.id.bytes, bipartite=1)
                        bipartite_graph.add_edge(dsRef.id.bytes, value)
                for dsRef in chain.from_iterable(quantum.outputs.values()):
                    bipartite_graph.add_node(dsRef.id.bytes, bipartite=1)
                    bipartite_graph.add_edge(value, dsRef.id.bytes)

        # Dimension universe
        if universe is None:
            raise RuntimeError(
                "Dimension universe or at least one quantum with a data ID "
                "must be provided when constructing a QuantumGraph."
            )
        self._universe = universe

        # Make graph of quanta relations, by projecting out the dataset nodes
        # in the bipartite_graph, leaving just the quanta.
        self._connectedQuanta = nx.algorithms.bipartite.projected_graph(
            bipartite_graph, self._nodeIdMap.values()
        )
        self._count = len(self._connectedQuanta)

        # Graph of task relations, used in various methods
        self._taskGraph = self._datasetDict.makeNetworkXGraph()

        # convert default dict into a regular to prevent accidental key
        # insertion
        self._taskToQuantumNode = dict(self._taskToQuantumNode.items())

        self._initInputRefs: dict[str, list[DatasetRef]] = {}
        self._initOutputRefs: dict[str, list[DatasetRef]] = {}
        self._globalInitOutputRefs: list[DatasetRef] = []
        self._registryDatasetTypes: list[DatasetType] = []
        if initInputs is not None:
            self._initInputRefs = {taskDef.label: list(refs) for taskDef, refs in initInputs.items()}
        if initOutputs is not None:
            self._initOutputRefs = {taskDef.label: list(refs) for taskDef, refs in initOutputs.items()}
        if globalInitOutputs is not None:
            self._globalInitOutputRefs = list(globalInitOutputs)
        if registryDatasetTypes is not None:
            self._registryDatasetTypes = list(registryDatasetTypes)

        # PipelineGraph is current constructed on first use.
        # TODO DM-40442: use PipelineGraph instead of TaskDef
        # collections.
        self._pipeline_graph: PipelineGraph | None = None

    @property
    def pipeline_graph(self) -> PipelineGraph:
        """A graph representation of the tasks and dataset types in the quantum
        graph.
        """
        if self._pipeline_graph is None:
            # Construct into a temporary for strong exception safety.
            pipeline_graph = PipelineGraph()
            for task_def in self._taskToQuantumNode.keys():
                pipeline_graph.add_task(
                    task_def.label, task_def.taskClass, task_def.config, connections=task_def.connections
                )
            dataset_types = {dataset_type.name: dataset_type for dataset_type in self._registryDatasetTypes}
            pipeline_graph.resolve(dimensions=self._universe, dataset_types=dataset_types)
            self._pipeline_graph = pipeline_graph
        return self._pipeline_graph

    def get_task_quanta(self, label: str) -> Mapping[uuid.UUID, Quantum]:
        """Return the quanta associated with the given task label.

        Parameters
        ----------
        label : `str`
            Task label.

        Returns
        -------
        quanta : `~collections.abc.Mapping` [ uuid.UUID, `Quantum` ]
            Mapping from quantum ID to quantum.  Empty if ``label`` does not
            correspond to a task in this graph.
        """
        task_def = self.findTaskDefByLabel(label)
        if not task_def:
            return {}
        return {node.nodeId: node.quantum for node in self.getNodesForTask(task_def)}

    @property
    def taskGraph(self) -> nx.DiGraph:
        """A graph representing the relations between the tasks inside
        the quantum graph (`networkx.DiGraph`).
        """
        return self._taskGraph

    @property
    def graph(self) -> nx.DiGraph:
        """A graph representing the relations between all the `QuantumNode`
        objects (`networkx.DiGraph`).

        The graph should usually be iterated over, or passed to methods of this
        class, but sometimes direct access to the ``networkx`` object may be
        helpful.
        """
        return self._connectedQuanta

    @property
    def inputQuanta(self) -> Iterable[QuantumNode]:
        """The nodes that are inputs to the graph (iterable [`QuantumNode`]).

        These are the nodes that do not depend on any other nodes in the
        graph.
        """
        return (q for q, n in self._connectedQuanta.in_degree if n == 0)

    @property
    def outputQuanta(self) -> Iterable[QuantumNode]:
        """The nodes that are outputs of the graph (iterable [`QuantumNode`]).

        These are the nodes that have no nodes that depend on them in the
        graph.
        """
        return [q for q, n in self._connectedQuanta.out_degree if n == 0]

    @property
    def allDatasetTypes(self) -> tuple[DatasetTypeName, ...]:
        """All the data set type names that are present in the graph
        (`tuple` [`str`]).

        These types do not include global init-outputs.
        """
        return tuple(self._datasetDict.keys())

    @property
    def isConnected(self) -> bool:
        """Whether all of the nodes in the graph are connected, ignoring
        directionality of connections (`bool`).
        """
        return nx.is_weakly_connected(self._connectedQuanta)

    def getQuantumNodeByNodeId(self, nodeId: uuid.UUID) -> QuantumNode:
        """Lookup a `QuantumNode` from an id associated with the node.

        Parameters
        ----------
        nodeId : `NodeId`
            The number associated with a node.

        Returns
        -------
        node : `QuantumNode`
            The node corresponding with input number.

        Raises
        ------
        KeyError
            Raised if the requested nodeId is not in the graph.
        """
        return self._nodeIdMap[nodeId]

    def getQuantaForTask(self, taskDef: TaskDef) -> frozenset[Quantum]:
        """Return all the `~lsst.daf.butler.Quantum` associated with a
        `TaskDef`.

        Parameters
        ----------
        taskDef : `TaskDef`
            The `TaskDef` for which `~lsst.daf.butler.Quantum` are to be
            queried.

        Returns
        -------
        quanta : `frozenset` of `~lsst.daf.butler.Quantum`
            The `set` of `~lsst.daf.butler.Quantum` that is associated with the
            specified `TaskDef`.
        """
        return frozenset(node.quantum for node in self._taskToQuantumNode.get(taskDef, ()))

    def getNumberOfQuantaForTask(self, taskDef: TaskDef) -> int:
        """Return the number of `~lsst.daf.butler.Quantum` associated with
        a `TaskDef`.

        Parameters
        ----------
        taskDef : `TaskDef`
            The `TaskDef` for which `~lsst.daf.butler.Quantum` are to be
            queried.

        Returns
        -------
        count : `int`
            The number of `~lsst.daf.butler.Quantum` that are associated with
            the specified `TaskDef`.
        """
        return len(self._taskToQuantumNode.get(taskDef, ()))

    def getNodesForTask(self, taskDef: TaskDef) -> frozenset[QuantumNode]:
        r"""Return all the `QuantumNode`\s associated with a `TaskDef`.

        Parameters
        ----------
        taskDef : `TaskDef`
            The `TaskDef` for which `~lsst.daf.butler.Quantum` are to be
            queried.

        Returns
        -------
        nodes : `frozenset` [ `QuantumNode` ]
            A `frozenset` of `QuantumNode` that is associated with the
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
            can also accept a `DatasetTypeName` which is a `~typing.NewType` of
            `str` for type safety in static type checking.

        Returns
        -------
        tasks : iterable of `TaskDef`
            `TaskDef` objects that have the specified `DatasetTypeName` as an
            input, list will be empty if no tasks use specified
            `DatasetTypeName` as an input.

        Raises
        ------
        KeyError
            Raised if the `DatasetTypeName` is not part of the `QuantumGraph`.
        """
        return (c for c in self._datasetDict.getConsumers(datasetTypeName))

    def findTaskWithOutput(self, datasetTypeName: DatasetTypeName) -> TaskDef | None:
        """Find all tasks that have the specified dataset type name as an
        output.

        Parameters
        ----------
        datasetTypeName : `str`
            A string representing the name of a dataset type to be queried,
            can also accept a `DatasetTypeName` which is a `~typing.NewType` of
            `str` for type safety in static type checking.

        Returns
        -------
        result : `TaskDef` or `None`
            `TaskDef` that outputs `DatasetTypeName` as an output or `None` if
            none of the tasks produce this `DatasetTypeName`.

        Raises
        ------
        KeyError
            Raised if the `DatasetTypeName` is not part of the `QuantumGraph`.
        """
        return self._datasetDict.getProducer(datasetTypeName)

    def tasksWithDSType(self, datasetTypeName: DatasetTypeName) -> Iterable[TaskDef]:
        """Find all tasks that are associated with the specified dataset type
        name.

        Parameters
        ----------
        datasetTypeName : `str`
            A string representing the name of a dataset type to be queried,
            can also accept a `DatasetTypeName` which is a `~typing.NewType` of
            `str` for type safety in static type checking.

        Returns
        -------
        result : iterable of `TaskDef`
            `TaskDef` objects that are associated with the specified
            `DatasetTypeName`.

        Raises
        ------
        KeyError
            Raised if the `DatasetTypeName` is not part of the `QuantumGraph`.
        """
        return self._datasetDict.getAll(datasetTypeName)

    def findTaskDefByName(self, taskName: str) -> list[TaskDef]:
        """Determine which `TaskDef` objects in this graph are associated
        with a `str` representing a task name (looks at the ``taskName``
        property of `TaskDef` objects).

        Returns a list of `TaskDef` objects as a `PipelineTask` may appear
        multiple times in a graph with different labels.

        Parameters
        ----------
        taskName : `str`
            Name of a task to search for.

        Returns
        -------
        result : `list` of `TaskDef`
            List of the `TaskDef` objects that have the name specified.
            Multiple values are returned in the case that a task is used
            multiple times with different labels.
        """
        results = []
        for task in self._taskToQuantumNode:
            split = task.taskName.split(".")
            if split[-1] == taskName:
                results.append(task)
        return results

    def findTaskDefByLabel(self, label: str) -> TaskDef | None:
        """Determine which `TaskDef` objects in this graph are associated
        with a `str` representing a tasks label.

        Parameters
        ----------
        label : `str`
            Name of a task to search for.

        Returns
        -------
        result : `TaskDef`
            `TaskDef` objects that has the specified label.
        """
        for task in self._taskToQuantumNode:
            if label == task.label:
                return task
        return None

    def findQuantaWithDSType(self, datasetTypeName: DatasetTypeName) -> set[Quantum]:
        r"""Return all the `~lsst.daf.butler.Quantum` that contain a specified
        `DatasetTypeName`.

        Parameters
        ----------
        datasetTypeName : `str`
            The name of the dataset type to search for as a string,
            can also accept a `DatasetTypeName` which is a `~typing.NewType` of
            `str` for type safety in static type checking.

        Returns
        -------
        result : `set` of `QuantumNode` objects
            A `set` of `QuantumNode`\s that contain specified
            `DatasetTypeName`.

        Raises
        ------
        KeyError
            Raised if the `DatasetTypeName` is not part of the `QuantumGraph`.
        """
        tasks = self._datasetDict.getAll(datasetTypeName)
        result: set[Quantum] = set()
        result = result.union(quantum for task in tasks for quantum in self.getQuantaForTask(task))
        return result

    def checkQuantumInGraph(self, quantum: Quantum) -> bool:
        """Check if specified quantum appears in the graph as part of a node.

        Parameters
        ----------
        quantum : `lsst.daf.butler.Quantum`
            The quantum to search for.

        Returns
        -------
        in_graph : `bool`
            The result of searching for the quantum.
        """
        return any(quantum == node.quantum for node in self)

    def writeDotGraph(self, output: str | io.BufferedIOBase) -> None:
        """Write out the graph as a dot graph.

        Parameters
        ----------
        output : `str` or `io.BufferedIOBase`
            Either a filesystem path to write to, or a file handle object.
        """
        write_dot(self._connectedQuanta, output)

    def subset(self: _T, nodes: QuantumNode | Iterable[QuantumNode]) -> _T:
        """Create a new graph object that contains the subset of the nodes
        specified as input. Node number is preserved.

        Parameters
        ----------
        nodes : `QuantumNode` or iterable of `QuantumNode`
            Nodes from which to create subset.

        Returns
        -------
        graph : instance of graph type
            An instance of the type from which the subset was created.
        """
        if not isinstance(nodes, Iterable):
            nodes = (nodes,)
        quantumSubgraph = self._connectedQuanta.subgraph(nodes).nodes
        quantumMap = defaultdict(set)

        dataset_type_names: set[str] = set()
        node: QuantumNode
        for node in quantumSubgraph:
            quantumMap[node.taskDef].add(node.quantum)
            dataset_type_names.update(
                dstype.name
                for dstype in chain(
                    node.quantum.inputs.keys(), node.quantum.outputs.keys(), node.quantum.initInputs.keys()
                )
            )

        # May need to trim dataset types from registryDatasetTypes.
        for taskDef in quantumMap:
            if refs := self.initOutputRefs(taskDef):
                dataset_type_names.update(ref.datasetType.name for ref in refs)
        dataset_type_names.update(ref.datasetType.name for ref in self._globalInitOutputRefs)
        registryDatasetTypes = [
            dstype for dstype in self._registryDatasetTypes if dstype.name in dataset_type_names
        ]

        # convert to standard dict to prevent accidental key insertion
        quantumDict: dict[TaskDef, set[Quantum]] = dict(quantumMap.items())
        # Create an empty graph, and then populate it with custom mapping
        newInst = type(self)({}, universe=self._universe)
        # TODO: Do we need to copy initInputs/initOutputs?
        newInst._buildGraphs(
            quantumDict,
            _quantumToNodeId={n.quantum: n.nodeId for n in nodes},
            _buildId=self._buildId,
            metadata=self._metadata,
            universe=self._universe,
            globalInitOutputs=self._globalInitOutputRefs,
            registryDatasetTypes=registryDatasetTypes,
        )
        return newInst

    def subsetToConnected(self: _T) -> tuple[_T, ...]:
        """Generate a list of subgraphs where each is connected.

        Returns
        -------
        result : `list` of `QuantumGraph`
            A list of graphs that are each connected.
        """
        return tuple(
            self.subset(connectedSet)
            for connectedSet in nx.weakly_connected_components(self._connectedQuanta)
        )

    def determineInputsToQuantumNode(self, node: QuantumNode) -> set[QuantumNode]:
        """Return a set of `QuantumNode` that are direct inputs to a specified
        node.

        Parameters
        ----------
        node : `QuantumNode`
            The node of the graph for which inputs are to be determined.

        Returns
        -------
        inputs : `set` of `QuantumNode`
            All the nodes that are direct inputs to specified node.
        """
        return set(self._connectedQuanta.predecessors(node))

    def determineOutputsOfQuantumNode(self, node: QuantumNode) -> set[QuantumNode]:
        """Return a set of `QuantumNode` that are direct outputs of a specified
        node.

        Parameters
        ----------
        node : `QuantumNode`
            The node of the graph for which outputs are to be determined.

        Returns
        -------
        outputs : `set` of `QuantumNode`
            All the nodes that are direct outputs to specified node.
        """
        return set(self._connectedQuanta.successors(node))

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
            All the nodes that are directly connected to specified node.
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
            The node for which all ancestors are to be determined.

        Returns
        -------
        ancestors : graph of `QuantumNode`
            Graph of node and all of its ancestors.
        """
        predecessorNodes = nx.ancestors(self._connectedQuanta, node)
        predecessorNodes.add(node)
        return self.subset(predecessorNodes)

    def findCycle(self) -> list[tuple[QuantumNode, QuantumNode]]:
        """Check a graph for the presense of cycles and returns the edges of
        any cycles found, or an empty list if there is no cycle.

        Returns
        -------
        result : `list` of `tuple` of  [ `QuantumNode`, `QuantumNode` ]
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
        uri : convertible to `~lsst.resources.ResourcePath`
            URI to where the graph should be saved.
        """
        buffer = self._buildSaveObject()
        path = ResourcePath(uri)
        if path.getExtension() not in (".qgraph"):
            raise TypeError(f"Can currently only save a graph in qgraph format not {uri}")
        path.write(buffer)  # type: ignore  # Ignore because bytearray is safe to use in place of bytes

    @property
    def metadata(self) -> MappingProxyType[str, Any]:
        """Extra data carried with the graph (mapping [`str`] or `None`).

        The mapping is a dynamic view of this object's metadata. Values should
        be able to be serialized in JSON.
        """
        return MappingProxyType(self._metadata)

    def get_init_input_refs(self, task_label: str) -> list[DatasetRef]:
        """Return the DatasetRefs for the given task's init inputs.

        Parameters
        ----------
        task_label : `str`
            Label of the task.

        Returns
        -------
        refs : `list` [ `lsst.daf.butler.DatasetRef` ]
            Dataset references.  Guaranteed to be a new list, not internal
            state.
        """
        return list(self._initInputRefs.get(task_label, ()))

    def get_init_output_refs(self, task_label: str) -> list[DatasetRef]:
        """Return the DatasetRefs for the given task's init outputs.

        Parameters
        ----------
        task_label : `str`
            Label of the task.

        Returns
        -------
        refs : `list` [ `lsst.daf.butler.DatasetRef` ]
            Dataset references.  Guaranteed to be a new list, not internal
            state.
        """
        return list(self._initOutputRefs.get(task_label, ()))

    def initInputRefs(self, taskDef: TaskDef) -> list[DatasetRef] | None:
        """Return DatasetRefs for a given task InitInputs.

        Parameters
        ----------
        taskDef : `TaskDef`
            Task definition structure.

        Returns
        -------
        refs : `list` [ `~lsst.daf.butler.DatasetRef` ] or `None`
            DatasetRef for the task InitInput, can be `None`. This can return
            either resolved or non-resolved reference.
        """
        return self._initInputRefs.get(taskDef.label)

    def initOutputRefs(self, taskDef: TaskDef) -> list[DatasetRef] | None:
        """Return DatasetRefs for a given task InitOutputs.

        Parameters
        ----------
        taskDef : `TaskDef`
            Task definition structure.

        Returns
        -------
        refs : `list` [ `~lsst.daf.butler.DatasetRef` ] or `None`
            DatasetRefs for the task InitOutput, can be `None`. This can return
            either resolved or non-resolved reference. Resolved reference will
            match Quantum's initInputs if this is an intermediate dataset type.
        """
        return self._initOutputRefs.get(taskDef.label)

    def globalInitOutputRefs(self) -> list[DatasetRef]:
        """Return DatasetRefs for global InitOutputs.

        Returns
        -------
        refs : `list` [ `~lsst.daf.butler.DatasetRef` ]
            DatasetRefs for global InitOutputs.
        """
        return self._globalInitOutputRefs

    def registryDatasetTypes(self) -> list[DatasetType]:
        """Return dataset types used by this graph, their definitions match
        dataset types from registry.

        Returns
        -------
        refs : `list` [ `~lsst.daf.butler.DatasetType` ]
            Dataset types for this graph.
        """
        return self._registryDatasetTypes

    @classmethod
    def loadUri(
        cls,
        uri: ResourcePathExpression,
        universe: DimensionUniverse | None = None,
        nodes: Iterable[uuid.UUID | str] | None = None,
        graphID: BuildId | None = None,
        minimumVersion: int = 3,
    ) -> QuantumGraph:
        """Read `QuantumGraph` from a URI.

        Parameters
        ----------
        uri : convertible to `~lsst.resources.ResourcePath`
            URI from where to load the graph.
        universe : `~lsst.daf.butler.DimensionUniverse`, optional
            If `None` it is loaded from the `QuantumGraph`
            saved structure. If supplied, the
            `~lsst.daf.butler.DimensionUniverse` from the loaded `QuantumGraph`
            will be validated against the supplied argument for compatibility.
        nodes : iterable of [ `uuid.UUID` | `str` ] or `None`
            UUIDs that correspond to nodes in the graph. If specified, only
            these nodes will be loaded. Defaults to None, in which case all
            nodes will be loaded.
        graphID : `str` or `None`
            If specified this ID is verified against the loaded graph prior to
            loading any Nodes. This defaults to None in which case no
            validation is done.
        minimumVersion : `int`
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
            Raised if file contains instance of a type other than
            `QuantumGraph`.
        ValueError
            Raised if one or more of the nodes requested is not in the
            `QuantumGraph` or if graphID parameter does not match the graph
            being loaded or if the supplied uri does not point at a valid
            `QuantumGraph` save file.
        RuntimeError
            Raise if Supplied `~lsst.daf.butler.DimensionUniverse` is not
            compatible with the `~lsst.daf.butler.DimensionUniverse` saved in
            the graph.
        """
        uri = ResourcePath(uri)
        if uri.getExtension() in {".qgraph"}:
            with LoadHelper(uri, minimumVersion) as loader:
                qgraph = loader.load(universe, nodes, graphID)
        else:
            raise ValueError(f"Only know how to handle files saved as `.qgraph`, not {uri}")
        if not isinstance(qgraph, QuantumGraph):
            raise TypeError(f"QuantumGraph file {uri} contains unexpected object type: {type(qgraph)}")
        return qgraph

    @classmethod
    def readHeader(cls, uri: ResourcePathExpression, minimumVersion: int = 3) -> str | None:
        """Read the header of a `QuantumGraph` pointed to by the uri parameter
        and return it as a string.

        Parameters
        ----------
        uri : convertible to `~lsst.resources.ResourcePath`
            The location of the `QuantumGraph` to load. If the argument is a
            string, it must correspond to a valid
            `~lsst.resources.ResourcePath` path.
        minimumVersion : `int`
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
            Raised if the extension of the file specified by uri is not a
            `QuantumGraph` extension.
        """
        uri = ResourcePath(uri)
        if uri.getExtension() in {".qgraph"}:
            return LoadHelper(uri, minimumVersion).readHeader()
        else:
            raise ValueError("Only know how to handle files saved as `.qgraph`")

    def buildAndPrintHeader(self) -> None:
        """Create a header that would be used in a save of this object and
        prints it out to standard out.
        """
        _, header = self._buildSaveObject(returnHeader=True)
        print(json.dumps(header))

    def save(self, file: BinaryIO) -> None:
        """Save QuantumGraph to a file.

        Parameters
        ----------
        file : `io.BufferedIOBase`
            File to write data open in binary mode.
        """
        buffer = self._buildSaveObject()
        file.write(buffer)  # type: ignore # Ignore because bytearray is safe to use in place of bytes

    def _buildSaveObject(self, returnHeader: bool = False) -> bytearray | tuple[bytearray, dict]:
        thing = PersistenceContextVars()
        result = thing.run(self._buildSaveObjectImpl, returnHeader)
        return result

    def _buildSaveObjectImpl(self, returnHeader: bool = False) -> bytearray | tuple[bytearray, dict]:
        # make some containers
        jsonData: deque[bytes] = deque()
        # node map is a list because json does not accept mapping keys that
        # are not strings, so we store a list of key, value pairs that will
        # be converted to a mapping on load
        nodeMap = []
        taskDefMap = {}
        headerData: dict[str, Any] = {}

        # Store the QuantumGraph BuildId, this will allow validating BuildIds
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
            taskDescription: dict[str, Any] = {}
            # save the fully qualified name.
            taskDescription["taskName"] = get_full_type_name(taskDef.taskClass)
            # save the config as a text stream that will be un-persisted on the
            # other end
            stream = io.StringIO()
            taskDef.config.saveToStream(stream)
            taskDescription["config"] = stream.getvalue()
            taskDescription["label"] = taskDef.label
            if (refs := self._initInputRefs.get(taskDef.label)) is not None:
                taskDescription["initInputRefs"] = [ref.to_json() for ref in refs]
            if (refs := self._initOutputRefs.get(taskDef.label)) is not None:
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
            dump = lzma.compress(json.dumps(taskDescription).encode(), preset=2)
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

            dump = lzma.compress(simpleNode.model_dump_json().encode(), preset=2)
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
            key: value.model_dump()
            for key, value in dimAccumulator.makeSerializedDimensionRecordMapping().items()
        }

        # need to serialize this as a series of key,value tuples because of
        # a limitation on how json cant do anything but strings as keys
        headerData["Nodes"] = nodeMap

        if self._globalInitOutputRefs:
            headerData["GlobalInitOutputRefs"] = [ref.to_json() for ref in self._globalInitOutputRefs]

        if self._registryDatasetTypes:
            headerData["RegistryDatasetTypes"] = [dstype.to_json() for dstype in self._registryDatasetTypes]

        # dump the headerData to json
        header_encode = lzma.compress(json.dumps(headerData).encode())

        # record the sizes as 2 unsigned long long numbers for a total of 16
        # bytes
        save_bytes = struct.pack(STRUCT_FMT_BASE, SAVE_VERSION)

        fmt_string = DESERIALIZER_MAP[SAVE_VERSION].FMT_STRING()
        map_lengths = struct.pack(fmt_string, len(header_encode))

        # write each component of the save out in a deterministic order
        buffer = bytearray()
        buffer.extend(MAGIC_BYTES)
        buffer.extend(save_bytes)
        buffer.extend(map_lengths)
        buffer.extend(header_encode)
        # Iterate over the length of jsonData, and for each element pop the
        # leftmost element off the deque and write it out. This is to save
        # memory, as the memory is added to the buffer object, it is removed
        # from from the container.
        #
        # Only this section needs to worry about memory pressure because
        # everything else written to the buffer prior to this data is
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
        universe: DimensionUniverse | None = None,
        nodes: Iterable[uuid.UUID] | None = None,
        graphID: BuildId | None = None,
        minimumVersion: int = 3,
    ) -> QuantumGraph:
        """Read `QuantumGraph` from a file that was made by `save`.

        Parameters
        ----------
        file : `io.IO` of bytes
            File with data open in binary mode.
        universe : `~lsst.daf.butler.DimensionUniverse`, optional
            If `None` it is loaded from the `QuantumGraph`
            saved structure. If supplied, the
            `~lsst.daf.butler.DimensionUniverse` from the loaded `QuantumGraph`
            will be validated against the supplied argument for compatibility.
        nodes : iterable of `uuid.UUID` or `None`
            UUIDs that correspond to nodes in the graph. If specified, only
            these nodes will be loaded. Defaults to None, in which case all
            nodes will be loaded.
        graphID : `str` or `None`
            If specified this ID is verified against the loaded graph prior to
            loading any Nodes. This defaults to None in which case no
            validation is done.
        minimumVersion : `int`
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
            Raised if data contains instance of a type other than
            `QuantumGraph`.
        ValueError
            Raised if one or more of the nodes requested is not in the
            `QuantumGraph` or if graphID parameter does not match the graph
            being loaded or if the supplied uri does not point at a valid
            `QuantumGraph` save file.
        """
        with LoadHelper(file, minimumVersion) as loader:
            qgraph = loader.load(universe, nodes, graphID)
        if not isinstance(qgraph, QuantumGraph):
            raise TypeError(f"QuantumGraph file contains unexpected object type: {type(qgraph)}")
        return qgraph

    def iterTaskGraph(self) -> Generator[TaskDef, None, None]:
        """Iterate over the `taskGraph` attribute in topological order.

        Yields
        ------
        taskDef : `TaskDef`
            `TaskDef` objects in topological order.
        """
        yield from nx.topological_sort(self.taskGraph)

    def updateRun(self, run: str, *, metadata_key: str | None = None, update_graph_id: bool = False) -> None:
        """Change output run and dataset ID for each output dataset.

        Parameters
        ----------
        run : `str`
            New output run name.
        metadata_key : `str` or `None`
            Specifies matadata key corresponding to output run name to update
            with new run name. If `None` or if metadata is missing it is not
            updated. If metadata is present but key is missing, it will be
            added.
        update_graph_id : `bool`, optional
            If `True` then also update graph ID with a new unique value.
        """
        dataset_id_map: dict[DatasetId, DatasetId] = {}

        def _update_output_refs(
            refs: Iterable[DatasetRef], run: str, dataset_id_map: MutableMapping[DatasetId, DatasetId]
        ) -> Iterator[DatasetRef]:
            """Update a collection of `~lsst.daf.butler.DatasetRef` with new
            run and dataset IDs.
            """
            for ref in refs:
                new_ref = ref.replace(run=run)
                dataset_id_map[ref.id] = new_ref.id
                yield new_ref

        def _update_intermediate_refs(
            refs: Iterable[DatasetRef], run: str, dataset_id_map: Mapping[DatasetId, DatasetId]
        ) -> Iterator[DatasetRef]:
            """Update intermediate references with new run and IDs. Only the
            references that appear in ``dataset_id_map`` are updated, others
            are returned unchanged.
            """
            for ref in refs:
                if dataset_id := dataset_id_map.get(ref.id):
                    ref = ref.replace(run=run, id=dataset_id)
                yield ref

        # Replace quantum output refs first.
        for node in self._connectedQuanta:
            quantum = node.quantum
            outputs = {
                dataset_type: tuple(_update_output_refs(refs, run, dataset_id_map))
                for dataset_type, refs in quantum.outputs.items()
            }
            updated_quantum = Quantum(
                taskName=quantum.taskName,
                dataId=quantum.dataId,
                initInputs=quantum.initInputs,
                inputs=quantum.inputs,
                outputs=outputs,
                datastore_records=quantum.datastore_records,
            )
            node._replace_quantum(updated_quantum)

        self._initOutputRefs = {
            task_def: list(_update_output_refs(refs, run, dataset_id_map))
            for task_def, refs in self._initOutputRefs.items()
        }
        self._globalInitOutputRefs = list(
            _update_output_refs(self._globalInitOutputRefs, run, dataset_id_map)
        )

        # Update all intermediates from their matching outputs.
        for node in self._connectedQuanta:
            quantum = node.quantum
            inputs = {
                dataset_type: tuple(_update_intermediate_refs(refs, run, dataset_id_map))
                for dataset_type, refs in quantum.inputs.items()
            }
            initInputs = list(_update_intermediate_refs(quantum.initInputs.values(), run, dataset_id_map))

            updated_quantum = Quantum(
                taskName=quantum.taskName,
                dataId=quantum.dataId,
                initInputs=initInputs,
                inputs=inputs,
                outputs=quantum.outputs,
                datastore_records=quantum.datastore_records,
            )
            node._replace_quantum(updated_quantum)

        self._initInputRefs = {
            task_def: list(_update_intermediate_refs(refs, run, dataset_id_map))
            for task_def, refs in self._initInputRefs.items()
        }

        if update_graph_id:
            self._buildId = BuildId(f"{time.time()}-{os.getpid()}")

        # Update run if given.
        if metadata_key is not None:
            self._metadata[metadata_key] = run

    @property
    def graphID(self) -> BuildId:
        """The ID generated by the graph at construction time (`str`)."""
        return self._buildId

    @property
    def universe(self) -> DimensionUniverse:
        """Dimension universe associated with this graph
        (`~lsst.daf.butler.DimensionUniverse`).
        """
        return self._universe

    def __iter__(self) -> Generator[QuantumNode, None, None]:
        yield from nx.topological_sort(self._connectedQuanta)

    def __len__(self) -> int:
        return self._count

    def __contains__(self, node: QuantumNode) -> bool:
        return self._connectedQuanta.has_node(node)

    def __getstate__(self) -> dict:
        """Store a compact form of the graph as a list of graph nodes, and a
        tuple of task labels and task configs. The full graph can be
        reconstructed with this information, and it preserves the ordering of
        the graph nodes.
        """
        universe: DimensionUniverse | None = None
        for node in self:
            dId = node.quantum.dataId
            if dId is None:
                continue
            universe = dId.universe
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

    def getSummary(self) -> QgraphSummary:
        """Create summary of graph.

        Returns
        -------
        summary : `QgraphSummary`
           Summary of QuantumGraph.
        """
        inCollection = self.metadata.get("input", None)
        if isinstance(inCollection, str):
            inCollection = [inCollection]
        summary = QgraphSummary(
            graphID=self.graphID,
            cmdLine=self.metadata.get("full_command", None),
            creationUTC=self.metadata.get("time", None),
            inputCollection=inCollection,
            outputCollection=self.metadata.get("output", None),
            outputRun=self.metadata.get("output_run", None),
        )
        for q in self:
            qts = summary.qgraphTaskSummaries.setdefault(
                q.taskDef.label, QgraphTaskSummary(taskLabel=q.taskDef.label)
            )
            qts.numQuanta += 1

            for k in q.quantum.inputs.keys():
                qts.numInputs[k.name] += 1

            for k in q.quantum.outputs.keys():
                qts.numOutputs[k.name] += 1

        return summary

    def make_init_qbb(
        self,
        butler_config: Config | ResourcePathExpression,
        *,
        config_search_paths: Iterable[str] | None = None,
    ) -> QuantumBackedButler:
        """Construct an quantum-backed butler suitable for reading and writing
        init input and init output datasets, respectively.

        This requires the full graph to have been loaded.

        Parameters
        ----------
        butler_config : `~lsst.daf.butler.Config` or \
                `~lsst.resources.ResourcePathExpression`
            A butler repository root, configuration filename, or configuration
            instance.
        config_search_paths : `~collections.abc.Iterable` [ `str` ], optional
            Additional search paths for butler configuration.

        Returns
        -------
        qbb : `~lsst.daf.butler.QuantumBackedButler`
            A limited butler that can ``get`` init-input datasets and ``put``
            init-output datasets.
        """
        universe = self.universe
        # Collect all init input/output dataset IDs.
        predicted_inputs: set[DatasetId] = set()
        predicted_outputs: set[DatasetId] = set()
        pipeline_graph = self.pipeline_graph
        for task_label in pipeline_graph.tasks:
            predicted_inputs.update(ref.id for ref in self.get_init_input_refs(task_label))
            predicted_outputs.update(ref.id for ref in self.get_init_output_refs(task_label))
        predicted_outputs.update(ref.id for ref in self.globalInitOutputRefs())
        # remove intermediates from inputs
        predicted_inputs -= predicted_outputs
        # Very inefficient way to extract datastore records from quantum graph,
        # we have to scan all quanta and look at their datastore records.
        datastore_records: dict[str, DatastoreRecordData] = {}
        for quantum_node in self:
            for store_name, records in quantum_node.quantum.datastore_records.items():
                subset = records.subset(predicted_inputs)
                if subset is not None:
                    datastore_records.setdefault(store_name, DatastoreRecordData()).update(subset)

        dataset_types = {dstype.name: dstype for dstype in self.registryDatasetTypes()}
        # Make butler from everything.
        return QuantumBackedButler.from_predicted(
            config=butler_config,
            predicted_inputs=predicted_inputs,
            predicted_outputs=predicted_outputs,
            dimensions=universe,
            datastore_records=datastore_records,
            search_paths=list(config_search_paths) if config_search_paths is not None else None,
            dataset_types=dataset_types,
        )

    def write_init_outputs(self, butler: LimitedButler, skip_existing: bool = True) -> None:
        """Write the init-output datasets for all tasks in the quantum graph.

        Parameters
        ----------
        butler : `lsst.daf.butler.LimitedButler`
            A limited butler data repository client.
        skip_existing : `bool`, optional
            If `True` (default) ignore init-outputs that already exist.  If
            `False`, raise.

        Raises
        ------
        lsst.daf.butler.registry.ConflictingDefinitionError
            Raised if an init-output dataset already exists and
            ``skip_existing=False``.
        """
        # Extract init-input and init-output refs from the QG.
        input_refs: dict[str, DatasetRef] = {}
        output_refs: dict[str, DatasetRef] = {}
        for task_node in self.pipeline_graph.tasks.values():
            input_refs.update(
                {ref.datasetType.name: ref for ref in self.get_init_input_refs(task_node.label)}
            )
            output_refs.update(
                {
                    ref.datasetType.name: ref
                    for ref in self.get_init_output_refs(task_node.label)
                    if ref.datasetType.name != task_node.init.config_output.dataset_type_name
                }
            )
        for ref, is_stored in butler.stored_many(output_refs.values()).items():
            if is_stored:
                if not skip_existing:
                    raise ConflictingDefinitionError(f"Init-output dataset {ref} already exists.")
                # We'll `put` whatever's left in output_refs at the end.
                del output_refs[ref.datasetType.name]
        # Instantiate tasks, reading overall init-inputs and gathering
        # init-output in-memory objects.
        init_outputs: list[tuple[Any, DatasetType]] = []
        self.pipeline_graph.instantiate_tasks(
            get_init_input=lambda dataset_type: butler.get(
                input_refs[dataset_type.name].overrideStorageClass(dataset_type.storageClass)
            ),
            init_outputs=init_outputs,
        )
        # Write init-outputs that weren't already present.
        for obj, dataset_type in init_outputs:
            if new_ref := output_refs.get(dataset_type.name):
                assert new_ref.datasetType.storageClass_name == dataset_type.storageClass_name, (
                    "QG init refs should use task connection storage classes."
                )
                butler.put(obj, new_ref)

    def write_configs(self, butler: LimitedButler, compare_existing: bool = True) -> None:
        """Write the config datasets for all tasks in the quantum graph.

        Parameters
        ----------
        butler : `lsst.daf.butler.LimitedButler`
            A limited butler data repository client.
        compare_existing : `bool`, optional
            If `True` check configs that already exist for consistency.  If
            `False`, always raise if configs already exist.

        Raises
        ------
        lsst.daf.butler.registry.ConflictingDefinitionError
            Raised if an config dataset already exists and
            ``compare_existing=False``, or if the existing config is not
            consistent with the config in the quantum graph.
        """
        to_put: list[tuple[PipelineTaskConfig, DatasetRef]] = []
        for task_node in self.pipeline_graph.tasks.values():
            dataset_type_name = task_node.init.config_output.dataset_type_name
            (ref,) = [  # noqa: UP027
                ref
                for ref in self.get_init_output_refs(task_node.label)
                if ref.datasetType.name == dataset_type_name
            ]
            try:
                old_config = butler.get(ref)
            except (LookupError, FileNotFoundError):
                old_config = None
            if old_config is not None:
                if not compare_existing:
                    raise ConflictingDefinitionError(f"Config dataset {ref} already exists.")
                if not task_node.config.compare(old_config, shortcut=False, output=log_config_mismatch):
                    raise ConflictingDefinitionError(
                        f"Config does not match existing task config {dataset_type_name!r} in "
                        "butler; tasks configurations must be consistent within the same run collection."
                    )
            else:
                to_put.append((task_node.config, ref))
        # We do writes at the end to minimize the mess we leave behind when we
        # raise an exception.
        for config, ref in to_put:
            butler.put(config, ref)

    def write_packages(self, butler: LimitedButler, compare_existing: bool = True) -> None:
        """Write the 'packages' dataset for the currently-active software
        versions.

        Parameters
        ----------
        butler : `lsst.daf.butler.LimitedButler`
            A limited butler data repository client.
        compare_existing : `bool`, optional
            If `True` check packages that already exist for consistency.  If
            `False`, always raise if the packages dataset already exists.

        Raises
        ------
        lsst.daf.butler.registry.ConflictingDefinitionError
            Raised if the packages dataset already exists and is not consistent
            with the current packages.
        """
        new_packages = Packages.fromSystem()
        (ref,) = self.globalInitOutputRefs()
        try:
            packages = butler.get(ref)
        except (LookupError, FileNotFoundError):
            packages = None
        if packages is not None:
            if not compare_existing:
                raise ConflictingDefinitionError(f"Packages dataset {ref} already exists.")
            if compare_packages(packages, new_packages):
                # have to remove existing dataset first; butler has no
                # replace option.
                butler.pruneDatasets([ref], unstore=True, purge=True)
                butler.put(packages, ref)
        else:
            butler.put(new_packages, ref)

    def init_output_run(self, butler: LimitedButler, existing: bool = True) -> None:
        """Initialize a new output RUN collection by writing init-output
        datasets (including configs and packages).

        Parameters
        ----------
        butler : `lsst.daf.butler.LimitedButler`
            A limited butler data repository client.
        existing : `bool`, optional
            If `True` check or ignore outputs that already exist.  If
            `False`, always raise if an output dataset already exists.

        Raises
        ------
        lsst.daf.butler.registry.ConflictingDefinitionError
            Raised if there are existing init output datasets, and either
            ``existing=False`` or their contents are not compatible with this
            graph.
        """
        self.write_configs(butler, compare_existing=existing)
        self.write_packages(butler, compare_existing=existing)
        self.write_init_outputs(butler, skip_existing=existing)

    def get_refs(
        self,
        *,
        include_init_inputs: bool = False,
        include_inputs: bool = False,
        include_intermediates: bool | None = None,
        include_init_outputs: bool = False,
        include_outputs: bool = False,
        conform_outputs: bool = True,
    ) -> tuple[set[DatasetRef], dict[str, DatastoreRecordData]]:
        """Get the requested dataset refs from the graph.

        Parameters
        ----------
        include_init_inputs : `bool`, optional
            Include init inputs.
        include_inputs : `bool`, optional
            Include inputs.
        include_intermediates : `bool` or `None`, optional
            If `None`, no special handling for intermediates is performed.
            If `True` intermediates are calculated even if other flags
            do not request datasets. If `False` intermediates will be removed
            from any results.
        include_init_outputs : `bool`, optional
            Include init outpus.
        include_outputs : `bool`, optional
            Include outputs.
        conform_outputs : `bool`, optional
            Whether any outputs found should have their dataset types conformed
            with the registry dataset types.

        Returns
        -------
        refs : `set` [ `lsst.daf.butler.DatasetRef` ]
            The requested dataset refs found in the graph.
        datastore_records : `dict` [ `str`, \
                `lsst.daf.butler.datastore.record_data.DatastoreRecordData` ]
            Any datastore records found.

        Notes
        -----
        Conforming and requesting inputs and outputs can result in the same
        dataset appearing in the results twice with differing storage classes.
        """
        datastore_records: dict[str, DatastoreRecordData] = {}
        init_input_refs: set[DatasetRef] = set()
        init_output_refs: set[DatasetRef] = set(self.globalInitOutputRefs())

        if include_intermediates is True:
            # Need to enable inputs and outputs even if not explicitly
            # requested.
            request_include_init_inputs = True
            request_include_inputs = True
            request_include_init_outputs = True
            request_include_outputs = True
        else:
            request_include_init_inputs = include_init_inputs
            request_include_inputs = include_inputs
            request_include_init_outputs = include_init_outputs
            request_include_outputs = include_outputs

        if request_include_init_inputs or request_include_init_outputs:
            for task_def in self.iterTaskGraph():
                if request_include_init_inputs:
                    if in_refs := self.initInputRefs(task_def):
                        init_input_refs.update(in_refs)
                if request_include_init_outputs:
                    if out_refs := self.initOutputRefs(task_def):
                        init_output_refs.update(out_refs)

        input_refs: set[DatasetRef] = set()
        output_refs: set[DatasetRef] = set()

        for qnode in self:
            if request_include_inputs:
                for other_refs in qnode.quantum.inputs.values():
                    input_refs.update(other_refs)
                # Inputs can come with datastore records.
                for store_name, records in qnode.quantum.datastore_records.items():
                    datastore_records.setdefault(store_name, DatastoreRecordData()).update(records)
            if request_include_outputs:
                for other_refs in qnode.quantum.outputs.values():
                    output_refs.update(other_refs)

        # Intermediates are the intersection of inputs and outputs. Must do
        # this analysis before conforming since dataset type changes will
        # change set membership.
        inter_msg = ""
        intermediates = set()
        if include_intermediates is not None:
            intermediates = (input_refs | init_input_refs) & (output_refs | init_output_refs)

        if include_intermediates is False:
            # Remove intermediates from results.
            init_input_refs -= intermediates
            input_refs -= intermediates
            init_output_refs -= intermediates
            output_refs -= intermediates
            inter_msg = f"; Intermediates removed: {len(intermediates)}"
            intermediates = set()
        elif include_intermediates is True:
            # Do not mention intermediates if all the input/output flags
            # would have resulted in them anyhow.
            if (
                (request_include_init_inputs is not include_init_inputs)
                or (request_include_inputs is not include_inputs)
                or (request_include_init_outputs is not include_init_outputs)
                or (request_include_outputs is not include_outputs)
            ):
                inter_msg = f"; including intermediates: {len(intermediates)}"

        # Assign intermediates to the relevant category.
        if not include_init_inputs:
            init_input_refs &= intermediates
        if not include_inputs:
            input_refs &= intermediates
        if not include_init_outputs:
            init_output_refs &= intermediates
        if not include_outputs:
            output_refs &= intermediates

        # Conforming can result in an input ref and an output ref appearing
        # in the returned results that are identical apart from storage class.
        if conform_outputs:
            # Get data repository definitions from the QuantumGraph; these can
            # have different storage classes than those in the quanta.
            dataset_types = {dstype.name: dstype for dstype in self.registryDatasetTypes()}

            def _update_ref(ref: DatasetRef) -> DatasetRef:
                internal_dataset_type = dataset_types.get(ref.datasetType.name, ref.datasetType)
                if internal_dataset_type.storageClass_name != ref.datasetType.storageClass_name:
                    ref = ref.replace(storage_class=internal_dataset_type.storageClass_name)
                return ref

            # Convert output_refs to the data repository storage classes, too.
            output_refs = {_update_ref(ref) for ref in output_refs}
            init_output_refs = {_update_ref(ref) for ref in init_output_refs}

        _LOG.verbose(
            "Found the following datasets. InitInputs: %d; Inputs: %d; InitOutputs: %s; Outputs: %d%s",
            len(init_input_refs),
            len(input_refs),
            len(init_output_refs),
            len(output_refs),
            inter_msg,
        )

        refs = input_refs | init_input_refs | init_output_refs | output_refs
        return refs, datastore_records
