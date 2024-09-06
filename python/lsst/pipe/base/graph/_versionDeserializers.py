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

__all__ = ("DESERIALIZER_MAP",)

import json
import lzma
import pickle
import struct
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from types import SimpleNamespace
from typing import TYPE_CHECKING, ClassVar, cast

import networkx as nx
from lsst.daf.butler import (
    DatasetRef,
    DatasetType,
    DimensionConfig,
    DimensionUniverse,
    Quantum,
    SerializedDimensionRecord,
)
from lsst.utils import doImportType

from ..config import PipelineTaskConfig
from ..pipeline import TaskDef
from ..pipelineTask import PipelineTask
from ._implDetails import DatasetTypeName, _DatasetTracker
from .quantumNode import QuantumNode, SerializedQuantumNode

if TYPE_CHECKING:
    from .graph import QuantumGraph


class StructSizeDescriptor:
    """Class level property. It exists to report the size
    (number of bytes) of whatever the formatter string is for a deserializer.
    """

    def __get__(self, inst: DeserializerBase | None, owner: type[DeserializerBase]) -> int:
        return struct.calcsize(owner.FMT_STRING())


@dataclass
class DeserializerBase(ABC):
    @classmethod
    @abstractmethod
    def FMT_STRING(cls) -> str:
        raise NotImplementedError("Base class does not implement this method")

    structSize: ClassVar[StructSizeDescriptor]

    preambleSize: int
    sizeBytes: bytes

    def __init_subclass__(cls) -> None:
        # attach the size decriptor
        cls.structSize = StructSizeDescriptor()
        super().__init_subclass__()

    def unpackHeader(self, rawHeader: bytes) -> str | None:
        """Transform the raw bytes corresponding to the header of a save into
        a string of the header information.

        Parameters
        ----------
        rawHeader : bytes
            The bytes that are to be parsed into the header information. These
            are the bytes after the preamble and structsize number of bytes
            and before the headerSize bytes.

        Returns
        -------
        header : `str` or `None`
            Header information as a string. Returns `None` if the save format
            has no header string implementation (such as save format 1 that is
            all pickle).
        """
        raise NotImplementedError("Base class does not implement this method")

    @property
    def headerSize(self) -> int:
        """Returns the number of bytes from the beginning of the file to the
        end of the metadata.
        """
        raise NotImplementedError("Base class does not implement this method")

    def readHeaderInfo(self, rawHeader: bytes) -> SimpleNamespace:
        """Parse the supplied raw bytes into the header information and
        byte ranges of specific TaskDefs and QuantumNodes.

        Parameters
        ----------
        rawHeader : bytes
            The bytes that are to be parsed into the header information. These
            are the bytes after the preamble and structsize number of bytes
            and before the headerSize bytes.
        """
        raise NotImplementedError("Base class does not implement this method")

    def constructGraph(
        self,
        nodes: set[uuid.UUID],
        _readBytes: Callable[[int, int], bytes],
        universe: DimensionUniverse | None = None,
    ) -> QuantumGraph:
        """Construct a graph from the deserialized information.

        Parameters
        ----------
        nodes : `set` of `uuid.UUID`
            The nodes to include in the graph.
        _readBytes : callable
            A callable that can be used to read bytes from the file handle.
            The callable will take two ints, start and stop, to use as the
            numerical bounds to read and returns a byte stream.
        universe : `~lsst.daf.butler.DimensionUniverse`
            The singleton of all dimensions known to the middleware registry.
        """
        raise NotImplementedError("Base class does not implement this method")

    def description(self) -> str:
        """Return the description of the serialized data format.

        Returns
        -------
        desc : `str`
            Description of serialized data format.
        """
        raise NotImplementedError("Base class does not implement this method")


Version1Description = """
The save file starts with the first few bytes corresponding to the magic bytes
in the QuantumGraph: `qgraph4\xf6\xe8\xa9`.

The next few bytes are 2 big endian unsigned 64 bit integers.

The first unsigned 64 bit integer corresponds to the number of bytes of a
python mapping of TaskDef labels to the byte ranges in the save file where the
definition can be loaded.

The second unsigned 64 bit integer corrresponds to the number of bytes of a
python mapping of QuantumGraph Node number to the byte ranges in the save file
where the node can be loaded. The byte range is indexed starting after
the `header` bytes of the magic bytes, size bytes, and bytes of the two
mappings.

Each of the above mappings are pickled and then lzma compressed, so to
deserialize the bytes, first lzma decompression must be performed and the
results passed to python pickle loader.

As stated above, each map contains byte ranges of the corresponding
datastructure. Theses bytes are also lzma compressed pickles, and should
be deserialized in a similar manner. The byte range is indexed starting after
the `header` bytes of the magic bytes, size bytes, and bytes of the two
mappings.

In addition to the the TaskDef byte locations, the TypeDef map also contains
an additional key '__GraphBuildID'. The value associated with this is the
unique id assigned to the graph at its creation time.
"""


@dataclass
class DeserializerV1(DeserializerBase):
    @classmethod
    def FMT_STRING(cls) -> str:
        return ">QQ"

    def __post_init__(self) -> None:
        self.taskDefMapSize, self.nodeMapSize = struct.unpack(self.FMT_STRING(), self.sizeBytes)

    @property
    def headerSize(self) -> int:
        return self.preambleSize + self.structSize + self.taskDefMapSize + self.nodeMapSize

    def readHeaderInfo(self, rawHeader: bytes) -> SimpleNamespace:
        returnValue = SimpleNamespace()
        returnValue.taskDefMap = pickle.loads(rawHeader[: self.taskDefMapSize])
        returnValue._buildId = returnValue.taskDefMap["__GraphBuildID"]
        returnValue.map = pickle.loads(rawHeader[self.taskDefMapSize :])
        returnValue.metadata = None
        self.returnValue = returnValue
        return returnValue

    def unpackHeader(self, rawHeader: bytes) -> str | None:
        return None

    def constructGraph(
        self,
        nodes: set[uuid.UUID],
        _readBytes: Callable[[int, int], bytes],
        universe: DimensionUniverse | None = None,
    ) -> QuantumGraph:
        # need to import here to avoid cyclic imports
        from . import QuantumGraph

        quanta: defaultdict[TaskDef, set[Quantum]] = defaultdict(set)
        quantumToNodeId: dict[Quantum, uuid.UUID] = {}
        loadedTaskDef = {}
        # loop over the nodes specified above
        for node in nodes:
            # Get the bytes to read from the map
            start, stop = self.returnValue.map[node]
            start += self.headerSize
            stop += self.headerSize

            # read the specified bytes, will be overloaded by subclasses
            # bytes are compressed, so decompress them
            dump = lzma.decompress(_readBytes(start, stop))

            # reconstruct node
            qNode = pickle.loads(dump)
            object.__setattr__(qNode, "nodeId", uuid.uuid4())

            # read the saved node, name. If it has been loaded, attach it, if
            # not read in the taskDef first, and then load it
            nodeTask = qNode.taskDef
            if nodeTask not in loadedTaskDef:
                # Get the byte ranges corresponding to this taskDef
                start, stop = self.returnValue.taskDefMap[nodeTask]
                start += self.headerSize
                stop += self.headerSize

                # load the taskDef, this method call will be overloaded by
                # subclasses.
                # bytes are compressed, so decompress them
                taskDef = pickle.loads(lzma.decompress(_readBytes(start, stop)))
                loadedTaskDef[nodeTask] = taskDef
            # Explicitly overload the "frozen-ness" of nodes to attach the
            # taskDef back into the un-persisted node
            object.__setattr__(qNode, "taskDef", loadedTaskDef[nodeTask])
            quanta[qNode.taskDef].add(qNode.quantum)

            # record the node for later processing
            quantumToNodeId[qNode.quantum] = qNode.nodeId

        # construct an empty new QuantumGraph object, and run the associated
        # creation method with the un-persisted data
        qGraph = object.__new__(QuantumGraph)
        qGraph._buildGraphs(
            quanta,
            _quantumToNodeId=quantumToNodeId,
            _buildId=self.returnValue._buildId,
            metadata=self.returnValue.metadata,
            universe=universe,
        )
        return qGraph

    def description(self) -> str:
        return Version1Description


Version2Description = """
The save file starts with the first few bytes corresponding to the magic bytes
in the QuantumGraph: `qgraph4\xf6\xe8\xa9`.

The next few bytes are a big endian unsigned long long.

The unsigned long long corresponds to the number of bytes of a python mapping
of header information. This mapping is encoded into json and then lzma
compressed, meaning the operations must be performed in the opposite order to
deserialize.

The json encoded header mapping contains 4 fields: TaskDefs, GraphBuildId,
Nodes, and Metadata.

The `TaskDefs` key corresponds to a value which is a mapping of Task label to
task data. The task data is a mapping of key to value, where the only key is
`bytes` and it corresponds to a tuple of a byte range of the start, stop
bytes (indexed after all the header bytes)

The `GraphBuildId` corresponds with a string that is the unique id assigned to
this graph when it was created.

The `Nodes` key is like the `TaskDefs` key except it corresponds to
QuantumNodes instead of TaskDefs. Another important difference is that JSON
formatting does not allow using numbers as keys, and this mapping is keyed by
the node number. Thus it is stored in JSON as two equal length lists, the first
being the keys, and the second the values associated with those keys.

The `Metadata` key is a mapping of strings to associated values. This metadata
may be anything that is important to be transported alongside the graph.

As stated above, each map contains byte ranges of the corresponding
datastructure. Theses bytes are also lzma compressed pickles, and should
be deserialized in a similar manner.
"""


@dataclass
class DeserializerV2(DeserializerBase):
    @classmethod
    def FMT_STRING(cls) -> str:
        return ">Q"

    def __post_init__(self) -> None:
        (self.mapSize,) = struct.unpack(self.FMT_STRING(), self.sizeBytes)

    @property
    def headerSize(self) -> int:
        return self.preambleSize + self.structSize + self.mapSize

    def readHeaderInfo(self, rawHeader: bytes) -> SimpleNamespace:
        uncompressedHeaderMap = self.unpackHeader(rawHeader)
        if uncompressedHeaderMap is None:
            raise ValueError(
                "This error is not possible because self.unpackHeader cannot return None,"
                " but is done to satisfy type checkers"
            )
        header = json.loads(uncompressedHeaderMap)
        returnValue = SimpleNamespace()
        returnValue.taskDefMap = header["TaskDefs"]
        returnValue._buildId = header["GraphBuildID"]
        returnValue.map = dict(header["Nodes"])
        returnValue.metadata = header["Metadata"]
        self.returnValue = returnValue
        return returnValue

    def unpackHeader(self, rawHeader: bytes) -> str | None:
        return lzma.decompress(rawHeader).decode()

    def constructGraph(
        self,
        nodes: set[uuid.UUID],
        _readBytes: Callable[[int, int], bytes],
        universe: DimensionUniverse | None = None,
    ) -> QuantumGraph:
        # need to import here to avoid cyclic imports
        from . import QuantumGraph

        quanta: defaultdict[TaskDef, set[Quantum]] = defaultdict(set)
        quantumToNodeId: dict[Quantum, uuid.UUID] = {}
        loadedTaskDef = {}
        # loop over the nodes specified above
        for node in nodes:
            # Get the bytes to read from the map
            start, stop = self.returnValue.map[node]["bytes"]
            start += self.headerSize
            stop += self.headerSize

            # read the specified bytes, will be overloaded by subclasses
            # bytes are compressed, so decompress them
            dump = lzma.decompress(_readBytes(start, stop))

            # reconstruct node
            qNode = pickle.loads(dump)
            object.__setattr__(qNode, "nodeId", uuid.uuid4())

            # read the saved node, name. If it has been loaded, attach it, if
            # not read in the taskDef first, and then load it
            nodeTask = qNode.taskDef
            if nodeTask not in loadedTaskDef:
                # Get the byte ranges corresponding to this taskDef
                start, stop = self.returnValue.taskDefMap[nodeTask]["bytes"]
                start += self.headerSize
                stop += self.headerSize

                # load the taskDef, this method call will be overloaded by
                # subclasses.
                # bytes are compressed, so decompress them
                taskDef = pickle.loads(lzma.decompress(_readBytes(start, stop)))
                loadedTaskDef[nodeTask] = taskDef
            # Explicitly overload the "frozen-ness" of nodes to attach the
            # taskDef back into the un-persisted node
            object.__setattr__(qNode, "taskDef", loadedTaskDef[nodeTask])
            quanta[qNode.taskDef].add(qNode.quantum)

            # record the node for later processing
            quantumToNodeId[qNode.quantum] = qNode.nodeId

        # construct an empty new QuantumGraph object, and run the associated
        # creation method with the un-persisted data
        qGraph = object.__new__(QuantumGraph)
        qGraph._buildGraphs(
            quanta,
            _quantumToNodeId=quantumToNodeId,
            _buildId=self.returnValue._buildId,
            metadata=self.returnValue.metadata,
            universe=universe,
        )
        return qGraph

    def description(self) -> str:
        return Version2Description


Version3Description = """
The save file starts with the first few bytes corresponding to the magic bytes
in the QuantumGraph: `qgraph4\xf6\xe8\xa9`.

The next few bytes are a big endian unsigned long long.

The unsigned long long corresponds to the number of bytes of a mapping
of header information. This mapping is encoded into json and then lzma
compressed, meaning the operations must be performed in the opposite order to
deserialize.

The json encoded header mapping contains 5 fields: GraphBuildId, TaskDefs,
Nodes, Metadata, and DimensionRecords.

The `GraphBuildId` key corresponds with a string that is the unique id assigned
to this graph when it was created.

The `TaskDefs` key corresponds to a value which is a mapping of Task label to
task data. The task data is a mapping of key to value. The keys of this mapping
are `bytes`, `inputs`, and `outputs`.

The  `TaskDefs` `bytes` key corresponds to a tuple of a byte range of the
start, stop bytes (indexed after all the header bytes).  This byte rage
corresponds to a lzma compressed json mapping. This mapping has keys of
`taskName`, corresponding to a fully qualified python class, `config` a
pex_config string that is used to configure the class, and `label` which
corresponds to a string that uniquely identifies the task within a given
execution pipeline.

The `TaskDefs` `inputs` key is associated with a list of tuples where each
tuple is a label of a task that is considered coming before a given task, and
the name of the dataset that is shared between the tasks (think node and edge
in a graph sense).

The `TaskDefs` `outputs` key is like inputs except the values in a list
correspond to all the output connections of a task.

The `Nodes` key is also a json mapping with keys corresponding to the UUIDs of
QuantumNodes. The values associated with these keys is another mapping with
the keys `bytes`, `inputs`, and `outputs`.

`Nodes` key `bytes` corresponds to a tuple of a byte range of the start, stop
bytes (indexed after all the header bytes). These bytes are a lzma compressed
json mapping which contains many sub elements, this mapping will be referred to
as the SerializedQuantumNode (related to the python class it corresponds to).

SerializedQUantumNodes have 3 keys, `quantum` corresponding to a json mapping
(described below) referred to as a SerializedQuantum, `taskLabel` a string
which corresponds to a label in the `TaskDefs` mapping, and `nodeId.

A SerializedQuantum has many keys; taskName, dataId, datasetTypeMapping,
initInputs, inputs, outputs, dimensionRecords.

like the `TaskDefs` key except it corresponds to
QuantumNodes instead of TaskDefs, and the keys of the mappings are string
representations of the UUIDs of the QuantumNodes.

The `Metadata` key is a mapping of strings to associated values. This metadata
may be anything that is important to be transported alongside the graph.

As stated above, each map contains byte ranges of the corresponding
datastructure. Theses bytes are also lzma compressed pickles, and should
be deserialized in a similar manner.
"""


@dataclass
class DeserializerV3(DeserializerBase):
    @classmethod
    def FMT_STRING(cls) -> str:
        return ">Q"

    def __post_init__(self) -> None:
        self.infoSize: int
        (self.infoSize,) = struct.unpack(self.FMT_STRING(), self.sizeBytes)

    @property
    def headerSize(self) -> int:
        return self.preambleSize + self.structSize + self.infoSize

    def readHeaderInfo(self, rawHeader: bytes) -> SimpleNamespace:
        uncompressedinfoMap = self.unpackHeader(rawHeader)
        assert uncompressedinfoMap is not None  # for python typing, this variant can't be None
        infoMap = json.loads(uncompressedinfoMap)
        infoMappings = SimpleNamespace()
        infoMappings.taskDefMap = infoMap["TaskDefs"]
        infoMappings._buildId = infoMap["GraphBuildID"]
        infoMappings.map = {uuid.UUID(k): v for k, v in infoMap["Nodes"]}
        infoMappings.metadata = infoMap["Metadata"]
        infoMappings.dimensionRecords = {}
        for k, v in infoMap["DimensionRecords"].items():
            infoMappings.dimensionRecords[int(k)] = SerializedDimensionRecord(**v)
        # This is important to be a get call here, so that it supports versions
        # of saved quantum graph that might not have a saved universe without
        # changing save format
        if (universeConfig := infoMap.get("universe")) is not None:
            universe = DimensionUniverse(config=DimensionConfig(universeConfig))
        else:
            universe = DimensionUniverse()
        infoMappings.universe = universe
        infoMappings.globalInitOutputRefs = []
        if (json_refs := infoMap.get("GlobalInitOutputRefs")) is not None:
            infoMappings.globalInitOutputRefs = [
                DatasetRef.from_json(json_ref, universe=universe) for json_ref in json_refs
            ]
        infoMappings.registryDatasetTypes = []
        if (json_refs := infoMap.get("RegistryDatasetTypes")) is not None:
            infoMappings.registryDatasetTypes = [
                DatasetType.from_json(json_ref, universe=universe) for json_ref in json_refs
            ]
        self.infoMappings = infoMappings
        return infoMappings

    def unpackHeader(self, rawHeader: bytes) -> str | None:
        return lzma.decompress(rawHeader).decode()

    def constructGraph(
        self,
        nodes: set[uuid.UUID],
        _readBytes: Callable[[int, int], bytes],
        universe: DimensionUniverse | None = None,
    ) -> QuantumGraph:
        # need to import here to avoid cyclic imports
        from . import QuantumGraph

        graph = nx.DiGraph()
        loadedTaskDef: dict[str, TaskDef] = {}
        container = {}
        datasetDict = _DatasetTracker(createInverse=True)
        taskToQuantumNode: defaultdict[TaskDef, set[QuantumNode]] = defaultdict(set)
        initInputRefs: dict[str, list[DatasetRef]] = {}
        initOutputRefs: dict[str, list[DatasetRef]] = {}

        if universe is not None:
            if not universe.isCompatibleWith(self.infoMappings.universe):
                saved = self.infoMappings.universe
                raise RuntimeError(
                    f"The saved dimension universe ({saved.namespace}@v{saved.version}) is not "
                    f"compatible with the supplied universe ({universe.namespace}@v{universe.version})."
                )
        else:
            universe = self.infoMappings.universe

        for node in nodes:
            start, stop = self.infoMappings.map[node]["bytes"]
            start, stop = start + self.headerSize, stop + self.headerSize
            # Read in the bytes corresponding to the node to load and
            # decompress it
            dump = json.loads(lzma.decompress(_readBytes(start, stop)))

            # Turn the json back into the pydandtic model
            nodeDeserialized = SerializedQuantumNode.direct(**dump)
            del dump

            # attach the dictionary of dimension records to the pydantic model
            # these are stored separately because the are stored over and over
            # and this saves a lot of space and time.
            nodeDeserialized.quantum.dimensionRecords = self.infoMappings.dimensionRecords
            # get the label for the current task
            nodeTaskLabel = nodeDeserialized.taskLabel

            if nodeTaskLabel not in loadedTaskDef:
                # Get the byte ranges corresponding to this taskDef
                start, stop = self.infoMappings.taskDefMap[nodeTaskLabel]["bytes"]
                start, stop = start + self.headerSize, stop + self.headerSize

                # bytes are compressed, so decompress them
                taskDefDump = json.loads(lzma.decompress(_readBytes(start, stop)))
                taskClass: type[PipelineTask] = doImportType(taskDefDump["taskName"])
                config: PipelineTaskConfig = taskClass.ConfigClass()
                config.loadFromStream(taskDefDump["config"])
                # Rebuild TaskDef
                recreatedTaskDef = TaskDef(
                    taskName=taskDefDump["taskName"],
                    taskClass=taskClass,
                    config=config,
                    label=taskDefDump["label"],
                )
                loadedTaskDef[nodeTaskLabel] = recreatedTaskDef

                # initInputRefs and initOutputRefs are optional
                if (refs := taskDefDump.get("initInputRefs")) is not None:
                    initInputRefs[recreatedTaskDef.label] = [
                        cast(DatasetRef, DatasetRef.from_json(ref, universe=universe)) for ref in refs
                    ]
                if (refs := taskDefDump.get("initOutputRefs")) is not None:
                    initOutputRefs[recreatedTaskDef.label] = [
                        cast(DatasetRef, DatasetRef.from_json(ref, universe=universe)) for ref in refs
                    ]

                # rebuild the mappings that associate dataset type names with
                # TaskDefs
                for _, input in self.infoMappings.taskDefMap[nodeTaskLabel]["inputs"]:
                    datasetDict.addConsumer(DatasetTypeName(input), recreatedTaskDef)

                added = set()
                for outputConnection in self.infoMappings.taskDefMap[nodeTaskLabel]["outputs"]:
                    typeName = outputConnection[1]
                    if typeName not in added:
                        added.add(typeName)
                        datasetDict.addProducer(DatasetTypeName(typeName), recreatedTaskDef)

            # reconstitute the node, passing in the dictionaries for the
            # loaded TaskDefs and dimension records. These are used to ensure
            # that each unique record is only loaded once
            qnode = QuantumNode.from_simple(nodeDeserialized, loadedTaskDef, universe)
            container[qnode.nodeId] = qnode
            taskToQuantumNode[loadedTaskDef[nodeTaskLabel]].add(qnode)

            # recreate the relations between each node from stored info
            graph.add_node(qnode)
            for id in self.infoMappings.map[qnode.nodeId]["inputs"]:
                # uuid is stored as a string, turn it back into a uuid
                id = uuid.UUID(id)
                # if the id is not yet in the container, dont make a connection
                # this is not an issue, because once it is, that id will add
                # the reverse connection
                if id in container:
                    graph.add_edge(container[id], qnode)
            for id in self.infoMappings.map[qnode.nodeId]["outputs"]:
                # uuid is stored as a string, turn it back into a uuid
                id = uuid.UUID(id)
                # if the id is not yet in the container, dont make a connection
                # this is not an issue, because once it is, that id will add
                # the reverse connection
                if id in container:
                    graph.add_edge(qnode, container[id])

        newGraph = object.__new__(QuantumGraph)
        newGraph._metadata = self.infoMappings.metadata
        newGraph._buildId = self.infoMappings._buildId
        newGraph._datasetDict = datasetDict
        newGraph._nodeIdMap = container
        newGraph._count = len(nodes)
        newGraph._taskToQuantumNode = dict(taskToQuantumNode.items())
        newGraph._taskGraph = datasetDict.makeNetworkXGraph()
        newGraph._connectedQuanta = graph
        newGraph._initInputRefs = initInputRefs
        newGraph._initOutputRefs = initOutputRefs
        newGraph._globalInitOutputRefs = self.infoMappings.globalInitOutputRefs
        newGraph._registryDatasetTypes = self.infoMappings.registryDatasetTypes
        newGraph._universe = universe
        newGraph._pipeline_graph = None
        return newGraph


DESERIALIZER_MAP: dict[int, type[DeserializerBase]] = {
    1: DeserializerV1,
    2: DeserializerV2,
    3: DeserializerV3,
}
