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

__all__ = ("LoadHelper")

from lsst.daf.butler import ButlerURI, Quantum
from lsst.daf.butler.core._butlerUri.s3 import ButlerS3URI
from lsst.daf.butler.core._butlerUri.file import ButlerFileURI

from ..pipeline import TaskDef
from .quantumNode import NodeId

import pickle
import struct
import functools
from dataclasses import dataclass

from collections import defaultdict, UserDict
from typing import (Optional, Iterable, DefaultDict, Set, Dict, TYPE_CHECKING, Type)

if TYPE_CHECKING:
    from . import QuantumGraph


class RegistryDict(UserDict):
    def __missing__(self, key):
        return DefaultLoadHelper


HELPER_REGISTRY = RegistryDict()


def register_helper(URICLass: Type[ButlerURI]):
    def wrapper(class_):
        HELPER_REGISTRY[URICLass] = class_
        return class_
    return wrapper


class DefaultLoadHelper:
    def __init__(self, uriObject: ButlerURI):
        self.uriObject = uriObject

        # Read the first 16 bytes which correspond to the two 8 bytes numbers
        # that are the sizes of the byte maps
        sizeBytes = self.uriObject.read(16)
        # Turn they encode bytes back into a python int object
        taskDefSize, nodeSize = struct.unpack('>QQ', sizeBytes)
        # Recode the total header size
        self.headerSize = 16+taskDefSize+nodeSize

        # read the map of taskDef bytes back in skipping the size bytes
        headerMaps = self.uriObject.read(self.headerSize)[16:]
        # read the map of taskDef bytes back in skipping the size bytes
        self.taskDefMap = pickle.loads(headerMaps[:taskDefSize])

        # read the map of the node objects back in skipping bytes
        # corresponding to sizes and taskDef byte map
        self.map = pickle.loads(headerMaps[taskDefSize:])

    def load(self, nodes: Optional[Iterable[int]] = None) -> QuantumGraph:
        # need to import here to avoid cyclic imports
        from . import QuantumGraph
        # Read in specified nodes, or all the nodes

        if nodes is None:
            nodes = list(self.map.keys())
            # if all nodes are to be read, force the reader from the base class
            # that will read all they bytes in one go
            _readBytes = functools.partial(DefaultLoadHelper._readBytes, self)
        else:
            # only some bytes are being read using the reader specialized for
            # this class
            nodes = list(nodes)
            _readBytes = self._readBytes
        # create a container for loaded data
        quanta: DefaultDict[TaskDef, Set[Quantum]] = defaultdict(set)
        quantumToNodeId: Dict[Quantum, NodeId] = {}
        loadedTaskDef = {}
        # loop over the nodes specified above
        for node in nodes:
            # Get the bytes to read from the map
            start, stop = self.map[node]
            start += self.headerSize
            stop += self.headerSize

            # read the specified bytes, will be overloaded by subclasses
            dump = _readBytes(start, stop)

            # reconstruct node
            qNode = pickle.loads(dump)

            # read the saved node, name. If it has been loaded, attach it, if
            # not read in the taskDef first, and then load it
            nodeTask = qNode.taskDef
            if nodeTask not in loadedTaskDef:
                # Get the byte ranges corresponding to this taskDef
                start, stop = self.taskDefMap[nodeTask]
                start += self.headerSize
                stop += self.headerSize

                # load the taskDef, this method call will be overloaded by
                # subclasses
                taskDef = pickle.loads(_readBytes(start, stop))
                loadedTaskDef[nodeTask] = taskDef
            # Explicitly overload the "frozen-ness" of nodes to attach the
            # taskDef back into the un-persisted node
            object.__setattr__(qNode, 'taskDef', loadedTaskDef[nodeTask])
            quanta[qNode.taskDef].add(qNode.quantum)

            # record the node for later processing
            quantumToNodeId[qNode.quantum] = qNode.nodeId

        # Get the id of this graph
        _buildId = qNode.nodeId.buildId if nodes else None  # type: ignore

        # construct an empty new QuantumGraph object, and run the associated
        # creation method with the un-persisted data
        qGraph = object.__new__(QuantumGraph)
        qGraph._buildGraphs(quanta, _quantumToNodeId=quantumToNodeId, _buildId=_buildId)
        return qGraph

    def _readBytes(self, start, stop):
        """Loads the specified byte range from the ButlerURI object

        In the base class, this actually will read all the bytes into a buffer
        from the specified ButlerURI object. Then for each method call will
        return the requested byte range. This is the most flexible
        implementation, as no special read is required. This will not give a
        speed up with any sub graph reads though.
        """
        if not hasattr(self, 'buffer'):
            self.buffer = self.uriObject.read()
        return self.buffer[start:stop]

    def close(self):
        """Cleans up an instance if needed. Base class does nothing
        """
        pass


@register_helper(ButlerS3URI)
class S3LoadHelper(DefaultLoadHelper):
    def _readBytes(self, start, stop):
        args = {}
        args["Range"] = f"bytes={start}-{stop}"
        try:
            response = self.uriObject.client.get_object(Bucket=self.uriObject.netloc,
                                                        Key=self.uriObject.relativeToPathRoot,
                                                        **args)
        except (self.uriObject.client.exceptions.NoSuchKey,
                self.uriObject.client.exceptions.NoSuchBucket) as err:
            raise FileNotFoundError(f"No such resource: {self}") from err
        body = response["Body"].read()
        response["Body"].close()
        return body


@register_helper(ButlerFileURI)
class FileLoadHelper(DefaultLoadHelper):
    def _readBytes(self, start, stop):
        if not hasattr(self, 'fileHandle'):
            self.fileHandle = open(self.uriObject.ospath, 'rb')
        self.fileHandle.seek(start)
        return self.fileHandle.read(stop-start)

    def close(self):
        self.fileHandle.close()


@dataclass
class LoadHelper:
    uri: ButlerURI
    """ButlerURI object from which the `QuantumGraph` is to be loaded
    """
    def __enter__(self):
        self._loaded = HELPER_REGISTRY[type(self.uri)](self.uri)
        return self._loaded

    def __exit__(self, type, value, traceback):
        self._loaded.close()
