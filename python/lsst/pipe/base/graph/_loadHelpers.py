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

from dataclasses import dataclass
import functools
import io
import lzma
import pickle
import struct

from collections import defaultdict, UserDict
from typing import (Optional, Iterable, DefaultDict, Set, Dict, TYPE_CHECKING, Type, Union)

if TYPE_CHECKING:
    from . import QuantumGraph


# Create a custom dict that will return the desired default if a key is missing
class RegistryDict(UserDict):
    def __missing__(self, key):
        return DefaultLoadHelper


# Create a registry to hold all the load Helper classes
HELPER_REGISTRY = RegistryDict()


def register_helper(URICLass: Union[Type[ButlerURI], Type[io.IO[bytes]]]):
    """Used to register classes as Load helpers

    When decorating a class the parameter is the class of "handle type", i.e.
    a ButlerURI type or open file handle that will be used to do the loading.
    This is then associated with the decorated class such that when the
    parameter type is used to load data, the appropriate helper to work with
    that data type can be returned.

    A decorator is used so that in theory someone could define another handler
    in a different module and register it for use.

    Parameters
    ----------
    URIClass : Type of `~lsst.daf.butler.ButlerURI` or `~io.IO` of bytes
        type for which the decorated class should be mapped to
    """
    def wrapper(class_):
        HELPER_REGISTRY[URICLass] = class_
        return class_
    return wrapper


class DefaultLoadHelper:
    """Default load helper for `QuantumGraph` save files

    This class, and its subclasses, are used to unpack a quantum graph save
    file. This file is a binary representation of the graph in a format that
    allows individual nodes to be loaded without needing to load the entire
    file.

    This default implementation has the interface to load select nodes
    from disk, but actually always loads the entire save file and simply
    returns what nodes (or all) are requested. This is intended to serve for
    all cases where there is a read method on the input parameter, but it is
    unknown how to read select bytes of the stream. It is the responsibility of
    sub classes to implement the method responsible for loading individual
    bytes from the stream.

    Parameters
    ----------
    uriObject : `~lsst.daf.butler.ButlerURI` or `io.IO` of bytes
        This is the object that will be used to retrieve the raw bytes of the
        save.

    Raises
    ------
    ValueError
        Raised if the specified file contains the wrong file signature and is
        not a `QuantumGraph` save
    """
    def __init__(self, uriObject: Union[ButlerURI, io.IO[bytes]]):
        self.uriObject = uriObject

        preambleSize, taskDefSize, nodeSize = self._readSizes()

        # Recode the total header size
        self.headerSize = preambleSize + taskDefSize + nodeSize

        self._readByteMappings(preambleSize, self.headerSize, taskDefSize)

    def _readSizes(self):
        # need to import here to avoid cyclic imports
        from .graph import STRUCT_FMT_STRING, MAGIC_BYTES
        # Read the first few bytes which correspond to the lengths of the
        # magic identifier bytes, 2 byte version
        # number and the two 8 bytes numbers that are the sizes of the byte
        # maps
        magicSize = len(MAGIC_BYTES)
        fmt = STRUCT_FMT_STRING
        fmtSize = struct.calcsize(fmt)
        preambleSize = magicSize + fmtSize

        headerBytes = self._readBytes(0, preambleSize)
        magic = headerBytes[:magicSize]
        sizeBytes = headerBytes[magicSize:]

        if magic != MAGIC_BYTES:
            raise ValueError("This file does not appear to be a quantum graph save got magic bytes "
                             f"{magic}, expected {MAGIC_BYTES}")

        # Turn they encode bytes back into a python int object
        save_version, taskDefSize, nodeSize = struct.unpack('>HQQ', sizeBytes)

        # Store the save version, so future read codes can make use of any
        # format changes to the save protocol
        self.save_version = save_version

        return preambleSize, taskDefSize, nodeSize

    def _readByteMappings(self, preambleSize, headerSize, taskDefSize):
        # Take the header size explicitly so subclasses can modify before
        # This task is called

        # read the bytes of taskDef bytes and nodes skipping the size bytes
        headerMaps = self._readBytes(preambleSize, headerSize)

        # read the map of taskDef bytes back in skipping the size bytes
        self.taskDefMap = pickle.loads(headerMaps[:taskDefSize])

        # read back in the graph id
        self._buildId = self.taskDefMap['__GraphBuildID']

        # read the map of the node objects back in skipping bytes
        # corresponding to the taskDef byte map
        self.map = pickle.loads(headerMaps[taskDefSize:])

    def load(self, nodes: Optional[Iterable[int]] = None, graphID: Optional[str] = None) -> QuantumGraph:
        """Loads in the specified nodes from the graph

        Load in the `QuantumGraph` containing only the nodes specified in the
        ``nodes`` parameter from the graph specified at object creation. If
        ``nodes`` is None (the default) the whole graph is loaded.

        Parameters
        ----------
        nodes : `Iterable` of `int` or `None`
            The nodes to load from the graph, loads all if value is None
            (the default)
        graphID : `str` or `None`
            If specified this ID is verified against the loaded graph prior to
            loading any Nodes. This defaults to None in which case no
            validation is done.

        Returns
        -------
        graph : `QuantumGraph`
            The loaded `QuantumGraph` object

        Raises
        ------
        ValueError
            Raised if one or more of the nodes requested is not in the
            `QuantumGraph` or if graphID parameter does not match the graph
            being loaded.
        """
        # need to import here to avoid cyclic imports
        from . import QuantumGraph
        if graphID is not None and self._buildId != graphID:
            raise ValueError('graphID does not match that of the graph being loaded')
        # Read in specified nodes, or all the nodes
        if nodes is None:
            nodes = list(self.map.keys())
            # if all nodes are to be read, force the reader from the base class
            # that will read all they bytes in one go
            _readBytes = functools.partial(DefaultLoadHelper._readBytes, self)
        else:
            # only some bytes are being read using the reader specialized for
            # this class
            # create a set to ensure nodes are only loaded once
            nodes = set(nodes)
            # verify that all nodes requested are in the graph
            remainder = nodes - self.map.keys()
            if remainder:
                raise ValueError("Nodes {remainder} were requested, but could not be found in the input "
                                 "graph")
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
            # bytes are compressed, so decompress them
            dump = lzma.decompress(_readBytes(start, stop))

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
                # subclasses.
                # bytes are compressed, so decompress them
                taskDef = pickle.loads(lzma.decompress(_readBytes(start, stop)))
                loadedTaskDef[nodeTask] = taskDef
            # Explicitly overload the "frozen-ness" of nodes to attach the
            # taskDef back into the un-persisted node
            object.__setattr__(qNode, 'taskDef', loadedTaskDef[nodeTask])
            quanta[qNode.taskDef].add(qNode.quantum)

            # record the node for later processing
            quantumToNodeId[qNode.quantum] = qNode.nodeId

        # construct an empty new QuantumGraph object, and run the associated
        # creation method with the un-persisted data
        qGraph = object.__new__(QuantumGraph)
        qGraph._buildGraphs(quanta, _quantumToNodeId=quantumToNodeId, _buildId=self._buildId)
        return qGraph

    def _readBytes(self, start: int, stop: int) -> bytes:
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
    # This subclass implements partial loading of a graph using a s3 uri
    def _readBytes(self, start: int, stop: int) -> bytes:
        args = {}
        # minus 1 in the stop range, because this header is inclusive rather
        # than standard python where the end point is generally exclusive
        args["Range"] = f"bytes={start}-{stop-1}"
        try:
            response = self.uriObject.client.get_object(Bucket=self.uriObject.netloc,
                                                        Key=self.uriObject.relativeToPathRoot,
                                                        **args)
        except (self.uriObject.client.exceptions.NoSuchKey,
                self.uriObject.client.exceptions.NoSuchBucket) as err:
            raise FileNotFoundError(f"No such resource: {self.uriObject}") from err
        body = response["Body"].read()
        response["Body"].close()
        return body


@register_helper(ButlerFileURI)
class FileLoadHelper(DefaultLoadHelper):
    # This subclass implements partial loading of a graph using a file uri
    def _readBytes(self, start: int, stop: int) -> bytes:
        if not hasattr(self, 'fileHandle'):
            self.fileHandle = open(self.uriObject.ospath, 'rb')
        self.fileHandle.seek(start)
        return self.fileHandle.read(stop-start)

    def close(self):
        if hasattr(self, 'fileHandle'):
            self.fileHandle.close()


@register_helper(io.IOBase)  # type: ignore
class OpenFileHandleHelper(DefaultLoadHelper):
    # This handler is special in that it does not get initialized with a
    # ButlerURI, but an open file handle.

    # Most everything stays the same, the variable is even stored as uriObject,
    # because the interface needed for reading is the same. Unfortunately
    # because we do not have Protocols yet, this can not be nicely expressed
    # with typing.

    # This helper does support partial loading

    def __init__(self, uriObject: io.IO[bytes]):
        # Explicitly annotate type and not infer from super
        self.uriObject: io.IO[bytes]
        super().__init__(uriObject)
        # This differs from the default __init__ to force the io object
        # back to the beginning so that in the case the entire file is to
        # read in the file is not already in a partially read state.
        self.uriObject.seek(0)

    def _readBytes(self, start: int, stop: int) -> bytes:
        self.uriObject.seek(start)
        result = self.uriObject.read(stop-start)
        return result


@dataclass
class LoadHelper:
    """This is a helper class to assist with selecting the appropriate loader
    and managing any contexts that may be needed.

    Note
    ----
    This class may go away or be modified in the future if some of the
    features of this module can be propagated to `~lsst.daf.butler.ButlerURI`.

    This helper will raise a `ValueError` if the specified file does not appear
    to be a valid `QuantumGraph` save file.
    """
    uri: ButlerURI
    """ButlerURI object from which the `QuantumGraph` is to be loaded
    """
    def __enter__(self):
        # Only one handler is registered for anything that is an instance of
        # IOBase, so if any type is a subtype of that, set the key explicitly
        # so the correct loader is found, otherwise index by the type
        if isinstance(self.uri, io.IOBase):
            key = io.IOBase
        else:
            key = type(self.uri)
        self._loaded = HELPER_REGISTRY[key](self.uri)
        return self._loaded

    def __exit__(self, type, value, traceback):
        self._loaded.close()
