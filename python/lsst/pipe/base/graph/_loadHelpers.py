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

__all__ = ("LoadHelper",)

import functools
import io
import struct
from dataclasses import dataclass
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Callable,
    ContextManager,
    Iterable,
    Optional,
    Set,
    Type,
    TypeVar,
    Union,
)
from uuid import UUID

from lsst.daf.butler import DimensionUniverse
from lsst.resources import ResourcePath
from lsst.resources.file import FileResourcePath
from lsst.resources.s3 import S3ResourcePath

if TYPE_CHECKING:
    from ._versionDeserializers import DeserializerBase
    from .graph import QuantumGraph


_T = TypeVar("_T")


# Create a custom dict that will return the desired default if a key is missing
class RegistryDict(dict):
    def __missing__(self, key: Any) -> Type[DefaultLoadHelper]:
        return DefaultLoadHelper


# Create a registry to hold all the load Helper classes
HELPER_REGISTRY = RegistryDict()


def register_helper(URIClass: Union[Type[ResourcePath], Type[BinaryIO]]) -> Callable[[_T], _T]:
    """Used to register classes as Load helpers

    When decorating a class the parameter is the class of "handle type", i.e.
    a ResourcePath type or open file handle that will be used to do the
    loading. This is then associated with the decorated class such that when
    the parameter type is used to load data, the appropriate helper to work
    with that data type can be returned.

    A decorator is used so that in theory someone could define another handler
    in a different module and register it for use.

    Parameters
    ----------
    URIClass : Type of `~lsst.resources.ResourcePath` or `~IO` of bytes
        type for which the decorated class should be mapped to
    """

    def wrapper(class_: _T) -> _T:
        HELPER_REGISTRY[URIClass] = class_
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
    uriObject : `~lsst.resources.ResourcePath` or `IO` of bytes
        This is the object that will be used to retrieve the raw bytes of the
        save.
    minimumVersion : `int`
        Minimum version of a save file to load. Set to -1 to load all
        versions. Older versions may need to be loaded, and re-saved
        to upgrade them to the latest format. This upgrade may not happen
        deterministically each time an older graph format is loaded. Because
        of this behavior, the minimumVersion parameter, forces a user to
        interact manually and take this into account before they can be used in
        production.

    Raises
    ------
    ValueError
        Raised if the specified file contains the wrong file signature and is
        not a `QuantumGraph` save, or if the graph save version is below the
        minimum specified version.
    """

    def __init__(self, uriObject: Union[ResourcePath, BinaryIO], minimumVersion: int):
        headerBytes = self.__setup_impl(uriObject, minimumVersion)
        self.headerInfo = self.deserializer.readHeaderInfo(headerBytes)

    def __setup_impl(self, uriObject: Union[ResourcePath, BinaryIO], minimumVersion: int) -> bytes:
        self.uriObject = uriObject
        # need to import here to avoid cyclic imports
        from ._versionDeserializers import DESERIALIZER_MAP
        from .graph import MAGIC_BYTES, SAVE_VERSION, STRUCT_FMT_BASE

        # Read the first few bytes which correspond to the  magic identifier
        # bytes, and save version
        magicSize = len(MAGIC_BYTES)
        # read in just the fmt base to determine the save version
        fmtSize = struct.calcsize(STRUCT_FMT_BASE)
        preambleSize = magicSize + fmtSize
        headerBytes = self._readBytes(0, preambleSize)
        magic = headerBytes[:magicSize]
        versionBytes = headerBytes[magicSize:]

        if magic != MAGIC_BYTES:
            raise ValueError(
                "This file does not appear to be a quantum graph save got magic bytes "
                f"{magic!r}, expected {MAGIC_BYTES!r}"
            )

        # unpack the save version bytes and verify it is a version that this
        # code can understand
        (save_version,) = struct.unpack(STRUCT_FMT_BASE, versionBytes)
        # loads can sometimes trigger upgrades in format to a latest version,
        # in which case accessory code might not match the upgraded graph.
        # I.E. switching from old node number to UUID. This clause necessitates
        # that users specifically interact with older graph versions and verify
        # everything happens appropriately.
        if save_version < minimumVersion:
            raise ValueError(
                f"The loaded QuantumGraph is version {save_version}, and the minimum "
                f"version specified is {minimumVersion}. Please re-run this method "
                "with a lower minimum version, then re-save the graph to automatically upgrade"
                "to the newest version. Older versions may not work correctly with newer code"
            )

        if save_version > SAVE_VERSION:
            raise RuntimeError(
                f"The version of this save file is {save_version}, but this version of"
                f"Quantum Graph software only knows how to read up to version {SAVE_VERSION}"
            )

        # select the appropriate deserializer for this save version
        deserializerClass = DESERIALIZER_MAP[save_version]

        # read in the bytes corresponding to the mappings and initialize the
        # deserializer. This will be the bytes that describe the following
        # byte boundaries of the header info
        sizeBytes = self._readBytes(preambleSize, preambleSize + deserializerClass.structSize)
        # DeserializerBase subclasses are required to have the same constructor
        # signature as the base class itself, but there is no way to express
        # this in the type system, so we just tell MyPy to ignore it.
        self.deserializer: DeserializerBase = deserializerClass(preambleSize, sizeBytes)  # type: ignore

        # get the header info
        headerBytes = self._readBytes(
            preambleSize + deserializerClass.structSize, self.deserializer.headerSize
        )
        return headerBytes

    @classmethod
    def dumpHeader(cls, uriObject: Union[ResourcePath, BinaryIO], minimumVersion: int = 3) -> Optional[str]:
        instance = cls.__new__(cls)
        headerBytes = instance.__setup_impl(uriObject, minimumVersion)
        header = instance.deserializer.unpackHeader(headerBytes)
        instance.close()
        return header

    def load(
        self,
        universe: Optional[DimensionUniverse] = None,
        nodes: Optional[Iterable[Union[UUID, str]]] = None,
        graphID: Optional[str] = None,
    ) -> QuantumGraph:
        """Loads in the specified nodes from the graph

        Load in the `QuantumGraph` containing only the nodes specified in the
        ``nodes`` parameter from the graph specified at object creation. If
        ``nodes`` is None (the default) the whole graph is loaded.

        Parameters
        ----------
        universe: `~lsst.daf.butler.DimensionUniverse` or None
            DimensionUniverse instance, not used by the method itself but
            needed to ensure that registry data structures are initialized.
            The universe saved with the graph is used, but if one is passed
            it will be used to validate the compatibility with the loaded
            graph universe.
        nodes : `Iterable` of `UUID` or `str`; or `None`
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
        RuntimeError
            Raise if Supplied DimensionUniverse is not compatible with the
            DimensionUniverse saved in the graph
        """
        # verify this is the expected graph
        if graphID is not None and self.headerInfo._buildId != graphID:
            raise ValueError("graphID does not match that of the graph being loaded")
        # Read in specified nodes, or all the nodes
        nodeSet: Set[UUID]
        if nodes is None:
            nodeSet = set(self.headerInfo.map.keys())
            # if all nodes are to be read, force the reader from the base class
            # that will read all they bytes in one go
            _readBytes: Callable[[int, int], bytes] = functools.partial(DefaultLoadHelper._readBytes, self)
        else:
            # only some bytes are being read using the reader specialized for
            # this class
            # create a set to ensure nodes are only loaded once
            nodeSet = {UUID(n) if isinstance(n, str) else n for n in nodes}
            # verify that all nodes requested are in the graph
            remainder = nodeSet - self.headerInfo.map.keys()
            if remainder:
                raise ValueError(
                    f"Nodes {remainder} were requested, but could not be found in the input graph"
                )
            _readBytes = self._readBytes
        if universe is None:
            universe = self.headerInfo.universe
        return self.deserializer.constructGraph(nodeSet, _readBytes, universe)

    def _readBytes(self, start: int, stop: int) -> bytes:
        """Loads the specified byte range from the ResourcePath object

        In the base class, this actually will read all the bytes into a buffer
        from the specified ResourcePath object. Then for each method call will
        return the requested byte range. This is the most flexible
        implementation, as no special read is required. This will not give a
        speed up with any sub graph reads though.
        """
        if not hasattr(self, "buffer"):
            self.buffer = self.uriObject.read()
        return self.buffer[start:stop]

    def close(self) -> None:
        """Cleans up an instance if needed. Base class does nothing"""
        pass


@register_helper(S3ResourcePath)
class S3LoadHelper(DefaultLoadHelper):
    # This subclass implements partial loading of a graph using a s3 uri
    def _readBytes(self, start: int, stop: int) -> bytes:
        args = {}
        # minus 1 in the stop range, because this header is inclusive rather
        # than standard python where the end point is generally exclusive
        args["Range"] = f"bytes={start}-{stop-1}"
        try:
            response = self.uriObject.client.get_object(
                Bucket=self.uriObject.netloc, Key=self.uriObject.relativeToPathRoot, **args
            )
        except (
            self.uriObject.client.exceptions.NoSuchKey,
            self.uriObject.client.exceptions.NoSuchBucket,
        ) as err:
            raise FileNotFoundError(f"No such resource: {self.uriObject}") from err
        body = response["Body"].read()
        response["Body"].close()
        return body

    uriObject: S3ResourcePath


@register_helper(FileResourcePath)
class FileLoadHelper(DefaultLoadHelper):
    # This subclass implements partial loading of a graph using a file uri
    def _readBytes(self, start: int, stop: int) -> bytes:
        if not hasattr(self, "fileHandle"):
            self.fileHandle = open(self.uriObject.ospath, "rb")
        self.fileHandle.seek(start)
        return self.fileHandle.read(stop - start)

    def close(self) -> None:
        if hasattr(self, "fileHandle"):
            self.fileHandle.close()

    uriObject: FileResourcePath


@register_helper(BinaryIO)
class OpenFileHandleHelper(DefaultLoadHelper):
    # This handler is special in that it does not get initialized with a
    # ResourcePath, but an open file handle.

    # Most everything stays the same, the variable is even stored as uriObject,
    # because the interface needed for reading is the same. Unfortunately
    # because we do not have Protocols yet, this can not be nicely expressed
    # with typing.

    # This helper does support partial loading

    def __init__(self, uriObject: BinaryIO, minimumVersion: int):
        # Explicitly annotate type and not infer from super
        self.uriObject: BinaryIO
        super().__init__(uriObject, minimumVersion=minimumVersion)
        # This differs from the default __init__ to force the io object
        # back to the beginning so that in the case the entire file is to
        # read in the file is not already in a partially read state.
        self.uriObject.seek(0)

    def _readBytes(self, start: int, stop: int) -> bytes:
        self.uriObject.seek(start)
        result = self.uriObject.read(stop - start)
        return result


@dataclass
class LoadHelper(ContextManager[DefaultLoadHelper]):
    """This is a helper class to assist with selecting the appropriate loader
    and managing any contexts that may be needed.

    Note
    ----
    This class may go away or be modified in the future if some of the
    features of this module can be propagated to
    `~lsst.resources.ResourcePath`.

    This helper will raise a `ValueError` if the specified file does not appear
    to be a valid `QuantumGraph` save file.
    """

    uri: Union[ResourcePath, BinaryIO]
    """ResourcePath object from which the `QuantumGraph` is to be loaded
    """
    minimumVersion: int
    """
    Minimum version of a save file to load. Set to -1 to load all
    versions. Older versions may need to be loaded, and re-saved
    to upgrade them to the latest format before they can be used in
    production.
    """

    def __enter__(self) -> DefaultLoadHelper:
        # Only one handler is registered for anything that is an instance of
        # IOBase, so if any type is a subtype of that, set the key explicitly
        # so the correct loader is found, otherwise index by the type
        self._loaded = self._determineLoader()(self.uri, self.minimumVersion)
        return self._loaded

    def __exit__(
        self,
        type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self._loaded.close()

    def _determineLoader(self) -> Type[DefaultLoadHelper]:
        key: Union[Type[ResourcePath], Type[BinaryIO]]
        # Typing for file-like types is a mess; BinaryIO isn't actually
        # a base class of what open(..., 'rb') returns, and MyPy claims
        # that IOBase and BinaryIO actually have incompatible method
        # signatures.  IOBase *is* a base class of what open(..., 'rb')
        # returns, so it's what we have to use at runtime.
        if isinstance(self.uri, io.IOBase):  # type: ignore
            key = BinaryIO
        else:
            key = type(self.uri)
        return HELPER_REGISTRY[key]

    def readHeader(self) -> Optional[str]:
        type_ = self._determineLoader()
        return type_.dumpHeader(self.uri, self.minimumVersion)
