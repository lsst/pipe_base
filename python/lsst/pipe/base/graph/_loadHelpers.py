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

import struct
from contextlib import ExitStack
from dataclasses import dataclass
from io import BufferedRandom, BytesIO
from types import TracebackType
from typing import TYPE_CHECKING, BinaryIO, ContextManager, Iterable, Optional, Set, Type, Union
from uuid import UUID

from lsst.daf.butler import DimensionUniverse
from lsst.resources import ResourceHandleProtocol, ResourcePath

if TYPE_CHECKING:
    from ._versionDeserializers import DeserializerBase
    from .graph import QuantumGraph


@dataclass
class LoadHelper(ContextManager["LoadHelper"]):
    """This is a helper class to assist with selecting the appropriate loader
    and managing any contexts that may be needed.

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

    def __post_init__(self) -> None:
        self._resourceHandle: Optional[ResourceHandleProtocol] = None
        self._exitStack = ExitStack()

    def _initialize(self) -> None:
        # need to import here to avoid cyclic imports
        from ._versionDeserializers import DESERIALIZER_MAP
        from .graph import MAGIC_BYTES, STRUCT_FMT_BASE

        # Read the first few bytes which correspond to the  magic identifier
        # bytes, and save version
        magicSize = len(MAGIC_BYTES)
        # read in just the fmt base to determine the save version
        fmtSize = struct.calcsize(STRUCT_FMT_BASE)
        preambleSize = magicSize + fmtSize
        headerBytes = self._readBytes(0, preambleSize)
        magic = headerBytes[:magicSize]
        versionBytes = headerBytes[magicSize:]

        save_version = self._validateSave(magic, versionBytes)

        # select the appropriate deserializer for this save version
        deserializerClass = DESERIALIZER_MAP[save_version]

        # read in the bytes corresponding to the mappings and initialize the
        # deserializer. This will be the bytes that describe the following
        # byte boundaries of the header info
        sizeBytes = self._readBytes(preambleSize, preambleSize + deserializerClass.structSize)
        # DeserializerBase subclasses are required to have the same constructor
        # signature as the base class itself, but there is no way to express
        # this in the type system, so we just tell MyPy to ignore it.
        self.deserializer: DeserializerBase = deserializerClass(preambleSize, sizeBytes)
        # get the header byte range for later reading
        self.headerBytesRange = (preambleSize + deserializerClass.structSize, self.deserializer.headerSize)

    def _validateSave(self, magic: bytes, versionBytes: bytes) -> int:
        """Implement validation on input file, prior to attempting to load it

        Paramters
        ---------
        magic : `bytes`
            The first few bytes of the file, used to verify it is a
            QuantumGraph save file
        versionBytes : `bytes`
            The next few bytes from the beginning of the file, used to parse
            which version of the QuantumGraph file the save corresponds to

        Returns
        -------
        save_version : `int`
            The save version parsed from the supplied bytes

        Raises
        ------
        ValueError
            Raised if the specified file contains the wrong file signature and
            is not a `QuantumGraph` save, or if the graph save version is
            below the minimum specified version.
        """
        from .graph import MAGIC_BYTES, SAVE_VERSION, STRUCT_FMT_BASE

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
        if save_version < self.minimumVersion:
            raise ValueError(
                f"The loaded QuantumGraph is version {save_version}, and the minimum "
                f"version specified is {self.minimumVersion}. Please re-run this method "
                "with a lower minimum version, then re-save the graph to automatically upgrade"
                "to the newest version. Older versions may not work correctly with newer code"
            )

        if save_version > SAVE_VERSION:
            raise ValueError(
                f"The version of this save file is {save_version}, but this version of"
                f"Quantum Graph software only knows how to read up to version {SAVE_VERSION}"
            )
        return save_version

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
            Raised if Supplied DimensionUniverse is not compatible with the
            DimensionUniverse saved in the graph
            Raised if the method was not called from within a context block
        """
        if self._resourceHandle is None:
            raise RuntimeError("Load can only be used within a context manager")

        headerInfo = self.deserializer.readHeaderInfo(self._readBytes(*self.headerBytesRange))
        # verify this is the expected graph
        if graphID is not None and headerInfo._buildId != graphID:
            raise ValueError("graphID does not match that of the graph being loaded")
        # Read in specified nodes, or all the nodes
        nodeSet: Set[UUID]
        if nodes is None:
            nodeSet = set(headerInfo.map.keys())
        else:
            # only some bytes are being read using the reader specialized for
            # this class
            # create a set to ensure nodes are only loaded once
            nodeSet = {UUID(n) if isinstance(n, str) else n for n in nodes}
            # verify that all nodes requested are in the graph
            remainder = nodeSet - headerInfo.map.keys()
            if remainder:
                raise ValueError(
                    f"Nodes {remainder} were requested, but could not be found in the input graph"
                )
        _readBytes = self._readBytes
        if universe is None:
            universe = headerInfo.universe
        return self.deserializer.constructGraph(nodeSet, _readBytes, universe)

    def _readBytes(self, start: int, stop: int) -> bytes:
        """Load the specified byte range from the ResourcePath object

        Parameters
        ----------
        start : `int`
            The beginning byte location to read
        end : `int`
            The end byte location to read

        Returns
        -------
        result : `bytes`
            The byte range specified from the `ResourceHandle`

        Raises
        ------
        RuntimeError
            Raise if the method was not called from within a context block
        """
        if self._resourceHandle is None:
            raise RuntimeError("_readBytes must be called from within a context block")
        self._resourceHandle.seek(start)
        return self._resourceHandle.read(stop - start)

    def __enter__(self) -> "LoadHelper":
        if isinstance(self.uri, (BinaryIO, BytesIO, BufferedRandom)):
            self._resourceHandle = self.uri
        else:
            self._resourceHandle = self._exitStack.enter_context(self.uri.open("rb"))
        self._initialize()
        return self

    def __exit__(
        self,
        type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        assert self._resourceHandle is not None
        self._exitStack.close()
        self._resourceHandle = None

    def readHeader(self) -> Optional[str]:
        with self as handle:
            result = handle.deserializer.unpackHeader(self._readBytes(*self.headerBytesRange))
        return result
