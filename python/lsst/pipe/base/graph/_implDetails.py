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

__all__ = ("_DatasetTracker", "DatasetTypeName")

from collections import defaultdict
from typing import Generic, NewType, TypeVar

import networkx as nx
from lsst.daf.butler import DatasetRef

from ..pipeline import TaskDef
from .quantumNode import QuantumNode

# NewTypes
DatasetTypeName = NewType("DatasetTypeName", str)

# Generic type parameters
_T = TypeVar("_T", DatasetTypeName, DatasetRef)
_U = TypeVar("_U", TaskDef, QuantumNode)


class _DatasetTracker(Generic[_T, _U]):
    r"""A generic container for tracking keys which are produced or
    consumed by some value. In the context of a QuantumGraph, keys may be
    `~lsst.daf.butler.DatasetRef`\ s and the values would be Quanta that either
    produce or consume those `~lsst.daf.butler.DatasetRef`\ s.

    Prameters
    ---------
    createInverse : bool
        When adding a key associated with a producer or consumer, also create
        and inverse mapping that allows looking up all the keys associated with
        some value. Defaults to False.
    """

    def __init__(self, createInverse: bool = False):
        self._producers: dict[_T, _U] = {}
        self._consumers: defaultdict[_T, set[_U]] = defaultdict(set)
        self._createInverse = createInverse
        if self._createInverse:
            self._itemsDict: defaultdict[_U, set[_T]] = defaultdict(set)

    def addProducer(self, key: _T, value: _U) -> None:
        """Add a key which is produced by some value.

        Parameters
        ----------
        key : `~typing.TypeVar`
            The type to track.
        value : `~typing.TypeVar`
            The type associated with the production of the key.

        Raises
        ------
        ValueError
            Raised if key is already declared to be produced by another value.
        """
        if (existing := self._producers.get(key)) is not None and existing != value:
            raise ValueError(f"Only one node is allowed to produce {key}, the current producer is {existing}")
        self._producers[key] = value
        if self._createInverse:
            self._itemsDict[value].add(key)

    def removeProducer(self, key: _T, value: _U) -> None:
        """Remove a value (e.g. `QuantumNode` or `TaskDef`) from being
        considered a producer of the corresponding key.

        It is not an error to remove a key that is not in the tracker.

        Parameters
        ----------
        key : `~typing.TypeVar`
            The type to track.
        value : `~typing.TypeVar`
            The type associated with the production of the key.
        """
        self._producers.pop(key, None)
        if self._createInverse:
            if result := self._itemsDict.get(value):
                result.discard(key)

    def addConsumer(self, key: _T, value: _U) -> None:
        """Add a key which is consumed by some value.

        Parameters
        ----------
        key : `~typing.TypeVar`
            The type to track.
        value : `~typing.TypeVar`
            The type associated with the consumption of the key.
        """
        self._consumers[key].add(value)
        if self._createInverse:
            self._itemsDict[value].add(key)

    def removeConsumer(self, key: _T, value: _U) -> None:
        """Remove a value (e.g. `QuantumNode` or `TaskDef`) from being
        considered a consumer of the corresponding key.

        It is not an error to remove a key that is not in the tracker.

        Parameters
        ----------
        key : `~typing.TypeVar`
            The type to track.
        value : `~typing.TypeVar`
            The type associated with the consumption of the key.
        """
        if (result := self._consumers.get(key)) is not None:
            result.discard(value)
        if self._createInverse:
            if result_inverse := self._itemsDict.get(value):
                result_inverse.discard(key)

    def getConsumers(self, key: _T) -> set[_U]:
        """Return all values associated with the consumption of the supplied
        key.

        Parameters
        ----------
        key : `~typing.TypeVar`
            The type which has been tracked in the `_DatasetTracker`.
        """
        return self._consumers.get(key, set())

    def getProducer(self, key: _T) -> _U | None:
        """Return the value associated with the consumption of the supplied
        key.

        Parameters
        ----------
        key : `~typing.TypeVar`
            The type which has been tracked in the `_DatasetTracker`.
        """
        # This tracker may have had all nodes associated with a key removed
        # and if there are no refs (empty set) should return None
        return producer if (producer := self._producers.get(key)) else None

    def getAll(self, key: _T) -> set[_U]:
        """Return all consumers and the producer associated with the the
        supplied key.

        Parameters
        ----------
        key : `~typing.TypeVar`
            The type which has been tracked in the `_DatasetTracker`.
        """
        return self.getConsumers(key).union(x for x in (self.getProducer(key),) if x is not None)

    @property
    def inverse(self) -> defaultdict[_U, set[_T]] | None:
        """Return the inverse mapping if class was instantiated to create an
        inverse, else return None.
        """
        return self._itemsDict if self._createInverse else None

    def makeNetworkXGraph(self) -> nx.DiGraph:
        """Create a NetworkX graph out of all the contained keys, using the
        relations of producer and consumers to create the edges.

        Returns
        -------
        graph : `networkx.DiGraph`
            The graph created out of the supplied keys and their relations.
        """
        graph = nx.DiGraph()
        for entry in self._producers.keys() | self._consumers.keys():
            producer = self.getProducer(entry)
            consumers = self.getConsumers(entry)
            # This block is for tasks that consume existing inputs
            if producer is None and consumers:
                for consumer in consumers:
                    graph.add_node(consumer)
            # This block is for tasks that produce output that is not consumed
            # in this graph
            elif producer is not None and not consumers:
                graph.add_node(producer)
            # all other connections
            else:
                for consumer in consumers:
                    graph.add_edge(producer, consumer)
        return graph

    def keys(self) -> set[_T]:
        """Return all tracked keys."""
        return self._producers.keys() | self._consumers.keys()

    def remove(self, key: _T) -> None:
        """Remove a key and its corresponding value from the tracker, this is
        a no-op if the key is not in the tracker.

        Parameters
        ----------
        key : `~typing.TypeVar`
            A key tracked by the `_DatasetTracker`.
        """
        self._producers.pop(key, None)
        self._consumers.pop(key, None)

    def __contains__(self, key: _T) -> bool:
        """Check if a key is in the `_DatasetTracker`.

        Parameters
        ----------
        key : `~typing.TypeVar`
            The key to check.

        Returns
        -------
        contains : `bool`
            Boolean of the presence of the supplied key.
        """
        return key in self._producers or key in self._consumers
