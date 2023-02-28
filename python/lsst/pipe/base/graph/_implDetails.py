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

__all__ = ("_DatasetTracker", "DatasetTypeName", "_pruner")

from collections import defaultdict
from itertools import chain
from typing import DefaultDict, Dict, Generic, Iterable, List, NewType, Optional, Set, TypeVar

import networkx as nx
from lsst.daf.butler import DatasetRef, DatasetType, NamedKeyDict, Quantum
from lsst.pipe.base.connections import AdjustQuantumHelper

from .._status import NoWorkFound
from ..pipeline import TaskDef
from .quantumNode import QuantumNode

# NewTypes
DatasetTypeName = NewType("DatasetTypeName", str)

# Generic type parameters
_T = TypeVar("_T", DatasetTypeName, DatasetRef)
_U = TypeVar("_U", TaskDef, QuantumNode)


class _DatasetTracker(Generic[_T, _U]):
    r"""This is a generic container for tracking keys which are produced or
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
        self._producers: Dict[_T, _U] = {}
        self._consumers: DefaultDict[_T, Set[_U]] = defaultdict(set)
        self._createInverse = createInverse
        if self._createInverse:
            self._itemsDict: DefaultDict[_U, Set[_T]] = defaultdict(set)

    def addProducer(self, key: _T, value: _U) -> None:
        """Add a key which is produced by some value.

        Parameters
        ----------
        key : TypeVar
            The type to track
        value : TypeVar
            The type associated with the production of the key

        Raises
        ------
        ValueError
            Raised if key is already declared to be produced by another value
        """
        if (existing := self._producers.get(key)) is not None and existing != value:
            raise ValueError(f"Only one node is allowed to produce {key}, the current producer is {existing}")
        self._producers[key] = value
        if self._createInverse:
            self._itemsDict[value].add(key)

    def removeProducer(self, key: _T, value: _U) -> None:
        """Remove a value (e.g. QuantumNode or TaskDef) from being considered
        a producer of the corresponding key. It is not an error to remove a
        key that is not in the tracker.

        Parameters
        ----------
        key : TypeVar
            The type to track
        value : TypeVar
            The type associated with the production of the key
        """
        self._producers.pop(key, None)
        if self._createInverse:
            if result := self._itemsDict.get(value):
                result.discard(key)

    def addConsumer(self, key: _T, value: _U) -> None:
        """Add a key which is consumed by some value.

        Parameters
        ----------
        key : TypeVar
            The type to track
        value : TypeVar
            The type associated with the consumption of the key
        """
        self._consumers[key].add(value)
        if self._createInverse:
            self._itemsDict[value].add(key)

    def removeConsumer(self, key: _T, value: _U) -> None:
        """Remove a value (e.g. QuantumNode or TaskDef) from being considered
        a consumer of the corresponding key. It is not an error to remove a
        key that is not in the tracker.

        Parameters
        ----------
        key : TypeVar
            The type to track
        value : TypeVar
            The type associated with the consumption of the key
        """
        if (result := self._consumers.get(key)) is not None:
            result.discard(value)
        if self._createInverse:
            if result_inverse := self._itemsDict.get(value):
                result_inverse.discard(key)

    def getConsumers(self, key: _T) -> Set[_U]:
        """Return all values associated with the consumption of the supplied
        key.

        Parameters
        ----------
        key : TypeVar
            The type which has been tracked in the _DatasetTracker
        """
        return self._consumers.get(key, set())

    def getProducer(self, key: _T) -> Optional[_U]:
        """Return the value associated with the consumption of the supplied
        key.

        Parameters
        ----------
        key : TypeVar
            The type which has been tracked in the _DatasetTracker
        """
        # This tracker may have had all nodes associated with a key removed
        # and if there are no refs (empty set) should return None
        return producer if (producer := self._producers.get(key)) else None

    def getAll(self, key: _T) -> set[_U]:
        """Return all consumers and the producer associated with the the
        supplied key.

        Parameters
        ----------
        key : TypeVar
            The type which has been tracked in the _DatasetTracker
        """

        return self.getConsumers(key).union(x for x in (self.getProducer(key),) if x is not None)

    @property
    def inverse(self) -> Optional[DefaultDict[_U, Set[_T]]]:
        """Return the inverse mapping if class was instantiated to create an
        inverse, else return None.
        """
        return self._itemsDict if self._createInverse else None

    def makeNetworkXGraph(self) -> nx.DiGraph:
        """Create a NetworkX graph out of all the contained keys, using the
        relations of producer and consumers to create the edges.

        Returns:
        graph : networkx.DiGraph
            The graph created out of the supplied keys and their relations
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

    def keys(self) -> Set[_T]:
        """Return all tracked keys."""
        return self._producers.keys() | self._consumers.keys()

    def remove(self, key: _T) -> None:
        """Remove a key and its corresponding value from the tracker, this is
        a no-op if the key is not in the tracker.

        Parameters
        ----------
        key : TypeVar
            A key tracked by the DatasetTracker
        """
        self._producers.pop(key, None)
        self._consumers.pop(key, None)

    def __contains__(self, key: _T) -> bool:
        """Check if a key is in the _DatasetTracker

        Parameters
        ----------
        key : TypeVar
            The key to check

        Returns
        -------
        contains : bool
            Boolean of the presence of the supplied key
        """
        return key in self._producers or key in self._consumers


def _pruner(
    datasetRefDict: _DatasetTracker[DatasetRef, QuantumNode],
    refsToRemove: Iterable[DatasetRef],
    *,
    alreadyPruned: Optional[Set[QuantumNode]] = None,
) -> None:
    r"""Prune supplied dataset refs out of datasetRefDict container, recursing
    to additional nodes dependant on pruned refs. This function modifies
    datasetRefDict in-place.

    Parameters
    ----------
    datasetRefDict : `_DatasetTracker[DatasetRef, QuantumNode]`
        The dataset tracker that maps `DatasetRef`\ s to the Quantum Nodes
        that produce/consume that `DatasetRef`
    refsToRemove : `Iterable` of `DatasetRef`
        The `DatasetRef`\ s which should be pruned from the input dataset
        tracker
    alreadyPruned : `set` of `QuantumNode`
        A set of nodes which have been pruned from the dataset tracker
    """
    if alreadyPruned is None:
        alreadyPruned = set()
    for ref in refsToRemove:
        # make a copy here, because this structure will be modified in
        # recursion, hitting a node more than once won't be much of an
        # issue, as we skip anything that has been processed
        nodes = set(datasetRefDict.getConsumers(ref))
        for node in nodes:
            # This node will never be associated with this ref
            datasetRefDict.removeConsumer(ref, node)
            if node in alreadyPruned:
                continue
            # find the connection corresponding to the input ref
            connectionRefs = node.quantum.inputs.get(ref.datasetType)
            if connectionRefs is None:
                # look to see if any inputs are component refs that match the
                # input ref to prune
                others = ref.datasetType.makeAllComponentDatasetTypes()
                # for each other component type check if there are assocated
                # refs
                for other in others:
                    connectionRefs = node.quantum.inputs.get(other)
                    if connectionRefs is not None:
                        # now search the component refs and see which one
                        # matches the ref to trim
                        for cr in connectionRefs:
                            if cr.makeCompositeRef() == ref:
                                toRemove = cr
                        break
                else:
                    # Ref must be an initInput ref and we want to ignore those
                    raise RuntimeError(f"Cannot prune on non-Input dataset type {ref.datasetType.name}")
            else:
                toRemove = ref

            tmpRefs = set(connectionRefs).difference((toRemove,))
            tmpConnections = NamedKeyDict[DatasetType, List[DatasetRef]](node.quantum.inputs.items())
            tmpConnections[toRemove.datasetType] = list(tmpRefs)
            helper = AdjustQuantumHelper(inputs=tmpConnections, outputs=node.quantum.outputs)
            assert node.quantum.dataId is not None, (
                "assert to make the type checker happy, it should not "
                "actually be possible to not have dataId set to None "
                "at this point"
            )

            # Try to adjust the quantum with the reduced refs to make sure the
            # node will still satisfy all its conditions.
            #
            # If it can't because NoWorkFound is raised, that means a
            # connection is no longer present, and the node should be removed
            # from the graph.
            try:
                helper.adjust_in_place(node.taskDef.connections, node.taskDef.label, node.quantum.dataId)
                newQuantum = Quantum(
                    taskName=node.quantum.taskName,
                    taskClass=node.quantum.taskClass,
                    dataId=node.quantum.dataId,
                    initInputs=node.quantum.initInputs,
                    inputs=helper.inputs,
                    outputs=helper.outputs,
                )
                # If the inputs or outputs were adjusted to something different
                # than what was supplied by the graph builder, dissassociate
                # node from those refs, and if they are output refs, prune them
                # from downstream tasks. This means that based on new inputs
                # the task wants to produce fewer outputs, or consume fewer
                # inputs.
                for condition, existingMapping, newMapping, remover in (
                    (
                        helper.inputs_adjusted,
                        node.quantum.inputs,
                        helper.inputs,
                        datasetRefDict.removeConsumer,
                    ),
                    (
                        helper.outputs_adjusted,
                        node.quantum.outputs,
                        helper.outputs,
                        datasetRefDict.removeProducer,
                    ),
                ):
                    if condition:
                        notNeeded = set()
                        for key in existingMapping:
                            if key not in newMapping:
                                compositeRefs = (
                                    r if not r.isComponent() else r.makeCompositeRef()
                                    for r in existingMapping[key]
                                )
                                notNeeded |= set(compositeRefs)
                                continue
                            notNeeded |= set(existingMapping[key]) - set(newMapping[key])
                        if notNeeded:
                            for ref in notNeeded:
                                if ref.isComponent():
                                    ref = ref.makeCompositeRef()
                                remover(ref, node)
                            if remover is datasetRefDict.removeProducer:
                                _pruner(datasetRefDict, notNeeded, alreadyPruned=alreadyPruned)
                object.__setattr__(node, "quantum", newQuantum)
                noWorkFound = False

            except NoWorkFound:
                noWorkFound = True

            if noWorkFound:
                # This will throw if the length is less than the minimum number
                for tmpRef in chain(
                    chain.from_iterable(node.quantum.inputs.values()), node.quantum.initInputs.values()
                ):
                    if tmpRef.isComponent():
                        tmpRef = tmpRef.makeCompositeRef()
                    datasetRefDict.removeConsumer(tmpRef, node)
                alreadyPruned.add(node)
                # prune all outputs produced by this node
                # mark that none of these will be produced
                forwardPrunes = set()
                for forwardRef in chain.from_iterable(node.quantum.outputs.values()):
                    datasetRefDict.removeProducer(forwardRef, node)
                    forwardPrunes.add(forwardRef)
                _pruner(datasetRefDict, forwardPrunes, alreadyPruned=alreadyPruned)
