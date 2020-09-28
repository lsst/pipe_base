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
from collections import defaultdict

__all__ = ("_DatasetTracker", "DatasetTypeName")

from dataclasses import dataclass, field
import networkx as nx
from typing import (DefaultDict, Generic, Optional, Set, TypeVar, Generator, Tuple, NewType,)

from lsst.daf.butler import DatasetRef

from .quantumNode import QuantumNode
from ..pipeline import TaskDef

# NewTypes
DatasetTypeName = NewType("DatasetTypeName", str)

# Generic type parameters
_T = TypeVar("_T", DatasetTypeName, DatasetRef)
_U = TypeVar("_U", TaskDef, QuantumNode)


@dataclass
class _DatasetTrackerElement(Generic[_U]):
    inputs: Set[_U] = field(default_factory=set)
    output: Optional[_U] = None


class _DatasetTracker(Generic[_T, _U]):
    def __init__(self):
        self._container: DefaultDict[_T, _DatasetTrackerElement[_U]] = defaultdict(_DatasetTrackerElement)

    def addInput(self, key: _T, value: _U):
        self._container[key].inputs.add(value)

    def addOutput(self, key: _T, value: _U):
        element = self._container[key]
        if element.output is not None:
            raise ValueError(f"Only one output for key {key} is allowed, "
                             f"the current output is set to {element.output}")
        element.output = value

    def getInputs(self, key: _T) -> Set[_U]:
        return self._container[key].inputs

    def getOutput(self, key: _T) -> Optional[_U]:
        return self._container[key].output

    def getAll(self, key: _T) -> Set[_U]:
        output = self._container[key].output
        if output is not None:
            return self._container[key].inputs.union((output,))
        return set(self._container[key].inputs)

    def makeNetworkXGraph(self) -> nx.DiGraph:
        graph = nx.DiGraph()
        graph.add_edges_from(self._datasetDictToEdgeIterator())
        if None in graph.nodes():
            graph.remove_node(None)
        return graph

    def _datasetDictToEdgeIterator(self) -> Generator[Tuple[Optional[_U], Optional[_U]], None, None]:
        """Helper function designed to be used in conjunction with
        `networkx.DiGraph.add_edges_from`. This takes a mapping of keys to
        `_DatasetTrackers` and yields successive pairs of elements that are to be
        considered connected by the graph.
        """
        for entry in self._container.values():
            # If there is no inputs and only outputs (likely in test cases or
            # building inits or something) use None as a Node, that will then be
            # removed later
            inputs = entry.inputs or (None,)
            for inpt in inputs:
                yield (entry.output, inpt)

    def keys(self) -> Set[_T]:
        return set(self._container.keys())
