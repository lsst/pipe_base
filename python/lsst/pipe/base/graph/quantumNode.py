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

__all__ = ("QuantumNode", "NodeId", "BuildId")

from dataclasses import dataclass
from typing import NewType

from ..pipeline import TaskDef
from lsst.daf.butler import Quantum, DatasetRef

BuildId = NewType("BuildId", str)


def _hashDsRef(ref: DatasetRef) -> int:
    return hash((ref.datasetType, ref.dataId))


@dataclass(frozen=True, eq=True)
class NodeId:
    """This represents an unique identifier of a node within an individual
    construction of a `QuantumGraph`. This identifier will stay constant
    through a pickle, and any `QuantumGraph` methods that return a new
    `QuantumGraph`.

    A `NodeId` will not be the same if a new graph is built containing the same
    information in a `QuantumNode`, or even built from exactly the same inputs.

    `NodeId`s do not play any role in deciding the equality or identity (hash)
    of a `QuantumNode`, and are mainly useful in debugging or working with
    various subsets of the same graph.

    This interface is a convenance only, and no guarantees on long term
    stability are made. New implementations might change the `NodeId`, or
    provide more or less guarantees.
    """
    number: int
    """The unique position of the node within the graph assigned at graph
    creation.
    """
    buildId: BuildId
    """Unique identifier created at the time the originating graph was created
    """


@dataclass(frozen=True)
class QuantumNode:
    """This class represents a node in the quantum graph.

    The quantum attribute represents the data that is to be processed at this
    node.
    """
    quantum: Quantum
    """The unit of data that is to be processed by this graph node"""
    taskDef: TaskDef
    """Definition of the task that will process the `Quantum` associated with
    this node.
    """
    nodeId: NodeId
    """The unique position of the node within the graph assigned at graph
    creation.
    """

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, QuantumNode):
            return False
        if self.quantum != other.quantum:
            return False
        return self.taskDef == other.taskDef

    def __hash__(self) -> int:
        """For graphs it is useful to have a more robust hash than provided
        by the default quantum id based hashing
        """
        return hash((self.taskDef, self.quantum))
