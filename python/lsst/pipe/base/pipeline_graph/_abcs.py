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

__all__ = (
    "Edge",
    "Node",
    "NodeKey",
    "NodeType",
)

import enum
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, NamedTuple, cast

import networkx
from lsst.daf.butler import DatasetRef, DatasetType, Registry
from lsst.utils.classes import immutable

if TYPE_CHECKING:
    from ._dataset_types import DatasetTypeNode


class NodeType(enum.Enum):
    """Enumeration of the types of nodes in a PipelineGraph."""

    DATASET_TYPE = 0
    TASK_INIT = 1
    TASK = 2

    @property
    def bipartite(self) -> int:
        """The integer used as the "bipartite" key for networkx's bipartite
        graph algorithms.
        """
        return int(self is not NodeType.DATASET_TYPE)

    def __lt__(self, other: NodeType) -> bool:
        return self.value < other.value


@immutable
class NodeKey(NamedTuple):
    """A special key type for nodes in networkx graphs.

    Using a tuple for the key allows tasks labels and dataset type names with
    the same string value to coexist in the graph.  Outside of the networkx
    graph access we provide (which is a small power-user subset of the overall
    PipelineGraph API), regular strings should be used instead to save users
    from having to ever construct these objects.

    NodeKey objects stringify to just their name, which is used both as a way
    to convert to the `str` objects used in the main public interface and as
    an easy way to usefully stringify containers returned directly by networkx
    algorithms (especially in error messages).  Note that this requires `repr`,
    not just `str`, because Python builtin containers always use `repr` on
    their items, even in their implementations for `str`.
    """

    node_type: NodeType
    """Node type enum for this key."""

    name: str
    """Task label or dataset type name."""

    def __repr__(self) -> str:
        return self.name

    def __str__(self) -> str:
        return self.name


@immutable
class Node(ABC):
    """Base class for nodes in a pipeline graph.

    Parameters
    ----------
    key : `NodeKey`
        The key for this node in networkx graphs.
    """

    def __init__(self, key: NodeKey):
        self.key = key

    key: NodeKey
    """The key for this node in networkx graphs."""

    @abstractmethod
    def _resolved(self, xgraph: networkx.DiGraph, registry: Registry) -> Node:
        """Resolve any dataset type and dimension names in this graph.

        Parameters
        ----------
        xgraph : `networkx.DiGraph`
            Directed bipartite graph representing the full pipeline.  Should
            not be modified.
        registry : `lsst.daf.butler.Registry`
            Registry that provides dimension and dataset type information.

        Returns
        -------
        node : `Node`
            Resolved version of this node.  May be self if the node is already
            resolved.
        """
        raise NotImplementedError()

    @abstractmethod
    def _unresolved(self) -> Node:
        """Revert this node to a form that just holds names for dataset types
        and dimensions, allowing `_reresolve` to have an effect if called
        again.

        Returns
        -------
        node : `Node`
            Resolved version of this node.  May be self if the node is already
            resolved.
        """
        raise NotImplementedError()

    @abstractmethod
    def _serialize(self, **kwargs: Any) -> dict[str, Any]:
        """Serialize the content of this node into a dictionary of built-in
        objects suitable for JSON conversion.

        This should not include the node's key, as it is always serialized in a
        context that already identifies that.
        """
        raise NotImplementedError()


@immutable
class Edge(ABC):
    """Base class for edges in a pipeline graph.

    This represents the link between a task node and an input or output dataset
    type.  Task-only and dataset-type-only views of the full graph do not have
    stateful edges.

    Parameters
    ----------
    task_key : `NodeKey`
        Key for the task node this edge is connected to.
    dataset_type_key : `NodeKey`
        Key for the dataset type node this edge is connected to.
    storage_class_name : `str`
        Name of the dataset type's storage class as seen by the task.
    is_init : `bool`
        Whether this dataset is read or written when the task is constructed,
        not when it is run.
    connection_name : `str`
        Internal name for the connection as seen by the task.
    """

    def __init__(
        self,
        *,
        task_key: NodeKey,
        dataset_type_key: NodeKey,
        storage_class_name: str,
        is_init: bool,
        connection_name: str,
    ):
        self.task_key = task_key
        self.dataset_type_key = dataset_type_key
        self.storage_class_name = storage_class_name
        self.is_init = is_init
        self._connection_name = connection_name

    task_key: NodeKey
    """Task part of the key for this edge in networkx graphs."""

    dataset_type_key: NodeKey
    """Task part of the key for this edge in networkx graphs."""

    storage_class_name: str
    """Storage class expected by this task.

    If `component` is not `None`, this is the component storage class, not the
    parent storage class.
    """

    is_init: bool
    """Whether this dataset is read or written when the task is constructed,
    not when it is run.
    """

    @property
    def task_label(self) -> str:
        """Label of the task."""
        return str(self.task_key)

    @property
    def parent_dataset_type_name(self) -> str:
        """Name of the parent dataset type.

        All dataset type nodes in a pipeline graph are for parent dataset
        types; components are represented by additional `ReadEdge` state.
        """
        return str(self.dataset_type_key)

    @property
    @abstractmethod
    def key(self) -> tuple[NodeKey, NodeKey]:
        """Pair that forms the key for this edge in networkx graphs.

        This tuple is ordered in the same direction as the pipeline flow:
        `task_key` precedes `dataset_type_key` for writes, and the
        reverse is true for reads.
        """
        raise NotImplementedError()

    def __eq__(self, other: object) -> bool:
        try:
            return self.key == cast(Edge, other).key
        except AttributeError:
            return NotImplemented

    def __hash__(self) -> int:
        return hash(self.key)

    def __repr__(self) -> str:
        return f"{self.key[0]} -> {self.key[1]}"

    @property
    def dataset_type_name(self) -> str:
        """Dataset type name seen by the task.

        This defaults to the parent dataset type name, which is appropriate
        for all writes and most reads.
        """
        return self.parent_dataset_type_name

    def adapt_dataset_type(self, dataset_type: DatasetType) -> DatasetType:
        """Transform the graph's definition of a dataset type (parent, with the
        registry or producer's storage class) to the one seen by this task.
        """
        raise NotImplementedError()

    @abstractmethod
    def adapt_dataset_ref(self, ref: DatasetRef) -> DatasetRef:
        """Transform the graph's definition of a dataset reference (parent
        dataset type, with the registry or producer's storage class) to the one
        seen by this task.
        """
        raise NotImplementedError()

    def _check_dataset_type(
        self, xgraph: networkx.DiGraph, dataset_type_node: DatasetTypeNode
    ) -> DatasetTypeNode | None:
        """Check the a potential graph-wide definition of a dataset type for
        consistency with this edge.

        Parameters
        -----------
        xgraph : `networkx.DiGraph`
            Directed bipartite graph representing the full pipeline.
        dataset_type_node : `DatasetTypeNode`
            Dataset type node to be checked and possibly updated.

        Returns
        -------
        updated : `bool`
            New `DatasetTypeNode` if it needs to be changed, or `None` if it
            does not.
        """
        pass

    def _serialize(self, **kwargs: Any) -> dict[str, Any]:
        """Serialize the content of this edge into a dictionary of built-in
        objects suitable for JSON conversion.

        This should not include the edge's parent dataset type and task label,
        as it is always serialized in a context that already identifies those.
        """
        return {
            "storage_class_name": self.storage_class_name,
            "connection_name": self._connection_name,
            **kwargs,
        }
