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

__all__ = ("DatasetTypeNode",)

import dataclasses
from collections.abc import Callable, Collection
from typing import TYPE_CHECKING, Any

import networkx

from lsst.daf.butler import DatasetRef, DatasetType, DimensionGroup, DimensionUniverse, StorageClass

from ._exceptions import DuplicateOutputError
from ._nodes import NodeKey, NodeType

if TYPE_CHECKING:
    from ._edges import ReadEdge, WriteEdge


@dataclasses.dataclass(frozen=True, eq=False)
class DatasetTypeNode:
    """A node in a pipeline graph that represents a resolved dataset type.

    Notes
    -----
    A dataset type node represents a common definition of the dataset type
    across the entire graph - it is never a component, and the storage class is
    the registry dataset type's storage class or (if there isn't one) the one
    defined by the producing task.

    Dataset type nodes are intentionally not equality comparable, since there
    are many different (and useful) ways to compare these objects with no clear
    winner as the most obvious behavior.
    """

    dataset_type: DatasetType
    """Common definition of this dataset type for the graph.
    """

    is_initial_query_constraint: bool
    """Whether this dataset should be included as a constraint in the initial
    query for data IDs in QuantumGraph generation.

    This is only `True` for dataset types that are overall regular inputs, and
    only if none of those input connections had ``deferQueryConstraint=True``.
    """

    is_prerequisite: bool
    """Whether this dataset type is a prerequisite input that must exist in
    the Registry before graph creation.
    """

    producing_edge: WriteEdge | None
    """The edge to the task that produces this dataset type."""

    consuming_edges: Collection[ReadEdge]
    """The edges to tasks that consume this dataset type."""

    @classmethod
    def _from_edges(
        cls,
        key: NodeKey,
        xgraph: networkx.MultiDiGraph,
        get_registered: Callable[[str], DatasetType | None],
        dimensions: DimensionUniverse,
        previous: DatasetTypeNode | None,
        visualization_only: bool = False,
    ) -> DatasetTypeNode:
        """Construct a dataset type node from its edges.

        Parameters
        ----------
        key : `NodeKey`
            Named tuple that holds the dataset type and serves as the node
            object in the internal networkx graph.
        xgraph : `networkx.MultiDiGraph`
            The internal networkx graph.
        get_registered : `~collections.abc.Callable` or `None`
            Callable that takes a dataset type name and returns the
            `DatasetType` registered in the data repository, or `None` if it is
            not registered.
        dimensions : `lsst.daf.butler.DimensionUniverse`
            Definitions of all dimensions.
        previous : `DatasetTypeNode` or `None`
            Previous node for this dataset type.
        visualization_only : `bool`, optional
            Resolve the graph as well as possible even when dimensions and
            storage classes cannot really be determined.  This can include
            using the ``universe.commonSkyPix`` as the assumed dimensions of
            connections that use the "skypix" placeholder and using "<UNKNOWN>"
            as a storage class name (which will fail if the storage class
            itself is ever actually loaded).

        Returns
        -------
        node : `DatasetTypeNode`
            Node consistent with all edges pointing to it and the data
            repository.
        """
        dataset_type = get_registered(key.name) if get_registered is not None else None
        is_registered = dataset_type is not None
        if previous is not None and previous.dataset_type == dataset_type:
            # This node was already resolved (with exactly the same edges
            # contributing, since we clear resolutions when edges are added or
            # removed).  The only thing that might have changed was the
            # definition in the registry, and it didn't.
            return previous
        is_initial_query_constraint = True
        is_prerequisite: bool | None = None
        producer: str | None = None
        producing_edge: WriteEdge | None = None
        # Iterate over the incoming edges to this node, which represent the
        # output connections of tasks that write this dataset type; these take
        # precedence over the inputs in determining the graph-wide dataset type
        # definition (and hence which storage class we register when using the
        # graph to register dataset types).  There should only be one such
        # connection, but we won't necessarily have checked that rule until
        # here.  As a result there can be at most one iteration of this loop.
        for _, _, producing_edge in xgraph.in_edges(key, data="instance"):
            assert producing_edge is not None, "Should only be None if we never loop."
            if producer is not None:
                raise DuplicateOutputError(
                    f"Dataset type {key.name!r} is produced by both {producing_edge.task_label!r} "
                    f"and {producer!r}."
                )
            producer = producing_edge.task_label
            dataset_type = producing_edge._resolve_dataset_type(dataset_type, universe=dimensions)
            is_prerequisite = False
            is_initial_query_constraint = False
        consuming_edge: ReadEdge
        consumers: list[str] = []
        consuming_edges = list(
            consuming_edge for _, _, consuming_edge in xgraph.out_edges(key, data="instance")
        )
        # Put edges that are not component datasets before any edges that are.
        consuming_edges.sort(key=lambda consuming_edge: consuming_edge.component is not None)
        for consuming_edge in consuming_edges:
            dataset_type, is_initial_query_constraint, is_prerequisite = consuming_edge._resolve_dataset_type(
                current=dataset_type,
                universe=dimensions,
                is_initial_query_constraint=is_initial_query_constraint,
                is_prerequisite=is_prerequisite,
                is_registered=is_registered,
                producer=producer,
                consumers=consumers,
                visualization_only=visualization_only,
            )
            consumers.append(consuming_edge.task_label)
        assert dataset_type is not None, "Graph structure guarantees at least one edge."
        assert is_prerequisite is not None, "Having at least one edge guarantees is_prerequisite is known."
        return DatasetTypeNode(
            dataset_type=dataset_type,
            is_initial_query_constraint=is_initial_query_constraint,
            is_prerequisite=is_prerequisite,
            producing_edge=producing_edge,
            consuming_edges=tuple(consuming_edges),
        )

    @property
    def name(self) -> str:
        """Name of the dataset type.

        This is always the parent dataset type, never that of a component.
        """
        return self.dataset_type.name

    @property
    def key(self) -> NodeKey:
        """Key that identifies this dataset type in internal and exported
        networkx graphs.
        """
        return NodeKey(NodeType.DATASET_TYPE, self.dataset_type.name)

    @property
    def dimensions(self) -> DimensionGroup:
        """Dimensions of the dataset type."""
        return self.dataset_type.dimensions

    @property
    def storage_class_name(self) -> str:
        """String name of the storage class for this dataset type."""
        return self.dataset_type.storageClass_name

    @property
    def storage_class(self) -> StorageClass:
        """Storage class for this dataset type."""
        return self.dataset_type.storageClass

    @property
    def is_calibration(self) -> bool:
        """Whether this dataset type can be included in
        `~lsst.daf.butler.CollectionType.CALIBRATION` collections.
        """
        return self.dataset_type.isCalibration()

    def __repr__(self) -> str:
        return f"{self.name} ({self.storage_class_name}, {self.dimensions})"

    def generalize_ref(self, ref: DatasetRef) -> DatasetRef:
        """Convert a `~lsst.daf.butler.DatasetRef` with the dataset type
        associated with some task to one with the common dataset type defined
        by this node.

        Parameters
        ----------
        ref : `lsst.daf.butler.DatasetRef`
            Reference whose dataset type is convertible to this node's, either
            because it is a component with the node's dataset type as its
            parent, or because it has a compatible storage class.

        Returns
        -------
        ref : `lsst.daf.butler.DatasetRef`
            Reference with exactly this node's dataset type.
        """
        if ref.isComponent():
            ref = ref.makeCompositeRef()
        if ref.datasetType.storageClass_name != self.dataset_type.storageClass_name:
            return ref.overrideStorageClass(self.dataset_type.storageClass_name)
        return ref

    def _to_xgraph_state(self) -> dict[str, Any]:
        """Convert this node's attributes into a dictionary suitable for use
        in exported networkx graphs.
        """
        return {
            "dataset_type": self.dataset_type,
            "is_initial_query_constraint": self.is_initial_query_constraint,
            "is_prerequisite": self.is_prerequisite,
            "dimensions": self.dimensions,
            "storage_class_name": self.dataset_type.storageClass_name,
            "bipartite": NodeType.DATASET_TYPE.bipartite,
        }
