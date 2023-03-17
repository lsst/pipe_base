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
    "DatasetTypeNode",
    "ResolvedDatasetTypeNode",
)

from typing import TYPE_CHECKING, Any

import networkx
from lsst.daf.butler import DatasetRef, DatasetType, Registry
from lsst.daf.butler.registry import MissingDatasetTypeError

from ._abcs import Node, NodeKey

if TYPE_CHECKING:
    from ._edges import ReadEdge, WriteEdge
    from ._tasks import TaskNode


class DatasetTypeNode(Node):
    """A node in a pipeline graph that represents a dataset type.

    Parameters
    ----------
    node : `NodeKey`
        Key for this node in the graph.

    Notes
    -----
    This class only holds information that can be pulled unambiguously from
    `.PipelineTask` a single definitions, without input from the data
    repository or other tasks - which amounts to just the parent dataset type
    name.  The `ResolvedDatasetTypeNode` subclass also includes information
    from the data repository and holds an actual `DatasetType` instance.

    A dataset type node represents a common definition of the dataset type
    across the entire graph, which means it never refers to a component.

    Dataset type nodes are intentionally not equality comparable, since there
    are many different (and useful) ways to compare its resolved variant, with
    no clear winner as the most obvious behavior.
    """

    @property
    def name(self) -> str:
        """Name of the dataset type.

        This is always the parent dataset type, never that of a component.
        """
        return str(self.key)

    def _resolved(self, xgraph: networkx.DiGraph, registry: Registry) -> ResolvedDatasetTypeNode:
        # Docstring inherited.
        try:
            dataset_type = registry.getDatasetType(self.name)
            in_data_repo = True
        except MissingDatasetTypeError:
            dataset_type = None
            in_data_repo = False
        is_initial_query_constraint = True
        is_prerequisite: bool | None = None
        producer: str | None = None
        write_edge: WriteEdge
        for _, _, write_edge in xgraph.in_edges(self.key, data="instance"):  # will iterate zero or one time
            task_node: TaskNode = xgraph.nodes[write_edge.task_key]["instance"]
            task_node_data = task_node._get_imported_data()
            dataset_type = write_edge._resolve_dataset_type(
                connection=task_node_data.connection_map[write_edge._connection_name],
                current=dataset_type,
                universe=registry.dimensions,
            )
            is_prerequisite = False
            is_initial_query_constraint = False
        read_edge: ReadEdge
        consumers: list[str] = []
        for _, _, read_edge in xgraph.out_edges(self.key, data="instance"):
            task_node = xgraph.nodes[read_edge.task_key]["instance"]
            task_node_data = task_node._get_imported_data()
            dataset_type, is_initial_query_constraint, is_prerequisite = read_edge._resolve_dataset_type(
                connection=task_node_data.connection_map[read_edge._connection_name],
                current=dataset_type,
                universe=registry.dimensions,
                is_initial_query_constraint=is_initial_query_constraint,
                is_prerequisite=is_prerequisite,
                in_data_repo=in_data_repo,
                producer=producer,
                consumers=consumers,
            )
            consumers.append(read_edge.task_label)
        assert dataset_type is not None, "Graph structure guarantees at least one edge."
        assert is_prerequisite is not None, "Having at least one edge guarantees is_prerequisite is known."
        return ResolvedDatasetTypeNode(
            key=self.key,
            dataset_type=dataset_type,
            is_initial_query_constraint=is_initial_query_constraint,
            is_prerequisite=is_prerequisite,
        )

    def _unresolved(self) -> DatasetTypeNode:
        # Docstring inherited.
        return self

    def _serialize(self, **kwargs: Any) -> dict[str, Any]:
        # Docstring inherited.
        return kwargs

    def _to_xgraph_state(self, import_tasks: bool) -> dict[str, Any]:
        # Docstring inherited.
        return {}


class ResolvedDatasetTypeNode(DatasetTypeNode):
    """A node in a resolved pipeline graph that represents a dataset type.

    Parameters
    ----------
    node : `NodeKey`
        Key for this node in the graph.
    is_prerequisite: `bool`
        Whether this dataset type is a prerequisite input that must exist in
        the Registry before graph creation.
    dataset_type : `DatasetType`
        Common definition of this dataset type for the graph.
    is_initial_query_constraint : `bool`
        Whether this dataset should be included as a constraint in the initial
        query for data IDs in QuantumGraph generation.

        This is only `True` for dataset types that are overall regular inputs,
        and also `False` if all such connections had
        ``deferQueryConstraint=True``.

    Notes
    -----
    A dataset type node represents a common definition of the dataset type
    across the entire graph - it is never a component, and when storage class
    information is present (in `ResolvedDatasetTypeNode`) this is the registry
    dataset type's storage class or (if there isn't one) the one defined by the
    producing task.

    Dataset type nodes are intentionally not equality comparable, since there
    are many different (and useful) ways to compare these objects with no clear
    winner as the most obvious behavior.
    """

    def __init__(
        self,
        key: NodeKey,
        *,
        is_prerequisite: bool,
        dataset_type: DatasetType,
        is_initial_query_constraint: bool,
    ):
        super().__init__(key)
        self.dataset_type = dataset_type
        self.is_initial_query_constraint = is_initial_query_constraint
        self.is_prerequisite = is_prerequisite

    dataset_type: DatasetType
    """Common definition of this dataset type for the graph.
    """

    is_initial_query_constraint: bool
    """Whether this dataset should be included as a constraint in the initial
    query for data IDs in QuantumGraph generation.

    This is only `True` for dataset types that are overall regular inputs, and
    also `False` if all such connections had ``deferQueryConstraint=True``.
    """

    is_prerequisite: bool
    """Whether this dataset type is a prerequisite input that must exist in
    the Registry before graph creation.
    """

    def _resolved(self, xgraph: networkx.DiGraph, registry: Registry) -> ResolvedDatasetTypeNode:
        # Docstring inherited.
        return self

    def _unresolved(self) -> DatasetTypeNode:
        # Docstring inherited.
        return DatasetTypeNode(key=self.key)

    def generalize_ref(self, ref: DatasetRef) -> DatasetRef:
        """Convert a `~lsst.daf.butler.DatasetRef` with the dataset type
        associated with some task to one with the common dataset type defined
        by this node.
        """
        if ref.isComponent():
            ref = ref.makeCompositeRef()
        if ref.datasetType.storageClass_name != self.dataset_type.storageClass_name:
            return ref.overrideStorageClass(self.dataset_type.storageClass_name)
        return ref

    def _serialize(self, **kwargs: Any) -> dict[str, Any]:
        # Docstring inherited.
        return super()._serialize(
            dimensions=list(self.dataset_type.dimensions.names),
            storage_class_name=self.dataset_type.storageClass_name,
            is_calibration=self.dataset_type.isCalibration(),
            is_initial_query_constraint=self.is_initial_query_constraint,
            is_prerequisite=self.is_prerequisite,
            **kwargs,
        )

    def _to_xgraph_state(self, import_tasks: bool) -> dict[str, Any]:
        # Docstring inherited.
        return {
            "dataset_type": self.dataset_type,
            "is_initial_query_constraint": self.is_initial_query_constraint,
            "is_prerequisite": self.is_prerequisite,
            "dimensions": self.dataset_type.dimensions,
        }
