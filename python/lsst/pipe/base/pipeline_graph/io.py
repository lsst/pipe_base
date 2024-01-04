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

__all__ = (
    "expect_not_none",
    "SerializedEdge",
    "SerializedTaskInitNode",
    "SerializedTaskNode",
    "SerializedDatasetTypeNode",
    "SerializedTaskSubset",
    "SerializedPipelineGraph",
)

from collections.abc import Mapping
from typing import Any, TypeVar

import networkx
import pydantic
from lsst.daf.butler import DatasetType, DimensionConfig, DimensionGroup, DimensionUniverse

from .. import automatic_connection_constants as acc
from ._dataset_types import DatasetTypeNode
from ._edges import Edge, ReadEdge, WriteEdge
from ._exceptions import PipelineGraphReadError
from ._nodes import NodeKey, NodeType
from ._pipeline_graph import PipelineGraph
from ._task_subsets import TaskSubset
from ._tasks import TaskImportMode, TaskInitNode, TaskNode

_U = TypeVar("_U")

_IO_VERSION_INFO = (0, 0, 1)
"""Version tuple embedded in saved PipelineGraphs.
"""


def expect_not_none(value: _U | None, msg: str) -> _U:
    """Check that a value is not `None` and return it.

    Parameters
    ----------
    value : `~typing.Any`
        Value to check.
    msg : `str`
        Error message for the case where ``value is None``.

    Returns
    -------
    value : `typing.Any`
        Value, guaranteed not to be `None`.

    Raises
    ------
    PipelineGraphReadError
        Raised with ``msg`` if ``value is None``.
    """
    if value is None:
        raise PipelineGraphReadError(msg)
    return value


class SerializedEdge(pydantic.BaseModel):
    """Struct used to represent a serialized `Edge` in a `PipelineGraph`.

    All `ReadEdge` and `WriteEdge` state not included here is instead
    effectively serialized by the context in which a `SerializedEdge` appears
    (e.g. the keys of the nested dictionaries in which it serves as the value
    type).
    """

    dataset_type_name: str
    """Full dataset type name (including component)."""

    storage_class: str
    """Name of the storage class."""

    raw_dimensions: list[str]
    """Raw dimensions of the dataset type from the task connections."""

    is_calibration: bool = False
    """Whether this dataset type can be included in
    `~lsst.daf.butler.CollectionType.CALIBRATION` collections."""

    defer_query_constraint: bool = False
    """If `True`, by default do not include this dataset type's existence as a
    constraint on the initial data ID query in QuantumGraph generation."""

    @classmethod
    def serialize(cls, target: Edge) -> SerializedEdge:
        """Transform an `Edge` to a `SerializedEdge`.

        Parameters
        ----------
        target : `Edge`
            The object to serialize.

        Returns
        -------
        `SerializedEdge`
            Model transformed into something that can be serialized.
        """
        return SerializedEdge.model_construct(
            storage_class=target.storage_class_name,
            dataset_type_name=target.dataset_type_name,
            raw_dimensions=sorted(target.raw_dimensions),
            is_calibration=target.is_calibration,
            defer_query_constraint=getattr(target, "defer_query_constraint", False),
        )

    def deserialize_read_edge(
        self,
        task_key: NodeKey,
        connection_name: str,
        dataset_type_keys: Mapping[str, NodeKey],
        is_prerequisite: bool = False,
    ) -> ReadEdge:
        """Transform a `SerializedEdge` to a `ReadEdge`.

        Parameters
        ----------
        task_key : `NodeKey`
            Key for the task node this edge is connected to.
        connection_name : `str`
            Internal name for the connection as seen by the task.
        dataset_type_keys : `~collections.abc.Mapping` [`str`, `NodeKey`]
            Mapping of dataset type name to node key.
        is_prerequisite : `bool`, optional
            Whether this dataset must be present in the data repository prior
            to `QuantumGraph` generation.

        Returns
        -------
        `ReadEdge`
            Deserialized object.
        """
        parent_dataset_type_name, component = DatasetType.splitDatasetTypeName(self.dataset_type_name)
        return ReadEdge(
            dataset_type_key=dataset_type_keys[parent_dataset_type_name],
            task_key=task_key,
            storage_class_name=self.storage_class,
            is_prerequisite=is_prerequisite,
            component=component,
            connection_name=connection_name,
            is_calibration=self.is_calibration,
            defer_query_constraint=self.defer_query_constraint,
            raw_dimensions=frozenset(self.raw_dimensions),
        )

    def deserialize_write_edge(
        self,
        task_key: NodeKey,
        connection_name: str,
        dataset_type_keys: Mapping[str, NodeKey],
    ) -> WriteEdge:
        """Transform a `SerializedEdge` to a `WriteEdge`.

        Parameters
        ----------
        task_key : `NodeKey`
            Key for the task node this edge is connected to.
        connection_name : `str`
            Internal name for the connection as seen by the task.
        dataset_type_keys : `~collections.abc.Mapping` [`str`, `NodeKey`]
            Mapping of dataset type name to node key.

        Returns
        -------
        `WriteEdge`
            Deserialized object.
        """
        return WriteEdge(
            task_key=task_key,
            dataset_type_key=dataset_type_keys[self.dataset_type_name],
            storage_class_name=self.storage_class,
            connection_name=connection_name,
            is_calibration=self.is_calibration,
            raw_dimensions=frozenset(self.raw_dimensions),
        )


class SerializedTaskInitNode(pydantic.BaseModel):
    """Struct used to represent a serialized `TaskInitNode` in a
    `PipelineGraph`.

    The task label is serialized by the context in which a
    `SerializedTaskInitNode` appears (e.g. the keys of the nested dictionary
    in which it serves as the value type), and the task class name and config
    string are save with the corresponding `SerializedTaskNode`.
    """

    inputs: dict[str, SerializedEdge]
    """Mapping of serialized init-input edges, keyed by connection name."""

    outputs: dict[str, SerializedEdge]
    """Mapping of serialized init-output edges, keyed by connection name."""

    config_output: SerializedEdge
    """The serialized config init-output edge."""

    index: int | None = None
    """The index of this node in the sorted sequence of `PipelineGraph`.

    This is `None` if the `PipelineGraph` was not sorted when it was
    serialized.
    """

    @classmethod
    def serialize(cls, target: TaskInitNode) -> SerializedTaskInitNode:
        """Transform a `TaskInitNode` to a `SerializedTaskInitNode`.

        Parameters
        ----------
        target : `TaskInitNode`
            Object to be serialized.

        Returns
        -------
        `SerializedTaskInitNode`
            Model that can be serialized.
        """
        return cls.model_construct(
            inputs={
                connection_name: SerializedEdge.serialize(edge)
                for connection_name, edge in sorted(target.inputs.items())
            },
            outputs={
                connection_name: SerializedEdge.serialize(edge)
                for connection_name, edge in sorted(target.outputs.items())
            },
            config_output=SerializedEdge.serialize(target.config_output),
        )

    def deserialize(
        self,
        key: NodeKey,
        task_class_name: str,
        config_str: str,
        dataset_type_keys: Mapping[str, NodeKey],
    ) -> TaskInitNode:
        """Transform a `SerializedTaskInitNode` to a `TaskInitNode`.

        Parameters
        ----------
        key : `NodeKey`
            Key that identifies this node in internal and exported networkx
            graphs.
        task_class_name : `str`, optional
            Fully-qualified name of the task class.  Must be provided if
            ``imported_data`` is not.
        config_str : `str`, optional
            Configuration for the task as a string of override statements.
        dataset_type_keys : `~collections.abc.Mapping` [`str`, `NodeKey`]
            Mapping of dataset type name to node key.

        Returns
        -------
        `TaskInitNode`
            Deserialized object.
        """
        return TaskInitNode(
            key,
            inputs={
                connection_name: serialized_edge.deserialize_read_edge(
                    key, connection_name, dataset_type_keys
                )
                for connection_name, serialized_edge in self.inputs.items()
            },
            outputs={
                connection_name: serialized_edge.deserialize_write_edge(
                    key, connection_name, dataset_type_keys
                )
                for connection_name, serialized_edge in self.outputs.items()
            },
            config_output=self.config_output.deserialize_write_edge(
                key, acc.CONFIG_INIT_OUTPUT_CONNECTION_NAME, dataset_type_keys
            ),
            task_class_name=task_class_name,
            config_str=config_str,
        )


class SerializedTaskNode(pydantic.BaseModel):
    """Struct used to represent a serialized `TaskNode` in a `PipelineGraph`.

    The task label is serialized by the context in which a
    `SerializedTaskNode` appears (e.g. the keys of the nested dictionary in
    which it serves as the value type).
    """

    task_class: str
    """Fully-qualified name of the task class."""

    init: SerializedTaskInitNode
    """Serialized task initialization node."""

    config_str: str
    """Configuration for the task as a string of override statements."""

    prerequisite_inputs: dict[str, SerializedEdge]
    """Mapping of serialized prerequisiste input edges, keyed by connection
    name.
    """

    inputs: dict[str, SerializedEdge]
    """Mapping of serialized input edges, keyed by connection name."""

    outputs: dict[str, SerializedEdge]
    """Mapping of serialized output edges, keyed by connection name."""

    metadata_output: SerializedEdge
    """The serialized metadata output edge."""

    dimensions: list[str]
    """The task's dimensions, if they were resolved."""

    log_output: SerializedEdge | None = None
    """The serialized log output edge."""

    index: int | None = None
    """The index of this node in the sorted sequence of `PipelineGraph`.

    This is `None` if the `PipelineGraph` was not sorted when it was
    serialized.
    """

    @classmethod
    def serialize(cls, target: TaskNode) -> SerializedTaskNode:
        """Transform a `TaskNode` to a `SerializedTaskNode`.

        Parameters
        ----------
        target : `TaskNode`
            Object to be serialized.

        Returns
        -------
        `SerializedTaskNode`
            Object tha can be serialized.
        """
        return cls.model_construct(
            task_class=target.task_class_name,
            init=SerializedTaskInitNode.serialize(target.init),
            config_str=target.get_config_str(),
            dimensions=list(target.raw_dimensions),
            prerequisite_inputs={
                connection_name: SerializedEdge.serialize(edge)
                for connection_name, edge in sorted(target.prerequisite_inputs.items())
            },
            inputs={
                connection_name: SerializedEdge.serialize(edge)
                for connection_name, edge in sorted(target.inputs.items())
            },
            outputs={
                connection_name: SerializedEdge.serialize(edge)
                for connection_name, edge in sorted(target.outputs.items())
            },
            metadata_output=SerializedEdge.serialize(target.metadata_output),
            log_output=(
                SerializedEdge.serialize(target.log_output) if target.log_output is not None else None
            ),
        )

    def deserialize(
        self,
        key: NodeKey,
        init_key: NodeKey,
        dataset_type_keys: Mapping[str, NodeKey],
        universe: DimensionUniverse | None,
    ) -> TaskNode:
        """Transform a `SerializedTaskNode` to a `TaskNode`.

        Parameters
        ----------
        key : `NodeKey`
            Key that identifies this node in internal and exported networkx
            graphs.
        init_key : `NodeKey`
            Key that identifies the init node in internal and exported networkx
            graphs.
        dataset_type_keys : `~collections.abc.Mapping` [`str`, `NodeKey`]
            Mapping of dataset type name to node key.
        universe : `~lsst.daf.butler.DimensionUniverse` or `None`
            The dimension universe.

        Returns
        -------
        `TaskNode`
            Deserialized object.
        """
        init = self.init.deserialize(
            init_key,
            task_class_name=self.task_class,
            config_str=expect_not_none(
                self.config_str, f"No serialized config file for task with label {key.name!r}."
            ),
            dataset_type_keys=dataset_type_keys,
        )
        inputs = {
            connection_name: serialized_edge.deserialize_read_edge(key, connection_name, dataset_type_keys)
            for connection_name, serialized_edge in self.inputs.items()
        }
        prerequisite_inputs = {
            connection_name: serialized_edge.deserialize_read_edge(
                key, connection_name, dataset_type_keys, is_prerequisite=True
            )
            for connection_name, serialized_edge in self.prerequisite_inputs.items()
        }
        outputs = {
            connection_name: serialized_edge.deserialize_write_edge(key, connection_name, dataset_type_keys)
            for connection_name, serialized_edge in self.outputs.items()
        }
        if (serialized_log_output := self.log_output) is not None:
            log_output = serialized_log_output.deserialize_write_edge(
                key, acc.LOG_OUTPUT_CONNECTION_NAME, dataset_type_keys
            )
        else:
            log_output = None
        metadata_output = self.metadata_output.deserialize_write_edge(
            key, acc.METADATA_OUTPUT_CONNECTION_NAME, dataset_type_keys
        )
        dimensions: frozenset[str] | DimensionGroup
        if universe is not None:
            dimensions = universe.conform(self.dimensions)
        else:
            dimensions = frozenset(self.dimensions)
        return TaskNode(
            key=key,
            init=init,
            inputs=inputs,
            prerequisite_inputs=prerequisite_inputs,
            outputs=outputs,
            log_output=log_output,
            metadata_output=metadata_output,
            dimensions=dimensions,
        )


class SerializedDatasetTypeNode(pydantic.BaseModel):
    """Struct used to represent a serialized `DatasetTypeNode` in a
    `PipelineGraph`.

    Unresolved dataset types are serialized as instances with at most the
    `index` attribute set, and are typically converted to JSON with pydantic's
    ``exclude_defaults=True`` option to keep this compact.

    The dataset typename is serialized by the context in which a
    `SerializedDatasetTypeNode` appears (e.g. the keys of the nested dictionary
    in which it serves as the value type).
    """

    dimensions: list[str] | None = None
    """Dimensions of the dataset type."""

    storage_class: str | None = None
    """Name of the storage class."""

    is_calibration: bool = False
    """Whether this dataset type is a calibration."""

    is_initial_query_constraint: bool = False
    """Whether this dataset type should be a query constraint during
    `QuantumGraph` generation."""

    is_prerequisite: bool = False
    """Whether datasets of this dataset type must exist in the input collection
    before `QuantumGraph` generation."""

    index: int | None = None
    """The index of this node in the sorted sequence of `PipelineGraph`.

    This is `None` if the `PipelineGraph` was not sorted when it was
    serialized.
    """

    @classmethod
    def serialize(cls, target: DatasetTypeNode | None) -> SerializedDatasetTypeNode:
        """Transform a `DatasetTypeNode` to a `SerializedDatasetTypeNode`.

        Parameters
        ----------
        target : `DatasetTypeNode` or `None`
            Object to serialize.

        Returns
        -------
        `SerializedDatasetTypeNode`
            Object in serializable form.
        """
        if target is None:
            return cls.model_construct()
        return cls.model_construct(
            dimensions=list(target.dataset_type.dimensions.names),
            storage_class=target.dataset_type.storageClass_name,
            is_calibration=target.dataset_type.isCalibration(),
            is_initial_query_constraint=target.is_initial_query_constraint,
            is_prerequisite=target.is_prerequisite,
        )

    def deserialize(
        self, key: NodeKey, xgraph: networkx.MultiDiGraph, universe: DimensionUniverse | None
    ) -> DatasetTypeNode | None:
        """Transform a `SerializedDatasetTypeNode` to a `DatasetTypeNode`.

        Parameters
        ----------
        key : `NodeKey`
            Key that identifies this node in internal and exported networkx
            graphs.
        xgraph : `networkx.MultiDiGraph`
            <unknown>.
        universe : `~lsst.daf.butler.DimensionUniverse` or `None`
            The dimension universe.

        Returns
        -------
        `DatasetTypeNode`
            Deserialized object.
        """
        if self.dimensions is not None:
            dataset_type = DatasetType(
                key.name,
                expect_not_none(
                    self.dimensions,
                    f"Serialized dataset type {key.name!r} has no dimensions.",
                ),
                storageClass=expect_not_none(
                    self.storage_class,
                    f"Serialized dataset type {key.name!r} has no storage class.",
                ),
                isCalibration=self.is_calibration,
                universe=expect_not_none(
                    universe,
                    f"Serialized dataset type {key.name!r} has dimensions, "
                    "but no dimension universe was stored.",
                ),
            )
            producer: str | None = None
            producing_edge: WriteEdge | None = None
            for _, _, producing_edge in xgraph.in_edges(key, data="instance"):
                assert producing_edge is not None, "Should only be None if we never loop."
                if producer is not None:
                    raise PipelineGraphReadError(
                        f"Serialized dataset type {key.name!r} is produced by both "
                        f"{producing_edge.task_label!r} and {producer!r} in resolved graph."
                    )
                producer = producing_edge.task_label
            consuming_edges = tuple(
                consuming_edge for _, _, consuming_edge in xgraph.in_edges(key, data="instance")
            )
            return DatasetTypeNode(
                dataset_type=dataset_type,
                is_prerequisite=self.is_prerequisite,
                is_initial_query_constraint=self.is_initial_query_constraint,
                producing_edge=producing_edge,
                consuming_edges=consuming_edges,
            )
        return None


class SerializedTaskSubset(pydantic.BaseModel):
    """Struct used to represent a serialized `TaskSubset` in a `PipelineGraph`.

    The subsetlabel is serialized by the context in which a
    `SerializedDatasetTypeNode` appears (e.g. the keys of the nested dictionary
    in which it serves as the value type).
    """

    description: str
    """Description of the subset."""

    tasks: list[str]
    """Labels of tasks in the subset, sorted lexicographically for
    determinism.
    """

    @classmethod
    def serialize(cls, target: TaskSubset) -> SerializedTaskSubset:
        """Transform a `TaskSubset` into a `SerializedTaskSubset`.

        Parameters
        ----------
        target : `TaskSubset`
            Object to serialize.

        Returns
        -------
        `SerializedTaskSubset`
            Object in serializable form.
        """
        return cls.model_construct(description=target._description, tasks=list(sorted(target)))

    def deserialize_task_subset(self, label: str, xgraph: networkx.MultiDiGraph) -> TaskSubset:
        """Transform a `SerializedTaskSubset` into a `TaskSubset`.

        Parameters
        ----------
        label : `str`
            Subset label.
        xgraph : `networkx.MultiDiGraph`
            <unknown>.

        Returns
        -------
        `TaskSubset`
            Deserialized object.
        """
        members = set(self.tasks)
        return TaskSubset(xgraph, label, members, self.description)


class SerializedPipelineGraph(pydantic.BaseModel):
    """Struct used to represent a serialized `PipelineGraph`."""

    version: str = ".".join(str(v) for v in _IO_VERSION_INFO)
    """Serialization version."""

    description: str
    """Human-readable description of the pipeline."""

    tasks: dict[str, SerializedTaskNode] = pydantic.Field(default_factory=dict)
    """Mapping of serialized tasks, keyed by label."""

    dataset_types: dict[str, SerializedDatasetTypeNode] = pydantic.Field(default_factory=dict)
    """Mapping of serialized dataset types, keyed by parent dataset type name.
    """

    task_subsets: dict[str, SerializedTaskSubset] = pydantic.Field(default_factory=dict)
    """Mapping of task subsets, keyed by subset label."""

    dimensions: dict[str, Any] | None = None
    """Dimension universe configuration."""

    data_id: dict[str, Any] = pydantic.Field(default_factory=dict)
    """Data ID that constrains all quanta generated from this pipeline."""

    @classmethod
    def serialize(cls, target: PipelineGraph) -> SerializedPipelineGraph:
        """Transform a `PipelineGraph` into a `SerializedPipelineGraph`.

        Parameters
        ----------
        target : `PipelineGraph`
            Object to serialize.

        Returns
        -------
        `SerializedPipelineGraph`
            Object in serializable form.
        """
        result = SerializedPipelineGraph.model_construct(
            description=target.description,
            tasks={label: SerializedTaskNode.serialize(node) for label, node in target.tasks.items()},
            dataset_types={
                name: SerializedDatasetTypeNode().serialize(target.dataset_types.get_if_resolved(name))
                for name in target.dataset_types
            },
            task_subsets={
                label: SerializedTaskSubset.serialize(subset) for label, subset in target.task_subsets.items()
            },
            dimensions=target.universe.dimensionConfig.toDict() if target.universe is not None else None,
            data_id=target._raw_data_id,
        )
        if target._sorted_keys:
            for index, node_key in enumerate(target._sorted_keys):
                match node_key.node_type:
                    case NodeType.TASK:
                        result.tasks[node_key.name].index = index
                    case NodeType.DATASET_TYPE:
                        result.dataset_types[node_key.name].index = index
                    case NodeType.TASK_INIT:
                        result.tasks[node_key.name].init.index = index
        return result

    def deserialize(
        self,
        import_mode: TaskImportMode,
    ) -> PipelineGraph:
        """Transform a `SerializedPipelineGraph` into a `PipelineGraph`.

        Parameters
        ----------
        import_mode : `TaskImportMode`
            Import mode.

        Returns
        -------
        `PipelineGraph`
            Deserialized object.
        """
        universe: DimensionUniverse | None = None
        if self.dimensions is not None:
            universe = DimensionUniverse(
                config=DimensionConfig(
                    expect_not_none(
                        self.dimensions,
                        "Serialized pipeline graph has not been resolved; "
                        "load it is a MutablePipelineGraph instead.",
                    )
                )
            )
        xgraph = networkx.MultiDiGraph()
        sort_index_map: dict[int, NodeKey] = {}
        # Save the dataset type keys after the first time we make them - these
        # may be tiny objects, but it's still to have only one copy of each
        # value floating around the graph.
        dataset_type_keys: dict[str, NodeKey] = {}
        for dataset_type_name, serialized_dataset_type in self.dataset_types.items():
            dataset_type_key = NodeKey(NodeType.DATASET_TYPE, dataset_type_name)
            # We intentionally don't attach a DatasetTypeNode instance here
            # yet, since we need edges to do that and those are saved with
            # the tasks.
            xgraph.add_node(dataset_type_key, bipartite=NodeType.DATASET_TYPE.value)
            if serialized_dataset_type.index is not None:
                sort_index_map[serialized_dataset_type.index] = dataset_type_key
            dataset_type_keys[dataset_type_name] = dataset_type_key
        for task_label, serialized_task in self.tasks.items():
            task_key = NodeKey(NodeType.TASK, task_label)
            task_init_key = NodeKey(NodeType.TASK_INIT, task_label)
            task_node = serialized_task.deserialize(task_key, task_init_key, dataset_type_keys, universe)
            if serialized_task.index is not None:
                sort_index_map[serialized_task.index] = task_key
            if serialized_task.init.index is not None:
                sort_index_map[serialized_task.init.index] = task_init_key
            xgraph.add_node(task_key, instance=task_node, bipartite=NodeType.TASK.bipartite)
            xgraph.add_node(task_init_key, instance=task_node.init, bipartite=NodeType.TASK_INIT.bipartite)
            xgraph.add_edge(task_init_key, task_key, Edge.INIT_TO_TASK_NAME, instance=None)
            for read_edge in task_node.init.iter_all_inputs():
                xgraph.add_edge(
                    read_edge.dataset_type_key,
                    read_edge.task_key,
                    read_edge.connection_name,
                    instance=read_edge,
                )
            for write_edge in task_node.init.iter_all_outputs():
                xgraph.add_edge(
                    write_edge.task_key,
                    write_edge.dataset_type_key,
                    write_edge.connection_name,
                    instance=write_edge,
                )
            for read_edge in task_node.iter_all_inputs():
                xgraph.add_edge(
                    read_edge.dataset_type_key,
                    read_edge.task_key,
                    read_edge.connection_name,
                    instance=read_edge,
                )
            for write_edge in task_node.iter_all_outputs():
                xgraph.add_edge(
                    write_edge.task_key,
                    write_edge.dataset_type_key,
                    write_edge.connection_name,
                    instance=write_edge,
                )
        # Iterate over dataset types again to add instances.
        for dataset_type_name, serialized_dataset_type in self.dataset_types.items():
            dataset_type_key = dataset_type_keys[dataset_type_name]
            xgraph.nodes[dataset_type_key]["instance"] = serialized_dataset_type.deserialize(
                dataset_type_key, xgraph, universe
            )
        result = PipelineGraph.__new__(PipelineGraph)
        result._init_from_args(
            xgraph,
            sorted_keys=[sort_index_map[i] for i in range(len(xgraph))] if sort_index_map else None,
            task_subsets={
                subset_label: serialized_subset.deserialize_task_subset(subset_label, xgraph)
                for subset_label, serialized_subset in self.task_subsets.items()
            },
            description=self.description,
            universe=universe,
            data_id=self.data_id,
        )
        result._import_and_configure(import_mode)
        return result
