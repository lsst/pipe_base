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

__all__ = ("WriteEdge", "ReadEdge")

from collections.abc import Mapping, Sequence
from typing import Any

import networkx
from lsst.daf.butler import DatasetRef, DatasetType, DimensionUniverse
from lsst.daf.butler.registry import MissingDatasetTypeError

from ..connectionTypes import BaseConnection
from ._abcs import Edge, NodeKey, NodeType
from ._dataset_types import DatasetTypeNode
from ._exceptions import ConnectionTypeConsistencyError, DuplicateOutputError, IncompatibleDatasetTypeError


class ReadEdge(Edge):
    """Representation of an input connection (including init-inputs and
    prerequisites) in a pipeline graph.

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
    is_prerequisite : `bool`
        Whether this dataset must be present in the data repository prior to
        `QuantumGraph` generation.
    connection_name : `str`
        Internal name for the connection as seen by the task.
    component : `str` or `None`
        Component of the dataset type requested by the task.
    """

    def __init__(
        self,
        dataset_type_key: NodeKey,
        task_key: NodeKey,
        *,
        storage_class_name: str,
        is_init: bool,
        is_prerequisite: bool,
        connection_name: str,
        component: str | None,
    ):
        super().__init__(
            task_key=task_key,
            dataset_type_key=dataset_type_key,
            storage_class_name=storage_class_name,
            is_init=is_init,
            connection_name=connection_name,
        )
        self.is_prerequisite = is_prerequisite
        self.component = component

    component: str | None
    """Component to add to `parent_dataset_type_name` to form the dataset type
    name seen by this task.
    """

    is_prerequisite: bool
    """Whether this dataset must be present in the data repository prior to
    `QuantumGraph` generation.
    """

    @property
    def key(self) -> tuple[NodeKey, NodeKey]:
        # Docstring inherited.
        return (self.dataset_type_key, self.task_key)

    @property
    def dataset_type_name(self) -> str:
        """Complete dataset type name, as seen by the task."""
        if self.component is not None:
            return f"{self.parent_dataset_type_name}.{self.component}"
        return self.parent_dataset_type_name

    def adapt_dataset_type(self, dataset_type: DatasetType) -> DatasetType:
        # Docstring inherited.
        if self.component is not None:
            assert (
                self.storage_class_name == dataset_type.storageClass_name
            ), "components with storage class overrides are not supported"
            return dataset_type.makeComponentDatasetType(self.component)
        if self.storage_class_name != dataset_type.storageClass_name:
            return dataset_type.overrideStorageClass(self.storage_class_name)
        return dataset_type

    def adapt_dataset_ref(self, ref: DatasetRef) -> DatasetRef:
        # Docstring inherited.
        if self.component is not None:
            assert (
                self.storage_class_name == ref.datasetType.storageClass_name
            ), "components with storage class overrides are not supported"
            return ref.makeComponentRef(self.component)
        if self.storage_class_name != ref.datasetType.storageClass_name:
            return ref.overrideStorageClass(self.storage_class_name)
        return ref

    @classmethod
    def _from_connection_map(
        cls,
        task_key: NodeKey,
        connection_name: str,
        connection_map: Mapping[str, BaseConnection],
        is_prerequisite: bool = False,
    ) -> ReadEdge:
        """Construct a `ReadEdge` instance from a `BaseConnection` object.

        Parameters
        ----------
        task_key : `NodeKey`
            Key for the associated task node or task init node.
        connection_name : `str`
            Internal name for the connection as seen by the task,.
        connection_map : Mapping [ `str`, `BaseConnection` ]
            Mapping of post-configuration object to draw dataset type
            information from, keyed by connection name.
        is_prerequisite : `bool`, optional
            Whether this dataset must be present in the data repository prior
            to `QuantumGraph` generation.

        Returns
        -------
        edge : `ReadEdge`
            New edge instance.
        """
        # Docstring inherited.
        connection = connection_map[connection_name]
        parent_dataset_type_name, component = DatasetType.splitDatasetTypeName(connection.name)
        return cls(
            dataset_type_key=NodeKey(NodeType.DATASET_TYPE, parent_dataset_type_name),
            task_key=task_key,
            component=component,
            storage_class_name=connection.storageClass,
            is_init=task_key.node_type is NodeType.TASK_INIT,
            is_prerequisite=is_prerequisite,
            connection_name=connection_name,
        )

    def _resolve_dataset_type(
        self,
        *,
        connection: BaseConnection,
        current: DatasetType | None,
        is_initial_query_constraint: bool,
        is_prerequisite: bool | None,
        universe: DimensionUniverse,
        producer: str | None,
        consumers: Sequence[str],
        in_data_repo: bool,
    ) -> tuple[DatasetType, bool, bool]:
        """Participate in the construction of the graph-wide `DatasetType`
        object associated with this edge.

        Parameters
        ----------
        connection : `.BaseConnection`
            Object provided by the task to describe this edge, or `None` if the
            edge was added by the framework.
        current : `lsst.daf.butler.DatasetType` or `None`
            The current graph-wide `DatasetType`, or `None`.  This will always
            be the registry's definition of the parent dataset type, if one
            exists.  If not, it will be the dataset type definition from the
            task in the graph that writes it, if there is one.  If there is no
            such task (or this edge represents that task), this will be `None`.
        is_initial_query_constraint : `bool`
            Whether this dataset type is currently marked as a constraint on
            the initial data ID query in QuantumGraph generation.
        is_prerequisite : `bool` | None`
            Whether this dataset type is marked as a prerequisite input in all
            edges processed so far.  `None` if this is the first edge.
        universe : `lsst.daf.butler.DimensionUniverse`
            Object that holds all dimension definitions.
        producer : `str` or `None`
            The label of the task that produces this dataset type in the
            pipeline, or `None` if it is an overall input or this is the
            producing task.
        consumers : `Sequence` [ `str` ]
            Labels for other consuming tasks that have already participated in
            this dataset type's resolution.
        in_data_repo : `bool`
            Whether are registration for this dataset type was found in the
            data repository.

        Returns
        -------
        dataset_type : `DatasetType`
            The updated graph-wide dataset type.  If ``current`` was provided,
            this must be equal to it.
        initial_query_constraint : `bool`
            If `True`, this task recommends that this dataset type not be
            included as a constraint in the initial data ID query during
            QuantumGraph generation.
        is_prerequisite : `bool`
            If `True`, this task consider this dataset type a prerequisite.

        Raises
        ------
        MissingDatasetTypeError
            Raised if ``current is None`` and this edge cannot define one on
            its own.
        IncompatibleDatasetTypeError
            Raised if ``current is not None`` and this edge's definition is not
            compatible with it.
        ConnectionTypeConsistencyError
            Raised if the dataset type node's ``is_init`` or
            ``is_prerequisite`` flags are inconsistent with this edge.
        """
        dimensions = connection.resolve_dimensions(
            universe,
            current.dimensions if current is not None else None,
            self.task_label,
        )
        # Only Input connections have a deferQueryConstraint attribute, but
        # InitInput and PrerequisiteInputs are never initial query constraints.
        # And all Input connections in the graph need to say
        # deferQueryConstraint=True for it to have an effect.
        is_initial_query_constraint = is_initial_query_constraint and not getattr(
            connection, "deferQueryConstraint", True
        )
        if is_prerequisite is None:
            is_prerequisite = self.is_prerequisite
        elif is_prerequisite and not self.is_prerequisite:
            raise ConnectionTypeConsistencyError(
                f"Dataset type {self.parent_dataset_type_name!r} is a prerequisite input to {consumers}, "
                f"but it is not a prerequisite to {self.task_label!r}."
            )
        elif not is_prerequisite and self.is_prerequisite:
            if producer is not None:
                raise ConnectionTypeConsistencyError(
                    f"Dataset type {self.parent_dataset_type_name!r} is a prerequisite input to "
                    f"{self.task_label}, but it is produced by {producer!r}."
                )
            else:
                raise ConnectionTypeConsistencyError(
                    f"Dataset type {self.parent_dataset_type_name!r} is a prerequisite input to "
                    f"{self.task_label}, but it is a regular input to {consumers!r}."
                )

        def report_current_origin() -> str:
            if in_data_repo:
                return "data repository"
            elif producer is not None:
                return f"producing task {producer!r}"
            else:
                return f"consuming task(s) {consumers!r}"

        if self.component is not None:
            if current is None:
                raise MissingDatasetTypeError(
                    f"Dataset type {self.parent_dataset_type_name!r} is not registered and not produced by "
                    f"this pipeline, but it used by task {self.task_label!r}, via component "
                    f"{self.component!r}. This pipeline cannot be resolved until the parent dataset type is "
                    "registered."
                )
            all_current_components = current.storageClass.allComponents()
            if self.component not in all_current_components:
                raise IncompatibleDatasetTypeError(
                    f"Dataset type {self.parent_dataset_type_name!r} has storage class "
                    f"{current.storageClass_name!r} (from {report_current_origin()}), "
                    f"which does not include component {self.component!r} "
                    f"as requested by task {self.task_label!r}."
                )
            if all_current_components[self.component].name != connection.storageClass:
                raise IncompatibleDatasetTypeError(
                    f"Dataset type '{self.parent_dataset_type_name}.{self.component}' has storage class "
                    f"{all_current_components[self.component].name!r} "
                    f"(from {report_current_origin()}), which does not match "
                    f"{connection.storageClass!r}, as requested by task {self.task_label!r}. "
                    "Note that storage class conversions of components are not supported."
                )
            return current, is_initial_query_constraint, is_prerequisite
        else:
            dataset_type = DatasetType(
                self.parent_dataset_type_name,
                dimensions,
                storageClass=connection.storageClass,
                isCalibration=connection.isCalibration,
            )
            if current is not None:
                if not dataset_type.is_compatible_with(current):
                    raise IncompatibleDatasetTypeError(
                        f"Incompatible definition for input dataset type {self.parent_dataset_type_name!r}; "
                        f"task {self.task_label!r} has {dataset_type}, but the definition "
                        f"from {report_current_origin()} is {current}."
                    )
                return current, is_initial_query_constraint, is_prerequisite
            else:
                return dataset_type, is_initial_query_constraint, is_prerequisite

    def _serialize(self, **kwargs: Any) -> dict[str, Any]:
        # Docstring inherited.
        result = super()._serialize(**kwargs)
        if self.component is not None:
            result["component"] = self.component
        return result

    def _to_xgraph_state(self) -> dict[str, Any]:
        result = super()._to_xgraph_state()
        result["component"] = self.component
        result["is_prerequisite"] = self.is_prerequisite
        return result


class WriteEdge(Edge):
    """Representation of an output connection (including init-outputs) in a
    pipeline graph.
    """

    @property
    def key(self) -> tuple[NodeKey, NodeKey]:
        # Docstring inherited.
        return (self.task_key, self.dataset_type_key)

    def adapt_dataset_type(self, dataset_type: DatasetType) -> DatasetType:
        # Docstring inherited.
        if self.storage_class_name != dataset_type.storageClass_name:
            return dataset_type.overrideStorageClass(self.storage_class_name)
        return dataset_type

    def adapt_dataset_ref(self, ref: DatasetRef) -> DatasetRef:
        # Docstring inherited.
        if self.storage_class_name != ref.datasetType.storageClass_name:
            return ref.overrideStorageClass(self.storage_class_name)
        return ref

    @classmethod
    def _from_connection_map(
        cls,
        task_key: NodeKey,
        connection_name: str,
        connection_map: Mapping[str, BaseConnection],
    ) -> WriteEdge:
        """Construct a `WriteEdge` instance from a `BaseConnection` object.

        Parameters
        ----------
        task_key : `NodeKey`
            Key for the associated task node or task init node.
        connection_name : `str`
            Internal name for the connection as seen by the task,.
        connection_map : Mapping [ `str`, `BaseConnection` ]
            Mapping of post-configuration object to draw dataset type
            information from, keyed by connection name.

        Returns
        -------
        edge : `WriteEdge`
            New edge instance.
        """
        # Docstring inherited.
        connection = connection_map[connection_name]
        parent_dataset_type_name, component = DatasetType.splitDatasetTypeName(connection.name)
        if component is not None:
            raise ValueError(
                f"Illegal output component dataset {connection.name!r} in task {task_key.name!r}."
            )
        return cls(
            task_key=task_key,
            dataset_type_key=NodeKey(NodeType.DATASET_TYPE, parent_dataset_type_name),
            storage_class_name=connection.storageClass,
            is_init=task_key.node_type is NodeType.TASK_INIT,
            connection_name=connection_name,
        )

    def _check_dataset_type(self, xgraph: networkx.DiGraph, dataset_type_node: DatasetTypeNode) -> None:
        # Docstring inherited.
        for existing_producer in xgraph.predecessors(dataset_type_node.key):
            raise DuplicateOutputError(
                f"Dataset type {dataset_type_node.name} is produced by both {self.task_label!r} "
                f"and {existing_producer!r}."
            )

    def _resolve_dataset_type(
        self, *, connection: BaseConnection, current: DatasetType | None, universe: DimensionUniverse
    ) -> DatasetType:
        """Participate in the construction of the graph-wide `DatasetType`
        object associated with this edge.

        Parameters
        ----------
        connection : `.BaseConnection`
            Object provided by the task to describe this edge, or `None` if the
            edge was added by the framework.
        current : `lsst.daf.butler.DatasetType` or `None`
            The current graph-wide `DatasetType`, or `None`.  This will always
            be the registry's definition of the parent dataset type, if one
            exists.  If not, it will be the dataset type definition from the
            task in the graph that writes it, if there is one.  If there is no
            such task (or this edge represents that task), this will be `None`.
        universe : `lsst.daf.butler.DimensionUniverse`
            Object that holds all dimension definitions.

        Returns
        -------
        dataset_type : `DatasetType`
            The updated graph-wide dataset type.  If ``current`` was provided,
            this must be equal to it.

        Raises
        ------
        IncompatibleDatasetTypeError
            Raised if ``current is not None`` and this edge's definition is not
            compatible with it.
        """
        dimensions = connection.resolve_dimensions(
            universe,
            current.dimensions if current is not None else None,
            self.task_label,
        )
        dataset_type = DatasetType(
            self.parent_dataset_type_name,
            dimensions,
            storageClass=connection.storageClass,
            isCalibration=connection.isCalibration,
        )
        if current is not None:
            if not current.is_compatible_with(dataset_type):
                raise IncompatibleDatasetTypeError(
                    f"Incompatible definition for output dataset type {self.parent_dataset_type_name!r}: "
                    f"task {self.task_label!r} has {current}, but data repository has {dataset_type}."
                )
            return current
        else:
            return dataset_type
