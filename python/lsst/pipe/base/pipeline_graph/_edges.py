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

__all__ = ("Edge", "ReadEdge", "WriteEdge")

from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from typing import Any, ClassVar, Self, TypeVar

from lsst.daf.butler import DatasetRef, DatasetType, DimensionUniverse, StorageClassFactory
from lsst.daf.butler.registry import MissingDatasetTypeError
from lsst.utils.classes import immutable

from ..connectionTypes import BaseConnection
from ._exceptions import ConnectionTypeConsistencyError, IncompatibleDatasetTypeError
from ._nodes import NodeKey, NodeType

_S = TypeVar("_S", bound="Edge")


@immutable
class Edge(ABC):
    """Base class for edges in a pipeline graph.

    This represents the link between a task node and an input or output dataset
    type.

    Parameters
    ----------
    task_key : `NodeKey`
        Key for the task node this edge is connected to.
    dataset_type_key : `NodeKey`
        Key for the dataset type node this edge is connected to.
    storage_class_name : `str`
        Name of the dataset type's storage class as seen by the task.
    connection_name : `str`
        Internal name for the connection as seen by the task.
    is_calibration : `bool`
        Whether this dataset type can be included in
        `~lsst.daf.butler.CollectionType.CALIBRATION` collections.
    raw_dimensions : `frozenset` [ `str` ]
        Raw dimensions from the connection definition.
    """

    def __init__(
        self,
        *,
        task_key: NodeKey,
        dataset_type_key: NodeKey,
        storage_class_name: str,
        connection_name: str,
        is_calibration: bool,
        raw_dimensions: frozenset[str],
    ):
        self.task_key = task_key
        self.dataset_type_key = dataset_type_key
        self.connection_name = connection_name
        self.storage_class_name = storage_class_name
        self.is_calibration = is_calibration
        self.raw_dimensions = raw_dimensions

    INIT_TO_TASK_NAME: ClassVar[str] = "INIT"
    """Edge key for the special edge that connects a task init node to the
    task node itself (for regular edges, this would be the connection name).
    """

    task_key: NodeKey
    """Task part of the key for this edge in networkx graphs."""

    dataset_type_key: NodeKey
    """Task part of the key for this edge in networkx graphs."""

    connection_name: str
    """Name used by the task to refer to this dataset type."""

    storage_class_name: str
    """Storage class expected by this task.

    If `ReadEdge.component` is not `None`, this is the component storage class,
    not the parent storage class.
    """

    is_calibration: bool
    """Whether this dataset type can be included in
    `~lsst.daf.butler.CollectionType.CALIBRATION` collections.
    """

    raw_dimensions: frozenset[str]
    """Raw dimensions in the task declaration.

    This can only be used safely for partial comparisons: two edges with the
    same ``raw_dimensions`` (and the same parent dataset type name) always have
    the same resolved dimensions, but edges with different ``raw_dimensions``
    may also have the same resolvd dimensions.
    """

    @property
    def is_init(self) -> bool:
        """Whether this dataset is read or written when the task is
        constructed, not when it is run.
        """
        return self.task_key.node_type is NodeType.TASK_INIT

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
    def nodes(self) -> tuple[NodeKey, NodeKey]:
        """The directed pair of `NodeKey` instances this edge connects.

        This tuple is ordered in the same direction as the pipeline flow:
        `task_key` precedes `dataset_type_key` for writes, and the
        reverse is true for reads.
        """
        raise NotImplementedError()

    @property
    def key(self) -> tuple[NodeKey, NodeKey, str]:
        """Ordered tuple of node keys and connection name that uniquely
        identifies this edge in a pipeline graph.
        """
        return self.nodes + (self.connection_name,)

    def __repr__(self) -> str:
        return f"{self.nodes[0]} -> {self.nodes[1]} ({self.connection_name})"

    @property
    def dataset_type_name(self) -> str:
        """Dataset type name seen by the task.

        This defaults to the parent dataset type name, which is appropriate
        for all writes and most reads.
        """
        return self.parent_dataset_type_name

    def diff(self: _S, other: _S, connection_type: str = "connection") -> list[str]:
        """Compare this edge to another one from a possibly-different
        configuration of the same task label.

        Parameters
        ----------
        other : `Edge`
            Another edge of the same type to compare to.
        connection_type : `str`
            Human-readable name of the connection type of this edge (e.g.
            "init input", "output") for use in returned messages.

        Returns
        -------
        differences : `list` [ `str` ]
            List of string messages describing differences between ``self`` and
            ``other``.  Will be empty if ``self == other`` or if the only
            difference is in the task label or connection name (which are not
            checked).  Messages will use 'A' to refer to ``self`` and 'B' to
            refer to ``other``.
        """
        result = []
        if self.dataset_type_name != other.dataset_type_name:
            result.append(
                f"{connection_type.capitalize()} {self.task_label}.{self.connection_name} has dataset type "
                f"{self.dataset_type_name!r} in A, but {other.dataset_type_name!r} in B."
            )
        if self.storage_class_name != other.storage_class_name:
            result.append(
                f"{connection_type.capitalize()} {self.task_label}.{self.connection_name} has storage class "
                f"{self.storage_class_name!r} in A, but {other.storage_class_name!r} in B."
            )
        if self.raw_dimensions != other.raw_dimensions:
            result.append(
                f"{connection_type.capitalize()} {self.task_label}.{self.connection_name} has raw dimensions "
                f"{set(self.raw_dimensions)} in A, but {set(other.raw_dimensions)} in B "
                "(differences in raw dimensions may not lead to differences in resolved dimensions, "
                "but this cannot be checked without re-resolving the dataset type)."
            )
        if self.is_calibration != other.is_calibration:
            result.append(
                f"{connection_type.capitalize()} {self.task_label}.{self.connection_name} is marked as a "
                f"calibration {'in A but not in B' if self.is_calibration else 'in B but not in A'}."
            )
        return result

    @abstractmethod
    def adapt_dataset_type(self, dataset_type: DatasetType) -> DatasetType:
        """Transform the graph's definition of a dataset type (parent, with the
        registry or producer's storage class) to the one seen by this task.

        Parameters
        ----------
        dataset_type : `~lsst.daf.butler.DatasetType`
            Graph's definition of dataset type.

        Returns
        -------
        out_dataset_type : `~lsst.daf.butler.DatasetType`
            Dataset type seen by this task.
        """
        raise NotImplementedError()

    @abstractmethod
    def adapt_dataset_ref(self, ref: DatasetRef) -> DatasetRef:
        """Transform the graph's definition of a dataset reference (parent
        dataset type, with the registry or producer's storage class) to the one
        seen by this task.

        Parameters
        ----------
        ref : `~lsst.daf.butler.DatasetRef`
            Graph's definition of the dataset reference.

        Returns
        -------
        out_dataset_ref : `~lsst.daf.butler.DatasetRef`
            Dataset reference seen by this task.
        """
        raise NotImplementedError()

    def _to_xgraph_state(self) -> dict[str, Any]:
        """Convert this edges's attributes into a dictionary suitable for use
        in exported networkx graphs.
        """
        return {
            "parent_dataset_type_name": self.parent_dataset_type_name,
            "storage_class_name": self.storage_class_name,
            "is_init": bool,
        }

    @classmethod
    def _unreduce(cls, kwargs: dict[str, Any]) -> Self:
        """Unpickle an `Edge` instance."""
        return cls(**kwargs)

    def __reduce__(self) -> tuple[Callable[[dict[str, Any]], Edge], tuple[dict[str, Any]]]:
        return (
            self._unreduce,
            (
                dict(
                    task_key=self.task_key,
                    dataset_type_key=self.dataset_type_key,
                    storage_class_name=self.storage_class_name,
                    connection_name=self.connection_name,
                    is_calibration=self.is_calibration,
                    raw_dimensions=self.raw_dimensions,
                ),
            ),
        )


class ReadEdge(Edge):
    """Representation of an input connection (including init-inputs and
    prerequisites) in a pipeline graph.

    Parameters
    ----------
    dataset_type_key : `NodeKey`
        Key for the dataset type node this edge is connected to.  This should
        hold the parent dataset type name for component dataset types.
    task_key : `NodeKey`
        Key for the task node this edge is connected to.
    storage_class_name : `str`
        Name of the dataset type's storage class as seen by the task.
    connection_name : `str`
        Internal name for the connection as seen by the task.
    is_calibration : `bool`
        Whether this dataset type can be included in
        `~lsst.daf.butler.CollectionType.CALIBRATION` collections.
    raw_dimensions : `frozenset` [ `str` ]
        Raw dimensions from the connection definition.
    is_prerequisite : `bool`
        Whether this dataset must be present in the data repository prior to
        `QuantumGraph` generation.
    component : `str` or `None`
        Component of the dataset type requested by the task.
    defer_query_constraint : `bool`
        If `True`, by default do not include this dataset type's existence as a
        constraint on the initial data ID query in QuantumGraph generation.

    Notes
    -----
    When included in an exported `networkx` graph (e.g.
    `PipelineGraph.make_xgraph`), read edges set the following edge attributes:

    - ``parent_dataset_type_name``
    - ``storage_class_name``
    - ``is_init``
    - ``component``
    - ``is_prerequisite``

    As with `ReadEdge` instance attributes, these descriptions of dataset types
    are those specific to a task, and may differ from the graph's resolved
    dataset type or (if `PipelineGraph.resolve` has not been called) there may
    not even be a consistent definition of the dataset type.
    """

    def __init__(
        self,
        dataset_type_key: NodeKey,
        task_key: NodeKey,
        *,
        storage_class_name: str,
        connection_name: str,
        is_calibration: bool,
        raw_dimensions: frozenset[str],
        is_prerequisite: bool,
        component: str | None,
        defer_query_constraint: bool,
    ):
        super().__init__(
            task_key=task_key,
            dataset_type_key=dataset_type_key,
            storage_class_name=storage_class_name,
            connection_name=connection_name,
            raw_dimensions=raw_dimensions,
            is_calibration=is_calibration,
        )
        self.is_prerequisite = is_prerequisite
        self.component = component
        self.defer_query_constraint = defer_query_constraint

    component: str | None
    """Component to add to `parent_dataset_type_name` to form the dataset type
    name seen by this task.
    """

    is_prerequisite: bool
    """Whether this dataset must be present in the data repository prior to
    `QuantumGraph` generation.
    """

    defer_query_constraint: bool
    """If `True`, by default do not include this dataset type's existence as a
    constraint on the initial data ID query in QuantumGraph generation.

    This can be `True` either because the connection class had
    ``deferQueryConstraint=True`` or because it had ``minimum=0``.
    """

    @property
    def nodes(self) -> tuple[NodeKey, NodeKey]:
        # Docstring inherited.
        return (self.dataset_type_key, self.task_key)

    @property
    def dataset_type_name(self) -> str:
        """Complete dataset type name, as seen by the task."""
        if self.component is not None:
            return f"{self.parent_dataset_type_name}.{self.component}"
        return self.parent_dataset_type_name

    def diff(self: ReadEdge, other: ReadEdge, connection_type: str = "connection") -> list[str]:
        # Docstring inherited.
        result = super().diff(other, connection_type)
        if self.defer_query_constraint != other.defer_query_constraint:
            result.append(
                f"{connection_type.capitalize()} {self.connection_name!r} is marked as a deferred query "
                f"constraint {'in A but not in B' if self.defer_query_constraint else 'in B but not in A'}."
            )
        return result

    def adapt_dataset_type(self, dataset_type: DatasetType) -> DatasetType:
        # Docstring inherited.
        if self.component is not None:
            dataset_type = dataset_type.makeComponentDatasetType(self.component)
        if self.storage_class_name != dataset_type.storageClass_name:
            return dataset_type.overrideStorageClass(self.storage_class_name)
        return dataset_type

    def adapt_dataset_ref(self, ref: DatasetRef) -> DatasetRef:
        # Docstring inherited.
        if self.component is not None:
            ref = ref.makeComponentRef(self.component)
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
        """Construct a `ReadEdge` instance from a `.BaseConnection` object.

        Parameters
        ----------
        task_key : `NodeKey`
            Key for the associated task node or task init node.
        connection_name : `str`
            Internal name for the connection as seen by the task,.
        connection_map : Mapping [ `str`, `.BaseConnection` ]
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
        connection = connection_map[connection_name]
        parent_dataset_type_name, component = DatasetType.splitDatasetTypeName(connection.name)
        return cls(
            dataset_type_key=NodeKey(NodeType.DATASET_TYPE, parent_dataset_type_name),
            task_key=task_key,
            component=component,
            storage_class_name=connection.storageClass,
            # InitInput connections don't have .isCalibration.
            is_calibration=getattr(connection, "isCalibration", False),
            is_prerequisite=is_prerequisite,
            connection_name=connection_name,
            # InitInput connections don't have a .dimensions because they
            # always have empty dimensions.
            raw_dimensions=frozenset(getattr(connection, "dimensions", frozenset())),
            # PrerequisiteInput and InitInput connections don't have a
            # .deferGraphConstraint, because they never constrain the initial
            # data ID query.
            defer_query_constraint=(
                getattr(connection, "deferGraphConstraint", False) or getattr(connection, "minimum", 1) == 0
            ),
        )

    def _resolve_dataset_type(
        self,
        *,
        current: DatasetType | None,
        is_initial_query_constraint: bool,
        is_prerequisite: bool | None,
        universe: DimensionUniverse,
        producer: str | None,
        consumers: Sequence[str],
        is_registered: bool,
        visualization_only: bool,
    ) -> tuple[DatasetType, bool, bool]:
        """Participate in the construction of the `DatasetTypeNode` object
        associated with this edge.

        Parameters
        ----------
        current : `lsst.daf.butler.DatasetType` or `None`
            The current graph-wide `DatasetType`, or `None`.  This will always
            be the registry's definition of the parent dataset type, if one
            exists.  If not, it will be the dataset type definition from the
            task in the graph that writes it, if there is one.  If there is no
            such task, this will be `None`.
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
            pipeline, or `None` if it is an overall input.
        consumers : `Sequence` [ `str` ]
            Labels for other consuming tasks that have already participated in
            this dataset type's resolution.
        is_registered : `bool`
            Whether a registration for this dataset type was found in the
            data repository.
        visualization_only : `bool`
            Resolve the graph as well as possible even when dimensions and
            storage classes cannot really be determined.  This can include
            using the ``universe.commonSkyPix`` as the assumed dimensions of
            connections that use the "skypix" placeholder and using "<UNKNOWN>"
            as a storage class name (which will fail if the storage class
            itself is ever actually loaded).

        Returns
        -------
        dataset_type : `DatasetType`
            The updated graph-wide dataset type.  If ``current`` was provided,
            this must be equal to it.
        is_initial_query_constraint : `bool`
            If `True`, this dataset type should be included as a constraint in
            the initial data ID query during QuantumGraph generation; this
            requires that ``is_initial_query_constraint`` also be `True` on
            input.
        is_prerequisite : `bool`
            Whether this dataset type is marked as a prerequisite input in this
            task and all other edges processed so far.

        Raises
        ------
        MissingDatasetTypeError
            Raised if ``current is None`` and this edge cannot define one on
            its own.
        IncompatibleDatasetTypeError
            Raised if ``current is not None`` and this edge's definition is not
            compatible with it.
        ConnectionTypeConsistencyError
            Raised if a prerequisite input for one task appears as a different
            kind of connection in any other task.
        """
        if "skypix" in self.raw_dimensions:
            if current is None:
                if visualization_only:
                    dimensions = universe.conform(
                        [d if d != "skypix" else universe.commonSkyPix.name for d in self.raw_dimensions]
                    )
                else:
                    raise MissingDatasetTypeError(
                        f"DatasetType '{self.dataset_type_name}' referenced by "
                        f"{self.task_label!r} uses 'skypix' as a dimension "
                        f"placeholder, but has not been registered with the data repository.  "
                        f"Note that reference catalog names are now used as the dataset "
                        f"type name instead of 'ref_cat'."
                    )
            else:
                rest1 = set(universe.conform(self.raw_dimensions - {"skypix"}).names)
                rest2 = current.dimensions.names - current.dimensions.skypix
                if rest1 != rest2:
                    raise IncompatibleDatasetTypeError(
                        f"Non-skypix dimensions for dataset type {self.dataset_type_name} declared in "
                        f"connections ({rest1}) are inconsistent with those in "
                        f"registry's version of this dataset ({rest2})."
                    )
                dimensions = current.dimensions
        else:
            dimensions = universe.conform(self.raw_dimensions)
        is_initial_query_constraint = is_initial_query_constraint and not self.defer_query_constraint
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
            if is_registered:
                return "data repository"
            elif producer is not None:
                return f"producing task {producer!r}"
            else:
                return f"consuming task(s) {consumers!r}"

        if self.component is not None:
            if current is None:
                if visualization_only:
                    current = DatasetType(
                        self.parent_dataset_type_name,
                        dimensions,
                        storageClass="<UNKNOWN>",
                        isCalibration=self.is_calibration,
                    )
                else:
                    raise MissingDatasetTypeError(
                        f"Dataset type {self.parent_dataset_type_name!r} is not registered and not produced "
                        f"by this pipeline, but it is used by task {self.task_label!r}, via component "
                        f"{self.component!r}. This pipeline cannot be resolved until the parent dataset "
                        "type is registered."
                    )
            else:
                all_current_components = current.storageClass.allComponents()
                if self.component not in all_current_components:
                    raise IncompatibleDatasetTypeError(
                        f"Dataset type {self.parent_dataset_type_name!r} has storage class "
                        f"{current.storageClass_name!r} (from {report_current_origin()}), "
                        f"which does not include component {self.component!r} "
                        f"as requested by task {self.task_label!r}."
                    )
                # Note that we can't actually make a fully-correct DatasetType
                # for the component the task wants, because we don't have the
                # parent storage class.
                current_component = all_current_components[self.component]
                if (
                    current_component.name != self.storage_class_name
                    and not StorageClassFactory()
                    .getStorageClass(self.storage_class_name)
                    .can_convert(current_component)
                ):
                    raise IncompatibleDatasetTypeError(
                        f"Dataset type '{self.parent_dataset_type_name}.{self.component}' has storage class "
                        f"{all_current_components[self.component].name!r} "
                        f"(from {report_current_origin()}), which cannot be converted to "
                        f"{self.storage_class_name!r}, as requested by task {self.task_label!r}."
                    )
            return current, is_initial_query_constraint, is_prerequisite
        else:
            dataset_type = DatasetType(
                self.parent_dataset_type_name,
                dimensions,
                storageClass=self.storage_class_name,
                isCalibration=self.is_calibration,
            )
            if current is not None:
                if not is_registered and producer is None:
                    # Current definition comes from another consumer; we
                    # require the dataset types to be exactly equal (not just
                    # compatible), since neither connection should take
                    # precedence.
                    if dataset_type != current:
                        raise MissingDatasetTypeError(
                            f"Definitions differ for input dataset type {self.parent_dataset_type_name!r}; "
                            f"task {self.task_label!r} has {dataset_type}, but the definition "
                            f"from {report_current_origin()} is {current}.  If the storage classes are "
                            "compatible but different, registering the dataset type in the data repository "
                            "in advance will avoid this error."
                        )
                elif not dataset_type.is_compatible_with(current):
                    raise IncompatibleDatasetTypeError(
                        f"Incompatible definition for input dataset type {self.parent_dataset_type_name!r}; "
                        f"task {self.task_label!r} has {dataset_type}, but the definition "
                        f"from {report_current_origin()} is {current}."
                    )
                return current, is_initial_query_constraint, is_prerequisite
            else:
                return dataset_type, is_initial_query_constraint, is_prerequisite

    def _to_xgraph_state(self) -> dict[str, Any]:
        # Docstring inherited.
        result = super()._to_xgraph_state()
        result["component"] = self.component
        result["is_prerequisite"] = self.is_prerequisite
        return result

    def __reduce__(self) -> tuple[Callable[[dict[str, Any]], Edge], tuple[dict[str, Any]]]:
        return (
            self._unreduce,
            (
                dict(
                    dataset_type_key=self.dataset_type_key,
                    task_key=self.task_key,
                    storage_class_name=self.storage_class_name,
                    connection_name=self.connection_name,
                    is_calibration=self.is_calibration,
                    raw_dimensions=self.raw_dimensions,
                    is_prerequisite=self.is_prerequisite,
                    component=self.component,
                    defer_query_constraint=self.defer_query_constraint,
                ),
            ),
        )


class WriteEdge(Edge):
    """Representation of an output connection (including init-outputs) in a
    pipeline graph.

    Notes
    -----
    When included in an exported `networkx` graph (e.g.
    `PipelineGraph.make_xgraph`), write edges set the following edge
    attributes:

    - ``parent_dataset_type_name``
    - ``storage_class_name``
    - ``is_init``

    As with `WRiteEdge` instance attributes, these descriptions of dataset
    types are those specific to a task, and may differ from the graph's
    resolved dataset type or (if `PipelineGraph.resolve` has not been called)
    there may not even be a consistent definition of the dataset type.
    """

    @property
    def nodes(self) -> tuple[NodeKey, NodeKey]:
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
        """Construct a `WriteEdge` instance from a `.BaseConnection` object.

        Parameters
        ----------
        task_key : `NodeKey`
            Key for the associated task node or task init node.
        connection_name : `str`
            Internal name for the connection as seen by the task,.
        connection_map : Mapping [ `str`, `.BaseConnection` ]
            Mapping of post-configuration object to draw dataset type
            information from, keyed by connection name.

        Returns
        -------
        edge : `WriteEdge`
            New edge instance.
        """
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
            connection_name=connection_name,
            # InitOutput connections don't have .isCalibration.
            is_calibration=getattr(connection, "isCalibration", False),
            # InitOutput connections don't have a .dimensions because they
            # always have empty dimensions.
            raw_dimensions=frozenset(getattr(connection, "dimensions", frozenset())),
        )

    def _resolve_dataset_type(self, current: DatasetType | None, universe: DimensionUniverse) -> DatasetType:
        """Participate in the construction of the `DatasetTypeNode` object
        associated with this edge.

        Parameters
        ----------
        current : `lsst.daf.butler.DatasetType` or `None`
            The current graph-wide `DatasetType`, or `None`.  This will always
            be the registry's definition of the parent dataset type, if one
            exists.
        universe : `lsst.daf.butler.DimensionUniverse`
            Object that holds all dimension definitions.

        Returns
        -------
        dataset_type : `DatasetType`
            A dataset type compatible with this edge.  If ``current`` was
            provided, this must be equal to it.

        Raises
        ------
        IncompatibleDatasetTypeError
            Raised if ``current is not None`` and this edge's definition is not
            compatible with it.
        """
        try:
            dimensions = universe.conform(self.raw_dimensions)
            dataset_type = DatasetType(
                self.parent_dataset_type_name,
                dimensions,
                storageClass=self.storage_class_name,
                isCalibration=self.is_calibration,
            )
        except Exception as err:
            err.add_note(f"In connection {self.connection_name!r} of task {self.task_label!r}.")
            raise
        if current is not None:
            if not current.is_compatible_with(dataset_type):
                raise IncompatibleDatasetTypeError(
                    f"Incompatible definition for output dataset type {self.parent_dataset_type_name!r}: "
                    f"task {self.task_label!r} has {dataset_type}, but data repository has {current}."
                )
            return current
        else:
            return dataset_type
