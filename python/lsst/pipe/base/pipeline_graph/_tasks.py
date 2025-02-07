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

__all__ = ("TaskImportMode", "TaskInitNode", "TaskNode")

import dataclasses
import enum
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, cast

from lsst.daf.butler import (
    DataCoordinate,
    DatasetRef,
    DatasetType,
    DimensionGroup,
    DimensionUniverse,
    Registry,
)
from lsst.pex.config import FieldValidationError
from lsst.utils.classes import immutable
from lsst.utils.doImport import doImportType
from lsst.utils.introspection import get_full_type_name

from .. import automatic_connection_constants as acc
from ..connections import PipelineTaskConnections
from ..connectionTypes import BaseConnection, BaseInput, InitOutput, Output
from ._edges import Edge, ReadEdge, WriteEdge
from ._exceptions import TaskNotImportedError, UnresolvedGraphError
from ._nodes import NodeKey, NodeType

if TYPE_CHECKING:
    from ..config import PipelineTaskConfig
    from ..pipelineTask import PipelineTask


class TaskImportMode(enum.Enum):
    """Enumeration of the ways to handle importing tasks when reading a
    serialized PipelineGraph.
    """

    DO_NOT_IMPORT = enum.auto()
    """Do not import tasks or instantiate their configs and connections."""

    REQUIRE_CONSISTENT_EDGES = enum.auto()
    """Import tasks and instantiate their config and connection objects, and
    check that the connections still define the same edges.
    """

    ASSUME_CONSISTENT_EDGES = enum.auto()
    """Import tasks and instantiate their config and connection objects, but do
    not check that the connections still define the same edges.

    This is safe only when the caller knows the task definition has not changed
    since the pipeline graph was persisted, such as when it was saved and
    loaded with the same pipeline version.
    """

    OVERRIDE_EDGES = enum.auto()
    """Import tasks and instantiate their config and connection objects, and
    allow the edges defined in those connections to override those in the
    persisted graph.

    This may cause dataset type nodes to be unresolved, since resolutions
    consistent with the original edges may be invalidated.
    """


@dataclasses.dataclass(frozen=True)
class _TaskNodeImportedData:
    """An internal struct that holds `TaskNode` and `TaskInitNode` state that
    requires task classes to be imported.
    """

    task_class: type[PipelineTask]
    """Type object for the task."""

    config: PipelineTaskConfig
    """Configuration object for the task."""

    connection_map: dict[str, BaseConnection]
    """Mapping from connection name to connection.

    In addition to ``connections.allConnections``, this also holds the
    "automatic" config, log, and metadata connections using the names defined
    in the `.automatic_connection_constants` module.
    """

    connections: PipelineTaskConnections
    """Configured connections object for the task."""

    @classmethod
    def configure(
        cls,
        label: str,
        task_class: type[PipelineTask],
        config: PipelineTaskConfig,
        connections: PipelineTaskConnections | None = None,
    ) -> _TaskNodeImportedData:
        """Construct while creating a `PipelineTaskConnections` instance if
        necessary.

        Parameters
        ----------
        label : `str`
            Label for the task in the pipeline.  Only used in error messages.
        task_class : `type` [ `.PipelineTask` ]
            Pipeline task `type` object.
        config : `.PipelineTaskConfig`
            Configuration for the task.
        connections : `.PipelineTaskConnections`, optional
            Object that describes the dataset types used by the task.  If not
            provided, one will be constructed from the given configuration.  If
            provided, it is assumed that ``config`` has already been validated
            and frozen.

        Returns
        -------
        data : `_TaskNodeImportedData`
            Instance of this struct.
        """
        if connections is None:
            # If we don't have connections yet, assume the config hasn't been
            # validated yet.
            try:
                config.validate()
            except FieldValidationError as err:
                err.fullname = f"{label}: {err.fullname}"
                raise err
            except Exception as err:
                raise ValueError(
                    f"Configuration validation failed for task {label!r} (see chained exception)."
                ) from err
            config.freeze()
            # MyPy doesn't see the metaclass attribute defined for this.
            connections = config.ConnectionsClass(config=config)  # type: ignore
        connection_map = dict(connections.allConnections)
        connection_map[acc.CONFIG_INIT_OUTPUT_CONNECTION_NAME] = InitOutput(
            acc.CONFIG_INIT_OUTPUT_TEMPLATE.format(label=label),
            acc.CONFIG_INIT_OUTPUT_STORAGE_CLASS,
        )
        connection_map[acc.METADATA_OUTPUT_CONNECTION_NAME] = Output(
            acc.METADATA_OUTPUT_TEMPLATE.format(label=label),
            acc.METADATA_OUTPUT_STORAGE_CLASS,
            dimensions=set(connections.dimensions),
        )
        if config.saveLogOutput:
            connection_map[acc.LOG_OUTPUT_CONNECTION_NAME] = Output(
                acc.LOG_OUTPUT_TEMPLATE.format(label=label),
                acc.LOG_OUTPUT_STORAGE_CLASS,
                dimensions=set(connections.dimensions),
            )
        return cls(task_class, config, connection_map, connections)


@immutable
class TaskInitNode:
    """A node in a pipeline graph that represents the construction of a
    `PipelineTask`.

    Parameters
    ----------
    key : `NodeKey`
        Key that identifies this node in internal and exported networkx graphs.
    inputs : `~collections.abc.Mapping` [ `str`, `ReadEdge` ]
        Graph edges that represent inputs required just to construct an
        instance of this task, keyed by connection name.
    outputs : ~collections.abc.Mapping` [ `str`, `WriteEdge` ]
        Graph edges that represent outputs of this task that are available
        after just constructing it, keyed by connection name.

        This does not include the special `config_init_output` edge; use
        `iter_all_outputs` to include that, too.
    config_output : `WriteEdge`
        The special init output edge that persists the task's configuration.
    imported_data : `_TaskNodeImportedData`, optional
        Internal struct that holds information that requires the task class to
        have been be imported.
    task_class_name : `str`, optional
        Fully-qualified name of the task class.  Must be provided if
        ``imported_data`` is not.
    config_str : `str`, optional
        Configuration for the task as a string of override statements.  Must be
        provided if ``imported_data`` is not.

    Notes
    -----
    When included in an exported `networkx` graph (e.g.
    `PipelineGraph.make_xgraph`), task initialization nodes set the following
    node attributes:

    - ``task_class_name``
    - ``bipartite`` (see `NodeType.bipartite`)
    - ``task_class`` (only if `is_imported` is `True`)
    - ``config`` (only if `is_imported` is `True`)
    """

    def __init__(
        self,
        key: NodeKey,
        *,
        inputs: Mapping[str, ReadEdge],
        outputs: Mapping[str, WriteEdge],
        config_output: WriteEdge,
        imported_data: _TaskNodeImportedData | None = None,
        task_class_name: str | None = None,
        config_str: str | None = None,
    ):
        self.key = key
        self.inputs = inputs
        self.outputs = outputs
        self.config_output = config_output
        # Instead of setting attributes to None, we do not set them at all;
        # this works better with the @immutable decorator, which supports
        # deferred initialization but not reassignment.
        if task_class_name is not None:
            self._task_class_name = task_class_name
        if config_str is not None:
            self._config_str = config_str
        if imported_data is not None:
            self._imported_data = imported_data
        else:
            assert self._task_class_name is not None and self._config_str is not None, (
                "If imported_data is not present, task_class_name and config_str must be."
            )

    key: NodeKey
    """Key that identifies this node in internal and exported networkx graphs.
    """

    inputs: Mapping[str, ReadEdge]
    """Graph edges that represent inputs required just to construct an instance
    of this task, keyed by connection name.
    """

    outputs: Mapping[str, WriteEdge]
    """Graph edges that represent outputs of this task that are available after
    just constructing it, keyed by connection name.

    This does not include the special `config_output` edge; use
    `iter_all_outputs` to include that, too.
    """

    config_output: WriteEdge
    """The special output edge that persists the task's configuration.
    """

    @property
    def label(self) -> str:
        """Label of this configuration of a task in the pipeline."""
        return str(self.key)

    @property
    def is_imported(self) -> bool:
        """Whether this the task type for this node has been imported and
        its configuration overrides applied.

        If this is `False`, the `task_class` and `config` attributes may not
        be accessed.
        """
        return hasattr(self, "_imported_data")

    @property
    def task_class(self) -> type[PipelineTask]:
        """Type object for the task.

        Accessing this attribute when `is_imported` is `False` will raise
        `TaskNotImportedError`, but accessing `task_class_name` will not.
        """
        return self._get_imported_data().task_class

    @property
    def task_class_name(self) -> str:
        """The fully-qualified string name of the task class."""
        try:
            return self._task_class_name
        except AttributeError:
            pass
        self._task_class_name = get_full_type_name(self.task_class)
        return self._task_class_name

    @property
    def config(self) -> PipelineTaskConfig:
        """Configuration for the task.

        This is always frozen.

        Accessing this attribute when `is_imported` is `False` will raise
        `TaskNotImportedError`, but calling `get_config_str` will not.
        """
        return self._get_imported_data().config

    def __repr__(self) -> str:
        return f"{self.label} [init] ({self.task_class_name})"

    def get_config_str(self) -> str:
        """Return the configuration for this task as a string of override
        statements.

        Returns
        -------
        config_str : `str`
            String containing configuration-overload statements.
        """
        try:
            return self._config_str
        except AttributeError:
            pass
        self._config_str = self.config.saveToString()
        return self._config_str

    def iter_all_inputs(self) -> Iterator[ReadEdge]:
        """Iterate over all inputs required for construction.

        This is the same as iteration over ``inputs.values()``, but it will be
        updated to include any automatic init-input connections added in the
        future, while `inputs` will continue to hold only task-defined init
        inputs.

        Yields
        ------
        `ReadEdge`
            All the inputs required for construction.
        """
        return iter(self.inputs.values())

    def iter_all_outputs(self) -> Iterator[WriteEdge]:
        """Iterate over all outputs available after construction, including
        special ones.

        Yields
        ------
        `ReadEdge`
            All the outputs available after construction.
        """
        yield from self.outputs.values()
        yield self.config_output

    def diff_edges(self, other: TaskInitNode) -> list[str]:
        """Compare the edges of this task initialization node to those from the
        same task label in a different pipeline.

        Parameters
        ----------
        other : `TaskInitNode`
            Other node to compare to. Must have the same task label, but need
            not have the same configuration or even the same task class.

        Returns
        -------
        differences : `list` [ `str` ]
            List of string messages describing differences between ``self`` and
            ``other``.  Will be empty if the two nodes have the same edges.
            Messages will use 'A' to refer to ``self`` and 'B' to refer to
            ``other``.
        """
        result = []
        result += _diff_edge_mapping(self.inputs, self.inputs, self.label, "init input")
        result += _diff_edge_mapping(self.outputs, other.outputs, self.label, "init output")
        result += self.config_output.diff(other.config_output, "config init output")
        return result

    def _to_xgraph_state(self) -> dict[str, Any]:
        """Convert this nodes's attributes into a dictionary suitable for use
        in exported networkx graphs.
        """
        result = {"task_class_name": self.task_class_name, "bipartite": NodeType.TASK_INIT.bipartite}
        if hasattr(self, "_imported_data"):
            result["task_class"] = self.task_class
            result["config"] = self.config
        return result

    def _get_imported_data(self) -> _TaskNodeImportedData:
        """Return the imported data struct.

        Returns
        -------
        imported_data : `_TaskNodeImportedData`
            Internal structure holding state that requires the task class to
            have been imported.

        Raises
        ------
        TaskNotImportedError
            Raised if `is_imported` is `False`.
        """
        try:
            return self._imported_data
        except AttributeError:
            raise TaskNotImportedError(
                f"Task class {self.task_class_name!r} for label {self.label!r} has not been imported "
                "(see PipelineGraph.import_and_configure)."
            ) from None

    @staticmethod
    def _unreduce(kwargs: dict[str, Any]) -> TaskInitNode:
        """Unpickle a `TaskInitNode` instance."""
        # Connections classes are not pickleable, so we can't use the
        # dataclass-provided pickle implementation of _TaskNodeImportedData,
        # and it's easier to just call its `configure` method than to fix it.
        if (imported_data_args := kwargs.pop("imported_data_args", None)) is not None:
            imported_data = _TaskNodeImportedData.configure(*imported_data_args)
        else:
            imported_data = None
        return TaskInitNode(imported_data=imported_data, **kwargs)

    def __reduce__(self) -> tuple[Callable[[dict[str, Any]], TaskInitNode], tuple[dict[str, Any]]]:
        kwargs = dict(
            key=self.key,
            inputs=self.inputs,
            outputs=self.outputs,
            config_output=self.config_output,
            task_class_name=getattr(self, "_task_class_name", None),
            config_str=getattr(self, "_config_str", None),
        )
        if hasattr(self, "_imported_data"):
            kwargs["imported_data_args"] = (
                self.label,
                self.task_class,
                self.config,
            )
        return (self._unreduce, (kwargs,))


@immutable
class TaskNode:
    """A node in a pipeline graph that represents a labeled configuration of a
    `PipelineTask`.

    Parameters
    ----------
    key : `NodeKey`
        Identifier for this node in networkx graphs.
    init : `TaskInitNode`
        Node representing the initialization of this task.
    prerequisite_inputs : `~collections.abc.Mapping` [ `str`, `ReadEdge` ]
        Graph edges that represent prerequisite inputs to this task, keyed by
        connection name.

        Prerequisite inputs must already exist in the data repository when a
        `QuantumGraph` is built, but have more flexibility in how they are
        looked up than regular inputs.
    inputs : `~collections.abc.Mapping` [ `str`, `ReadEdge` ]
        Graph edges that represent regular runtime inputs to this task, keyed
        by connection name.
    outputs : ~collections.abc.Mapping` [ `str`, `WriteEdge` ]
        Graph edges that represent regular runtime outputs of this task, keyed
        by connection name.

        This does not include the special `log_output` and `metadata_output`
        edges; use `iter_all_outputs` to include that, too.
    log_output : `WriteEdge` or `None`
        The special runtime output that persists the task's logs.
    metadata_output : `WriteEdge`
        The special runtime output that persists the task's metadata.
    dimensions : `lsst.daf.butler.DimensionGroup` or `frozenset` [ `str` ]
        Dimensions of the task.  If a `frozenset`, the dimensions have not been
        resolved by a `~lsst.daf.butler.DimensionUniverse` and cannot be safely
        compared to other sets of dimensions.

    Notes
    -----
    Task nodes are intentionally not equality comparable, since there are many
    different (and useful) ways to compare these objects with no clear winner
    as the most obvious behavior.

    When included in an exported `networkx` graph (e.g.
    `PipelineGraph.make_xgraph`), task nodes set the following node attributes:

    - ``task_class_name``
    - ``bipartite`` (see `NodeType.bipartite`)
    - ``task_class`` (only if `is_imported` is `True`)
    - ``config`` (only if `is_imported` is `True`)
    """

    def __init__(
        self,
        key: NodeKey,
        init: TaskInitNode,
        *,
        prerequisite_inputs: Mapping[str, ReadEdge],
        inputs: Mapping[str, ReadEdge],
        outputs: Mapping[str, WriteEdge],
        log_output: WriteEdge | None,
        metadata_output: WriteEdge,
        dimensions: DimensionGroup | frozenset[str],
    ):
        self.key = key
        self.init = init
        self.prerequisite_inputs = prerequisite_inputs
        self.inputs = inputs
        self.outputs = outputs
        self.log_output = log_output
        self.metadata_output = metadata_output
        self._dimensions = dimensions

    @staticmethod
    def _from_imported_data(
        key: NodeKey,
        init_key: NodeKey,
        data: _TaskNodeImportedData,
        universe: DimensionUniverse | None,
    ) -> TaskNode:
        """Construct from a `PipelineTask` type and its configuration.

        Parameters
        ----------
        key : `NodeKey`
            Identifier for this node in networkx graphs.
        init_key : `TaskInitNode`
            Node representing the initialization of this task.
        data : `_TaskNodeImportedData`
            Internal struct that holds information that requires the task class
            to have been be imported.
        universe : `lsst.daf.butler.DimensionUniverse` or `None`
            Definitions of all dimensions.

        Returns
        -------
        node : `TaskNode`
            New task node.

        Raises
        ------
        ValueError
            Raised if configuration validation failed when constructing
            ``connections``.
        """
        init_inputs = {
            name: ReadEdge._from_connection_map(init_key, name, data.connection_map)
            for name in data.connections.initInputs
        }
        prerequisite_inputs = {
            name: ReadEdge._from_connection_map(key, name, data.connection_map, is_prerequisite=True)
            for name in data.connections.prerequisiteInputs
        }
        inputs = {
            name: ReadEdge._from_connection_map(key, name, data.connection_map)
            for name in data.connections.inputs
            if not getattr(data.connections, name).deferBinding
        }
        init_outputs = {
            name: WriteEdge._from_connection_map(init_key, name, data.connection_map)
            for name in data.connections.initOutputs
        }
        outputs = {
            name: WriteEdge._from_connection_map(key, name, data.connection_map)
            for name in data.connections.outputs
        }
        init = TaskInitNode(
            key=init_key,
            inputs=init_inputs,
            outputs=init_outputs,
            config_output=WriteEdge._from_connection_map(
                init_key, acc.CONFIG_INIT_OUTPUT_CONNECTION_NAME, data.connection_map
            ),
            imported_data=data,
        )
        instance = TaskNode(
            key=key,
            init=init,
            prerequisite_inputs=prerequisite_inputs,
            inputs=inputs,
            outputs=outputs,
            log_output=(
                WriteEdge._from_connection_map(key, acc.LOG_OUTPUT_CONNECTION_NAME, data.connection_map)
                if data.config.saveLogOutput
                else None
            ),
            metadata_output=WriteEdge._from_connection_map(
                key, acc.METADATA_OUTPUT_CONNECTION_NAME, data.connection_map
            ),
            dimensions=(
                frozenset(data.connections.dimensions)
                if universe is None
                else universe.conform(data.connections.dimensions)
            ),
        )
        return instance

    key: NodeKey
    """Key that identifies this node in internal and exported networkx graphs.
    """

    prerequisite_inputs: Mapping[str, ReadEdge]
    """Graph edges that represent prerequisite inputs to this task.

    Prerequisite inputs must already exist in the data repository when a
    `QuantumGraph` is built, but have more flexibility in how they are looked
    up than regular inputs.
    """

    inputs: Mapping[str, ReadEdge]
    """Graph edges that represent regular runtime inputs to this task.
    """

    outputs: Mapping[str, WriteEdge]
    """Graph edges that represent regular runtime outputs of this task.

    This does not include the special `log_output` and `metadata_output` edges;
    use `iter_all_outputs` to include that, too.
    """

    log_output: WriteEdge | None
    """The special runtime output that persists the task's logs.
    """

    metadata_output: WriteEdge
    """The special runtime output that persists the task's metadata.
    """

    @property
    def label(self) -> str:
        """Label of this configuration of a task in the pipeline."""
        return self.key.name

    @property
    def is_imported(self) -> bool:
        """Whether this the task type for this node has been imported and
        its configuration overrides applied.

        If this is `False`, the `task_class` and `config` attributes may not
        be accessed.
        """
        return self.init.is_imported

    @property
    def task_class(self) -> type[PipelineTask]:
        """Type object for the task.

        Accessing this attribute when `is_imported` is `False` will raise
        `TaskNotImportedError`, but accessing `task_class_name` will not.
        """
        return self.init.task_class

    @property
    def task_class_name(self) -> str:
        """The fully-qualified string name of the task class."""
        return self.init.task_class_name

    @property
    def config(self) -> PipelineTaskConfig:
        """Configuration for the task.

        This is always frozen.

        Accessing this attribute when `is_imported` is `False` will raise
        `TaskNotImportedError`, but calling `get_config_str` will not.
        """
        return self.init.config

    @property
    def has_resolved_dimensions(self) -> bool:
        """Whether the `dimensions` attribute may be accessed.

        If `False`, the `raw_dimensions` attribute may be used to obtain a
        set of dimension names that has not been resolved by a
        `~lsst.daf.butler.DimensionsUniverse`.
        """
        return type(self._dimensions) is DimensionGroup

    @property
    def dimensions(self) -> DimensionGroup:
        """Standardized dimensions of the task."""
        if not self.has_resolved_dimensions:
            raise UnresolvedGraphError(f"Dimensions for task {self.label!r} have not been resolved.")
        return cast(DimensionGroup, self._dimensions)

    @property
    def raw_dimensions(self) -> frozenset[str]:
        """Raw dimensions of the task, with standardization by a
        `~lsst.daf.butler.DimensionUniverse` not guaranteed.
        """
        if self.has_resolved_dimensions:
            return frozenset(cast(DimensionGroup, self._dimensions).names)
        else:
            return cast(frozenset[str], self._dimensions)

    def __repr__(self) -> str:
        if self.has_resolved_dimensions:
            return f"{self.label} ({self.task_class_name}, {self.dimensions})"
        else:
            return f"{self.label} ({self.task_class_name})"

    def get_config_str(self) -> str:
        """Return the configuration for this task as a string of override
        statements.

        Returns
        -------
        config_str : `str`
            String containing configuration-overload statements.
        """
        return self.init.get_config_str()

    def iter_all_inputs(self) -> Iterator[ReadEdge]:
        """Iterate over all runtime inputs, including both regular inputs and
        prerequisites.

        Yields
        ------
        `ReadEdge`
            All the runtime inputs.
        """
        yield from self.prerequisite_inputs.values()
        yield from self.inputs.values()

    def iter_all_outputs(self) -> Iterator[WriteEdge]:
        """Iterate over all runtime outputs, including special ones.

        Yields
        ------
        `ReadEdge`
            All the runtime outputs.
        """
        yield from self.outputs.values()
        yield self.metadata_output
        if self.log_output is not None:
            yield self.log_output

    def diff_edges(self, other: TaskNode) -> list[str]:
        """Compare the edges of this task node to those from the same task
        label in a different pipeline.

        This also calls `TaskInitNode.diff_edges`.

        Parameters
        ----------
        other : `TaskInitNode`
            Other node to compare to. Must have the same task label, but need
            not have the same configuration or even the same task class.

        Returns
        -------
        differences : `list` [ `str` ]
            List of string messages describing differences between ``self`` and
            ``other``.  Will be empty if the two nodes have the same edges.
            Messages will use 'A' to refer to ``self`` and 'B' to refer to
            ``other``.
        """
        result = self.init.diff_edges(other.init)
        result += _diff_edge_mapping(
            self.prerequisite_inputs, other.prerequisite_inputs, self.label, "prerequisite input"
        )
        result += _diff_edge_mapping(self.inputs, other.inputs, self.label, "input")
        result += _diff_edge_mapping(self.outputs, other.outputs, self.label, "output")
        if self.log_output is not None:
            if other.log_output is not None:
                result += self.log_output.diff(other.log_output, "log output")
            else:
                result.append("Log output is present in A, but not in B.")
        elif other.log_output is not None:
            result.append("Log output is present in B, but not in A.")
        result += self.metadata_output.diff(other.metadata_output, "metadata output")
        return result

    def get_lookup_function(
        self, connection_name: str
    ) -> Callable[[DatasetType, Registry, DataCoordinate, Sequence[str]], Iterable[DatasetRef]] | None:
        """Return the custom dataset query function for an edge, if one exists.

        Parameters
        ----------
        connection_name : `str`
            Name of the connection.

        Returns
        -------
        lookup_function : `~collections.abc.Callable` or `None`
            Callable that takes a dataset type, a butler registry, a data
            coordinate (the quantum data ID), and an ordered list of
            collections to search, and returns an iterable of
            `~lsst.daf.butler.DatasetRef`.
        """
        return getattr(self._get_imported_data().connection_map[connection_name], "lookupFunction", None)

    def is_optional(self, connection_name: str) -> bool:
        """Check whether the given connection has ``minimum==0``.

        Parameters
        ----------
        connection_name : `str`
            Name of the connection.

        Returns
        -------
        optional : `bool`
            Whether this task can run without any datasets for the given
            connection.
        """
        connection = getattr(self.get_connections(), connection_name)
        return isinstance(connection, BaseInput) and connection.minimum == 0

    def get_connections(self) -> PipelineTaskConnections:
        """Return the connections class instance for this task.

        Returns
        -------
        connections : `.PipelineTaskConnections`
            Task-provided object that defines inputs and outputs from
            configuration.
        """
        return self._get_imported_data().connections

    def get_spatial_bounds_connections(self) -> frozenset[str]:
        """Return the names of connections whose data IDs should be included
        in the calculation of the spatial bounds for this task's quanta.

        Returns
        -------
        connection_names : `frozenset` [ `str` ]
            Names of connections with spatial dimensions.
        """
        return frozenset(self._get_imported_data().connections.getSpatialBoundsConnections())

    def get_temporal_bounds_connections(self) -> frozenset[str]:
        """Return the names of connections whose data IDs should be included
        in the calculation of the temporal bounds for this task's quanta.

        Returns
        -------
        connection_names : `frozenset` [ `str` ]
            Names of connections with temporal dimensions.
        """
        return frozenset(self._get_imported_data().connections.getTemporalBoundsConnections())

    def _imported_and_configured(self, rebuild: bool) -> TaskNode:
        """Import the task class and use it to construct a new instance.

        Parameters
        ----------
        rebuild : `bool`
            If `True`, import the task class and configure its connections to
            generate new edges that may differ from the current ones.  If
            `False`, import the task class but just update the `task_class` and
            `config` attributes, and assume the edges have not changed.

        Returns
        -------
        node : `TaskNode`
            Task node instance for which `is_imported` is `True`.  Will be
            ``self`` if this is the case already.
        """
        from ..pipelineTask import PipelineTask

        if self.is_imported:
            return self
        task_class = doImportType(self.task_class_name)
        if not issubclass(task_class, PipelineTask):
            raise TypeError(f"{self.task_class_name!r} is not a PipelineTask subclass.")
        config = task_class.ConfigClass()
        config.loadFromString(self.get_config_str())
        return self._reconfigured(config, rebuild=rebuild, task_class=task_class)

    def _reconfigured(
        self,
        config: PipelineTaskConfig,
        rebuild: bool,
        task_class: type[PipelineTask] | None = None,
    ) -> TaskNode:
        """Return a version of this node with new configuration.

        Parameters
        ----------
        config : `.PipelineTaskConfig`
            New configuration for the task.
        rebuild : `bool`
            If `True`, use the configured connections to generate new edges
            that may differ from the current ones.  If `False`, just update the
            `task_class` and `config` attributes, and assume the edges have not
            changed.
        task_class : `type` [ `PipelineTask` ], optional
            Subclass of `PipelineTask`.  This defaults to ``self.task_class`,
            but may be passed as an argument if that is not available because
            the task class was not imported when ``self`` was constructed.

        Returns
        -------
        node : `TaskNode`
            Task node instance with the new config.
        """
        if task_class is None:
            task_class = self.task_class
        imported_data = _TaskNodeImportedData.configure(self.key.name, task_class, config)
        if rebuild:
            return self._from_imported_data(
                self.key,
                self.init.key,
                imported_data,
                universe=self._dimensions.universe if type(self._dimensions) is DimensionGroup else None,
            )
        else:
            return TaskNode(
                self.key,
                TaskInitNode(
                    self.init.key,
                    inputs=self.init.inputs,
                    outputs=self.init.outputs,
                    config_output=self.init.config_output,
                    imported_data=imported_data,
                ),
                prerequisite_inputs=self.prerequisite_inputs,
                inputs=self.inputs,
                outputs=self.outputs,
                log_output=self.log_output,
                metadata_output=self.metadata_output,
                dimensions=self._dimensions,
            )

    def _resolved(self, universe: DimensionUniverse | None) -> TaskNode:
        """Return an otherwise-equivalent task node with resolved dimensions.

        Parameters
        ----------
        universe : `lsst.daf.butler.DimensionUniverse` or `None`
            Definitions for all dimensions.

        Returns
        -------
        node : `TaskNode`
            Task node instance with `dimensions` resolved by the given
            universe.  Will be ``self`` if this is the case already.
        """
        if self.has_resolved_dimensions:
            if cast(DimensionGroup, self._dimensions).universe is universe:
                return self
        elif universe is None:
            return self
        return TaskNode(
            key=self.key,
            init=self.init,
            prerequisite_inputs=self.prerequisite_inputs,
            inputs=self.inputs,
            outputs=self.outputs,
            log_output=self.log_output,
            metadata_output=self.metadata_output,
            dimensions=(
                universe.conform(self.raw_dimensions) if universe is not None else self.raw_dimensions
            ),
        )

    def _to_xgraph_state(self) -> dict[str, Any]:
        """Convert this nodes's attributes into a dictionary suitable for use
        in exported networkx graphs.
        """
        result = self.init._to_xgraph_state()
        if self.has_resolved_dimensions:
            result["dimensions"] = self._dimensions
        result["raw_dimensions"] = self.raw_dimensions
        return result

    def _get_imported_data(self) -> _TaskNodeImportedData:
        """Return the imported data struct.

        Returns
        -------
        imported_data : `_TaskNodeImportedData`
            Internal structure holding state that requires the task class to
            have been imported.

        Raises
        ------
        TaskNotImportedError
            Raised if `is_imported` is `False`.
        """
        return self.init._get_imported_data()

    @staticmethod
    def _unreduce(kwargs: dict[str, Any]) -> TaskNode:
        """Unpickle a `TaskNode` instance."""
        return TaskNode(**kwargs)

    def __reduce__(self) -> tuple[Callable[[dict[str, Any]], TaskNode], tuple[dict[str, Any]]]:
        return (
            self._unreduce,
            (
                dict(
                    key=self.key,
                    init=self.init,
                    prerequisite_inputs=self.prerequisite_inputs,
                    inputs=self.inputs,
                    outputs=self.outputs,
                    log_output=self.log_output,
                    metadata_output=self.metadata_output,
                    dimensions=self._dimensions,
                ),
            ),
        )


def _diff_edge_mapping(
    a_mapping: Mapping[str, Edge], b_mapping: Mapping[str, Edge], task_label: str, connection_type: str
) -> list[str]:
    """Compare a pair of mappings of edges.

    Parameters
    ----------
    a_mapping : `~collections.abc.Mapping` [ `str`, `Edge` ]
        First mapping to compare.  Expected to have connection names as keys.
    b_mapping : `~collections.abc.Mapping` [ `str`, `Edge` ]
        First mapping to compare.  If keys differ from those of ``a_mapping``,
        this will be reported as a difference (in addition to element-wise
        comparisons).
    task_label : `str`
        Task label associated with both mappings.
    connection_type : `str`
        Type of connection (e.g. "input" or "init output") associated with both
        connections.  This is a human-readable string to include in difference
        messages.

    Returns
    -------
    differences : `list` [ `str` ]
        List of string messages describing differences between the two
        mappings. Will be empty if the two mappings have the same edges.
        Messages will include "A" and "B", and are expected to be a preceded
        by a message describing what "A" and "B" are in the context in which
        this method is called.

    Notes
    -----
    This is expected to be used to compare one edge-holding mapping attribute
    of a task or task init node to the same attribute on another task or task
    init node (i.e. any of `TaskNode.inputs`, `TaskNode.outputs`,
    `TaskNode.prerequisite_inputs`, `TaskInitNode.inputs`,
    `TaskInitNode.outputs`).
    """
    results = []
    b_to_do = set(b_mapping.keys())
    for connection_name, a_edge in a_mapping.items():
        if (b_edge := b_mapping.get(connection_name)) is None:
            results.append(
                f"{connection_type.capitalize()} {connection_name!r} of task "
                f"{task_label!r} exists in A, but not in B (or it may have a different connection type)."
            )
        else:
            results.extend(a_edge.diff(b_edge, connection_type))
        b_to_do.discard(connection_name)
    for connection_name in b_to_do:
        results.append(
            f"{connection_type.capitalize()} {connection_name!r} of task "
            f"{task_label!r} exists in A, but not in B (or it may have a different connection type)."
        )
    return results
