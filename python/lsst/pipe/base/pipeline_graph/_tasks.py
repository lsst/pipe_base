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

__all__ = ("TaskNode", "TaskInitNode", "ResolvedTaskNode")

import dataclasses
from collections.abc import Iterator, Set
from operator import attrgetter
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast

import networkx
from lsst.daf.butler import DimensionGraph, Registry
from lsst.utils.classes import immutable
from lsst.utils.doImport import doImportType
from lsst.utils.introspection import get_full_type_name

from .. import automatic_connection_constants as acc
from ..connections import PipelineTaskConnections
from ..connectionTypes import BaseConnection, InitOutput, Output
from ._abcs import Edge, Node, NodeKey, NodeType
from ._edges import ReadEdge, WriteEdge

if TYPE_CHECKING:
    from ..config import PipelineTaskConfig
    from ..pipelineTask import PipelineTask


@dataclasses.dataclass(frozen=True)
class _TaskNodeImportedData:
    """Internal struct that holds TaskNode state that requires task classes
    to be imported.
    """

    is_imported: ClassVar[Literal[True]] = True

    task_class: type[PipelineTask]

    config: PipelineTaskConfig

    connection_map: dict[str, BaseConnection]

    connections: PipelineTaskConnections

    @classmethod
    def configure(
        cls,
        label: str,
        task_class: type[PipelineTask],
        config: PipelineTaskConfig,
        connections: PipelineTaskConnections | None = None,
    ) -> _TaskNodeImportedData:
        """Construct while creating a PipelineTaskConnections if necessary.

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
            except Exception as err:
                raise ValueError(
                    f"Configuration validation failed for task {label!r} (see chained exception)."
                ) from err
            config.freeze()
            connections = task_class.ConfigClass.ConnectionsClass(config=config)
        connection_map = dict(connections.allConnections)
        connection_map[acc.CONFIG_INIT_OUTPUT_CONNECTION_NAME] = InitOutput(
            acc.CONFIG_INIT_OUTPUT_TEMPLATE.format(label=label),
            acc.CONFIG_INIT_OUTPUT_STORAGE_CLASS,
        )
        if not config.saveMetadata:
            raise ValueError(f"Metadata for task {label} cannot be disabled.")
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


@dataclasses.dataclass(frozen=True)
class _TaskNodeSerializedData:
    """Internal struct that holds a serialized form of _TaskNodeImportedData.

    Parameters
    ----------
    task_class_name : `str`
        Fully qualified pipeline task class name.
    config_str : `str`
        Complete string-serialized config overrides for this task.
    """

    is_imported: ClassVar[Literal[False]] = False

    task_class_name: str
    config_str: str

    def import_and_configure(self, label: str) -> _TaskNodeImportedData:
        task_class = doImportType(self.task_class_name)
        config = task_class.ConfigClass()
        config.loadFromString(self.config_str)
        return _TaskNodeImportedData.configure(label, task_class, config)


@immutable
class TaskInitNode(Node):
    """A node in a pipeline graph that represents the construction of a
    `PipelineTask`.

    Parameters
    ----------
    key : `NodeKey`
        Key for this node in the graph.
    inputs : `~collections.abc.Set` [ `ReadEdge` ]
        Graph edges that represent inputs required just to construct an
        instance of this task.
    outputs : ~collections.abc.Set` [ `WriteEdge` ]
        Graph edges that represent outputs of this task that are available
        after just constructing it.

        This does not include the special `config_init_output` edge; use
        `iter_all_init_outputs` to include that, too.
    config_output : `WriteEdge`
        The special init output edge that persists the task's configuration.
    data : `_TaskNodeImportedData` or `_TaskNodeSerializedData`
        Internal struct that either holds information that requires the task
        class to have been be imported, or the serialized form of that.
    """

    def __init__(
        self,
        key: NodeKey,
        *,
        inputs: Set[ReadEdge],
        outputs: Set[WriteEdge],
        config_output: WriteEdge,
        data: _TaskNodeImportedData | _TaskNodeSerializedData,
    ):
        super().__init__(key)
        self.inputs = inputs
        self.outputs = outputs
        self.config_output = config_output
        self._data = data

    inputs: Set[ReadEdge]
    """Graph edges that represent inputs required just to construct an instance
    of this task.
    """

    outputs: Set[WriteEdge]
    """Graph edges that represent outputs of this task that are available after
    just constructing it.

    This does not include the special `config_output` edge; use
    `iter_all_outputs` to include that, too.
    """

    config_output: WriteEdge
    """The special output edge that persists the task's configuration.
    """

    def iter_all_inputs(self) -> Iterator[ReadEdge]:
        """Iterate over all inputs required for construction."""
        return iter(self.inputs)

    def iter_all_outputs(self) -> Iterator[WriteEdge]:
        """Iterate over all outputs available after construction, including
        special ones.
        """
        yield from self.outputs
        yield self.config_output

    @property
    def label(self) -> str:
        """Label of this configuration of a task in the pipeline."""
        return str(self.key)

    @property
    def task_class(self) -> type[PipelineTask]:
        """Type object for the task."""
        return self._get_imported_data().task_class

    @property
    def task_class_name(self) -> str:
        """The fully-qualified string name of the task class."""
        if not self._data.is_imported:
            return self._data.task_class_name
        else:
            return get_full_type_name(self._data.task_class)

    @property
    def config(self) -> PipelineTaskConfig:
        """Configuration for the task."""
        return self._get_imported_data().config

    def _resolved(self, xgraph: networkx.DiGraph, registry: Registry) -> TaskInitNode:
        # Docstring inherited.
        return self

    def _unresolved(self) -> TaskInitNode:
        # Docstring inherited.
        return self

    def _serialize(self, **kwargs: Any) -> dict[str, Any]:
        # Docstring inherited.
        result: dict[str, Any] = {"task_class": get_full_type_name(self.task_class), **kwargs}
        for field in ("inputs", "outputs"):
            edges: Set[Edge] = getattr(self, field)
            result[field] = {
                edge.parent_dataset_type_name: edge._serialize()
                for edge in sorted(edges, key=attrgetter("parent_dataset_type_name"))
            }
        result["config_output"] = self.config_output._serialize(
            dataset_type_name=self.config_output.dataset_type_name
        )
        return result

    def _get_imported_data(self) -> _TaskNodeImportedData:
        if not self._data.is_imported:
            # We circumvent @immutable's protections here but very much still
            # obey them in spirit: this is delayed initialization, not logical
            # mutation, since none of the state prior to this method is
            # publicly accessible, and the state added by this method is only
            # accessible via accessors that call it.  It is important that we
            # remove the old "serialization" state, since the act of importing
            # and configuring a task can change what we would write if we were
            # to serialize it again:
            # - the task_class_name may have pointed to an alias, and now we
            #   want to save it in its true location, since this is how one
            #   would migrate a task from one package to another;
            # - new config fields may have been added to the code since the
            #   original config_str was written.
            object.__setattr__(self, "_data", self._data.import_and_configure(self.label))
        return cast(_TaskNodeImportedData, self._data)


@immutable
class TaskNode(Node):
    """A node in a pipeline graph that represents a labeled configuration of a
    `PipelineTask`.

    Parameters
    ----------
    key : `NodeKey`
        Key for this node in the graph.
    init : `TaskInitNode`
        Node representing the initialization of this task.
    prerequisite_inputs : `~collections.abc.Set` [ `ReadEdge` ]
        Graph edges that represent prerequisite inputs to this task.

        Prerequisite inputs must already exist in the data repository when a
        `QuantumGraph` is built, but have more flexibility in how they are
        looked up than regular inputs.
    inputs : `~collections.abc.Set` [ `ReadEdge` ]
        Graph edges that represent regular runtime inputs to this task.
    outputs : ~collections.abc.Set` [ `WriteEdge` ]
        Graph edges that represent regular runtime outputs of this task.

        This does not include the special `log_output` and `metadata_output`
        edges; use `iter_all_outputs` to include that, too.
    log_output : `WriteEdge` or `None`
        The special runtime output that persists the task's logs.
    metadata_output : `WriteEdge`
        The special runtime output that persists the task's metadata.

    Notes
    -----
    This class only holds information that can be pulled from `.PipelineTask`
    definitions.  Its `ResolvedDatasetTypeNode` subclass includes the
    normalized dimensions of the task.

    Task nodes are intentionally not equality comparable, since there are many
    different (and useful) ways to compare these objects with no clear winner
    as the most obvious behavior.
    """

    def __init__(
        self,
        key: NodeKey,
        init: TaskInitNode,
        *,
        prerequisite_inputs: Set[ReadEdge],
        inputs: Set[ReadEdge],
        outputs: Set[WriteEdge],
        log_output: WriteEdge | None,
        metadata_output: WriteEdge,
    ):
        super().__init__(key)
        self.init = init
        self.prerequisite_inputs = prerequisite_inputs
        self.inputs = inputs
        self.outputs = outputs
        self.log_output = log_output
        self.metadata_output = metadata_output

    @staticmethod
    def _from_imported_data(
        label: str,
        data: _TaskNodeImportedData,
    ) -> TaskNode:
        """Construct from a `PipelineTask` type and its configuration.

        Parameters
        ----------
        label : `str`
            Label for the task in the pipeline.
        data : `_TaskNodeImportedData`
            Internal data for the node.

        Returns
        -------
        node : `TaskNode`
            New task node.
        state: `dict` [ `str`, `Any` ]
            State object for the networkx representation of this node.  The
            returned ``node`` object is the value of the "instance" key.

        Raises
        ------
        ConnectionTypeConsistencyError
            Raised if the task defines a dataset type's ``is_init`` or
            ``is_prequisite`` flags in a way that is inconsistent with some
            other task in the graph.
        IncompatibleDatasetTypeError
            Raised if the task defines a dataset type differently from some
            other task in the graph.  Note that checks for dataset type
            dimension consistency do not occur until the graph is resolved.
        ValueError
            Raised if configuration validation failed when constructing.
            ``connections``.
        RuntimeError
            Raised if an unexpected exception (which will be chained) occurred
            at a stage that may have left the graph in an inconsistent state.
            All other exceptions should leave the graph unchanged.
        """
        key = NodeKey(NodeType.TASK, label)
        init_key = NodeKey(NodeType.TASK_INIT, label)
        init_inputs = {
            ReadEdge._from_connection_map(init_key, name, data.connection_map)
            for name in data.connections.initInputs
        }
        prerequisite_inputs = {
            ReadEdge._from_connection_map(key, name, data.connection_map, is_prerequisite=True)
            for name in data.connections.prerequisiteInputs
        }
        inputs = {
            ReadEdge._from_connection_map(key, name, data.connection_map) for name in data.connections.inputs
        }
        init_outputs = {
            WriteEdge._from_connection_map(init_key, name, data.connection_map)
            for name in data.connections.initOutputs
        }
        outputs = {
            WriteEdge._from_connection_map(key, name, data.connection_map)
            for name in data.connections.outputs
        }
        init = TaskInitNode(
            key=init_key,
            inputs=init_inputs,
            outputs=init_outputs,
            config_output=WriteEdge._from_connection_map(
                init_key, acc.CONFIG_INIT_OUTPUT_CONNECTION_NAME, data.connection_map
            ),
            data=data,
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
                init_key, acc.METADATA_OUTPUT_CONNECTION_NAME, data.connection_map
            ),
        )
        return instance

    prerequisite_inputs: Set[ReadEdge]
    """Graph edges that represent prerequisite inputs to this task.

    Prerequisite inputs must already exist in the data repository when a
    `QuantumGraph` is built, but have more flexibility in how they are looked
    up than regular inputs.
    """

    inputs: Set[ReadEdge]
    """Graph edges that represent regular runtime inputs to this task.
    """

    outputs: Set[WriteEdge]
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
        return str(self.key)

    @property
    def task_class(self) -> type[PipelineTask]:
        """Type object for the task."""
        return self.init.task_class

    @property
    def task_class_name(self) -> str:
        """The fully-qualified string name of the task class."""
        return self.init.task_class_name

    @property
    def config(self) -> PipelineTaskConfig:
        """Configuration for the task."""
        return self.init.config

    def iter_all_inputs(self) -> Iterator[ReadEdge]:
        """Iterate over all runtime inputs, including both regular inputs and
        prerequisites.
        """
        yield from self.prerequisite_inputs
        yield from self.inputs

    def iter_all_outputs(self) -> Iterator[WriteEdge]:
        """Iterate over all runtime outputs, including special ones."""
        yield from self.outputs
        yield self.metadata_output
        if self.log_output is not None:
            yield self.log_output

    def _get_imported_data(self) -> _TaskNodeImportedData:
        return self.init._get_imported_data()

    def _resolved(self, xgraph: networkx.DiGraph, registry: Registry) -> ResolvedTaskNode:
        # Docstring inherited.
        return ResolvedTaskNode(
            key=self.key,
            init=self.init,
            prerequisite_inputs=self.prerequisite_inputs,
            inputs=self.inputs,
            outputs=self.outputs,
            log_output=self.log_output,
            metadata_output=self.metadata_output,
            dimensions=registry.dimensions.extract(self._get_imported_data().connections.dimensions),
        )

    def _unresolved(self) -> TaskNode:
        # Docstring inherited.
        return self

    def _serialize(self, **kwargs: Any) -> dict[str, Any]:
        # Docstring inherited.
        result: dict[str, Any] = {
            "task_class": get_full_type_name(self.task_class),
            "init": self.init._serialize(),
            **kwargs,
        }
        for field in ("prerequisite_inputs", "inputs", "outputs"):
            edges: Set[Edge] = getattr(self, field)
            result[field] = {
                edge.parent_dataset_type_name: edge._serialize()
                for edge in sorted(edges, key=attrgetter("parent_dataset_type_name"))
            }
        if self.log_output is not None:
            result["log_output"] = self.log_output._serialize(
                dataset_type_name=self.log_output.dataset_type_name
            )
        result["metadata_output"] = self.metadata_output._serialize(
            dataset_type_name=self.metadata_output.dataset_type_name
        )
        return result


@immutable
class ResolvedTaskNode(TaskNode):
    """A task node that also holds the standardized dimensions of the task."""

    def __init__(
        self,
        key: NodeKey,
        init: TaskInitNode,
        *,
        prerequisite_inputs: Set[ReadEdge],
        inputs: Set[ReadEdge],
        outputs: Set[WriteEdge],
        log_output: WriteEdge | None,
        metadata_output: WriteEdge,
        dimensions: DimensionGraph,
    ):
        super().__init__(
            key,
            init=init,
            prerequisite_inputs=prerequisite_inputs,
            inputs=inputs,
            outputs=outputs,
            log_output=log_output,
            metadata_output=metadata_output,
        )
        self.dimensions = dimensions

    dimensions: DimensionGraph
    """Standardized dimensions of the task.
    """

    def _resolved(self, xgraph: networkx.DiGraph, registry: Registry) -> ResolvedTaskNode:
        # Docstring inherited.
        return self

    def _unresolved(self) -> TaskNode:
        # Docstring inherited.
        return TaskNode(
            key=self.key,
            init=self.init,
            prerequisite_inputs=self.prerequisite_inputs,
            inputs=self.inputs,
            outputs=self.outputs,
            log_output=self.log_output,
            metadata_output=self.metadata_output,
        )

    def _serialize(self, **kwargs: Any) -> dict[str, Any]:
        # Docstring inherited.
        return super()._serialize(dimensions=list(self.dimensions.names), **kwargs)
