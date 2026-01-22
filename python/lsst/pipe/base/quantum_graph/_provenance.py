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
    "ProvenanceDatasetInfo",
    "ProvenanceDatasetModel",
    "ProvenanceInitQuantumInfo",
    "ProvenanceInitQuantumModel",
    "ProvenanceLogRecordsModel",
    "ProvenanceQuantumGraph",
    "ProvenanceQuantumGraphReader",
    "ProvenanceQuantumGraphWriter",
    "ProvenanceQuantumInfo",
    "ProvenanceQuantumModel",
    "ProvenanceQuantumScanData",
    "ProvenanceQuantumScanModels",
    "ProvenanceQuantumScanStatus",
    "ProvenanceTaskMetadataModel",
)


import dataclasses
import enum
import itertools
import sys
import uuid
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import ExitStack, contextmanager
from typing import TYPE_CHECKING, Any, TypeAlias, TypedDict, TypeVar

import astropy.table
import networkx
import numpy as np
import pydantic

from lsst.daf.butler import DataCoordinate
from lsst.daf.butler.logging import ButlerLogRecord, ButlerLogRecords
from lsst.resources import ResourcePathExpression
from lsst.utils.iteration import ensure_iterable
from lsst.utils.logging import LsstLogAdapter, getLogger
from lsst.utils.packages import Packages

from .. import automatic_connection_constants as acc
from .._status import ExceptionInfo, QuantumAttemptStatus, QuantumSuccessCaveats
from .._task_metadata import TaskMetadata
from ..log_capture import _ExecutionLogRecordsExtra
from ..log_on_close import LogOnClose
from ..pipeline_graph import PipelineGraph, TaskImportMode, TaskInitNode
from ..resource_usage import QuantumResourceUsage
from ._common import (
    BaseQuantumGraph,
    BaseQuantumGraphReader,
    BaseQuantumGraphWriter,
    ConnectionName,
    DataCoordinateValues,
    DatasetInfo,
    DatasetTypeName,
    HeaderModel,
    QuantumInfo,
    TaskLabel,
)
from ._multiblock import Compressor, MultiblockReader, MultiblockWriter
from ._predicted import (
    PredictedDatasetModel,
    PredictedQuantumDatasetsModel,
    PredictedQuantumGraph,
    PredictedQuantumGraphComponents,
)

# Sphinx needs imports for type annotations of base class members.
if "sphinx" in sys.modules:
    import zipfile  # noqa: F401

    from ._multiblock import AddressReader, Decompressor  # noqa: F401


_T = TypeVar("_T")

LoopWrapper: TypeAlias = Callable[[Iterable[_T]], Iterable[_T]]

_LOG = getLogger(__file__)

DATASET_ADDRESS_INDEX = 0
QUANTUM_ADDRESS_INDEX = 1
LOG_ADDRESS_INDEX = 2
METADATA_ADDRESS_INDEX = 3

DATASET_MB_NAME = "datasets"
QUANTUM_MB_NAME = "quanta"
LOG_MB_NAME = "logs"
METADATA_MB_NAME = "metadata"


def pass_through(arg: _T) -> _T:
    return arg


class ProvenanceDatasetInfo(DatasetInfo):
    """A typed dictionary that annotates the attributes of the NetworkX graph
    node data for a provenance dataset.

    Since NetworkX types are not generic over their node mapping type, this has
    to be used explicitly, e.g.::

        node_data: ProvenanceDatasetInfo = xgraph.nodes[dataset_id]

    where ``xgraph`` is `ProvenanceQuantumGraph.bipartite_xgraph`.
    """

    dataset_id: uuid.UUID
    """Unique identifier for the dataset."""

    produced: bool
    """Whether this dataset was produced (vs. only predicted).

    This is always `True` for overall input datasets.  It is also `True` for
    datasets that were produced and then removed before/during transfer back to
    the central butler repository, so it may not reflect the continued
    existence of the dataset.
    """


class ProvenanceQuantumInfo(QuantumInfo):
    """A typed dictionary that annotates the attributes of the NetworkX graph
    node data for a provenance quantum.

    Since NetworkX types are not generic over their node mapping type, this has
    to be used explicitly, e.g.::

        node_data: ProvenanceQuantumInfo = xgraph.nodes[quantum_id]

    where ``xgraph`` is `ProvenanceQuantumGraph.bipartite_xgraph` or
    `ProvenanceQuantumGraph.quantum_only_xgraph`
    """

    status: QuantumAttemptStatus
    """Enumerated status for the quantum.

    This corresponds to the last attempt to run this quantum, or
    `QuantumAttemptStatus.BLOCKED` if there were no attempts.
    """

    caveats: QuantumSuccessCaveats | None
    """Flags indicating caveats on successful quanta.

    This corresponds to the last attempt to run this quantum.
    """

    exception: ExceptionInfo | None
    """Information about an exception raised when the quantum was executing.

    This corresponds to the last attempt to run this quantum.
    """

    resource_usage: QuantumResourceUsage | None
    """Resource usage information (timing, memory use) for this quantum.

    This corresponds to the last attempt to run this quantum.
    """

    attempts: list[ProvenanceQuantumAttemptModel]
    """Information about each attempt to run this quantum.

    An entry is added merely if the quantum *should* have been attempted; an
    empty `list` is used only for quanta that were blocked by an upstream
    failure.
    """

    metadata_id: uuid.UUID
    """ID of this quantum's metadata dataset."""

    log_id: uuid.UUID
    """ID of this quantum's log dataset."""


class ProvenanceInitQuantumInfo(TypedDict):
    """A typed dictionary that annotates the attributes of the NetworkX graph
    node data for a provenance init quantum.

    Since NetworkX types are not generic over their node mapping type, this has
    to be used explicitly, e.g.::

        node_data: ProvenanceInitQuantumInfo = xgraph.nodes[quantum_id]

    where ``xgraph`` is `ProvenanceQuantumGraph.bipartite_xgraph`.
    """

    data_id: DataCoordinate
    """Data ID of the quantum.

    This is always an empty ID; this key exists to allow init-quanta and
    regular quanta to be treated more similarly.
    """

    task_label: str
    """Label of the task for this quantum."""

    pipeline_node: TaskInitNode
    """Node in the pipeline graph for this task's init-only step."""

    config_id: uuid.UUID
    """ID of this task's config dataset."""


class ProvenanceDatasetModel(PredictedDatasetModel):
    """Data model for the datasets in a provenance quantum graph file."""

    produced: bool
    """Whether this dataset was produced (vs. only predicted).

    This is always `True` for overall input datasets.  It is also `True` for
    datasets that were produced and then removed before/during transfer back to
    the central butler repository, so it may not reflect the continued
    existence of the dataset.
    """

    producer: uuid.UUID | None = None
    """ID of the quantum that produced this dataset.

    This is `None` for overall inputs to the graph.
    """

    consumers: list[uuid.UUID] = pydantic.Field(default_factory=list)
    """IDs of quanta that were predicted to consume this dataset."""

    @property
    def node_id(self) -> uuid.UUID:
        """Alias for the dataset ID."""
        return self.dataset_id

    @classmethod
    def from_predicted(
        cls,
        predicted: PredictedDatasetModel,
        producer: uuid.UUID | None = None,
        consumers: Iterable[uuid.UUID] = (),
    ) -> ProvenanceDatasetModel:
        """Construct from a predicted dataset model.

        Parameters
        ----------
        predicted : `PredictedDatasetModel`
            Information about the dataset from the predicted graph.
        producer : `uuid.UUID` or `None`, optional
            ID of the quantum that was predicted to produce this dataset.
        consumers : `~collections.abc.Iterable` [`uuid.UUID`], optional
            IDs of the quanta that were predicted to consume this dataset.

        Returns
        -------
        provenance : `ProvenanceDatasetModel`
            Provenance dataset model.

        Notes
        -----
        This initializes `produced` to `True` when ``producer is None`` and
        `False` otherwise, on the assumption that it will be updated later.
        """
        return cls.model_construct(
            dataset_id=predicted.dataset_id,
            dataset_type_name=predicted.dataset_type_name,
            data_coordinate=predicted.data_coordinate,
            run=predicted.run,
            produced=(producer is None),  # if it's not produced by this QG, it's an overall input
            producer=producer,
            consumers=list(consumers),
        )

    def _add_to_graph(self, graph: ProvenanceQuantumGraph) -> None:
        """Add this dataset and its edges to quanta to a provenance graph.

        Parameters
        ----------
        graph : `ProvenanceQuantumGraph`
            Graph to update in place.

        Notes
        -----
        This method adds:

        - a ``bipartite_xgraph`` dataset node with full attributes;
        - ``bipartite_xgraph`` edges to adjacent quanta (which adds quantum
          nodes with no attributes), without populating edge attributes;
        - ``quantum_only_xgraph`` edges for each pair of quanta in which one
          produces this dataset and another consumes it (this also adds quantum
          nodes with no attributes).
        """
        dataset_type_node = graph.pipeline_graph.dataset_types[self.dataset_type_name]
        data_id = DataCoordinate.from_full_values(dataset_type_node.dimensions, tuple(self.data_coordinate))
        graph._bipartite_xgraph.add_node(
            self.dataset_id,
            data_id=data_id,
            dataset_type_name=self.dataset_type_name,
            pipeline_node=dataset_type_node,
            run=self.run,
            produced=self.produced,
        )
        if self.producer is not None:
            graph._bipartite_xgraph.add_edge(self.producer, self.dataset_id)
        for consumer_id in self.consumers:
            graph._bipartite_xgraph.add_edge(self.dataset_id, consumer_id)
            if self.producer is not None:
                graph._quantum_only_xgraph.add_edge(self.producer, consumer_id)
        graph._datasets_by_type[self.dataset_type_name][data_id] = self.dataset_id

    # Work around the fact that Sphinx chokes on Pydantic docstring formatting,
    # when we inherit those docstrings in our public classes.
    if "sphinx" in sys.modules and not TYPE_CHECKING:

        def copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.copy`."""
            return super().copy(*args, **kwargs)

        def model_dump(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump`."""
            return super().model_dump(*args, **kwargs)

        def model_dump_json(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump_json`."""
            return super().model_dump(*args, **kwargs)

        def model_copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_copy`."""
            return super().model_copy(*args, **kwargs)

        @classmethod
        def model_construct(cls, *args: Any, **kwargs: Any) -> Any:  # type: ignore[misc, override]
            """See `pydantic.BaseModel.model_construct`."""
            return super().model_construct(*args, **kwargs)

        @classmethod
        def model_json_schema(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_json_schema`."""
            return super().model_json_schema(*args, **kwargs)

        @classmethod
        def model_validate(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate`."""
            return super().model_validate(*args, **kwargs)

        @classmethod
        def model_validate_json(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate_json`."""
            return super().model_validate_json(*args, **kwargs)

        @classmethod
        def model_validate_strings(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate_strings`."""
            return super().model_validate_strings(*args, **kwargs)


class ProvenanceQuantumAttemptModel(pydantic.BaseModel):
    """Data model for a now-superseded attempt to run a quantum in a
    provenance quantum graph file.
    """

    attempt: int = 0
    """Counter incremented for every attempt to execute this quantum."""

    status: QuantumAttemptStatus = QuantumAttemptStatus.UNKNOWN
    """Enumerated status for the quantum."""

    caveats: QuantumSuccessCaveats | None = None
    """Flags indicating caveats on successful quanta."""

    exception: ExceptionInfo | None = None
    """Information about an exception raised when the quantum was executing."""

    resource_usage: QuantumResourceUsage | None = None
    """Resource usage information (timing, memory use) for this quantum."""

    previous_process_quanta: list[uuid.UUID] = pydantic.Field(default_factory=list)
    """The IDs of other quanta previously executed in the same process as this
    one.
    """

    # Work around the fact that Sphinx chokes on Pydantic docstring formatting,
    # when we inherit those docstrings in our public classes.
    if "sphinx" in sys.modules and not TYPE_CHECKING:

        def copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.copy`."""
            return super().copy(*args, **kwargs)

        def model_dump(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump`."""
            return super().model_dump(*args, **kwargs)

        def model_dump_json(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump_json`."""
            return super().model_dump(*args, **kwargs)

        def model_copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_copy`."""
            return super().model_copy(*args, **kwargs)

        @classmethod
        def model_construct(cls, *args: Any, **kwargs: Any) -> Any:  # type: ignore[misc, override]
            """See `pydantic.BaseModel.model_construct`."""
            return super().model_construct(*args, **kwargs)

        @classmethod
        def model_json_schema(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_json_schema`."""
            return super().model_json_schema(*args, **kwargs)

        @classmethod
        def model_validate(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate`."""
            return super().model_validate(*args, **kwargs)

        @classmethod
        def model_validate_json(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate_json`."""
            return super().model_validate_json(*args, **kwargs)

        @classmethod
        def model_validate_strings(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate_strings`."""
            return super().model_validate_strings(*args, **kwargs)


class ProvenanceLogRecordsModel(pydantic.BaseModel):
    """Data model for storing execution logs in a provenance quantum graph
    file.
    """

    attempts: list[list[ButlerLogRecord] | None] = pydantic.Field(default_factory=list)
    """Logs from attempts to run this task, ordered chronologically from first
    to last.
    """

    # Work around the fact that Sphinx chokes on Pydantic docstring formatting,
    # when we inherit those docstrings in our public classes.
    if "sphinx" in sys.modules and not TYPE_CHECKING:

        def copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.copy`."""
            return super().copy(*args, **kwargs)

        def model_dump(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump`."""
            return super().model_dump(*args, **kwargs)

        def model_dump_json(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump_json`."""
            return super().model_dump(*args, **kwargs)

        def model_copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_copy`."""
            return super().model_copy(*args, **kwargs)

        @classmethod
        def model_construct(cls, *args: Any, **kwargs: Any) -> Any:  # type: ignore[misc, override]
            """See `pydantic.BaseModel.model_construct`."""
            return super().model_construct(*args, **kwargs)

        @classmethod
        def model_json_schema(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_json_schema`."""
            return super().model_json_schema(*args, **kwargs)

        @classmethod
        def model_validate(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate`."""
            return super().model_validate(*args, **kwargs)

        @classmethod
        def model_validate_json(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate_json`."""
            return super().model_validate_json(*args, **kwargs)

        @classmethod
        def model_validate_strings(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate_strings`."""
            return super().model_validate_strings(*args, **kwargs)


class ProvenanceTaskMetadataModel(pydantic.BaseModel):
    """Data model for storing task metadata in a provenance quantum graph
    file.
    """

    attempts: list[TaskMetadata | None] = pydantic.Field(default_factory=list)
    """Metadata from attempts to run this task, ordered chronologically from
    first to last.
    """

    # Work around the fact that Sphinx chokes on Pydantic docstring formatting,
    # when we inherit those docstrings in our public classes.
    if "sphinx" in sys.modules and not TYPE_CHECKING:

        def copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.copy`."""
            return super().copy(*args, **kwargs)

        def model_dump(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump`."""
            return super().model_dump(*args, **kwargs)

        def model_dump_json(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump_json`."""
            return super().model_dump(*args, **kwargs)

        def model_copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_copy`."""
            return super().model_copy(*args, **kwargs)

        @classmethod
        def model_construct(cls, *args: Any, **kwargs: Any) -> Any:  # type: ignore[misc, override]
            """See `pydantic.BaseModel.model_construct`."""
            return super().model_construct(*args, **kwargs)

        @classmethod
        def model_json_schema(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_json_schema`."""
            return super().model_json_schema(*args, **kwargs)

        @classmethod
        def model_validate(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate`."""
            return super().model_validate(*args, **kwargs)

        @classmethod
        def model_validate_json(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate_json`."""
            return super().model_validate_json(*args, **kwargs)

        @classmethod
        def model_validate_strings(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate_strings`."""
            return super().model_validate_strings(*args, **kwargs)


class ProvenanceQuantumModel(pydantic.BaseModel):
    """Data model for the quanta in a provenance quantum graph file."""

    quantum_id: uuid.UUID
    """Unique identifier for the quantum."""

    task_label: TaskLabel
    """Name of the type of this dataset."""

    data_coordinate: DataCoordinateValues = pydantic.Field(default_factory=list)
    """The full values (required and implied) of this dataset's data ID."""

    inputs: dict[ConnectionName, list[uuid.UUID]] = pydantic.Field(default_factory=dict)
    """IDs of the datasets predicted to be consumed by this quantum, grouped by
    connection name.
    """

    outputs: dict[ConnectionName, list[uuid.UUID]] = pydantic.Field(default_factory=dict)
    """IDs of the datasets predicted to be produced by this quantum, grouped by
    connection name.
    """

    attempts: list[ProvenanceQuantumAttemptModel] = pydantic.Field(default_factory=list)
    """Provenance for all attempts to execute this quantum, ordered
    chronologically from first to last.

    An entry is added merely if the quantum *should* have been attempted; an
    empty `list` is used only for quanta that were blocked by an upstream
    failure.
    """

    @property
    def node_id(self) -> uuid.UUID:
        """Alias for the quantum ID."""
        return self.quantum_id

    @classmethod
    def from_predicted(cls, predicted: PredictedQuantumDatasetsModel) -> ProvenanceQuantumModel:
        """Construct from a predicted quantum model.

        Parameters
        ----------
        predicted : `PredictedQuantumDatasetsModel`
            Information about the quantum from the predicted graph.

        Returns
        -------
        provenance : `ProvenanceQuantumModel`
            Provenance quantum model.
        """
        inputs = {
            connection_name: [d.dataset_id for d in predicted_inputs]
            for connection_name, predicted_inputs in predicted.inputs.items()
        }
        outputs = {
            connection_name: [d.dataset_id for d in predicted_outputs]
            for connection_name, predicted_outputs in predicted.outputs.items()
        }
        return cls(
            quantum_id=predicted.quantum_id,
            task_label=predicted.task_label,
            data_coordinate=predicted.data_coordinate,
            inputs=inputs,
            outputs=outputs,
        )

    def _add_to_graph(self, graph: ProvenanceQuantumGraph) -> None:
        """Add this quantum and its edges to datasets to a provenance graph.

        Parameters
        ----------
        graph : `ProvenanceQuantumGraph`
            Graph to update in place.

        Notes
        -----
        This method adds:

        - a ``bipartite_xgraph`` quantum node with full attributes;
        - a ``quantum_only_xgraph`` quantum node with full attributes;
        - ``bipartite_xgraph`` edges to adjacent datasets (which adds datasets
          nodes with no attributes), while populating those edge attributes;
        - ``quantum_only_xgraph`` edges to any adjacent quantum that has also
          already been loaded.
        """
        task_node = graph.pipeline_graph.tasks[self.task_label]
        data_id = DataCoordinate.from_full_values(task_node.dimensions, tuple(self.data_coordinate))
        last_attempt = (
            self.attempts[-1]
            if self.attempts
            else ProvenanceQuantumAttemptModel(status=QuantumAttemptStatus.BLOCKED)
        )
        graph._bipartite_xgraph.add_node(
            self.quantum_id,
            data_id=data_id,
            task_label=self.task_label,
            pipeline_node=task_node,
            status=last_attempt.status,
            caveats=last_attempt.caveats,
            exception=last_attempt.exception,
            resource_usage=last_attempt.resource_usage,
            attempts=self.attempts,
        )
        graph._quanta_by_task_label[self.task_label][data_id] = self.quantum_id
        graph._quantum_only_xgraph.add_node(self.quantum_id, **graph._bipartite_xgraph.nodes[self.quantum_id])
        for connection_name, dataset_ids in self.inputs.items():
            read_edge = task_node.get_input_edge(connection_name)
            for dataset_id in dataset_ids:
                graph._bipartite_xgraph.add_edge(dataset_id, self.quantum_id, is_read=True)
                graph._bipartite_xgraph.edges[dataset_id, self.quantum_id].setdefault(
                    "pipeline_edges", []
                ).append(read_edge)
        for connection_name, dataset_ids in self.outputs.items():
            write_edge = task_node.get_output_edge(connection_name)
            if connection_name == acc.METADATA_OUTPUT_CONNECTION_NAME:
                graph._bipartite_xgraph.add_node(
                    dataset_ids[0],
                    data_id=data_id,
                    dataset_type_name=write_edge.dataset_type_name,
                    pipeline_node=graph.pipeline_graph.dataset_types[write_edge.dataset_type_name],
                    run=graph.header.output_run,
                    produced=last_attempt.status.has_metadata,
                )
                graph._datasets_by_type[write_edge.dataset_type_name][data_id] = dataset_ids[0]
                graph._bipartite_xgraph.nodes[self.quantum_id]["metadata_id"] = dataset_ids[0]
                graph._quantum_only_xgraph.nodes[self.quantum_id]["metadata_id"] = dataset_ids[0]
            if connection_name == acc.LOG_OUTPUT_CONNECTION_NAME:
                graph._bipartite_xgraph.add_node(
                    dataset_ids[0],
                    data_id=data_id,
                    dataset_type_name=write_edge.dataset_type_name,
                    pipeline_node=graph.pipeline_graph.dataset_types[write_edge.dataset_type_name],
                    run=graph.header.output_run,
                    produced=last_attempt.status.has_log,
                )
                graph._datasets_by_type[write_edge.dataset_type_name][data_id] = dataset_ids[0]
                graph._bipartite_xgraph.nodes[self.quantum_id]["log_id"] = dataset_ids[0]
                graph._quantum_only_xgraph.nodes[self.quantum_id]["log_id"] = dataset_ids[0]
            for dataset_id in dataset_ids:
                graph._bipartite_xgraph.add_edge(
                    self.quantum_id,
                    dataset_id,
                    is_read=False,
                    # There can only be one pipeline edge for an output.
                    pipeline_edges=[write_edge],
                )
        for dataset_id in graph._bipartite_xgraph.predecessors(self.quantum_id):
            for upstream_quantum_id in graph._bipartite_xgraph.predecessors(dataset_id):
                graph._quantum_only_xgraph.add_edge(upstream_quantum_id, self.quantum_id)
        for dataset_id in graph._bipartite_xgraph.successors(self.quantum_id):
            for downstream_quantum_id in graph._bipartite_xgraph.successors(dataset_id):
                graph._quantum_only_xgraph.add_edge(self.quantum_id, downstream_quantum_id)

    # Work around the fact that Sphinx chokes on Pydantic docstring formatting,
    # when we inherit those docstrings in our public classes.
    if "sphinx" in sys.modules and not TYPE_CHECKING:

        def copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.copy`."""
            return super().copy(*args, **kwargs)

        def model_dump(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump`."""
            return super().model_dump(*args, **kwargs)

        def model_dump_json(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump_json`."""
            return super().model_dump(*args, **kwargs)

        def model_copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_copy`."""
            return super().model_copy(*args, **kwargs)

        @classmethod
        def model_construct(cls, *args: Any, **kwargs: Any) -> Any:  # type: ignore[misc, override]
            """See `pydantic.BaseModel.model_construct`."""
            return super().model_construct(*args, **kwargs)

        @classmethod
        def model_json_schema(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_json_schema`."""
            return super().model_json_schema(*args, **kwargs)

        @classmethod
        def model_validate(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate`."""
            return super().model_validate(*args, **kwargs)

        @classmethod
        def model_validate_json(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate_json`."""
            return super().model_validate_json(*args, **kwargs)

        @classmethod
        def model_validate_strings(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate_strings`."""
            return super().model_validate_strings(*args, **kwargs)


class ProvenanceInitQuantumModel(pydantic.BaseModel):
    """Data model for the special "init" quanta in a provenance quantum graph
    file.
    """

    quantum_id: uuid.UUID
    """Unique identifier for the quantum."""

    task_label: TaskLabel
    """Name of the type of this dataset.

    This is always a parent dataset type name, not a component.

    Note that full dataset type definitions are stored in the pipeline graph.
    """

    inputs: dict[ConnectionName, uuid.UUID] = pydantic.Field(default_factory=dict)
    """IDs of the datasets predicted to be consumed by this quantum, grouped by
    connection name.
    """

    outputs: dict[ConnectionName, uuid.UUID] = pydantic.Field(default_factory=dict)
    """IDs of the datasets predicted to be produced by this quantum, grouped by
    connection name.
    """

    @classmethod
    def from_predicted(cls, predicted: PredictedQuantumDatasetsModel) -> ProvenanceInitQuantumModel:
        """Construct from a predicted quantum model.

        Parameters
        ----------
        predicted : `PredictedQuantumDatasetsModel`
            Information about the quantum from the predicted graph.

        Returns
        -------
        provenance : `ProvenanceInitQuantumModel`
            Provenance init quantum model.
        """
        inputs = {
            connection_name: predicted_inputs[0].dataset_id
            for connection_name, predicted_inputs in predicted.inputs.items()
        }
        outputs = {
            connection_name: predicted_outputs[0].dataset_id
            for connection_name, predicted_outputs in predicted.outputs.items()
        }
        return cls(
            quantum_id=predicted.quantum_id,
            task_label=predicted.task_label,
            inputs=inputs,
            outputs=outputs,
        )

    def _add_to_graph(self, graph: ProvenanceQuantumGraph, empty_data_id: DataCoordinate) -> None:
        """Add this quantum and its edges to datasets to a provenance graph.

        Parameters
        ----------
        graph : `ProvenanceQuantumGraph`
            Graph to update in place.
        empty_data_id : `lsst.daf.butler.DataCoordinate`
            The empty data ID for the appropriate dimension universe.

        Notes
        -----
        This method adds:

        - a ``bipartite_xgraph`` quantum node with full attributes;
        - ``bipartite_xgraph`` edges to adjacent datasets (which adds datasets
          nodes with no attributes), while populating those edge attributes;
        """
        task_init_node = graph.pipeline_graph.tasks[self.task_label].init
        graph._bipartite_xgraph.add_node(
            self.quantum_id, data_id=empty_data_id, task_label=self.task_label, pipeline_node=task_init_node
        )
        for connection_name, dataset_id in self.inputs.items():
            read_edge = task_init_node.get_input_edge(connection_name)
            graph._bipartite_xgraph.add_edge(dataset_id, self.quantum_id, is_read=True)
            graph._bipartite_xgraph.edges[dataset_id, self.quantum_id].setdefault(
                "pipeline_edges", []
            ).append(read_edge)
        for connection_name, dataset_id in self.outputs.items():
            write_edge = task_init_node.get_output_edge(connection_name)
            graph._bipartite_xgraph.add_node(
                dataset_id,
                data_id=empty_data_id,
                dataset_type_name=write_edge.dataset_type_name,
                pipeline_node=graph.pipeline_graph.dataset_types[write_edge.dataset_type_name],
                run=graph.header.output_run,
                produced=True,
            )
            graph._datasets_by_type[write_edge.dataset_type_name][empty_data_id] = dataset_id
            graph._bipartite_xgraph.add_edge(
                self.quantum_id,
                dataset_id,
                is_read=False,
                # There can only be one pipeline edge for an output.
                pipeline_edges=[write_edge],
            )
            if write_edge.connection_name == acc.CONFIG_INIT_OUTPUT_CONNECTION_NAME:
                graph._bipartite_xgraph.nodes[self.quantum_id]["config_id"] = dataset_id
        graph._init_quanta[self.task_label] = self.quantum_id

    # Work around the fact that Sphinx chokes on Pydantic docstring formatting,
    # when we inherit those docstrings in our public classes.
    if "sphinx" in sys.modules and not TYPE_CHECKING:

        def copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.copy`."""
            return super().copy(*args, **kwargs)

        def model_dump(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump`."""
            return super().model_dump(*args, **kwargs)

        def model_dump_json(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump_json`."""
            return super().model_dump(*args, **kwargs)

        def model_copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_copy`."""
            return super().model_copy(*args, **kwargs)

        @classmethod
        def model_construct(cls, *args: Any, **kwargs: Any) -> Any:  # type: ignore[misc, override]
            """See `pydantic.BaseModel.model_construct`."""
            return super().model_construct(*args, **kwargs)

        @classmethod
        def model_json_schema(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_json_schema`."""
            return super().model_json_schema(*args, **kwargs)

        @classmethod
        def model_validate(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate`."""
            return super().model_validate(*args, **kwargs)

        @classmethod
        def model_validate_json(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate_json`."""
            return super().model_validate_json(*args, **kwargs)

        @classmethod
        def model_validate_strings(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate_strings`."""
            return super().model_validate_strings(*args, **kwargs)


class ProvenanceInitQuantaModel(pydantic.RootModel):
    """Data model for the init quanta in a provenance graph."""

    root: list[ProvenanceInitQuantumModel] = pydantic.Field(default_factory=list)
    """List of special "init" quanta, one for each task."""

    def _add_to_graph(self, graph: ProvenanceQuantumGraph) -> None:
        """Add this quantum and its edges to datasets to a provenance graph.

        Parameters
        ----------
        graph : `ProvenanceQuantumGraph`
            Graph to update in place.
        """
        empty_data_id = DataCoordinate.make_empty(graph.pipeline_graph.universe)
        for init_quantum in self.root:
            init_quantum._add_to_graph(graph, empty_data_id=empty_data_id)

    # Work around the fact that Sphinx chokes on Pydantic docstring formatting,
    # when we inherit those docstrings in our public classes.
    if "sphinx" in sys.modules and not TYPE_CHECKING:

        def copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.copy`."""
            return super().copy(*args, **kwargs)

        def model_dump(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump`."""
            return super().model_dump(*args, **kwargs)

        def model_dump_json(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump_json`."""
            return super().model_dump(*args, **kwargs)

        def model_copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_copy`."""
            return super().model_copy(*args, **kwargs)

        @classmethod
        def model_construct(cls, *args: Any, **kwargs: Any) -> Any:  # type: ignore[misc, override]
            """See `pydantic.BaseModel.model_construct`."""
            return super().model_construct(*args, **kwargs)

        @classmethod
        def model_json_schema(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_json_schema`."""
            return super().model_json_schema(*args, **kwargs)

        @classmethod
        def model_validate(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate`."""
            return super().model_validate(*args, **kwargs)

        @classmethod
        def model_validate_json(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate_json`."""
            return super().model_validate_json(*args, **kwargs)

        @classmethod
        def model_validate_strings(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate_strings`."""
            return super().model_validate_strings(*args, **kwargs)


class ProvenanceQuantumGraph(BaseQuantumGraph):
    """A quantum graph that represents processing that has already been
    executed.

    Parameters
    ----------
    header : `HeaderModel`
        General metadata shared with other quantum graph types.
    pipeline_graph : `.pipeline_graph.PipelineGraph`
        Graph of tasks and dataset types.  May contain a superset of the tasks
        and dataset types that actually have quanta and datasets in the quantum
        graph.

    Notes
    -----
    A provenance quantum graph is generally obtained via the
    `ProvenanceQuantumGraphReader.graph` attribute, which is updated in-place
    as information is read from disk.
    """

    def __init__(self, header: HeaderModel, pipeline_graph: PipelineGraph) -> None:
        super().__init__(header, pipeline_graph)
        self._init_quanta: dict[TaskLabel, uuid.UUID] = {}
        self._quantum_only_xgraph = networkx.DiGraph()
        self._bipartite_xgraph = networkx.DiGraph()
        self._quanta_by_task_label: dict[str, dict[DataCoordinate, uuid.UUID]] = {
            task_label: {} for task_label in self.pipeline_graph.tasks.keys()
        }
        self._datasets_by_type: dict[str, dict[DataCoordinate, uuid.UUID]] = {
            dataset_type_name: {} for dataset_type_name in self.pipeline_graph.dataset_types.keys()
        }

    @property
    def init_quanta(self) -> Mapping[TaskLabel, uuid.UUID]:
        """A mapping from task label to the ID of the special init quantum for
        that task.

        This is populated by the ``init_quanta`` component.  Additional
        information about each init quantum can be found by using the ID to
        look up node attributes in the `bipartite_xgraph`, i.e.::

            info: ProvenanceInitQuantumInfo = qg.bipartite_xgraph.nodes[id]
        """
        return self._init_quanta

    @property
    def quanta_by_task(self) -> Mapping[TaskLabel, Mapping[DataCoordinate, uuid.UUID]]:
        """A nested mapping of all quanta, keyed first by task name and then by
        data ID.

        Notes
        -----
        This is populated one quantum at a time as they are read.  All tasks in
        the pipeline graph are included, even if none of their quanta were
        loaded (i.e. nested mappings may be empty).

        The returned object may be an internal dictionary; as the type
        annotation indicates, it should not be modified in place.
        """
        return self._quanta_by_task_label

    @property
    def datasets_by_type(self) -> Mapping[DatasetTypeName, Mapping[DataCoordinate, uuid.UUID]]:
        """A nested mapping of all datasets, keyed first by dataset type name
        and then by data ID.

        Notes
        -----
        This is populated one dataset at a time as they are read. All dataset
        types in the pipeline graph are included, even if none of their
        datasets were loaded (i.e. nested mappings may be empty).

        Reading a quantum also populates its log and metadata datasets.

        The returned object may be an internal dictionary; as the type
        annotation indicates, it should not be modified in place.
        """
        return self._datasets_by_type

    @property
    def quantum_only_xgraph(self) -> networkx.DiGraph:
        """A directed acyclic graph with quanta as nodes (and datasets elided).

        Notes
        -----
        Node keys are quantum UUIDs, and are populated one quantum at a time as
        they are loaded.  Loading quanta (via
        `ProvenanceQuantumGraphReader.read_quanta`) will add the loaded nodes
        with full attributes and add edges to adjacent nodes with no
        attributes. Loading datasets (via
        `ProvenanceQuantumGraphReader.read_datasets`) will also add edges and
        nodes with no attributes.

        Node attributes are described by the `ProvenanceQuantumInfo` types.

        This graph does not include special "init" quanta.

        The returned object is a read-only view of an internal one.
        """
        return self._quantum_only_xgraph.copy(as_view=True)

    @property
    def bipartite_xgraph(self) -> networkx.DiGraph:
        """A directed acyclic graph with quantum and dataset nodes.

        Notes
        -----
        Node keys are quantum or dataset UUIDs, and are populated one quantum
        or dataset at a time as they are loaded.  Loading quanta (via
        `ProvenanceQuantumGraphReader.read_quanta`) or datasets (via
        `ProvenanceQuantumGraphReader.read_datasets`) will load those nodes
        with full attributes and edges to adjacent nodes with no attributes.
        Loading quanta is necessary to populate edge attributes.
        Reading a quantum also populates its log and metadata datasets.

        Node attributes are described by the
        `ProvenanceQuantumInfo`, `ProvenanceInitQuantumInfo`, and
        `ProvenanceDatasetInfo` types.

        This graph includes init-input and init-output datasets, but it does
        *not* reflect the dependency between each task's special "init" quantum
        and its runtime quanta (as this would require edges between quanta, and
        that would break the "bipartite" property).

        The returned object is a read-only view of an internal one.
        """
        return self._bipartite_xgraph.copy(as_view=True)

    def make_quantum_table(self) -> astropy.table.Table:
        """Construct an `astropy.table.Table` with a tabular summary of the
        quanta.

        Returns
        -------
        table : `astropy.table.Table`
            A table view of the quantum information.  This only includes
            counts of status categories and caveats, not any per-data-ID
            detail.

        Notes
        -----
        Success caveats in the table are represented by their
        `~QuantumSuccessCaveats.concise` form, so when pretty-printing this
        table for users, the `~QuantumSuccessCaveats.legend` should generally
        be printed as well.
        """
        rows = []
        for task_label, quanta_for_task in self.quanta_by_task.items():
            if not self.header.n_task_quanta[task_label]:
                continue
            status_counts = Counter[QuantumAttemptStatus](
                self._quantum_only_xgraph.nodes[q]["status"] for q in quanta_for_task.values()
            )
            caveat_counts = Counter[QuantumSuccessCaveats | None](
                self._quantum_only_xgraph.nodes[q]["caveats"] for q in quanta_for_task.values()
            )
            caveat_counts.pop(QuantumSuccessCaveats.NO_CAVEATS, None)
            caveat_counts.pop(None, None)
            if len(caveat_counts) > 1:
                caveats = "(multiple)"
            elif len(caveat_counts) == 1:
                ((code, count),) = caveat_counts.items()
                # MyPy can't tell that the pop(None, None) above makes None
                # impossible here.
                caveats = f"{code.concise()}({count})"  # type: ignore[union-attr]
            else:
                caveats = ""
            rows.append(
                {
                    "Task": task_label,
                    "Unknown": status_counts.get(QuantumAttemptStatus.UNKNOWN, 0),
                    "Successful": status_counts.get(QuantumAttemptStatus.SUCCESSFUL, 0),
                    "Caveats": caveats,
                    "Blocked": status_counts.get(QuantumAttemptStatus.BLOCKED, 0),
                    "Failed": status_counts.get(QuantumAttemptStatus.FAILED, 0),
                    "TOTAL": len(quanta_for_task),
                    "EXPECTED": self.header.n_task_quanta[task_label],
                }
            )
        return astropy.table.Table(rows)

    def make_exception_table(self) -> astropy.table.Table:
        """Construct an `astropy.table.Table` with counts for each exception
        type raised by each task.

        Returns
        -------
        table : `astropy.table.Table`
            A table with columns for task label, exception type, and counts.
        """
        rows = []
        for task_label, quanta_for_task in self.quanta_by_task.items():
            counts_by_type = Counter(
                exc_info.type_name
                for q in quanta_for_task.values()
                if (exc_info := self._quantum_only_xgraph.nodes[q]["exception"]) is not None
            )
            for type_name, count in counts_by_type.items():
                rows.append({"Task": task_label, "Exception": type_name, "Count": count})
        return astropy.table.Table(rows)

    def make_task_resource_usage_table(
        self, task_label: TaskLabel, include_data_ids: bool = False
    ) -> astropy.table.Table:
        """Make a table of resource usage for a single task.

        Parameters
        ----------
        task_label : `str`
            Label of the task to extract resource usage for.
        include_data_ids : `bool`, optional
            Whether to also include data ID columns.

        Returns
        -------
        table : `astropy.table.Table`
            A table with columns for quantum ID and all fields in
            `QuantumResourceUsage`.
        """
        quanta_for_task = self.quanta_by_task[task_label]
        dtype_terms: list[tuple[str, np.dtype]] = [("quantum_id", np.dtype((np.void, 16)))]
        if include_data_ids:
            dimensions = self.pipeline_graph.tasks[task_label].dimensions
            for dimension_name in dimensions.data_coordinate_keys:
                dtype = np.dtype(self.pipeline_graph.universe.dimensions[dimension_name].primary_key.pytype)
                dtype_terms.append((dimension_name, dtype))
        fields = QuantumResourceUsage.get_numpy_fields()
        dtype_terms.extend(fields.items())
        row_dtype = np.dtype(dtype_terms)
        rows: list[object] = []
        for data_id, quantum_id in quanta_for_task.items():
            info: ProvenanceQuantumInfo = self._quantum_only_xgraph.nodes[quantum_id]
            if (resource_usage := info["resource_usage"]) is not None:
                row: tuple[object, ...] = (quantum_id.bytes,)
                if include_data_ids:
                    row += data_id.full_values
                row += resource_usage.get_numpy_row()
                rows.append(row)
        array = np.array(rows, dtype=row_dtype)
        return astropy.table.Table(array, units=QuantumResourceUsage.get_units())


@dataclasses.dataclass
class ProvenanceQuantumGraphReader(BaseQuantumGraphReader):
    """A helper class for reading provenance quantum graphs.

    Notes
    -----
    The `open` context manager should be used to construct new instances.
    Instances cannot be used after the context manager exits, except to access
    the `graph` attribute`.

    The various ``read_*`` methods in this class update the `graph` attribute
    in place.
    """

    graph: ProvenanceQuantumGraph = dataclasses.field(init=False)
    """Loaded provenance graph, populated in place as components are read."""

    @classmethod
    @contextmanager
    def open(
        cls,
        uri: ResourcePathExpression,
        *,
        page_size: int | None = None,
        import_mode: TaskImportMode = TaskImportMode.DO_NOT_IMPORT,
    ) -> Iterator[ProvenanceQuantumGraphReader]:
        """Construct a reader from a URI.

        Parameters
        ----------
        uri : convertible to `lsst.resources.ResourcePath`
            URI to open.  Should have a ``.qg`` extension.
        page_size : `int`, optional
            Approximate number of bytes to read at once from address files and
            multi-block files. Note that this does not set a page size for
            *all* reads, but it does affect the smallest, most numerous reads.
            Can also be set via the ``LSST_QG_PAGE_SIZE`` environment variable.
        import_mode : `.pipeline_graph.TaskImportMode`, optional
            How to handle importing the task classes referenced in the pipeline
            graph.

        Returns
        -------
        reader : `contextlib.AbstractContextManager` [ \
                `ProvenanceQuantumGraphReader` ]
            A context manager that returns the reader when entered.
        """
        with cls._open(
            uri,
            graph_type="provenance",
            address_filename="nodes",
            page_size=page_size,
            import_mode=import_mode,
            n_addresses=4,
        ) as self:
            yield self

    def __post_init__(self) -> None:
        self.graph = ProvenanceQuantumGraph(self.header, self.pipeline_graph)

    def read_init_quanta(self) -> None:
        """Read the thin graph, with all edge information and categorization of
        quanta by task label.
        """
        init_quanta = self._read_single_block("init_quanta", ProvenanceInitQuantaModel)
        for init_quantum in init_quanta.root:
            self.graph._init_quanta[init_quantum.task_label] = init_quantum.quantum_id
        init_quanta._add_to_graph(self.graph)

    def read_full_graph(self) -> None:
        """Read all bipartite edges and all quantum and dataset node
        attributes, fully populating the `graph` attribute.

        Notes
        -----
        This does not read logs, metadata, or packages ; those must always be
        fetched explicitly.
        """
        self.read_init_quanta()
        self.read_datasets()
        self.read_quanta()

    def read_datasets(self, datasets: Iterable[uuid.UUID] | None = None) -> None:
        """Read information about the given datasets.

        Parameters
        ----------
        datasets : `~collections.abc.Iterable` [`uuid.UUID`], optional
            Iterable of dataset IDs to load.  If not provided, all datasets
            will be loaded.  The UUIDs and indices of quanta will be ignored.
        """
        self._read_nodes(datasets, DATASET_ADDRESS_INDEX, DATASET_MB_NAME, ProvenanceDatasetModel)

    def read_quanta(self, quanta: Iterable[uuid.UUID] | None = None) -> None:
        """Read information about the given quanta.

        Parameters
        ----------
        quanta : `~collections.abc.Iterable` [`uuid.UUID`], optional
            Iterable of quantum IDs to load.  If not provided, all quanta will
            be loaded.  The UUIDs and indices of datasets and special init
            quanta will be ignored.
        """
        self._read_nodes(quanta, QUANTUM_ADDRESS_INDEX, QUANTUM_MB_NAME, ProvenanceQuantumModel)

    def _read_nodes(
        self,
        nodes: Iterable[uuid.UUID] | None,
        address_index: int,
        mb_name: str,
        model_type: type[ProvenanceDatasetModel] | type[ProvenanceQuantumModel],
    ) -> None:
        node: ProvenanceDatasetModel | ProvenanceQuantumModel | None
        if nodes is None:
            self.address_reader.read_all()
            nodes = self.address_reader.rows.keys()
            for node in MultiblockReader.read_all_models_in_zip(
                self.zf,
                mb_name,
                model_type,
                self.decompressor,
                int_size=self.header.int_size,
                page_size=self.page_size,
            ):
                if "pipeline_node" in self.graph._bipartite_xgraph.nodes.get(node.node_id, {}):
                    # Use the old node to reduce memory usage (since it might
                    # also have other outstanding reference holders).
                    continue
                node._add_to_graph(self.graph)
        else:
            with MultiblockReader.open_in_zip(self.zf, mb_name, int_size=self.header.int_size) as mb_reader:
                for node_id_or_index in nodes:
                    address_row = self.address_reader.find(node_id_or_index)
                    if "pipeline_node" in self.graph._bipartite_xgraph.nodes.get(address_row.key, {}):
                        # Use the old node to reduce memory usage (since it
                        # might also have other outstanding reference holders).
                        continue
                    node = mb_reader.read_model(
                        address_row.addresses[address_index], model_type, self.decompressor
                    )
                    if node is not None:
                        node._add_to_graph(self.graph)

    def fetch_logs(self, nodes: Iterable[uuid.UUID]) -> dict[uuid.UUID, list[ButlerLogRecords | None]]:
        """Fetch log datasets.

        Parameters
        ----------
        nodes : `~collections.abc.Iterable` [ `uuid.UUID` ]
            UUIDs of the log datasets themselves or of the quanta they
            correspond to.

        Returns
        -------
        logs : `dict` [ `uuid.UUID`, `list` [\
                `lsst.daf.butler.ButlerLogRecords` or `None`] ]
            Logs for the given IDs.  Each value is a list of
            `lsst.daf.butler.ButlerLogRecords` instances representing different
            execution attempts, ordered chronologically from first to last.
            Attempts where logs were missing will have `None` in this list.
        """
        result: dict[uuid.UUID, list[ButlerLogRecords | None]] = {}
        with MultiblockReader.open_in_zip(self.zf, LOG_MB_NAME, int_size=self.header.int_size) as mb_reader:
            for node_id_or_index in nodes:
                address_row = self.address_reader.find(node_id_or_index)
                logs_by_attempt = mb_reader.read_model(
                    address_row.addresses[LOG_ADDRESS_INDEX], ProvenanceLogRecordsModel, self.decompressor
                )
                if logs_by_attempt is not None:
                    result[node_id_or_index] = [
                        ButlerLogRecords.from_records(attempt_logs) if attempt_logs is not None else None
                        for attempt_logs in logs_by_attempt.attempts
                    ]
        return result

    def fetch_metadata(self, nodes: Iterable[uuid.UUID]) -> dict[uuid.UUID, list[TaskMetadata | None]]:
        """Fetch metadata datasets.

        Parameters
        ----------
        nodes : `~collections.abc.Iterable` [ `uuid.UUID` ]
            UUIDs of the metadata datasets themselves or of the quanta they
            correspond to.

        Returns
        -------
        metadata : `dict` [ `uuid.UUID`, `list` [`.TaskMetadata`] ]
            Metadata for the given IDs.  Each value is a list of
            `.TaskMetadata` instances representing different execution
            attempts, ordered chronologically from first to last. Attempts
            where metadata was missing (not written even in the fallback extra
            provenance in the logs) will have `None` in this list.
        """
        result: dict[uuid.UUID, list[TaskMetadata | None]] = {}
        with MultiblockReader.open_in_zip(
            self.zf, METADATA_MB_NAME, int_size=self.header.int_size
        ) as mb_reader:
            for node_id_or_index in nodes:
                address_row = self.address_reader.find(node_id_or_index)
                metadata_by_attempt = mb_reader.read_model(
                    address_row.addresses[METADATA_ADDRESS_INDEX],
                    ProvenanceTaskMetadataModel,
                    self.decompressor,
                )
                if metadata_by_attempt is not None:
                    result[node_id_or_index] = metadata_by_attempt.attempts
        return result

    def fetch_packages(self) -> Packages:
        """Fetch package version information."""
        data = self._read_single_block_raw("packages")
        return Packages.fromBytes(data, format="json")


class ProvenanceQuantumGraphWriter:
    """A struct of low-level writer objects for the main components of a
    provenance quantum graph.

    Parameters
    ----------
    output_path : `str`
        Path to write the graph to.
    exit_stack : `contextlib.ExitStack`
        Object that can be used to manage multiple context managers.
    log_on_close : `LogOnClose`
        Factory for context managers that log when closed.
    predicted : `.PredictedQuantumGraphComponents`
        Components of the predicted graph.
    zstd_level : `int`, optional
        Compression level.
    cdict_data : `bytes` or `None`, optional
        Bytes representation of the compression dictionary used by the
        compressor.
    loop_wrapper : `~collections.abc.Callable`, optional
        A callable that takes an iterable and returns an equivalent one, to be
        used in all potentially-large loops.  This can be used to add progress
        reporting or check for cancelation signals.
    log : `LsstLogAdapter`, optional
        Logger to use for debug messages.
    """

    def __init__(
        self,
        output_path: str,
        *,
        exit_stack: ExitStack,
        log_on_close: LogOnClose,
        predicted: PredictedQuantumGraphComponents | PredictedQuantumGraph,
        zstd_level: int = 10,
        cdict_data: bytes | None = None,
        loop_wrapper: LoopWrapper = pass_through,
        log: LsstLogAdapter | None = None,
    ) -> None:
        header = predicted.header.model_copy()
        header.graph_type = "provenance"
        if log is None:
            log = _LOG
        self.log = log
        self._base_writer = exit_stack.enter_context(
            log_on_close.wrap(
                BaseQuantumGraphWriter.open(
                    output_path,
                    header,
                    predicted.pipeline_graph,
                    address_filename="nodes",
                    zstd_level=zstd_level,
                    cdict_data=cdict_data,
                ),
                "Finishing writing provenance quantum graph.",
            )
        )
        self._base_writer.address_writer.addresses = [{}, {}, {}, {}]
        self._log_writer = exit_stack.enter_context(
            log_on_close.wrap(
                MultiblockWriter.open_in_zip(
                    self._base_writer.zf, LOG_MB_NAME, header.int_size, use_tempfile=True
                ),
                "Copying logs into zip archive.",
            ),
        )
        self._base_writer.address_writer.addresses[LOG_ADDRESS_INDEX] = self._log_writer.addresses
        self._metadata_writer = exit_stack.enter_context(
            log_on_close.wrap(
                MultiblockWriter.open_in_zip(
                    self._base_writer.zf, METADATA_MB_NAME, header.int_size, use_tempfile=True
                ),
                "Copying metadata into zip archive.",
            )
        )
        self._base_writer.address_writer.addresses[METADATA_ADDRESS_INDEX] = self._metadata_writer.addresses
        self._dataset_writer = exit_stack.enter_context(
            log_on_close.wrap(
                MultiblockWriter.open_in_zip(
                    self._base_writer.zf, DATASET_MB_NAME, header.int_size, use_tempfile=True
                ),
                "Copying dataset provenance into zip archive.",
            )
        )
        self._base_writer.address_writer.addresses[DATASET_ADDRESS_INDEX] = self._dataset_writer.addresses
        self._quantum_writer = exit_stack.enter_context(
            log_on_close.wrap(
                MultiblockWriter.open_in_zip(
                    self._base_writer.zf, QUANTUM_MB_NAME, header.int_size, use_tempfile=True
                ),
                "Copying quantum provenance into zip archive.",
            )
        )
        self._base_writer.address_writer.addresses[QUANTUM_ADDRESS_INDEX] = self._quantum_writer.addresses
        self._init_predicted_quanta(predicted)
        self._populate_xgraph_and_inputs(loop_wrapper)
        self._existing_init_outputs: set[uuid.UUID] = set()

    def _init_predicted_quanta(
        self, predicted: PredictedQuantumGraph | PredictedQuantumGraphComponents
    ) -> None:
        self._predicted_init_quanta: list[PredictedQuantumDatasetsModel] = []
        self._predicted_quanta: dict[uuid.UUID, PredictedQuantumDatasetsModel] = {}
        if isinstance(predicted, PredictedQuantumGraph):
            self._predicted_init_quanta.extend(predicted._init_quanta.values())
            self._predicted_quanta.update(predicted._quantum_datasets)
        else:
            self._predicted_init_quanta.extend(predicted.init_quanta.root)
            self._predicted_quanta.update(predicted.quantum_datasets)
        self._predicted_quanta.update({q.quantum_id: q for q in self._predicted_init_quanta})

    def _populate_xgraph_and_inputs(self, loop_wrapper: LoopWrapper = pass_through) -> None:
        self._xgraph = networkx.DiGraph()
        self._overall_inputs: dict[uuid.UUID, PredictedDatasetModel] = {}
        output_dataset_ids: set[uuid.UUID] = set()
        for predicted_quantum in loop_wrapper(self._predicted_quanta.values()):
            if not predicted_quantum.task_label:
                # Skip the 'packages' producer quantum.
                continue
            output_dataset_ids.update(predicted_quantum.iter_output_dataset_ids())
        for predicted_quantum in loop_wrapper(self._predicted_quanta.values()):
            if not predicted_quantum.task_label:
                # Skip the 'packages' producer quantum.
                continue
            for predicted_input in itertools.chain.from_iterable(predicted_quantum.inputs.values()):
                self._xgraph.add_edge(predicted_input.dataset_id, predicted_quantum.quantum_id)
                if predicted_input.dataset_id not in output_dataset_ids:
                    self._overall_inputs.setdefault(predicted_input.dataset_id, predicted_input)
            for predicted_output in itertools.chain.from_iterable(predicted_quantum.outputs.values()):
                self._xgraph.add_edge(predicted_quantum.quantum_id, predicted_output.dataset_id)

    @property
    def compressor(self) -> Compressor:
        """Object that should be used to compress all JSON blocks."""
        return self._base_writer.compressor

    def write_packages(self) -> None:
        """Write package version information to the provenance graph."""
        packages = Packages.fromSystem(include_all=True)
        data = packages.toBytes("json")
        self._base_writer.write_single_block("packages", data)

    def write_overall_inputs(self, loop_wrapper: LoopWrapper = pass_through) -> None:
        """Write provenance for overall-input datasets.

        Parameters
        ----------
        loop_wrapper : `~collections.abc.Callable`, optional
            A callable that takes an iterable and returns an equivalent one, to
            be used in all potentially-large loops.  This can be used to add
            progress reporting or check for cancelation signals.
        """
        for predicted_input in loop_wrapper(self._overall_inputs.values()):
            if predicted_input.dataset_id not in self._dataset_writer.addresses:
                self._dataset_writer.write_model(
                    predicted_input.dataset_id,
                    ProvenanceDatasetModel.from_predicted(
                        predicted_input,
                        producer=None,
                        consumers=self._xgraph.successors(predicted_input.dataset_id),
                    ),
                    self.compressor,
                )
        del self._overall_inputs

    def write_init_outputs(self, assume_existence: bool = True) -> None:
        """Write provenance for init-output datasets and init-quanta.

        Parameters
        ----------
        assume_existence : `bool`, optional
            If `True`, just assume all init-outputs exist.
        """
        init_quanta = ProvenanceInitQuantaModel()
        for predicted_init_quantum in self._predicted_init_quanta:
            if not predicted_init_quantum.task_label:
                # Skip the 'packages' producer quantum.
                continue
            for predicted_output in itertools.chain.from_iterable(predicted_init_quantum.outputs.values()):
                provenance_output = ProvenanceDatasetModel.from_predicted(
                    predicted_output,
                    producer=predicted_init_quantum.quantum_id,
                    consumers=self._xgraph.successors(predicted_output.dataset_id),
                )
                provenance_output.produced = assume_existence or (
                    provenance_output.dataset_id in self._existing_init_outputs
                )
                self._dataset_writer.write_model(
                    provenance_output.dataset_id, provenance_output, self.compressor
                )
            init_quanta.root.append(ProvenanceInitQuantumModel.from_predicted(predicted_init_quantum))
        self._base_writer.write_single_model("init_quanta", init_quanta)

    def write_quantum_provenance(
        self, quantum_id: uuid.UUID, metadata: TaskMetadata | None, logs: ButlerLogRecords | None
    ) -> None:
        """Gather and write provenance for a quantum.

        Parameters
        ----------
        quantum_id : `uuid.UUID`
            Unique ID for the quantum.
        metadata : `..TaskMetadata` or `None`
            Task metadata.
        logs : `lsst.daf.butler.logging.ButlerLogRecords` or `None`
            Task logs.
        """
        predicted_quantum = self._predicted_quanta[quantum_id]
        provenance_models = ProvenanceQuantumScanModels.from_metadata_and_logs(
            predicted_quantum, metadata, logs, incomplete=False
        )
        scan_data = provenance_models.to_scan_data(predicted_quantum, compressor=self.compressor)
        self.write_scan_data(scan_data)

    def write_scan_data(self, scan_data: ProvenanceQuantumScanData) -> None:
        """Write the output of a quantum provenance scan to disk.

        Parameters
        ----------
        scan_data : `ProvenanceQuantumScanData`
            Result of a quantum provenance scan.
        """
        if scan_data.status is ProvenanceQuantumScanStatus.INIT:
            self.log.debug("Handling init-output scan for %s.", scan_data.quantum_id)
            self._existing_init_outputs.update(scan_data.existing_outputs)
            return
        self.log.debug("Handling quantum scan for %s.", scan_data.quantum_id)
        # We shouldn't need this predicted quantum after this method runs; pop
        # from the dict it in the hopes that'll free up some memory when we're
        # done.
        predicted_quantum = self._predicted_quanta.pop(scan_data.quantum_id)
        outputs: dict[uuid.UUID, bytes] = {}
        for predicted_output in itertools.chain.from_iterable(predicted_quantum.outputs.values()):
            provenance_output = ProvenanceDatasetModel.from_predicted(
                predicted_output,
                producer=predicted_quantum.quantum_id,
                consumers=self._xgraph.successors(predicted_output.dataset_id),
            )
            provenance_output.produced = provenance_output.dataset_id in scan_data.existing_outputs
            outputs[provenance_output.dataset_id] = self.compressor.compress(
                provenance_output.model_dump_json().encode()
            )
        if not scan_data.quantum:
            scan_data.quantum = (
                ProvenanceQuantumModel.from_predicted(predicted_quantum).model_dump_json().encode()
            )
            if scan_data.is_compressed:
                scan_data.quantum = self.compressor.compress(scan_data.quantum)
        if not scan_data.is_compressed:
            scan_data.quantum = self.compressor.compress(scan_data.quantum)
            if scan_data.metadata:
                scan_data.metadata = self.compressor.compress(scan_data.metadata)
            if scan_data.logs:
                scan_data.logs = self.compressor.compress(scan_data.logs)
        self.log.debug("Writing quantum %s.", scan_data.quantum_id)
        self._quantum_writer.write_bytes(scan_data.quantum_id, scan_data.quantum)
        for dataset_id, dataset_data in outputs.items():
            self._dataset_writer.write_bytes(dataset_id, dataset_data)
        if scan_data.metadata:
            (metadata_output,) = predicted_quantum.outputs[acc.METADATA_OUTPUT_CONNECTION_NAME]
            address = self._metadata_writer.write_bytes(scan_data.quantum_id, scan_data.metadata)
            self._metadata_writer.addresses[metadata_output.dataset_id] = address
        if scan_data.logs:
            (log_output,) = predicted_quantum.outputs[acc.LOG_OUTPUT_CONNECTION_NAME]
            address = self._log_writer.write_bytes(scan_data.quantum_id, scan_data.logs)
            self._log_writer.addresses[log_output.dataset_id] = address


class ProvenanceQuantumScanStatus(enum.Enum):
    """Status enum for quantum scanning.

    Note that this records the status for the *scanning* which is distinct
    from the status of the quantum's execution.
    """

    INCOMPLETE = enum.auto()
    """The quantum is not necessarily done running, and cannot be scanned
    conclusively yet.
    """

    ABANDONED = enum.auto()
    """The quantum's execution appears to have failed but we cannot rule out
    the possibility that it could be recovered, but we've also waited long
    enough (according to `ScannerTimeConfigDict.retry_timeout`) that it's time
    to stop trying for now.

    This state means `ProvenanceQuantumScanModels.from_metadata_and_logs` must
    be run again with ``incomplete=False``.
    """

    SUCCESSFUL = enum.auto()
    """The quantum was conclusively scanned and was executed successfully,
    unblocking scans for downstream quanta.
    """

    FAILED = enum.auto()
    """The quantum was conclusively scanned and failed execution, blocking
    scans for downstream quanta.
    """

    BLOCKED = enum.auto()
    """A quantum upstream of this one failed."""

    INIT = enum.auto()
    """Init quanta need special handling, because they don't have logs and
    metadata.
    """


@dataclasses.dataclass
class ProvenanceQuantumScanModels:
    """A struct that represents provenance information for a single quantum."""

    quantum_id: uuid.UUID
    """Unique ID for the quantum."""

    status: ProvenanceQuantumScanStatus = ProvenanceQuantumScanStatus.INCOMPLETE
    """Combined status for the scan and the execution of the quantum."""

    attempts: list[ProvenanceQuantumAttemptModel] = dataclasses.field(default_factory=list)
    """Provenance information about each attempt to run the quantum."""

    output_existence: dict[uuid.UUID, bool] = dataclasses.field(default_factory=dict)
    """Unique IDs of the output datasets mapped to whether they were actually
    produced.
    """

    metadata: ProvenanceTaskMetadataModel = dataclasses.field(default_factory=ProvenanceTaskMetadataModel)
    """Task metadata information for each attempt.
    """

    logs: ProvenanceLogRecordsModel = dataclasses.field(default_factory=ProvenanceLogRecordsModel)
    """Log records for each attempt.
    """

    @classmethod
    def from_metadata_and_logs(
        cls,
        predicted: PredictedQuantumDatasetsModel,
        metadata: TaskMetadata | None,
        logs: ButlerLogRecords | None,
        *,
        incomplete: bool = False,
    ) -> ProvenanceQuantumScanModels:
        """Construct provenance information from task metadata and logs.

        Parameters
        ----------
        predicted : `PredictedQuantumDatasetsModel`
            Information about the predicted quantum.
        metadata : `..TaskMetadata` or `None`
            Task metadata.
        logs : `lsst.daf.butler.logging.ButlerLogRecords` or `None`
            Task logs.
        incomplete : `bool`, optional
            If `True`, treat execution failures as possibly-incomplete quanta
            and do not fully process them; instead just set the status to
            `ProvenanceQuantumScanStatus.ABANDONED` and return.

        Returns
        -------
        scan_models : `ProvenanceQuantumScanModels`
            Struct of models that describe quantum provenance.

        Notes
        -----
        This method does not necessarily fully populate the `output_existence`
        field; it does what it can given the information in the metadata and
        logs, but the caller is responsible for filling in the existence status
        for any predicted outputs that are not present at all in that `dict`.
        """
        self = ProvenanceQuantumScanModels(predicted.quantum_id)
        last_attempt = ProvenanceQuantumAttemptModel()
        self._process_logs(predicted, logs, last_attempt, incomplete=incomplete)
        self._process_metadata(predicted, metadata, last_attempt, incomplete=incomplete)
        if self.status is ProvenanceQuantumScanStatus.ABANDONED:
            return self
        self._reconcile_attempts(last_attempt)
        self._extract_output_existence(predicted)
        return self

    def _process_logs(
        self,
        predicted: PredictedQuantumDatasetsModel,
        logs: ButlerLogRecords | None,
        last_attempt: ProvenanceQuantumAttemptModel,
        *,
        incomplete: bool,
    ) -> None:
        (predicted_log_dataset,) = predicted.outputs[acc.LOG_OUTPUT_CONNECTION_NAME]
        if logs is None:
            self.output_existence[predicted_log_dataset.dataset_id] = False
            if incomplete:
                self.status = ProvenanceQuantumScanStatus.ABANDONED
            else:
                self.status = ProvenanceQuantumScanStatus.FAILED
        else:
            # Set the attempt's run status to FAILED, since the default is
            # UNKNOWN (i.e. logs *and* metadata are missing) and we now know
            # the logs exist.  This will usually get replaced by SUCCESSFUL
            # when we look for metadata next.
            last_attempt.status = QuantumAttemptStatus.FAILED
            self.output_existence[predicted_log_dataset.dataset_id] = True
            if logs.extra:
                log_extra = _ExecutionLogRecordsExtra.model_validate(logs.extra)
                self._extract_from_log_extra(log_extra, last_attempt=last_attempt)
            self.logs.attempts.append(list(logs))

    def _extract_from_log_extra(
        self,
        log_extra: _ExecutionLogRecordsExtra,
        last_attempt: ProvenanceQuantumAttemptModel | None,
    ) -> None:
        for previous_attempt_log_extra in log_extra.previous_attempts:
            self._extract_from_log_extra(
                previous_attempt_log_extra,
                last_attempt=None,
            )
        quantum_attempt: ProvenanceQuantumAttemptModel
        if last_attempt is None:
            # This is not the last attempt, so it must be a failure.
            quantum_attempt = ProvenanceQuantumAttemptModel(
                attempt=len(self.attempts), status=QuantumAttemptStatus.FAILED
            )
            # We also need to get the logs from this extra provenance, since
            # they won't be the main section of the log records.
            self.logs.attempts.append(log_extra.logs)
            # The special last attempt is only appended after we attempt to
            # read metadata later, but we have to append this one now.
            self.attempts.append(quantum_attempt)
        else:
            assert not log_extra.logs, "Logs for the last attempt should not be stored in the extra JSON."
            quantum_attempt = last_attempt
        if log_extra.exception is not None or log_extra.metadata is not None or last_attempt is None:
            # We won't be getting a separate metadata dataset, so anything we
            # might get from the metadata has to come from this extra
            # provenance in the logs.
            quantum_attempt.exception = log_extra.exception
            if log_extra.metadata is not None:
                quantum_attempt.resource_usage = QuantumResourceUsage.from_task_metadata(log_extra.metadata)
                self.metadata.attempts.append(log_extra.metadata)
            else:
                self.metadata.attempts.append(None)
        # Regardless of whether this is the last attempt or not, we can only
        # get the previous_process_quanta from the log extra.
        quantum_attempt.previous_process_quanta.extend(log_extra.previous_process_quanta)

    def _process_metadata(
        self,
        predicted: PredictedQuantumDatasetsModel,
        metadata: TaskMetadata | None,
        last_attempt: ProvenanceQuantumAttemptModel,
        *,
        incomplete: bool,
    ) -> None:
        (predicted_metadata_dataset,) = predicted.outputs[acc.METADATA_OUTPUT_CONNECTION_NAME]
        if metadata is None:
            self.output_existence[predicted_metadata_dataset.dataset_id] = False
            if incomplete:
                self.status = ProvenanceQuantumScanStatus.ABANDONED
            else:
                self.status = ProvenanceQuantumScanStatus.FAILED
        else:
            self.status = ProvenanceQuantumScanStatus.SUCCESSFUL
            self.output_existence[predicted_metadata_dataset.dataset_id] = True
            last_attempt.status = QuantumAttemptStatus.SUCCESSFUL
            try:
                # Int conversion guards against spurious conversion to
                # float that can apparently sometimes happen in
                # TaskMetadata.
                last_attempt.caveats = QuantumSuccessCaveats(int(metadata["quantum"]["caveats"]))
            except LookupError:
                pass
            try:
                last_attempt.exception = ExceptionInfo._from_metadata(
                    metadata[predicted.task_label]["failure"]
                )
            except LookupError:
                pass
            last_attempt.resource_usage = QuantumResourceUsage.from_task_metadata(metadata)
            self.metadata.attempts.append(metadata)

    def _reconcile_attempts(self, last_attempt: ProvenanceQuantumAttemptModel) -> None:
        last_attempt.attempt = len(self.attempts)
        self.attempts.append(last_attempt)
        assert self.status is not ProvenanceQuantumScanStatus.INCOMPLETE
        assert self.status is not ProvenanceQuantumScanStatus.ABANDONED
        if len(self.logs.attempts) < len(self.attempts):
            # Logs were not found for this attempt; must have been a hard error
            # that kept the `finally` block from running or otherwise
            # interrupted the writing of the logs.
            self.logs.attempts.append(None)
            if self.status is ProvenanceQuantumScanStatus.SUCCESSFUL:
                # But we found the metadata!  Either that hard error happened
                # at a very unlucky time (in between those two writes), or
                # something even weirder happened.
                self.attempts[-1].status = QuantumAttemptStatus.ABORTED_SUCCESS
            else:
                self.attempts[-1].status = QuantumAttemptStatus.FAILED
        if len(self.metadata.attempts) < len(self.attempts):
            # Metadata missing usually just means a failure.  In any case, the
            # status will already be correct, either because it was set to a
            # failure when we read the logs, or left at UNKNOWN if there were
            # no logs.  Note that scanners never process BLOCKED quanta at all.
            self.metadata.attempts.append(None)
        assert len(self.logs.attempts) == len(self.attempts) or len(self.metadata.attempts) == len(
            self.attempts
        ), (
            "The only way we can add more than one quantum attempt is by "
            "extracting info stored with the logs, and that always appends "
            "a log attempt and a metadata attempt, so this must be a bug in "
            "this class."
        )

    def _extract_output_existence(self, predicted: PredictedQuantumDatasetsModel) -> None:
        try:
            outputs_put = self.metadata.attempts[-1]["quantum"].getArray("outputs")  # type: ignore[index]
        except (
            IndexError,  # metadata.attempts is empty
            TypeError,  # metadata.attempts[-1] is None
            LookupError,  # no 'quantum' entry in metadata or 'outputs' in that
        ):
            pass
        else:
            for id_str in ensure_iterable(outputs_put):
                self.output_existence[uuid.UUID(id_str)] = True
            # If the metadata told us what it wrote, anything not in that
            # list was not written.
            for predicted_output in itertools.chain.from_iterable(predicted.outputs.values()):
                self.output_existence.setdefault(predicted_output.dataset_id, False)

    def to_scan_data(
        self: ProvenanceQuantumScanModels,
        predicted_quantum: PredictedQuantumDatasetsModel,
        compressor: Compressor | None = None,
    ) -> ProvenanceQuantumScanData:
        """Convert these models to JSON data.

        Parameters
        ----------
        predicted_quantum : `PredictedQuantumDatasetsModel`
            Information about the predicted quantum.
        compressor : `Compressor`
            Object that can compress bytes.

        Returns
        -------
        scan_data : `ProvenanceQuantumScanData`
            Scan information ready for serialization.
        """
        quantum: ProvenanceInitQuantumModel | ProvenanceQuantumModel
        if self.status is ProvenanceQuantumScanStatus.INIT:
            quantum = ProvenanceInitQuantumModel.from_predicted(predicted_quantum)
        else:
            quantum = ProvenanceQuantumModel.from_predicted(predicted_quantum)
            quantum.attempts = self.attempts
        for predicted_output in itertools.chain.from_iterable(predicted_quantum.outputs.values()):
            if predicted_output.dataset_id not in self.output_existence:
                raise RuntimeError(
                    "Logic bug in provenance gathering or execution invariants: "
                    f"no existence information for output {predicted_output.dataset_id} "
                    f"({predicted_output.dataset_type_name}@{predicted_output.data_coordinate})."
                )
        data = ProvenanceQuantumScanData(
            self.quantum_id,
            self.status,
            existing_outputs={
                dataset_id for dataset_id, was_produced in self.output_existence.items() if was_produced
            },
            quantum=quantum.model_dump_json().encode(),
            logs=self.logs.model_dump_json().encode() if self.logs.attempts else b"",
            metadata=self.metadata.model_dump_json().encode() if self.metadata.attempts else b"",
        )
        if compressor is not None:
            data.compress(compressor)
        return data


@dataclasses.dataclass
class ProvenanceQuantumScanData:
    """A struct that represents ready-for-serialization provenance information
    for a single quantum.
    """

    quantum_id: uuid.UUID
    """Unique ID for the quantum."""

    status: ProvenanceQuantumScanStatus
    """Combined status for the scan and the execution of the quantum."""

    existing_outputs: set[uuid.UUID] = dataclasses.field(default_factory=set)
    """Unique IDs of the output datasets that were actually written."""

    quantum: bytes = b""
    """Serialized quantum provenance model.

    This may be empty for quanta that had no attempts.
    """

    metadata: bytes = b""
    """Serialized task metadata."""

    logs: bytes = b""
    """Serialized logs."""

    is_compressed: bool = False
    """Whether the `quantum`, `metadata`, and `log` attributes are
    compressed.
    """

    def compress(self, compressor: Compressor) -> None:
        """Compress the data in this struct if it has not been compressed
        already.

        Parameters
        ----------
        compressor : `Compressor`
            Object with a ``compress`` method that takes and returns `bytes`.
        """
        if not self.is_compressed:
            self.quantum = compressor.compress(self.quantum)
            self.logs = compressor.compress(self.logs) if self.logs else b""
            self.metadata = compressor.compress(self.metadata) if self.metadata else b""
            self.is_compressed = True
