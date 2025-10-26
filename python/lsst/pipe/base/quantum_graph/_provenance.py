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
    "ProvenanceQuantumGraph",
    "ProvenanceQuantumGraphReader",
    "ProvenanceQuantumInfo",
    "ProvenanceQuantumModel",
)


import dataclasses
import sys
import uuid
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Self, TypedDict

import astropy.table
import networkx
import numpy as np
import pydantic

from lsst.daf.butler import DataCoordinate
from lsst.resources import ResourcePathExpression
from lsst.utils.packages import Packages

from .._status import QuantumSuccessCaveats
from ..pipeline_graph import PipelineGraph, TaskImportMode, TaskInitNode
from ..quantum_provenance_graph import ExceptionInfo, QuantumRunStatus
from ..resource_usage import QuantumResourceUsage
from ._common import (
    BaseQuantumGraph,
    BaseQuantumGraphReader,
    ConnectionName,
    DataCoordinateValues,
    DatasetIndex,
    DatasetInfo,
    DatasetTypeName,
    HeaderModel,
    QuantumIndex,
    QuantumInfo,
    TaskLabel,
)
from ._multiblock import AddressReader, MultiblockReader
from ._predicted import PredictedDatasetModel, PredictedQuantumDatasetsModel

if TYPE_CHECKING:
    from lsst.daf.butler.logging import ButlerLogRecords

    from .._task_metadata import TaskMetadata


DATASET_ADDRESS_INDEX = 0
QUANTUM_ADDRESS_INDEX = 1
LOG_ADDRESS_INDEX = 2
METADATA_ADDRESS_INDEX = 3

DATASET_MB_NAME = "datasets"
QUANTUM_MB_NAME = "quanta"
LOG_MB_NAME = "logs"
METADATA_MB_NAME = "metadata"


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

    status: QuantumRunStatus
    """Enumerated status for the quantum."""

    caveats: QuantumSuccessCaveats | None
    """Flags indicating caveats on successful quanta."""

    exception: ExceptionInfo | None
    """Information about an exception raised when the quantum was executing."""

    resource_usage: QuantumResourceUsage | None
    """Resource usage information (timing, memory use) for this quantum."""


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


class ProvenanceDatasetModel(PredictedDatasetModel):
    """Data model for the datasets in a provenance quantum graph file."""

    produced: bool
    """Whether this dataset was produced (vs. only predicted).

    This is always `True` for overall input datasets.  It is also `True` for
    datasets that were produced and then removed before/during transfer back to
    the central butler repository, so it may not reflect the continued
    existence of the dataset.
    """

    producer: QuantumIndex | None = None
    """Internal integer ID of the quantum that produced this dataset.

    This is `None` for overall inputs to the graph.
    """

    consumers: list[QuantumIndex] = pydantic.Field(default_factory=list)
    """Internal integer IDs of quanta that were predicted to consume this
    dataset.
    """

    @property
    def node_id(self) -> uuid.UUID:
        """Alias for the dataset ID."""
        return self.dataset_id

    @classmethod
    def from_predicted(
        cls,
        predicted: PredictedDatasetModel,
        producer: QuantumIndex | None = None,
        consumers: Iterable[QuantumIndex] = (),
    ) -> ProvenanceDatasetModel:
        """Construct from a predicted dataset model.

        Parameters
        ----------
        predicted : `PredictedDatasetModel`
            Information about the dataset from the predicted graph.
        producer : `int` or `None`, optional
            Internal ID of the quantum that was predicted to produce this
            dataset.
        consumers : `~collections.abc.Iterable` [`int`], optional
            Internal IDs of the quanta that were predicted to consume this
            dataset.

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

    def _add_to_graph(self, graph: ProvenanceQuantumGraph, address_reader: AddressReader) -> None:
        """Add this dataset and its edges to quanta to a provenance graph.

        Parameters
        ----------
        graph : `ProvenanceQuantumGraph`
            Graph to update in place.
        address_reader : `AddressReader`
            Reader object that can be used to look up UUIDs from integer
            indexes.

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
        producer_id: uuid.UUID | None = None
        if self.producer is not None:
            producer_id = address_reader.find(self.producer).key
            graph._bipartite_xgraph.add_edge(producer_id, self.dataset_id)
        for consumer_index in self.consumers:
            consumer_id = address_reader.find(consumer_index).key
            graph._bipartite_xgraph.add_edge(self.dataset_id, consumer_id)
            if producer_id is not None:
                graph._quantum_only_xgraph.add_edge(producer_id, consumer_id)
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


class ProvenanceQuantumModel(pydantic.BaseModel):
    """Data model for the quanta in a provenance quantum graph file."""

    quantum_id: uuid.UUID
    """Unique identifier for the quantum."""

    task_label: TaskLabel
    """Name of the type of this dataset.

    This is always a parent dataset type name, not a component.

    Note that full dataset type definitions are stored in the pipeline graph.
    """

    data_coordinate: DataCoordinateValues = pydantic.Field(default_factory=list)
    """The full values (required and implied) of this dataset's data ID."""

    status: QuantumRunStatus = QuantumRunStatus.METADATA_MISSING
    """Enumerated status for the quantum."""

    caveats: QuantumSuccessCaveats | None = None
    """Flags indicating caveats on successful quanta."""

    exception: ExceptionInfo | None = None
    """Information about an exception raised when the quantum was executing."""

    inputs: dict[ConnectionName, list[DatasetIndex]] = pydantic.Field(default_factory=dict)
    """Internal integer IDs of the datasets predicted to be consumed by this
    quantum, grouped by connection name.
    """

    outputs: dict[ConnectionName, list[DatasetIndex]] = pydantic.Field(default_factory=dict)
    """Internal integer IDs of the datasets predicted to be produced by this
    quantum, grouped by connection name.
    """

    resource_usage: QuantumResourceUsage | None = None
    """Resource usage information (timing, memory use) for this quantum."""

    @property
    def node_id(self) -> uuid.UUID:
        """Alias for the quantum ID."""
        return self.quantum_id

    @classmethod
    def from_predicted(
        cls, predicted: PredictedQuantumDatasetsModel, indices: Mapping[uuid.UUID, int]
    ) -> ProvenanceQuantumModel:
        """Construct from a predicted quantum model.

        Parameters
        ----------
        predicted : `PredictedQuantumDatasetsModel`
            Information about the quantum from the predicted graph.
        indices : `~collections.abc.Mapping [`uuid.UUID`, `int`]
            Mapping from quantum or dataset UUID to internal integer ID.

        Returns
        -------
        provenance : `ProvenanceQuantumModel`
            Provenance quantum model.
        """
        inputs = {
            connection_name: [indices[d.dataset_id] for d in predicted_inputs]
            for connection_name, predicted_inputs in predicted.inputs.items()
        }
        outputs = {
            connection_name: [indices[d.dataset_id] for d in predicted_outputs]
            for connection_name, predicted_outputs in predicted.outputs.items()
        }
        return cls(
            quantum_id=predicted.quantum_id,
            task_label=predicted.task_label,
            data_coordinate=predicted.data_coordinate,
            inputs=inputs,
            outputs=outputs,
        )

    def _add_to_graph(self, graph: ProvenanceQuantumGraph, address_reader: AddressReader) -> None:
        """Add this quantum and its edges to datasets to a provenance graph.

        Parameters
        ----------
        graph : `ProvenanceQuantumGraph`
            Graph to update in place.
        address_reader : `AddressReader`
            Reader object that can be used to look up UUIDs from integer
            indexes.

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
        graph._bipartite_xgraph.add_node(
            self.quantum_id,
            data_id=data_id,
            task_label=self.task_label,
            pipeline_node=task_node,
            status=self.status,
            caveats=self.caveats,
            exception=self.exception,
            resource_usage=self.resource_usage,
        )
        for connection_name, dataset_indices in self.inputs.items():
            read_edge = task_node.get_input_edge(connection_name)
            for dataset_index in dataset_indices:
                dataset_id = address_reader.find(dataset_index).key
                graph._bipartite_xgraph.add_edge(dataset_id, self.quantum_id, is_read=True)
                graph._bipartite_xgraph.edges[dataset_id, self.quantum_id].setdefault(
                    "pipeline_edges", []
                ).append(read_edge)
        for connection_name, dataset_indices in self.outputs.items():
            write_edge = task_node.get_output_edge(connection_name)
            for dataset_index in dataset_indices:
                dataset_id = address_reader.find(dataset_index).key
                graph._bipartite_xgraph.add_edge(
                    self.quantum_id,
                    dataset_id,
                    is_read=False,
                    # There can only be one pipeline edge for an output.
                    pipeline_edges=[write_edge],
                )
        graph._quanta_by_task_label[self.task_label][data_id] = self.quantum_id
        graph._quantum_only_xgraph.add_node(self.quantum_id, **graph._bipartite_xgraph.nodes[self.quantum_id])
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

    inputs: dict[ConnectionName, DatasetIndex] = pydantic.Field(default_factory=dict)
    """Internal integer IDs of the datasets predicted to be consumed by this
    quantum, grouped by connection name.
    """

    outputs: dict[ConnectionName, DatasetIndex] = pydantic.Field(default_factory=dict)
    """Internal integer IDs of the datasets predicted to be produced by this
    quantum, grouped by connection name.
    """

    @classmethod
    def from_predicted(
        cls, predicted: PredictedQuantumDatasetsModel, indices: Mapping[uuid.UUID, int]
    ) -> ProvenanceInitQuantumModel:
        """Construct from a predicted quantum model.

        Parameters
        ----------
        predicted : `PredictedQuantumDatasetsModel`
            Information about the quantum from the predicted graph.
        indices : `~collections.abc.Mapping [`uuid.UUID`, `int`]
            Mapping from quantum or dataset UUID to internal integer ID.

        Returns
        -------
        provenance : `ProvenanceInitQuantumModel`
            Provenance init quantum model.
        """
        inputs = {
            connection_name: indices[predicted_inputs[0].dataset_id]
            for connection_name, predicted_inputs in predicted.inputs.items()
        }
        outputs = {
            connection_name: indices[predicted_outputs[0].dataset_id]
            for connection_name, predicted_outputs in predicted.outputs.items()
        }
        return cls(
            quantum_id=predicted.quantum_id,
            task_label=predicted.task_label,
            inputs=inputs,
            outputs=outputs,
        )

    def _add_to_graph(
        self,
        graph: ProvenanceQuantumGraph,
        address_reader: AddressReader,
        empty_data_id: DataCoordinate,
    ) -> None:
        """Add this quantum and its edges to datasets to a provenance graph.

        Parameters
        ----------
        graph : `ProvenanceQuantumGraph`
            Graph to update in place.
        address_reader : `AddressReader`
            Reader object that can be used to look up UUIDs from integer
            indexes.
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
        for connection_name, dataset_index in self.inputs.items():
            read_edge = task_init_node.get_input_edge(connection_name)
            dataset_id = address_reader.find(dataset_index).key
            graph._bipartite_xgraph.add_edge(dataset_id, self.quantum_id, is_read=True)
            graph._bipartite_xgraph.edges[dataset_id, self.quantum_id].setdefault(
                "pipeline_edges", []
            ).append(read_edge)
        for connection_name, dataset_index in self.outputs.items():
            write_edge = task_init_node.get_output_edge(connection_name)
            dataset_id = address_reader.find(dataset_index).key
            graph._bipartite_xgraph.add_edge(
                self.quantum_id,
                dataset_id,
                is_read=False,
                # There can only be one pipeline edge for an output.
                pipeline_edges=[write_edge],
            )
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

    def _add_to_graph(self, graph: ProvenanceQuantumGraph, address_reader: AddressReader) -> None:
        """Add this quantum and its edges to datasets to a provenance graph.

        Parameters
        ----------
        graph : `ProvenanceQuantumGraph`
            Graph to update in place.
        address_reader : `AddressReader`
            Reader object that can be used to look up UUIDs from integer
            indexes.
        """
        empty_data_id = DataCoordinate.make_empty(graph.pipeline_graph.universe)
        for init_quantum in self.root:
            init_quantum._add_to_graph(graph, address_reader, empty_data_id=empty_data_id)

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
        Loading quanta necessary to populate edge attributes.

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
            status_counts = Counter[QuantumRunStatus](
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
                    "Unknown": status_counts.get(QuantumRunStatus.METADATA_MISSING, 0),
                    "Successful": status_counts.get(QuantumRunStatus.SUCCESSFUL, 0),
                    "Caveats": caveats,
                    "Blocked": status_counts.get(QuantumRunStatus.BLOCKED, 0),
                    "Failed": status_counts.get(QuantumRunStatus.FAILED, 0),
                    "TOTAL": len(quanta_for_task),
                    "EXPECTED": self.header.n_task_quanta[task_label],
                }
            )
        return astropy.table.Table(rows)

    def make_exception_table(self) -> astropy.table.Table:
        """Construct an `astropy.table.Table` with counts for each exception
        type raised by each task.

        At present this only includes information from partial-outputs-error
        successes, since exception information for failures is not tracked.
        This may change in the future.

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
    in place and return ``self``.
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

    def read_init_quanta(self) -> Self:
        """Read the thin graph, with all edge information and categorization of
        quanta by task label.

        Returns
        -------
        self : `ProvenanceQuantumGraphReader`
            The reader (to permit method-chaining).
        """
        init_quanta = self._read_single_block("init_quanta", ProvenanceInitQuantaModel)
        for init_quantum in init_quanta.root:
            self.graph._init_quanta[init_quantum.task_label] = init_quantum.quantum_id
        init_quanta._add_to_graph(self.graph, self.address_reader)
        return self

    def read_full_graph(self) -> Self:
        """Read all bipartite edges and all quantum and dataset node
        attributes, fully populating the `graph` attribute.

        Returns
        -------
        self : `ProvenanceQuantumGraphReader`
            The reader (to permit method-chaining).

        Notes
        -----
        This does not read logs, metadata, or packages ; those must always be
        fetched explicitly.
        """
        self.read_init_quanta()
        self.read_datasets()
        self.read_quanta()
        return self

    def read_datasets(self, datasets: Iterable[uuid.UUID | DatasetIndex] | None = None) -> Self:
        """Read information about the given datasets.

        Parameters
        ----------
        datasets : `~collections.abc.Iterable` [`uuid.UUID` or `int`], optional
            Iterable of dataset IDs or indices to load.  If not provided, all
            datasets will be loaded.  The UUIDs and indices of quanta will be
            ignored.

        Return
        -------
        self : `ProvenanceQuantumGraphReader`
            The reader (to permit method-chaining).
        """
        return self._read_nodes(datasets, DATASET_ADDRESS_INDEX, DATASET_MB_NAME, ProvenanceDatasetModel)

    def read_quanta(self, quanta: Iterable[uuid.UUID | QuantumIndex] | None = None) -> Self:
        """Read information about the given quanta.

        Parameters
        ----------
        quanta : `~collections.abc.Iterable` [`uuid.UUID` or `int`], optional
            Iterable of quantum IDs or indices to load.  If not provided, all
            quanta will be loaded.  The UUIDs and indices of datasets and
            special init quanta will be ignored.

        Return
        -------
        self : `ProvenanceQuantumGraphReader`
            The reader (to permit method-chaining).
        """
        return self._read_nodes(quanta, QUANTUM_ADDRESS_INDEX, QUANTUM_MB_NAME, ProvenanceQuantumModel)

    def _read_nodes(
        self,
        nodes: Iterable[uuid.UUID | int] | None,
        address_index: int,
        mb_name: str,
        model_type: type[ProvenanceDatasetModel] | type[ProvenanceQuantumModel],
    ) -> Self:
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
                node._add_to_graph(self.graph, self.address_reader)
        with MultiblockReader.open_in_zip(self.zf, mb_name, int_size=self.header.int_size) as mb_reader:
            for node_id_or_index in nodes:
                address_row = self.address_reader.find(node_id_or_index)
                if "pipeline_node" in self.graph._bipartite_xgraph.nodes.get(address_row.key, {}):
                    # Use the old node to reduce memory usage (since it might
                    # also have other outstanding reference holders).
                    continue
                node = mb_reader.read_model(
                    address_row.addresses[address_index], model_type, self.decompressor
                )
                if node is not None:
                    node._add_to_graph(self.graph, self.address_reader)
        return self

    def fetch_logs(
        self, nodes: Iterable[uuid.UUID | DatasetIndex | QuantumIndex]
    ) -> dict[uuid.UUID | DatasetIndex | QuantumIndex, ButlerLogRecords]:
        """Fetch log datasets.

        Parameters
        ----------
        nodes : `~collections.abc.Iterable` [ `uuid.UUID` ]
            UUIDs of the log datasets themselves or of the quanta they
            correspond to.

        Returns
        -------
        logs : `dict` [ `uuid.UUID`, `ButlerLogRecords`]
            Logs for the given IDs.
        """
        from lsst.daf.butler.logging import ButlerLogRecords

        result: dict[uuid.UUID | DatasetIndex | QuantumIndex, ButlerLogRecords] = {}
        with MultiblockReader.open_in_zip(self.zf, LOG_MB_NAME, int_size=self.header.int_size) as mb_reader:
            for node_id_or_index in nodes:
                address_row = self.address_reader.find(node_id_or_index)
                log_data = mb_reader.read_bytes(address_row.addresses[LOG_ADDRESS_INDEX])
                if log_data is not None:
                    log = ButlerLogRecords.from_raw(self.decompressor.decompress(log_data))
                    result[node_id_or_index] = log
        return result

    def fetch_metadata(
        self, nodes: Iterable[uuid.UUID | DatasetIndex | QuantumIndex]
    ) -> dict[uuid.UUID | DatasetIndex | QuantumIndex, TaskMetadata]:
        """Fetch metadata datasets.

        Parameters
        ----------
        nodes : `~collections.abc.Iterable` [ `uuid.UUID` ]
            UUIDs of the metadata datasets themselves or of the quanta they
            correspond to.

        Returns
        -------
        metadata : `dict` [ `uuid.UUID`, `TaskMetadata`]
            Metadata for the given IDs.
        """
        from .._task_metadata import TaskMetadata

        result: dict[uuid.UUID | DatasetIndex | QuantumIndex, TaskMetadata] = {}
        with MultiblockReader.open_in_zip(
            self.zf, METADATA_MB_NAME, int_size=self.header.int_size
        ) as mb_reader:
            for node_id_or_index in nodes:
                address_row = self.address_reader.find(node_id_or_index)
                metadata = mb_reader.read_model(
                    address_row.addresses[METADATA_ADDRESS_INDEX], TaskMetadata, self.decompressor
                )
                if metadata is not None:
                    result[node_id_or_index] = metadata
        return result

    def fetch_packages(self) -> Packages:
        """Fetch package version information."""
        data = self._read_single_block_raw("packages")
        return Packages.fromBytes(data, format="json")
