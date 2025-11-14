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
    "PredictedDatasetInfo",
    "PredictedDatasetModel",
    "PredictedInitQuantaModel",
    "PredictedQuantumDatasetsModel",
    "PredictedQuantumGraph",
    "PredictedQuantumGraphComponents",
    "PredictedQuantumGraphReader",
    "PredictedQuantumInfo",
    "PredictedThinGraphModel",
    "PredictedThinQuantumModel",
)

import dataclasses
import itertools
import logging
import sys
import uuid
import warnings
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from typing import TYPE_CHECKING, Any, TypeVar, cast

import networkx
import networkx.algorithms.bipartite
import pydantic
import zstandard

from lsst.daf.butler import (
    Config,
    DataCoordinate,
    DataIdValue,
    DatasetRef,
    DatasetType,
    DimensionDataAttacher,
    DimensionDataExtractor,
    DimensionGroup,
    DimensionRecordSetDeserializer,
    LimitedButler,
    Quantum,
    QuantumBackedButler,
    SerializableDimensionData,
)
from lsst.daf.butler._rubin import generate_uuidv7
from lsst.daf.butler.datastore.record_data import DatastoreRecordData, SerializedDatastoreRecordData
from lsst.daf.butler.registry import ConflictingDefinitionError
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils.packages import Packages

from .. import automatic_connection_constants as acc
from ..pipeline import TaskDef
from ..pipeline_graph import (
    PipelineGraph,
    TaskImportMode,
    TaskInitNode,
    TaskNode,
    compare_packages,
    log_config_mismatch,
)
from ._common import (
    FORMAT_VERSION,
    BaseQuantumGraph,
    BaseQuantumGraphReader,
    BaseQuantumGraphWriter,
    ConnectionName,
    DataCoordinateValues,
    DatasetInfo,
    DatasetTypeName,
    DatastoreName,
    HeaderModel,
    IncompleteQuantumGraphError,
    QuantumIndex,
    QuantumInfo,
    TaskLabel,
)
from ._multiblock import DEFAULT_PAGE_SIZE, AddressRow, MultiblockReader, MultiblockWriter

if TYPE_CHECKING:
    from ..config import PipelineTaskConfig
    from ..graph import QgraphSummary, QuantumGraph

_LOG = logging.getLogger(__name__)


_T = TypeVar("_T", bound=pydantic.BaseModel)


class _PredictedThinQuantumModelV0(pydantic.BaseModel):
    """Data model for a quantum data ID and internal integer ID in a predicted
    quantum graph.
    """

    quantum_index: QuantumIndex
    """Internal integer ID for this quantum."""

    data_coordinate: DataCoordinateValues = pydantic.Field(default_factory=list)
    """Full (required and implied) data coordinate values for this quantum."""


class PredictedThinQuantumModel(pydantic.BaseModel):
    """Data model for a quantum data ID and UUID in a predicted
    quantum graph.
    """

    quantum_id: uuid.UUID
    """Universally unique ID for this quantum."""

    data_coordinate: DataCoordinateValues = pydantic.Field(default_factory=list)
    """Full (required and implied) data coordinate values for this quantum."""

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


class _PredictedThinGraphModelV0(pydantic.BaseModel):
    """Data model for the predicted quantum graph component that maps each
    task label to the data IDs and internal integer IDs of its quanta.
    """

    quanta: dict[TaskLabel, list[_PredictedThinQuantumModelV0]] = pydantic.Field(default_factory=dict)
    """Minimal descriptions of all quanta, grouped by task label."""

    edges: list[tuple[QuantumIndex, QuantumIndex]] = pydantic.Field(default_factory=list)
    """Pairs of (predecessor, successor) internal integer quantum IDs."""

    def _upgraded(self, address_rows: Mapping[uuid.UUID, AddressRow]) -> PredictedThinGraphModel:
        """Convert to the v1+ model."""
        uuid_by_index = {v.index: k for k, v in address_rows.items()}
        return PredictedThinGraphModel(
            quanta={
                task_label: [
                    PredictedThinQuantumModel(
                        quantum_id=uuid_by_index[q.quantum_index], data_coordinate=q.data_coordinate
                    )
                    for q in quanta
                ]
                for task_label, quanta in self.quanta.items()
            },
            edges=[(uuid_by_index[index1], uuid_by_index[index2]) for index1, index2 in self.edges],
        )


class PredictedThinGraphModel(pydantic.BaseModel):
    """Data model for the predicted quantum graph component that maps each
    task label to the data IDs and UUIDs of its quanta.
    """

    quanta: dict[TaskLabel, list[PredictedThinQuantumModel]] = pydantic.Field(default_factory=dict)
    """Minimal descriptions of all quanta, grouped by task label."""

    edges: list[tuple[uuid.UUID, uuid.UUID]] = pydantic.Field(default_factory=list)
    """Pairs of (predecessor, successor) quantum IDs."""

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


class PredictedDatasetModel(pydantic.BaseModel):
    """Data model for the datasets in a predicted quantum graph file."""

    dataset_id: uuid.UUID
    """Universally unique ID for the dataset."""

    dataset_type_name: DatasetTypeName
    """Name of the type of this dataset.

    This is always a parent dataset type name, not a component.

    Note that full dataset type definitions are stored in the pipeline graph.
    """

    data_coordinate: DataCoordinateValues = pydantic.Field(default_factory=list)
    """The full values (required and implied) of this dataset's data ID."""

    run: str
    """This dataset's RUN collection name."""

    @classmethod
    def from_dataset_ref(cls, ref: DatasetRef) -> PredictedDatasetModel:
        """Construct from a butler `~lsst.daf.butler.DatasetRef`.

        Parameters
        ----------
        ref : `lsst.daf.butler.DatasetRef`
            Dataset reference.

        Returns
        -------
        model : `PredictedDatasetModel`
            Model for the dataset.
        """
        dataset_type_name, _ = DatasetType.splitDatasetTypeName(ref.datasetType.name)
        return cls.model_construct(
            dataset_id=ref.id,
            dataset_type_name=dataset_type_name,
            data_coordinate=list(ref.dataId.full_values),
            run=ref.run,
        )

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


class PredictedQuantumDatasetsModel(pydantic.BaseModel):
    """Data model for a description of a single predicted quantum that includes
    its inputs and outputs.
    """

    quantum_id: uuid.UUID
    """Universally unique ID for the quantum."""

    task_label: TaskLabel
    """Label of the task.

    Note that task label definitions are stored in the pipeline graph.
    """

    data_coordinate: DataCoordinateValues = pydantic.Field(default_factory=list)
    """The full values (required and implied) of this quantum's data ID."""

    inputs: dict[ConnectionName, list[PredictedDatasetModel]] = pydantic.Field(default_factory=dict)
    """The input datasets to this quantum, grouped by connection name."""

    outputs: dict[ConnectionName, list[PredictedDatasetModel]] = pydantic.Field(default_factory=dict)
    """The datasets output by this quantum, grouped by connection name."""

    datastore_records: dict[DatastoreName, SerializedDatastoreRecordData] = pydantic.Field(
        default_factory=dict
    )
    """Datastore records for inputs to this quantum that are already present in
    the data repository.
    """

    def iter_input_dataset_ids(self) -> Iterator[uuid.UUID]:
        """Return an iterator over the UUIDs of all datasets consumed by this
        quantum.

        Returns
        -------
        iter : `~collections.abc.Iterator` [ `uuid.UUID` ]
            Iterator over dataset IDs.
        """
        for datasets in self.inputs.values():
            for dataset in datasets:
                yield dataset.dataset_id

    def iter_output_dataset_ids(self) -> Iterator[uuid.UUID]:
        """Return an iterator over the UUIDs of all datasets produced by this
        quantum.

        Returns
        -------
        iter : `~collections.abc.Iterator` [ `uuid.UUID` ]
            Iterator over dataset IDs.
        """
        for datasets in self.outputs.values():
            for dataset in datasets:
                yield dataset.dataset_id

    def iter_dataset_ids(self) -> Iterator[uuid.UUID]:
        """Return an iterator over the UUIDs of all datasets referenced by this
        quantum.

        Returns
        -------
        iter : `~collections.abc.Iterator` [ `uuid.UUID` ]
            Iterator over dataset IDs.
        """
        yield from self.iter_input_dataset_ids()
        yield from self.iter_output_dataset_ids()

    def deserialize_datastore_records(self) -> dict[DatastoreName, DatastoreRecordData]:
        """Deserialize the mapping of datastore records."""
        return {
            datastore_name: DatastoreRecordData.from_simple(serialized_records)
            for datastore_name, serialized_records in self.datastore_records.items()
        }

    @classmethod
    def from_execution_quantum(
        cls, task_node: TaskNode, quantum: Quantum, quantum_id: uuid.UUID
    ) -> PredictedQuantumDatasetsModel:
        """Construct from an `lsst.daf.butler.Quantum` instance.

        Parameters
        ----------
        task_node : `.pipeline_graph.TaskNode`
            Task node from the pipeline graph.
        quantum : `lsst.daf.butler.quantum`
            Quantum object.
        quantum_id : `uuid.UUID`
            ID for this quantum.

        Returns
        -------
        model : `PredictedFullQuantumModel`
            Model for this quantum.
        """
        result: PredictedQuantumDatasetsModel = cls.model_construct(
            quantum_id=quantum_id,
            task_label=task_node.label,
            data_coordinate=list(cast(DataCoordinate, quantum.dataId).full_values),
        )
        for read_edge in task_node.iter_all_inputs():
            refs = sorted(quantum.inputs[read_edge.dataset_type_name], key=lambda ref: ref.dataId)
            result.inputs[read_edge.connection_name] = [
                PredictedDatasetModel.from_dataset_ref(ref) for ref in refs
            ]
        for write_edge in task_node.iter_all_outputs():
            refs = sorted(quantum.outputs[write_edge.dataset_type_name], key=lambda ref: ref.dataId)
            result.outputs[write_edge.connection_name] = [
                PredictedDatasetModel.from_dataset_ref(ref) for ref in refs
            ]
        result.datastore_records = {
            store_name: records.to_simple() for store_name, records in quantum.datastore_records.items()
        }
        return result

    @classmethod
    def from_old_quantum_graph_init(
        cls, task_init_node: TaskInitNode, old_quantum_graph: QuantumGraph
    ) -> PredictedQuantumDatasetsModel:
        """Construct from the init-input and init-output dataset types of a
        task in an old `QuantumGraph` instance.

        Parameters
        ----------
        task_init_node : `.pipeline_graph.TaskNode`
            Task init node from the pipeline graph.
        old_quantum_graph : `QuantumGraph`
            Quantum graph.

        Returns
        -------
        model : `PredictedFullQuantumModel`
            Model for this "init" quantum.
        """
        task_def = old_quantum_graph.findTaskDefByLabel(task_init_node.label)
        assert task_def is not None
        init_input_refs = {
            ref.datasetType.name: ref for ref in (old_quantum_graph.initInputRefs(task_def) or [])
        }
        init_output_refs = {
            ref.datasetType.name: ref for ref in (old_quantum_graph.initOutputRefs(task_def) or [])
        }
        init_input_ids = {ref.id for ref in init_input_refs.values()}
        result: PredictedQuantumDatasetsModel = cls.model_construct(
            quantum_id=generate_uuidv7(), task_label=task_init_node.label
        )
        for read_edge in task_init_node.iter_all_inputs():
            ref = init_input_refs[read_edge.dataset_type_name]
            result.inputs[read_edge.connection_name] = [PredictedDatasetModel.from_dataset_ref(ref)]
        for write_edge in task_init_node.iter_all_outputs():
            ref = init_output_refs[write_edge.dataset_type_name]
            result.outputs[write_edge.connection_name] = [PredictedDatasetModel.from_dataset_ref(ref)]
        datastore_records: dict[str, DatastoreRecordData] = {}
        for quantum in old_quantum_graph.get_task_quanta(task_init_node.label).values():
            for store_name, records in quantum.datastore_records.items():
                subset = records.subset(init_input_ids)
                if subset is not None:
                    datastore_records.setdefault(store_name, DatastoreRecordData()).update(subset)
            break  # All quanta have same init-inputs, so we only need one.
        result.datastore_records = {
            store_name: records.to_simple() for store_name, records in datastore_records.items()
        }
        return result

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


class PredictedInitQuantaModel(pydantic.RootModel):
    """Data model for the init-inputs and init-outputs of a predicted quantum
    graph.
    """

    root: list[PredictedQuantumDatasetsModel] = pydantic.Field(default_factory=list)
    """List of special "init" quanta: one for each task, and another for global
    init-outputs.
    """

    def update_from_old_quantum_graph(self, old_quantum_graph: QuantumGraph) -> None:
        """Update this model in-place by extracting from an old `QuantumGraph`
        instance.

        Parameters
        ----------
        old_quantum_graph : `QuantumGraph`
            Quantum graph.
        """
        global_init_quantum = PredictedQuantumDatasetsModel.model_construct(
            quantum_id=generate_uuidv7(), task_label=""
        )
        for ref in old_quantum_graph.globalInitOutputRefs():
            global_init_quantum.outputs[ref.datasetType.name] = [PredictedDatasetModel.from_dataset_ref(ref)]
        self.root.append(global_init_quantum)
        for task_node in old_quantum_graph.pipeline_graph.tasks.values():
            self.root.append(
                PredictedQuantumDatasetsModel.from_old_quantum_graph_init(task_node.init, old_quantum_graph)
            )

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


class PredictedQuantumInfo(QuantumInfo):
    """A typed dictionary that annotates the attributes of the NetworkX graph
    node data for a predicted quantum.

    Since NetworkX types are not generic over their node mapping type, this has
    to be used explicitly, e.g.::

        node_data: PredictedQuantumInfo = xgraph.nodes[quantum_id]

    where ``xgraph`` can be either `PredictedQuantumGraph.quantum_only_xgraph`
    or `PredictedQuantumGraph.bipartite_xgraph`.
    """

    quantum: Quantum
    """Quantum object that can be passed directly to an executor.

    This attribute is only present if
    `PredictedQuantumGraph.build_execution_quanta` has been run on this node's
    quantum ID already.
    """


class PredictedDatasetInfo(DatasetInfo):
    """A typed dictionary that annotates the attributes of the NetworkX graph
    node data for a dataset.

    Since NetworkX types are not generic over their node mapping type, this has
    to be used explicitly, e.g.::

        node_data: PredictedDatasetInfo = xgraph.nodes[dataset_ids]

    where ``xgraph`` is from the `PredictedQuantumGraph.bipartite_xgraph`
    property.
    """


class PredictedQuantumGraph(BaseQuantumGraph):
    """A directed acyclic graph that predicts a processing run and supports it
    during execution.

    Parameters
    ----------
    components : `PredictedQuantumGraphComponents`
        A struct of components used to construct the graph.

    Notes
    -----
    Iteration over a `PredictedQuantumGraph` yields loaded quantum IDs in
    deterministic topological order (but the tiebreaker is unspecified).  The
    `len` of a `PredictedQuantumGraph` is the number of loaded non-init quanta,
    i.e. the same as the number of quanta iterated over.
    """

    def __init__(self, components: PredictedQuantumGraphComponents):
        if not components.header.graph_type == "predicted":
            raise TypeError(f"Header is for a {components.header.graph_type!r} graph, not 'predicted'.")
        super().__init__(components.header, components.pipeline_graph)
        self._quantum_only_xgraph = networkx.DiGraph()
        self._bipartite_xgraph = networkx.DiGraph()
        self._quanta_by_task_label: dict[str, dict[DataCoordinate, uuid.UUID]] = {
            task_label: {} for task_label in self.pipeline_graph.tasks.keys()
        }
        self._datasets_by_type: dict[str, dict[DataCoordinate, uuid.UUID]] = {
            dataset_type_name: {} for dataset_type_name in self.pipeline_graph.dataset_types.keys()
        }
        self._datasets_by_type[self.pipeline_graph.packages_dataset_type.name] = {}
        self._dimension_data = components.dimension_data
        self._add_init_quanta(components.init_quanta)
        self._quantum_datasets: dict[uuid.UUID, PredictedQuantumDatasetsModel] = {}
        self._expanded_data_ids: dict[DataCoordinate, DataCoordinate] = {}
        self._add_thin_graph(components.thin_graph)
        for quantum_datasets in components.quantum_datasets.values():
            self._add_quantum_datasets(quantum_datasets)
        if not components.thin_graph.edges:
            # If we loaded the thin_graph, we've already populated this graph.
            self._quantum_only_xgraph.update(
                networkx.algorithms.bipartite.projected_graph(
                    networkx.DiGraph(self._bipartite_xgraph),
                    self._quantum_only_xgraph.nodes.keys(),
                )
            )
        if _LOG.isEnabledFor(logging.DEBUG):
            for quantum_id in self:
                _LOG.debug(
                    "%s: %s @ %s",
                    quantum_id,
                    self._quantum_only_xgraph.nodes[quantum_id]["task_label"],
                    self._quantum_only_xgraph.nodes[quantum_id]["data_id"].required,
                )

    def _add_init_quanta(self, component: PredictedInitQuantaModel) -> None:
        self._init_quanta = {q.task_label: q for q in component.root}
        empty_data_id = DataCoordinate.make_empty(self.pipeline_graph.universe)
        for quantum_datasets in self._init_quanta.values():
            for init_datasets in itertools.chain(
                quantum_datasets.inputs.values(), quantum_datasets.outputs.values()
            ):
                for init_dataset in init_datasets:
                    self._datasets_by_type[init_dataset.dataset_type_name][empty_data_id] = (
                        init_dataset.dataset_id
                    )
            _LOG.debug(
                "%s: %s @ init",
                quantum_datasets.quantum_id,
                quantum_datasets.task_label,
            )

    def _add_thin_graph(self, component: PredictedThinGraphModel) -> None:
        self._quantum_only_xgraph.add_edges_from(component.edges)
        for task_label, thin_quanta_for_task in component.quanta.items():
            for thin_quantum in thin_quanta_for_task:
                self._add_quantum(thin_quantum.quantum_id, task_label, thin_quantum.data_coordinate)

    def _add_quantum_datasets(self, quantum_datasets: PredictedQuantumDatasetsModel) -> None:
        self._quantum_datasets[quantum_datasets.quantum_id] = quantum_datasets
        self._add_quantum(
            quantum_datasets.quantum_id, quantum_datasets.task_label, quantum_datasets.data_coordinate
        )
        task_node = self.pipeline_graph.tasks[quantum_datasets.task_label]
        for connection_name, input_datasets in quantum_datasets.inputs.items():
            pipeline_edge = task_node.get_input_edge(connection_name)
            for input_dataset in input_datasets:
                self._add_dataset(input_dataset)
                self._bipartite_xgraph.add_edge(
                    input_dataset.dataset_id,
                    quantum_datasets.quantum_id,
                    key=connection_name,
                    is_read=True,
                )
                # There might be multiple input connections for the same
                # dataset type.
                self._bipartite_xgraph.edges[
                    input_dataset.dataset_id, quantum_datasets.quantum_id
                ].setdefault("pipeline_edges", []).append(pipeline_edge)
        for connection_name, output_datasets in quantum_datasets.outputs.items():
            pipeline_edges = [task_node.get_output_edge(connection_name)]
            for output_dataset in output_datasets:
                self._add_dataset(output_dataset)
                self._bipartite_xgraph.add_edge(
                    quantum_datasets.quantum_id,
                    output_dataset.dataset_id,
                    key=connection_name,
                    is_read=False,
                    pipeline_edges=pipeline_edges,
                )

    def _add_quantum(
        self, quantum_id: uuid.UUID, task_label: str, data_coordinate_values: Sequence[DataIdValue]
    ) -> None:
        task_node = self.pipeline_graph.tasks[task_label]
        self._quantum_only_xgraph.add_node(quantum_id, task_label=task_label, pipeline_node=task_node)
        self._bipartite_xgraph.add_node(quantum_id, task_label=task_label, pipeline_node=task_node)
        data_coordinate_values = tuple(data_coordinate_values)
        dimensions = self.pipeline_graph.tasks[task_label].dimensions
        data_id = DataCoordinate.from_full_values(dimensions, tuple(data_coordinate_values))
        self._quantum_only_xgraph.nodes[quantum_id].setdefault("data_id", data_id)
        self._bipartite_xgraph.nodes[quantum_id].setdefault("data_id", data_id)
        self._quanta_by_task_label[task_label][data_id] = quantum_id

    def _add_dataset(self, model: PredictedDatasetModel) -> None:
        dataset_type_node = self.pipeline_graph.dataset_types[model.dataset_type_name]
        data_id = DataCoordinate.from_full_values(dataset_type_node.dimensions, tuple(model.data_coordinate))
        self._bipartite_xgraph.add_node(
            model.dataset_id,
            dataset_type_name=dataset_type_node.name,
            pipeline_node=dataset_type_node,
            run=model.run,
        )
        self._bipartite_xgraph.nodes[model.dataset_id].setdefault("data_id", data_id)
        self._datasets_by_type[model.dataset_type_name][data_id] = model.dataset_id

    @classmethod
    def open(
        cls,
        uri: ResourcePathExpression,
        page_size: int = DEFAULT_PAGE_SIZE,
        import_mode: TaskImportMode = TaskImportMode.ASSUME_CONSISTENT_EDGES,
    ) -> AbstractContextManager[PredictedQuantumGraphReader]:
        """Open a quantum graph and return a reader to load from it.

        Parameters
        ----------
        uri : convertible to `lsst.resources.ResourcePath`
            URI to open.  Should have a ``.qg`` extension.
        page_size : `int`, optional
            Approximate number of bytes to read at once from address files.
            Note that this does not set a page size for *all* reads, but it
            does affect the smallest, most numerous reads.
        import_mode : `.pipeline_graph.TaskImportMode`, optional
            How to handle importing the task classes referenced in the pipeline
            graph.

        Returns
        -------
        reader : `contextlib.AbstractContextManager` [ \
                `PredictedQuantumGraphReader` ]
            A context manager that returns the reader when entered.
        """
        return PredictedQuantumGraphReader.open(uri, page_size=page_size, import_mode=import_mode)

    @classmethod
    def read_execution_quanta(
        cls,
        uri: ResourcePathExpression,
        quantum_ids: Iterable[uuid.UUID] | None = None,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> PredictedQuantumGraph:
        """Read one or more executable quanta from a quantum graph file.

        Parameters
        ----------
        uri : convertible to `lsst.resources.ResourcePath`
            URI to open.  Should have a ``.qg`` extension for new quantum graph
            files, or ``.qgraph`` for the old format.
        quantum_ids : `~collections.abc.Iterable` [ `uuid.UUID` ], optional
            Iterable of quantum IDs to load.  If not provided, all quanta will
            be loaded.  The UUIDs of special init quanta will be ignored.
        page_size : `int`, optional
            Approximate number of bytes to read at once from address files.
            Note that this does not set a page size for *all* reads, but it
            does affect the smallest, most numerous reads.

        Returns
        -------
        quantum_graph : `PredictedQuantumGraph` ]
            A quantum graph that can build execution quanta for all of the
            given IDs.
        """
        return PredictedQuantumGraphComponents.read_execution_quanta(
            uri,
            quantum_ids,
            page_size=page_size,
        ).assemble()

    @property
    def quanta_by_task(self) -> Mapping[str, Mapping[DataCoordinate, uuid.UUID]]:
        """A nested mapping of all quanta, keyed first by task name and then by
        data ID.

        Notes
        -----
        This is populated by the ``thin_graph`` component (all quanta are
        added) and the `quantum_datasets`` component (only loaded quanta are
        added).  All tasks in the pipeline graph are included, even if none of
        their quanta were loaded (i.e. nested mappings may be empty).

        The returned object may be an internal dictionary; as the type
        annotation indicates, it should not be modified in place.
        """
        return self._quanta_by_task_label

    @property
    def datasets_by_type(self) -> Mapping[str, Mapping[DataCoordinate, uuid.UUID]]:
        """A nested mapping of all datasets, keyed first by dataset type name
        and then by data ID.

        Notes
        -----
        This is populated only by the ``quantum_datasets`` and ``init_quanta``
        components, and only datasets referenced by loaded quanta are present.
        All dataset types in the pipeline graph are included, even if none of
        their datasets were loaded (i.e. nested mappings may be empty).

        The returned object may be an internal dictionary; as the type
        annotation indicates, it should not be modified in place.
        """
        return self._datasets_by_type

    @property
    def quantum_only_xgraph(self) -> networkx.DiGraph:
        """A directed acyclic graph with quanta as nodes and datasets elided.

        Notes
        -----
        Node keys are quantum UUIDs, and are populated by the ``thin_graph``
        component (all nodes and edges) and ``quantum_datasets`` component
        (only those that were loaded).

        Node state dictionaries are described by the
        `PredictedQuantumInfo` type.

        The returned object is a read-only view of an internal one.
        """
        return self._quantum_only_xgraph.copy(as_view=True)

    @property
    def bipartite_xgraph(self) -> networkx.MultiDiGraph:
        """A directed acyclic graph with quantum and dataset nodes.

        This graph never includes init-input and init-output datasets.

        Notes
        -----
        Node keys are quantum or dataset UUIDs.  Nodes for quanta are present
        if the ``thin_graph`` component is loaded (all nodes) or if the
        ``quantum_datasets`` component is loaded (just loaded quanta). Edges
        and dataset nodes are only present for quanta whose
        ``quantum_datasets`` were loaded.

        Node state dictionaries are described by the
        `PredictedQuantumInfo` and `PredictedDatasetInfo` types.

        The returned object is a read-only view of an internal one.
        """
        return self._bipartite_xgraph.copy(as_view=True)

    @property
    def dimension_data(self) -> DimensionDataAttacher | None:
        """All dimension records needed to expand the data IDS in the graph.

        This may be `None` if the dimension data was not loaded.  If all
        execution quanta have been built, all records are guaranteed to have
        been deserialized and the ``records`` attribute is complete.  In other
        cases some records may still only be present in the ``deserializers``
        attribute.
        """
        return self._dimension_data

    def __iter__(self) -> Iterator[uuid.UUID]:
        for quanta_for_task in self.quanta_by_task.values():
            for data_id in sorted(quanta_for_task.keys()):
                yield quanta_for_task[data_id]

    def __len__(self) -> int:
        return len(self._quantum_only_xgraph)

    def get_init_inputs(self, task_label: str) -> dict[ConnectionName, DatasetRef]:
        """Return the init-input datasets for the given task.

        Parameters
        ----------
        task_label : `str`
            Label of the task.

        Returns
        -------
        init_inputs : `dict` [ `str`, `lsst.daf.butler.DatasetRef` ]
            Dataset references for init-input datasets, keyed by connection
            name.  Dataset types storage classes match the task connection
            declarations, not necessarily the data repository, and may be
            components.
        """
        if self._init_quanta is None:
            raise IncompleteQuantumGraphError("The init_quanta component was not loaded.")
        task_init_node = self.pipeline_graph.tasks[task_label].init
        return {
            connection_name: task_init_node.inputs[connection_name].adapt_dataset_ref(
                self._make_init_ref(datasets[0])
            )
            for connection_name, datasets in self._init_quanta[task_label].inputs.items()
        }

    def get_init_outputs(self, task_label: str) -> dict[ConnectionName, DatasetRef]:
        """Return the init-output datasets for the given task.

        Parameters
        ----------
        task_label : `str`
            Label of the task.  ``""`` may be used to get global init-outputs.

        Returns
        -------
        init_outputs : `dict` [ `str`, `lsst.daf.butler.DatasetRef` ]
            Dataset references for init-outputs datasets, keyed by connection
            name.  Dataset types storage classes match the task connection
            declarations, not necessarily the data repository.
        """
        if self._init_quanta is None:
            raise IncompleteQuantumGraphError("The init_quanta component was not loaded.")
        if not task_label:
            (datasets,) = self._init_quanta[""].outputs.values()
            return {
                acc.PACKAGES_INIT_OUTPUT_NAME: DatasetRef(
                    self.pipeline_graph.packages_dataset_type,
                    DataCoordinate.make_empty(self.pipeline_graph.universe),
                    run=datasets[0].run,
                    id=datasets[0].dataset_id,
                    conform=False,
                )
            }
        task_init_node = self.pipeline_graph.tasks[task_label].init
        result: dict[ConnectionName, DatasetRef] = {}
        for connection_name, datasets in self._init_quanta[task_label].outputs.items():
            if connection_name == acc.CONFIG_INIT_OUTPUT_CONNECTION_NAME:
                edge = task_init_node.config_output
            else:
                edge = task_init_node.outputs[connection_name]
            result[connection_name] = edge.adapt_dataset_ref(self._make_init_ref(datasets[0]))
        return result

    def _make_init_ref(self, dataset: PredictedDatasetModel) -> DatasetRef:
        dataset_type = self.pipeline_graph.dataset_types[dataset.dataset_type_name].dataset_type
        return DatasetRef(
            dataset_type,
            DataCoordinate.make_empty(self.pipeline_graph.universe),
            run=dataset.run,
            id=dataset.dataset_id,
            conform=False,
        )

    def build_execution_quanta(
        self,
        quantum_ids: Iterable[uuid.UUID] | None = None,
        task_label: str | None = None,
    ) -> dict[uuid.UUID, Quantum]:
        """Build `lsst.daf.butler.Quantum` objects suitable for executing
        tasks.

        In addition to returning the quantum objects directly, this also causes
        the `quantum_only_xgraph` and `bipartite_xgraph` graphs to include a
        ``quantum`` attribute for the affected quanta.

        Parameters
        ----------
        quantum_ids : `~collections.abc.Iterable` [ `uuid.UUID` ], optional
            IDs of all quanta to return.  If not provided, all quanta for the
            given task label (if given) or graph are returned.
        task_label : `str`, optional
            Task label whose quanta should be generated.  Ignored if
            ``quantum_ids`` is not `None`.

        Returns
        -------
        quanta : `dict` [ `uuid.UUID`, `lsst.daf.butler.Quantum` ]
            Mapping of quanta, keyed by UUID.  All dataset types are adapted to
            the task's storage class declarations and inputs may be components.
            All data IDs have dimension records attached.
        """
        if not self._init_quanta:
            raise IncompleteQuantumGraphError(
                "Cannot build execution quanta without loading the ``init_quanta`` component."
            )
        if quantum_ids is None:
            if task_label is not None:
                quantum_ids = self._quanta_by_task_label[task_label].values()
            else:
                quantum_ids = self._quantum_only_xgraph.nodes.keys()
        else:
            # Guard against single-pass iterators.
            quantum_ids = list(quantum_ids)
        del task_label  # make sure we don't accidentally use this.
        result: dict[uuid.UUID, Quantum] = {}
        self._expand_execution_quantum_data_ids(quantum_ids)
        task_init_datastore_records: dict[TaskLabel, dict[DatastoreName, DatastoreRecordData]] = {}
        for quantum_id in quantum_ids:
            quantum_node_dict: PredictedQuantumInfo = self._quantum_only_xgraph.nodes[quantum_id]
            if "quantum" in quantum_node_dict:
                result[quantum_id] = quantum_node_dict["quantum"]
                continue
            # We've declare the info dict keys to all be required because that
            # saves a lot of casting, but the reality is that they can either
            # be fully populated or totally unpopulated.  But that makes mypy
            # think the check above always succeeds.
            try:  # type:ignore [unreachable]
                quantum_datasets = self._quantum_datasets[quantum_id]
            except KeyError:
                raise IncompleteQuantumGraphError(
                    f"Full quantum information for {quantum_id} was not loaded."
                ) from None
            task_node = self.pipeline_graph.tasks[quantum_datasets.task_label]
            quantum_data_id = self._expanded_data_ids[self._bipartite_xgraph.nodes[quantum_id]["data_id"]]
            inputs = self._build_execution_quantum_refs(task_node, quantum_datasets.inputs)
            outputs = self._build_execution_quantum_refs(task_node, quantum_datasets.outputs)
            if task_node.label not in task_init_datastore_records:
                task_init_datastore_records[task_node.label] = self._init_quanta[
                    task_node.label
                ].deserialize_datastore_records()
            quantum = Quantum(
                taskName=task_node.task_class_name,
                taskClass=task_node.task_class,
                dataId=quantum_data_id,
                initInputs={
                    ref.datasetType: ref for ref in self.get_init_inputs(quantum_datasets.task_label).values()
                },
                inputs=inputs,
                outputs=outputs,
                datastore_records=DatastoreRecordData.merge_mappings(
                    quantum_datasets.deserialize_datastore_records(),
                    task_init_datastore_records[task_node.label],
                ),
            )
            self._quantum_only_xgraph.nodes[quantum_id]["quantum"] = quantum
            self._bipartite_xgraph.nodes[quantum_id]["quantum"] = quantum
            result[quantum_id] = quantum
        return result

    def _expand_execution_quantum_data_ids(self, quantum_ids: Iterable[uuid.UUID]) -> None:
        if self._dimension_data is None:
            raise IncompleteQuantumGraphError(
                "Cannot build execution quanta without loading the ``dimension_data`` component."
            )
        data_ids_to_expand: dict[DimensionGroup, set[DataCoordinate]] = defaultdict(set)
        for quantum_id in quantum_ids:
            data_id: DataCoordinate = self._bipartite_xgraph.nodes[quantum_id]["data_id"]
            if data_id.hasRecords():
                self._expanded_data_ids[data_id] = data_id
            else:
                data_ids_to_expand[data_id.dimensions].add(data_id)
            for dataset_id in itertools.chain(
                self._bipartite_xgraph.predecessors(quantum_id),
                self._bipartite_xgraph.successors(quantum_id),
            ):
                data_id = self._bipartite_xgraph.nodes[dataset_id]["data_id"]
                if data_id.hasRecords():
                    self._expanded_data_ids[data_id] = data_id
                else:
                    data_ids_to_expand[data_id.dimensions].add(data_id)
        for dimensions, data_ids_for_dimensions in data_ids_to_expand.items():
            self._expanded_data_ids.update(
                (d, d) for d in self._dimension_data.attach(dimensions, data_ids_for_dimensions)
            )

    def _build_execution_quantum_refs(
        self, task_node: TaskNode, model_mapping: dict[ConnectionName, list[PredictedDatasetModel]]
    ) -> dict[DatasetType, list[DatasetRef]]:
        results: dict[DatasetType, list[DatasetRef]] = {}
        for connection_name, datasets in model_mapping.items():
            edge = task_node.get_edge(connection_name)
            dataset_type = edge.adapt_dataset_type(
                self.pipeline_graph.dataset_types[edge.parent_dataset_type_name].dataset_type
            )
            results[dataset_type] = [self._make_general_ref(dataset_type, d.dataset_id) for d in datasets]
        return results

    def _make_general_ref(self, dataset_type: DatasetType, dataset_id: uuid.UUID) -> DatasetRef:
        node_state = self._bipartite_xgraph.nodes[dataset_id]
        data_id = self._expanded_data_ids[node_state["data_id"]]
        return DatasetRef(dataset_type, data_id, run=node_state["run"], id=dataset_id)

    def make_init_qbb(
        self,
        butler_config: Config | ResourcePathExpression,
        *,
        config_search_paths: Iterable[str] | None = None,
    ) -> QuantumBackedButler:
        """Construct an quantum-backed butler suitable for reading and writing
        init input and init output datasets, respectively.

        This only requires the ``init_quanta`` component to have been loaded.

        Parameters
        ----------
        butler_config : `~lsst.daf.butler.Config` or \
                `~lsst.resources.ResourcePathExpression`
            A butler repository root, configuration filename, or configuration
            instance.
        config_search_paths : `~collections.abc.Iterable` [ `str` ], optional
            Additional search paths for butler configuration.

        Returns
        -------
        qbb : `~lsst.daf.butler.QuantumBackedButler`
            A limited butler that can ``get`` init-input datasets and ``put``
            init-output datasets.
        """
        # Collect all init input/output dataset IDs.
        predicted_inputs: set[uuid.UUID] = set()
        predicted_outputs: set[uuid.UUID] = set()
        datastore_record_maps: list[dict[DatastoreName, DatastoreRecordData]] = []
        for init_quantum_datasets in self._init_quanta.values():
            predicted_inputs.update(
                d.dataset_id for d in itertools.chain.from_iterable(init_quantum_datasets.inputs.values())
            )
            predicted_outputs.update(
                d.dataset_id for d in itertools.chain.from_iterable(init_quantum_datasets.outputs.values())
            )
            datastore_record_maps.append(
                {
                    datastore_name: DatastoreRecordData.from_simple(serialized_records)
                    for datastore_name, serialized_records in init_quantum_datasets.datastore_records.items()
                }
            )
        # Remove intermediates from inputs.
        predicted_inputs -= predicted_outputs
        dataset_types = {d.name: d.dataset_type for d in self.pipeline_graph.dataset_types.values()}
        # Make butler from everything.
        return QuantumBackedButler.from_predicted(
            config=butler_config,
            predicted_inputs=predicted_inputs,
            predicted_outputs=predicted_outputs,
            dimensions=self.pipeline_graph.universe,
            datastore_records=DatastoreRecordData.merge_mappings(*datastore_record_maps),
            search_paths=list(config_search_paths) if config_search_paths is not None else None,
            dataset_types=dataset_types,
        )

    def write_init_outputs(self, butler: LimitedButler, skip_existing: bool = True) -> None:
        """Write the init-output datasets for all tasks in the quantum graph.

        This only requires the ``init_quanta`` component to have been loaded.

        Parameters
        ----------
        butler : `lsst.daf.butler.LimitedButler`
            A limited butler data repository client.
        skip_existing : `bool`, optional
            If `True` (default) ignore init-outputs that already exist.  If
            `False`, raise.

        Raises
        ------
        lsst.daf.butler.registry.ConflictingDefinitionError
            Raised if an init-output dataset already exists and
            ``skip_existing=False``.
        """
        # Extract init-input and init-output refs from the QG.
        input_refs: dict[str, DatasetRef] = {}
        output_refs: dict[str, DatasetRef] = {}
        for task_node in self.pipeline_graph.tasks.values():
            if task_node.label not in self._init_quanta:
                continue
            input_refs.update(
                {ref.datasetType.name: ref for ref in self.get_init_inputs(task_node.label).values()}
            )
            output_refs.update(
                {
                    ref.datasetType.name: ref
                    for ref in self.get_init_outputs(task_node.label).values()
                    if ref.datasetType.name != task_node.init.config_output.dataset_type_name
                }
            )
        for ref, is_stored in butler.stored_many(output_refs.values()).items():
            if is_stored:
                if not skip_existing:
                    raise ConflictingDefinitionError(f"Init-output dataset {ref} already exists.")
                # We'll `put` whatever's left in output_refs at the end.
                del output_refs[ref.datasetType.name]
        # Instantiate tasks, reading overall init-inputs and gathering
        # init-output in-memory objects.
        init_outputs: list[tuple[Any, DatasetType]] = []
        self.pipeline_graph.instantiate_tasks(
            get_init_input=lambda dataset_type: butler.get(
                input_refs[dataset_type.name].overrideStorageClass(dataset_type.storageClass)
            ),
            init_outputs=init_outputs,
            # A task can be in the pipeline graph without having an init
            # quantum if it doesn't have any regular quanta either (e.g. they
            # were all skipped), and the _init_quanta has a "" entry for global
            # init-outputs that we don't want to pass here.
            labels=self.pipeline_graph.tasks.keys() & self._init_quanta.keys(),
        )
        # Write init-outputs that weren't already present.
        for obj, dataset_type in init_outputs:
            if new_ref := output_refs.get(dataset_type.name):
                assert new_ref.datasetType.storageClass_name == dataset_type.storageClass_name, (
                    "QG init refs should use task connection storage classes."
                )
                butler.put(obj, new_ref)

    def write_configs(self, butler: LimitedButler, compare_existing: bool = True) -> None:
        """Write the config datasets for all tasks in the quantum graph.

        Parameters
        ----------
        butler : `lsst.daf.butler.LimitedButler`
            A limited butler data repository client.
        compare_existing : `bool`, optional
            If `True` check configs that already exist for consistency.  If
            `False`, always raise if configs already exist.

        Raises
        ------
        lsst.daf.butler.registry.ConflictingDefinitionError
            Raised if an config dataset already exists and
            ``compare_existing=False``, or if the existing config is not
            consistent with the config in the quantum graph.
        """
        to_put: list[tuple[PipelineTaskConfig, DatasetRef]] = []
        for task_node in self.pipeline_graph.tasks.values():
            if task_node.label not in self._init_quanta:
                continue
            dataset_type_name = task_node.init.config_output.dataset_type_name
            ref = self.get_init_outputs(task_node.label)[acc.CONFIG_INIT_OUTPUT_CONNECTION_NAME]
            try:
                old_config = butler.get(ref)
            except (LookupError, FileNotFoundError):
                old_config = None
            if old_config is not None:
                if not compare_existing:
                    raise ConflictingDefinitionError(f"Config dataset {ref} already exists.")
                if not task_node.config.compare(old_config, shortcut=False, output=log_config_mismatch):
                    raise ConflictingDefinitionError(
                        f"Config does not match existing task config {dataset_type_name!r} in "
                        "butler; tasks configurations must be consistent within the same run collection."
                    )
            else:
                to_put.append((task_node.config, ref))
        # We do writes at the end to minimize the mess we leave behind when we
        # raise an exception.
        for config, ref in to_put:
            butler.put(config, ref)

    def write_packages(self, butler: LimitedButler, compare_existing: bool = True) -> None:
        """Write the 'packages' dataset for the currently-active software
        versions.

        Parameters
        ----------
        butler : `lsst.daf.butler.LimitedButler`
            A limited butler data repository client.
        compare_existing : `bool`, optional
            If `True` check packages that already exist for consistency.  If
            `False`, always raise if the packages dataset already exists.

        Raises
        ------
        lsst.daf.butler.registry.ConflictingDefinitionError
            Raised if the packages dataset already exists and is not consistent
            with the current packages.
        """
        new_packages = Packages.fromSystem()
        (ref,) = self.get_init_outputs("").values()
        try:
            packages = butler.get(ref)
        except (LookupError, FileNotFoundError):
            packages = None
        if packages is not None:
            if not compare_existing:
                raise ConflictingDefinitionError(f"Packages dataset {ref} already exists.")
            if compare_packages(packages, new_packages):
                # have to remove existing dataset first; butler has no
                # replace option.
                butler.pruneDatasets([ref], unstore=True, purge=True)
                butler.put(packages, ref)
        else:
            butler.put(new_packages, ref)

    def init_output_run(self, butler: LimitedButler, existing: bool = True) -> None:
        """Initialize a new output RUN collection by writing init-output
        datasets (including configs and packages).

        Parameters
        ----------
        butler : `lsst.daf.butler.LimitedButler`
            A limited butler data repository client.
        existing : `bool`, optional
            If `True` check or ignore outputs that already exist.  If
            `False`, always raise if an output dataset already exists.

        Raises
        ------
        lsst.daf.butler.registry.ConflictingDefinitionError
            Raised if there are existing init output datasets, and either
            ``existing=False`` or their contents are not compatible with this
            graph.
        """
        self.write_configs(butler, compare_existing=existing)
        self.write_packages(butler, compare_existing=existing)
        self.write_init_outputs(butler, skip_existing=existing)

    @classmethod
    def from_old_quantum_graph(cls, old_quantum_graph: QuantumGraph) -> PredictedQuantumGraph:
        """Construct from an old `QuantumGraph` instance.

        Parameters
        ----------
        old_quantum_graph : `QuantumGraph`
            Quantum graph to transform.

        Returns
        -------
        predicted_quantum_graph : `PredictedQuantumGraph`
            A new predicted quantum graph.
        """
        return PredictedQuantumGraphComponents.from_old_quantum_graph(old_quantum_graph).assemble()

    def to_old_quantum_graph(self) -> QuantumGraph:
        """Transform into an old `QuantumGraph` instance.

        Returns
        -------
        old_quantum_graph : `QuantumGraph`
            Old quantum graph.

        Notes
        -----
        This can only be called on graphs that have loaded all quantum
        datasets, init datasets, and dimension records.
        """
        from ..graph import QuantumGraph

        quanta: dict[TaskDef, set[Quantum]] = {}
        quantum_to_quantum_id: dict[Quantum, uuid.UUID] = {}
        init_inputs: dict[TaskDef, list[DatasetRef]] = {}
        init_outputs: dict[TaskDef, list[DatasetRef]] = {}
        for task_def in self.pipeline_graph._iter_task_defs():
            if not self._quanta_by_task_label.get(task_def.label):
                continue
            quanta_for_task: set[Quantum] = set()
            for quantum_id, quantum in self.build_execution_quanta(task_label=task_def.label).items():
                quanta_for_task.add(quantum)
                quantum_to_quantum_id[quantum] = quantum_id
            quanta[task_def] = quanta_for_task
            init_inputs[task_def] = list(self.get_init_inputs(task_def.label).values())
            init_outputs[task_def] = list(self.get_init_outputs(task_def.label).values())
        global_init_outputs = list(self.get_init_outputs("").values())
        registry_dataset_types = [d.dataset_type for d in self.pipeline_graph.dataset_types.values()]
        result = object.__new__(QuantumGraph)
        result._buildGraphs(
            quanta,
            _quantumToNodeId=quantum_to_quantum_id,
            metadata=self.header.to_old_metadata(),
            universe=self.pipeline_graph.universe,
            initInputs=init_inputs,
            initOutputs=init_outputs,
            globalInitOutputs=global_init_outputs,
            registryDatasetTypes=registry_dataset_types,
        )
        return result

    def _make_summary(self) -> QgraphSummary:
        from ..graph import QgraphSummary, QgraphTaskSummary

        summary = QgraphSummary(
            cmdLine=self.header.command or None,
            creationUTC=str(self.header.timestamp) if self.header.timestamp is not None else None,
            inputCollection=self.header.inputs or None,
            outputCollection=self.header.output,
            outputRun=self.header.output_run,
        )
        for task_label, quanta_for_task in self.quanta_by_task.items():
            task_summary = QgraphTaskSummary(taskLabel=task_label, numQuanta=len(quanta_for_task))
            task_node = self.pipeline_graph.tasks[task_label]
            for quantum_id in quanta_for_task.values():
                quantum_datasets = self._quantum_datasets[quantum_id]
                for connection_name, input_datasets in quantum_datasets.inputs.items():
                    task_summary.numInputs[
                        task_node.get_input_edge(connection_name).parent_dataset_type_name
                    ] += len(input_datasets)
                for connection_name, output_datasets in quantum_datasets.outputs.items():
                    task_summary.numOutputs[
                        task_node.get_output_edge(connection_name).parent_dataset_type_name
                    ] += len(output_datasets)
            summary.qgraphTaskSummaries[task_label] = task_summary
        return summary


@dataclasses.dataclass(kw_only=True)
class PredictedQuantumGraphComponents:
    """A helper class for building and writing predicted quantum graphs.

    Notes
    -----
    This class is a simple struct of model classes to allow different tools
    that build predicted quantum graphs to assemble them in whatever order they
    prefer.  It does not enforce any internal invariants (e.g. the quantum and
    dataset counts in the header, different representations of quanta, internal
    ID sorting, etc.), but it does provide methods that can satisfy them.
    """

    def __post_init__(self) -> None:
        self.header.graph_type = "predicted"

    header: HeaderModel = dataclasses.field(default_factory=HeaderModel)
    """Basic metadata about the graph."""

    pipeline_graph: PipelineGraph
    """Description of the pipeline this graph runs, including all task label
    and dataset type definitions.

    This may include tasks that do not have any quanta (e.g. due to skipping
    already-executed tasks).

    This also includes the dimension universe used to construct the graph.
    """

    dimension_data: DimensionDataAttacher | None = None
    """Object that can attach dimension records to data IDs.
    """

    init_quanta: PredictedInitQuantaModel = dataclasses.field(default_factory=PredictedInitQuantaModel)
    """A list of special quanta that describe the init-inputs and init-outputs
    of the graph.

    Tasks that are included in the pipeline graph but do not have any quanta
    may or may not have an init quantum, but tasks that do have regular quanta
    always have an init quantum as well.

    When used to construct a `PredictedQuantumGraph`, this must have either
    zero entries or all tasks in the pipeline.
    """

    thin_graph: PredictedThinGraphModel = dataclasses.field(default_factory=PredictedThinGraphModel)
    """A lightweight quantum-quantum DAG with task labels and data IDs only.

    This does not include the special "init" quanta.
    """

    quantum_datasets: dict[uuid.UUID, PredictedQuantumDatasetsModel] = dataclasses.field(default_factory=dict)
    """The full descriptions of all quanta, including input and output
    dataset, keyed by UUID.

    When used to construct a `PredictedQuantumGraph`, this need not have all
    entries.

    This does not include special "init" quanta.
    """

    def make_dataset_ref(self, predicted: PredictedDatasetModel) -> DatasetRef:
        """Make a `lsst.daf.butler.DatasetRef` from information in the
        predicted quantum graph.

        Parameters
        ----------
        predicted : `PredictedDatasetModel`
            Model for the dataset in the predicted graph.

        Returns
        -------
        ref : `lsst.daf.butler.DatasetRef`
            A dataset reference.  Data ID will be expanded if and only if
            the dimension data has been loaded.
        """
        try:
            dataset_type = self.pipeline_graph.dataset_types[predicted.dataset_type_name].dataset_type
        except KeyError:
            if predicted.dataset_type_name == acc.PACKAGES_INIT_OUTPUT_NAME:
                dataset_type = self.pipeline_graph.packages_dataset_type
            else:
                raise
        data_id = DataCoordinate.from_full_values(dataset_type.dimensions, tuple(predicted.data_coordinate))
        if self.dimension_data is not None:
            (data_id,) = self.dimension_data.attach(dataset_type.dimensions, [data_id])
        return DatasetRef(
            dataset_type,
            data_id,
            run=predicted.run,
            id=predicted.dataset_id,
        )

    def set_thin_graph(self) -> None:
        """Populate the `thin_graph` component from the `pipeline_graph`,
        `quantum_datasets` components (which must be complete).
        """
        bipartite_xgraph = networkx.DiGraph()
        self.thin_graph.quanta = {task_label: [] for task_label in self.pipeline_graph.tasks}
        graph_quantum_ids: list[uuid.UUID] = []
        for quantum_datasets in self.quantum_datasets.values():
            self.thin_graph.quanta[quantum_datasets.task_label].append(
                PredictedThinQuantumModel.model_construct(
                    quantum_id=quantum_datasets.quantum_id,
                    data_coordinate=quantum_datasets.data_coordinate,
                )
            )
            for dataset in itertools.chain.from_iterable(quantum_datasets.inputs.values()):
                bipartite_xgraph.add_edge(dataset.dataset_id, quantum_datasets.quantum_id)
            for dataset in itertools.chain.from_iterable(quantum_datasets.outputs.values()):
                bipartite_xgraph.add_edge(quantum_datasets.quantum_id, dataset.dataset_id)
            graph_quantum_ids.append(quantum_datasets.quantum_id)
        quantum_only_xgraph: networkx.DiGraph = networkx.bipartite.projected_graph(
            bipartite_xgraph, graph_quantum_ids
        )
        self.thin_graph.edges = list(quantum_only_xgraph.edges)

    def set_header_counts(self) -> None:
        """Populate the quantum and dataset counts in the header from the
        `thin_graph`, `init_quanta`, and `quantum_datasets` components.
        """
        self.header.n_quanta = len(self.quantum_datasets)
        self.header.n_task_quanta = {
            task_label: len(thin_quanta) for task_label, thin_quanta in self.thin_graph.quanta.items()
        }
        all_dataset_ids: set[uuid.UUID] = set()
        for quantum_datasets in itertools.chain(self.init_quanta.root, self.quantum_datasets.values()):
            all_dataset_ids.update(quantum_datasets.iter_dataset_ids())
        self.header.n_datasets = len(all_dataset_ids)

    def update_output_run(self, output_run: str) -> None:
        """Update the output `~lsst.daf.butler.CollectionType.RUN` collection
        name in all datasets and regenerate all output dataset and quantum
        UUIDs.

        Parameters
        ----------
        output_run : `str`
            New output `~lsst.daf.butler.CollectionType.RUN` collection name.
        """
        uuid_map: dict[uuid.UUID, uuid.UUID] = {}
        # Do all outputs and then all inputs in separate passes so we don't
        # need to rely on topological ordering of anything.
        for quantum_datasets in itertools.chain(self.init_quanta.root, self.quantum_datasets.values()):
            new_quantum_id = generate_uuidv7()
            quantum_datasets.quantum_id = new_quantum_id
            for output_dataset in itertools.chain.from_iterable(quantum_datasets.outputs.values()):
                assert output_dataset.run == self.header.output_run, (
                    f"Incorrect run {output_dataset.run} for output dataset {output_dataset.dataset_id}."
                )
                new_dataset_id = generate_uuidv7()
                uuid_map[output_dataset.dataset_id] = new_dataset_id
                output_dataset.dataset_id = new_dataset_id
                output_dataset.run = output_run
        for quantum_datasets in itertools.chain(self.init_quanta.root, self.quantum_datasets.values()):
            for input_dataset in itertools.chain.from_iterable(quantum_datasets.inputs.values()):
                if input_dataset.run == self.header.output_run:
                    input_dataset.run = output_run
                    input_dataset.dataset_id = uuid_map.get(
                        input_dataset.dataset_id,
                        # This dataset isn't necessary an output of the graph
                        # just because it's in the output run; the graph could
                        # have been built with extend_run=True.
                        input_dataset.dataset_id,
                    )
        # Update the keys of the quantum_datasets dict.
        self.quantum_datasets = {qd.quantum_id: qd for qd in self.quantum_datasets.values()}
        # Since the UUIDs have changed, the thin graph needs to be rewritten.
        self.set_thin_graph()
        # Update the header last, since we use it above to get the old run.
        self.header.output_run = output_run

    def assemble(self) -> PredictedQuantumGraph:
        """Construct a `PredictedQuantumGraph` from these components."""
        return PredictedQuantumGraph(self)

    @classmethod
    def read_execution_quanta(
        cls,
        uri: ResourcePathExpression,
        quantum_ids: Iterable[uuid.UUID] | None = None,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> PredictedQuantumGraphComponents:
        """Read one or more executable quanta from a quantum graph file.

        Parameters
        ----------
        uri : convertible to `lsst.resources.ResourcePath`
            URI to open.  Should have a ``.qg`` extension for new quantum graph
            files, or ``.qgraph`` for the old format.
        quantum_ids : `~collections.abc.Iterable` [ `uuid.UUID` ], optional
            Iterable of quantum IDs to load.  If not provided, all quanta will
            be loaded.  The UUIDs of special init quanta will be ignored.
        page_size : `int`, optional
            Approximate number of bytes to read at once from address files.
            Note that this does not set a page size for *all* reads, but it
            does affect the smallest, most numerous reads.

        Returns
        -------
        components : `PredictedQuantumGraphComponents` ]
            Components for quantum graph that can build execution quanta for
            all of the given IDs.
        """
        uri = ResourcePath(uri)
        if uri.getExtension() == ".qgraph":
            _LOG.warning(
                f"Reading and converting old quantum graph {uri}.  "
                "Use the '.qg' extension to write in the new format."
            )
            from ..graph import QuantumGraph

            old_qg = QuantumGraph.loadUri(uri, nodes=quantum_ids)
            return PredictedQuantumGraphComponents.from_old_quantum_graph(old_qg)

        with PredictedQuantumGraph.open(uri, page_size=page_size) as reader:
            reader.read_execution_quanta(quantum_ids)
            return reader.components

    @classmethod
    def from_old_quantum_graph(cls, old_quantum_graph: QuantumGraph) -> PredictedQuantumGraphComponents:
        """Construct from an old `QuantumGraph` instance.

        Parameters
        ----------
        old_quantum_graph : `QuantumGraph`
            Quantum graph to transform.

        Returns
        -------
        components : `PredictedQuantumGraphComponents`
            Components for a new predicted quantum graph.
        """
        header = HeaderModel.from_old_quantum_graph(old_quantum_graph)
        result = cls(header=header, pipeline_graph=old_quantum_graph.pipeline_graph)
        result.init_quanta.update_from_old_quantum_graph(old_quantum_graph)
        dimension_data_extractor = DimensionDataExtractor.from_dimension_group(
            old_quantum_graph.pipeline_graph.get_all_dimensions()
        )
        for task_node in old_quantum_graph.pipeline_graph.tasks.values():
            task_quanta = old_quantum_graph.get_task_quanta(task_node.label)
            for quantum_id, quantum in task_quanta.items():
                result.quantum_datasets[quantum_id] = PredictedQuantumDatasetsModel.from_execution_quantum(
                    task_node, quantum, quantum_id
                )
                dimension_data_extractor.update([cast(DataCoordinate, quantum.dataId)])
                for refs in itertools.chain(quantum.inputs.values(), quantum.outputs.values()):
                    dimension_data_extractor.update(ref.dataId for ref in refs)
        result.dimension_data = DimensionDataAttacher(
            records=dimension_data_extractor.records.values(),
            dimensions=result.pipeline_graph.get_all_dimensions(),
        )
        result.set_thin_graph()
        result.set_header_counts()
        return result

    def write(
        self,
        uri: ResourcePathExpression,
        *,
        zstd_level: int = 10,
        zstd_dict_size: int = 32768,
        zstd_dict_n_inputs: int = 512,
    ) -> None:
        """Write the graph to a file.

        Parameters
        ----------
        uri : convertible to `lsst.resources.ResourcePath`
            Path to write to.  Should have a ``.qg`` extension, or ``.qgraph``
            to force writing the old format.
        zstd_level : `int`, optional
            ZStandard compression level to use on JSON blocks.
        zstd_dict_size : `int`, optional
            Size of a ZStandard dictionary that shares compression information
            across components.  Set to zero to disable the dictionary.
            Dictionary compression is automatically disabled if the number of
            quanta is smaller than ``zstd_dict_n_inputs``.
        zstd_dict_n_inputs : `int`, optional
            Maximum number of `PredictedQuantumDatasetsModel` JSON
            representations to feed the ZStandard dictionary training routine.

        Notes
        -----
        Only a complete predicted quantum graph with all components fully
        populated should be written.
        """
        if self.header.n_task_quanta != {
            task_label: len(quanta) for task_label, quanta in self.thin_graph.quanta.items()
        }:
            raise RuntimeError(
                "Cannot save graph after partial read of quanta: thin graph is inconsistent with header."
            )
        # Ensure we record the actual version we're about to write, in case
        # we're rewriting an old graph in a new format.
        self.header.version = FORMAT_VERSION
        uri = ResourcePath(uri)
        match uri.getExtension():
            case ".qg":
                pass
            case ".qgraph":
                _LOG.warning(
                    "Converting to an old-format quantum graph.. "
                    "Use '.qg' instead of '.qgraph' to save in the new format."
                )
                old_qg = self.assemble().to_old_quantum_graph()
                old_qg.saveUri(uri)
                return
            case ext:
                raise ValueError(
                    f"Unsupported extension {ext!r} for quantum graph; "
                    "expected '.qg' (or '.qgraph' to force the old format)."
                )
        cdict: zstandard.ZstdCompressionDict | None = None
        cdict_data: bytes | None = None
        quantum_datasets_json: dict[uuid.UUID, bytes] = {}
        if len(self.quantum_datasets) < zstd_dict_n_inputs:
            # ZStandard will fail if we ask to use a compression dict without
            # giving it enough data, and it only helps if we have a lot of
            # quanta.
            zstd_dict_size = 0
        if zstd_dict_size:
            quantum_datasets_json = {
                quantum_model.quantum_id: quantum_model.model_dump_json().encode()
                for quantum_model in itertools.islice(self.quantum_datasets.values(), zstd_dict_n_inputs)
            }
            try:
                cdict = zstandard.train_dictionary(
                    zstd_dict_size,
                    list(quantum_datasets_json.values()),
                    level=zstd_level,
                )
            except zstandard.ZstdError as err:
                warnings.warn(f"Not using a compression dictionary: {err}.")
                cdict = None
            else:
                cdict_data = cdict.as_bytes()
        compressor = zstandard.ZstdCompressor(level=zstd_level, dict_data=cdict)
        indices = {quantum_id: n for n, quantum_id in enumerate(sorted(self.quantum_datasets.keys()))}
        with BaseQuantumGraphWriter.open(
            uri,
            header=self.header,
            pipeline_graph=self.pipeline_graph,
            indices=indices,
            address_filename="quanta",
            compressor=compressor,
            cdict_data=cdict_data,
        ) as writer:
            writer.write_single_model("thin_graph", self.thin_graph)
            if self.dimension_data is None:
                raise IncompleteQuantumGraphError(
                    "Cannot save predicted quantum graph with no dimension data."
                )
            serialized_dimension_data = self.dimension_data.serialized()
            writer.write_single_model("dimension_data", serialized_dimension_data)
            del serialized_dimension_data
            writer.write_single_model("init_quanta", self.init_quanta)
            with MultiblockWriter.open_in_zip(
                writer.zf, "quantum_datasets", writer.int_size
            ) as quantum_datasets_mb:
                for quantum_model in self.quantum_datasets.values():
                    if json_data := quantum_datasets_json.get(quantum_model.quantum_id):
                        quantum_datasets_mb.write_bytes(
                            quantum_model.quantum_id, writer.compressor.compress(json_data)
                        )
                    else:
                        quantum_datasets_mb.write_model(
                            quantum_model.quantum_id, quantum_model, writer.compressor
                        )
            writer.address_writer.addresses.append(quantum_datasets_mb.addresses)


@dataclasses.dataclass
class PredictedQuantumGraphReader(BaseQuantumGraphReader):
    """A helper class for reading predicted quantum graphs."""

    components: PredictedQuantumGraphComponents = dataclasses.field(init=False)
    """Quantum graph components populated by this reader's methods."""

    @classmethod
    @contextmanager
    def open(
        cls,
        uri: ResourcePathExpression,
        *,
        page_size: int = DEFAULT_PAGE_SIZE,
        import_mode: TaskImportMode = TaskImportMode.ASSUME_CONSISTENT_EDGES,
    ) -> Iterator[PredictedQuantumGraphReader]:
        """Construct a reader from a URI.

        Parameters
        ----------
        uri : convertible to `lsst.resources.ResourcePath`
            URI to open.  Should have a ``.qg`` extension.
        page_size : `int`, optional
            Approximate number of bytes to read at once from address files.
            Note that this does not set a page size for *all* reads, but it
            does affect the smallest, most numerous reads.
        import_mode : `.pipeline_graph.TaskImportMode`, optional
            How to handle importing the task classes referenced in the pipeline
            graph.

        Returns
        -------
        reader : `contextlib.AbstractContextManager` [ \
                `PredictedQuantumGraphReader` ]
            A context manager that returns the reader when entered.
        """
        with cls._open(
            uri,
            graph_type="predicted",
            address_filename="quanta",
            page_size=page_size,
            import_mode=import_mode,
            n_addresses=1,
        ) as self:
            yield self

    def __post_init__(self) -> None:
        self.components = PredictedQuantumGraphComponents(
            header=self.header, pipeline_graph=self.pipeline_graph
        )

    def finish(self) -> PredictedQuantumGraph:
        """Construct a `PredictedQuantumGraph` instance from this reader."""
        return self.components.assemble()

    def read_all(self) -> None:
        """Read all components in full."""
        self.read_thin_graph()
        self.read_execution_quanta()

    def read_thin_graph(self) -> None:
        """Read the thin graph.

        The thin graph is a quantum-quantum DAG with just task labels and data
        IDs as node attributes.  It always includes all regular quanta, and
        does not include init-input or init-output information.
        """
        if not self.components.thin_graph.quanta:
            if self.header.version > 0:
                self.components.thin_graph = self._read_single_block("thin_graph", PredictedThinGraphModel)
            else:
                self.address_reader.read_all()
                thin_graph_v0 = self._read_single_block("thin_graph", _PredictedThinGraphModelV0)
                self.components.thin_graph = thin_graph_v0._upgraded(self.address_reader.rows)

    def read_init_quanta(self) -> None:
        """Read the list of special quanta that represent init-inputs and
        init-outputs.
        """
        if not self.components.init_quanta.root:
            self.components.init_quanta = self._read_single_block("init_quanta", PredictedInitQuantaModel)

    def read_dimension_data(self) -> None:
        """Read all dimension records.

        Record data IDs will be immediately deserialized, while other fields
        will be left in serialized form until they are needed.
        """
        if self.components.dimension_data is None:
            serializable_dimension_data = self._read_single_block("dimension_data", SerializableDimensionData)
            self.components.dimension_data = DimensionDataAttacher(
                deserializers=[
                    DimensionRecordSetDeserializer.from_raw(
                        self.components.pipeline_graph.universe[element], serialized_records
                    )
                    for element, serialized_records in serializable_dimension_data.root.items()
                ],
                dimensions=DimensionGroup.union(
                    *self.components.pipeline_graph.group_by_dimensions(prerequisites=True).keys(),
                    universe=self.components.pipeline_graph.universe,
                ),
            )

    def read_quantum_datasets(self, quantum_ids: Iterable[uuid.UUID] | None = None) -> None:
        """Read information about all datasets produced and consumed by the
        given quantum IDs.

        Parameters
        ----------
        quantum_ids : `~collections.abc.Iterable` [ `uuid.UUID` ], optional
            Iterable of quantum IDs to load.  If not provided, all quanta will
            be loaded.  The UUIDs of special init quanta will be ignored.
        """
        quantum_datasets: PredictedQuantumDatasetsModel | None
        if quantum_ids is None:
            if len(self.components.quantum_datasets) != self.header.n_quanta:
                for quantum_datasets in MultiblockReader.read_all_models_in_zip(
                    self.zf,
                    "quantum_datasets",
                    PredictedQuantumDatasetsModel,
                    self.decompressor,
                    int_size=self.components.header.int_size,
                    page_size=self.page_size,
                ):
                    self.components.quantum_datasets.setdefault(quantum_datasets.quantum_id, quantum_datasets)
                self.address_reader.read_all()
            return
        with MultiblockReader.open_in_zip(
            self.zf, "quantum_datasets", int_size=self.components.header.int_size
        ) as mb_reader:
            for quantum_id in quantum_ids:
                if quantum_id in self.components.quantum_datasets:
                    continue
                address_row = self.address_reader.find(quantum_id)
                quantum_datasets = mb_reader.read_model(
                    address_row.addresses[0], PredictedQuantumDatasetsModel, self.decompressor
                )
                if quantum_datasets is not None:
                    self.components.quantum_datasets[address_row.key] = quantum_datasets
        return

    def read_execution_quanta(self, quantum_ids: Iterable[uuid.UUID] | None = None) -> None:
        """Read all information needed to execute the given quanta.

        Parameters
        ----------
        quantum_ids : `~collections.abc.Iterable` [ `uuid.UUID` ], optional
            Iterable of quantum IDs to load.  If not provided, all quanta will
            be loaded.  The UUIDs of special init quanta will be ignored.
        """
        self.read_init_quanta()
        self.read_dimension_data()
        self.read_quantum_datasets(quantum_ids)
