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
    "HeaderModel",
    "PredictedDatasetModel",
    "PredictedInitQuantaModel",
    "PredictedQuantumDatasetsModel",
    "PredictedQuantumGraph",
    "PredictedQuantumGraphComponents",
    "PredictedQuantumGraphReader",
    "PredictedThinGraphModel",
    "PredictedThinQuantumModel",
)

import dataclasses
import datetime
import getpass
import itertools
import logging
import operator
import uuid
import zipfile
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from io import DEFAULT_BUFFER_SIZE
from typing import TYPE_CHECKING, Any, TypeAlias, cast

import networkx
import networkx.algorithms.bipartite
import pydantic
import zstandard

from lsst.daf.butler import (
    DataCoordinate,
    DataIdValue,
    DatasetRef,
    DatasetType,
    DimensionDataAttacher,
    DimensionDataExtractor,
    DimensionGroup,
    DimensionRecordSetDeserializer,
    Quantum,
    SerializableDimensionData,
)
from lsst.daf.butler.datastore.record_data import DatastoreRecordData, SerializedDatastoreRecordData
from lsst.resources import ResourcePath, ResourcePathExpression

from .. import automatic_connection_constants as acc
from ..pipeline import TaskDef
from ..pipeline_graph import NodeType, PipelineGraph, TaskInitNode, TaskNode, WriteEdge
from ..pipeline_graph.io import SerializedPipelineGraph, TaskImportMode
from ._multiblock import (
    AddressReader,
    AddressWriter,
    Compressor,
    Decompressor,
    MultiblockReader,
    MultiblockWriter,
)

if TYPE_CHECKING:
    from ..graph import QuantumGraph

TaskLabel: TypeAlias = str
DatasetTypeName: TypeAlias = str
ConnectionName: TypeAlias = str
QuantumIndex: TypeAlias = int
DatastoreName: TypeAlias = str
DimensionElementName: TypeAlias = str
DataCoordinateValues: TypeAlias = list[DataIdValue]

_LOG = logging.getLogger(__name__)


class IncompleteQuantumGraphError(RuntimeError):
    pass


class HeaderModel(pydantic.BaseModel):
    """Data model for the header of a predicted quantum graph file."""

    version: int = 0
    """File format / data model version number."""

    graph_type: str = "predicted"
    """Type of quantum graph stored in this file."""

    inputs: list[str] = pydantic.Field(default_factory=list)
    """List of input collections used to build the quantum graph."""

    output: str | None = ""
    """Output CHAINED collection provided when building the quantum graph."""

    output_run: str = ""
    """Output RUN collection for all output datasets in this graph."""

    user: str = pydantic.Field(default_factory=getpass.getuser)
    """Username of the process that built this quantum graph."""

    timestamp: datetime.datetime = pydantic.Field(default_factory=datetime.datetime.now)
    """Timestamp for when this quantum graph was built.

    It is unspecified exactly which point during quantum-graph generation this
    timestamp is recorded.
    """

    metadata: dict[str, Any] = pydantic.Field(default_factory=dict)
    """Free-form metadata associated with this quantum graph at build time."""

    int_size: int = 8
    """Number of bytes in the integers used in this file's multi-block and
    address files.
    """

    n_quanta: int = 0
    """Total number of quanta in this graph.

    This includes special "init" quanta (one for each task and one global one).
    """

    n_datasets: int = 0
    """Total number of distinct datasets in this graph."""

    n_task_quanta: dict[str, int] = pydantic.Field(default_factory=dict)
    """Number of quanta for each task label.

    This does not include special "init" quanta.
    """

    @classmethod
    def from_old_quantum_graph(cls, old_quantum_graph: QuantumGraph) -> HeaderModel:
        """Extract a header from an old `QuantumGraph` instance.

        Parameters
        ----------
        old_quantum_graph : `QuantumGraph`
            Quantum graph to extract a header from.

        Returns
        -------
        header : `PredictedHeaderModel`
            Header for a new predicted quantum graph.
        """
        metadata = dict(old_quantum_graph.metadata)
        metadata.pop("packages", None)
        return cls(
            inputs=list(metadata.pop("input", [])),
            output=metadata.pop("output", None),
            output_run=metadata.pop("output_run", ""),
            user=metadata.pop("user", ""),
            timestamp=metadata.pop("time", datetime.datetime.now()),
            metadata=metadata,
        )

    def to_old_metadata(self) -> dict[str, Any]:
        """Return a dictionary using the key conventions used in old quantum
        graph files.
        """
        result = self.metadata.copy()
        result["input"] = self.inputs
        result["output"] = self.output
        result["output_run"] = self.output_run
        result["user"] = self.user
        result["time"] = self.timestamp
        return result


class PredictedThinQuantumModel(pydantic.BaseModel):
    """Data model for a quantum data ID and internal integer ID in a predicted
    quantum graph.
    """

    quantum_index: QuantumIndex
    """Internal integer ID for this quantum."""

    data_coordinate: DataCoordinateValues = pydantic.Field(default_factory=list)
    """Full (required and implied) data coordinate values for this quantum."""


class PredictedThinGraphModel(pydantic.BaseModel):
    """Data model for the predicted quantum graph component that maps each
    task label to the data IDs and internal integer IDs of its quanta.
    """

    quanta: dict[TaskLabel, list[PredictedThinQuantumModel]] = pydantic.Field(default_factory=dict)
    """Minimal descriptions of all quanta, grouped by task label."""

    edges: list[tuple[QuantumIndex, QuantumIndex]] = pydantic.Field(default_factory=list)
    """Pairs of (predecessor, successor) internal integer quantum IDs."""


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

    def iter_dataset_ids(self) -> Iterator[uuid.UUID]:
        """Return an iterator over the UUIDs of all datasets referenced by this
        quantum.

        Returns
        -------
        iter : `~collections.abc.Iterator` [ `uuid.UUID` ]
            Iterator over dataset IDs.
        """
        for datasets in itertools.chain(self.inputs.values(), self.outputs.values()):
            for dataset in datasets:
                yield dataset.dataset_id

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
        result = cls.model_construct(
            quantum_id=quantum_id,
            task_label=task_node.label,
            data_id=list(cast(DataCoordinate, quantum.dataId).full_values),
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
        result = cls.model_construct(quantum_id=uuid.uuid4(), task_label=task_init_node.label)
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
            quantum_id=uuid.uuid4(), task_label=""
        )
        for ref in old_quantum_graph.globalInitOutputRefs():
            global_init_quantum.outputs[ref.datasetType.name] = [PredictedDatasetModel.from_dataset_ref(ref)]
        self.root.append(global_init_quantum)
        for task_node in old_quantum_graph.pipeline_graph.tasks.values():
            self.root.append(
                PredictedQuantumDatasetsModel.from_old_quantum_graph_init(task_node.init, old_quantum_graph)
            )


class PredictedQuantumGraph:
    """A directed acyclic graph that predicts a processing run and supports it
    during execution.

    Parameters
    ----------
    components : `PredictedQuantumGraphComponents`
        A struct of components used to construct the graph.
    """

    def __init__(self, components: PredictedQuantumGraphComponents):
        if not components.header.graph_type == "predicted":
            raise TypeError(f"Header is for a {components.header.graph_type!r} graph, not 'predicted'.")
        self.header = components.header
        self.pipeline_graph = components.pipeline_graph
        self._quantum_only_xgraph = networkx.DiGraph()
        self._bipartite_xgraph = networkx.MultiDiGraph()
        self._quanta_by_task_label: dict[str, dict[DataCoordinate, uuid.UUID]] = {
            task_label: {} for task_label in self.pipeline_graph.tasks.keys()
        }
        self._datasets_by_type: dict[str, dict[DataCoordinate, uuid.UUID]] = {
            dataset_type_name: {} for dataset_type_name in self.pipeline_graph.dataset_types.keys()
        }
        uuid_by_index = {v: k for k, v in components.quantum_indices.items()}
        self._dimension_data = components.dimension_data
        self._init_quanta: dict[str, PredictedQuantumDatasetsModel] = {
            q.task_label: q for q in components.init_quanta.root
        }
        self._quantum_datasets: dict[uuid.UUID, PredictedQuantumDatasetsModel] = {}
        self._expanded_data_ids: dict[DataCoordinate, DataCoordinate] = {}
        for index1, index2 in components.thin_graph.edges:
            self._quantum_only_xgraph.add_edge(uuid_by_index[index1], uuid_by_index[index2])
        for task_label, thin_quanta_for_task in components.thin_graph.quanta.items():
            for thin_quantum in thin_quanta_for_task:
                self._add_quantum(
                    uuid_by_index[thin_quantum.quantum_index],
                    task_label,
                    thin_quantum.data_coordinate,
                )
        for quantum_datasets in components.quantum_datasets.values():
            self._quantum_datasets[quantum_datasets.quantum_id] = quantum_datasets
            self._add_quantum(
                quantum_datasets.quantum_id, quantum_datasets.task_label, quantum_datasets.data_coordinate
            )
            for connection_name, input_datasets in quantum_datasets.inputs.items():
                for input_dataset in input_datasets:
                    self._add_dataset(input_dataset)
                    self._bipartite_xgraph.add_edge(
                        input_dataset.dataset_id, quantum_datasets.quantum_id, key=connection_name
                    )
            for connection_name, output_datasets in quantum_datasets.outputs.items():
                for output_dataset in output_datasets:
                    self._add_dataset(output_dataset)
                    self._bipartite_xgraph.add_edge(
                        quantum_datasets.quantum_id, output_dataset.dataset_id, key=connection_name
                    )
        if not components.thin_graph.edges:
            self._quantum_only_xgraph.update(
                networkx.algorithms.bipartite.projected_graph(
                    networkx.DiGraph(self._bipartite_xgraph),
                    self._quantum_only_xgraph.nodes.keys(),
                )
            )

    def _add_quantum(
        self, quantum_id: uuid.UUID, task_label: str, data_coordinate_values: Sequence[DataIdValue]
    ) -> None:
        self._quantum_only_xgraph.add_node(quantum_id, task_label=task_label)
        self._bipartite_xgraph.add_node(quantum_id, task_label=task_label, bipartite=NodeType.TASK.bipartite)
        data_coordinate_values = tuple(data_coordinate_values)
        dimensions = self.pipeline_graph.tasks[task_label].dimensions
        data_id = DataCoordinate.from_full_values(dimensions, tuple(data_coordinate_values))
        self._quantum_only_xgraph.nodes[quantum_id].setdefault("data_id", data_id)
        self._bipartite_xgraph.nodes[quantum_id].setdefault("data_id", data_id)
        self._quanta_by_task_label[task_label][data_id] = quantum_id

    def _add_dataset(self, model: PredictedDatasetModel) -> None:
        self._bipartite_xgraph.add_node(
            model.dataset_id,
            dataset_type=self.pipeline_graph.dataset_types[model.dataset_type_name].dataset_type,
            run=model.run,
            bipartite=NodeType.DATASET_TYPE.bipartite,
        )
        data_coordinate_values = tuple(model.data_coordinate)
        dimensions = self.pipeline_graph.dataset_types[model.dataset_type_name].dimensions
        data_id = DataCoordinate.from_full_values(dimensions, data_coordinate_values)
        self._bipartite_xgraph.nodes[model.dataset_id].setdefault("data_id", data_id)
        self._datasets_by_type[model.dataset_type_name][data_id] = model.dataset_id

    @classmethod
    def open(
        cls, uri: ResourcePathExpression, page_size: int = DEFAULT_BUFFER_SIZE
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

        Returns
        -------
        reader : `contextlib.AbstractContextManager` [ \
                `PredictedQuantumGraphReader` ]
            A context manager that returns the reader when entered.
        """
        return PredictedQuantumGraphReader.open(uri, page_size=page_size)

    @classmethod
    def read_execution_quanta(
        cls,
        uri: ResourcePathExpression,
        quantum_ids: Iterable[uuid.UUID] | None = None,
        page_size: int = DEFAULT_BUFFER_SIZE,
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
        uri = ResourcePath(uri)
        if uri.getExtension() == ".qgraph":
            _LOG.warning(
                f"Reading and converting old quantum graph {uri}.  "
                "Use the '.qg' extension to write in the new format."
            )
            from ..graph import QuantumGraph

            old_qg = QuantumGraph.loadUri(uri, nodes=quantum_ids)
            return PredictedQuantumGraphComponents.from_old_quantum_graph(old_qg).assemble()

        with cls.open(uri, page_size=page_size) as reader:
            reader.read_execution_quanta(quantum_ids)
            return reader.finish()

    @property
    def quanta(self) -> Mapping[str, Mapping[DataCoordinate, uuid.UUID]]:
        """A nested of all quanta, keyed first by task name and then by
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
    def datasets(self) -> Mapping[str, Mapping[DataCoordinate, uuid.UUID]]:
        """A nested mapping of all datasets, keyed first by dataset type name
        and then by data ID.

        Notes
        -----
        This is populated only by the ``quantum_datasets`` component, and only
        datasets referenced by loaded quanta are present.  All dataset types in
        the pipeline graph are included, even if none of their datasets were
        loaded (i.e. nested mappings may be empty).

        The returned object may be an internal dictionary; as the type
        annotation indicates, it should not be modified in place.
        """
        return self._datasets_by_type

    @property
    def quantum_only_xgraph(self) -> networkx.DiGraph:
        """A directed acyclic graph with quanta as nodes and datasets elided.

        Notes
        -----
        Node keys are quantum UUIDs, and are present for all quanta only if the
        ``thin_graph`` component was loaded or if all `quantum_datasets`
        entries were loaded.  Edges will present for all loaded quanta.

        The returned object is a read-only view of an internal one.
        """
        return self._quantum_only_xgraph.copy(as_view=True)

    @property
    def bipartite_xgraph(self) -> networkx.MultiDiGraph:
        """A directed acyclic graph with quantum and dataset nodes.

        This graph never includes init-input and init-output datasets.

        Notes
        -----
        Node keys are quantum or dataset UUIDs, and are present for all quanta
        if the ``thin_graph`` component is loaded, and are always populated
        with ``data_id`` and ``task_label`` attributes. Edges and dataset nodes
        are only present for quanta whose ``quantum_datasets` were loaded.
        Datasets are always populated with ``data_id`` and `dataset_type``
        attributes, and edges are keyed by their connection name.

        The returned object is a read-only view of an internal one.
        """
        return self._bipartite_xgraph.copy(as_view=True)

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
            Mapping of quanta, keyed by UUID and sorted topologically and
            deterministically.  All dataset types are adapted to the task's
            storage class declarations and inputs may be components. All data
            IDs have dimension records attached.
        """
        if not self._init_quanta:
            raise IncompleteQuantumGraphError(
                "Cannot build execution quanta without loading the ``init_quanta`` component."
            )
        if self._dimension_data is None:
            raise IncompleteQuantumGraphError(
                "Cannot build execution quanta without loading the ``dimension_data`` component."
            )
        if quantum_ids is None:
            if task_label is not None:
                quantum_ids = self._quanta_by_task_label[task_label].values()
            else:
                quantum_ids = self._quantum_only_xgraph.nodes.keys()
            del task_label  # make sure we don't accidentally use this.
        result: dict[uuid.UUID, Quantum] = {}
        data_ids_to_expand: dict[DimensionGroup, set[DataCoordinate]] = defaultdict(set)
        for quantum_id in quantum_ids:
            data_id: DataCoordinate = self._bipartite_xgraph.nodes[quantum_id]["data_id"]
            if not data_id.hasRecords():
                data_ids_to_expand[data_id.dimensions].add(data_id)
            for dataset_id in itertools.chain(
                self._bipartite_xgraph.predecessors(quantum_id),
                self._bipartite_xgraph.successors(quantum_id),
            ):
                data_id = self._bipartite_xgraph.nodes[dataset_id]["data_id"]
                if not data_id.hasRecords():
                    data_ids_to_expand[data_id.dimensions].add(data_id)
        for dimensions, data_ids_for_dimensions in data_ids_to_expand.items():
            self._expanded_data_ids.update(
                (d, d) for d in self._dimension_data.attach(dimensions, data_ids_for_dimensions)
            )
        ordered_ids = networkx.dag.lexicographical_topological_sort(
            self._quantum_only_xgraph.subgraph(quantum_ids),
            key=lambda q: self._quantum_only_xgraph.nodes[q]["data_id"],
        )
        task_init_datastore_records: dict[TaskLabel, dict[DatastoreName, DatastoreRecordData]] = {}
        for quantum_id in ordered_ids:
            try:
                quantum_datasets = self._quantum_datasets[quantum_id]
            except KeyError:
                raise IncompleteQuantumGraphError(
                    f"Full quantum information for {quantum_id} was not loaded."
                ) from None
            task_node = self.pipeline_graph.tasks[quantum_datasets.task_label]
            quantum_data_id = self._expanded_data_ids[self._bipartite_xgraph.nodes[quantum_id]["data_id"]]
            inputs: dict[DatasetType, list[DatasetRef]] = {}
            for connection_name, input_datasets in quantum_datasets.inputs.items():
                if (read_edge := task_node.inputs.get(connection_name)) is None:
                    read_edge = task_node.prerequisite_inputs[connection_name]
                dataset_type = read_edge.adapt_dataset_type(
                    self.pipeline_graph.dataset_types[read_edge.parent_dataset_type_name].dataset_type
                )
                inputs[dataset_type] = [
                    self._make_general_ref(dataset_type, d.dataset_id) for d in input_datasets
                ]
            outputs: dict[DatasetType, list[DatasetRef]] = {}
            for connection_name, output_datasets in quantum_datasets.outputs.items():
                match connection_name:
                    case acc.LOG_OUTPUT_CONNECTION_NAME:
                        write_edge = cast(WriteEdge, task_node.log_output)
                    case acc.METADATA_OUTPUT_CONNECTION_NAME:
                        write_edge = task_node.metadata_output
                    case _:
                        write_edge = task_node.outputs[connection_name]
                dataset_type = write_edge.adapt_dataset_type(
                    self.pipeline_graph.dataset_types[write_edge.parent_dataset_type_name].dataset_type
                )
                outputs[dataset_type] = [
                    self._make_general_ref(dataset_type, d.dataset_id) for d in output_datasets
                ]
            if task_node.label not in task_init_datastore_records:
                init_quantum_datasets = self._init_quanta[task_node.label]
                task_init_datastore_records[task_node.label] = {
                    datastore_name: DatastoreRecordData.from_simple(serialized_records)
                    for datastore_name, serialized_records in init_quantum_datasets.datastore_records.items()
                }
            datastore_records = {
                datastore_name: DatastoreRecordData.from_simple(serialized_records)
                for datastore_name, serialized_records in quantum_datasets.datastore_records.items()
            }
            result[quantum_id] = Quantum(
                taskName=task_node.task_class_name,
                taskClass=task_node.task_class,
                dataId=quantum_data_id,
                initInputs={
                    ref.datasetType: ref for ref in self.get_init_inputs(quantum_datasets.task_label).values()
                },
                inputs=inputs,
                outputs=outputs,
                datastore_records=DatastoreRecordData.merge_mappings(
                    datastore_records, task_init_datastore_records[task_node.label]
                ),
            )
        return result

    def _make_general_ref(self, dataset_type: DatasetType, dataset_id: uuid.UUID) -> DatasetRef:
        node_state = self._bipartite_xgraph.nodes[dataset_id]
        data_id = self._expanded_data_ids[node_state["data_id"]]
        return DatasetRef(dataset_type, data_id, run=node_state["run"], id=dataset_id)

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


@dataclasses.dataclass(kw_only=True)
class PredictedQuantumGraphComponents:
    """A helper class for building and writing predicted quantum graphs.

    Notes
    -----
    This class is a simple struct of model classes to allow different tools
    that build predicted quantum graphs to assemble them in whatever order they
    prefer.  It does not enforce any internal invariants (e.g. the quantum and
    dataset counts in the header, different representations of quanta, internal
    ID sorting, etc.).
    """

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

    This uses internal integer IDs ("indexes") for node IDs.

    This does not include the special "init" quanta.
    """

    quantum_datasets: dict[uuid.UUID, PredictedQuantumDatasetsModel] = dataclasses.field(default_factory=dict)
    """The full descriptions of all quanta, including input and output
    dataset, keyed by UUID.

    When used to construct a `PredictedQuantumGraph`, this need not have all
    entries.

    This does not include special "init" quanta.
    """

    quantum_indices: dict[uuid.UUID, QuantumIndex] = dataclasses.field(default_factory=dict)
    """A mapping from external universal quantum ID to internal integer ID.

    While this `dict` does not need to be sorted, the internal integer IDs do
    need to correspond exactly to ``enumerate(sorted(uuids))``.

    When used to construct a `PredictedQuantumGraph`, this must be fully
    populated if `thin_graph` is.  It can be empty otherwise.

    This does include special "init" quanta.
    """

    def set_quantum_indices(self) -> None:
        """Populate the `quantum_indices` component by sorting the UUIDs in the
        `init_quanta` and `quantum_datasets` components (which must both be
        complete).
        """
        all_quantum_ids = [q.quantum_id for q in self.init_quanta.root]
        all_quantum_ids.extend(self.quantum_datasets.keys())
        all_quantum_ids.sort(key=operator.attrgetter("int"))
        self.quantum_indices = {quantum_id: index for index, quantum_id in enumerate(all_quantum_ids)}

    def set_thin_graph(self) -> None:
        """Populate the `thin_graph` component from the `pipeline_graph`,
        `quantum_datasets` and `quantum_indices` components (which must all be
        complete).
        """
        bipartite_xgraph = networkx.DiGraph()
        self.thin_graph.quanta = {task_label: [] for task_label in self.pipeline_graph.tasks}
        graph_quantum_indices = []
        for quantum_datasets in self.quantum_datasets.values():
            quantum_index = self.quantum_indices[quantum_datasets.quantum_id]
            self.thin_graph.quanta[quantum_datasets.task_label].append(
                PredictedThinQuantumModel.model_construct(
                    quantum_index=quantum_index,
                    data_coordinate=quantum_datasets.data_coordinate,
                )
            )
            for dataset in itertools.chain.from_iterable(quantum_datasets.inputs.values()):
                bipartite_xgraph.add_edge(dataset.dataset_id, quantum_index)
            for dataset in itertools.chain.from_iterable(quantum_datasets.outputs.values()):
                bipartite_xgraph.add_edge(quantum_index, dataset.dataset_id)
            graph_quantum_indices.append(quantum_index)
        quantum_only_xgraph: networkx.DiGraph = networkx.bipartite.projected_graph(
            bipartite_xgraph, graph_quantum_indices
        )
        self.thin_graph.edges = list(quantum_only_xgraph.edges)

    def set_header_counts(self) -> None:
        """Populate the quantum and dataset counts in the header from the
        `quantum_indices`, `thin_graph`, `init_quanta`, and `quantum_datasets`
        components.
        """
        self.header.n_quanta = len(self.quantum_indices)
        self.header.n_task_quanta = {
            task_label: len(thin_quanta) for task_label, thin_quanta in self.thin_graph.quanta.items()
        }
        all_dataset_ids: set[uuid.UUID] = set()
        for quantum_datasets in itertools.chain(self.init_quanta.root, self.quantum_datasets.values()):
            all_dataset_ids.update(quantum_datasets.iter_dataset_ids())
        self.header.n_datasets = len(all_dataset_ids)

    def assemble(self) -> PredictedQuantumGraph:
        """Construct a `PredictedQuantumGraph` from these components."""
        return PredictedQuantumGraph(self)

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
        result.dimension_data = DimensionDataAttacher(records=dimension_data_extractor.records.values())
        result.set_quantum_indices()
        result.set_thin_graph()
        result.set_header_counts()
        return result

    def write(
        self,
        uri: ResourcePathExpression,
        *,
        zstd_level: int = 10,
        zstd_dict_size: int = 4096,
    ) -> None:
        """Write the graph to a file.

        Parameters
        ----------
        uri : convertible to `lsst.resources.ResourcePath`
            Path to write to.  Should have a ``.qg`` extension, or ``.qgraph``
            to force writing the old format.  If there is no extension, ``.qg``
            will be added.
        zstd_level : `int`, optional
            ZStandard compression level to use on JSON blocks.
        zstd_dict_size : `int`, optional
            Size of a ZStandard dictionary that shares compression information
            across components.  Set to zero to disable the dictionary.

        Notes
        -----
        Only a complete predicted quantum graph with all components fully
        populated should be written.
        """
        if self.header.n_quanta != len(self.quantum_indices):
            raise RuntimeError(
                f"Cannot save graph after partial read of quanta: expected {self.header.n_quanta}, "
                f"got {len(self.quantum_indices)}."
            )
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
            case "":
                uri = uri.updatedExtension(".qg")
            case ext:
                raise ValueError(
                    f"Unsupported extension {ext!r} for quantum graph; "
                    "expected '.qg' (or '.qgraph' to force the old format)."
                )
        quantum_address_writer = AddressWriter(self.quantum_indices)
        header_json = self.header.model_dump_json().encode()
        pipeline_graph_json = (
            SerializedPipelineGraph.serialize(self.pipeline_graph).model_dump_json().encode()
        )
        thin_graph_json = self.thin_graph.model_dump_json().encode()
        cdict: zstandard.ZstdCompressionDict | None = None
        if zstd_dict_size:
            cdict = zstandard.train_dictionary(
                zstd_dict_size,
                [header_json, pipeline_graph_json, thin_graph_json],
                level=zstd_level,
            )
        compressor = zstandard.ZstdCompressor(level=zstd_level, dict_data=cdict)
        with uri.open(mode="wb") as stream:
            with zipfile.ZipFile(stream, mode="w", compression=zipfile.ZIP_STORED) as zf:
                self._write_single_block(zf, "header", header_json, compressor)
                if cdict is not None:
                    zf.writestr("compression_dict", cdict.as_bytes())
                self._write_single_block(zf, "pipeline_graph", pipeline_graph_json, compressor)
                self._write_single_block(zf, "thin_graph", thin_graph_json, compressor)
                if self.dimension_data is None:
                    raise IncompleteQuantumGraphError(
                        "Cannot save predicted quantum graph with no dimension data."
                    )
                serialized_dimension_data = self.dimension_data.serialized()
                self._write_single_model(zf, "dimension_data", serialized_dimension_data, compressor)
                del serialized_dimension_data
                self._write_single_model(zf, "init_quanta", self.init_quanta, compressor)
                with MultiblockWriter.open_in_zip(
                    zf, "quantum_datasets", self.header.int_size
                ) as quantum_datasets_mb:
                    for quantum_model in self.quantum_datasets.values():
                        quantum_datasets_mb.write_model(quantum_model.quantum_id, quantum_model, compressor)
                quantum_address_writer.addresses.append(quantum_datasets_mb.addresses)
                quantum_address_writer.write_to_zip(zf, "quanta", int_size=self.header.int_size)

    def _write_single_model(
        self, zf: zipfile.ZipFile, name: str, model: pydantic.BaseModel, compressor: Compressor
    ) -> None:
        """Write a single compressed JSON block as a 'file' in a zip archive.

        Parameters
        ----------
        zf : `zipfile.ZipFile`
            Zip archive to add the file to.
        name : `str`
            Base name of the file.  An extension will be added.
        model : `pydantic.BaseModel`
            Pydantic model to convert to JSON.
        compressor : `Compressor`
            Object with a `compress` method that takes and returns `bytes`.
        """
        json_data = model.model_dump_json().encode()
        self._write_single_block(zf, name, json_data, compressor)

    def _write_single_block(
        self, zf: zipfile.ZipFile, name: str, json_data: bytes, compressor: Compressor
    ) -> None:
        """Write a single compressed JSON block as a 'file' in a zip archive.

        Parameters
        ----------
        zf : `zipfile.ZipFile`
            Zip archive to add the file to.
        name : `str`
            Base name of the file.  An extension will be added.
        json_data : `bytes`
            Raw JSON to compress and write.
        compressor : `Compressor`
            Object with a `compress` method that takes and returns `bytes`.
        """
        compressed_data = compressor.compress(json_data)
        zf.writestr(f"{name}.json.zst", compressed_data)


@dataclasses.dataclass
class PredictedQuantumGraphReader:
    """A helper class for reading predicted quantum graphs."""

    components: PredictedQuantumGraphComponents
    """Quantum graph components populated by this reader's methods."""

    zf: zipfile.ZipFile
    """The zip archive that represents the quantum graph on disk."""

    decompressor: Decompressor
    """A decompressor for all compressed JSON blocks."""

    address_reader: AddressReader
    """A helper object for reading addresses into the full-quantum multi-block
    files.
    """

    @classmethod
    @contextmanager
    def open(
        cls, uri: ResourcePathExpression, page_size: int = DEFAULT_BUFFER_SIZE
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

        Returns
        -------
        reader : `contextlib.AbstractContextManager` [ \
                `PredictedQuantumGraphReader` ]
            A context manager that returns the reader when entered.
        """
        uri = ResourcePath(uri)
        cdict: zstandard.ZstdCompressionDict | None = None
        with uri.open(mode="rb") as zf_stream:
            with zipfile.ZipFile(zf_stream, "r") as zf:
                if (cdict_path := zipfile.Path("compression_dict")).exists():
                    cdict = zstandard.ZstdCompressionDict(cdict_path.read_bytes())
                decompressor = zstandard.ZstdDecompressor(cdict)
                header = cls._read_single_block_static("header", HeaderModel, zf, decompressor)
                if not header.graph_type == "predicted":
                    raise TypeError(f"Header is for a {header.graph_type!r} graph, not 'predicted'.")
                serialized_pipeline_graph = cls._read_single_block_static(
                    "pipeline_graph", SerializedPipelineGraph, zf, decompressor
                )
                pipeline_graph = serialized_pipeline_graph.deserialize(TaskImportMode.DO_NOT_IMPORT)
                with AddressReader.open_in_zip(
                    zf, "quanta", page_size=page_size, int_size=header.int_size
                ) as address_reader:
                    yield cls(
                        components=PredictedQuantumGraphComponents(
                            header=header, pipeline_graph=pipeline_graph
                        ),
                        zf=zf,
                        decompressor=decompressor,
                        address_reader=address_reader,
                    )

    def finish(self) -> PredictedQuantumGraph:
        """Construct a `PredictedQuantumGraph` instance from this reader."""
        return self.components.assemble()

    def read_thin_graph(self) -> None:
        """Read the thin graph.

        The thin graph is a quantum-quantum DAG with internal integer IDs for
        nodes and just task labels and data IDs as node attributes.  It always
        includes all regular quanta, and does not include init-input or
        init-output information.
        """
        if not self.components.thin_graph.quanta:
            self.components.thin_graph = self._read_single_block("thin_graph", PredictedThinGraphModel)
        if len(self.components.quantum_indices) != self.components.header.n_quanta:
            self.address_reader.read_all()
            self.components.quantum_indices.update(
                {row.key: row.index for row in self.address_reader.rows.values()}
            )

    def read_init_quanta(self) -> None:
        """Read the list of special quanta that represent init-inputs and
        init-outputs.
        """
        if not self.components.init_quanta:
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
        if quantum_ids is None:
            self.address_reader.read_all()
            quantum_ids = self.address_reader.rows.keys()
        with MultiblockReader.open_in_zip(
            self.zf, "quantum_datasets", self.components.header.int_size
        ) as mb_reader:
            for quantum_id in quantum_ids:
                address_row = self.address_reader.find(quantum_id)
                self.components.quantum_indices[address_row.key] = address_row.index
                if address_row.key not in self.components.quantum_datasets:
                    quantum_datasets = mb_reader.read_model(
                        address_row.addresses[0], PredictedQuantumDatasetsModel, self.decompressor
                    )
                    if quantum_datasets is not None:
                        self.components.quantum_datasets[address_row.key] = quantum_datasets

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

    @staticmethod
    def _read_single_block_static[T: pydantic.BaseModel](
        name: str, model_type: type[T], zf: zipfile.ZipFile, decompressor: Decompressor
    ) -> T:
        """Read a single compressed JSON block from a 'file' in a zip archive.

        Parameters
        ----------
        zf : `zipfile.ZipFile`
            Zip archive to read the file from.
        name : `str`
            Base name of the file.  An extension will be added.
        model_type : `type` [ `pydantic.BaseModel` ]
            Pydantic model to validate JSON with.
        decompressor : `Deompressor`
            Object with a `decompress` method that takes and returns `bytes`.

        Returns
        -------
        model : `pydantic.BaseModel`
            Validated model.
        """
        compressed_data = zf.read(f"{name}.json.zst")
        json_data = decompressor.decompress(compressed_data)
        return model_type.model_validate_json(json_data)

    def _read_single_block[T: pydantic.BaseModel](self, name: str, model_type: type[T]) -> T:
        """Read a single compressed JSON block from a 'file' in a zip archive.

        Parameters
        ----------
        zf : `zipfile.ZipFile`
            Zip archive to read the file from.
        name : `str`
            Base name of the file.  An extension will be added.
        model_type : `type` [ `pydantic.BaseModel` ]
            Pydantic model to validate JSON with.
        decompressor : `Deompressor`
            Object with a `decompress` method that takes and returns `bytes`.

        Returns
        -------
        model : `pydantic.BaseModel`
            Validated model.
        """
        return self._read_single_block_static(name, model_type, self.zf, self.decompressor)
