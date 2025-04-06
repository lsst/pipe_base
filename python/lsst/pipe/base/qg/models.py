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

__all__ = ()

import contextlib
import dataclasses
import datetime
import itertools
import logging
import lzma
import os
import uuid
import zipfile
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping
from contextlib import ExitStack
from io import BytesIO
from operator import attrgetter
from types import ModuleType
from typing import IO, Protocol, Self, TypeAlias, TypeVar, cast

import click
import pydantic
import tqdm

from lsst.daf.butler import (
    DataCoordinate,
    DataIdValue,
    DatasetRef,
    DimensionGroup,
    DimensionRecordSet,
    DimensionRecordSetDeserializer,
    Quantum,
    SerializedKeyValueDimensionRecord,
)
from lsst.daf.butler.datastore.record_data import DatastoreRecordData, SerializedDatastoreRecordData
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils.timer import time_this

from .. import automatic_connection_constants as acc
from .._status import QuantumSuccessCaveats
from ..graph import QuantumGraph
from ..pipeline_graph import PipelineGraph, TaskImportMode, TaskInitNode, TaskNode
from ..pipeline_graph.io import SerializedPipelineGraph
from ..quantum_provenance_graph import ExceptionInfo, QuantumRunStatus
from .address import Address, AddressReader, AddressWriter

try:
    zstandard: ModuleType | None
    import zstandard
except ImportError:
    zstandard = None


_LOG = logging.getLogger(__name__)

TaskLabel: TypeAlias = str
DatasetTypeName: TypeAlias = str
ConnectionName: TypeAlias = str
QuantumIndex: TypeAlias = int
DatasetIndex: TypeAlias = int
ConnectionIndex: TypeAlias = int
DatastoreName: TypeAlias = str
DimensionElementName: TypeAlias = str
DataCoordinateValues: TypeAlias = list[DataIdValue]


class Compressor(Protocol):
    def compress(self, data: bytes) -> bytes: ...


class Decompressor(Protocol):
    def decompress(self, data: bytes) -> bytes: ...


_T = TypeVar("_T", bound=pydantic.BaseModel)


class HeaderModel(pydantic.BaseModel):
    version: int = pydantic.Field(default=0)
    pipeline: SerializedPipelineGraph
    inputs: list[str] = pydantic.Field(default_factory=list)
    output: str | None = pydantic.Field(default=None)
    output_run: str
    user: str
    timestamp: datetime.datetime = pydantic.Field(default_factory=datetime.datetime.now)
    metadata: dict[str, str | int | float | datetime.datetime] = pydantic.Field(default_factory=dict)
    n_quanta: int = 0
    n_datasets: int = 0

    @classmethod
    def from_quantum_graph(cls, quantum_graph: QuantumGraph) -> Self:
        metadata = dict(quantum_graph.metadata)
        return cls(
            pipeline=SerializedPipelineGraph.serialize(quantum_graph.pipeline_graph),
            inputs=list(metadata.pop("input", [])),
            output=metadata.pop("output", None),
            output_run=metadata.pop("output_run"),
            user=metadata.pop("user"),
        )


class RuntimeThinQuantumModel(pydantic.BaseModel):
    quantum_index: QuantumIndex
    data_id: DataCoordinateValues = pydantic.Field(default_factory=list)


class DatasetModel(pydantic.BaseModel):
    dataset_id: uuid.UUID
    dataset_type_name: DatasetTypeName
    data_id: DataCoordinateValues = pydantic.Field(default_factory=list)
    run: str
    exists: bool

    @classmethod
    def from_ref(cls, ref: DatasetRef) -> DatasetModel:
        return cls.model_construct(
            dataset_id=ref.id,
            dataset_type_name=ref.datasetType.name,
            data_id=list(ref.dataId.required_values),
            run=ref.run,
            exists=False,
        )


class RuntimeFullQuantumModel(pydantic.BaseModel):
    quantum_id: uuid.UUID
    task_label: TaskLabel
    data_id: DataCoordinateValues = pydantic.Field(default_factory=list)
    metadata_id: uuid.UUID | None = None
    log_id: uuid.UUID | None = None
    inputs: dict[ConnectionName, list[DatasetModel]] = pydantic.Field(default_factory=dict)
    outputs: dict[ConnectionName, list[DatasetModel]] = pydantic.Field(default_factory=dict)
    datastore_records: dict[DatastoreName, SerializedDatastoreRecordData] = pydantic.Field(
        default_factory=dict
    )

    def iter_dataset_ids(self) -> Iterator[uuid.UUID]:
        if self.metadata_id is not None:
            yield self.metadata_id
        if self.log_id is not None:
            yield self.log_id
        for datasets in itertools.chain(self.inputs.values(), self.outputs.values()):
            for dataset in datasets:
                yield dataset.dataset_id

    @classmethod
    def from_quantum(
        cls, task_node: TaskNode, quantum: Quantum, quantum_id: uuid.UUID
    ) -> RuntimeFullQuantumModel:
        result = cls.model_construct(
            quantum_id=quantum_id,
            task_label=task_node.label,
            data_id=list(cast(DataCoordinate, quantum.dataId).required_values),
        )
        for read_edge in task_node.iter_all_inputs():
            refs = sorted(quantum.inputs[read_edge.dataset_type_name], key=lambda ref: ref.dataId)
            result.inputs[read_edge.connection_name] = [DatasetModel.from_ref(ref) for ref in refs]
        for write_edge in task_node.iter_all_outputs():
            refs = sorted(quantum.outputs[write_edge.dataset_type_name], key=lambda ref: ref.dataId)
            result.inputs[write_edge.connection_name] = [DatasetModel.from_ref(ref) for ref in refs]
        result.datastore_records = {
            store_name: records.to_simple() for store_name, records in quantum.datastore_records.items()
        }
        return result

    @classmethod
    def from_quantum_graph_init(
        cls, task_init_node: TaskInitNode, quantum_graph: QuantumGraph
    ) -> RuntimeFullQuantumModel:
        task_def = quantum_graph.findTaskDefByLabel(task_init_node.label)
        assert task_def is not None
        init_input_refs = {ref.datasetType.name: ref for ref in (quantum_graph.initInputRefs(task_def) or [])}
        init_output_refs = {
            ref.datasetType.name: ref for ref in (quantum_graph.initOutputRefs(task_def) or [])
        }
        init_input_ids = {ref.id for ref in init_input_refs.values()}
        result = cls.model_construct(quantum_id=uuid.uuid4(), task_label=task_init_node.label)
        for read_edge in task_init_node.iter_all_inputs():
            ref = init_input_refs[read_edge.dataset_type_name]
            result.inputs[read_edge.connection_name] = [DatasetModel.from_ref(ref)]
        for write_edge in task_init_node.iter_all_outputs():
            ref = init_output_refs[write_edge.dataset_type_name]
            result.outputs[write_edge.connection_name] = [DatasetModel.from_ref(ref)]
        datastore_records: dict[str, DatastoreRecordData] = {}
        for quantum in quantum_graph.get_task_quanta(task_init_node.label).values():
            for store_name, records in quantum.datastore_records.items():
                subset = records.subset(init_input_ids)
                if subset is not None:
                    datastore_records.setdefault(store_name, DatastoreRecordData()).update(subset)
            break  # All quanta have same init-inputs, so we only need one.
        result.datastore_records = {
            store_name: records.to_simple() for store_name, records in datastore_records.items()
        }
        return result


class ProvenanceQuantumModel(pydantic.BaseModel):
    quantum_id: uuid.UUID
    task_label: TaskLabel
    data_id: DataCoordinateValues = pydantic.Field(default_factory=list)
    metadata_id: uuid.UUID | None = None
    log_id: uuid.UUID | None = None
    status: QuantumRunStatus = QuantumRunStatus.METADATA_MISSING
    caveats: QuantumSuccessCaveats | None = None
    exception: ExceptionInfo | None = None
    start_time: datetime.datetime | None = None
    end_time: datetime.datetime | None = None
    max_rss: float | None = None
    host: str | None = None

    @classmethod
    def from_runtime_quantum(cls, runtime_quantum: RuntimeFullQuantumModel) -> ProvenanceQuantumModel:
        return cls.model_construct(
            quantum_id=runtime_quantum.quantum_id,
            task_label=runtime_quantum.task_label,
            data_id=runtime_quantum.data_id,
            metadata_id=runtime_quantum.inputs[acc.METADATA_OUTPUT_CONNECTION_NAME][0].dataset_id,
            log_id=runtime_quantum.inputs[acc.LOG_OUTPUT_CONNECTION_NAME][0].dataset_id,
        )


class DimensionDataModel(pydantic.RootModel):
    root: dict[DimensionElementName, list[SerializedKeyValueDimensionRecord]] = pydantic.Field(
        default_factory=dict
    )

    @classmethod
    def from_quantum_graph(cls, quantum_graph: QuantumGraph) -> DimensionDataModel:
        universe = quantum_graph.pipeline_graph.universe
        assert universe is not None
        data_ids: defaultdict[DimensionGroup, set[DataCoordinate]] = defaultdict(set)
        for task_node in tqdm.tqdm(
            quantum_graph.pipeline_graph.tasks.values(), "Extracting dimension record data IDs"
        ):
            for quantum in quantum_graph.get_task_quanta(task_node.label).values():
                assert quantum.dataId is not None
                data_ids[quantum.dataId.dimensions].add(quantum.dataId)
                for refs in itertools.chain(quantum.inputs.values(), quantum.outputs.values()):
                    for ref in refs:
                        data_ids[ref.dataId.dimensions].add(ref.dataId)
        all_dimension_names: set[str] = set()
        for task_node in quantum_graph.pipeline_graph.tasks.values():
            all_dimension_names.update(task_node.dimensions.names)
        for dataset_type_node in quantum_graph.pipeline_graph.dataset_types.values():
            all_dimension_names.update(dataset_type_node.dimensions.names)
        all_dimensions = universe.conform(all_dimension_names)
        result = cls()
        for element in tqdm.tqdm(all_dimensions.elements, "Extracting dimension records from data IDs"):
            record_set = DimensionRecordSet(element, universe=universe)
            for data_id_group, data_ids_for_group in data_ids.items():
                if element in data_id_group.elements:
                    record_set.update_from_data_coordinates(data_ids_for_group)
            result.root[element] = record_set.serialize_records()
        return result


class RuntimeThinQuantaModel(pydantic.RootModel):
    root: dict[TaskLabel, list[RuntimeThinQuantumModel]] = pydantic.Field(default_factory=dict)


class QuantumEdgeListModel(pydantic.RootModel):
    root: list[tuple[QuantumIndex, QuantumIndex]] = pydantic.Field(default_factory=list)


class RuntimeInitQuantaModel(pydantic.RootModel):
    root: list[RuntimeFullQuantumModel] = pydantic.Field(default_factory=list)

    def update_from_quantum_graph(self, quantum_graph: QuantumGraph) -> None:
        global_init_quantum = RuntimeFullQuantumModel.model_construct(quantum_id=uuid.uuid4(), task_label="")
        for ref in quantum_graph.globalInitOutputRefs():
            global_init_quantum.outputs[ref.datasetType.name] = [DatasetModel.from_ref(ref)]
        self.root.append(global_init_quantum)
        for task_node in quantum_graph.pipeline_graph.tasks.values():
            self.root.append(RuntimeFullQuantumModel.from_quantum_graph_init(task_node.init, quantum_graph))


class BipartiteEdgeModel(pydantic.BaseModel):
    dataset: DatasetIndex
    quantum: QuantumIndex
    connection: ConnectionName

    @classmethod
    def generate(
        cls,
        quantum_index: QuantumIndex,
        datasets: Mapping[ConnectionName, Iterable[DatasetModel]],
        dataset_indices: Mapping[uuid.UUID, DatasetIndex],
    ) -> Iterator[BipartiteEdgeModel]:
        for connection_name, connection_datasets in datasets.items():
            for dataset in connection_datasets:
                yield cls.model_construct(
                    quantum=quantum_index,
                    dataset=dataset_indices[dataset.dataset_id],
                    connection=connection_name,
                )


class BipartiteEdgeListModel(pydantic.BaseModel):
    reads: list[BipartiteEdgeModel] = pydantic.Field(default_factory=list)
    writes: list[BipartiteEdgeModel] = pydantic.Field(default_factory=list)


@dataclasses.dataclass
class BaseGraph:
    header: HeaderModel
    pipeline_graph: PipelineGraph
    quantum_edges: QuantumEdgeListModel = dataclasses.field(default_factory=QuantumEdgeListModel)
    bipartite_edges: BipartiteEdgeListModel = dataclasses.field(default_factory=BipartiteEdgeListModel)
    quantum_indices: dict[uuid.UUID, QuantumIndex] = dataclasses.field(default_factory=dict)
    dataset_indices: dict[uuid.UUID, DatasetIndex] = dataclasses.field(default_factory=dict)

    def write_full_quanta(self, zf: zipfile.ZipFile, compressor: Compressor, ext: str) -> AddressWriter:
        raise NotImplementedError()

    def write_full_datasets(self, zf: zipfile.ZipFile, compressor: Compressor, ext: str) -> AddressWriter:
        raise NotImplementedError()

    def write(self, uri: ResourcePathExpression, zstd_level: int = 10, int_size: int = 4) -> None:
        import humanize

        if self.header.n_quanta != len(self.quantum_indices):
            raise RuntimeError("Cannot save graph after partial read of quanta.")
        if self.header.n_datasets != len(self.dataset_indices):
            raise RuntimeError("Cannot save graph after partial read of quanta.")

        uri = ResourcePath(uri)
        if zstandard is not None:
            compressor = zstandard.ZstdCompressor(level=zstd_level)
            ext = "zst"
        else:
            compressor = lzma
            ext = "xz"
        with uri.open(mode="wb") as stream:
            with zipfile.ZipFile(stream, mode="w", compression=zipfile.ZIP_STORED) as zf:
                total_size: int = 0
                for field in dataclasses.fields(self):
                    attr = getattr(self, field.name)
                    if isinstance(attr, pydantic.BaseModel):
                        data = attr.model_dump_json().encode()
                        compressed = compressor.compress(data)
                        _LOG.info(
                            f"{field.name}: {humanize.naturalsize(len(data))} -> "
                            f"{humanize.naturalsize(len(compressed))}"
                        )
                        zf.writestr(f"{field.name}.json.{ext}", compressed)
                        total_size += len(compressed)
                quantum_address_writer = self.write_full_quanta(zf, compressor, ext)
                total_size += quantum_address_writer.total
                with zf.open("quantum_addresses.dat", mode="w") as stream:
                    quantum_addresses_size = quantum_address_writer.write(stream, int_size)
                    _LOG.info(f"quantum_addresses: {humanize.naturalsize(quantum_addresses_size)}")
                    total_size += quantum_addresses_size
                dataset_address_writer = self.write_full_datasets(zf, compressor, ext)
                total_size += dataset_address_writer.total
                with zf.open("dataset_addresses.dat", mode="w") as stream:
                    dataset_addresses_size = dataset_address_writer.write(stream, int_size)
                    _LOG.info(f"dataset_addresses: {humanize.naturalsize(dataset_addresses_size)}")
                    total_size += dataset_address_writer.write(stream, int_size)
                _LOG.info(f"unzipped size: {humanize.naturalsize(total_size)}.")
        _LOG.info(f"zipped size: {humanize.naturalsize(uri.size())}.")


@dataclasses.dataclass
class GraphReader:
    @classmethod
    @contextlib.contextmanager
    def open(cls, uri: ResourcePathExpression, preload_all: bool) -> Iterator[Self]:
        uri = ResourcePath(uri)
        with ExitStack() as exit_stack:
            stream: IO[bytes]
            if preload_all:
                with time_this(_LOG, "Reading raw bytes", level=logging.INFO):
                    data = uri.read()
                    stream = BytesIO(data)
            else:
                # Something isn't quite right either in MyPy or ResourcePath's
                # file-like object.
                stream = cast(IO[bytes], exit_stack.enter_context(uri.open(mode="rb")))
            with time_this(_LOG, "Initializing zip file", level=logging.INFO):
                zf = exit_stack.enter_context(zipfile.ZipFile(stream, mode="r"))
            yield cls(zf)

    def __init__(self, zf: zipfile.ZipFile) -> None:
        self.zf = zf
        if zipfile.Path(self.zf, "header.json.zst").exists():
            if zstandard is None:
                raise RuntimeError(f"Cannot read {self.zf.filename} without zstandard.")
            self.decompressor = zstandard.ZstdDecompressor()
            self.ext = "zst"
        elif zipfile.Path(self.zf, "header.json.xz").exists():
            self.decompressor = lzma
            self.ext = "xz"
        else:
            raise RuntimeError(f"{self.zf.filename} does not include the expected quantum graph header.")
        self.header = self.read_model(HeaderModel, "header")
        with time_this(_LOG, "Deserializing pipeline graph", level=logging.INFO):
            self.pipeline_graph = SerializedPipelineGraph.deserialize(
                self.header.pipeline, import_mode=TaskImportMode.DO_NOT_IMPORT
            )

    @contextlib.contextmanager
    def quantum_address_reader(self) -> Iterator[AddressReader]:
        with self.zf.open("quantum_addresses.dat", mode="r") as stream:
            yield AddressReader(stream)

    @contextlib.contextmanager
    def dataset_address_reader(self) -> Iterator[AddressReader]:
        with self.zf.open("dataset_addresses.dat", mode="r") as stream:
            yield AddressReader(stream)

    def read_model(self, cls: type[_T], name: str) -> _T:
        with time_this(_LOG, f"Reading {name}", level=logging.INFO):
            compressed_data = self.zf.read(f"{name}.json.{self.ext}")
        with time_this(_LOG, f"Decompressing {name}", level=logging.INFO):
            data = self.decompressor.decompress(compressed_data)
        with time_this(_LOG, f"Parsing and validating {name}", level=logging.INFO):
            return cls.model_validate_json(data)

    zf: zipfile.ZipFile
    decompressor: Decompressor
    ext: str
    header: HeaderModel
    pipeline_graph: PipelineGraph


@dataclasses.dataclass
class RuntimeGraph(BaseGraph):
    dimension_data: DimensionDataModel = dataclasses.field(default_factory=DimensionDataModel)
    init_quanta: RuntimeInitQuantaModel = dataclasses.field(default_factory=RuntimeInitQuantaModel)
    thin_quanta: RuntimeThinQuantaModel = dataclasses.field(default_factory=RuntimeThinQuantaModel)
    full_quanta: dict[QuantumIndex, RuntimeFullQuantumModel] = dataclasses.field(default_factory=dict)

    @classmethod
    def from_quantum_graph(cls, quantum_graph: QuantumGraph) -> RuntimeGraph:
        header = HeaderModel.from_quantum_graph(quantum_graph)
        dimension_data = DimensionDataModel.from_quantum_graph(quantum_graph)
        result = cls(
            header=header,
            pipeline_graph=quantum_graph.pipeline_graph,
            dimension_data=dimension_data,
        )
        result.init_quanta.update_from_quantum_graph(quantum_graph)
        all_quanta = list(result.init_quanta.root)
        init_quantum_ids = {q.quantum_id for q in result.init_quanta.root}
        for task_node in tqdm.tqdm(result.pipeline_graph.tasks.values(), "Extracting full quanta by task"):
            task_quanta = quantum_graph.get_task_quanta(task_node.label)
            for quantum_id, quantum in tqdm.tqdm(task_quanta.items(), task_node.label, leave=False):
                all_quanta.append(RuntimeFullQuantumModel.from_quantum(task_node, quantum, quantum_id))
            result.thin_quanta.root[task_node.label] = []
        all_quanta.sort(key=lambda q: q.quantum_id.int)
        dataset_ids: set[uuid.UUID] = set()
        for quantum_index, full_quantum in tqdm.tqdm(enumerate(all_quanta), "Adding thin quanta"):
            result.quantum_indices[full_quantum.quantum_id] = quantum_index
            dataset_ids.update(full_quantum.iter_dataset_ids())
            if full_quantum.quantum_id not in init_quantum_ids:
                result.full_quanta[quantum_index] = full_quantum
                result.thin_quanta.root[full_quantum.task_label].append(
                    RuntimeThinQuantumModel(quantum_index=quantum_index, data_id=full_quantum.data_id)
                )
        for dataset_index, dataset_id in tqdm.tqdm(
            enumerate(sorted(dataset_ids, key=attrgetter("int"))), "Setting dataset indices"
        ):
            result.dataset_indices[dataset_id] = dataset_index
        for a, b in tqdm.tqdm(quantum_graph.graph.edges, "Extracting quantum-only graph edges"):
            result.quantum_edges.root.append(
                (result.quantum_indices[a.nodeId], result.quantum_indices[b.nodeId])
            )
        for quantum_index, full_quantum in tqdm.tqdm(
            enumerate(all_quanta), "Extracting bipartite graph edges"
        ):
            result.bipartite_edges.reads.extend(
                BipartiteEdgeModel.generate(quantum_index, full_quantum.inputs, result.dataset_indices)
            )
            result.bipartite_edges.writes.extend(
                BipartiteEdgeModel.generate(quantum_index, full_quantum.outputs, result.dataset_indices)
            )
        result.header.n_quanta = len(result.quantum_indices)
        result.header.n_datasets = len(result.dataset_indices)
        return result

    def write_full_quanta(self, zf: zipfile.ZipFile, compressor: Compressor, ext: str) -> AddressWriter:
        import humanize

        addresses: dict[uuid.UUID, Address] = {}
        with zf.open("full_quanta.dat", mode="w") as stream:
            offset = 0
            for quantum_id, index in tqdm.tqdm(
                self.quantum_indices.items(),
                "Dumping, compressing, and writing full runtime quanta",
            ):
                if (full_quantum := self.full_quanta.get(index)) is not None:
                    model_bytes = compressor.compress(full_quantum.model_dump_json().encode())
                    stream.write(model_bytes)
                    addresses[quantum_id] = Address(index, [offset], [len(model_bytes)])
                    offset += len(model_bytes)
                else:
                    addresses[quantum_id] = Address(index, [offset], [0])
        _LOG.info(f"full_quanta: {humanize.naturalsize(offset)}.")
        return AddressWriter(1, addresses, offset)

    def write_full_datasets(self, zf: zipfile.ZipFile, compressor: Compressor, ext: str) -> AddressWriter:
        return AddressWriter(
            n_offsets=0,
            addresses={
                dataset_id: Address(index, offsets=[], sizes=[])
                for dataset_id, index in self.dataset_indices.items()
            },
            total=0,
        )

    @classmethod
    def read_zip(
        cls,
        uri: ResourcePathExpression,
        *,
        full_quanta: Iterable[uuid.UUID] | None = None,
        read_dimension_data: bool = True,
        read_init_quanta: bool = True,
        read_thin_quanta: bool = True,
        read_quantum_edges: bool = True,
        read_bipartite_edges: bool = True,
        read_quantum_indices: bool = True,
        read_dataset_indices: bool = True,
    ) -> Self:
        if full_quanta is not None:
            full_quanta = set(full_quanta)
        with GraphReader.open(uri, preload_all=(full_quanta is None)) as reader:
            result = cls(header=reader.header, pipeline_graph=reader.pipeline_graph)
            if read_dimension_data:
                result.dimension_data = reader.read_model(DimensionDataModel, "dimension_data")
            if read_init_quanta:
                result.init_quanta = reader.read_model(RuntimeInitQuantaModel, "init_quanta")
            if read_thin_quanta:
                result.thin_quanta = reader.read_model(RuntimeThinQuantaModel, "thin_quanta")
            if read_quantum_edges:
                result.quantum_edges = reader.read_model(QuantumEdgeListModel, "quantum_edges")
            if read_bipartite_edges:
                result.bipartite_edges = reader.read_model(BipartiteEdgeListModel, "bipartite_edges")
            if read_dataset_indices:
                with time_this(_LOG, "Reading dataset indices", level=logging.INFO):
                    with reader.dataset_address_reader() as dataset_address_reader:
                        dataset_addresses = dataset_address_reader.read_all()
                    result.quantum_indices = {
                        dataset_id: address.index for dataset_id, address in dataset_addresses.items()
                    }
            with time_this(_LOG, "Reading addresses for full runtime quanta", level=logging.INFO):
                with reader.quantum_address_reader() as quantum_address_reader:
                    if read_quantum_indices or full_quanta is None:
                        quantum_addresses = quantum_address_reader.read_all()
                    else:
                        quantum_addresses = {
                            quantum_id: quantum_address_reader.find(quantum_id) for quantum_id in full_quanta
                        }
                    if full_quanta is None:
                        full_quanta = quantum_addresses.keys()
                result.quantum_indices = {
                    quantum_id: address.index for quantum_id, address in quantum_addresses.items()
                }
            with time_this(_LOG, f"Reading {len(quantum_addresses)} full runtime quanta", level=logging.INFO):
                compressed_data: dict[uuid.UUID, bytes] = {}
                with reader.zf.open("full_quanta.dat", mode="r") as stream:
                    for quantum_id in full_quanta:
                        address = quantum_addresses[quantum_id]
                        if address.sizes[0]:
                            stream.seek(address.offsets[0])
                            compressed_data[quantum_id] = stream.read(address.sizes[0])
            with time_this(
                _LOG, f"Decompressing {len(quantum_addresses)} full runtime quanta", level=logging.INFO
            ):
                decompressed_data = {
                    quantum_id: reader.decompressor.decompress(data)
                    for quantum_id, data in tqdm.tqdm(
                        compressed_data.items(), "Decompressing full quanta.", leave=False
                    )
                }
            with time_this(
                _LOG, f"Parsing and validating {len(quantum_addresses)} full quanta", level=logging.INFO
            ):
                for quantum_id, data in tqdm.tqdm(
                    decompressed_data.items(),
                    "Parsing and validating full quanta",
                    leave=False,
                ):
                    result.full_quanta[result.quantum_indices[quantum_id]] = (
                        RuntimeFullQuantumModel.model_validate_json(data)
                    )
            return result

    def deserialize(self) -> dict[str, DimensionRecordSetDeserializer]:
        with time_this(_LOG, "Deserializing dimension record data IDs", level=logging.INFO):
            universe = self.pipeline_graph.universe
            assert universe is not None
            dimension_records = {
                element_name: DimensionRecordSetDeserializer.from_raw(universe[element_name], raw_records)
                for element_name, raw_records in self.dimension_data.root.items()
            }
        return dimension_records


@click.group()
def main() -> None:
    pass


@main.command()
@click.argument("uri")
def rewrite(uri: str) -> None:
    import warnings

    import humanize

    logging.basicConfig(level=logging.INFO)

    with time_this(_LOG, msg="Reading original file", level=logging.INFO):
        with warnings.catch_warnings():
            warnings.simplefilter(action="ignore", category=FutureWarning)
            qg = QuantumGraph.loadUri(uri)
    _LOG.info(f"{uri} ({humanize.naturalsize(os.stat(uri).st_size)}. {len(qg)} quanta).")
    with time_this(_LOG, msg="Converting to runtime model", level=logging.INFO):
        runtime_graph = RuntimeGraph.from_quantum_graph(qg)
    basename, _ = os.path.splitext(uri)
    with time_this(_LOG, msg="Writing runtime model to zip.", level=logging.INFO):
        runtime_graph.write(f"{basename}-runtime.zip")


@main.command()
@click.argument("uri")
@click.option("--init-quanta/--no-init-quanta", default=True)
@click.option("--thin-quanta/--no-thin-quanta", default=True)
@click.option("--quantum-edges/--no-quantum-edges", default=True)
@click.option("--bipartite-edges/--no-bipartite-edges", default=True)
@click.option("--quantum-indices/--no-quantum-indices", default=True)
@click.option("--dataset-indices/--no-dataset-indices", default=True)
@click.option("--quantum-id", type=str, multiple=True, default=[])
def read(
    *,
    uri: str,
    init_quanta: bool,
    thin_quanta: bool,
    quantum_edges: bool,
    bipartite_edges: bool,
    quantum_indices: bool,
    dataset_indices: bool,
    quantum_id: list[str],
) -> None:
    logging.basicConfig(level=logging.INFO)
    quanta = [uuid.UUID(i) for i in quantum_id] or None
    with time_this(_LOG, msg=f"Reading {uri}", level=logging.INFO):
        model = RuntimeGraph.read_zip(
            uri,
            read_init_quanta=init_quanta,
            read_thin_quanta=thin_quanta,
            read_quantum_edges=quantum_edges,
            read_bipartite_edges=bipartite_edges,
            read_quantum_indices=quantum_indices,
            read_dataset_indices=dataset_indices,
            full_quanta=quanta,
        )
        model.deserialize()


if __name__ == "__main__":
    main()
