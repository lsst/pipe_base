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

import datetime
import itertools
import logging
import lzma
import os
import uuid
import zipfile
from collections import defaultdict
from collections.abc import Iterable
from contextlib import ExitStack
from io import BytesIO
from typing import ClassVar, Generic, Self, TypeVar

import click
import pydantic
import tqdm

from lsst.daf.butler import (
    DataCoordinate,
    DataIdValue,
    DimensionGroup,
    DimensionRecordSet,
    DimensionRecordSetDeserializer,
    Quantum,
    SerializedKeyValueDimensionRecord,
)
from lsst.daf.butler.datastore.record_data import DatastoreRecordData, SerializedDatastoreRecordData
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils.timer import time_this

from ..graph import QuantumGraph
from ..pipeline_graph import PipelineGraph, TaskImportMode, TaskNode
from ..pipeline_graph.io import SerializedPipelineGraph

try:
    import zstandard
except ImportError:
    zstandard = None


_LOG = logging.getLogger(__name__)


_T = TypeVar("_T")


class RuntimeDatasetModel(pydantic.BaseModel):
    dataset_id: uuid.UUID
    data_id: list[DataIdValue] = pydantic.Field(default_factory=list)
    run: str


class RuntimeQuantumModel(pydantic.BaseModel):
    task_label: str
    data_id: list[DataIdValue] = pydantic.Field(default_factory=list)
    inputs: dict[str, list[RuntimeDatasetModel]] = pydantic.Field(default_factory=dict)
    outputs: dict[str, list[RuntimeDatasetModel]] = pydantic.Field(default_factory=dict)
    datastore_records: dict[str, SerializedDatastoreRecordData] = pydantic.Field(default_factory=dict)

    @classmethod
    def from_quantum(cls, task_node: TaskNode, quantum: Quantum) -> RuntimeQuantumModel:
        result = cls.model_construct(
            task_label=task_node.label,
            data_id=list(quantum.dataId.required_values),
        )
        for read_edge in task_node.iter_all_inputs():
            refs = sorted(quantum.inputs[read_edge.dataset_type_name], key=lambda ref: ref.dataId)
            result.inputs[read_edge.connection_name] = [
                RuntimeDatasetModel.model_construct(
                    dataset_id=ref.id, data_id=list(ref.dataId.required_values), run=ref.run
                )
                for ref in refs
            ]
        for write_edge in task_node.iter_all_outputs():
            refs = sorted(quantum.outputs[write_edge.dataset_type_name], key=lambda ref: ref.dataId)
            result.inputs[write_edge.connection_name] = [
                RuntimeDatasetModel.model_construct(
                    dataset_id=ref.id, data_id=list(ref.dataId.required_values), run=ref.run
                )
                for ref in refs
            ]
        result.datastore_records = {
            store_name: records.to_simple() for store_name, records in quantum.datastore_records.items()
        }
        return result

    @classmethod
    def from_quantum_graph_init(cls, task_node: TaskNode, quantum_graph: QuantumGraph) -> RuntimeQuantumModel:
        task_def = quantum_graph.findTaskDefByLabel(task_node.label)
        init_input_refs = {ref.datasetType.name: ref for ref in (quantum_graph.initInputRefs(task_def) or [])}
        init_output_refs = {
            ref.datasetType.name: ref for ref in (quantum_graph.initOutputRefs(task_def) or [])
        }
        init_input_ids = {ref.id for ref in init_input_refs.values()}
        result = cls.model_construct(task_label=task_node.label)
        for read_edge in task_node.init.iter_all_inputs():
            ref = init_input_refs[read_edge.dataset_type_name]
            result.inputs[read_edge.connection_name] = [
                RuntimeDatasetModel.model_construct(dataset_id=ref.id, run=ref.run)
            ]
        for write_edge in task_node.init.iter_all_outputs():
            ref = init_output_refs[write_edge.dataset_type_name]
            result.outputs[write_edge.connection_name] = [
                RuntimeDatasetModel.model_construct(dataset_id=ref.id, run=ref.run)
            ]
        datastore_records: dict[str, DatastoreRecordData] = {}
        for quantum in quantum_graph.get_task_quanta(task_node.label).values():
            for store_name, records in quantum.datastore_records.items():
                subset = records.subset(init_input_ids)
                if subset is not None:
                    datastore_records.setdefault(store_name, DatastoreRecordData()).update(subset)
            break  # All quanta have same init-inputs, so we only need one.
        result.datastore_records = {
            store_name: records.to_simple() for store_name, records in datastore_records.items()
        }
        return result


class RuntimeHeaderModel(pydantic.BaseModel):
    version: int = pydantic.Field(default=0)
    pipeline: SerializedPipelineGraph
    inputs: list[str] = pydantic.Field(default_factory=list)
    output: str | None = pydantic.Field(default=None)
    output_run: str
    user: str
    timestamp: datetime.datetime = pydantic.Field(default_factory=datetime.datetime.now)
    init_quanta: list[RuntimeQuantumModel] = pydantic.Field(default_factory=list)
    global_outputs: dict[str, RuntimeDatasetModel] = pydantic.Field(default_factory=dict)
    metadata: dict[str, str | int | float | datetime.datetime] = pydantic.Field(default_factory=dict)

    @classmethod
    def from_quantum_graph(cls, quantum_graph: QuantumGraph) -> RuntimeHeaderModel:
        metadata = dict(quantum_graph.metadata)
        header = RuntimeHeaderModel(
            pipeline=SerializedPipelineGraph.serialize(quantum_graph.pipeline_graph),
            inputs=list(metadata.pop("input", [])),
            output=metadata.pop("output", None),
            output_run=metadata.pop("output_run"),
            user=metadata.pop("user"),
        )
        for task_node in quantum_graph.pipeline_graph.tasks.values():
            header.init_quanta.append(RuntimeQuantumModel.from_quantum_graph_init(task_node, quantum_graph))
        (packages_ref,) = quantum_graph.globalInitOutputRefs()
        header.global_outputs["packages"] = RuntimeDatasetModel(
            dataset_id=packages_ref.id, run=packages_ref.run
        )
        return header


class RuntimeDimensionDataModel(pydantic.RootModel):
    root: dict[str, list[SerializedKeyValueDimensionRecord]] = pydantic.Field(default_factory=dict)

    @classmethod
    def from_quantum_graph(cls, quantum_graph: QuantumGraph) -> RuntimeDimensionDataModel:
        universe = quantum_graph.pipeline_graph.universe
        data_ids: defaultdict[DimensionGroup, set[DataCoordinate]] = defaultdict(set)
        for task_node in tqdm.tqdm(
            quantum_graph.pipeline_graph.tasks.values(), "Extracting dimension record data IDs."
        ):
            for quantum in quantum_graph.get_task_quanta(task_node.label).values():
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
        for element in tqdm.tqdm(all_dimensions.elements, "Extracting dimension records from data IDs."):
            record_set = DimensionRecordSet(element, universe=universe)
            for data_id_group, data_ids_for_group in data_ids.items():
                if element in data_id_group.elements:
                    record_set.update_from_data_coordinates(data_ids_for_group)
            result.root[element] = record_set.serialize_records()
        return result


class RuntimeGraphModelBase(pydantic.BaseModel, Generic[_T]):
    header: RuntimeHeaderModel
    dimension_data: RuntimeDimensionDataModel
    thin_quanta: dict[str, dict[uuid.UUID, list[str | int]]]
    quantum_edges: list[tuple[_T, _T]]
    runtime_quanta: dict[_T, RuntimeQuantumModel]

    thin_quanta_adapter: ClassVar[pydantic.TypeAdapter[dict[str, dict[uuid.UUID, list[str | int]]]]] = (
        pydantic.TypeAdapter(dict[str, dict[uuid.UUID, list[str | int]]])
    )
    quantum_edges_adapter: ClassVar[pydantic.TypeAdapter[list[tuple[_T, _T]]]]

    def make_uuid_to_int_map(self) -> dict[uuid.UUID, int]:
        uuid_to_int: dict[uuid.UUID, int] = {}
        n = 0
        for quanta in self.thin_quanta.values():
            for quantum_id in quanta.keys():
                uuid_to_int[quantum_id] = n
                n += 1
        return uuid_to_int

    def runtime_quantum_filename(self, key: _T, ext: str) -> str:
        raise NotImplementedError()

    def get_quantum_key(self, quantum_id: uuid.UUID) -> _T:
        raise NotImplementedError()

    def get_all_quanta(self) -> Iterable[uuid.UUID]:
        raise NotImplementedError()

    def write(self, uri: ResourcePathExpression, zstd_level: int = 10) -> None:
        import humanize

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

                def write_and_report(name: str, data: bytes) -> int:
                    compressed = compressor.compress(data)
                    _LOG.info(
                        f"{name}: {humanize.naturalsize(len(data))} -> "
                        f"{humanize.naturalsize(len(compressed))}"
                    )
                    zf.writestr(name, compressed)
                    return len(compressed)

                total_size += write_and_report(f"header.json.{ext}", self.header.model_dump_json().encode())
                total_size += write_and_report(
                    f"dimension_data.json.{ext}", self.dimension_data.model_dump_json().encode()
                )
                total_size += write_and_report(
                    f"thin_quanta.json.{ext}", self.thin_quanta_adapter.dump_json(self.thin_quanta)
                )
                total_size += write_and_report(
                    f"quantum_edges.json.{ext}", self.quantum_edges_adapter.dump_json(self.quantum_edges)
                )
                zf.mkdir("runtime_quanta")
                runtime_quanta_size_uncompressed: int = 0
                runtime_quanta_size_compressed: int = 0
                for quantum_key, quantum_model in tqdm.tqdm(
                    self.runtime_quanta.items(), "Writing runtime quanta."
                ):
                    data = quantum_model.model_dump_json().encode()
                    compressed = compressor.compress(data)
                    runtime_quanta_size_uncompressed += len(data)
                    runtime_quanta_size_compressed += len(compressed)
                    zf.writestr(
                        os.path.join("runtime_quanta", self.runtime_quantum_filename(quantum_key, ext)),
                        compressed,
                    )
                total_size += runtime_quanta_size_compressed
                _LOG.info(
                    f"runtime_quanta: {humanize.naturalsize(runtime_quanta_size_uncompressed)} -> "
                    f"{humanize.naturalsize(runtime_quanta_size_compressed)}."
                )
                _LOG.info(f"unzipped size: {humanize.naturalsize(total_size)}.")
        _LOG.info(f"zipped size: {humanize.naturalsize(uri.size())}.")

    @classmethod
    def read_zip(
        cls,
        uri: ResourcePathExpression,
        *,
        quanta: Iterable[uuid.UUID] | None = None,
        read_quantum_edges: bool = True,
        read_thin_quanta: bool = True,
    ) -> Self:
        uri = ResourcePath(uri)
        with ExitStack() as exit_stack:
            if quanta is None:
                read_thin_quanta = True
                with time_this(_LOG, "Reading raw bytes", level=logging.INFO):
                    data = uri.read()
                    stream = BytesIO(data)
            else:
                stream = exit_stack.enter_context(uri.open(mode="rb"))
            with time_this(_LOG, "Initializing zip file", level=logging.INFO):
                zf = exit_stack.enter_context(zipfile.ZipFile(stream, mode="r"))
            with time_this(_LOG, "Reading header", level=logging.INFO):
                if zipfile.Path(zf, "header.json.zst").exists():
                    if zstandard is None:
                        raise RuntimeError(f"Cannot read {zf.filename} without zstandard.")
                    decompressor = zstandard.ZstdDecompressor()
                    ext = "zst"
                elif zipfile.Path(zf, "header.json.xz").exists():
                    decompressor = lzma
                    ext = "xz"
                else:
                    raise RuntimeError(f"{zf.filename} does not include the expected quantum graph header.")

                def read_decompressed(name: str) -> bytes:
                    return decompressor.decompress(zf.read(name))

                header = RuntimeHeaderModel.model_validate_json(read_decompressed(f"header.json.{ext}"))
            with time_this(_LOG, "Reading dimension data", level=logging.INFO):
                dimension_data = RuntimeDimensionDataModel.model_validate_json(
                    read_decompressed(f"dimension_data.json.{ext}")
                )
            with time_this(_LOG, "Reading dimension data", level=logging.INFO):
                dimension_data = RuntimeDimensionDataModel.model_validate_json(
                    read_decompressed(f"dimension_data.json.{ext}")
                )
            if read_thin_quanta:
                with time_this(_LOG, "Reading thin quanta", level=logging.INFO):
                    thin_quanta = cls.thin_quanta_adapter.validate_json(
                        read_decompressed(f"thin_quanta.json.{ext}")
                    )
            else:
                thin_quanta = {}
            if read_quantum_edges:
                with time_this(_LOG, "Reading quantum edges", level=logging.INFO):
                    quantum_edges = cls.quantum_edges_adapter.validate_json(
                        read_decompressed(f"quantum_edges.json.{ext}")
                    )
            else:
                quantum_edges = []
            result = cls.model_construct(
                header=header,
                dimension_data=dimension_data,
                thin_quanta=thin_quanta,
                quantum_edges=quantum_edges,
                runtime_quanta={},
            )
            if quanta is None:
                quanta = result.get_all_quanta()
            with time_this(_LOG, f"Reading {len(quanta)} runtime quanta", level=logging.INFO):
                runtime_quanta_data = {
                    quantum_id: zf.read(
                        os.path.join(
                            "runtime_quanta",
                            result.runtime_quantum_filename(result.get_quantum_key(quantum_id), ext),
                        )
                    )
                    for quantum_id in tqdm.tqdm(quanta, "Reading runtime quanta.", leave=False)
                }
            with time_this(_LOG, f"Decompressing {len(quanta)} runtime quanta", level=logging.INFO):
                runtime_quanta_decompressed = {
                    quantum_id: decompressor.decompress(data)
                    for quantum_id, data in tqdm.tqdm(
                        runtime_quanta_data.items(), "Decompressing runtime quanta.", leave=False
                    )
                }
            with time_this(_LOG, f"Parsing and validating {len(quanta)} runtime quanta", level=logging.INFO):
                for quantum_id, data in tqdm.tqdm(
                    runtime_quanta_decompressed.items(),
                    "Parsing and validating runtime quanta.",
                    leave=False,
                ):
                    result.runtime_quanta[quantum_id] = RuntimeQuantumModel.model_validate_json(data)

            return result

    def deserialize(self) -> tuple[PipelineGraph, dict[str, DimensionRecordSetDeserializer]]:
        with time_this(_LOG, "Deserializing pipeline graph", level=logging.INFO):
            pipeline_graph = SerializedPipelineGraph.deserialize(
                self.header.pipeline, import_mode=TaskImportMode.DO_NOT_IMPORT
            )
        with time_this(_LOG, "Deserializing dimension record data IDs", level=logging.INFO):
            dimension_records = {
                element_name: DimensionRecordSetDeserializer.from_raw(
                    pipeline_graph.universe[element_name], raw_records
                )
                for element_name, raw_records in self.dimension_data.root.items()
            }
        return pipeline_graph, dimension_records


class RuntimeGraphModelUUID(RuntimeGraphModelBase[uuid.UUID]):
    quantum_edges_adapter: ClassVar[pydantic.TypeAdapter[list[tuple[uuid.UUID, uuid.UUID]]]] = (
        pydantic.TypeAdapter(list[tuple[uuid.UUID, uuid.UUID]])
    )

    _all_uuids: list[uuid.UUID]

    def model_post_init(self, __context):
        super().model_post_init(__context)
        self._all_uuids = []
        for quanta in self.thin_quanta.values():
            for quantum_id in quanta.keys():
                self._all_uuids.append(quantum_id)

    @classmethod
    def from_quantum_graph(cls, quantum_graph: QuantumGraph) -> RuntimeGraphModelUUID:
        header = RuntimeHeaderModel.from_quantum_graph(quantum_graph)
        dimension_data = RuntimeDimensionDataModel.from_quantum_graph(quantum_graph)
        thin_quanta = {}
        for task_node in tqdm.tqdm(
            quantum_graph.pipeline_graph.tasks.values(), "Extracting quantum-only nodes.", leave=False
        ):
            task_quanta = {}
            thin_quanta[task_node.label] = task_quanta
            for quantum_uuid, quantum in quantum_graph.get_task_quanta(task_node.label).items():
                task_quanta[quantum_uuid] = list(quantum.dataId.required_values)
        quantum_edges = [
            (a.nodeId, b.nodeId)
            for a, b in tqdm.tqdm(quantum_graph.graph.edges, "Extracting quantum-only graph edges.")
        ]
        runtime_quanta = {
            node.nodeId: RuntimeQuantumModel.from_quantum(
                quantum_graph.pipeline_graph.tasks[node.taskDef.label], node.quantum
            )
            for node in tqdm.tqdm(quantum_graph, "Extracting runtime quanta.")
        }
        return cls.model_construct(
            header=header,
            dimension_data=dimension_data,
            thin_quanta=thin_quanta,
            quantum_edges=quantum_edges,
            runtime_quanta=runtime_quanta,
        )

    def with_integer_ids(self) -> RuntimeGraphModelInt:
        uuid_to_int = self.make_uuid_to_int_map()
        quantum_edges = [(uuid_to_int[a], uuid_to_int[b]) for a, b in self.quantum_edges]
        runtime_quanta = {uuid_to_int[k]: v for k, v in self.runtime_quanta.items()}
        return RuntimeGraphModelInt.model_construct(
            header=self.header,
            dimension_data=self.dimension_data,
            thin_quanta=self.thin_quanta,
            quantum_edges=quantum_edges,
            runtime_quanta=runtime_quanta,
        )

    def runtime_quantum_filename(self, key: uuid.UUID, ext: str) -> str:
        return f"{key.hex}.json.{ext}"

    def get_quantum_key(self, quantum_id: uuid.UUID) -> uuid.UUID:
        return quantum_id

    def get_all_quanta(self) -> Iterable[uuid.UUID]:
        return self._all_uuids


class RuntimeGraphModelInt(RuntimeGraphModelBase[int]):
    quantum_edges_adapter: ClassVar[pydantic.TypeAdapter[list[tuple[int, int]]]] = pydantic.TypeAdapter(
        list[tuple[int, int]]
    )

    _uuid_to_int: dict[uuid.UUID, int]

    def model_post_init(self, __context):
        super().model_post_init(__context)
        self._uuid_to_int = {}
        n = 0
        for quanta in self.thin_quanta.values():
            for quantum_id in quanta.keys():
                self._uuid_to_int[quantum_id] = n
                n += 1

    def runtime_quantum_filename(self, key: int, ext: str) -> str:
        return f"{key:08d}.json.{ext}"

    def get_quantum_key(self, quantum_id: uuid.UUID) -> int:
        return self._uuid_to_int[quantum_id]

    def get_all_quanta(self) -> Iterable[uuid.UUID]:
        return self._uuid_to_int.keys()


@click.group()
def main():
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
        runtime_model = RuntimeGraphModelUUID.from_quantum_graph(qg)
    basename, _ = os.path.splitext(uri)
    with time_this(_LOG, msg="Writing UUID runtime model to zip.", level=logging.INFO):
        runtime_model.write(f"{basename}-uuid.zip")
    with time_this(_LOG, msg="Writing int runtime model to zip.", level=logging.INFO):
        runtime_model.with_integer_ids().write(f"{basename}-int.zip")


@main.command()
@click.argument("uri")
@click.option("--integers/--uuids", default=False)
@click.option("--thin-quanta/--no-thin-quanta", default=True)
@click.option("--quantum-edges/--no-quantum-edges", default=True)
@click.option("--quantum-id", type=str, multiple=True, default=[])
def read(
    *,
    uri: str,
    integers: bool,
    thin_quanta: bool,
    quantum_edges: bool,
    quantum_id: list[str],
) -> None:
    logging.basicConfig(level=logging.INFO)
    cls = RuntimeGraphModelInt if integers else RuntimeGraphModelUUID
    quanta = [uuid.UUID(i) for i in quantum_id] or None
    with time_this(_LOG, msg=f"Reading {uri}", level=logging.INFO):
        model = cls.read_zip(
            uri, read_thin_quanta=thin_quanta, read_quantum_edges=quantum_edges, quanta=quanta
        )
        model.deserialize()


if __name__ == "__main__":
    main()
