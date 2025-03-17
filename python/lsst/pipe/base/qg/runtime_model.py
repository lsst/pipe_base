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
import lzma
import uuid
import zipfile
from collections import defaultdict
from collections.abc import Iterable

import pyarrow as pa
import pyarrow.parquet as pq
import pydantic
import tqdm

from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.daf.butler import (
    DataCoordinate,
    DataIdValue,
    DimensionGroup,
    DimensionRecordSet,
    Quantum,
    SerializedDimensionRecord,
)
from lsst.daf.butler.datastore.record_data import DatastoreRecordData, SerializedDatastoreRecordData

from ..graph import QuantumGraph
from ..pipeline_graph import TaskNode
from ..pipeline_graph.io import SerializedPipelineGraph
from .graph_model import QuantumOnlyGraphModel


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
        result = cls.model_construct(task_lable=task_node.label)
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
    def from_quantum_graph(cls, quantum_graph: QuantumGraph) -> RuntimeGraphModel:
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
    root: dict[str, list[SerializedDimensionRecord]] = pydantic.Field(default_factory=dict)

    @classmethod
    def from_quantum_graph(cls, quantum_graph: QuantumGraph) -> RuntimeDimensionDataModel:
        universe = quantum_graph.pipeline_graph.universe
        data_ids: defaultdict[DimensionGroup, set[DataCoordinate]] = defaultdict(set)
        for task_node in tqdm.tqdm(
            quantum_graph.pipeline_graph.tasks.values(), "Extracting dimension record data IDs.", leave=False
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
        for element in tqdm.tqdm(
            all_dimensions.elements, "Extracting dimension records from data IDs.", leave=False
        ):
            record_set = DimensionRecordSet(element, universe=universe)
            for data_id_group, data_ids_for_group in data_ids.items():
                if element in data_id_group.elements:
                    record_set.update_from_data_coordinates(data_ids_for_group)
            result.root[element] = [r.to_simple() for r in record_set]
        return result


class RuntimeGraphModel(pydantic.BaseModel):
    header: RuntimeHeaderModel
    graph: QuantumOnlyGraphModel | None
    dimension_data: RuntimeDimensionDataModel
    quanta: dict[uuid.UUID, RuntimeQuantumModel]

    @classmethod
    def from_quantum_graph(cls, quantum_graph: QuantumGraph) -> RuntimeGraphModel:
        header = RuntimeHeaderModel.from_quantum_graph(quantum_graph)
        graph = QuantumOnlyGraphModel.from_quantum_graph(quantum_graph)
        dimension_data = RuntimeDimensionDataModel.from_quantum_graph(quantum_graph)
        quanta = {
            node.nodeId: RuntimeQuantumModel.from_quantum(
                quantum_graph.pipeline_graph.tasks[node.taskDef.label], node.quantum
            )
            for node in tqdm.tqdm(quantum_graph, "Extracting runtime quanta.", leave=False)
        }
        return cls(
            header=header,
            graph=graph,
            dimension_data=dimension_data,
            quanta=quanta,
        )

    def print_storage_costs(self) -> None:
        copy1 = self.model_copy()
        copy1.quanta = {}
        copy2 = self.model_copy()
        copy2.quanta = {}
        copy2.graph = None
        print_json_sizes("Header", [self.header])
        print_json_sizes("Dimension Records", [self.dimension_data])
        print_json_sizes("Progressive Header", [copy2])
        print_json_sizes("Quantum-Only Graph", [self.graph])
        print_json_sizes("Consolidated Header", [copy1])
        print_json_sizes(
            "Quantum Models", tqdm.tqdm(self.quanta.values(), "Computing quantum model sizes", leave=False)
        )

    def write_zip(self, uri: ResourcePathExpression) -> None:
        uri = ResourcePath(uri)
        with uri.open(mode="wb") as stream:
            with zipfile.ZipFile(stream, mode="w", compression=zipfile.ZIP_LZMA) as zip:
                zip.writestr("header.json", self.header.model_dump_json())
                zip.writestr("dimension_data.json", self.dimension_data.model_dump_json())
                zip.writestr("graph.json", self.graph.model_dump_json())
                zip.mkdir("quanta")
                for quantum_uuid, quantum_model in tqdm.tqdm(
                    self.quanta.items(), "Writing quanta.", leave=False
                ):
                    zip.writestr(f"quanta/{quantum_uuid.hex}.json", quantum_model.model_dump_json())


def compute_parquet_size(table: pa.Table) -> int:
    writer = pa.BufferOutputStream()
    pq.write_table(table, writer)
    return len(writer.getvalue())


def print_json_sizes(name: str, models: Iterable[pydantic.BaseModel]) -> None:
    import humanize

    uncompressed_size: int = 0
    lzma_size: int = 0
    n = 0
    for model in models:
        json_bytes = model.model_dump_json().encode()
        uncompressed_size += len(json_bytes)
        lzma_size += len(lzma.compress(json_bytes))
        n += 1
    print(f"{name} ({n}), uncompressed JSON: {humanize.naturalsize(uncompressed_size)}.")
    print(f"{name} ({n}), lzma-compressed JSON: {humanize.naturalsize(lzma_size)}.")


def _main():
    import os
    import sys
    import warnings

    import humanize

    filename = sys.argv[1]
    with warnings.catch_warnings():
        warnings.simplefilter(action="ignore", category=FutureWarning)
        qg = QuantumGraph.loadUri(filename)
    print(f"{filename} ({humanize.naturalsize(os.stat(filename).st_size)}).")
    runtime_model = RuntimeGraphModel.from_quantum_graph(qg)
    basename, _ = os.path.splitext(filename)
    runtime_model.write_zip(f"{basename}.zip")


if __name__ == "__main__":
    _main()
