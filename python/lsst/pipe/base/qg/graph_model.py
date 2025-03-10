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

import bz2
import gzip
import itertools
import lzma
import uuid
from collections import defaultdict
from typing import TYPE_CHECKING, Generic, TypeAlias, TypeVar

import pyarrow as pa
import pyarrow.parquet as pq
import pydantic
import tqdm

from lsst.daf.butler import DataCoordinate, DimensionRecordSet, DimensionRecordTable
from lsst.daf.butler.arrow_utils import ToArrow

from ..graph import QuantumGraph
from ..pipeline_graph.io import SerializedPipelineGraph

if TYPE_CHECKING:
    from lsst.daf.butler import DimensionGroup

    from ..pipeline_graph import PipelineGraph


_N = TypeVar("_N")
_E = TypeVar("_E")

NodeDict: TypeAlias = dict[uuid.UUID, list[str | int]]
EdgeList: TypeAlias = list[tuple[uuid.UUID, uuid.UUID]]


class BipartiteGraphModel(pydantic.BaseModel, Generic[_N, _E]):
    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)

    pipeline: SerializedPipelineGraph
    edges: _E
    quanta: dict[str, _N] = pydantic.Field(default_factory=dict)
    datasets: dict[str, _N] = pydantic.Field(default_factory=dict)

    @classmethod
    def from_quantum_graph(cls, quantum_graph: QuantumGraph) -> BipartiteGraphModel[NodeDict, EdgeList]:
        pipeline_graph = quantum_graph.pipeline_graph
        result = cls.model_construct(pipeline=SerializedPipelineGraph.serialize(pipeline_graph), edges=[])
        for dataset_type_node in pipeline_graph.dataset_types.values():
            result.datasets[dataset_type_node.name] = {}
        for task_node in tqdm.tqdm(
            pipeline_graph.tasks.values(), "Constructing bipartite graph", leave=False
        ):
            task_quanta = {}
            result.quanta[task_node.label] = task_quanta
            for quantum_uuid, quantum in quantum_graph.get_task_quanta(task_node.label).items():
                task_quanta[quantum_uuid] = list(quantum.dataId.required_values)
                for dataset_type, refs in quantum.inputs.items():
                    dataset_type_name, _ = dataset_type.splitDatasetTypeName(dataset_type.name)
                    for ref in refs:
                        result.edges.append((ref.id, quantum_uuid))
                        result.datasets[dataset_type_name][ref.id] = list(ref.dataId.required_values)
                for dataset_type, refs in quantum.outputs.items():
                    for ref in refs:
                        result.edges.append((quantum_uuid, ref.id))
                        result.datasets[dataset_type.name][ref.id] = list(ref.dataId.required_values)
        return result

    def with_table_edges(self: BipartiteGraphModel[_N, EdgeList]) -> BipartiteGraphModel[_N, pa.Table]:
        in_list = []
        out_list = []
        in_converter = ToArrow.for_uuid("in", nullable=False)
        out_converter = ToArrow.for_uuid("out", nullable=False)
        for in_uuid, out_uuid in self.edges:
            in_converter.append(in_uuid, in_list)
            out_converter.append(out_uuid, out_list)
        edge_table = pa.table(
            [in_converter.finish(in_list), out_converter.finish(out_list)],
            names=[in_converter.name, out_converter.name],
        )
        return BipartiteGraphModel.model_construct(
            pipeline=self.pipeline, edges=edge_table, quanta=self.quanta, datasets=self.datasets
        )

    def with_table_nodes(
        self: BipartiteGraphModel[NodeDict, _E], pipeline_graph: PipelineGraph
    ) -> BipartiteGraphModel[pa.Table, _E]:
        result = BipartiteGraphModel.model_construct(
            pipeline=self.pipeline, edges=self.edges, quanta={}, datasets={}
        )
        for task_node in pipeline_graph.tasks.values():
            result.quanta[task_node.label] = self._node_dict_to_table(
                self.quanta[task_node.label], task_node.dimensions
            )
        for dataset_type_node in pipeline_graph.dataset_types.values():
            result.datasets[dataset_type_node.name] = self._node_dict_to_table(
                self.datasets[dataset_type_node.name], dataset_type_node.dimensions
            )
        return result

    @staticmethod
    def _node_dict_to_table(node_dict: NodeDict, dimensions: DimensionGroup) -> pa.Table:
        node_id_converter = ToArrow.for_uuid("uuid", nullable=False)
        column_lists = {node_id_converter.name: []}
        converters: list[ToArrow] = []
        for dimension_name in dimensions.required:
            converters.append(dimensions.universe.dimensions[dimension_name].to_arrow(dimensions))
            column_lists[dimension_name] = []
        for node_id, data_id_values in node_dict.items():
            node_id_converter.append(node_id, column_lists[node_id_converter.name])
            for converter, data_id_value in zip(converters, data_id_values):
                converter.append(data_id_value, column_lists[converter.name])
        arrays: dict[str, pa.Array] = {
            node_id_converter.name: node_id_converter.finish(column_lists[node_id_converter.name])
        }
        for converter in converters:
            arrays[converter.name] = converter.finish(column_lists[converter.name])
        return pa.table(list(arrays.values()), names=list(arrays.keys()))


class QuantumOnlyGraphModel(pydantic.BaseModel, Generic[_N, _E]):
    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)

    pipeline: SerializedPipelineGraph
    edges: _E
    quanta: dict[str, _N] = pydantic.Field(default_factory=dict)

    @classmethod
    def from_quantum_graph(cls, quantum_graph: QuantumGraph) -> BipartiteGraphModel[NodeDict, EdgeList]:
        pipeline_graph = quantum_graph.pipeline_graph
        result: BipartiteGraphModel[NodeDict, EdgeList] = cls.model_construct(
            pipeline=SerializedPipelineGraph.serialize(pipeline_graph), edges=[]
        )
        for a, b in tqdm.tqdm(quantum_graph.graph.edges, "Extracting quantum-only graph edges.", leave=False):
            result.edges.append((a.nodeId, b.nodeId))
        for task_node in tqdm.tqdm(
            pipeline_graph.tasks.values(), "Extracting quantum-only nodes.", leave=False
        ):
            task_quanta = {}
            result.quanta[task_node.label] = task_quanta
            for quantum_uuid, quantum in quantum_graph.get_task_quanta(task_node.label).items():
                task_quanta[quantum_uuid] = list(quantum.dataId.required_values)
        return result


def extract_dimension_records(quantum_graph: QuantumGraph) -> dict[str, DimensionRecordSet]:
    universe = quantum_graph.pipeline_graph.universe
    data_ids: defaultdict[DimensionGroup, set[DataCoordinate]] = defaultdict(set)
    for task_node in tqdm.tqdm(
        quantum_graph.pipeline_graph.tasks.values(), "Extracting data IDs.", leave=False
    ):
        for quantum in quantum_graph.get_task_quanta(task_node.label).values():
            data_ids[quantum.dataId.dimensions].add(quantum.dataId)
            for refs in itertools.chain(quantum.inputs.values(), quantum.outputs.values()):
                for ref in refs:
                    data_ids[ref.dataId.dimensions].add(ref.dataId)
    result: dict[str, DimensionRecordSet] = {}
    all_dimension_names: set[str] = set()
    for task_node in quantum_graph.pipeline_graph.tasks.values():
        all_dimension_names.update(task_node.dimensions.names)
    for dataset_type_node in quantum_graph.pipeline_graph.dataset_types.values():
        all_dimension_names.update(dataset_type_node.dimensions.names)
    all_dimensions = universe.conform(all_dimension_names)
    for element in tqdm.tqdm(all_dimensions.elements, "Extracting dimension records.", leave=False):
        records = DimensionRecordSet(element, universe=universe)
        for data_id_group, data_ids_for_group in data_ids.items():
            if element in data_id_group.elements:
                records.update_from_data_coordinates(data_ids_for_group)
        result[element] = records
    return result


def compute_parquet_size(table: pa.Table) -> int:
    writer = pa.BufferOutputStream()
    pq.write_table(table, writer)
    return len(writer.getvalue())


def add_sizes(name: str, json_bytes: bytes, sizes: dict[str, int], extra_compress: bool = False) -> None:
    sizes[f"{name}, uncompressed"] = len(json_bytes)
    sizes[f"{name}, lzma"] = len(lzma.compress(json_bytes))
    if extra_compress:
        sizes[f"{name}, bz2"] = len(bz2.compress(json_bytes))
        sizes[f"{name}, gzip"] = len(gzip.compress(json_bytes))


def compute_storage_costs(quantum_graph: QuantumGraph) -> dict[str, int]:
    sizes: dict[str, int] = {}
    bpg = BipartiteGraphModel.from_quantum_graph(quantum_graph)
    bpg_bytes = bpg.model_dump_json().encode()
    add_sizes("Bipartite graph, JSON", bpg_bytes, sizes)
    qog = QuantumOnlyGraphModel.from_quantum_graph(quantum_graph)
    qog_bytes = qog.model_dump_json().encode()
    add_sizes("Quantum-only graph, JSON", qog_bytes, sizes)
    dimension_records = extract_dimension_records(quantum_graph)
    cached_record_size: int = 0
    for element, record_set in tqdm.tqdm(
        dimension_records.items(), "Computing dimension record table sizes", leave=False
    ):
        record_table = DimensionRecordTable(element, record_set, universe=quantum_graph.universe)
        record_table_size = compute_parquet_size(record_table.to_arrow())
        if quantum_graph.universe[element].is_cached:
            cached_record_size += record_table_size
        else:
            sizes[f"{element} records ({len(record_set)})"] = record_table_size
    sizes["Other dimension records"] = cached_record_size
    return sizes


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
    for k, v in compute_storage_costs(qg).items():
        print(f"{k}: {humanize.naturalsize(v)}")


if __name__ == "__main__":
    _main()
