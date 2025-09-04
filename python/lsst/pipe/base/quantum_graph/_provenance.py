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

import dataclasses
import logging
import uuid
from collections.abc import Iterable
from typing import TypeVar

import networkx
import pydantic

from lsst.daf.butler import DataCoordinate

from .._status import QuantumSuccessCaveats
from ..pipeline_graph import NodeBipartite
from ..quantum_provenance_graph import ExceptionInfo, QuantumRunStatus
from ._common import (
    BaseQuantumGraphReader,
    ConnectionName,
    DataCoordinateValues,
    DatasetIndex,
    QuantumIndex,
    TaskLabel,
)
from ._multiblock import (
    MultiblockReader,
)
from ._predicted import PredictedDatasetModel, PredictedQuantumDatasetsModel

_LOG = logging.getLogger(__name__)


_T = TypeVar("_T", bound=pydantic.BaseModel)


class ProvenanceDatasetModel(PredictedDatasetModel):
    exists: bool
    producer: uuid.UUID | None = None

    @classmethod
    def from_predicted(
        cls, predicted: PredictedDatasetModel, producer: uuid.UUID | None = None
    ) -> ProvenanceDatasetModel:
        return cls.model_construct(
            dataset_id=predicted.dataset_id,
            dataset_type_name=predicted.dataset_type_name,
            data_coordinate=predicted.data_coordinate,
            run=predicted.run,
            exists=(producer is None),  # if it's not produced by this QG, it's an overall input
            producer=producer,
        )


class ProvenanceQuantumModel(pydantic.BaseModel):
    quantum_id: uuid.UUID
    task_label: TaskLabel
    data_coordinate: DataCoordinateValues = pydantic.Field(default_factory=list)
    status: QuantumRunStatus = QuantumRunStatus.METADATA_MISSING
    caveats: QuantumSuccessCaveats | None = None
    exception: ExceptionInfo | None = None

    @classmethod
    def from_predicted(cls, predicted: PredictedQuantumDatasetsModel) -> ProvenanceQuantumModel:
        return cls(
            quantum_id=predicted.quantum_id,
            task_label=predicted.task_label,
            data_coordinate=predicted.data_coordinate,
        )


class ProvenanceInitQuantumModel(pydantic.BaseModel):
    quantum_id: uuid.UUID
    quantum_index: QuantumIndex
    task_label: TaskLabel

    @classmethod
    def from_predicted(
        cls, predicted: PredictedQuantumDatasetsModel, quantum_index: QuantumIndex
    ) -> ProvenanceInitQuantumModel:
        return cls(
            quantum_id=predicted.quantum_id,
            task_label=predicted.task_label,
            quantum_index=quantum_index,
        )


class ProvenanceInitQuantaModel(pydantic.RootModel):
    root: list[ProvenanceInitQuantumModel] = pydantic.Field(default_factory=list)


class BipartiteEdgeModel(pydantic.BaseModel):
    dataset: DatasetIndex
    quantum: QuantumIndex
    connection: ConnectionName

    def update_xgraph(self, xgraph: networkx.MultiDiGraph, is_read: bool) -> None:
        if is_read:
            xgraph.add_edge(self.dataset, self.quantum, self.connection)
        else:
            xgraph.add_edge(self.quantum, self.dataset, self.connection)
        dataset_info = xgraph.nodes[self.dataset]
        dataset_info["bipartite"] = NodeBipartite.DATASET_OR_TYPE
        quantum_info = xgraph.nodes[self.quantum]
        quantum_info["bipartite"] = NodeBipartite.TASK_OR_QUANTUM


class BipartiteEdgeListModel(pydantic.BaseModel):
    init_reads: list[BipartiteEdgeModel] = pydantic.Field(default_factory=list)
    init_writes: list[BipartiteEdgeModel] = pydantic.Field(default_factory=list)
    reads: list[BipartiteEdgeModel] = pydantic.Field(default_factory=list)
    writes: list[BipartiteEdgeModel] = pydantic.Field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(self.reads or self.writes or self.init_reads or self.init_writes)


@dataclasses.dataclass
class ProvenanceQuantumGraphReader(BaseQuantumGraphReader):
    """A helper class for reading provenance quantum graphs."""

    init_bipartite_xgraph: networkx.MultiDiGraph = dataclasses.field(default_factory=networkx.MultiDiGraph)
    """A bipartite graph of init-output datasets and the special "init quanta"
    that produce them, populated automatically as they are loaded.

    Unlike the NetworkX graph properties of `BaseQuantumGraph` instances, this
    one uses integer quantum and dataset indexes as keys, with "quantum_id"
    and "dataset_id" node attributes for the UUIDs.
    """

    bipartite_xgraph: networkx.MultiDiGraph = dataclasses.field(default_factory=networkx.MultiDiGraph)
    """A bipartite graph of datasets and quanta, populated automatically as
    they are loaded.

    Unlike the NetworkX graph properties of `BaseQuantumGraph` instances, this
    one uses integer quantum and dataset indexes as keys, with "quantum_id"
    and "dataset_id" node attributes for the UUIDs.
    """

    indices: dict[uuid.UUID, QuantumIndex | DatasetIndex] = dataclasses.field(default_factory=dict)

    def read_bipartite_edges(self) -> ProvenanceQuantumGraphReader:
        bipartite_edges = self._read_single_block("bipartite_edges", BipartiteEdgeListModel)
        for edge in bipartite_edges.init_reads:
            edge.update_xgraph(self.init_bipartite_xgraph, is_read=True)
        for edge in bipartite_edges.init_writes:
            edge.update_xgraph(self.init_bipartite_xgraph, is_read=False)
        for edge in bipartite_edges.reads:
            edge.update_xgraph(self.bipartite_xgraph, is_read=True)
        for edge in bipartite_edges.writes:
            edge.update_xgraph(self.bipartite_xgraph, is_read=False)
        return self

    def read_init_quanta(self) -> ProvenanceQuantumGraphReader:
        """Read the list of special quanta that represent init-inputs and
        init-outputs.
        """
        empty_data_id = DataCoordinate.make_empty(self.pipeline_graph.universe)
        init_quanta = self._read_single_block("init_quanta", ProvenanceInitQuantaModel)
        for quantum in init_quanta.root:
            info = self.init_bipartite_xgraph.nodes[quantum.quantum_index]
            info["task_label"] = quantum.task_label
            info["quantum_id"] = quantum.quantum_id
            info["data_id"] = empty_data_id
            info["loaded"] = True
        return self

    def read_quanta(self, quantum_ids: Iterable[uuid.UUID] | None = None) -> ProvenanceQuantumGraphReader:
        """Read information about the given quanta and/or datasets.

        Parameters
        ----------
        quantum_ids : `~collections.abc.Iterable` [ `uuid.UUID` ], optional
            Iterable of quantum IDs to load.  If not provided, all quanta will
            be loaded.  The UUIDs of special init quanta will be ignored.
        """
        if quantum_ids is None:
            self.address_reader.read_all()
            quantum_ids = self.address_reader.rows.keys()
        with MultiblockReader.open_in_zip(self.zf, "quanta", self.header.int_size) as mb_reader:
            for quantum_id in quantum_ids:
                if quantum_id in self.indices:
                    continue
                address_row = self.address_reader.find(quantum_id)
                quantum = mb_reader.read_model(
                    address_row.addresses[0], ProvenanceQuantumModel, self.decompressor
                )
                if quantum is not None:
                    self.indices[address_row.key] = address_row.index
                    info = self.bipartite_xgraph.nodes[address_row.index]
                    info["quantum_id"] = quantum.quantum_id
                    info["task_label"] = quantum.task_label
                    task_node = self.pipeline_graph.tasks[quantum.task_label]
                    info["task_node"] = task_node
                    info["data_id"] = DataCoordinate.from_full_values(
                        task_node.dimensions, tuple(quantum.data_coordinate)
                    )
                    info["status"] = quantum.status
                    info["caveats"] = quantum.caveats
                    info["exception"] = quantum.exception
                    info["bipartite"] = NodeBipartite.TASK_OR_QUANTUM
        return self

    def read_datasets(self, dataset_ids: Iterable[uuid.UUID] | None = None) -> ProvenanceQuantumGraphReader:
        """Read information about the given datasets.

        Parameters
        ----------
        dataset_ids : `~collections.abc.Iterable` [ `uuid.UUID` ], optional
            Iterable of dataste IDs to load.  If not provided, all datasets
            will be loaded.  The UUIDs of special init quanta will be ignored.
        """
        if dataset_ids is None:
            self.address_reader.read_all()
            dataset_ids = self.address_reader.rows.keys()
        with MultiblockReader.open_in_zip(self.zf, "datasets", self.header.int_size) as mb_reader:
            for dataset_id in dataset_ids:
                if dataset_id in self.indices:
                    continue
                address_row = self.address_reader.find(dataset_id)
                dataset = mb_reader.read_model(
                    address_row.addresses[1], ProvenanceDatasetModel, self.decompressor
                )
                if dataset is not None:
                    self.indices[address_row.key] = address_row.index
                    info = self.init_bipartite_xgraph.nodes[address_row.index]
                    info["dataset_id"] = dataset.dataset_id
                    dt_node = self.pipeline_graph.dataset_types[dataset.dataset_type_name]
                    info["dataset_type"] = dt_node.dataset_type
                    info["run"] = dataset.run
                    info["data_id"] = DataCoordinate.from_full_values(
                        dt_node.dimensions, tuple(dataset.data_coordinate)
                    )
                    info["exists"] = dataset.exists
                    info["producer"] = dataset.producer
                    info["bipartite"] = NodeBipartite.DATASET_OR_TYPE
        return self
