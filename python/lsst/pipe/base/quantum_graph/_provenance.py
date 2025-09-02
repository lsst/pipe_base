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

import logging
import uuid
from collections.abc import Iterable, Iterator, Mapping

import pydantic

from .._status import QuantumSuccessCaveats
from ..quantum_provenance_graph import ExceptionInfo, QuantumRunStatus
from ._common import ConnectionName, DataCoordinateValues, DatasetIndex, QuantumIndex, TaskLabel
from ._predicted import PredictedDatasetModel, PredictedQuantumDatasetsModel

_LOG = logging.getLogger(__name__)


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


class ProvenanceInitQuantaModel(pydantic.RootModel):
    root: list[ProvenanceQuantumModel] = pydantic.Field(default_factory=list)


class BipartiteEdgeModel(pydantic.BaseModel):
    dataset: DatasetIndex
    quantum: QuantumIndex
    connection: ConnectionName
    ignored: bool = False

    @classmethod
    def generate(
        cls,
        quantum_index: QuantumIndex,
        datasets: Mapping[ConnectionName, Iterable[PredictedDatasetModel]],
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
    init_reads: list[BipartiteEdgeModel] = pydantic.Field(default_factory=list)
    init_writes: list[BipartiteEdgeModel] = pydantic.Field(default_factory=list)
    reads: list[BipartiteEdgeModel] = pydantic.Field(default_factory=list)
    writes: list[BipartiteEdgeModel] = pydantic.Field(default_factory=list)
