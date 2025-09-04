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
import enum
import uuid

from ... import automatic_connection_constants as acc
from ..._status import QuantumSuccessCaveats
from ...quantum_provenance_graph import ExceptionInfo, QuantumRunStatus
from .._multiblock import Compressor
from .._predicted import (
    PredictedQuantumDatasetsModel,
)
from .._provenance import ProvenanceQuantumModel
from . import db
from ._config import ScannerTimeConfigDict


@dataclasses.dataclass
class DatasetScanResult:
    dataset_id: uuid.UUID
    producer: uuid.UUID | None
    exists: bool = False
    provenance: bytes = b""

    def to_db(self) -> db.Dataset:
        return db.Dataset(
            dataset_id=self.dataset_id, exists=self.exists, provenance=self.provenance, producer=self.producer
        )


@dataclasses.dataclass
class MetadataScanResult(DatasetScanResult):
    content: bytes = b""
    exception: ExceptionInfo | None = None
    caveats: QuantumSuccessCaveats | None = None
    ids_put: set[uuid.UUID] | None = None

    def __bool__(self) -> bool:
        return bool(self.content)


@dataclasses.dataclass
class LogScanResult(DatasetScanResult):
    content: bytes = b""

    def __bool__(self) -> bool:
        return bool(self.content)


class QuantumScanStatus(enum.Enum):
    INCOMPLETE = enum.auto()
    ABANDONED = enum.auto()
    SUCCESSFUL = enum.auto()
    FAILED = enum.auto()
    BLOCKED = enum.auto()


@dataclasses.dataclass
class QuantumScanResult:
    predicted: PredictedQuantumDatasetsModel
    metadata: MetadataScanResult = dataclasses.field(default=MetadataScanResult)
    log: LogScanResult = dataclasses.field(default=LogScanResult)
    provenance: bytes = b""
    status: QuantumScanStatus = QuantumScanStatus.INCOMPLETE
    outputs: list[DatasetScanResult] | None = None
    wait_interval: float | None = None
    first_failure_time: float | None = None

    @classmethod
    def from_predicted(cls, predicted: PredictedQuantumDatasetsModel) -> QuantumScanResult:
        (predicted_metadata,) = predicted.outputs[acc.METADATA_OUTPUT_CONNECTION_NAME]
        (predicted_log,) = predicted.outputs[acc.LOG_OUTPUT_CONNECTION_NAME]
        return cls(
            predicted,
            MetadataScanResult(dataset_id=predicted_metadata.dataset_id, producer=predicted.quantum_id),
            LogScanResult(dataset_id=predicted_log.dataset_id, producer=predicted.quantum_id),
        )

    def to_db(self) -> db.Quantum:
        if self.status is QuantumScanStatus.SUCCESSFUL:
            successful = True
        else:
            assert self.status is QuantumScanStatus.FAILED, (
                f"Cannot write incomplete scan for {self.quantum_id} with status {self.status}."
            )
            successful = False
        return db.Quantum(
            quantum_id=self.quantum_id,
            successful=successful,
            provenance=self.provenance,
            log_id=self.log.dataset_id,
            log_content=self.log.content,
            metadata_id=self.metadata.dataset_id,
            metadata_content=self.metadata.content,
        )

    @property
    def quantum_id(self) -> uuid.UUID:
        return self.predicted.quantum_id

    def update_wait_interval(self, times_for_task: ScannerTimeConfigDict) -> None:
        if not self.wait_interval:
            self.wait_interval = times_for_task["wait"]
        else:
            self.wait_interval *= times_for_task["wait_factor"]
        if self.wait_interval > times_for_task["wait_max"]:
            self.wait_interval = times_for_task["wait_max"]

    def set_provenance(self, compressor: Compressor, blocked: bool = False) -> None:
        provenance = ProvenanceQuantumModel.from_predicted(self.predicted)
        provenance.exception = self.metadata.exception
        provenance.caveats = self.metadata.caveats
        if blocked:
            provenance.status = QuantumRunStatus.BLOCKED
        elif self.metadata:
            if self.log:
                provenance.status = QuantumRunStatus.SUCCESSFUL
            else:
                provenance.status = QuantumRunStatus.LOGS_MISSING
        else:
            if self.log:
                provenance.status = QuantumRunStatus.FAILED
            else:
                provenance.status = QuantumRunStatus.METADATA_MISSING
        self.provenance = compressor.compress(provenance.model_dump_json().encode())
