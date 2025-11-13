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
    "InProgressScan",
    "IngestRequest",
    "ScanReport",
    "ScanStatus",
    "WriteRequest",
)

import dataclasses
import enum
import uuid

from lsst.daf.butler.datastore.record_data import DatastoreRecordData

from .._common import DatastoreName
from .._predicted import PredictedDatasetModel
from .._provenance import (
    ProvenanceLogRecordsModel,
    ProvenanceQuantumAttemptModel,
    ProvenanceTaskMetadataModel,
)


class ScanStatus(enum.Enum):
    """Status enum for quantum scanning.

    Note that this records the status for the *scanning* which is distinct
    from the status of the quantum's execution.
    """

    INCOMPLETE = enum.auto()
    """The quantum is not necessarily done running, and cannot be scanned
    conclusively yet.
    """

    ABANDONED = enum.auto()
    """The quantum's execution appears to have failed but we cannot rule out
    the possibility that it could be recovered, but we've also waited long
    enough (according to `ScannerTimeConfigDict.retry_timeout`) that it's time
    to stop trying for now.

    This state means a later run with `ScannerConfig.assume_complete` is
    required.
    """

    SUCCESSFUL = enum.auto()
    """The quantum was conclusively scanned and was executed successfully,
    unblocking scans for downstream quanta.
    """

    FAILED = enum.auto()
    """The quantum was conclusively scanned and failed execution, blocking
    scans for downstream quanta.
    """

    BLOCKED = enum.auto()
    """A quantum upstream of this one failed."""

    INIT = enum.auto()
    """Init quanta need special handling, because they don't have logs and
    metadata.
    """


@dataclasses.dataclass
class ScanReport:
    """Minimal information needed about a completed scan by the supervisor."""

    quantum_id: uuid.UUID
    """Unique ID of the quantum."""

    status: ScanStatus
    """Combined status of the scan and the execution of the quantum."""


@dataclasses.dataclass
class IngestRequest:
    """A request to ingest datasets produced by a single quantum."""

    producer_id: uuid.UUID
    """ID of the quantum that produced these datasets."""

    datasets: list[PredictedDatasetModel]
    """Registry information about the datasets."""

    records: dict[DatastoreName, DatastoreRecordData]
    """Datastore information about the datasets."""

    def __bool__(self) -> bool:
        return bool(self.datasets or self.records)


@dataclasses.dataclass
class InProgressScan:
    """A struct that represents a quantum that is being scanned."""

    quantum_id: uuid.UUID
    """Unique ID for the quantum."""

    status: ScanStatus
    """Combined status for the scan and the execution of the quantum."""

    attempts: list[ProvenanceQuantumAttemptModel] = dataclasses.field(default_factory=list)
    """Provenance information about each attempt to run the quantum."""

    outputs: dict[uuid.UUID, bool] = dataclasses.field(default_factory=dict)
    """Unique IDs of the output datasets mapped to whether they were actually
    produced.
    """

    metadata: ProvenanceTaskMetadataModel = dataclasses.field(default_factory=ProvenanceTaskMetadataModel)
    """Task metadata information for each attempt.
    """

    logs: ProvenanceLogRecordsModel = dataclasses.field(default_factory=ProvenanceLogRecordsModel)
    """Log records for each attempt.
    """


@dataclasses.dataclass
class WriteRequest:
    """A struct that represents a request to write provenance for a quantum."""

    quantum_id: uuid.UUID
    """Unique ID for the quantum."""

    status: ScanStatus
    """Combined status for the scan and the execution of the quantum."""

    existing_outputs: set[uuid.UUID] = dataclasses.field(default_factory=set)
    """Unique IDs of the output datasets that were actually written."""

    quantum: bytes = b""
    """Serialized quantum provenance model.

    This may be empty for quanta that had no attempts.
    """

    metadata: bytes = b""
    """Serialized task metadata."""

    logs: bytes = b""
    """Serialized logs."""

    is_compressed: bool = False
    """Whether the `quantum`, `metadata`, and `log` attributes are
    compressed.
    """
