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
    "IngestRequest",
    "ScanReport",
    "ScanResult",
    "ScanStatus",
)

import dataclasses
import enum
import pickle
import uuid

from lsst.daf.butler.datastore.record_data import DatastoreRecordData

from ..._status import QuantumSuccessCaveats
from ...quantum_provenance_graph import ExceptionInfo, QuantumRunStatus
from ...resource_usage import QuantumResourceUsage
from .._common import DatastoreName
from .._predicted import PredictedDatasetModel


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

    data: bytes
    """Pickled form of the datasets and dimension records.

    Guaranteed to be empty if there are no datasets to ingest.

    We store the serialized form in the request so we don't have to do an
    extra round of pickle/unpickle when going from the scanner database to
    the ingest request queue.
    """

    @classmethod
    def pack(
        cls,
        producer_id: uuid.UUID,
        datasets: list[PredictedDatasetModel],
        records: dict[DatastoreName, DatastoreRecordData],
    ) -> IngestRequest:
        """Construct a request.

        Parameters
        ----------
        producer_id : `uuid.UUID`
            Unique ID of the quantum that produced these datasets.
        datasets : `list` [ `.PredictedDatasetModel` ]
            Registry dataset information.
        records : `dict` [ `str`, \
                `lsst.daf.butler.datastores.record_data.DatastoreRecordData` ]
            Datastore dataset information.

        Returns
        -------
        request : `IngestRequest`
            An ingest request with the dataset information already pickled.
        """
        data = b""
        if datasets or records:
            data = pickle.dumps((datasets, records))
        return cls(producer_id, data)

    def unpack(self) -> tuple[list[PredictedDatasetModel], dict[DatastoreName, DatastoreRecordData]]:
        """Unpack the dataset information from the request.

        Returns
        -------
        datasets : `list` [ `.PredictedDatasetModel` ]
            Registry dataset information.
        records : `dict` [ `str`, \
                `lsst.daf.butler.datastores.record_data.DatastoreRecordData` ]
            Datastore dataset information.
        """
        if not self.data:
            return [], {}
        return pickle.loads(self.data)

    def __bool__(self) -> bool:
        return bool(self.data)


@dataclasses.dataclass
class ScanResult:
    """A struct that represents the result of scanning a quantum."""

    quantum_id: uuid.UUID
    """Unique ID for the quantum."""

    status: ScanStatus
    """Combined status for the scan and the execution of the quantum."""

    caveats: QuantumSuccessCaveats | None = None
    """Flags indicating caveats on successful quanta."""

    exception: ExceptionInfo | None = None
    """Information about an exception raised when the quantum was executing."""

    resource_usage: QuantumResourceUsage | None = None
    """Resource usage information (timing, memory use) for this quantum."""

    existing_outputs: set[uuid.UUID] = dataclasses.field(default_factory=set)
    """Unique IDs of the output datasets that were actually written."""

    metadata: bytes = b""
    """Raw content of the metadata dataset."""

    log: bytes = b""
    """Raw content of the log dataset."""

    is_compressed: bool = False
    """Whether the `metadata` and `log` attributes are compressed."""

    def get_run_status(self) -> QuantumRunStatus:
        """Translate the scan status and metadata/log presence into a run
        status.
        """
        if self.status is ScanStatus.BLOCKED:
            return QuantumRunStatus.BLOCKED
        if self.status is ScanStatus.INIT:
            return QuantumRunStatus.SUCCESSFUL
        if self.log:
            if self.metadata:
                return QuantumRunStatus.SUCCESSFUL
            else:
                return QuantumRunStatus.FAILED
        else:
            if self.metadata:
                return QuantumRunStatus.LOGS_MISSING
            else:
                return QuantumRunStatus.METADATA_MISSING
