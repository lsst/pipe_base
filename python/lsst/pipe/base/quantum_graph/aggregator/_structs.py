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
    "IngestConfirmation",
    "IngestRequest",
    "ScanReport",
    "ScanResult",
    "ScanStatus",
)

import dataclasses
import enum
import uuid

from ..._status import QuantumSuccessCaveats
from ...quantum_provenance_graph import ExceptionInfo, QuantumRunStatus


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
    """The quantum was conclusively scanned and was executed successfuly,
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
    quantum_id: uuid.UUID
    status: ScanStatus


@dataclasses.dataclass
class IngestRequest:
    scanner_id: int
    producer_id: uuid.UUID
    pickled_datasets: bytes


@dataclasses.dataclass
class IngestConfirmation:
    scanner_id: int
    producer_ids: list[uuid.UUID]


@dataclasses.dataclass
class ScanResult:
    quantum_id: uuid.UUID
    status: ScanStatus
    caveats: QuantumSuccessCaveats | None = None
    exception: ExceptionInfo | None = None
    existing_outputs: set[uuid.UUID] = dataclasses.field(default_factory=set)
    metadata: bytes = b""
    log: bytes = b""

    def get_run_status(self) -> QuantumRunStatus:
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
