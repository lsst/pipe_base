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

__all__ = ("IngestRequest", "ScanReport")

import dataclasses
import uuid

from lsst.daf.butler import DatasetRef
from lsst.daf.butler.datastore.record_data import DatastoreRecordData

from .._common import DatastoreName
from .._provenance import ProvenanceQuantumScanStatus


@dataclasses.dataclass
class ScanReport:
    """Minimal information needed about a completed scan by the supervisor."""

    quantum_id: uuid.UUID
    """Unique ID of the quantum."""

    status: ProvenanceQuantumScanStatus
    """Combined status of the scan and the execution of the quantum."""


@dataclasses.dataclass
class IngestRequest:
    """A request to ingest datasets produced by a single quantum."""

    producer_id: uuid.UUID
    """ID of the quantum that produced these datasets."""

    refs: list[DatasetRef]
    """Registry information about the datasets."""

    records: dict[DatastoreName, DatastoreRecordData]
    """Datastore information about the datasets."""

    def __bool__(self) -> bool:
        return bool(self.refs or self.records)
