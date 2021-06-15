# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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
    "DatasetContentError",
    "DatasetCountError",
    "MockBehavior",
    "RunQuantumHelper",
)

from collections import defaultdict
import dataclasses
from typing import Callable, ClassVar, DefaultDict, Optional

from lsst.daf.butler import DataCoordinate, DatasetRef


class DatasetContentError(RuntimeError):
    """Error raised when the content passed between mock PipelineTasks does
    not match expectations.
    """


class DatasetCountError(RuntimeError):
    """Error raised when the number of datasets passed between mock
    PipelineTasks does not match expectations.
    """


def make_data_for_ref(ref):
    return {
        "dataset_type": ref.datasetType.name,
        "data_id": ref.dataId.byName(),
    }


@dataclasses.dataclass
class MockBehavior:
    read_predicate: Callable[[DatasetRef], bool] = lambda ref: True
    write_predicate: Callable[[DatasetRef], bool] = lambda ref: True
    run_callback: Callable[[], None] = lambda: None
    expected_counts: DefaultDict[str, Optional[int]] = lambda connection_name: None

    singleton: ClassVar[DefaultDict[str, DefaultDict[DataCoordinate]]] = defaultdict(
        lambda: defaultdict(MockBehavior)
    )


class RunQuantumHelper:
    def __init__(self, label, butlerQC, inputRefs, outputRefs):
        self.label = label
        self.behavior = MockBehavior.singleton[label][butlerQC.quantum.dataId]
        self.butlerQC = butlerQC
        self.inputRefs = inputRefs
        self.outputRefs = outputRefs

    @property
    def identifier(self) -> str:
        return f"{self.label}@{self.butlerQC.quantum.dataId}"

    def run(self):
        self.behavior.run_callback()

    def read_one(self, connection_name):
        ref = getattr(self.inputRefs, connection_name)
        if not isinstance(ref, DatasetRef):
            raise DatasetCountError(
                f"Expected a single DatasetRef for connection {connection_name!r} "
                f"on {self.identifier}, got {ref}."
            )
        self._read_ref(ref)

    def read_many(self, connection_name, minimum=1, maximum=None):
        refs = getattr(self.inputRefs, connection_name, [])
        self._check_counts(refs, connection_name, minimum=minimum, maximum=maximum)
        for ref in refs:
            self._read_ref(ref)

    def write_one(self, connection_name):
        ref = getattr(self.outputRefs, connection_name)
        if not isinstance(ref, DatasetRef):
            raise DatasetCountError(
                f"Expected a single DatasetRef for connection {connection_name!r} "
                f"on {self.identifier}, got {ref}."
            )
        if self.behavior.write_predicate(ref):
            self.butlerQC.put(make_data_for_ref(ref), ref)

    def write_many(self, connection_name, minimum=1, maximum=None):
        refs = getattr(self.inputRefs, connection_name, [])
        self._check_counts(refs, connection_name, minimum=minimum, maximum=maximum)
        for ref in refs:
            if self.behavior.write_predicate(ref):
                self.butlerQC.put(make_data_for_ref(ref), ref)

    def _read_ref(self, ref):
        if self.behavior.read_predicate(ref):
            got = self.butlerQC.get(ref)
            expected = make_data_for_ref(ref)
            if got != expected:
                raise DatasetContentError(got, expected, self.identifier)

    def _check_counts(self, refs, connection_name, minimum=1, maximum=None):
        if (expected_count := self.behavior.expected_counts[connection_name]) is not None:
            minimum = expected_count
            maximum = expected_count
        refs = getattr(self.inputRefs, connection_name, [])
        if len(refs) < minimum:
            raise DatasetCountError(
                f"Not enough DatasetRefs for connection {connection_name!r} "
                f"on {self.identifier}; expected at least {minimum}, got {len(refs)}."
            )
        if maximum is not None and len(refs) > maximum:
            raise DatasetCountError(
                f"Too many DatasetRefs for connection {connection_name!r} "
                f"on {self.identifier}; expected at most {maximum}, got {len(refs)}."
            )
