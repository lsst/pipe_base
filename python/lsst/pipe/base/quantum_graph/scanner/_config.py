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

from collections import ChainMap
from typing import TypedDict

import pydantic

from lsst.resources import ResourcePath

from .._common import TaskLabel


class ScannerTimeConfigDict(TypedDict):
    wait: float
    """Wait time (s) between the first attempt to scan for a quantum's
    completion and the second attempt.

    When a quantum's log dataset appears without metadata (indicating a
    failure) the wait time is reset to this value.
    """

    wait_factor: float
    """Multiplier for `wait` applied on each attempt after the second.
    """

    wait_max: float
    """Maximum wait interval (s) for scans.
    """

    retry_timeout: float
    """Maximum total wait time since the first scan with a log dataset present
    before giving up.

    Quanta with log and no metadata have failed at least once, but may be
    retried and hence turn into successes if we wait.
    """


def make_scanner_time_defaults() -> ScannerTimeConfigDict:
    return dict(
        wait=60.0,
        wait_factor=1.2,
        wait_max=600.0,
        retry_timeout=600.0,
    )


class ScannerTimeOverrideDict(ScannerTimeConfigDict, total=False):
    pass


class ScannerConfig(pydantic.BaseModel):
    predicted_path: ResourcePath
    butler_path: ResourcePath
    db_path: ResourcePath
    output_path: ResourcePath
    checkpoint_path: ResourcePath | None = None
    vacuum_on_checkpoint: bool = True
    n_processes: int = 1
    zstd_level: int = 10
    assume_complete: bool = False
    default_times: ScannerTimeConfigDict = pydantic.Field(default_factory=make_scanner_time_defaults)
    task_times: dict[TaskLabel, ScannerTimeOverrideDict] = pydantic.Field(default_factory=dict)

    idle_timeout: float = 600.0
    """Minimum time to wait (s) before shutting down the scanner when no
    progress is being made at all.

    When different tasks have different timeouts, the maximum of the timeouts
    of all pending work is used.
    """

    checkpoint_interval: float = 1200.0
    """Time (s) between checkpoints that flush the local database to persistent
    storage.
    """

    ingest_interval: float = 1200.0
    """Time (s) between ingests into the central butler database."""

    delete_dataset_types: list[str] = pydantic.Field(default_factory=list)

    delete_metadata: bool = True

    delete_log: bool = True

    def get_times_for_task(self, task_label: TaskLabel) -> ScannerTimeConfigDict:
        if task_label in self.task_times:
            return ChainMap(self.task_times[task_label], self.default_times)  # type: ignore
        else:
            return self.default_times
