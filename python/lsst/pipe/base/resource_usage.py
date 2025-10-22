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

__all__ = ("QuantumResourceUsage",)

import datetime

import numpy as np
import pydantic

from ._task_metadata import TaskMetadata


class QuantumResourceUsage(pydantic.BaseModel):
    """A struct holding the most frequently used resource usage metrics for
    executed quanta.
    """

    memory: float = pydantic.Field(json_schema_extra={"unit": "byte"})
    """Maximum memory usage of the process that ran this quantum, in bytes.

    This is derived from the maximum resident set size at the end of the
    quantum's execution, and is expected to only grow as multiple quanta are
    executed in the same process; that makes this a lower bound on the actual
    memory use of any particular quantum.
    """

    start: datetime.datetime
    """Start time (UTC), corresponding to the beginning of the "prep" period.
    """

    prep_time: float = pydantic.Field(json_schema_extra={"unit": "s"})
    """Wall-clock time in seconds spent preparing for the execution of this
    quantum.

    This includes checking for existing inputs, outputs, clobbering when
    necessary, and running the `..PipelineTaskConnections.adjustQuantum` hook.
    It does not include process startup times, import times, or butler
    initialization overheads.
    """

    init_time: float = pydantic.Field(json_schema_extra={"unit": "s"})
    """Wall-clock time in seconds spent initializing the task used to run this
    quantum.

    This includes the time spent reading init-inputs.
    """

    run_time: float = pydantic.Field(json_schema_extra={"unit": "s"})
    """Wall-clock time in seconds spent executing this quantum.

    This includes time spent reading inputs and writing outputs.  It does not
    include the time spent writing the special log and metadata datasets.
    """

    run_time_cpu: float = pydantic.Field(json_schema_extra={"unit": "s"})
    """CPU time in seconds spent executing this quantum.

    This includes time spent reading inputs and writing outputs, to the extent
    that those operations actually spend CPU time at all.  It does not include
    the time spent writing the special log and metadata datasets.
    """

    @property
    def total_time(self) -> float:
        """Total wall-clock time spent in "prep", "init", and "run"."""
        return self.prep_time + self.init_time + self.run_time

    @property
    def end(self) -> datetime.datetime:
        """End time (UTC), corresponding to the end of the "run" period."""
        return self.start + datetime.timedelta(seconds=self.total_time)

    @classmethod
    def from_task_metadata(cls, metadata: TaskMetadata) -> QuantumResourceUsage | None:
        """Extract resource usage information from task metadata.

        Parameters
        ----------
        metadata : `TaskMetadata`
            Metadata written by
            `.single_quantum_executor.SingleQuantumExecutor`.

        Returns
        -------
        resource_usage : `QuantumResourceUsage` or `None`
            Resource usage information for this quantum, or `None` if the
            expected fields were not found.
        """
        try:
            quantum_metadata = metadata["quantum"]
        except KeyError:
            return None
        end = datetime.datetime.fromisoformat(quantum_metadata["endUtc"])
        start = datetime.datetime.fromisoformat(quantum_metadata["prepUtc"])
        try:
            start_init = datetime.datetime.fromisoformat(quantum_metadata["initUtc"])
        except KeyError:
            start_init = end
        try:
            start_run = datetime.datetime.fromisoformat(quantum_metadata["startUtc"])
        except KeyError:
            start_run = end
        try:
            run_time_cpu = quantum_metadata["endCpuTime"] - quantum_metadata["startCpuTime"]
        except KeyError:
            run_time_cpu = 0.0
        return cls(
            memory=quantum_metadata["endMaxResidentSetSize"],
            start=start,
            prep_time=(start_init - start).total_seconds(),
            init_time=(start_run - start_init).total_seconds(),
            run_time=(end - start_run).total_seconds(),
            run_time_cpu=run_time_cpu,
        )

    @staticmethod
    def get_numpy_fields() -> dict[str, np.dtype]:
        """Return a mapping from field name to the `numpy.dtype` for that
        field.
        """
        return {
            "memory": np.dtype(np.float32),
            "start": np.dtype(np.float64),
            "prep_time": np.dtype(np.float32),
            "init_time": np.dtype(np.float32),
            "run_time": np.dtype(np.float32),
            "run_time_cpu": np.dtype(np.float32),
        }

    @staticmethod
    def get_units() -> dict[str, str | None]:
        """Return a mapping from field name to units.

        Units are astropy-compatible strings.
        """
        return {
            "memory": "byte",
            "start": "s",
            "prep_time": "s",
            "init_time": "s",
            "run_time": "s",
            "run_time_cpu": "s",
        }

    def get_numpy_row(self) -> tuple[object, ...]:
        """Convert this object to a `tuple` that can used to initialize a
        NumPy structured array with the fields from `get_numpy_fields`.
        """
        return (
            self.memory,
            self.start.timestamp(),
            self.prep_time,
            self.init_time,
            self.run_time,
            self.run_time_cpu,
        )
