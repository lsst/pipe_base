# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ["GcMetrics"]

import gc
from collections import defaultdict
from types import TracebackType
from typing import Self

import pydantic

from ._task_metadata import TaskMetadata


def _gc_stats() -> dict[str, list[int]]:
    """Convert result of `gc.get_stats` to a dictionary of lists."""
    result: dict[str, list[int]] = defaultdict(list)
    for gen_stats in gc.get_stats():
        for key, stat in gen_stats.items():
            result[key].append(stat)
    return result


class GcMetrics(pydantic.BaseModel):
    """Context manager which collects GC metrics and converts them into
    a dictionary suitable for TaskMetadata.
    """

    start_isenabled: bool | None = None
    """Whether GC is enabled on entering context (`bool` or `None`)."""

    end_isenabled: bool | None = None
    """Whether GC is enabled on exiting context (`bool` or `None`)."""

    start_threshold: list[int] | None = None
    """GC thresholds on entering context (`list`[`int`] or `None`)."""

    end_threshold: list[int] | None = None
    """GC thresholds on exiting context (`list`[`int`] or `None`)."""

    start_count: list[int] | None = None
    """GC collection counts on entering context (`list`[`int`] or `None`)."""

    end_count: list[int] | None = None
    """GC collection counts on exiting context (`list`[`int`] or `None`)."""

    start_stats: dict[str, list[int]] | None = None
    """GC stats on entering context (`dict`[`str`, `list`[`int`]] or `None`).

    These are the same values as returned from `gc.get_stats` but rearranged
    to be indexed by string key first and generation second.
    """

    end_stats: dict[str, list[int]] | None = None
    """GC stats on exiting context, same format as `start_stats`
    (`dict`[`str`, `list`[`int`]] or `None`).
    """

    def __enter__(self) -> Self:
        self.start_isenabled = gc.isenabled()
        self.start_threshold = list(gc.get_threshold())
        self.start_count = list(gc.get_count())
        self.start_stats = _gc_stats()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.end_isenabled = gc.isenabled()
        self.end_threshold = list(gc.get_threshold())
        self.end_count = list(gc.get_count())
        self.end_stats = _gc_stats()

    @classmethod
    def from_task_metadata(cls, metadata: TaskMetadata) -> GcMetrics | None:
        """Extract GC metrics from task metadata.

        Parameters
        ----------
        metadata : `TaskMetadata`
            Metadata written by
            `.single_quantum_executor.SingleQuantumExecutor`.

        Returns
        -------
        gc_metrics : `GcMetrics` or `None`
            GC metrics for this quantum, or `None` if the expected fields were
            not found.
        """
        try:
            quantum_metadata = metadata["quantum"]
        except KeyError:
            return None
        try:
            gc_metadata = quantum_metadata["gc_metrics"]
        except KeyError:
            return None

        return GcMetrics(**gc_metadata.to_dict())
