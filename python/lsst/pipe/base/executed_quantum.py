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

__all__ = (
)

import enum
from typing import FrozenSet
import pydantic

from lsst.daf.butler import DatasetId


class NoWorkQuantum(BaseException):
    """An exception raised when a Quantum should not exist because there is no
    work for it to do.

    This usually occurs because a non-optional input dataset is not present, or
    a spatiotemporal overlap that was conservatively predicted does not
    actually exist.

    This inherits from BaseException because it is used to signal a case that
    we don't consider a real error, even though we often want to use try/except
    logic to trap it.
    """


class RepeatableQuantumError(RuntimeError):
    """Exception that may be raised by PipelineTasks (and code they delegate
    to) in order to indicate that a repeatable problem that will not be
    addressed by retries.

    This usually indicates that the algorithm and the data it has been given
    are somehow incompatible, and the task should run fine on most other data.

    This exception may be used as a base class for more specific questions, or
    used directly while chaining another exception, e.g.::

        try:
            run_code()
        except SomeOtherError as err:
            raise RepeatableQuantumError() from err

    This may be used for missing input data when the desired behavior is to
    cause all downstream tasks being run be blocked, forcing the user to
    address the problem.  When the desired behavior is to skip this quantum and
    attempt downstream tasks (or skip them) without its outputs, raise
    `NoWorkQuantum` instead.  When the desired behavior is to write only some
    outputs, the task should exit as usual and will be considered a success.
    """


class InvalidQuantumError(Exception):
    """Exception that may be raised by PipelineTasks (and code they delegate
    to) in order to indicate logic bug or configuration problem.

    This usually indicates that the configured algorithm itself is invalid and
    will not run on a significant fraction of quanta (often all of them).

    This exception may be used as a base class for more specific questions, or
    used directly while chaining another exception, e.g.::

        try:
            run_code()
        except SomeOtherError as err:
            raise RepeatableQuantumError() from err

    Raising this exception in `PipelineTask.runQuantum` or something it calls
    is a last resort - whenever possible, such problems should cause exceptions
    in ``__init__`` or in QuantumGraph generation.  It should never be used
    for missing data.
    """


class QuantumStatusCategory(enum.Enum):
    SUCCEEDED = enum.auto()
    """Quantum ran to completion.

    This usually means at least some predicted outputs were actually produced,
    but does not guarantee it unless
    `PipelineTaskConnections.hasPostWriteLogic` returns `False`.
    """

    NO_WORK_FOUND = enum.auto()
    """Quantum was run but found it had no work to do, and produced no outputs
    (other than metadata).

    Rerunning a task that had this status will change the result only if its
    `~ExtendedQuantumStatus.available_inputs` change.  This status may be
    invoked by a `PipelineTask` by raising `NoWorkQuantum`.
    """

    NO_WORK_SKIPPED = enum.auto()
    """Quantum was not run by the execution harness, because at least one
    required input was predicted but not actually produced by an upstream task.

    Tasks with this state should have metadata written directly by the
    execution harness, and should never be rerun unless its
    `~ExtendedQuantumStatus.available_inputs` change such that all required
    inputs are now available.
    """

    INTERRUPTED = enum.auto()
    """Quantum caught an external signal indicating it should stop execution,
    and then shut down cleanly.

    This state should never be set if all predicted outputs were produced and
    `PipelineTaskConnections.hasPostWriteLogic` returns `False`; execution
    harnesses should record this as a success even if a last-moment
    interruption attempt was detected.
    """

    FAILED_EXCEPTION = enum.auto()
    """Quantum raised a Python exception that was caught by the execution
    harness.

    This state does not attempt to distinguish between repeatable problems
    and transient ones; rerunning a quantum with this status may or may not
    yield a different result.
    """

    FAILED_UNKNOWN = enum.auto()
    """Quantum failed for an unknown reason.

    This state does not attempt to distinguish between repeatable problems
    and transient ones; rerunning a quantum with this status may or may not
    yield a different result.

    This state cannot usually be set by Python execution harnesses that run
    in the same process as the task code, but it may be set by higher-level
    systems in the case of e.g. segfaults, and should be assumed in cases where
    the file or dataset that would normally contain status information isn't
    present at all.
    """

    FAILED_REPEATABLE = enum.auto()
    """Quantum failed due to a problem that the task was able to recognize as
    non-transient and highly likely to affect any attempt to rerun this
    quantum.

    This status can be invoked by a `PipelineTask` by raising
    `RepeatableQuantumError`.
    """

    FAILED_INVALID = enum.auto()
    """Quantum failed because of a configuration problem or task logic bug that
    must be fixed by the user.

    Execution harnesses may shut down entire runs if this status is detected in
    any quantum.

    This should be set if a task failure (not an interruption) occurs after all
    predicted outputs have been produced and
    `PipelineTaskConnections.hasPostWriteLogic` returns `False`, as this
    indicates that this method has been implemented incorrectly.

    This status can be invoked by a `PipelineTask` by raising
    `InvalidQuantumError`.
    """

    @property
    def can_retry(self) -> bool:
        return self is self.FAILED_EXCEPTION or self is self.INTERRUPTED

    @property
    def is_no_work(self) -> bool:
        return self is self.NO_WORK_FOUND or self is self.NO_WORK_SKIPPED

    @property
    def is_success(self) -> bool:
        return self is self.SUCCEEDED or self is self.is_no_work

    @property
    def is_failure(self) -> bool:
        return (
            self is self.FAILED_EXCEPTION
            or self is self.FAILED_UNKNOWN
            or self is self.FAILED_REPEATABLE
            or self is self.FAILED_INVALID
        )


class ExtendedQuantumStatus(pydantic.BaseModel):
    """Struct used to record the state of a quantum that has been run.
    """

    category: QuantumStatusCategory
    """Category describing the qualitative execution status for this quantum.
    """

    available_inputs: FrozenSet[DatasetId] = frozenset()
    """IDs of all input datasets that were actually available to the task
    at execution time.

    This may differ from the predicted inputs by removal of datasets that
    were not actually produced by upstream tasks.

    This field will be set for all quanta for which provenance is successfully
    written, regardless of status category.
    """

    actual_inputs: FrozenSet[DatasetId] = frozenset()
    """IDs of all input datasets actually used by the task.

    Any dataset that can affect the output of the algorithm should be included.
    For example, if a dataset is ultimately identified as some kind of outlier,
    but was itself used in the determination of whether other datasets were or
    were not outliers, it is still considered an actual input.

    If a `PipelineTask` never reads a dataset at all, it will automatically be
    removed from `actual_inputs`.  It may also explicitly call
    `ButlerQuantumContext.makeInputUnused`.
    """

    actual_outputs: FrozenSet[DatasetId] = frozenset()
    """IDs of all output dataset actually produced by this task.

    This is set automatically by calls to `ButlerQuantumContext.put`;
    `PipelineTask` authors should not have to do anything manually.
    """

    #
    # Notably missing:
    #
    # - Quantum identifiers.  I'd like to wait for DM-30266, and then we need
    #   to think about how much we want to normalize/denormalize predicted and
    #   executed quantum information.
    #
    # - Exception objects.  These look like a pain to serialize well, but
    #   doing it well seems really valuable.  Maybe
    #   https://github.com/ionelmc/python-tblib?
    #
    # - Host information and resource usage.  Just haven't gotten around to it,
    #   and I bet other people have schemas I should just copy from.
    #
