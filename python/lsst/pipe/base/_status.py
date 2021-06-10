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
    "NoWorkFound",
    "RepeatableQuantumError",
    "InvalidQuantumError",
)


class NoWorkFound(BaseException):
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
    address the problem.  When the desired behavior is to skip all of this
    quantum and attempt downstream tasks (or skip them) without its its
    outputs, raise `NoWorkFound` or return without raising instead.
    """

    EXIT_CODE = 20


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

    EXIT_CODE = 21
