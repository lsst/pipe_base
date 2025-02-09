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

import abc
import enum
import logging
from typing import TYPE_CHECKING, ClassVar, Protocol

from lsst.utils import introspection

from ._task_metadata import GetSetDictMetadata, NestedMetadataDict

if TYPE_CHECKING:
    from lsst.utils.logging import LsstLogAdapter

__all__ = (
    "AlgorithmError",
    "AnnotatedPartialOutputsError",
    "InvalidQuantumError",
    "NoWorkFound",
    "QuantumSuccessCaveats",
    "RepeatableQuantumError",
    "UnprocessableDataError",
    "UpstreamFailureNoWorkFound",
)


class QuantumSuccessCaveats(enum.Flag):
    """Flags that add caveats to a "successful" quantum.

    Quanta can be considered successful even if they do not produce some of
    their expected outputs (and even if they do not produce all of their
    expected outputs), as long as the condition is sufficiently well understood
    that downstream processing should succeed.
    """

    NO_CAVEATS = 0
    """All outputs were produced and no exceptions were raised."""

    ANY_OUTPUTS_MISSING = enum.auto()
    """At least one predicted output was not produced."""

    ALL_OUTPUTS_MISSING = enum.auto()
    """No predicted outputs (except logs and metadata) were produced.

    `ANY_OUTPUTS_MISSING` is also set whenever this flag is set.
    """

    NO_WORK = enum.auto()
    """A subclass of `NoWorkFound` was raised.

    This does not necessarily imply that `ANY_OUTPUTS_MISSING` is not set,
    since a `PipelineTask.runQuantum` implementation could raise it after
    directly writing all of its predicted outputs.
    """

    ADJUST_QUANTUM_RAISED = enum.auto()
    """`NoWorkFound` was raised by `PipelineTaskConnnections.adjustQuantum`.

    This indicates that if a new `QuantumGraph` had been generated immediately
    before running this quantum, that quantum would not have even been
    included, because required inputs that were expected to exist by the time
    it was run (in the original `QuantumGraph`) were not actually produced.

    `NO_WORK` and `ALL_OUTPUTS_MISSING` are also set whenever this flag is set.
    """

    UPSTREAM_FAILURE_NO_WORK = enum.auto()
    """`UpstreamFailureNoWorkFound` was raised by `PipelineTask.runQuantum`.

    This exception is raised by downstream tasks when an upstream task's
    outputs were incomplete in a way that blocks it from running, often
    because the upstream task raised `AnnotatedPartialOutputsError`.

    `NO_WORK` is also set whenever this flag is set.
    """

    UNPROCESSABLE_DATA = enum.auto()
    """`UnprocessableDataError` was raised by `PipelineTask.runQuantum`.

    `NO_WORK` is also set whenever this flag is set.
    """

    PARTIAL_OUTPUTS_ERROR = enum.auto()
    """`AnnotatedPartialOutputsError` was raised by `PipelineTask.runQuantum`
    and the execution system was instructed to consider this a qualified
    success.
    """

    @classmethod
    def from_adjust_quantum_no_work(cls) -> QuantumSuccessCaveats:
        """Return the set of flags appropriate for a quantum for which
        `PipelineTaskConnections.adjustdQuantum` raised `NoWorkFound`.
        """
        return cls.NO_WORK | cls.ADJUST_QUANTUM_RAISED | cls.ANY_OUTPUTS_MISSING | cls.ALL_OUTPUTS_MISSING

    def concise(self) -> str:
        """Return a concise string representation of the flags.

        Returns
        -------
        s : `str`
            Two-character string representation, with the first character
            indicating whether any predicted outputs were missing and the
            second representing any exceptions raised.  This representation is
            not always complete; some rare combinations of flags are displayed
            as if only one of the flags was set.

        Notes
        -----
        The `legend` method returns a description of the returned codes.
        """
        char1 = ""
        if self & QuantumSuccessCaveats.ALL_OUTPUTS_MISSING:
            char1 = "*"
        elif self & QuantumSuccessCaveats.ANY_OUTPUTS_MISSING:
            char1 = "+"
        char2 = ""
        if self & QuantumSuccessCaveats.ADJUST_QUANTUM_RAISED:
            char2 = "A"
        elif self & QuantumSuccessCaveats.UNPROCESSABLE_DATA:
            char2 = "D"
        elif self & QuantumSuccessCaveats.UPSTREAM_FAILURE_NO_WORK:
            char2 = "U"
        elif self & QuantumSuccessCaveats.PARTIAL_OUTPUTS_ERROR:
            char2 = "P"
        elif self & QuantumSuccessCaveats.NO_WORK:
            char2 = "N"
        return char1 + char2

    @staticmethod
    def legend() -> dict[str, str]:
        """Return a `dict` with human-readable descriptions of the characters
        used in `concise`.

        Returns
        -------
        legend : `dict` [ `str`, `str` ]
            Mapping from character code to description.
        """
        return {
            "+": "at least one predicted output was missing, but not all were",
            "*": "all predicated outputs were missing (besides logs and metadata)",
            "A": "adjustQuantum raised NoWorkFound; a regenerated QG would not include this quantum",
            "D": "algorithm considers data too bad to be processable",
            "U": "one or more input dataset was incomplete due to an upstream failure",
            "P": "task failed but wrote partial outputs; considered a partial success",
            "N": "runQuantum raised NoWorkFound",
        }


class GetSetDictMetadataHolder(Protocol):
    """Protocol for objects that have a ``metadata`` attribute that satisfies
    `GetSetDictMetadata`.
    """

    @property
    def metadata(self) -> GetSetDictMetadata | None:
        pass


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

    FLAGS: ClassVar = QuantumSuccessCaveats.NO_WORK


class UpstreamFailureNoWorkFound(NoWorkFound):
    """A specialization of `NoWorkFound` that indicates that an upstream task
    had a problem that was ignored (e.g. to prevent a single-detector failure
    from bringing down an entire visit).
    """

    FLAGS: ClassVar = QuantumSuccessCaveats.NO_WORK | QuantumSuccessCaveats.UPSTREAM_FAILURE_NO_WORK


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


class AlgorithmError(RepeatableQuantumError, abc.ABC):
    """Exception that may be raised by PipelineTasks (and code they delegate
    to) in order to indicate a repeatable algorithmic failure that will not be
    addressed by retries.

    Subclass this exception to define the metadata associated with the error
    (for example: number of data points in a fit vs. degrees of freedom).
    """

    @property
    @abc.abstractmethod
    def metadata(self) -> NestedMetadataDict | None:
        """Metadata from the raising `~lsst.pipe.base.Task` with more
        information about the failure. The contents of the dict are
        `~lsst.pipe.base.Task`-dependent, and must have `str` keys and `str`,
        `int`, `float`, `bool`, or nested-dictionary (with the same key and
        value types) values.
        """
        raise NotImplementedError


class UnprocessableDataError(NoWorkFound):
    """A specialization of `NoWorkFound` that will be [subclassed and] raised
    by Tasks to indicate a failure to process their inputs for some reason that
    is non-recoverable.

    Notes
    -----
    An example is a known bright star that causes PSF measurement to fail, and
    that makes that detector entirely non-recoverable. Another example is an
    image with an oddly shaped PSF (e.g. due to a failure to achieve focus)
    that warrants the image being flagged as "poor quality" which should not
    have further processing attempted.

    The `NoWorkFound` inheritance ensures the job will not be considered a
    failure (i.e. such that no human time will inadvertently be spent chasing
    it down).

    Do not raise this unless we are convinced that the data cannot (or should
    not) be processed, even by a better algorithm. Most instances where this
    error would be raised likely require an RFC to explicitly define the
    situation.
    """

    FLAGS: ClassVar = QuantumSuccessCaveats.NO_WORK | QuantumSuccessCaveats.UNPROCESSABLE_DATA


class AnnotatedPartialOutputsError(RepeatableQuantumError):
    """Exception that runQuantum raises when the (partial) outputs it has
    written contain information about their own incompleteness or degraded
    quality.

    Clients should construct this exception by calling `annotate` instead of
    calling the constructor directly. However, `annotate` does not chain the
    exception; this must still be done by the client.

    This exception should always chain the original error. When the
    executor catches this exception, it will report the original exception. In
    contrast, other exceptions raised from ``runQuantum`` are considered to
    invalidate any outputs that are already written.
    """

    FLAGS: ClassVar = QuantumSuccessCaveats.PARTIAL_OUTPUTS_ERROR

    @classmethod
    def annotate(
        cls, error: Exception, *args: GetSetDictMetadataHolder | None, log: logging.Logger | LsstLogAdapter
    ) -> AnnotatedPartialOutputsError:
        """Set metadata on outputs to explain the nature of the failure.

        Parameters
        ----------
        error : `Exception`
            Exception that caused the task to fail.
        *args : `GetSetDictMetadataHolder`
            Objects (e.g. Task, Exposure, SimpleCatalog) to annotate with
            failure information. They must have a `metadata` property.
        log : `logging.Logger`
            Log to send error message to.

        Returns
        -------
        error : `AnnotatedPartialOutputsError`
            Exception that the failing task can ``raise from`` with the
            passed-in exception.

        Notes
        -----
        This should be called from within an except block that has caught an
        exception. Here is an example of handling a failure in
        ``PipelineTask.runQuantum`` that annotates and writes partial outputs:

        .. code-block:: py
            :name: annotate-error-example

            def runQuantum(self, butlerQC, inputRefs, outputRefs):
                inputs = butlerQC.get(inputRefs)
                exposures = inputs.pop("exposures")
                assert not inputs, "runQuantum got more inputs than expected"

                result = pipeBase.Struct(catalog=None)
                try:
                    self.run(exposure)
                except pipeBase.AlgorithmError as e:
                    error = pipeBase.AnnotatedPartialOutputsError.annotate(
                        e, self, result.catalog, log=self.log
                    )
                    raise error from e
                finally:
                    butlerQC.put(result, outputRefs)
        """
        failure_info = {
            "message": str(error),
            "type": introspection.get_full_type_name(error),
        }
        if other := getattr(error, "metadata", None):
            failure_info["metadata"] = other

        # NOTE: Can't fully test this in pipe_base because afw is not a
        # dependency; test_calibrateImage.py in pipe_tasks gives more coverage.
        for item in args:
            # Some outputs may not exist, so we cannot set metadata on them.
            if item is None:
                continue
            item.metadata.set_dict("failure", failure_info)  # type: ignore

        log.debug(
            "Task failed with only partial outputs; see exception message for details.",
            exc_info=error,
        )

        return cls("Task failed and wrote partial outputs: see chained exception for details.")


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
