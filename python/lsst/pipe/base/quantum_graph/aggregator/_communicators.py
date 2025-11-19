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
    "FatalWorkerError",
    "IngesterCommunicator",
    "ScannerCommunicator",
    "SpawnProcessContext",
    "SupervisorCommunicator",
    "ThreadingContext",
    "WorkerContext",
)

import cProfile
import dataclasses
import enum
import logging
import multiprocessing.context
import multiprocessing.synchronize
import os
import queue
import signal
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator
from contextlib import AbstractContextManager, ExitStack, contextmanager
from traceback import format_exception
from types import TracebackType
from typing import Any, Literal, Self, TypeAlias, TypeVar, Union

from lsst.utils.logging import VERBOSE, LsstLogAdapter

from ._config import AggregatorConfig
from ._progress import ProgressManager, make_worker_log
from ._structs import IngestRequest, ScanReport, WriteRequest

_T = TypeVar("_T")

_TINY_TIMEOUT = 0.01

# multiprocessing.Queue is a type according to the standard library type stubs,
# but it's really a function at runtime.  But since the Python <= 3.11 type
# alias syntax uses the real runtime things we need to use strings, and hence
# we need to use Union.  With Python 3.12's 'type' statement this gets cleaner.
Queue: TypeAlias = Union["queue.Queue[_T]", "multiprocessing.Queue[_T]"]

Event: TypeAlias = threading.Event | multiprocessing.synchronize.Event

Worker: TypeAlias = threading.Thread | multiprocessing.context.SpawnProcess


class WorkerContext(ABC):
    """A simple abstract interface that can be implemented by both threading
    and multiprocessing.
    """

    @abstractmethod
    def make_queue(self) -> Queue[Any]:
        """Make an empty queue that can be used to pass objects between
        workers in this context.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_event(self) -> Event:
        """Make an event that can be used to communicate a boolean state change
        to workers in this context.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_worker(
        self, target: Callable[..., None], args: tuple[Any, ...], name: str | None = None
    ) -> Worker:
        """Make a worker that runs the given callable.

        Parameters
        ----------
        target : `~collections.abc.Callable`
            A callable to invoke on the worker.
        args : `tuple`
            Positional arguments to pass to the callable.
        name : `str`, optional
            Human-readable name for the worker.

        Returns
        -------
        worker : `threading.Thread` or `multiprocessing.Process`
            Process or thread.  Will need to have its ``start`` method called
            to actually begin.
        """
        raise NotImplementedError()


class ThreadingContext(WorkerContext):
    """An implementation of `WorkerContext` backed by the `threading`
    module.
    """

    def make_queue(self) -> Queue[Any]:
        return queue.Queue()

    def make_event(self) -> Event:
        return threading.Event()

    def make_worker(
        self, target: Callable[..., None], args: tuple[Any, ...], name: str | None = None
    ) -> Worker:
        return threading.Thread(target=target, args=args, name=name)


class SpawnProcessContext(WorkerContext):
    """An implementation of `WorkerContext` backed by the `multiprocessing`
    module, with new processes started by spawning.
    """

    def __init__(self) -> None:
        self._ctx = multiprocessing.get_context("spawn")

    def make_queue(self) -> Queue[Any]:
        return self._ctx.Queue()

    def make_event(self) -> Event:
        return self._ctx.Event()

    def make_worker(
        self, target: Callable[..., None], args: tuple[Any, ...], name: str | None = None
    ) -> Worker:
        return self._ctx.Process(target=target, args=args, name=name)


def _get_from_queue(q: Queue[_T], block: bool = False, timeout: float | None = None) -> _T | None:
    """Get an object from a queue and return `None` if it is empty.

    Parameters
    ----------
    q : `Queue`
        Queue to get an object from.
    block : `bool`
        Whether to block until an object is available.
    timeout : `float` or `None`, optional
        Maximum number of seconds to wait while blocking.

    Returns
    -------
    obj : `object` or `None`
        Object from the queue, or `None` if it was empty.
    """
    try:
        return q.get(block=block, timeout=timeout)
    except queue.Empty:
        return None


class FatalWorkerError(BaseException):
    """An exception raised by communicators when one worker (including the
    supervisor) has caught an exception in order to signal the others to shut
    down.
    """


class _Sentinel(enum.Enum):
    """Sentinel values used to indicate sequence points or worker shutdown
    conditions.
    """

    NO_MORE_SCAN_REQUESTS = enum.auto()
    """Sentinel sent from the supervisor to scanners to indicate that there are
    no more quanta left to be scanned.
    """

    NO_MORE_INGEST_REQUESTS = enum.auto()
    """Sentinel sent from scanners to the ingester to indicate that there are
    will be no more ingest requests from a particular worker.
    """

    NO_MORE_WRITE_REQUESTS = enum.auto()
    """Sentinel sent from scanners and the supervisor to the writer to
    indicate that there are will be no more write requests from a particular
    worker.
    """

    WRITE_REPORT = enum.auto()
    """Sentinel sent from the writer to the supervisor to report that a
    quantum's provenance was written.
    """

    SCANNER_DONE = enum.auto()
    """Sentinel sent from scanners to the supervisor to report that they are
    done and shutting down.
    """

    INGESTER_DONE = enum.auto()
    """Sentinel sent from the ingester to the supervisor to report that it is
    done and shutting down.
    """

    WRITER_DONE = enum.auto()
    """Sentinel sent from the writer to the supervisor to report that it is
    done and shutting down.
    """


@dataclasses.dataclass
class _WorkerErrorMessage:
    """An internal worker used to pass information about an error that occurred
    on a worker back to the supervisor.

    As a rule, these are unexpected, unrecoverable exceptions.
    """

    worker: str
    """Name of the originating worker."""

    traceback: str
    """A logged exception traceback.

    Note that this is not a `BaseException` subclass that can actually be
    re-raised on the supervisor; it's just something we can log to make the
    right traceback appear on the screen.  If something silences that printing
    in favor of its own exception management (pytest!) this information
    disappears.
    """


@dataclasses.dataclass
class _ScanRequest:
    """An internal struct passed from the supervisor to the scanners to request
    a quantum be scanned.
    """

    quantum_id: uuid.UUID
    """ID of the quantum to be scanned."""


@dataclasses.dataclass
class _IngestReport:
    """An internal struct passed from the ingester to the supervisor to report
    a completed ingest batch.
    """

    n_producers: int
    """Number of producing quanta whose datasets were ingested.

    We use quanta rather than datasets as the count here because the supervisor
    knows the total number of quanta in advance but not the total number of
    datasets to be ingested, so it's a lot easier to attach a denominator
    and/or progress bar to this number.
    """


@dataclasses.dataclass
class _ProgressLog:
    """A high-level log message sent from a worker to the supervisor.

    These are messages that should appear to come from the main
    'aggregate-graph' logger, not a worker-specific one.
    """

    message: str
    """Log message."""

    level: int
    """Log level."""


@dataclasses.dataclass
class _CompressionDictionary:
    """An internal struct used to send the compression dictionary from the
    writer to the scanners.
    """

    data: bytes
    """The `bytes` representation of a `zstandard.ZstdCompressionDict`.
    """


Report: TypeAlias = (
    ScanReport
    | _IngestReport
    | _WorkerErrorMessage
    | _ProgressLog
    | Literal[
        _Sentinel.WRITE_REPORT,
        _Sentinel.SCANNER_DONE,
        _Sentinel.INGESTER_DONE,
        _Sentinel.WRITER_DONE,
    ]
)


class SupervisorCommunicator:
    """A helper object that lets the supervisor direct the other workers.

    Parameters
    ----------
    log : `lsst.utils.logging.LsstLogAdapter`
        LSST-customized logger.
    n_scanners : `int`
        Number of scanner workers.
    context : `WorkerContext`
        Abstraction over threading vs. multiprocessing.
    config : `AggregatorConfig`
        Configuration for the aggregator.
    """

    def __init__(
        self,
        log: LsstLogAdapter,
        n_scanners: int,
        context: WorkerContext,
        config: AggregatorConfig,
    ) -> None:
        self.config = config
        self.progress = ProgressManager(log, config)
        self.n_scanners = n_scanners
        # The supervisor sends scan requests to scanners on this queue.
        # When complete, the supervisor sends n_scanners sentinals and each
        # scanner is careful to only take one before it starts its shutdown.
        self._scan_requests: Queue[_ScanRequest | Literal[_Sentinel.NO_MORE_SCAN_REQUESTS]] = (
            context.make_queue()
        )
        # The scanners send ingest requests to the ingester on this queue. Each
        # scanner sends one sentinal when it is done, and the ingester is
        # careful to wait for n_scanners sentinals to arrive before it starts
        # its shutdown.
        self._ingest_requests: Queue[IngestRequest | Literal[_Sentinel.NO_MORE_INGEST_REQUESTS]] = (
            context.make_queue()
        )
        # The scanners send write requests to the writer on this queue (which
        # will be `None` if we're not writing).  The supervisor also sends
        # write requests for blocked quanta (which we don't scan).  Each
        # scanner and the supervisor send one sentinal when done, and the
        # writer waits for (n_scanners + 1) sentinals to arrive before it
        # starts its shutdown.
        self._write_requests: Queue[WriteRequest | Literal[_Sentinel.NO_MORE_WRITE_REQUESTS]] | None = (
            context.make_queue() if config.output_path is not None else None
        )
        # All other workers use this queue to send many different kinds of
        # reports the supervisor.  The supervisor waits for a _DONE sentinal
        # from each worker before it finishes its shutdown.
        self._reports: Queue[Report] = context.make_queue()
        # The writer sends the compression dictionary to the scanners on this
        # queue.  It puts n_scanners copies on the queue, and each scanner only
        # takes one.  The compression_dict queue has no sentinal because it is
        # only used at most once; the supervisor takes responsibility for
        # clearing it out shutting down.
        self._compression_dict: Queue[_CompressionDictionary] = context.make_queue()
        # The supervisor sets this event when it receives an interrupt request
        # from an exception in the main process (usually KeyboardInterrupt).
        # Worker communicators check this in their polling loops and raise
        # FatalWorkerError when they see it set.
        self._cancel_event: Event = context.make_event()
        # Track what state we are in closing down, so we can start at the right
        # point if we're interrupted and __exit__ needs to clean up.  Note that
        # we can't rely on a non-exception __exit__ to do any shutdown work
        # that might be slow, since a KeyboardInterrupt that occurs when
        # __exit__ is already running can't be caught inside __exit__.
        self._sent_no_more_scan_requests = False
        self._sent_no_more_write_requests = False
        self._n_scanners_done = 0
        self._ingester_done = False
        self._writer_done = self._write_requests is None

    def wait_for_workers_to_finish(self, already_failing: bool = False) -> None:
        if not self._sent_no_more_scan_requests:
            for _ in range(self.n_scanners):
                self._scan_requests.put(_Sentinel.NO_MORE_SCAN_REQUESTS, block=False)
            self._sent_no_more_scan_requests = True
        if not self._sent_no_more_write_requests and self._write_requests is not None:
            self._write_requests.put(_Sentinel.NO_MORE_WRITE_REQUESTS, block=False)
            self._sent_no_more_write_requests = True
        while not (self._ingester_done and self._writer_done and self._n_scanners_done == self.n_scanners):
            match self._handle_progress_reports(
                self._reports.get(block=True), already_failing=already_failing
            ):
                case None | ScanReport() | _IngestReport():
                    pass
                case _Sentinel.INGESTER_DONE:
                    self._ingester_done = True
                    self.progress.quantum_ingests.close()
                case _Sentinel.SCANNER_DONE:
                    self._n_scanners_done += 1
                    self.progress.scans.close()
                case _Sentinel.WRITER_DONE:
                    self._writer_done = True
                    self.progress.writes.close()
                case unexpected:
                    raise AssertionError(f"Unexpected message {unexpected!r} to supervisor.")
            self.log.verbose(
                "Blocking on reports queue: ingester_done=%s, writer_done=%s, n_scanners_done=%s.",
                self._ingester_done,
                self._writer_done,
                self._n_scanners_done,
            )
        while _get_from_queue(self._compression_dict) is not None:
            self.log.verbose("Flushing compression dict queue.")
        self.log.verbose("Checking that all queues are empty.")
        self._expect_empty_queue(self._scan_requests)
        self._expect_empty_queue(self._ingest_requests)
        if self._write_requests is not None:
            self._expect_empty_queue(self._write_requests)
        self._expect_empty_queue(self._reports)
        self._expect_empty_queue(self._compression_dict)

    def __enter__(self) -> Self:
        self.progress.__enter__()
        # We make the low-level logger in __enter__ instead of __init__ only
        # because that's the pattern used by true workers (where it matters).
        self.log = make_worker_log("supervisor", self.config)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            if exc_type is not FatalWorkerError:
                self.progress.log.critical(f"Caught {exc_type.__name__}; attempting to shut down cleanly.")
            self._cancel_event.set()
        self.wait_for_workers_to_finish(already_failing=exc_type is not None)
        self.progress.__exit__(exc_type, exc_value, traceback)

    def request_scan(self, quantum_id: uuid.UUID) -> None:
        """Send a request to the scanners to scan the given quantum.

        Parameters
        ----------
        quantum_id : `uuid.UUID`
            ID of the quantum to scan.
        """
        self._scan_requests.put(_ScanRequest(quantum_id), block=False)

    def request_write(self, request: WriteRequest) -> None:
        """Send a request to the writer to write provenance for the given scan.

        Parameters
        ----------
        request : `WriteRequest`
            Information from scanning a quantum (or knowing you don't have to,
            in the case of blocked quanta).
        """
        assert self._write_requests is not None, "Writer should not be used if writing is disabled."
        self._write_requests.put(request, block=False)

    def poll(self) -> Iterator[ScanReport]:
        """Poll for reports from workers while sending scan requests.

        Yields
        ------
        scan_report : `ScanReport`
            A report from a scanner that a quantum was scanned.

        Notes
        -----
        This iterator blocks until the first scan report is received, and then
        it continues until the report queue is empty.
        """
        block = True
        msg = _get_from_queue(self._reports, block=block)
        while msg is not None:
            match self._handle_progress_reports(msg):
                case ScanReport() as scan_report:
                    block = False
                    yield scan_report
                case None:
                    pass
                case unexpected:
                    raise AssertionError(f"Unexpected message {unexpected!r} to supervisor.")
            msg = _get_from_queue(self._reports, block=block)

    def _handle_progress_reports(
        self, report: Report, already_failing: bool = False
    ) -> (
        ScanReport
        | Literal[
            _Sentinel.SCANNER_DONE,
            _Sentinel.INGESTER_DONE,
            _Sentinel.WRITER_DONE,
        ]
        | None
    ):
        """Handle reports to the supervisor that can appear at any time, and
        are typically just updates to the progress we've made.

        This includes:

        - exceptions from workers (which raise `FatalWorkerError` here to
          trigger ``__exit__``);
        - ingest reports;
        - write reports;
        - progress logs.

        If one of these is handled, `None` is returned; otherwise the original
        report is returned.
        """
        match report:
            case _WorkerErrorMessage(traceback=traceback, worker=worker):
                self.progress.log.fatal("Exception raised on %s: \n%s", worker, traceback)
                if not already_failing:
                    raise FatalWorkerError()
            case _IngestReport(n_producers=n_producers):
                self.progress.quantum_ingests.update(n_producers)
            case _Sentinel.WRITE_REPORT:
                self.progress.writes.update(1)
            case _ProgressLog(message=message, level=level):
                self.progress.log.log(level, "%s [after %0.1fs]", message, self.progress.elapsed_time)
            case _:
                return report
        return None

    @staticmethod
    def _expect_empty_queue(queue: Queue[Any]) -> None:
        """Assert that the given queue is empty."""
        if (msg := _get_from_queue(queue, block=False, timeout=0)) is not None:
            raise AssertionError(f"Queue is not empty; found {msg!r}.")


class WorkerCommunicator:
    """A base class for non-supervisor workers.

    Parameters
    ----------
    supervisor : `SupervisorCommunicator`
        Communicator for the supervisor to grab queues and information from.
    name : `str`
        Human-readable name for this worker.

    Notes
    -----
    Each worker communicator is constructed in the main process and entered as
    a context manager on the actual worker process, so attributes that cannot
    be pickled are constructed in ``__enter__`` instead of ``__init__``.

    Worker communicators provide access to an `AggregatorConfig` and a logger
    to their workers.  As context managers, they handle exceptions and ensure
    clean shutdowns, and since most workers need to use a lot of other context
    managers (for file reading and writing, mostly), they provide an `enter`
    method to keep every worker from also having to be a context manager just
    to hold a context manager instance attribute.

    Worker communicators can also be configured to record and dump profiling
    information.
    """

    def __init__(self, supervisor: SupervisorCommunicator, name: str):
        self.name = name
        self.config = supervisor.config
        self._reports = supervisor._reports
        self._cancel_event = supervisor._cancel_event

    def __enter__(self) -> Self:
        self.log = make_worker_log(self.name, self.config)
        self.log.verbose("%s has PID %s (parent is %s).", self.name, os.getpid(), os.getppid())
        self._exit_stack = ExitStack().__enter__()
        if self.config.n_processes > 1:
            # Multiprocessing: ignore interrupts so we can shut down cleanly.
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            if self.config.worker_profile_dir is not None:
                # We use time.time because we're interested in wall-clock time,
                # not just CPU effort, since this is I/O-bound work.
                self._profiler = cProfile.Profile(timer=time.time)
                self._profiler.enable()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        if self.config.worker_profile_dir is not None and self.config.n_processes > 1:
            self._profiler.disable()
            os.makedirs(self.config.worker_profile_dir, exist_ok=True)
            self._profiler.dump_stats(os.path.join(self.config.worker_profile_dir, f"{self.name}.profile"))
        if exc_value is not None:
            assert exc_type is not None, "Should be guaranteed by Python, but MyPy doesn't know that."
            if exc_type is not FatalWorkerError:
                self.log.warning("Error raised on this worker.", exc_info=(exc_type, exc_value, traceback))
                assert exc_type is not None and traceback is not None
                self._reports.put(
                    _WorkerErrorMessage(
                        self.name,
                        "".join(format_exception(exc_type, exc_value, traceback)),
                    ),
                    block=False,
                )
                self.log.debug("Error message sent to supervisor.")
            else:
                self.log.warning("Shutting down due to exception raised on another worker.")
        self._exit_stack.__exit__(exc_type, exc_value, traceback)
        return True

    def log_progress(self, level: int, message: str) -> None:
        """Send a high-level log message to the supervisor.

        Parameters
        ----------
        level : `int`
            Log level.  Should be ``VERBOSE`` or higher.
        message : `str`
            Log message.
        """
        self._reports.put(_ProgressLog(message=message, level=level), block=False)

    def enter(
        self,
        cm: AbstractContextManager[_T],
        on_close: str | None = None,
        level: int = VERBOSE,
        is_progress_log: bool = False,
    ) -> _T:
        """Enter a context manager that will be exited when the communicator's
        context is exited.

        Parameters
        ----------
        cm : `contextlib.AbstractContextManager`
            A context manager to enter.
        on_close : `str`, optional
            A log message to emit (on the worker's logger) just before the
            given context manager is exited.  This can be used to indicate
            what's going on when an ``__exit__`` implementation has a lot of
            work to do (e.g. moving a large file into a zip archive).
        level : `int`, optional
            Level for the ``on_close`` log message.
        is_progress_log : `bool`, optional
            If `True`, send the ``on_close`` message to the supervisor via
            `log_progress` as well as the worker's logger.
        """
        if on_close is None:
            return self._exit_stack.enter_context(cm)

        @contextmanager
        def wrapper() -> Iterator[_T]:
            with cm as result:
                yield result
                self.log.log(level, on_close)
                if is_progress_log:
                    self.log_progress(level, on_close)

        return self._exit_stack.enter_context(wrapper())

    def check_for_cancel(self) -> None:
        """Check for a cancel signal from the supervisor and raise
        `FatalWorkerError` if it is present.
        """
        if self._cancel_event.is_set():
            raise FatalWorkerError()


class ScannerCommunicator(WorkerCommunicator):
    """A communicator for scanner workers.

    Parameters
    ----------
    supervisor : `SupervisorCommunicator`
        Communicator for the supervisor to grab queues and information from.
    scanner_id : `int`
        Integer ID for this canner.
    """

    def __init__(self, supervisor: SupervisorCommunicator, scanner_id: int):
        super().__init__(supervisor, f"scanner-{scanner_id:03d}")
        self.scanner_id = scanner_id
        self._scan_requests = supervisor._scan_requests
        self._ingest_requests = supervisor._ingest_requests
        self._write_requests = supervisor._write_requests
        self._compression_dict = supervisor._compression_dict
        self._got_no_more_scan_requests: bool = False
        self._sent_no_more_ingest_requests: bool = False

    def report_scan(self, msg: ScanReport) -> None:
        """Report a completed scan to the supervisor.

        Parameters
        ----------
        msg : `ScanReport`
            Report to send.
        """
        self._reports.put(msg, block=False)

    def request_ingest(self, request: IngestRequest) -> None:
        """Ask the ingester to ingest a quantum's outputs.

        Parameters
        ----------
        request : `IngestRequest`
            Description of the datasets to ingest.

        Notes
        -----
        If this request has no datasets, this automatically reports the ingest
        as complete to the supervisor instead of sending it to the ingester.
        """
        if request:
            self._ingest_requests.put(request, block=False)
        else:
            self._reports.put(_IngestReport(1), block=False)

    def request_write(self, request: WriteRequest) -> None:
        """Ask the writer to write provenance for a quantum.

        Parameters
        ----------
        request : `WriteRequest`
            Result of scanning a quantum.
        """
        assert self._write_requests is not None, "Writer should not be used if writing is disabled."
        self._write_requests.put(request, block=False)

    def get_compression_dict(self) -> bytes | None:
        """Attempt to get the compression dict from the writer.

        Returns
        -------
        data : `bytes` or `None`
            The `bytes` representation of the compression dictionary, or `None`
            if the compression dictionary is not yet available.

        Notes
        -----
        A scanner should only call this method before it actually has the
        compression dict.
        """
        if (cdict := _get_from_queue(self._compression_dict)) is not None:
            return cdict.data
        return None

    def poll(self) -> Iterator[uuid.UUID]:
        """Poll for scan requests to process.

        Yields
        ------
        quantum_id : `uuid.UUID`
            ID of a new quantum to scan.

        Notes
        -----
        This iterator ends when the supervisor reports that it is done
        traversing the graph.
        """
        while True:
            self.check_for_cancel()
            scan_request = _get_from_queue(self._scan_requests, block=True, timeout=self.config.worker_sleep)
            if scan_request is _Sentinel.NO_MORE_SCAN_REQUESTS:
                self._got_no_more_scan_requests = True
                return
            if scan_request is not None:
                yield scan_request.quantum_id

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        result = super().__exit__(exc_type, exc_value, traceback)
        self._ingest_requests.put(_Sentinel.NO_MORE_INGEST_REQUESTS, block=False)
        if self._write_requests is not None:
            self._write_requests.put(_Sentinel.NO_MORE_WRITE_REQUESTS, block=False)
        while not self._got_no_more_scan_requests:
            self.log.debug("Clearing scan request queue (~%d remaining)", self._scan_requests.qsize())
            if (
                not self._got_no_more_scan_requests
                and self._scan_requests.get() is _Sentinel.NO_MORE_SCAN_REQUESTS
            ):
                self._got_no_more_scan_requests = True
        # We let the supervisor clear out the compression dict queue, because
        # a single scanner can't know if it ever got sent out or not.
        self.log.verbose("Sending done sentinal.")
        self._reports.put(_Sentinel.SCANNER_DONE, block=False)
        return result


class IngesterCommunicator(WorkerCommunicator):
    """A communicator for the ingester worker.

    Parameters
    ----------
    supervisor : `SupervisorCommunicator`
        Communicator for the supervisor to grab queues and information from.
    """

    def __init__(self, supervisor: SupervisorCommunicator):
        super().__init__(supervisor, "ingester")
        self.n_scanners = supervisor.n_scanners
        self._ingest_requests = supervisor._ingest_requests
        self._n_requesters_done = 0

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        result = super().__exit__(exc_type, exc_value, traceback)
        while self._n_requesters_done != self.n_scanners:
            self.log.debug(
                "Waiting for %d requesters to be done (currently %d).",
                self.n_scanners,
                self._n_requesters_done,
            )
            if self._ingest_requests.get(block=True) is _Sentinel.NO_MORE_INGEST_REQUESTS:
                self._n_requesters_done += 1
        self.log.verbose("Sending done sentinal.")
        self._reports.put(_Sentinel.INGESTER_DONE, block=False)
        return result

    def report_ingest(self, n_producers: int) -> None:
        """Report to the supervisor that an ingest batch was completed.

        Parameters
        ----------
        n_producers : `int`
            Number of producing quanta whose datasets were ingested.
        """
        self._reports.put(_IngestReport(n_producers), block=False)

    def poll(self) -> Iterator[IngestRequest]:
        """Poll for ingest requests from the scanner workers.

        Yields
        ------
        request : `IngestRequest`
            A request to ingest datasets produced by a single quantum.

        Notes
        -----
        This iterator ends when all scanners indicate that they are done making
        ingest requests.
        """
        while True:
            self.check_for_cancel()
            ingest_request = _get_from_queue(self._ingest_requests, block=True, timeout=_TINY_TIMEOUT)
            if ingest_request is _Sentinel.NO_MORE_INGEST_REQUESTS:
                self._n_requesters_done += 1
                if self._n_requesters_done == self.n_scanners:
                    return
                else:
                    continue
            if ingest_request is not None:
                yield ingest_request


class WriterCommunicator(WorkerCommunicator):
    """A communicator for the writer worker.

    Parameters
    ----------
    supervisor : `SupervisorCommunicator`
        Communicator for the supervisor to grab queues and information from.
    """

    def __init__(self, supervisor: SupervisorCommunicator):
        assert supervisor._write_requests is not None
        super().__init__(supervisor, "writer")
        self.n_scanners = supervisor.n_scanners
        self._write_requests = supervisor._write_requests
        self._compression_dict = supervisor._compression_dict
        self._n_requesters = supervisor.n_scanners + 1
        self._n_requesters_done = 0
        self._sent_compression_dict = False

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        result = super().__exit__(exc_type, exc_value, traceback)
        if exc_type is None:
            self.log_progress(logging.INFO, "Provenance quantum graph written successfully.")
        while self._n_requesters_done != self._n_requesters:
            self.log.debug(
                "Waiting for %d requesters to be done (currently %d).",
                self._n_requesters,
                self._n_requesters_done,
            )
            if self._write_requests.get(block=True) is _Sentinel.NO_MORE_WRITE_REQUESTS:
                self._n_requesters_done += 1
        self.log.verbose("Sending done sentinal.")
        self._reports.put(_Sentinel.WRITER_DONE, block=False)
        return result

    def poll(self) -> Iterator[WriteRequest]:
        """Poll for writer requests from the scanner workers and supervisor.

        Yields
        ------
        request : `WriteRequest`
            The result of a quantum scan.

        Notes
        -----
        This iterator ends when all scanners and the supervisor indicate that
        they are done making write requests.
        """
        while True:
            self.check_for_cancel()
            write_request = _get_from_queue(self._write_requests, block=True, timeout=_TINY_TIMEOUT)
            if write_request is _Sentinel.NO_MORE_WRITE_REQUESTS:
                self._n_requesters_done += 1
                if self._n_requesters_done == self._n_requesters:
                    return
                else:
                    continue
            if write_request is not None:
                yield write_request

    def send_compression_dict(self, cdict_data: bytes) -> None:
        """Send the compression dictionary to the scanners.

        Parameters
        ----------
        cdict_data : `bytes`
            The `bytes` representation of the compression dictionary.
        """
        self.log.debug("Sending compression dictionary.")
        for _ in range(self.n_scanners):
            self._compression_dict.put(_CompressionDictionary(cdict_data), block=False)
        self._sent_compression_dict = True

    def report_write(self) -> None:
        """Report to the supervisor that provenance for a quantum was written
        to the graph.
        """
        self._reports.put(_Sentinel.WRITE_REPORT, block=False)

    def periodically_check_for_cancel(self, iterable: Iterable[_T], n: int = 100) -> Iterator[_T]:
        """Iterate while checking for a cancellation signal every ``n``
        iterations.

        Parameters
        ----------
        iterable : `~collections.abc.Iterable`
            Object to iterate over.
        n : `int`
            Check for cancellation every ``n`` iterations.

        Returns
        -------
        iterator : `~collections.abc.Iterator`
            Iterator.
        """
        i = 0
        for entry in iterable:
            yield entry
            i += 1
            if i % n == 0:
                self.check_for_cancel()
