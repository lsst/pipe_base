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
    "CancelError",
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
import multiprocessing.context
import multiprocessing.synchronize
import os
import queue
import signal
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, ExitStack, contextmanager
from traceback import TracebackException
from types import TracebackType
from typing import Any, Literal, Self

from lsst.utils.logging import VERBOSE

from ._config import AggregatorConfig
from ._progress import Progress, make_worker_log
from ._structs import IngestRequest, ScanReport, ScanResult

type Queue[_T] = queue.Queue[_T] | multiprocessing.Queue[_T]

type Event = threading.Event | multiprocessing.synchronize.Event

type Worker = threading.Thread | multiprocessing.context.SpawnProcess


class WorkerContext(ABC):
    @abstractmethod
    def make_queue(self) -> Queue[Any]:
        raise NotImplementedError()

    @abstractmethod
    def make_event(self) -> Event:
        raise NotImplementedError()

    @abstractmethod
    def make_worker(
        self, target: Callable[..., None], args: tuple[Any, ...], name: str | None = None
    ) -> Worker:
        raise NotImplementedError()


class ThreadingContext(WorkerContext):
    def make_queue(self) -> Queue[Any]:
        return queue.Queue()

    def make_event(self) -> Event:
        return threading.Event()

    def make_worker(
        self, target: Callable[..., None], args: tuple[Any, ...], name: str | None = None
    ) -> Worker:
        return threading.Thread(target=target, args=args, name=name)


class SpawnProcessContext(WorkerContext):
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


def _get_from_queue[T](q: Queue[T], block: bool = False, timeout: float | None = None) -> T | None:
    try:
        return q.get(block=block, timeout=timeout)
    except queue.Empty:
        return None


class CancelError(BaseException):
    pass


class _Sentinal(enum.Enum):
    NO_MORE_SCAN_REQUESTS = enum.auto()
    NO_MORE_INGEST_REQUESTS = enum.auto()
    NO_MORE_WRITE_REQUESTS = enum.auto()
    WRITE_REPORT = enum.auto()
    SCANNER_DONE = enum.auto()
    INGESTER_DONE = enum.auto()
    WRITER_DONE = enum.auto()


_TINY_TIMEOUT = 0.01


@dataclasses.dataclass
class _WorkerError:
    worker: str
    exception: TracebackException


@dataclasses.dataclass
class _ScanRequest:
    quantum_id: uuid.UUID


@dataclasses.dataclass
class _IngestReport:
    n_producers: int


@dataclasses.dataclass
class _ResumeCompleted:
    n_quanta_loaded: int
    n_ingests_loaded: int


@dataclasses.dataclass
class _ProgressLog:
    message: str
    level: int


@dataclasses.dataclass
class _CompressionDictionary:
    data: bytes


type Report = (
    ScanReport
    | _IngestReport
    | _ResumeCompleted
    | _WorkerError
    | _ProgressLog
    | Literal[
        _Sentinal.WRITE_REPORT,
        _Sentinal.SCANNER_DONE,
        _Sentinal.INGESTER_DONE,
        _Sentinal.WRITER_DONE,
    ]
)


class SupervisorCommunicator:
    def __init__(self, n_scanners: int, context: WorkerContext, config: AggregatorConfig) -> None:
        self.config = config
        self.progress = Progress(config)
        self.n_scanners = n_scanners
        self._scan_requests: Queue[_ScanRequest | Literal[_Sentinal.NO_MORE_SCAN_REQUESTS]] = (
            context.make_queue()
        )
        self._ingest_requests: Queue[IngestRequest | Literal[_Sentinal.NO_MORE_INGEST_REQUESTS]] = (
            context.make_queue()
        )
        self._write_requests: Queue[ScanResult | Literal[_Sentinal.NO_MORE_WRITE_REQUESTS]] | None = (
            context.make_queue() if config.output_path is not None else None
        )
        self._reports: Queue[Report] = context.make_queue()
        self._compression_dict: Queue[_CompressionDictionary] = context.make_queue()
        self._cancel_event: Event = context.make_event()

    def __enter__(self) -> Self:
        self.progress.__enter__()
        self.log = make_worker_log("supervisor", self.config)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            self.progress.log.critical(f"Caught {exc_type.__name__}; attempting to shut down cleanly.")
            self._cancel_event.set()
        else:
            self.progress.log.verbose("Waiting for workers to finish.")
        for _ in range(self.n_scanners):
            self._scan_requests.put(_Sentinal.NO_MORE_SCAN_REQUESTS, block=False)
        writer_done = True
        if self._write_requests is not None:
            self._write_requests.put(_Sentinal.NO_MORE_WRITE_REQUESTS, block=False)
            writer_done = False
        ingester_done = False
        n_scanners_done = 0
        while not (ingester_done and writer_done and n_scanners_done == self.n_scanners):
            self.log.verbose(
                "Blocking on reports queue: ingester_done=%s, writer_done=%s, n_scanners_done=%s.",
                ingester_done,
                writer_done,
                n_scanners_done,
            )
            match self._handle_progress_reports(self._reports.get(block=True)):
                case None | _ResumeCompleted() | ScanReport() | _IngestReport():
                    pass
                case _Sentinal.INGESTER_DONE:
                    ingester_done = True
                case _Sentinal.SCANNER_DONE:
                    n_scanners_done += 1
                case _Sentinal.WRITER_DONE:
                    writer_done = True
                case unexpected:
                    raise AssertionError(f"Unexpected message {unexpected!r} to supervisor.")
        while _get_from_queue(self._compression_dict) is not None:
            self.log.verbose("Flushing compression dict queue.")
            pass
        self.log.verbose("Checking that all queues are empty.")
        self._expect_empty_queue(self._scan_requests)
        self._expect_empty_queue(self._ingest_requests)
        if self._write_requests is not None:
            self._expect_empty_queue(self._write_requests)
        self._expect_empty_queue(self._reports)
        self._expect_empty_queue(self._compression_dict)
        # We emit the final INFO-level message here, because we need to squeeze
        # it in between waiting for the workers to tell us they're done and
        # telling the progress context to (in interactive mode, at least) stop
        # redirecting log messages to play nicely with progress bars.
        if exc_type is None:
            self.progress.log.info("Aggregation complete.")
        self.progress.__exit__(exc_type, exc_value, traceback)
        return None

    def poll_resuming(self) -> Iterator[ScanReport]:
        n_done: int = 0
        while True:
            match self._handle_progress_reports(self._reports.get(block=True)):
                case ScanReport() as scan_report:
                    yield scan_report
                case None:
                    pass
                case _ResumeCompleted(n_quanta_loaded=n_quanta_loaded, n_ingests_loaded=n_ingests_loaded):
                    self.progress.report_ingests(n_quanta_loaded - n_ingests_loaded)
                    n_done += 1
                    if n_done == self.n_scanners:
                        self.progress.finish_resuming()
                        return
                case unexpected:
                    raise AssertionError(f"Unexpected message {unexpected!r} to supervisor.")

    def request_scan(self, quantum_id: uuid.UUID) -> None:
        self._scan_requests.put(_ScanRequest(quantum_id), block=False)

    def request_write(self, scan_result: ScanResult) -> None:
        assert self._write_requests is not None, "Writer should not be used if writing is disabled."
        self._write_requests.put(scan_result, block=False)

    def poll_scanning(self, timeout: float) -> Iterator[ScanReport]:
        block = True
        msg = _get_from_queue(self._reports, block=block, timeout=timeout)
        while msg is not None:
            match self._handle_progress_reports(msg):
                case ScanReport() as scan_report:
                    block = False
                    yield scan_report
                case None:
                    pass
                case unexpected:
                    raise AssertionError(f"Unexpected message {unexpected!r} to supervisor.")
            msg = _get_from_queue(self._reports, block=block, timeout=timeout)
        if block:
            # We still didn't get a real scan return after a timeout.
            raise TimeoutError(f"No progress made after more than {timeout} seconds.") from None

    def _handle_progress_reports(
        self, report: Report
    ) -> (
        ScanReport
        | _ResumeCompleted
        | Literal[
            _Sentinal.SCANNER_DONE,
            _Sentinal.INGESTER_DONE,
            _Sentinal.WRITER_DONE,
        ]
        | None
    ):
        match report:
            case _WorkerError(exception=exception, worker=worker):
                exception.print()
                raise CancelError(f"Caught exception from {worker} (traceback above).")
            case _IngestReport(n_producers=n_producers):
                self.progress.report_ingests(n_producers)
            case _Sentinal.WRITE_REPORT:
                self.progress.report_write()
            case _ProgressLog(message=message, level=level):
                self.progress.log.log(level, message)
            case _:
                return report
        return None

    def _expect_empty_queue(self, queue: Queue[Any]) -> None:
        if (msg := _get_from_queue(queue, block=False, timeout=0)) is not None:
            raise AssertionError(f"Queue is not empty; found {msg!r}.")


class WorkerCommunicator:
    def __init__(self, supervisor: SupervisorCommunicator, name: str):
        self.name = name
        self.config = supervisor.config
        self._reports = supervisor._reports
        self._cancel_event = supervisor._cancel_event

    def __enter__(self) -> Self:
        self.log = make_worker_log(self.name, self.config)
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
            if exc_type is not CancelError:
                assert exc_type is not None and traceback is not None
                self._reports.put(
                    _WorkerError(self.name, TracebackException(exc_type, exc_value, traceback)), block=False
                )
            return True
        return self._exit_stack.__exit__(exc_type, exc_value, traceback)

    def log_progress(self, level: int, message: str) -> None:
        self._reports.put(_ProgressLog(message=message, level=level), block=False)

    def enter[T](
        self,
        cm: AbstractContextManager[T],
        on_close: str | None = None,
        level: int = VERBOSE,
        is_progress_log: bool = False,
    ) -> T:
        if on_close is None:
            return self._exit_stack.enter_context(cm)

        @contextmanager
        def wrapper() -> Iterator[T]:
            with cm as result:
                yield result
                self.log.log(level, on_close)
                if is_progress_log:
                    self.log_progress(level, on_close)

        return self._exit_stack.enter_context(wrapper())


class ScannerCommunicator(WorkerCommunicator):
    def __init__(self, supervisor: SupervisorCommunicator, scanner_id: int):
        super().__init__(supervisor, f"scanner-{scanner_id:03d}")
        self.scanner_id = scanner_id
        self._scan_requests = supervisor._scan_requests
        self._ingest_requests = supervisor._ingest_requests
        self._write_requests = supervisor._write_requests
        self._compression_dict = supervisor._compression_dict
        self._got_no_more_scan_requests: bool = False
        self._sent_no_more_ingest_requests: bool = False

    def return_scan(self, msg: ScanReport) -> None:
        self._reports.put(msg, block=False)

    def report_resume_completed(self, n_quanta_loaded: int, n_ingests_loaded: int) -> None:
        self._reports.put(_ResumeCompleted(n_quanta_loaded, n_ingests_loaded), block=False)

    def request_ingest(self, request: IngestRequest) -> None:
        if request:
            self._ingest_requests.put(request, block=False)
        else:
            self._reports.put(_IngestReport(1), block=False)

    def request_write(self, scan_results: ScanResult) -> None:
        assert self._write_requests is not None, "Writer should not be used if writing is disabled."
        self._write_requests.put(scan_results, block=False)

    def get_compression_dict(self) -> bytes | None:
        if (cdict := _get_from_queue(self._compression_dict)) is not None:
            return cdict.data
        return None

    def poll(self) -> Iterator[uuid.UUID | None]:
        while True:
            if self._cancel_event.is_set():
                raise CancelError()
            scan_request = _get_from_queue(self._scan_requests)
            if scan_request is _Sentinal.NO_MORE_SCAN_REQUESTS:
                self._got_no_more_scan_requests = True
                return
            yield (scan_request.quantum_id if scan_request is not None else None)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        result = super().__exit__(exc_type, exc_value, traceback)
        self._ingest_requests.put(_Sentinal.NO_MORE_INGEST_REQUESTS, block=False)
        if self._write_requests is not None:
            self._write_requests.put(_Sentinal.NO_MORE_WRITE_REQUESTS, block=False)
        while not self._got_no_more_scan_requests:
            self.log.debug("Clearing scan request queue (~%d remaining)", self._scan_requests.qsize())
            if (
                not self._got_no_more_scan_requests
                and self._scan_requests.get() is _Sentinal.NO_MORE_SCAN_REQUESTS
            ):
                self._got_no_more_scan_requests = True
        # We let the supervisor clear out the compression dict queue, because
        # a single scanner can't know if it ever got sent out or not.
        self.log.verbose("Sending done sentinal.")
        self._reports.put(_Sentinal.SCANNER_DONE, block=False)
        return result


class IngesterCommunicator(WorkerCommunicator):
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
            if self._ingest_requests.get(block=True) is _Sentinal.NO_MORE_INGEST_REQUESTS:
                self._n_requesters_done += 1
        self.log.verbose("Sending done sentinal.")
        self._reports.put(_Sentinal.INGESTER_DONE, block=False)
        return result

    def report_ingest(self, n_producers: int) -> None:
        self._reports.put(_IngestReport(n_producers), block=False)

    def poll(self) -> Iterator[IngestRequest]:
        while True:
            if self._cancel_event.is_set():
                raise CancelError()
            ingest_request = _get_from_queue(self._ingest_requests, block=True, timeout=_TINY_TIMEOUT)
            if ingest_request is _Sentinal.NO_MORE_INGEST_REQUESTS:
                self._n_requesters_done += 1
                if self._n_requesters_done == self.n_scanners:
                    return
                else:
                    continue
            if ingest_request is not None:
                yield ingest_request


class WriterCommunicator(WorkerCommunicator):
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
        while self._n_requesters_done != self._n_requesters:
            self.log.debug(
                "Waiting for %d requesters to be done (currently %d).",
                self._n_requesters,
                self._n_requesters_done,
            )
            if self._write_requests.get(block=True) is _Sentinal.NO_MORE_WRITE_REQUESTS:
                self._n_requesters_done += 1
        self.log.verbose("Sending done sentinal.")
        self._reports.put(_Sentinal.WRITER_DONE, block=False)
        return result

    def poll(self) -> Iterator[ScanResult]:
        while True:
            if self._cancel_event.is_set():
                raise CancelError()
            write_request = _get_from_queue(self._write_requests, block=True, timeout=_TINY_TIMEOUT)
            if write_request is _Sentinal.NO_MORE_WRITE_REQUESTS:
                self._got_no_more_write_requests = True
                self._n_requesters_done += 1
                if self._n_requesters_done == self._n_requesters:
                    return
                else:
                    continue
            if write_request is not None:
                yield write_request

    def send_compression_dict(self, cdict_data: bytes) -> None:
        self.log.debug("Sending compression dictionary.")
        for _ in range(self.n_scanners):
            self._compression_dict.put(_CompressionDictionary(cdict_data), block=False)
        self._sent_compression_dict = True

    def report_write(self) -> None:
        self._reports.put(_Sentinal.WRITE_REPORT, block=False)
