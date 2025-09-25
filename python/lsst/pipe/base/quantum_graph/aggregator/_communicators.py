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

import dataclasses
import enum
import multiprocessing.context
import multiprocessing.synchronize
import queue
import threading
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, Literal

from ._progress import SupervisorProgress, WorkerProgress
from ._structs import IngestConfirmation, IngestRequest, ScanReport, ScanResult

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
        return threading.Thread(target=target, args=args, name=name, daemon=True)


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
        return self._ctx.Process(target=target, args=args, name=name, daemon=True)


def _get_from_queue[_T](q: Queue[_T], block: bool = False, timeout: float | None = None) -> _T | None:
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
    NO_MORE_INGEST_CONFIRMATIONS = enum.auto()
    WRITE_REPORT = enum.auto()
    SCANNER_DONE = enum.auto()
    INGESTER_DONE = enum.auto()
    WRITER_DONE = enum.auto()


@dataclasses.dataclass
class _WorkerError:
    worker: str
    exception: BaseException


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
class _CompressionDictionary:
    data: bytes


type Report = (
    ScanReport
    | _IngestReport
    | _ResumeCompleted
    | _WorkerError
    | Literal[
        _Sentinal.WRITE_REPORT,
        _Sentinal.SCANNER_DONE,
        _Sentinal.INGESTER_DONE,
        _Sentinal.WRITER_DONE,
    ]
)


class SupervisorCommunicator:
    def __init__(
        self, n_scanners: int, context: WorkerContext, writes_enabled: bool, progress: SupervisorProgress
    ) -> None:
        self._scan_requests: Queue[_ScanRequest | Literal[_Sentinal.NO_MORE_SCAN_REQUESTS]] = (
            context.make_queue()
        )
        self._ingest_requests: Queue[IngestRequest | Literal[_Sentinal.NO_MORE_INGEST_REQUESTS]] = (
            context.make_queue()
        )
        self._write_requests: Queue[ScanResult | Literal[_Sentinal.NO_MORE_WRITE_REQUESTS]] | None = (
            context.make_queue() if writes_enabled else None
        )
        self._reports: Queue[Report] = context.make_queue()
        self._ingest_confirmations: dict[
            int,
            Queue[IngestConfirmation | Literal[_Sentinal.NO_MORE_INGEST_CONFIRMATIONS]],
        ] = {scanner_id: context.make_queue() for scanner_id in range(n_scanners)}
        self._compression_dict: Queue[_CompressionDictionary] = context.make_queue()
        self._cancel_event: Event = context.make_event()
        self._progress = progress

    @property
    def n_scanners(self) -> int:
        return len(self._ingest_confirmations)

    def poll_resuming(self) -> Iterator[ScanReport]:
        n_done: int = 0
        while True:
            match self._handle_progress_reports(self._reports.get(block=True)):
                case ScanReport() as scan_report:
                    yield scan_report
                case None:
                    pass
                case _ResumeCompleted(n_quanta_loaded=n_quanta_loaded, n_ingests_loaded=n_ingests_loaded):
                    self._progress.report_ingests(n_quanta_loaded - n_ingests_loaded)
                    n_done += 1
                    if n_done == len(self._ingest_confirmations):
                        return
                case unexpected:
                    raise AssertionError(f"Unexpected message {unexpected!r} to supervisor.")

    def cancel(self) -> None:
        self._cancel_event.set()

    def request_scan(self, quantum_id: uuid.UUID) -> None:
        self._scan_requests.put(_ScanRequest(quantum_id))

    def request_write(self, scan_result: ScanResult) -> None:
        assert self._write_requests is not None, "Writer should not be used if writing is disabled."
        self._write_requests.put(scan_result)

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

    def shutdown(self) -> None:
        for _ in self._ingest_confirmations:
            self._scan_requests.put(_Sentinal.NO_MORE_SCAN_REQUESTS)
        writer_done = True
        if self._write_requests is not None:
            self._write_requests.put(_Sentinal.NO_MORE_WRITE_REQUESTS)
            writer_done = False
        ingester_done = False
        n_scanners_done = 0
        while not (ingester_done and writer_done and n_scanners_done == len(self._ingest_confirmations)):
            match self._handle_progress_reports(self._reports.get(block=True)):
                case None:
                    pass
                case _Sentinal.INGESTER_DONE:
                    ingester_done = True
                case _Sentinal.SCANNER_DONE:
                    n_scanners_done += 1
                case _Sentinal.WRITER_DONE:
                    writer_done = True
                case unexpected:
                    raise AssertionError(f"Unexpected message {unexpected!r} to supervisor.")
        self._expect_empty_queue(self._scan_requests)
        self._expect_empty_queue(self._ingest_requests)
        if self._write_requests is not None:
            self._expect_empty_queue(self._write_requests)
        for q in self._ingest_confirmations.values():
            self._expect_empty_queue(q)
        self._expect_empty_queue(self._reports)
        self._expect_empty_queue(self._compression_dict)

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
            case _WorkerError(exception=exception):
                raise exception
            case _IngestReport(n_producers=n_producers):
                self._progress.report_ingests(n_producers)
            case _Sentinal.WRITE_REPORT:
                self._progress.report_write()
            case _:
                return report
        return None

    def _expect_empty_queue(self, queue: Queue[Any]) -> None:
        if (msg := _get_from_queue(queue, block=False, timeout=0)) is not None:
            raise AssertionError(f"Queue is not empty; found {msg!r}.")


class ScannerCommunicator:
    def __init__(self, supervisor: SupervisorCommunicator, scanner_id: int):
        self.scanner_id = scanner_id
        self.progress = WorkerProgress
        self._scan_requests = supervisor._scan_requests
        self._ingest_requests = supervisor._ingest_requests
        self._write_requests = supervisor._write_requests
        self._to_supervisor = supervisor._reports
        self._to_scanner = supervisor._ingest_confirmations[scanner_id]
        self._cancel_event = supervisor._cancel_event
        self._compression_dict = supervisor._compression_dict
        self._got_compression_dict: bool = False

    @contextmanager
    def handling_errors(self) -> Iterator[None]:
        try:
            yield
        except BaseException as err:
            self._to_supervisor.put(_WorkerError(f"Scanner {self.scanner_id}", err))

    def return_scan(self, msg: ScanReport) -> None:
        self._to_supervisor.put(msg)

    def report_resume_completed(self, n_quanta_loaded: int, n_ingests_loaded: int) -> None:
        self._to_supervisor.put(_ResumeCompleted(n_quanta_loaded, n_ingests_loaded))

    def request_ingest(self, request: IngestRequest) -> None:
        if request.pickled_datasets:
            self._ingest_requests.put(request)
        else:
            self._to_supervisor.put(_IngestReport(1))

    def request_write(self, scan_results: ScanResult) -> None:
        assert self._write_requests is not None, "Writer should not be used if writing is disabled."
        self._write_requests.put(scan_results)

    def get_compression_dict(self) -> bytes | None:
        assert not self._got_compression_dict
        if (cdict := _get_from_queue(self._compression_dict)) is not None:
            self._got_compression_dict = True
            return cdict.data
        return None

    def poll_for_scan_requests(self) -> Iterator[tuple[uuid.UUID | None, IngestConfirmation | None]]:
        while True:
            if self._cancel_event.is_set():
                raise CancelError()
            ingest_confirmation: IngestConfirmation | None = None
            match _get_from_queue(self._to_scanner):
                case None:
                    pass
                case IngestConfirmation() as ingest_confirmation:
                    assert ingest_confirmation.scanner_id == self.scanner_id
                case unexpected:
                    raise AssertionError(f"Unexpected value in to_scanner queue: {unexpected}.")
            scan_request = _get_from_queue(self._scan_requests)
            if scan_request is _Sentinal.NO_MORE_SCAN_REQUESTS:
                if ingest_confirmation is not None:
                    yield None, ingest_confirmation
                return
            yield (scan_request.quantum_id if scan_request is not None else None, ingest_confirmation)

    def poll_for_ingest_confirmations(self) -> Iterator[IngestConfirmation]:
        self._ingest_requests.put(_Sentinal.NO_MORE_INGEST_REQUESTS)
        if self._write_requests is not None:
            self._write_requests.put(_Sentinal.NO_MORE_WRITE_REQUESTS)
        while True:
            if self._cancel_event.is_set():
                raise CancelError()
            match self._to_scanner.get(block=True):
                case IngestConfirmation() as ingest_confirmation:
                    assert ingest_confirmation.scanner_id == self.scanner_id
                    yield ingest_confirmation
                case _Sentinal.NO_MORE_INGEST_CONFIRMATIONS:
                    self._to_supervisor.put(_Sentinal.SCANNER_DONE)
                    return
                case unexpected:
                    raise AssertionError(f"Unexpected value in to_scanner queue: {unexpected}.")

    def shutdown(self) -> None:
        if not self._got_compression_dict:
            self._compression_dict.get()


class IngesterCommunicator:
    def __init__(self, supervisor: SupervisorCommunicator):
        self._ingest_requests = supervisor._ingest_requests
        self._to_supervisor = supervisor._reports
        self._to_scanners = supervisor._ingest_confirmations
        self._cancel_event = supervisor._cancel_event
        self._n_requesters_done = 0

    @contextmanager
    def handling_errors(self) -> Iterator[None]:
        try:
            yield
        except BaseException as err:
            self._to_supervisor.put(_WorkerError("Ingester", err))

    def confirm_ingest(self, scanner_id: int, producer_ids: list[uuid.UUID]) -> None:
        self._to_scanners[scanner_id].put(IngestConfirmation(scanner_id, producer_ids))

    def report_ingest(self, n_producers: int) -> None:
        self._to_supervisor.put(_IngestReport(n_producers))

    def poll(self) -> Iterator[IngestRequest]:
        while True:
            if self._cancel_event.is_set():
                raise CancelError()
            ingest_request = self._ingest_requests.get(block=True)
            if ingest_request is _Sentinal.NO_MORE_INGEST_REQUESTS:
                self._n_requesters_done += 1
                if self._n_requesters_done == len(self._to_scanners):
                    return
                else:
                    continue
            yield ingest_request

    def send_no_more_ingest_confirmations(self) -> None:
        for q in self._to_scanners.values():
            q.put(_Sentinal.NO_MORE_INGEST_CONFIRMATIONS)
        self._to_supervisor.put(_Sentinal.INGESTER_DONE)


class WriterCommunicator:
    def __init__(self, supervisor: SupervisorCommunicator):
        assert supervisor._write_requests is not None
        self._write_requests = supervisor._write_requests
        self._to_supervisor = supervisor._reports
        self._compression_dict = supervisor._compression_dict
        self._cancel_event = supervisor._cancel_event
        self._n_requesters = supervisor.n_scanners + 1
        self._n_scanners = supervisor.n_scanners
        self._n_requesters_done = 0

    @contextmanager
    def handling_errors(self) -> Iterator[None]:
        try:
            yield
        except BaseException as err:
            self._to_supervisor.put(_WorkerError("Writer", err))

    def poll(self) -> Iterator[ScanResult]:
        while True:
            if self._cancel_event.is_set():
                raise CancelError()
            write_request = self._write_requests.get(block=True)
            if write_request is _Sentinal.NO_MORE_WRITE_REQUESTS:
                self._n_requesters_done += 1
                if self._n_requesters_done == self._n_requesters:
                    return
                else:
                    continue
            yield write_request

    def send_compression_dict(self, cdict_data: bytes) -> None:
        for _ in range(self._n_scanners):
            self._compression_dict.put(_CompressionDictionary(cdict_data))

    def report_write(self) -> None:
        self._to_supervisor.put(_Sentinal.WRITE_REPORT)

    def send_writer_done(self) -> None:
        self._to_supervisor.put(_Sentinal.WRITER_DONE)
