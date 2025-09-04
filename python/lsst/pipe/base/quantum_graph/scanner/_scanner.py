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

import asyncio
import concurrent.futures
import dataclasses
import multiprocessing
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager

import networkx
import sqlalchemy
from sqlalchemy.orm import Session

from lsst.daf.butler import Butler

from ...graph_walker import GraphWalker
from . import db
from ._config import ScannerConfig
from ._results import QuantumScanResult, QuantumScanStatus
from ._worker import ScannerWorker
from ._writer import ScannerWriter


@dataclasses.dataclass
class Scanner:
    worker: ScannerWorker
    walker: GraphWalker[uuid.UUID]
    engine: sqlalchemy.Engine
    butler: Butler
    executor: concurrent.futures.Executor
    writer: ScannerWriter = dataclasses.field(default_factory=ScannerWriter)

    @property
    def config(self) -> ScannerConfig:
        return self.worker.config

    @classmethod
    @contextmanager
    def open(cls, config: ScannerConfig) -> Iterator[Scanner]:
        with ScannerWorker.open(config) as worker:
            walker = cls._make_walker(worker)
            if config.checkpoint_path is not None and config.checkpoint_path.exists():
                config.db_path.transfer_from(config.checkpoint_path, "copy")
            elif not config.db_path.exists():
                raise FileNotFoundError(f"Scanner database {config.db_path} does not exist.")
            executor: concurrent.futures.Executor
            if config.n_processes > 1:
                executor = concurrent.futures.ProcessPoolExecutor(
                    max_workers=config.n_processes - 1,
                    mp_context=multiprocessing.get_context("spawn"),
                    initializer=ScannerWorker._initialize_for_pool,
                    initargs=(config,),
                )
            else:
                from .utils import SequentialExecutor

                executor = SequentialExecutor()
                ScannerWorker.instance = worker
            with executor:
                yield cls(
                    worker,
                    walker,
                    Butler.from_config(config.butler_path, writeable=True),
                    config.create_db_engine(),
                    executor,
                )

    @staticmethod
    def _make_walker(worker: ScannerWorker) -> GraphWalker[uuid.UUID]:
        worker.reader.read_thin_graph()
        uuid_by_index = {
            quantum_index: quantum_id
            for quantum_id, quantum_index in worker.reader.components.quantum_indices.items()
        }
        xgraph = networkx.DiGraph(
            [(uuid_by_index[a], uuid_by_index[b]) for a, b in worker.reader.components.thin_graph.edges]
        )
        return GraphWalker(xgraph)

    def _load_progress(self) -> None:
        with Session(self.engine) as session:
            quanta = {
                row.quantum_id: row.succeeded
                for row in session.execute(sqlalchemy.select(db.Quantum.quantum_id, db.Quantum.successful))
            }
            self.writer.n_uningested = session.execute(
                sqlalchemy.select(sqlalchemy.sql.count()).select_from(db.ToIngest)
            )
        for ready in self.walker:
            progressing = False
            for quantum_id in ready:
                match quanta.pop(quantum_id, None):
                    case True:
                        self.walker.finish(quantum_id)
                        progressing = True
                    case False:
                        self.walker.fail(quantum_id)
                        progressing = True
                    case None:
                        self.walker.defer(quantum_id)
                    case unexpected:
                        raise AssertionError(
                            f"Unexpected status {unexpected!r} in database for {quantum_id}."
                        )
            if not progressing:
                break
        assert not quanta, f"Logic error in loading graph progress from database: {quanta} not in walker."

    async def scan_graph(self) -> None:
        pending: set[asyncio.Future[QuantumScanResult]] = set()
        last_progress_time = time.time()
        exiting = False
        for ready in self.walker:
            for quantum_id in ready:
                pending.add(self._submit_quantum_scan(quantum_id))
            if pending:
                done, pending = await asyncio.wait(pending, return_when="FIRST_COMPLETED")
                for scan_task in done:
                    result = scan_task.result()
                    quantum_id = result.quantum_id
                    match result.status:
                        case QuantumScanStatus.INCOMPLETE:
                            # Wait a while and scan for this quantum again
                            # later.
                            pending.add(self._submit_quantum_rescan(result))
                        case QuantumScanStatus.ABANDONED:
                            # This quantum has failed and has stayed that way
                            # for long enough we should stop checking on it.
                            # Leave it unscanned, for a later invocation with
                            # assume_complete=True to act on it.
                            self.walker.fail(quantum_id)
                        case QuantumScanStatus.SUCCESSFUL:
                            self.walker.finish(quantum_id)
                            self.writer.add_quantum_scan(result, self.worker)
                            last_progress_time = time.time()
                        case QuantumScanStatus.FAILED:
                            # We ignore the blocked quanta for now, because
                            # generating their provenance does not require any
                            # I/O or existence checks and we want to keep the
                            # progress DB compact.
                            self.walker.fail(quantum_id)
                            self.writer.add_quantum_scan(result, self.worker)
                            last_progress_time = time.time()
                        case unexpected:
                            raise AssertionError(
                                f"Unexpected status {unexpected!r} in scanner loop for {quantum_id}."
                            )
            if time.time() - last_progress_time > self.config.idle_timeout:
                exiting = True
            self.writer.write_if_ready(self.config, self.engine, self.butler, self.worker, force_all=exiting)
            if exiting:
                raise NotImplementedError("TODO: cancel running jobs, insert database rows and shut down")

    def _submit_quantum_scan(self, quantum_id: uuid.UUID) -> asyncio.Future[QuantumScanResult]:
        loop = asyncio.get_running_loop()
        return loop.run_in_executor(self.executor, ScannerWorker.scan_quantum_in_pool, quantum_id, None)

    def _submit_quantum_rescan(self, result: QuantumScanResult) -> asyncio.Future[QuantumScanResult]:
        return asyncio.create_task(self._rescan_quantum(result))

    async def _rescan_quantum(self, result: QuantumScanResult) -> QuantumScanResult:
        assert result.wait_interval is not None, "wait_interval should be set before submitting rescan."
        await asyncio.sleep(result.wait_interval)
        loop = asyncio.get_running_loop()
        quantum_scan = loop.run_in_executor(
            self.executor, ScannerWorker.scan_quantum_in_pool, result.quantum_id, result
        )
        return await quantum_scan
