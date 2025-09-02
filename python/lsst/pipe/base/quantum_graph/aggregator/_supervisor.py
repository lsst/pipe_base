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

__all__ = ("Supervisor",)

import dataclasses
import uuid
from collections.abc import Iterator
from contextlib import contextmanager

import networkx

from ...graph_walker import GraphWalker
from ...pipeline_graph import TaskImportMode
from .._predicted import PredictedQuantumGraphReader
from ._communicators import (
    IngesterCommunicator,
    ScannerCommunicator,
    SpawnProcessContext,
    SupervisorCommunicator,
    ThreadingContext,
    Worker,
    WriterCommunicator,
)
from ._config import AggregatorConfig
from ._ingester import Ingester
from ._progress import SupervisorProgress
from ._scanner import Scanner
from ._structs import ScanReport, ScanResult, ScanStatus
from ._writer import Writer


@dataclasses.dataclass
class Supervisor:
    config: AggregatorConfig
    _reader: PredictedQuantumGraphReader
    _walker: GraphWalker[uuid.UUID]
    _progress: SupervisorProgress
    _n_abandoned: int = 0

    @classmethod
    @contextmanager
    def open(cls, config: AggregatorConfig, progress: SupervisorProgress) -> Iterator[Supervisor]:
        progress.log.verbose("Reading predicted quantum graph.")
        with PredictedQuantumGraphReader.open(
            config.predicted_path, import_mode=TaskImportMode.DO_NOT_IMPORT
        ) as reader:
            reader.read_thin_graph()
            reader.address_reader.read_all()
            reader.read_init_quanta()
            reader.read_dimension_data()
            progress.log.verbose("Analyzing predicted graph.")
            walker = cls._make_walker(reader)
            yield cls(config, reader, walker, progress)

    @staticmethod
    def _make_walker(reader: PredictedQuantumGraphReader) -> GraphWalker[uuid.UUID]:
        """Construct a graph walker from the predicted quantum graph."""
        uuid_by_index = {
            quantum_index: quantum_id
            for quantum_id, quantum_index in reader.components.quantum_indices.items()
        }
        xgraph = networkx.DiGraph(
            [(uuid_by_index[a], uuid_by_index[b]) for a, b in reader.components.thin_graph.edges]
        )
        # Add init quanta as nodes without edges, because the scanner should
        # only be run after init outputs are all written and hence we don't
        # care when we process them.
        for init_quantum in reader.components.init_quanta.root[1:]:  # skip 'packages' producer
            xgraph.add_node(init_quantum.quantum_id)
        return GraphWalker(xgraph)

    @classmethod
    def run(cls, config: AggregatorConfig) -> None:
        progress = SupervisorProgress(config)
        progress.log.info("Scanning quanta into internal database storage.")
        ctx = ThreadingContext() if config.n_processes == 1 else SpawnProcessContext()
        comms = SupervisorCommunicator(
            config.n_processes,
            ctx,
            writes_enabled=(config.output_path is not None),
            progress=progress,
        )
        workers: list[Worker] = []
        writer: Worker | None = None
        try:
            if config.output_path is not None:
                writer = ctx.make_worker(
                    target=Writer.run,
                    args=(config, WriterCommunicator(comms)),
                    name="writer",
                )
                writer.start()
            for scanner_id in range(config.n_processes):
                worker = ctx.make_worker(
                    target=Scanner.run,
                    args=(config, ScannerCommunicator(comms, scanner_id)),
                    name=f"scanner-{scanner_id:03d}",
                )
                worker.start()
                workers.append(worker)
            worker = ctx.make_worker(
                target=Ingester.run, args=(config, IngesterCommunicator(comms)), name="ingester"
            )
            worker.start()
            workers.append(worker)
            with cls.open(config, progress) as supervisor:
                supervisor._run(comms)
            progress.log.verbose("Shutting down scanner and ingester workers.")
            for w in workers:
                w.join()
            if writer is not None:
                progress.log.verbose("Moving multi-block files into zip archive.")
                writer.join()
            progress.log.info("Done writing provenance graph.")
        except BaseException as err:
            progress.log.critical("Caught %s; asking workers to shut down cleanly.", type(err).__name__)
            comms.cancel()
            raise

    def _run(self, comms: SupervisorCommunicator) -> None:
        """Scan the outputs of the quantum graph to gather provenance."""
        with self._progress.quanta(
            self._reader.header.n_quanta + len(self._reader.components.init_quanta.root) - 1  # no 'packages'
        ):
            self._progress.log.verbose("Waiting for scanners to load any previous scans.")
            for scan_return in comms.poll_resuming():
                self._handle_return(scan_return, comms)
            ready_set: set[uuid.UUID] = set()
            for ready_quanta in self._walker:
                ready_set.update(ready_quanta)
                while ready_set:
                    comms.request_scan(ready_set.pop())
                for scan_return in comms.poll_scanning(timeout=self.config.idle_timeout):
                    self._handle_return(scan_return, comms)
            if self._n_abandoned:
                raise TimeoutError(
                    f"{self._n_abandoned} {'quanta' if self._n_abandoned > 1 else 'quantum'} abandoned "
                    "after exceeding retry_timeout.  Re-run with assume_complete=True after all retry "
                    "attempts have been exhausted."
                )
            comms.shutdown()

    def _handle_return(self, scan_report: ScanReport, comms: SupervisorCommunicator) -> None:
        match scan_report.status:
            case ScanStatus.SUCCESSFUL | ScanStatus.INIT:
                self._progress.log.debug("Scan complete for %s: quantum succeeded.", scan_report.quantum_id)
                self._walker.finish(scan_report.quantum_id)
            case ScanStatus.FAILED:
                self._progress.log.debug("Scan complete for %s: quantum failed.", scan_report.quantum_id)
                blocked_quanta = self._walker.fail(scan_report.quantum_id)
                if self.config.output_path is not None:
                    for blocked_quantum_id in blocked_quanta:
                        comms.request_write(ScanResult(blocked_quantum_id, status=ScanStatus.BLOCKED))
                        self._progress.report_scan()
            case ScanStatus.ABANDONED:
                self._progress.log.debug("Abandoning scan for %s: quantum failed but may be retried.")
                self._walker.fail(scan_report.quantum_id)
                self._n_abandoned += 1
            case unexpected:
                raise AssertionError(
                    f"Unexpected status {unexpected!r} in scanner loop for {scan_report.quantum_id}."
                )
        self._progress.report_scan()
