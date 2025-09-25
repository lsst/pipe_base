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
from ._scanner import Scanner
from ._structs import ScanReport, ScanResult, ScanStatus
from ._writer import Writer


@dataclasses.dataclass
class Supervisor:
    comms: SupervisorCommunicator
    reader: PredictedQuantumGraphReader
    walker: GraphWalker[uuid.UUID] = dataclasses.field(init=False)
    n_abandoned: int = 0

    @classmethod
    @contextmanager
    def open(cls, comms: SupervisorCommunicator) -> Iterator[Supervisor]:
        comms.progress.log.info("Reading predicted quantum graph.")
        with PredictedQuantumGraphReader.open(
            comms.config.predicted_path, import_mode=TaskImportMode.DO_NOT_IMPORT
        ) as reader:
            reader.read_thin_graph()
            reader.address_reader.read_all()
            reader.read_init_quanta()
            reader.read_dimension_data()
            yield cls(comms, reader)

    def __post_init__(self) -> None:
        # Construct a graph walker from the predicted quantum graph.
        self.comms.progress.log.info("Analyzing predicted graph.")
        uuid_by_index = {
            quantum_index: quantum_id
            for quantum_id, quantum_index in self.reader.components.quantum_indices.items()
        }
        xgraph = networkx.DiGraph(
            [(uuid_by_index[a], uuid_by_index[b]) for a, b in self.reader.components.thin_graph.edges]
        )
        # Add init quanta as nodes without edges, because the scanner should
        # only be run after init outputs are all written and hence we don't
        # care when we process them.
        for init_quantum in self.reader.components.init_quanta.root[1:]:  # skip 'packages' producer
            xgraph.add_node(init_quantum.quantum_id)
        self.walker = GraphWalker(xgraph)

    @classmethod
    def run(cls, config: AggregatorConfig) -> None:
        ctx = ThreadingContext() if config.n_processes == 1 else SpawnProcessContext()
        workers: list[Worker] = []
        with SupervisorCommunicator(config.n_processes, ctx, config) as comms:
            comms.progress.log.verbose("Starting workers.")
            if config.output_path is not None:
                writer_comms = WriterCommunicator(comms)
                worker = ctx.make_worker(target=Writer.run, args=(writer_comms,), name=writer_comms.name)
                worker.start()
                workers.append(worker)
            for scanner_id in range(config.n_processes):
                scanner_comms = ScannerCommunicator(comms, scanner_id)
                worker = ctx.make_worker(target=Scanner.run, args=(scanner_comms,), name=scanner_comms.name)
                worker.start()
                workers.append(worker)
            ingester_comms = IngesterCommunicator(comms)
            worker = ctx.make_worker(target=Ingester.run, args=(ingester_comms,), name=ingester_comms.name)
            worker.start()
            workers.append(worker)
            with cls.open(comms) as supervisor:
                supervisor.loop()
        for w in workers:
            w.join()

    def loop(self) -> None:
        """Scan the outputs of the quantum graph to gather provenance."""
        with self.comms.progress.quanta(
            self.reader.header.n_quanta + len(self.reader.components.init_quanta.root) - 1  # no 'packages'
        ):
            self.comms.progress.log.info("Waiting for scanners to load any previous scans.")
            for scan_return in self.comms.poll_resuming():
                self.handle_report(scan_return, self.comms)
            ready_set: set[uuid.UUID] = set()
            for ready_quanta in self.walker:
                self.comms.log.debug("Sending %d new quanta to scan queue.", len(ready_quanta))
                ready_set.update(ready_quanta)
                while ready_set:
                    self.comms.request_scan(ready_set.pop())
                for scan_return in self.comms.poll_scanning(timeout=self.comms.config.idle_timeout):
                    self.handle_report(scan_return, self.comms)
            if self.n_abandoned:
                raise TimeoutError(
                    f"{self.n_abandoned} {'quanta' if self.n_abandoned > 1 else 'quantum'} abandoned "
                    "after exceeding retry_timeout.  Re-run with assume_complete=True after all retry "
                    "attempts have been exhausted."
                )

    def handle_report(self, scan_report: ScanReport, comms: SupervisorCommunicator) -> None:
        match scan_report.status:
            case ScanStatus.SUCCESSFUL | ScanStatus.INIT:
                self.comms.log.debug("Scan complete for %s: quantum succeeded.", scan_report.quantum_id)
                self.walker.finish(scan_report.quantum_id)
            case ScanStatus.FAILED:
                self.comms.log.debug("Scan complete for %s: quantum failed.", scan_report.quantum_id)
                blocked_quanta = self.walker.fail(scan_report.quantum_id)
                if self.comms.config.output_path is not None:
                    for blocked_quantum_id in blocked_quanta:
                        comms.request_write(ScanResult(blocked_quantum_id, status=ScanStatus.BLOCKED))
                        self.comms.progress.report_scan()
            case ScanStatus.ABANDONED:
                self.comms.log.debug("Abandoning scan for %s: quantum failed but may be retried.")
                self.walker.fail(scan_report.quantum_id)
                self.n_abandoned += 1
            case unexpected:
                raise AssertionError(
                    f"Unexpected status {unexpected!r} in scanner loop for {scan_report.quantum_id}."
                )
        self.comms.progress.report_scan()
