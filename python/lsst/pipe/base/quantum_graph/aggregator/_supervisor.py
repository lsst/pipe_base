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

__all__ = ("aggregate_graph",)

import dataclasses
import itertools
import uuid

import astropy.units as u
import networkx

from lsst.utils.logging import getLogger
from lsst.utils.usage import get_peak_mem_usage

from ...graph_walker import GraphWalker
from ...pipeline_graph import TaskImportMode
from .._predicted import PredictedQuantumGraphComponents, PredictedQuantumGraphReader
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
from ._structs import ScanReport, ScanStatus, WriteRequest
from ._writer import Writer


@dataclasses.dataclass
class Supervisor:
    """The main process/thread for the provenance aggregator."""

    predicted_path: str
    """Path to the predicted quantum graph."""

    comms: SupervisorCommunicator
    """Communicator object for the supervisor."""

    predicted: PredictedQuantumGraphComponents = dataclasses.field(init=False)
    """Components of the predicted quantum graph."""

    walker: GraphWalker[uuid.UUID] = dataclasses.field(init=False)
    """Iterator that traverses the quantum graph."""

    n_abandoned: int = 0
    """Number of quanta we abandoned because they did not complete in time and
    we could not assume they had failed.
    """

    def __post_init__(self) -> None:
        self.comms.progress.log.info("Reading predicted quantum graph.")
        with PredictedQuantumGraphReader.open(
            self.predicted_path, import_mode=TaskImportMode.DO_NOT_IMPORT
        ) as reader:
            reader.read_thin_graph()
            reader.read_init_quanta()
            self.predicted = reader.components
        self.comms.progress.log.info("Analyzing predicted graph.")
        xgraph = networkx.DiGraph(self.predicted.thin_graph.edges)
        # Make sure all quanta are in the graph, even if they don't have any
        # quantum-only edges.
        for thin_quantum in itertools.chain.from_iterable(self.predicted.thin_graph.quanta.values()):
            xgraph.add_node(thin_quantum.quantum_id)
        # Add init quanta as nodes without edges, because the scanner should
        # only be run after init outputs are all written and hence we don't
        # care when we process them.
        for init_quantum in self.predicted.init_quanta.root:
            xgraph.add_node(init_quantum.quantum_id)
        self.walker = GraphWalker(xgraph)

    def loop(self) -> None:
        """Scan the outputs of the quantum graph to gather provenance and
        ingest outputs.
        """
        n_quanta = self.predicted.header.n_quanta + len(self.predicted.init_quanta.root)
        self.comms.progress.scans.total = n_quanta
        self.comms.progress.writes.total = n_quanta
        self.comms.progress.quantum_ingests.total = n_quanta
        ready_set: set[uuid.UUID] = set()
        for ready_quanta in self.walker:
            self.comms.log.debug("Sending %d new quanta to scan queue.", len(ready_quanta))
            ready_set.update(ready_quanta)
            while ready_set:
                self.comms.request_scan(ready_set.pop())
            for scan_return in self.comms.poll():
                self.handle_report(scan_return)

    def handle_report(self, scan_report: ScanReport) -> None:
        """Handle a report from a scanner.

        Parameters
        ----------
        scan_report : `ScanReport`
            Information about the scan.
        """
        match scan_report.status:
            case ScanStatus.SUCCESSFUL | ScanStatus.INIT:
                self.comms.log.debug("Scan complete for %s: quantum succeeded.", scan_report.quantum_id)
                self.walker.finish(scan_report.quantum_id)
            case ScanStatus.FAILED:
                self.comms.log.debug("Scan complete for %s: quantum failed.", scan_report.quantum_id)
                blocked_quanta = self.walker.fail(scan_report.quantum_id)
                for blocked_quantum_id in blocked_quanta:
                    if self.comms.config.output_path is not None:
                        self.comms.request_write(WriteRequest(blocked_quantum_id, status=ScanStatus.BLOCKED))
                    self.comms.progress.scans.update(1)
                self.comms.progress.quantum_ingests.update(len(blocked_quanta))
            case ScanStatus.ABANDONED:
                self.comms.log.debug("Abandoning scan for %s: quantum has not succeeded (yet).")
                self.walker.fail(scan_report.quantum_id)
                self.n_abandoned += 1
            case unexpected:
                raise AssertionError(
                    f"Unexpected status {unexpected!r} in scanner loop for {scan_report.quantum_id}."
                )
        self.comms.progress.scans.update(1)


def aggregate_graph(predicted_path: str, butler_path: str, config: AggregatorConfig) -> None:
    """Run the graph aggregator tool.

    Parameters
    ----------
    predicted_path : `str`
        Path to the predicted quantum graph.
    butler_path : `str`
        Path or alias to the central butler repository.
    config : `AggregatorConfig`
        Configuration for the aggregator.
    """
    log = getLogger("lsst.pipe.base.quantum_graph.aggregator")
    ctx = ThreadingContext() if config.n_processes == 1 else SpawnProcessContext()
    scanners: list[Worker] = []
    ingester: Worker
    writer: Worker | None = None
    with SupervisorCommunicator(log, config.n_processes, ctx, config) as comms:
        comms.progress.log.verbose("Starting workers.")
        if config.output_path is not None:
            writer_comms = WriterCommunicator(comms)
            writer = ctx.make_worker(
                target=Writer.run,
                args=(predicted_path, writer_comms),
                name=writer_comms.name,
            )
            writer.start()
        for scanner_id in range(config.n_processes):
            scanner_comms = ScannerCommunicator(comms, scanner_id)
            worker = ctx.make_worker(
                target=Scanner.run,
                args=(predicted_path, butler_path, scanner_comms),
                name=scanner_comms.name,
            )
            worker.start()
            scanners.append(worker)
        ingester_comms = IngesterCommunicator(comms)
        ingester = ctx.make_worker(
            target=Ingester.run,
            args=(predicted_path, butler_path, ingester_comms),
            name=ingester_comms.name,
        )
        ingester.start()
        supervisor = Supervisor(predicted_path, comms)
        supervisor.loop()
        log.info(
            "Scanning complete after %0.1fs; waiting for workers to finish.",
            comms.progress.elapsed_time,
        )
        comms.wait_for_workers_to_finish()
        if supervisor.n_abandoned:
            raise RuntimeError(
                f"{supervisor.n_abandoned} {'quanta' if supervisor.n_abandoned > 1 else 'quantum'} "
                "abandoned because they did not succeed.  Re-run with assume_complete=True after all retry "
                "attempts have been exhausted."
            )
    for w in scanners:
        w.join()
    ingester.join()
    if writer is not None and writer.is_alive():
        log.info("Waiting for writer process to close (garbage collecting can be very slow).")
        writer.join()
    # We can't get memory usage for children until they've joined.
    parent_mem, child_mem = get_peak_mem_usage()
    # This is actually an upper bound on the peak (since the peaks could be
    # at different times), but since we expect memory usage to be more smooth
    # than spiky that's fine.
    total_mem: u.Quantity = parent_mem + child_mem
    log.info(
        "All aggregation tasks complete after %0.1fs; peak memory usage ≤ %0.1f MB.",
        comms.progress.elapsed_time,
        total_mem.to(u.MB).value,
    )
