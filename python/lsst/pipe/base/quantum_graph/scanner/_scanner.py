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
import itertools
import logging
import multiprocessing
import time
import uuid
from collections.abc import Callable
from typing import ParamSpec, TypeVar

import networkx
import sqlalchemy
from sqlalchemy.orm import Session

from lsst.resources import ResourcePath, ResourcePathExpression

from ...graph_walker import GraphWalker
from ...quantum_provenance_graph import QuantumRunStatus
from .._common import QuantumIndex
from .._provenance import ProvenanceDatasetModel, ProvenanceQuantumModel
from . import db
from ._worker import (
    QuantumScanResult,
    QuantumScanStatus,
    ScannerWorker,
    ScannerWorkerConfig,
)

_LOG = logging.getLogger(__name__)

_T = TypeVar("_T")
_P = ParamSpec("_P")


class SequentialExecutor(concurrent.futures.Executor):
    def submit(
        self, fn: Callable[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs
    ) -> concurrent.futures.Future[_T]:
        f = concurrent.futures.Future[_T]()
        try:
            result = fn(*args, **kwargs)
        except BaseException as e:
            f.set_exception(e)
        else:
            f.set_result(result)
        return f

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        pass


@dataclasses.dataclass
class Scanner:
    config: ScannerWorkerConfig
    db_path: ResourcePath
    checkpoint_path: ResourcePath | None
    init_quanta_done: set[QuantumIndex] = dataclasses.field(default_factory=set)
    quantum_only_xgraph: networkx.DiGraph = dataclasses.field(default_factory=networkx.DiGraph)
    quantum_indices: dict[uuid.UUID, QuantumIndex] = dataclasses.field(default_factory=dict)
    datasets_done: set[uuid.UUID] = dataclasses.field(default_factory=set)
    db: sqlalchemy.Engine = dataclasses.field(init=False)

    @classmethod
    async def scan(
        cls,
        *,
        predicted_path: ResourcePathExpression,
        butler_path: ResourcePathExpression,
        db_path: ResourcePathExpression,
        checkpoint_path: ResourcePathExpression | None,
        n_processes: int = 1,
        config: ScannerWorkerConfig,
    ) -> None:
        predicted_path = ResourcePath(predicted_path)
        butler_path = ResourcePath(butler_path)
        db_path = ResourcePath(db_path)
        checkpoint_path = ResourcePath(checkpoint_path) if checkpoint_path is not None else None
        with ScannerWorker.open(predicted_path, butler_path, config=config) as worker:
            new: bool = False
            if checkpoint_path is not None and checkpoint_path.exists():
                db_path.transfer_from(checkpoint_path, "copy")
            elif not db_path.exists():
                new = True
            self = cls(config, db_path, checkpoint_path)
            self._load_predicted_quanta(worker)
            if new:
                db.DatabaseModel.metadata.create_all(self.db)
            else:
                self.datasets_done = db.read_progress(
                    self.db, self.init_quanta_done, self.quantum_only_xgraph
                )
            self.scan_init_outputs(worker)
            executor: concurrent.futures.Executor
            if n_processes > 1:
                executor = concurrent.futures.ProcessPoolExecutor(
                    max_workers=n_processes - 1,
                    mp_context=multiprocessing.get_context("spawn"),
                    initializer=ScannerWorker._initialize_for_pool,
                    initargs=(predicted_path, butler_path, config),
                )
            else:
                executor = SequentialExecutor()
                ScannerWorker.instance = worker
            with executor:
                await self.scan_graph(executor)

    def __post_init__(self) -> None:
        self.db = sqlalchemy.create_engine(
            f"sqlite:///{self.db_path.ospath}", connect_args={"autocommit": False}
        )

    def _load_predicted_quanta(self, worker: ScannerWorker) -> None:
        worker.reader.read_thin_graph()
        worker.reader.read_init_quanta()
        self.quantum_only_xgraph.add_edges_from(worker.reader.components.thin_graph.edges)
        self.quantum_indices = worker.reader.components.quantum_indices
        for quantum_id, quantum_index in self.quantum_indices.items():
            self.quantum_only_xgraph.nodes[quantum_index]["quantum_id"] = quantum_id
        for task_label, thin_quanta in worker.reader.components.thin_graph.quanta.items():
            for thin_quantum in thin_quanta:
                self.quantum_only_xgraph.nodes[thin_quantum.quantum_index]["task_label"] = task_label

    def scan_init_outputs(self, worker: ScannerWorker) -> None:
        db_datasets: dict[uuid.UUID, db.Dataset] = {}
        db_quanta: dict[QuantumIndex, db.Quantum] = {}
        db_reads: list[db.InitReadEdge] = []
        for predicted_quantum in worker.reader.components.init_quanta.root:
            quantum_index = self.quantum_indices[predicted_quantum.quantum_id]
            if quantum_index in self.init_quanta_done:
                continue
            all_exist: bool = True
            for connection_name, predicted_datasets in predicted_quantum.inputs.items():
                for predicted_dataset in predicted_datasets:
                    dataset_id = predicted_dataset.dataset_id
                    if dataset_id not in self.datasets_done and dataset_id not in db_datasets:
                        # This was an overall init-input, and we assume those
                        # exist because they must exist for the QG to be built.
                        provenance_dataset = ProvenanceDatasetModel.from_predicted(predicted_dataset)
                        db_datasets[dataset_id] = db.Dataset(
                            dataset_id=dataset_id,
                            exists=provenance_dataset.exists,
                            provenance=worker.compressor.compress(
                                provenance_dataset.model_dump_json().encode()
                            ),
                            producer=None,
                        )
                    db_reads.append(
                        db.InitReadEdge(
                            dataset_id=dataset_id,
                            quantum_index=quantum_index,
                            connection_name=connection_name,
                        )
                    )
            for predicted_dataset in itertools.chain.from_iterable(predicted_quantum.outputs.values()):
                scan_result = worker.scan_dataset(predicted_dataset, producer=predicted_quantum.quantum_id)
                db_datasets[predicted_dataset.dataset_id] = scan_result.to_db()
                if not scan_result.exists:
                    all_exist = False
            provenance_quantum = ProvenanceQuantumModel.from_predicted(predicted_quantum)
            provenance_quantum.status = QuantumRunStatus.SUCCESSFUL if all_exist else QuantumRunStatus.FAILED
            db_quanta[quantum_index] = db.Quantum(
                quantum_index=quantum_index,
                succeeded=all_exist,
                provenance=worker.compressor.compress(provenance_quantum.model_dump_json().encode()),
            )
        with Session(self.db) as session:
            with session.begin():
                session.add_all(db_datasets.values())
                session.add_all(db_quanta.values())
                session.add_all(db_reads)
            self.datasets_done.update(db_datasets.keys())
            self.init_quanta_done.update(db_quanta.keys())

    async def scan_graph(self, executor: concurrent.futures.Executor) -> None:
        walker = GraphWalker[QuantumIndex](self.quantum_only_xgraph.copy())
        pending: set[asyncio.Task[QuantumScanResult]] = set()
        scan_results: list[QuantumScanResult] = []
        last_progress_time = last_write_time = last_checkpoint_time = time.time()
        exiting = False
        for ready in walker:
            for quantum_index in ready:
                # Check to see if this quantum was already processed in a
                # previous invocation of this tool.
                succeeded = self.quantum_only_xgraph.nodes[quantum_index].get("succeeded", None)
                if succeeded:
                    walker.finish(quantum_index)
                elif succeeded is not None:
                    for blocked_quantum_index in walker.fail(quantum_index):
                        blocked_quantum_info = self.quantum_only_xgraph.nodes[blocked_quantum_index]
                        if "succeeded" not in blocked_quantum_info:
                            # The blocking quantum was processed in the last
                            # invocation, but this blocked quantum was not.
                            pending.add(
                                asyncio.create_task(
                                    self.process_blocked_quantum(executor, blocked_quantum_info["quantum_id"])
                                )
                            )
                else:
                    # This quantum has not been processed before (it may have
                    # been scanned, but it didn't make it into the DB and/or
                    # checkpoint).
                    quantum_info = self.quantum_only_xgraph.nodes[quantum_index]
                    pending.add(asyncio.create_task(self.scan_quantum(executor, quantum_info["quantum_id"])))
            if pending:
                done, pending = await asyncio.wait(pending, return_when="FIRST_COMPLETED")
                for scan_task in done:
                    quantum_result = scan_task.result()
                    quantum_index = self.quantum_indices[quantum_result.quantum_id]
                    match quantum_result.scan_status:
                        case QuantumScanStatus.RESCAN:
                            # Wait a while and scan for this quantum again
                            # later.
                            quantum_info = self.quantum_only_xgraph.nodes[quantum_index]
                            pending.add(asyncio.create_task(self.rescan_quantum(executor, quantum_result)))
                        case QuantumScanStatus.ABANDON:
                            # This quantum has failed and has stayed that way
                            # for long enough we should stop checking on it.
                            # Leave it unscanned, for a later invocation with
                            # assume_complete=True to act on it.
                            walker.fail(quantum_index)
                        case QuantumScanStatus.DONE:
                            # This quantum either succeeded or we're configured
                            # to consider any failures final.
                            if quantum_result.quantum_status.blocks_downstream:
                                for blocked_quantum_index in walker.fail(quantum_index):
                                    blocked_quantum_info = self.quantum_only_xgraph.nodes[
                                        blocked_quantum_index
                                    ]
                                    pending.add(
                                        asyncio.create_task(
                                            self.process_blocked_quantum(
                                                executor, blocked_quantum_info["quantum_id"]
                                            )
                                        )
                                    )
                            else:
                                walker.finish(quantum_index)
                            scan_results.append(quantum_result)
                            last_progress_time = time.time()
                        case QuantumScanStatus.BLOCKED:
                            # Something upstream of this quantum failed, so we
                            # didn't really scan it, and we don't need to
                            # update the walker.
                            scan_results.append(quantum_result)
                            last_progress_time = time.time()
            if time.time() - last_progress_time > self.config.idle_timeout:
                exiting = True
            if scan_results:
                if (
                    (len(scan_results) > self.config.write_max_quanta)
                    or (time.time() - last_write_time > self.config.write_interval)
                    or exiting
                ):
                    raise NotImplementedError("TODO: write to local DB")
                    last_write_time = time.time()
                    scan_results.clear()
                    if (
                        self.checkpoint_path
                        and time.time() - last_checkpoint_time > self.config.checkpoint_interval
                    ):
                        raise NotImplementedError("TODO: save to checkpoint")
                        last_checkpoint_time = time.time()
            if exiting:
                raise NotImplementedError("TODO: cancel running jobs, insert database rows and shut down")

    async def scan_quantum(
        self, executor: concurrent.futures.Executor, quantum_id: uuid.UUID
    ) -> QuantumScanResult:
        loop = asyncio.get_running_loop()
        quantum_scan = loop.run_in_executor(executor, ScannerWorker.scan_quantum_in_pool, quantum_id, None)
        return await quantum_scan

    async def rescan_quantum(
        self, executor: concurrent.futures.Executor, result: QuantumScanResult
    ) -> QuantumScanResult:
        assert result.wait_interval is not None, "wait_interval should be set before submitting rescan."
        await asyncio.sleep(result.wait_interval)
        loop = asyncio.get_running_loop()
        quantum_scan = loop.run_in_executor(
            executor, ScannerWorker.scan_quantum_in_pool, result.quantum_id, result
        )
        return await quantum_scan

    async def process_blocked_quantum(
        self, executor: concurrent.futures.Executor, quantum_id: uuid.UUID
    ) -> QuantumScanResult:
        loop = asyncio.get_running_loop()
        quantum_scan = loop.run_in_executor(
            executor, ScannerWorker.process_blocked_quantum_in_pool, quantum_id
        )
        return await quantum_scan
