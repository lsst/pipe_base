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
import multiprocessing
import time
import uuid
from collections.abc import Iterable, Iterator
from contextlib import contextmanager

import networkx

from lsst.daf.butler import Butler, DatasetRef

from ...graph_walker import GraphWalker
from . import _storage, utils
from ._config import ScannerConfig
from ._deletion_tracker import DeletionTracker
from ._results import QuantumScanStatus
from ._worker import ScannerWorker


@dataclasses.dataclass
class Scanner:
    worker: ScannerWorker
    walker: GraphWalker[uuid.UUID]
    deletion_tracker: DeletionTracker
    butler: Butler
    executor: concurrent.futures.Executor
    storage: _storage.ScannerStorage
    blocked_quanta: set[uuid.UUID] = set()

    @property
    def config(self) -> ScannerConfig:
        return self.worker.config

    @classmethod
    @contextmanager
    def open(cls, config: ScannerConfig) -> Iterator[Scanner]:
        with ScannerWorker.open(config) as worker:
            walker = cls._make_walker(worker)
            deletion_tracker = DeletionTracker.load(
                worker,
                config.delete_dataset_types,
                all_metadata=config.delete_metadata,
                all_log=config.delete_log,
            )
            executor: concurrent.futures.Executor
            if config.n_processes > 1:
                executor = concurrent.futures.ProcessPoolExecutor(
                    max_workers=config.n_processes - 1,
                    mp_context=multiprocessing.get_context("spawn"),
                    initializer=ScannerWorker._initialize_for_pool,
                    initargs=(config,),
                )
            else:
                executor = utils.SequentialExecutor()
                ScannerWorker.instance = worker
            with _storage.ScannerStorage(config) as storage:
                with executor:
                    yield cls(
                        worker,
                        walker,
                        deletion_tracker,
                        Butler.from_config(config.butler_path, writeable=True),
                        executor,
                        storage,
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

    def _load_progress(self) -> set[asyncio.Task[None]]:
        quanta = self.storage.load_progress()
        for ready in self.walker:
            progressing = False
            for quantum_id in ready:
                match quanta.pop(quantum_id, None):
                    # We can ignore the ToDelete rows returned by the deletion
                    # tracker here, because we know those rows are always going
                    # to be committed in the same transaction that added the
                    # Quantum row, and hence they're already in the DB or
                    # already acted upon and deleted.
                    case True:
                        self.walker.finish(quantum_id)
                        self.deletion_tracker.mark_quantum_complete(quantum_id)
                        progressing = True
                    case False:
                        self.blocked_quanta.update(self.walker.fail(quantum_id))
                        self.deletion_tracker.mark_quantum_complete(quantum_id)
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
        # Initialize our pending task list by deleting any datasets we'd
        # cleared for deletion in a previous invocation that was interrupted.
        pending: set[asyncio.Task[None]] = {
            asyncio.create_task(self._delete_datasets(self.storage.start_deletions(reset=True)))
        }
        # Ingest any datasets that were left in the queue by that previous run.
        self._ingest_to_central_butler()
        return pending

    async def scan_graph(self) -> None:
        pending = self._load_progress()
        last_checkpoint = time.time()
        last_ingest = time.time()
        try:
            # Main loop: schedule scans for quanta
            while (ready := next(self.walker, None)) is not None or pending:
                if ready is not None:
                    for quantum_id in ready:
                        pending.add(asyncio.create_task(self._scan_quantum(quantum_id)))
                done, pending = await asyncio.wait(
                    pending, return_when="FIRST_COMPLETED", timeout=self.config.idle_timeout
                )
                if not done:
                    raise TimeoutError(f"No progress made for {self.config.idle_timeout}s.")
                if time.time() - last_checkpoint > self.config.checkpoint_interval:
                    self.storage.checkpoint()
                    pending.add(asyncio.create_task(self._delete_datasets(self.storage.start_deletions())))
                if time.time() - last_ingest > self.config.ingest_interval:
                    self._ingest_to_central_butler()
        finally:
            self._ingest_to_central_butler()
            self.storage.checkpoint()

    async def _scan_quantum(self, quantum_id: uuid.UUID) -> None:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            self.executor, ScannerWorker.scan_quantum_in_pool, quantum_id, None
        )
        while result.status is QuantumScanStatus.INCOMPLETE:
            assert result.wait_interval is not None, "wait_interval should be set by initial scan"
            await asyncio.sleep(result.wait_interval)
            result = await loop.run_in_executor(
                self.executor, ScannerWorker.scan_quantum_in_pool, result.quantum_id, result
            )
        if result.status is QuantumScanStatus.ABANDONED:
            # This quantum has failed and has stayed that way for long
            # enough we should stop checking on it. Leave it unscanned, for
            # a later invocation with assume_complete=True to act on it.
            self.walker.fail(result.quantum_id)
            return
        match result.status:
            case QuantumScanStatus.SUCCESSFUL:
                self.walker.finish(result.quantum_id)
            case QuantumScanStatus.FAILED:
                self.last_progress_time = time.time()
            case unexpected:
                raise AssertionError(f"Unexpected status {unexpected!r} in scanner loop for {quantum_id}.")
        predicted_outputs_by_id = {
            d.dataset_id: d for d in itertools.chain.from_iterable(result.predicted.outputs.values())
        }
        to_delete = self.deletion_tracker.mark_quantum_complete(result.quantum_id)
        to_ingest: list[bytes] = []
        for dataset in result.iter_all_outputs():
            ref = self.worker.make_ref(predicted_outputs_by_id[dataset.dataset_id])
            json_ref = ref.to_json().encode()
            to_delete.update(
                self.deletion_tracker.mark_dataset_complete(
                    ref.datasetType.name, dataset.dataset_id, json_ref
                )
            )
            to_ingest.append(json_ref)
        self.storage.save_quantum(result, to_ingest, to_delete.items())
        self.last_progress_time = time.time()

    async def _delete_datasets(self, to_delete: Iterable[bytes]) -> None:
        loop = asyncio.get_running_loop()
        pending = {
            loop.run_in_executor(self.executor, ScannerWorker.delete_dataset_in_pool, json_ref)
            for json_ref in to_delete
        }
        deleted_dataset_ids = [f.result() for f in asyncio.as_completed(pending)]
        self.storage.finish_deletions(deleted_dataset_ids)

    def _ingest_to_central_butler(self) -> None:
        with self.storage.flush_ingest_queue() as json_refs:
            refs = [DatasetRef.from_json(json_ref) for json_ref in json_refs]
            with utils.ScannedFileTransferSource.wrap(self.worker.qbb, refs):
                self.butler.transfer_from(self.worker.qbb, refs, transfer_dimensions=False)

    async def finish(self) -> None:
        raise NotImplementedError("TODO")
