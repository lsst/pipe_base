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

import dataclasses
import itertools
import time
import uuid
from collections.abc import Iterable, Iterator
from contextlib import contextmanager

import sqlalchemy
from sqlalchemy.orm import Session

from lsst.daf.butler import Butler, DatasetRef, QuantumBackedButler
from lsst.daf.butler.datastore import FileTransferMap, FileTransferSource
from lsst.resources import ResourcePath

from .._predicted import PredictedQuantumDatasetsModel
from . import db
from ._config import ScannerConfig
from ._results import DatasetScanResult, QuantumScanResult
from ._worker import ScannerWorker


@dataclasses.dataclass
class ScannerWriter:
    last_write: float = dataclasses.field(default_factory=time.time)
    last_checkpoint: float = dataclasses.field(default_factory=time.time)
    last_ingest: float = dataclasses.field(default_factory=time.time)
    datasets: list[db.Dataset] = dataclasses.field(default_factory=list)
    quanta: list[db.Quantum | db.InitQuantum] = dataclasses.field(default_factory=list)
    to_ingest: list[db.ToIngest] = dataclasses.field(default_factory=list)
    n_uningested: int = 0

    def add_quantum_scan(self, quantum_scan: QuantumScanResult, worker: ScannerWorker) -> None:
        self.quanta.append(quantum_scan.to_db())
        assert quantum_scan.outputs is not None, (
            f"Cannot write incomplete scan for {quantum_scan.quantum_id} with not outputs."
        )
        self.add_dataset_scans(
            quantum_scan.predicted,
            itertools.chain(quantum_scan.outputs, quantum_scan.metadata, quantum_scan.log),
            worker,
        )

    def add_dataset_scans(
        self,
        predicted_quantum: PredictedQuantumDatasetsModel,
        dataset_scans: Iterable[DatasetScanResult],
        worker: ScannerWorker,
    ) -> None:
        predicted_outputs_by_id = {
            d.dataset_id: d for d in itertools.chain.from_iterable(predicted_quantum.outputs.values())
        }
        for dataset_result in dataset_scans:
            self.datasets.append(dataset_result.to_db())
            if dataset_result.exists:
                ref = worker.make_ref(predicted_outputs_by_id[dataset_result.dataset_id])
                self.to_ingest.append(
                    db.ToIngest(
                        dataset_id=dataset_result.dataset_id,
                        ref=ref.to_simple().model_dump_json().encode(),
                    )
                )
                self.n_uningested += 1

    def write_if_ready(
        self,
        config: ScannerConfig,
        engine: sqlalchemy.Engine,
        butler: Butler,
        worker: ScannerWorker,
        force_all: bool = False,
    ) -> None:
        start = time.time()
        ready_for_ingest = (
            force_all
            or (
                self.n_uningested > config.ingest_min_datasets
                and start - self.last_ingest > config.ingest_interval
            )
            or (self.n_uningested > config.ingest_max_datasets)
        )
        ready_for_write = force_all or (
            ready_for_ingest  # always write to local DB before ingest to central butler.
            or (start - self.last_write > config.write_interval)
            or len(self.datasets) > config.write_max_datasets
            or len(self.quanta) > config.write_max_quanta
        )
        ready_for_checkpoint = config.checkpoint_path is not None and (
            force_all or start - self.last_checkpoint > config.checkpoint_interval
        )
        if ready_for_write:
            with Session(engine) as session:
                with session.begin():
                    session.add_all(self.datasets)
                    self.datasets.clear()
                    session.add_all(self.quanta)
                    self.quanta.clear()
                    session.add_all(self.to_ingest)
                    self.to_ingest.clear()
                self.last_write = time.time()
                if ready_for_checkpoint and config.vacuum_on_checkpoint:
                    session.execute(sqlalchemy.text("VACUUM"))
        if ready_for_checkpoint:
            config.checkpoint_path.transfer_from(config.db_path, "copy")
            self.last_checkpoint = time.time()
        if ready_for_ingest:
            refs = []
            with Session(engine) as session:
                with session.begin():
                    for to_ingest in session.scalars(sqlalchemy.select(db.ToIngest)):
                        refs.append(DatasetRef.from_json(to_ingest.ref, universe=worker.qbb.dimensions))
                    for to_ingest in self.to_ingest:
                        refs.append(DatasetRef.from_json(to_ingest.ref, universe=worker.qbb.dimensions))
                    assert len(refs) == self.n_uningested, (
                        f"Mismatch in uningested dataset counts: {len(refs)} != {self.n_uningested}."
                    )
                    # Override QBB transfer hooks to report that all given file
                    # artifacts exist (without checking) since we're only
                    # asking to transfer things whose existence we've already
                    # checked, and we *really* don't want to check again.
                    with ScannedFileTransferSource.wrap(worker.qbb):
                        butler.transfer_from(worker.qbb, refs, transfer_dimensions=False)
                    session.execute(sqlalchemy.delete(db.ToIngest))
            self.n_uningested = 0
            self.last_ingest = time.time()


class ScannedFileTransferSource(FileTransferSource):
    name: str = "scanned output datasets"

    @classmethod
    @contextmanager
    def wrap(cls, qbb: QuantumBackedButler) -> Iterator[None]:
        try:
            qbb._file_transfer_source = ScannedFileTransferSource(qbb._file_transfer_source)
            yield
        finally:
            qbb._file_transfer_source = qbb._file_transfer_source._base

    def __init__(self, base: FileTransferSource):
        self._base = base

    def get_file_info_for_transfer(self, dataset_ids: Iterable[uuid.UUID]) -> FileTransferMap:
        return self._base.get_file_info_for_transfer(dataset_ids)

    def locate_missing_files_for_transfer(
        self, refs: Iterable[DatasetRef], artifact_existence: dict[ResourcePath, bool]
    ) -> FileTransferMap:
        refs = list(refs)
        for ref in refs:
            artifact_existence[ref.id] = True
        return self._base.locate_missing_files_for_transfer(refs, artifact_existence)
