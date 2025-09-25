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

__all__ = ("Ingester",)

import dataclasses
import uuid
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager

from lsst.daf.butler import Butler, DatasetRef, QuantumBackedButler
from lsst.resources import ResourcePath

from ...pipeline_graph import TaskImportMode
from .._predicted import PredictedQuantumGraphReader
from . import utils
from ._communicators import IngesterCommunicator


@dataclasses.dataclass
class Ingester:
    """A helper class for the provenance scanner that handles ingestion into
    the central butler repository.

    In multiprocessing mode, this helper runs in a dedicated process, accepting
    ingest requests from scanner workers and reporting back to them when they
    are complete.
    """

    comms: IngesterCommunicator

    reader: PredictedQuantumGraphReader

    qbb: QuantumBackedButler = dataclasses.field(init=False)
    """A quantum-backed butler used for log and metadata reads, existence
    checks for other outputs (when necessary), and deleting datasets that
    are not needed anymore.
    """

    butler: Butler = dataclasses.field(init=False)

    returns_pending: dict[int, list[uuid.UUID]] = dataclasses.field(init=False)
    datasets_pending: list[DatasetRef] = dataclasses.field(default_factory=list)
    artifacts_pending: dict[ResourcePath, bool] = dataclasses.field(default_factory=dict)

    @classmethod
    @contextmanager
    def open(cls, comms: IngesterCommunicator) -> Iterator[Ingester]:
        with PredictedQuantumGraphReader.open(
            comms.config.predicted_path, import_mode=TaskImportMode.DO_NOT_IMPORT
        ) as reader:
            reader.read_dimension_data()
            yield cls(comms, reader)

    def __post_init__(self) -> None:
        if self.comms.config.enable_mocks:
            import lsst.pipe.base.tests.mocks  # noqa: F401
        self.qbb = utils.make_qbb(self.comms.config.butler_path, self.reader.pipeline_graph)
        self.butler = Butler.from_config(
            self.comms.config.butler_path, writeable=not self.comms.config.dry_run
        )
        self.returns_pending = defaultdict(list)

    @staticmethod
    def run(comms: IngesterCommunicator) -> None:
        with comms, Ingester.open(comms) as ingester:
            ingester.loop()

    def loop(self) -> None:
        self.comms.log.info("Starting ingester.")
        for ingest_request in self.comms.poll():
            producer_ids_from_worker = self.returns_pending[ingest_request.scanner_id]
            self.comms.log.debug(
                f"Got ingest request for {ingest_request.producer_id} from {ingest_request.scanner_id}."
            )
            producer_ids_from_worker.append(ingest_request.producer_id)
            predicted_datasets, artifacts = ingest_request.unpack()
            for predicted_dataset in predicted_datasets:
                ref = self.reader.make_dataset_ref(predicted_dataset)
                self.datasets_pending.append(ref)
            self.artifacts_pending.update(dict.fromkeys(artifacts, True))
            if len(self.datasets_pending) > self.comms.config.ingest_batch_size:
                self.comms.log.verbose("Ingesting %d datasets.", len(self.datasets_pending))
                self.ingest()
        self.comms.log.info("All ingest requests received.")
        if self.datasets_pending:
            self.comms.log.verbose("Ingesting %d final datasets.", len(self.datasets_pending))
            self.ingest()
        self.comms.log.info("Informing workers that ingests are complete.")

    def ingest(self) -> None:
        with utils.PrescannedFileTransferSource.wrap_qbb(self.qbb, self.artifacts_pending):
            self.butler.transfer_from(
                self.qbb,
                self.datasets_pending,
                transfer=None,
                transfer_dimensions=False,
                register_dataset_types=True,
                dry_run=self.comms.config.dry_run,
            )
        n_producers = 0
        for worker_id, producer_ids_from_worker in self.returns_pending.items():
            self.comms.confirm_ingest(worker_id, producer_ids_from_worker)
            n_producers += len(producer_ids_from_worker)
        self.comms.report_ingest(n_producers)
        self.datasets_pending.clear()
        self.returns_pending.clear()
        self.artifacts_pending.clear()
