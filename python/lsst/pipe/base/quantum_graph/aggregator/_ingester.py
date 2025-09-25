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
import pickle
import uuid
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager

from lsst.daf.butler import Butler, DatasetRef, QuantumBackedButler

from ...pipeline_graph import TaskImportMode
from .._predicted import PredictedQuantumGraphReader
from . import utils
from ._communicators import CancelError, IngesterCommunicator
from ._config import AggregatorConfig
from ._progress import BaseProgress, WorkerProgress


@dataclasses.dataclass
class Ingester:
    """A helper class for the provenance scanner that handles ingestion into
    the central butler repository.

    In multiprocessing mode, this helper runs in a dedicated process, accepting
    ingest requests from scanner workers and reporting back to them when they
    are complete.
    """

    reader: PredictedQuantumGraphReader

    config: AggregatorConfig
    """Configuration for the scanner."""

    progress: BaseProgress

    comms: IngesterCommunicator

    qbb: QuantumBackedButler = dataclasses.field(init=False)
    """A quantum-backed butler used for log and metadata reads, existence
    checks for other outputs (when necessary), and deleting datasets that
    are not needed anymore.
    """

    butler: Butler = dataclasses.field(init=False)

    returns_pending: dict[int, list[uuid.UUID]] = dataclasses.field(init=False)
    datasets_pending: list[DatasetRef] = dataclasses.field(default_factory=list)

    @classmethod
    @contextmanager
    def open(
        cls, config: AggregatorConfig, progress: BaseProgress, comms: IngesterCommunicator
    ) -> Iterator[Ingester]:
        """Construct a scanner worker in a context manager.

        Parameters
        ----------
        config : `AggregatorConfig`
            Configuration for the scanner.
        progress : `BaseProgress`
            Progress reporting helper.
        comms : `IngesterCommunicator`
            Helper for communicating with other workers.
        """
        with PredictedQuantumGraphReader.open(
            config.predicted_path, import_mode=TaskImportMode.DO_NOT_IMPORT
        ) as reader:
            reader.read_dimension_data()
            yield cls(reader=reader, config=config, progress=progress, comms=comms)

    def __post_init__(self) -> None:
        with PredictedQuantumGraphReader.open(
            self.config.predicted_path, import_mode=TaskImportMode.DO_NOT_IMPORT
        ) as reader:
            self.qbb = utils.make_qbb(self.config.butler_path, reader.pipeline_graph)
        self.butler = Butler.from_config(self.config.butler_path, writeable=not self.config.dry_run)
        self.returns_pending = defaultdict(list)

    def _loop(self) -> bool:
        try:
            for ingest_request in self.comms.poll():
                producer_ids_from_worker = self.returns_pending[ingest_request.scanner_id]
                self.progress.log.debug(
                    f"Got ingest request for {ingest_request.producer_id} from {ingest_request.scanner_id}."
                )
                producer_ids_from_worker.append(ingest_request.producer_id)
                for dataset in pickle.loads(ingest_request.pickled_datasets):
                    ref = self.reader.make_dataset_ref(dataset)
                    self.datasets_pending.append(ref)
                if len(self.datasets_pending) > self.config.ingest_batch_size:
                    self.progress.log.debug("Ingesting %d datasets.", len(self.datasets_pending))
                    self.ingest()
            if self.datasets_pending:
                self.progress.log.debug("Ingesting %d final datasets.", len(self.datasets_pending))
                self.ingest()
            self.progress.log.debug("Informing workers that ingests are complete.")
            self.comms.send_no_more_ingest_confirmations()
            return True
        except CancelError:
            self.progress.log.debug("Cancel event set.")
            return False

    def ingest(self) -> None:
        if not self.config.dry_run:
            with utils.PrescannedFileTransferSource.wrap_qbb(self.qbb, self.datasets_pending):
                self.butler.transfer_from(self.qbb, self.datasets_pending, transfer_dimensions=False)
        n_producers = 0
        for worker_id, producer_ids_from_worker in self.returns_pending.items():
            self.comms.confirm_ingest(worker_id, producer_ids_from_worker)
            n_producers += len(producer_ids_from_worker)
        self.comms.report_ingest(n_producers)
        self.datasets_pending.clear()
        self.returns_pending.clear()

    @staticmethod
    def run(config: AggregatorConfig, comms: IngesterCommunicator) -> None:
        with WorkerProgress("ingester", config) as progress:
            with comms.handling_errors():
                with Ingester.open(config, progress, comms) as ingester:
                    ingester._loop()
            progress.log.debug("Shutting down ingester.")
