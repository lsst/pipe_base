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
import logging
import time
import uuid
from collections import defaultdict

from lsst.daf.butler import Butler, CollectionType, DatasetRef, DimensionGroup
from lsst.daf.butler.datastore.record_data import DatastoreRecordData
from lsst.daf.butler.registry import ConflictingDefinitionError

from ...pipeline_graph import TaskImportMode
from .._common import DatastoreName
from .._predicted import PredictedDatasetModel, PredictedQuantumGraphReader
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

    reader: PredictedQuantumGraphReader = dataclasses.field(init=False)

    butler: Butler = dataclasses.field(init=False)

    n_producers_pending: int = 0
    refs_pending: defaultdict[DimensionGroup, list[DatasetRef]] = dataclasses.field(
        default_factory=lambda: defaultdict(list)
    )
    records_pending: dict[DatastoreName, DatastoreRecordData] = dataclasses.field(default_factory=dict)
    already_ingested: set[uuid.UUID] | None = None
    last_ingest_time: float = dataclasses.field(default_factory=time.time)

    def __post_init__(self) -> None:
        self.reader = self.comms.enter(
            PredictedQuantumGraphReader.open(
                self.comms.config.predicted_path, import_mode=TaskImportMode.DO_NOT_IMPORT
            )
        )
        if self.comms.config.enable_mocks:
            import lsst.pipe.base.tests.mocks  # noqa: F401
        self.butler = Butler.from_config(
            self.comms.config.butler_path, writeable=not self.comms.config.dry_run
        )

    @property
    def n_datasets_pending(self) -> int:
        return sum(len(v) for v in self.refs_pending.values())

    @staticmethod
    def run(comms: IngesterCommunicator) -> None:
        with comms:
            ingester = Ingester(comms)
            ingester.loop()

    def loop(self) -> None:
        self.comms.log.info("Starting ingester.")
        self.butler.collections.register(self.reader.header.output_run)
        if self.comms.config.register_dataset_types:
            self.reader.pipeline_graph.register_dataset_types(
                self.butler,
                include_inputs=False,
                include_packages=True,
                include_configs=True,
                include_logs=True,
            )
        if self.comms.config.defensive_ingest:
            self.fetch_already_ingested()
        self.comms.log.info("Startup completed in %ss.", time.time() - self.last_ingest_time)
        self.last_ingest_time = time.time()
        for ingest_request in self.comms.poll():
            self.n_producers_pending += 1
            self.comms.log.debug(
                f"Got ingest request for {ingest_request.producer_id} from {ingest_request.scanner_id}."
            )
            datasets, datastore_records = ingest_request.unpack()
            self.update_pending(datasets, datastore_records)
            if self.n_datasets_pending > self.comms.config.ingest_batch_size:
                self.ingest()
        self.comms.log.info("All ingest requests received.")
        # We use 'while' in case this fails with a conflict and we switch to
        # switch to defensive mode (should be at most two iterations).
        ingest_start_time = time.time()
        while self.n_datasets_pending:
            n_datasets = self.n_datasets_pending
            self.ingest()
            self.comms.log.verbose(
                "Gathered %d final datasets in %ss and ingested them in %ss.",
                n_datasets,
                ingest_start_time - self.last_ingest_time,
                time.time() - ingest_start_time,
            )
        if self.n_producers_pending:
            # We can finish with returns pending if we filtered out all of
            # the datasets we started with as already existing.
            self.report()
        if self.comms.config.update_output_chain:
            self.update_output_chain()
        self.comms.log.info("Informing workers that ingests are complete.")

    def ingest(self) -> None:
        ingest_start_time = time.time()
        self.comms.log.verbose(
            "Gathered %d datasets from %d quanta in %ss.",
            self.n_datasets_pending,
            self.n_producers_pending,
            ingest_start_time - self.last_ingest_time,
        )
        try:
            if not self.comms.config.dry_run:
                with self.butler.registry.transaction():
                    for refs in self.refs_pending.values():
                        self.butler.registry._importDatasets(refs, expand=False, assume_new=True)
                    self.butler._datastore.import_records(self.records_pending)
            self.last_ingest_time = time.time()
            self.comms.log.verbose(
                "Ingested %d datasets from %d quanta in %ss.",
                self.n_datasets_pending,
                self.n_producers_pending,
                self.last_ingest_time - ingest_start_time,
            )
        except ConflictingDefinitionError:
            if self.already_ingested is None:
                self.comms.log_progress(
                    logging.WARNING,
                    "Conflict in ingest attempt; querying for existing datasets and "
                    "switching to defensive ingest mode.",
                )
                self.fetch_already_ingested()
                # We just return instead of trying again immediately because we
                # might have just shrunk the number of pending datasets below
                # the batch threshold.
                return
            else:
                raise
        self.report()
        self.records_pending.clear()
        self.refs_pending.clear()

    def report(self) -> None:
        self.comms.report_ingest(self.n_producers_pending)
        self.n_producers_pending = 0

    def fetch_already_ingested(self) -> None:
        self.comms.log.info("Fetching all UUIDs in output collection %r.", self.reader.header.output_run)
        self.already_ingested = set(
            self.butler.registry._fetch_run_dataset_ids(self.reader.header.output_run)
        )
        kept: set[uuid.UUID] = set()
        for dimensions, refs in self.refs_pending.items():
            filtered_refs: list[DatasetRef] = []
            for ref in refs:
                if ref.id not in self.already_ingested:
                    kept.add(ref.id)
                    filtered_refs.append(ref)
            self.refs_pending[dimensions] = filtered_refs
        for datastore_name, datastore_records in list(self.records_pending.items()):
            if (filtered_records := datastore_records.subset(kept)) is not None:
                self.records_pending[datastore_name] = filtered_records
            else:
                del self.records_pending[datastore_name]

    def update_pending(
        self, datasets: list[PredictedDatasetModel], records: dict[DatastoreName, DatastoreRecordData]
    ) -> None:
        if self.already_ingested is not None:
            datasets = [d for d in datasets if d.dataset_id not in self.already_ingested]
            kept = {d.dataset_id for d in datasets}
            records = {
                datastore_name: filtered_records
                for datastore_name, original_records in records.items()
                if (filtered_records := original_records.subset(kept)) is not None
            }
        for dataset in datasets:
            ref = self.reader.make_dataset_ref(dataset)
            self.refs_pending[ref.datasetType.dimensions].append(ref)
        for datastore_name, datastore_records in records.items():
            if (existing_records := self.records_pending.get(datastore_name)) is not None:
                existing_records.update(datastore_records)
            else:
                self.records_pending[datastore_name] = datastore_records

    def update_output_chain(self) -> None:
        if self.reader.header.output is None:
            return
        if self.butler.collections.register(self.reader.header.output, CollectionType.CHAINED):
            # Chain is new; need to add inputs, but we want to flatten them
            # first.
            if self.reader.header.inputs:
                flattened = self.butler.collections.query(self.reader.header.inputs, flatten_chains=True)
                self.butler.collections.extend_chain(self.reader.header.output, flattened)
        self.butler.collections.prepend_chain(self.reader.header.output, self.reader.header.output_run)
