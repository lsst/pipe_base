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
from contextlib import AbstractContextManager
from typing import Any, Literal, Self

from lsst.daf.butler import Butler, CollectionType, DatasetRef, DimensionGroup
from lsst.daf.butler.datastore.record_data import DatastoreRecordData
from lsst.daf.butler.registry import ConflictingDefinitionError

from ...pipeline_graph import TaskImportMode
from .._common import DatastoreName
from .._predicted import PredictedDatasetModel, PredictedQuantumGraphComponents, PredictedQuantumGraphReader
from ._communicators import IngesterCommunicator


@dataclasses.dataclass
class Ingester(AbstractContextManager):
    """A helper class for the provenance aggregator that handles ingestion into
    the central butler repository.
    """

    predicted_path: str
    """Path to the predicted quantum graph."""

    butler_path: str
    """Path or alias to the central butler repository."""

    comms: IngesterCommunicator
    """Communicator object for this worker."""

    predicted: PredictedQuantumGraphComponents = dataclasses.field(init=False)
    """Components of the predicted graph."""

    butler: Butler = dataclasses.field(init=False)
    """Client for the central butler repository."""

    n_datasets_ingested: int = 0
    """Total number of datasets ingested by this invocation."""

    n_datasets_skipped: int = 0
    """Total number of datasets skipped because they were already present."""

    n_producers_pending: int = 0
    """Number of quanta whose outputs are currently pending ingest."""

    refs_pending: defaultdict[DimensionGroup, list[DatasetRef]] = dataclasses.field(
        default_factory=lambda: defaultdict(list)
    )
    """Dataset references pending ingest, grouped by their dimensions."""

    records_pending: dict[DatastoreName, DatastoreRecordData] = dataclasses.field(default_factory=dict)
    """Datastore records pending ingest, grouped by datastore name."""

    already_ingested: set[uuid.UUID] | None = None
    """A set of all dataset IDs already present in the output RUN
    collection.

    If this is not `None`, the ingester is in defensive ingest mode, either
    because it was configured to query for these dataset IDs up front, or
    because a transaction failed due to a dataset already being present.
    """

    last_ingest_time: float = dataclasses.field(default_factory=time.time)
    """POSIX timestamp since the last ingest transaction concluded."""

    def __post_init__(self) -> None:
        self.comms.log.verbose("Reading from predicted quantum graph.")
        with PredictedQuantumGraphReader.open(
            self.predicted_path, import_mode=TaskImportMode.DO_NOT_IMPORT
        ) as reader:
            # We only need the header and pipeline graph.
            self.predicted = reader.components
        if self.comms.config.mock_storage_classes:
            import lsst.pipe.base.tests.mocks  # noqa: F401
        self.comms.log.verbose("Initializing butler.")
        self.butler = Butler.from_config(self.butler_path, writeable=not self.comms.config.dry_run)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        try:
            self.butler.close()
        except Exception:
            self.comms.log.exception("An exception occurred during Ingester exit")
        return False

    @property
    def n_datasets_pending(self) -> int:
        """The number of butler datasets currently pending."""
        return sum(len(v) for v in self.refs_pending.values())

    @staticmethod
    def run(predicted_path: str, butler_path: str, comms: IngesterCommunicator) -> None:
        """Run the ingester.

        Parameters
        ----------
        predicted_path : `str`
            Path to the predicted quantum graph.
        butler_path : `str`
            Path or alias to the central butler repository.
        comms : `IngesterCommunicator`
            Communicator for the ingester.

        Notes
        -----
        This method is designed to run as the ``target`` in
        `WorkerContext.make_worker`.
        """
        with comms, Ingester(predicted_path, butler_path, comms) as ingester:
            ingester.loop()

    def loop(self) -> None:
        """Run the main loop for the ingester."""
        self.comms.log.verbose("Registering collections and dataset types.")
        if not self.comms.config.dry_run:
            if self.comms.config.register_dataset_types:
                self.predicted.pipeline_graph.register_dataset_types(
                    self.butler,
                    include_inputs=False,
                    include_packages=True,
                    include_configs=True,
                    include_logs=True,
                )
            self.butler.collections.register(self.predicted.header.output_run)
            # Updating the output chain cannot happen inside the caching
            # context.
            if self.comms.config.update_output_chain:
                self.update_output_chain()
        with self.butler.registry.caching_context():
            if self.comms.config.defensive_ingest:
                self.fetch_already_ingested()
            self.comms.log.info("Startup completed in %ss.", time.time() - self.last_ingest_time)
            self.last_ingest_time = time.time()
            for ingest_request in self.comms.poll():
                self.n_producers_pending += 1
                self.comms.log.debug(f"Got ingest request for producer {ingest_request.producer_id}.")
                self.update_pending(ingest_request.datasets, ingest_request.records)
                if self.n_datasets_pending > self.comms.config.ingest_batch_size:
                    self.ingest()
            self.comms.log.info("All ingest requests received.")
            # We use 'while' in case this fails with a conflict and we switch
            # to defensive mode (should be at most two iterations).
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
        self.comms.log_progress(
            logging.INFO,
            f"Ingested {self.n_datasets_ingested} dataset(s); "
            f"skipped {self.n_datasets_skipped} already present.",
        )

    def ingest(self) -> None:
        """Ingest all pending datasets and report success to the supervisor."""
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
            self.n_datasets_ingested += self.n_datasets_pending
        except ConflictingDefinitionError:
            if self.already_ingested is None:
                self.comms.log_progress(
                    logging.INFO,
                    "Some outputs seem to have already been ingested; querying for existing datasets and "
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
        """Report a successful ingest to the supervisor."""
        self.comms.report_ingest(self.n_producers_pending)
        self.n_producers_pending = 0

    def fetch_already_ingested(self) -> None:
        """Query for the UUIDs of all dataset already present in the output
        RUN collection, and filter and pending datasets accordingly.
        """
        self.comms.log.info("Fetching all UUIDs in output collection %r.", self.predicted.header.output_run)
        self.already_ingested = set(
            self.butler.registry._fetch_run_dataset_ids(self.predicted.header.output_run)
        )
        kept: set[uuid.UUID] = set()
        for dimensions, refs in self.refs_pending.items():
            filtered_refs: list[DatasetRef] = []
            for ref in refs:
                if ref.id not in self.already_ingested:
                    kept.add(ref.id)
                    filtered_refs.append(ref)
                else:
                    self.n_datasets_skipped += 1
            self.refs_pending[dimensions] = filtered_refs
        for datastore_name, datastore_records in list(self.records_pending.items()):
            if (filtered_records := datastore_records.subset(kept)) is not None:
                self.records_pending[datastore_name] = filtered_records
            else:
                del self.records_pending[datastore_name]

    def update_pending(
        self, datasets: list[PredictedDatasetModel], records: dict[DatastoreName, DatastoreRecordData]
    ) -> None:
        """Add an ingest request to the pending-ingest data structures.

        Parameters
        ----------
        datasets : `list` [ `PredictedDatasetModel` ]
            Registry information about the datasets.
        records : `dict` [ `str`, \
                `lsst.daf.butler.datastore.record_data.DatastoreRecordData` ]
            Datastore information about the datasets.
        """
        n_given = len(datasets)
        if self.already_ingested is not None:
            datasets = [d for d in datasets if d.dataset_id not in self.already_ingested]
            kept = {d.dataset_id for d in datasets}
            self.n_datasets_skipped += n_given - len(kept)
            records = {
                datastore_name: filtered_records
                for datastore_name, original_records in records.items()
                if (filtered_records := original_records.subset(kept)) is not None
            }
        for dataset in datasets:
            ref = self.predicted.make_dataset_ref(dataset)
            self.refs_pending[ref.datasetType.dimensions].append(ref)
        for datastore_name, datastore_records in records.items():
            if (existing_records := self.records_pending.get(datastore_name)) is not None:
                existing_records.update(datastore_records)
            else:
                self.records_pending[datastore_name] = datastore_records

    def update_output_chain(self) -> None:
        """Update the output CHAINED collection to include the output RUN
        collection (and the inputs, if the output CHAINED collection does not
        exist).

        Notes
        -----
        This method cannot be called inside the registry caching context.
        """
        if self.predicted.header.output is None:
            return
        self.comms.log.info(
            "Updating output collection %s to include %s.",
            self.predicted.header.output,
            self.predicted.header.output_run,
        )
        if self.butler.collections.register(self.predicted.header.output, CollectionType.CHAINED):
            # Chain is new; need to add inputs, but we want to flatten them
            # first.
            if self.predicted.header.inputs:
                flattened = self.butler.collections.query(self.predicted.header.inputs, flatten_chains=True)
                self.butler.collections.extend_chain(self.predicted.header.output, flattened)
        self.butler.collections.prepend_chain(self.predicted.header.output, self.predicted.header.output_run)
