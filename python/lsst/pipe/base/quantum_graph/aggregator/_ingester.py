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

from lsst.daf.butler import Butler, CollectionType, DatasetRef, DimensionGroup, FileDataset
from lsst.daf.butler.datastore.record_data import DatastoreRecordData
from lsst.daf.butler.registry import ConflictingDefinitionError
from lsst.resources import ResourcePath

from ...pipeline_graph import TaskImportMode
from .._common import DatastoreName
from .._predicted import PredictedQuantumGraphComponents, PredictedQuantumGraphReader
from ..formatter import ProvenanceFormatter
from ._communicators import IngesterCommunicator
from ._structs import GraphIngestRequest, ProvenanceIngestRequest, QuantumIngestRequest


@dataclasses.dataclass
class Ingester:
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

    n_producers_requested: int = 0
    """Number of quanta whose outputs are currently pending ingest."""

    output_refs_pending: defaultdict[DimensionGroup, list[DatasetRef]] = dataclasses.field(
        default_factory=lambda: defaultdict(list)
    )
    """Regular output dataset references pending ingest, grouped by their
    dimensions.
    """

    records_pending: dict[DatastoreName, DatastoreRecordData] = dataclasses.field(default_factory=dict)
    """Datastore records pending ingest, grouped by datastore name."""

    n_provenance_datasets_requested: int = 0
    """Number of provenance datasets requested, including those we've skipped
    because they already exist.
    """

    provenance_refs_pending: list[DatasetRef] = dataclasses.field(default_factory=list)
    """Special provenance datasets pending ingest."""

    already_ingested: set[uuid.UUID] | None = None
    """A set of all dataset IDs already present in the output RUN
    collection.

    If this is not `None`, the ingester is in defensive ingest mode, either
    because it was configured to query for these dataset IDs up front, or
    because a transaction failed due to a dataset already being present.
    """

    provenance_qg_path: ResourcePath | None = None
    """Path to the provenance QG."""

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

    @property
    def n_datasets_pending(self) -> int:
        """The number of butler datasets currently pending."""
        return sum(len(v) for v in self.output_refs_pending.values()) + len(self.provenance_refs_pending)

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
        with comms:
            ingester = Ingester(predicted_path, butler_path, comms)
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
                self.butler.registry.registerDatasetType(
                    self.comms.config.get_graph_dataset_type(self.butler.dimensions)
                )
                for task_label, n_quanta in self.predicted.header.n_task_quanta.items():
                    if n_quanta:
                        self.butler.registry.registerDatasetType(
                            self.comms.config.get_quantum_dataset_type(
                                self.predicted.pipeline_graph.tasks[task_label]
                            )
                        )
            self.butler.collections.register(self.predicted.header.output_run)
            # Updating the output chain cannot happen inside the caching
            # context.
            if self.comms.config.update_output_chain:
                self.update_output_chain()
        with self.butler.registry.caching_context():
            if self.comms.config.recover:
                self.fetch_already_ingested()
            self.comms.log.info("Startup completed in %ss.", time.time() - self.last_ingest_time)
            self.last_ingest_time = time.time()
            # Poll for ingest requests.
            for ingest_request in self.comms.poll():
                match ingest_request:
                    case QuantumIngestRequest():
                        self.n_producers_requested += 1
                        self.comms.log.debug(f"Got ingest request for producer {ingest_request.producer_id}.")
                        self.update_outputs_pending(refs=ingest_request.refs, records=ingest_request.records)
                    case ProvenanceIngestRequest():
                        self.n_provenance_datasets_requested += len(ingest_request.refs)
                        self.comms.log.debug(
                            f"Got provenance ingest request for {len(ingest_request.refs)} datasets."
                        )
                        self.update_provenance_datasets_pending(ingest_request.refs)
                    case GraphIngestRequest():
                        # The Writer is done; ingest the provenance QG
                        # immediately to let scanners start deleting datasets.
                        self.comms.log.verbose("Ingesting provenance quantum graph.")
                        self.ingest_provenance_qg(ingest_request)
                    case unexpected:
                        raise AssertionError(f"Unexpected ingest_request: {unexpected!r}.")
                if self.n_datasets_pending > self.comms.config.ingest_batch_size:
                    self.ingest()
            self.comms.log.info("All ingest requests received.")
            # We use 'while' in case this fails with a conflict and we switch
            # to defensive mode (should be at most two iterations).
            while self.n_datasets_pending:
                self.ingest()
            if self.n_producers_requested:
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
            "Gathered %d datasets (from %d quanta and %d provenance datasets) in %0.1fs.",
            self.n_datasets_pending,
            self.n_producers_requested,
            self.n_provenance_datasets_requested,
            ingest_start_time - self.last_ingest_time,
        )
        try:
            if not self.comms.config.dry_run:
                with self.butler.registry.transaction():
                    for refs in self.output_refs_pending.values():
                        self.butler.registry._importDatasets(refs, expand=False, assume_new=True)
                    if self.records_pending:
                        self.butler._datastore.import_records(self.records_pending)
                    if self.provenance_refs_pending:
                        assert self.provenance_qg_path is not None, "Graph should be ingested already."
                        file_dataset = FileDataset(
                            self.provenance_qg_path,
                            self.provenance_refs_pending,
                            formatter=ProvenanceFormatter,
                        )
                        self.butler.ingest(file_dataset, transfer=None)
            self.last_ingest_time = time.time()
            self.comms.log.verbose(
                "Ingested %d datasets (from %d quanta and %d provenance datasets) in %0.1fs.",
                self.n_datasets_pending,
                self.n_producers_requested,
                self.n_provenance_datasets_requested,
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
        self.output_refs_pending.clear()
        self.provenance_refs_pending.clear()

    def report(self) -> None:
        """Report a successful ingest to the supervisor."""
        self.comms.report_ingest(self.n_producers_requested, self.n_provenance_datasets_requested)
        self.n_producers_requested = 0
        self.n_provenance_datasets_requested = 0

    def fetch_already_ingested(self) -> None:
        """Query for the UUIDs of all dataset already present in the output
        RUN collection, and filter and pending datasets accordingly.
        """
        self.comms.log.info("Fetching all UUIDs in output collection %r.", self.predicted.header.output_run)
        self.already_ingested = set(
            self.butler.registry._fetch_run_dataset_ids(self.predicted.header.output_run)
        )
        # Filter the regular outputs.
        kept: set[uuid.UUID] = set()
        for dimensions, refs in self.output_refs_pending.items():
            filtered_refs: list[DatasetRef] = []
            for ref in refs:
                if ref.id not in self.already_ingested:
                    kept.add(ref.id)
                    filtered_refs.append(ref)
                else:
                    self.n_datasets_skipped += 1
            self.output_refs_pending[dimensions] = filtered_refs
        for datastore_name, datastore_records in list(self.records_pending.items()):
            if (filtered_records := datastore_records.subset(kept)) is not None:
                self.records_pending[datastore_name] = filtered_records
            else:
                del self.records_pending[datastore_name]
        # Filter the provenance datasets.
        filtered_refs = []
        for ref in self.provenance_refs_pending:
            if ref.id not in self.already_ingested:
                filtered_refs.append(ref)
            else:
                self.n_datasets_skipped += 1
        self.provenance_refs_pending = filtered_refs

    def update_outputs_pending(
        self,
        refs: list[DatasetRef],
        records: dict[DatastoreName, DatastoreRecordData],
    ) -> None:
        """Add a quantum ingest request to the pending-ingest data structures.

        Parameters
        ----------
        refs : `list` [ `lsst.daf.butler.DatasetRef` ]
            Registry information about regular quantum-output datasets.
        records : `dict` [ `str`, \
                `lsst.daf.butler.datastore.record_data.DatastoreRecordData` ]
            Datastore information about the quantum-output datasets.
        """
        n_given = len(refs)
        if self.already_ingested is not None:
            refs = [ref for ref in refs if ref.id not in self.already_ingested]
            kept = {ref.id for ref in refs}
            self.n_datasets_skipped += n_given - len(kept)
            records = {
                datastore_name: filtered_records
                for datastore_name, original_records in records.items()
                if (filtered_records := original_records.subset(kept)) is not None
            }
        for ref in refs:
            self.output_refs_pending[ref.datasetType.dimensions].append(ref)
        for datastore_name, datastore_records in records.items():
            if (existing_records := self.records_pending.get(datastore_name)) is not None:
                existing_records.update(datastore_records)
            else:
                self.records_pending[datastore_name] = datastore_records

    def update_provenance_datasets_pending(self, provenance_refs: list[DatasetRef]) -> None:
        """Add an ingest request to the pending-ingest data structures.

        Parameters
        ----------
        provenance_refs : `list` [ `lsst.daf.butler.DatasetRef` ]
            Provenance dataset references.
        """
        n_given = len(provenance_refs)
        if self.already_ingested is not None:
            provenance_refs = [d for d in provenance_refs if d.id not in self.already_ingested]
            self.n_datasets_skipped += n_given - len(provenance_refs)
        self.provenance_refs_pending.extend(provenance_refs)

    def ingest_provenance_qg(self, request: GraphIngestRequest) -> None:
        """Ingest the provenance QG dataset.

        Parameters
        ----------
        request : `GraphIngestRequest`
            Details of the graph ingestion request.
        """
        if not self.comms.config.dry_run:
            with self.butler.registry.transaction():
                self.butler.ingest(request.file_dataset, transfer=request.transfer, skip_existing=False)
            self.provenance_qg_path = self.butler.getURI(request.file_dataset.refs[0])
        self.comms.report_qg_ingested()
        self.comms.log_progress(logging.INFO, "Ingested provenance quantum graph.")

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
