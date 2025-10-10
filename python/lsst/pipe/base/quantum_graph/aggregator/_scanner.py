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

__all__ = ("Scanner",)

import dataclasses
import itertools
import uuid

import zstandard

from lsst.daf.butler import ButlerLogRecords, DataCoordinate, DatasetRef, DatasetType, QuantumBackedButler
from lsst.utils.iteration import ensure_iterable

from ... import automatic_connection_constants as acc
from ..._status import QuantumSuccessCaveats
from ..._task_metadata import TaskMetadata
from ...pipeline_graph import PipelineGraph, TaskImportMode
from ...quantum_provenance_graph import ExceptionInfo
from ...resource_usage import QuantumResourceUsage
from .._common import DatasetTypeName
from .._multiblock import Compressor
from .._predicted import (
    PredictedDatasetModel,
    PredictedQuantumDatasetsModel,
    PredictedQuantumGraphReader,
)
from ._communicators import ScannerCommunicator
from ._structs import ProvenanceIngestRequest, QuantumIngestRequest, ScanReport, ScanResult, ScanStatus


@dataclasses.dataclass
class Scanner:
    """A helper class for the provenance aggregator that reads metadata and log
    files and scans for which outputs exist.
    """

    predicted_path: str
    """Path to the predicted quantum graph."""

    butler_path: str
    """Path or alias to the central butler repository."""

    comms: ScannerCommunicator
    """Communicator object for this worker."""

    reader: PredictedQuantumGraphReader = dataclasses.field(init=False)
    """Reader for the predicted quantum graph."""

    qbb: QuantumBackedButler = dataclasses.field(init=False)
    """A quantum-backed butler used for log and metadata reads and existence
    checks for other outputs (when necessary).
    """

    compressor: Compressor | None = None
    """Object used to compress JSON blocks.

    This is `None` until a compression dictionary is received from the writer
    process.
    """

    init_quanta: dict[uuid.UUID, PredictedQuantumDatasetsModel] = dataclasses.field(init=False)
    """Dictionary mapping init quantum IDs to their predicted models."""

    remove_dataset_types: set[DatasetTypeName] = dataclasses.field(default_factory=set)
    """Set of dataset type names whose original files will be removed.

    This includes datasets that are not being retained as well as datasets that
    are being aggregated into an ingested provenance graph.
    """

    provenance_dataset_types: set[DatasetTypeName] = dataclasses.field(default_factory=set)
    """Set of dataset type names that should be ingested after the provenance
    graph dataset has been ingested, because they are backed by the same file.

    This is a subset of `remove_dataset_types`.
    """

    pending_removals: list[DatasetRef] = dataclasses.field(default_factory=list)
    """List of datasets that should be deleted by this scanner after the
    provenance graph is ingested.
    """

    pending_ingests: list[DatasetRef] = dataclasses.field(default_factory=list)
    """List of datasets that should be sent for ingest by this scanner after
    the provenance graph is ingested.

    This *almost* a subset of `pending_removals`; it also includes the special
    quantum provenance datasets.
    """

    def __post_init__(self) -> None:
        if self.comms.config.mock_storage_classes:
            import lsst.pipe.base.tests.mocks  # noqa: F401
        self.comms.log.verbose("Reading from predicted quantum graph.")
        self.reader = self.comms.enter(
            PredictedQuantumGraphReader.open(self.predicted_path, import_mode=TaskImportMode.DO_NOT_IMPORT)
        )
        self.reader.read_dimension_data()
        self.reader.read_init_quanta()
        self.comms.log.verbose("Initializing quantum-backed butler.")
        self.qbb = self.make_qbb(self.butler_path, self.reader.pipeline_graph)
        self.init_quanta = {q.quantum_id: q for q in self.reader.components.init_quanta.root}
        self.remove_dataset_types = set(self.comms.config.remove_dataset_types)
        # If we are ingesting provenance, delete original {logs, configs,
        # packages} and ingest with provenance graph backing (if at all).
        if self.comms.config.ingest_provenance:
            for task_node in self.reader.pipeline_graph.tasks.values():
                self.remove_dataset_types.add(task_node.metadata_output.dataset_type_name)
                self.provenance_dataset_types.add(task_node.metadata_output.dataset_type_name)
                self.comms.log.verbose(
                    "Will remove %s and re-ingest with provenance QG backing.",
                    task_node.metadata_output.dataset_type_name,
                )
                if task_node.log_output is not None:
                    self.remove_dataset_types.add(task_node.log_output.dataset_type_name)
                    if self.comms.config.ingest_logs:
                        self.provenance_dataset_types.add(task_node.log_output.dataset_type_name)
                        self.comms.log.verbose(
                            "Will remove %s and re-ingest with provenance QG backing.",
                            task_node.log_output.dataset_type_name,
                        )
                    else:
                        self.comms.log.verbose("Will remove %s.", task_node.log_output.dataset_type_name)
                self.remove_dataset_types.add(task_node.init.config_output.dataset_type_name)
                if self.comms.config.ingest_configs:
                    self.provenance_dataset_types.add(task_node.init.config_output.dataset_type_name)
                    self.comms.log.verbose(
                        "Will remove %s and re-ingest with provenance QG backing.",
                        task_node.init.config_output.dataset_type_name,
                    )
                else:
                    self.comms.log.verbose("Will remove %s.", task_node.init.config_output.dataset_type_name)
            self.remove_dataset_types.add(acc.PACKAGES_INIT_OUTPUT_NAME)
            if self.comms.config.ingest_packages:
                self.provenance_dataset_types.add(acc.PACKAGES_INIT_OUTPUT_NAME)
                self.comms.log.verbose(
                    "Will remove %s and re-ingest with provenance QG backing.",
                    acc.PACKAGES_INIT_OUTPUT_NAME,
                )
            else:
                self.comms.log.verbose("Will remove %s.", acc.PACKAGES_INIT_OUTPUT_NAME)

    @staticmethod
    def make_qbb(butler_config: str, pipeline_graph: PipelineGraph) -> QuantumBackedButler:
        """Make quantum-backed butler that can operate on the outputs of the
        quantum graph.

        Parameters
        ----------
        butler_config : `str`
            Path or alias for the central butler repository that shares storage
            with the quantum-backed butler.
        pipeline_graph : `..pipeline_graph.PipelineGraph`
            Graph of tasks and dataset types.

        Returns
        -------
        qbb : `lsst.daf.butler.QuantumBackedButler`
            Quantum-backed butler.  This does not have the datastore records
            needed to read overall-inputs.
        """
        return QuantumBackedButler.from_predicted(
            butler_config,
            predicted_inputs=[],
            predicted_outputs=[],
            dimensions=pipeline_graph.universe,
            # We don't need the datastore records in the QG because we're
            # only going to read metadata and logs, and those are never
            # overall inputs.
            datastore_records={},
            dataset_types={node.name: node.dataset_type for node in pipeline_graph.dataset_types.values()},
        )

    @property
    def pipeline_graph(self) -> PipelineGraph:
        """Graph of tasks and dataset types."""
        return self.reader.pipeline_graph

    @staticmethod
    def run(predicted_path: str, butler_path: str, comms: ScannerCommunicator) -> None:
        """Run the scanner.

        Parameters
        ----------
        predicted_path : `str`
            Path to the predicted quantum graph.
        butler_path : `str`
            Path or alias to the central butler repository.
        comms : `ScannerCommunicator`
            Communicator for the scanner.

        Notes
        -----
        This method is designed to run as the ``target`` in
        `WorkerContext.make_worker`.
        """
        with comms:
            scanner = Scanner(predicted_path, butler_path, comms)
            scanner.loop()

    def loop(self) -> None:
        """Run the main loop for the scanner."""
        self.comms.log.info("Scan request loop beginning.")
        for quantum_id in self.comms.poll():
            if self.compressor is None and (cdict_data := self.comms.get_compression_dict()) is not None:
                self.compressor = zstandard.ZstdCompressor(
                    self.comms.config.zstd_level, zstandard.ZstdCompressionDict(cdict_data)
                )
            self.scan_quantum(quantum_id)
        if self.comms.config.incomplete:
            return
        if self.pending_ingests or self.pending_removals:
            self.comms.log.info("Waiting for provenance QG to be ingested.")
            self.comms.wait_until_qg_ingested()
        if self.pending_ingests:
            self.comms.log.info("Sending provenance ingest requests.")
            for ref_batch in itertools.batched(self.pending_ingests, self.comms.config.ingest_batch_size):
                self.comms.log.verbose("Sending a %d-dataset provenance ingest request.", len(ref_batch))
                self.comms.request_ingest(ProvenanceIngestRequest(list(ref_batch)))
        if self.pending_removals:
            self.comms.log.info("Deleting datasets.")
            for ref_batch in itertools.batched(self.pending_removals, self.comms.config.delete_batch_size):
                self.comms.log.verbose("Removing %d datasets.", len(ref_batch))
                if not self.comms.config.dry_run:
                    self.qbb.pruneDatasets(ref_batch, unstore=True, purge=True)
                self.comms.report_removals(len(ref_batch))

    def scan_dataset(self, predicted: PredictedDatasetModel) -> bool:
        """Scan for a dataset's existence.

        Parameters
        ----------
        predicted : `.PredictedDatasetModel`
            Information about the dataset from the predicted graph.

        Returns
        -------
        exists : `bool``
            Whether the dataset exists
        """
        ref = self.reader.components.make_dataset_ref(predicted)
        return self.qbb.stored(ref)

    def scan_quantum(self, quantum_id: uuid.UUID) -> ScanResult:
        """Scan for a quantum's completion and error status, and its output
        datasets' existence.

        Parameters
        ----------
        quantum_id : `uuid.UUID`
            Unique ID for the quantum.

        Returns
        -------
        result : `ScanResult`
            Scan result struct.
        """
        if (predicted_quantum := self.init_quanta.get(quantum_id)) is not None:
            result = ScanResult(predicted_quantum.quantum_id, status=ScanStatus.INIT)
            self.comms.log.debug("Created init scan for %s (%s)", quantum_id, predicted_quantum.task_label)
        else:
            self.reader.read_quantum_datasets([quantum_id])
            predicted_quantum = self.reader.components.quantum_datasets[quantum_id]
            self.comms.log.debug(
                "Scanning %s (%s@%s)",
                quantum_id,
                predicted_quantum.task_label,
                predicted_quantum.data_coordinate,
            )
            result = ScanResult(predicted_quantum.quantum_id, ScanStatus.INCOMPLETE)
            del self.reader.components.quantum_datasets[quantum_id]
            log_id = self._read_and_compress_log(predicted_quantum, result)
            if self.comms.config.incomplete and not result.log:
                self.comms.log.debug("Abandoning scan for %s; no log dataset.", quantum_id)
                result.status = ScanStatus.ABANDONED
                self.comms.report_scan(ScanReport(result.quantum_id, result.status, 0, 0))
                return result
            metadata_id = self._read_and_compress_metadata(predicted_quantum, result)
            if result.metadata:
                result.status = ScanStatus.SUCCESSFUL
                result.existing_outputs.add(metadata_id)
            elif not self.comms.config.incomplete:
                result.status = ScanStatus.FAILED
            else:
                # We found the log dataset, but no metadata; this means the
                # quantum failed, but a retry might still happen that could
                # turn it into a success if we can't yet assume the run is
                # complete.
                self.comms.log.debug("Abandoning scan for %s.", quantum_id)
                result.status = ScanStatus.ABANDONED
                self.comms.report_scan(ScanReport(result.quantum_id, result.status, 0, 0))
                return result
            if result.log:
                result.existing_outputs.add(log_id)
        for predicted_output in itertools.chain.from_iterable(predicted_quantum.outputs.values()):
            if predicted_output.dataset_id not in result.existing_outputs and self.scan_dataset(
                predicted_output
            ):
                result.existing_outputs.add(predicted_output.dataset_id)
        to_ingest = self._make_ingest_request_and_report(predicted_quantum, result)
        assert result.status is not ScanStatus.INCOMPLETE
        assert result.status is not ScanStatus.ABANDONED
        if self.comms.config.actually_ingest_provenance:
            self.comms.request_write(result)
        self.comms.request_ingest(to_ingest)
        self.comms.log.debug("Finished scan for %s.", quantum_id)
        return result

    def _make_ingest_request_and_report(
        self, predicted_quantum: PredictedQuantumDatasetsModel, result: ScanResult
    ) -> QuantumIngestRequest:
        """Make an ingest request from a quantum scan, and report that scan
        to the supervisor.

        Parameters
        ----------
        predicted_quantum : `PredictedQuantumDatasetsModel`
            Information about the predicted quantum.
        result : `ScanResult`
            Result of a quantum scan.

        Returns
        -------
        ingest_request : `IngestRequest`
            A request to be sent to the ingester.
        """
        predicted_outputs_by_id = {
            d.dataset_id: d for d in itertools.chain.from_iterable(predicted_quantum.outputs.values())
        }
        to_ingest_predicted: list[PredictedDatasetModel] = []
        to_ingest_refs: list[DatasetRef] = []
        n_removals: int = 0
        n_deferred_ingests: int = 0
        for dataset_id in result.existing_outputs:
            predicted_output = predicted_outputs_by_id[dataset_id]
            ref = self.reader.components.make_dataset_ref(predicted_output)
            if predicted_output.dataset_type_name in self.provenance_dataset_types:
                self.pending_removals.append(ref)
                n_removals += 1
                self.pending_ingests.append(ref)
                n_deferred_ingests += 1
            elif predicted_output.dataset_type_name in self.remove_dataset_types:
                self.pending_removals.append(ref)
                n_removals += 1
            else:
                to_ingest_predicted.append(predicted_output)
                to_ingest_refs.append(ref)
        if self.comms.config.ingest_provenance and result.status is not ScanStatus.INIT:
            self.pending_ingests.append(self._make_provenance_quantum_ref(predicted_quantum))
            n_deferred_ingests += 1
        self.comms.log.debug(
            "Identified %d removals and %d deferred ingests for %s.",
            n_removals,
            n_deferred_ingests,
            predicted_quantum.quantum_id,
        )
        to_ingest_records = self.qbb._datastore.export_predicted_records(to_ingest_refs)
        self.comms.report_scan(ScanReport(result.quantum_id, result.status, n_removals, n_deferred_ingests))
        return QuantumIngestRequest(result.quantum_id, to_ingest_predicted, to_ingest_records)

    def _make_provenance_quantum_ref(self, predicted: PredictedQuantumDatasetsModel) -> DatasetRef:
        """Make a `DatasetRef` for a quantum provenance dataset.

        Predicted
        ---------
        predicted : `PredictedQuantumDatasetsModel`
            Information about the predicted quantum.

        Returns
        -------
        ref : `lsst.daf.butler.DatasetRef`
            Butler dataset reference.
        """
        task_node = self.reader.pipeline_graph.tasks[predicted.task_label]
        return DatasetRef(
            DatasetType(
                self.comms.config.quantum_dataset_type_template.format(predicted.task_label),
                task_node.dimensions,
                "ProvenanceQuantumGraph",
            ),
            DataCoordinate.from_full_values(task_node.dimensions, tuple(predicted.data_coordinate)),
            run=self.reader.header.output_run,
            id=predicted.quantum_id,
        )

    def _read_and_compress_metadata(
        self, predicted_quantum: PredictedQuantumDatasetsModel, result: ScanResult
    ) -> uuid.UUID:
        """Attempt to read the metadata dataset for a quantum to extract
        provenance information from it.

        Parameters
        ----------
        predicted_quantum : `PredictedQuantumDatasetsModel`
            Information about the predicted quantum.
        result : `ScanResult`
            Result object to be modified in-place.

        Returns
        -------
        dataset_id : `uuid.UUID`
            UUID of the metadata dataset.
        """
        assert not result.metadata, "We shouldn't be scanning again if we already read the metadata."
        (predicted_dataset,) = predicted_quantum.outputs[acc.METADATA_OUTPUT_CONNECTION_NAME]
        ref = self.reader.components.make_dataset_ref(predicted_dataset)
        try:
            # This assumes QBB metadata writes are atomic, which should be the
            # case. If it's not we'll probably get pydantic validation errors
            # here.
            content: TaskMetadata = self.qbb.get(ref, storageClass="TaskMetadata")
        except FileNotFoundError:
            if self.comms.config.incomplete:
                return ref.id
        else:
            try:
                # Int conversion guards against spurious conversion to
                # float that can apparently sometimes happen in
                # TaskMetadata.
                result.caveats = QuantumSuccessCaveats(int(content["quantum"]["caveats"]))
            except LookupError:
                pass
            try:
                result.exception = ExceptionInfo._from_metadata(
                    content[predicted_quantum.task_label]["failure"]
                )
            except LookupError:
                pass
            try:
                result.existing_outputs = {
                    uuid.UUID(id_str) for id_str in ensure_iterable(content["quantum"].getArray("outputs"))
                }
            except LookupError:
                pass
            result.resource_usage = QuantumResourceUsage.from_task_metadata(content)
            result.metadata = content.model_dump_json().encode()
            if self.compressor is not None:
                result.metadata = self.compressor.compress(result.metadata)
                result.is_compressed = True
        return ref.id

    def _read_and_compress_log(
        self, predicted_quantum: PredictedQuantumDatasetsModel, result: ScanResult
    ) -> uuid.UUID:
        """Attempt to read the log dataset for a quantum to test for the
        quantum's completion (the log is always written last) and aggregate
        the log content in the provenance quantum graph.

        Parameters
        ----------
        predicted_quantum : `PredictedQuantumDatasetsModel`
            Information about the predicted quantum.
        result : `ScanResult`
            Result object to be modified in-place.

        Returns
        -------
        dataset_id : `uuid.UUID`
            UUID of the log dataset.
        """
        (predicted_dataset,) = predicted_quantum.outputs[acc.LOG_OUTPUT_CONNECTION_NAME]
        ref = self.reader.components.make_dataset_ref(predicted_dataset)
        try:
            # This assumes QBB log writes are atomic, which should be the case.
            # If it's not we'll probably get pydantic validation errors here.
            content: ButlerLogRecords = self.qbb.get(ref)
        except FileNotFoundError:
            if self.comms.config.incomplete:
                return ref.id
        else:
            result.log = content.model_dump_json().encode()
            if self.compressor is not None:
                result.log = self.compressor.compress(result.log)
                result.is_compressed = True
        return ref.id
