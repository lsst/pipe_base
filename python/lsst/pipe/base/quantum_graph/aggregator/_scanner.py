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
from contextlib import AbstractContextManager
from typing import Any, Literal, Self

import zstandard

from lsst.daf.butler import ButlerLogRecords, DatasetRef, QuantumBackedButler

from ... import automatic_connection_constants as acc
from ..._task_metadata import TaskMetadata
from ...pipeline_graph import PipelineGraph, TaskImportMode
from .._multiblock import Compressor
from .._predicted import (
    PredictedDatasetModel,
    PredictedQuantumDatasetsModel,
    PredictedQuantumGraphReader,
)
from .._provenance import ProvenanceQuantumScanModels, ProvenanceQuantumScanStatus
from ._communicators import ScannerCommunicator
from ._structs import IngestRequest, ScanReport


@dataclasses.dataclass
class Scanner(AbstractContextManager):
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

    def __post_init__(self) -> None:
        if self.comms.config.mock_storage_classes:
            import lsst.pipe.base.tests.mocks  # noqa: F401
        self.comms.log.verbose("Reading from predicted quantum graph.")
        self.reader = self.comms.exit_stack.enter_context(
            PredictedQuantumGraphReader.open(self.predicted_path, import_mode=TaskImportMode.DO_NOT_IMPORT)
        )
        self.reader.read_dimension_data()
        self.reader.read_init_quanta()
        self.comms.log.verbose("Initializing quantum-backed butler.")
        self.qbb = self.make_qbb(self.butler_path, self.reader.pipeline_graph)
        self.init_quanta = {q.quantum_id: q for q in self.reader.components.init_quanta.root}

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        try:
            self.qbb.close()
        except Exception:
            self.comms.log.exception("An exception occurred during Ingester exit")
        return False

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
        with comms, Scanner(predicted_path, butler_path, comms) as scanner:
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

    def scan_dataset(self, predicted: PredictedDatasetModel) -> bool:
        """Scan for a dataset's existence.

        Parameters
        ----------
        predicted : `.PredictedDatasetModel`
            Information about the dataset from the predicted graph.

        Returns
        -------
        exists : `bool``
            Whether the dataset exists.
        """
        ref = self.reader.components.make_dataset_ref(predicted)
        return self.qbb.stored(ref)

    def scan_quantum(self, quantum_id: uuid.UUID) -> ProvenanceQuantumScanModels:
        """Scan for a quantum's completion and error status, and its output
        datasets' existence.

        Parameters
        ----------
        quantum_id : `uuid.UUID`
            Unique ID for the quantum.

        Returns
        -------
        result : `ProvenanceQuantumScanModels`
            Scan result struct.
        """
        if (predicted_quantum := self.init_quanta.get(quantum_id)) is not None:
            result = ProvenanceQuantumScanModels(
                predicted_quantum.quantum_id, status=ProvenanceQuantumScanStatus.INIT
            )
            self.comms.log.debug("Created init scan for %s (%s)", quantum_id, predicted_quantum.task_label)
        else:
            self.reader.read_quantum_datasets([quantum_id])
            predicted_quantum = self.reader.components.quantum_datasets.pop(quantum_id)
            self.comms.log.debug(
                "Scanning %s (%s@%s)",
                quantum_id,
                predicted_quantum.task_label,
                predicted_quantum.data_coordinate,
            )
            logs = self._read_log(predicted_quantum)
            metadata = self._read_metadata(predicted_quantum)
            result = ProvenanceQuantumScanModels.from_metadata_and_logs(
                predicted_quantum, metadata, logs, incomplete=self.comms.config.incomplete
            )
            if result.status is ProvenanceQuantumScanStatus.ABANDONED:
                self.comms.log.debug("Abandoning scan for failed quantum %s.", quantum_id)
                self.comms.report_scan(ScanReport(result.quantum_id, result.status))
                return result
        for predicted_output in itertools.chain.from_iterable(predicted_quantum.outputs.values()):
            if predicted_output.dataset_id not in result.output_existence:
                result.output_existence[predicted_output.dataset_id] = self.scan_dataset(predicted_output)
        to_ingest = self._make_ingest_request(predicted_quantum, result)
        if self.comms.config.is_writing_provenance:
            to_write = result.to_scan_data(predicted_quantum, compressor=self.compressor)
            self.comms.request_write(to_write)
        self.comms.request_ingest(to_ingest)
        self.comms.report_scan(ScanReport(result.quantum_id, result.status))
        self.comms.log.debug("Finished scan for %s.", quantum_id)
        return result

    def _make_ingest_request(
        self, predicted_quantum: PredictedQuantumDatasetsModel, result: ProvenanceQuantumScanModels
    ) -> IngestRequest:
        """Make an ingest request from a quantum scan.

        Parameters
        ----------
        predicted_quantum : `PredictedQuantumDatasetsModel`
            Information about the predicted quantum.
        result : `ProvenanceQuantumScanModels`
            Result of a quantum scan.

        Returns
        -------
        ingest_request : `IngestRequest`
            A request to be sent to the ingester.
        """
        predicted_outputs_by_id = {
            d.dataset_id: d for d in itertools.chain.from_iterable(predicted_quantum.outputs.values())
        }
        to_ingest_refs: list[DatasetRef] = []
        for dataset_id, was_produced in result.output_existence.items():
            if was_produced:
                predicted_output = predicted_outputs_by_id[dataset_id]
                to_ingest_refs.append(self.reader.components.make_dataset_ref(predicted_output))
        to_ingest_records = self.qbb._datastore.export_predicted_records(to_ingest_refs)
        return IngestRequest(result.quantum_id, to_ingest_refs, to_ingest_records)

    def _read_metadata(self, predicted_quantum: PredictedQuantumDatasetsModel) -> TaskMetadata | None:
        """Attempt to read the metadata dataset for a quantum.

        Parameters
        ----------
        predicted_quantum : `PredictedQuantumDatasetsModel`
            Information about the predicted quantum.

        Returns
        -------
        metadata : `...TaskMetadata` or `None`
            Task metadata.
        """
        (predicted_dataset,) = predicted_quantum.outputs[acc.METADATA_OUTPUT_CONNECTION_NAME]
        ref = self.reader.components.make_dataset_ref(predicted_dataset)
        try:
            # This assumes QBB metadata writes are atomic, which should be the
            # case. If it's not we'll probably get pydantic validation errors
            # here.
            return self.qbb.get(ref, storageClass="TaskMetadata")
        except FileNotFoundError:
            return None

    def _read_log(self, predicted_quantum: PredictedQuantumDatasetsModel) -> ButlerLogRecords | None:
        """Attempt to read the log dataset for a quantum.

        Parameters
        ----------
        predicted_quantum : `PredictedQuantumDatasetsModel`
            Information about the predicted quantum.

        Returns
        -------
        logs : `lsst.daf.butler.logging.ButlerLogRecords` or `None`
            Task logs.
        """
        (predicted_dataset,) = predicted_quantum.outputs[acc.LOG_OUTPUT_CONNECTION_NAME]
        ref = self.reader.components.make_dataset_ref(predicted_dataset)
        try:
            # This assumes QBB log writes are atomic, which should be the case.
            # If it's not we'll probably get pydantic validation errors here.
            return self.qbb.get(ref)
        except FileNotFoundError:
            return None
