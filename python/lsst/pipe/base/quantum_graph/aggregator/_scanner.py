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
from lsst.utils.iteration import ensure_iterable

from ... import automatic_connection_constants as acc
from ..._status import ExceptionInfo, QuantumAttemptStatus, QuantumSuccessCaveats
from ..._task_metadata import TaskMetadata
from ...log_capture import _ExecutionLogRecordsExtra
from ...pipeline_graph import PipelineGraph, TaskImportMode
from ...resource_usage import QuantumResourceUsage
from .._multiblock import Compressor
from .._predicted import (
    PredictedDatasetModel,
    PredictedQuantumDatasetsModel,
    PredictedQuantumGraphReader,
)
from .._provenance import ProvenanceInitQuantumModel, ProvenanceQuantumAttemptModel, ProvenanceQuantumModel
from ._communicators import ScannerCommunicator
from ._structs import IngestRequest, InProgressScan, ScanReport, ScanStatus, WriteRequest


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
        self.reader = self.comms.enter(
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

    def scan_quantum(self, quantum_id: uuid.UUID) -> InProgressScan:
        """Scan for a quantum's completion and error status, and its output
        datasets' existence.

        Parameters
        ----------
        quantum_id : `uuid.UUID`
            Unique ID for the quantum.

        Returns
        -------
        result : `InProgressScan`
            Scan result struct.
        """
        if (predicted_quantum := self.init_quanta.get(quantum_id)) is not None:
            result = InProgressScan(predicted_quantum.quantum_id, status=ScanStatus.INIT)
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
            result = InProgressScan(predicted_quantum.quantum_id, ScanStatus.INCOMPLETE)
            del self.reader.components.quantum_datasets[quantum_id]
            last_attempt = ProvenanceQuantumAttemptModel()
            if not self._read_log(predicted_quantum, result, last_attempt):
                self.comms.log.debug("Abandoning scan for %s; no log dataset.", quantum_id)
                self.comms.report_scan(ScanReport(result.quantum_id, result.status))
                return result
            if not self._read_metadata(predicted_quantum, result, last_attempt):
                # We found the log dataset, but no metadata; this means the
                # quantum failed, but a retry might still happen that could
                # turn it into a success if we can't yet assume the run is
                # complete.
                self.comms.log.debug("Abandoning scan for %s.", quantum_id)
                self.comms.report_scan(ScanReport(result.quantum_id, result.status))
                return result
            last_attempt.attempt = len(result.attempts)
            result.attempts.append(last_attempt)
        assert result.status is not ScanStatus.INCOMPLETE
        assert result.status is not ScanStatus.ABANDONED

        if len(result.logs.attempts) < len(result.attempts):
            # Logs were not found for this attempt; must have been a hard error
            # that kept the `finally` block from running or otherwise
            # interrupted the writing of the logs.
            result.logs.attempts.append(None)
            if result.status is ScanStatus.SUCCESSFUL:
                # But we found the metadata!  Either that hard error happened
                # at a very unlucky time (in between those two writes), or
                # something even weirder happened.
                result.attempts[-1].status = QuantumAttemptStatus.LOGS_MISSING
            else:
                result.attempts[-1].status = QuantumAttemptStatus.FAILED
        if len(result.metadata.attempts) < len(result.attempts):
            # Metadata missing usually just means a failure.  In any case, the
            # status will already be correct, either because it was set to a
            # failure when we read the logs, or left at UNKNOWN if there were
            # no logs.  Note that scanners never process BLOCKED quanta at all.
            result.metadata.attempts.append(None)
        assert len(result.logs.attempts) == len(result.attempts) or len(result.metadata.attempts) == len(
            result.attempts
        ), (
            "The only way we can add more than one quantum attempt is by "
            "extracting info stored with the logs, and that always appends "
            "a log attempt and a metadata attempt, so this must be a bug in "
            "the scanner."
        )
        # Scan for output dataset existence, skipping any the metadata reported
        # on as well as and the metadata and logs themselves (since we just
        # checked those).
        for predicted_output in itertools.chain.from_iterable(predicted_quantum.outputs.values()):
            if predicted_output.dataset_id not in result.outputs:
                result.outputs[predicted_output.dataset_id] = self.scan_dataset(predicted_output)
        to_ingest = self._make_ingest_request(predicted_quantum, result)
        if self.comms.config.output_path is not None:
            to_write = self._make_write_request(predicted_quantum, result)
            self.comms.request_write(to_write)
        self.comms.request_ingest(to_ingest)
        self.comms.report_scan(ScanReport(result.quantum_id, result.status))
        self.comms.log.debug("Finished scan for %s.", quantum_id)
        return result

    def _make_ingest_request(
        self, predicted_quantum: PredictedQuantumDatasetsModel, result: InProgressScan
    ) -> IngestRequest:
        """Make an ingest request from a quantum scan.

        Parameters
        ----------
        predicted_quantum : `PredictedQuantumDatasetsModel`
            Information about the predicted quantum.
        result : `InProgressScan`
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
        for dataset_id, was_produced in result.outputs.items():
            if was_produced:
                predicted_output = predicted_outputs_by_id[dataset_id]
                to_ingest_predicted.append(predicted_output)
                to_ingest_refs.append(self.reader.components.make_dataset_ref(predicted_output))
        to_ingest_records = self.qbb._datastore.export_predicted_records(to_ingest_refs)
        return IngestRequest(result.quantum_id, to_ingest_predicted, to_ingest_records)

    def _make_write_request(
        self, predicted_quantum: PredictedQuantumDatasetsModel, result: InProgressScan
    ) -> WriteRequest:
        """Make a write request from a quantum scan.

        Parameters
        ----------
        predicted_quantum : `PredictedQuantumDatasetsModel`
            Information about the predicted quantum.
        result : `InProgressScan`
            Result of a quantum scan.

        Returns
        -------
        write_request : `WriteRequest`
            A request to be sent to the writer.
        """
        quantum: ProvenanceInitQuantumModel | ProvenanceQuantumModel
        if result.status is ScanStatus.INIT:
            quantum = ProvenanceInitQuantumModel.from_predicted(predicted_quantum)
        else:
            quantum = ProvenanceQuantumModel.from_predicted(predicted_quantum)
            quantum.attempts = result.attempts
        request = WriteRequest(
            result.quantum_id,
            result.status,
            existing_outputs={
                dataset_id for dataset_id, was_produced in result.outputs.items() if was_produced
            },
            quantum=quantum.model_dump_json().encode(),
            logs=result.logs.model_dump_json().encode() if result.logs.attempts else b"",
            metadata=result.metadata.model_dump_json().encode() if result.metadata.attempts else b"",
        )
        if self.compressor is not None:
            request.quantum = self.compressor.compress(request.quantum)
            request.logs = self.compressor.compress(request.logs) if request.logs else b""
            request.metadata = self.compressor.compress(request.metadata) if request.metadata else b""
            request.is_compressed = True
        return request

    def _read_metadata(
        self,
        predicted_quantum: PredictedQuantumDatasetsModel,
        result: InProgressScan,
        last_attempt: ProvenanceQuantumAttemptModel,
    ) -> bool:
        """Attempt to read the metadata dataset for a quantum to extract
        provenance information from it.

        Parameters
        ----------
        predicted_quantum : `PredictedQuantumDatasetsModel`
            Information about the predicted quantum.
        result : `InProgressScan`
            Result object to be modified in-place.
        last_attempt : `ScanningProvenanceQuantumAttemptModel`
            Structure to fill in with information about the last attempt to
            run this quantum.

        Returns
        -------
        complete : `bool`
            Whether the quantum is complete.
        """
        (predicted_dataset,) = predicted_quantum.outputs[acc.METADATA_OUTPUT_CONNECTION_NAME]
        ref = self.reader.components.make_dataset_ref(predicted_dataset)
        try:
            # This assumes QBB metadata writes are atomic, which should be the
            # case. If it's not we'll probably get pydantic validation errors
            # here.
            metadata: TaskMetadata = self.qbb.get(ref, storageClass="TaskMetadata")
        except FileNotFoundError:
            result.outputs[ref.id] = False
            if self.comms.config.assume_complete:
                result.status = ScanStatus.FAILED
            else:
                result.status = ScanStatus.ABANDONED
                return False
        else:
            result.status = ScanStatus.SUCCESSFUL
            result.outputs[ref.id] = True
            last_attempt.status = QuantumAttemptStatus.SUCCESSFUL
            try:
                # Int conversion guards against spurious conversion to
                # float that can apparently sometimes happen in
                # TaskMetadata.
                last_attempt.caveats = QuantumSuccessCaveats(int(metadata["quantum"]["caveats"]))
            except LookupError:
                pass
            try:
                last_attempt.exception = ExceptionInfo._from_metadata(
                    metadata[predicted_quantum.task_label]["failure"]
                )
            except LookupError:
                pass
            try:
                for id_str in ensure_iterable(metadata["quantum"].getArray("outputs")):
                    result.outputs[uuid.UUID(id_str)]
            except LookupError:
                pass
            else:
                # If the metadata told us what it wrote, anything not in that
                # list was not written.
                for predicted_output in itertools.chain.from_iterable(predicted_quantum.outputs.values()):
                    result.outputs.setdefault(predicted_output.dataset_id, False)
            last_attempt.resource_usage = QuantumResourceUsage.from_task_metadata(metadata)
            result.metadata.attempts.append(metadata)
        return True

    def _read_log(
        self,
        predicted_quantum: PredictedQuantumDatasetsModel,
        result: InProgressScan,
        last_attempt: ProvenanceQuantumAttemptModel,
    ) -> bool:
        """Attempt to read the log dataset for a quantum to test for the
        quantum's completion (the log is always written last) and aggregate
        the log content in the provenance quantum graph.

        Parameters
        ----------
        predicted_quantum : `PredictedQuantumDatasetsModel`
            Information about the predicted quantum.
        result : `InProgressScan`
            Result object to be modified in-place.
        last_attempt : `ScanningProvenanceQuantumAttemptModel`
            Structure to fill in with information about the last attempt to
            run this quantum.

        Returns
        -------
        complete : `bool`
            Whether the quantum is complete.
        """
        (predicted_dataset,) = predicted_quantum.outputs[acc.LOG_OUTPUT_CONNECTION_NAME]
        ref = self.reader.components.make_dataset_ref(predicted_dataset)
        try:
            # This assumes QBB log writes are atomic, which should be the case.
            # If it's not we'll probably get pydantic validation errors here.
            log_records: ButlerLogRecords = self.qbb.get(ref)
        except FileNotFoundError:
            result.outputs[ref.id] = False
            if self.comms.config.assume_complete:
                result.status = ScanStatus.FAILED
            else:
                result.status = ScanStatus.ABANDONED
                return False
        else:
            # Set the attempt's run status to FAILED, since the default is
            # UNKNOWN (i.e. logs *and* metadata are missing) and we now know
            # the logs exist.  This will usually get replaced by SUCCESSFUL
            # when we look for metadata next.
            last_attempt.status = QuantumAttemptStatus.FAILED
            result.outputs[ref.id] = True
            if log_records.extra:
                log_extra = _ExecutionLogRecordsExtra.model_validate(log_records.extra)
                self._extract_from_log_extra(log_extra, result, last_attempt=last_attempt)
            result.logs.attempts.append(list(log_records))
        return True

    def _extract_from_log_extra(
        self,
        log_extra: _ExecutionLogRecordsExtra,
        result: InProgressScan,
        last_attempt: ProvenanceQuantumAttemptModel | None,
    ) -> None:
        for previous_attempt_log_extra in log_extra.previous_attempts:
            self._extract_from_log_extra(previous_attempt_log_extra, result, last_attempt=None)
        quantum_attempt: ProvenanceQuantumAttemptModel
        if last_attempt is None:
            # This is not the last attempt, so it must be a failure.
            quantum_attempt = ProvenanceQuantumAttemptModel(
                attempt=len(result.attempts), status=QuantumAttemptStatus.FAILED
            )
            # We also need to get the logs from this extra provenance, since
            # they won't be the main section of the log records.
            result.logs.attempts.append(log_extra.logs)
            # The special last attempt is only appended after we attempt to
            # read metadata later, but we have to append this one now.
            result.attempts.append(quantum_attempt)
        else:
            assert not log_extra.logs, "Logs for the last attempt should not be stored in the extra JSON."
            quantum_attempt = last_attempt
        if log_extra.exception is not None or log_extra.metadata is not None or last_attempt is None:
            # We won't be getting a separate metadata dataset, so anything we
            # might get from the metadata has to come from this extra
            # provenance in the logs.
            quantum_attempt.exception = log_extra.exception
            if log_extra.metadata is not None:
                quantum_attempt.resource_usage = QuantumResourceUsage.from_task_metadata(log_extra.metadata)
                result.metadata.attempts.append(log_extra.metadata)
            else:
                result.metadata.attempts.append(None)
        # Regardless of whether this is the last attempt or not, we can only
        # get the previous_process_quanta from the log extra.
        quantum_attempt.previous_process_quanta.extend(log_extra.previous_process_quanta)
