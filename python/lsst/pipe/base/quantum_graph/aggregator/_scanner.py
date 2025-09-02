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

import asyncio
import dataclasses
import itertools
import time
import uuid
from typing import Literal

import zstandard

from lsst.daf.butler import ButlerLogRecords, DatasetRef, QuantumBackedButler
from lsst.utils.iteration import ensure_iterable

from ... import automatic_connection_constants as acc
from ..._status import QuantumSuccessCaveats
from ..._task_metadata import TaskMetadata
from ...pipeline_graph import PipelineGraph, TaskImportMode
from ...quantum_provenance_graph import ExceptionInfo
from ...resource_usage import QuantumResourceUsage
from .._multiblock import Compressor
from .._predicted import (
    PredictedDatasetModel,
    PredictedQuantumDatasetsModel,
    PredictedQuantumGraphReader,
)
from ._communicators import ScannerCommunicator
from ._config import ScannerTimeConfigDict
from ._storage import Storage
from ._structs import IngestRequest, ScanReport, ScanResult, ScanStatus


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

    storage: Storage | None = None
    """Object that mediates access to the scanner's SQLite database."""

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
        self.comms.log.verbose("Initializing scanner storage.")
        if self.comms.config.db_dir is not None:
            self.storage = self.comms.enter(
                Storage(self.comms.config, self.comms.scanner_id, trust_local=False),
                "Closing scanner storage.",
            )
        self.comms.log.verbose("Initializing quantum-backed butler.")
        self.qbb = self.make_qbb(self.butler_path, self.reader.pipeline_graph)
        self.init_quanta = {q.quantum_id: q for q in self.reader.components.init_quanta.root}

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
            scanner.resume()
            asyncio.run(scanner.loop())

    def resume(self) -> None:
        """Load previous scans from the SQLite database."""
        n_ingests_loaded = 0
        n_quanta_loaded = 0
        if self.storage is not None:
            self.comms.log.info("Sending any past ingests that may not have completed.")
            for ingest_request in self.storage.fetch_ingests():
                self.comms.request_ingest(ingest_request)
                n_ingests_loaded += 1
            self.comms.log.info("Loading and returning past scans.")
            for scan_return, write_request in self.storage.resume():
                self.comms.report_scan(scan_return)
                n_quanta_loaded += 1
                if write_request is not None:
                    self.comms.request_write(write_request)
        self.comms.report_resume_completed(n_quanta_loaded, n_ingests_loaded)

    async def loop(self) -> None:
        """Run the main loop for the scanner.

        This is an async method to allow us to pause scanning a quantum that
        may not have completed yet while moving on to others.  This only comes
        into play when `AggregatorConfig.assume_complete` is `False`.
        """
        asyncio.get_running_loop().set_task_factory(asyncio.eager_task_factory)
        pending: set[asyncio.Task[ScanResult]] = set()
        self.comms.log.info("Scan request loop beginning.")
        for scan_request in self.comms.poll():
            if self.compressor is None and (cdict_data := self.comms.get_compression_dict()) is not None:
                self.compressor = zstandard.ZstdCompressor(
                    self.comms.config.zstd_level, zstandard.ZstdCompressionDict(cdict_data)
                )
            pending = await self.scan_or_wait(scan_request, pending)
        self.comms.log.info("Waiting for pending tasks to complete.")
        pending = await self.scan_or_wait(None, pending, return_when="ALL_COMPLETED")
        assert not pending, "No scans should be pending at this point."
        if self.storage is not None and self.storage.needs_checkpoint:
            self.comms.log.info("Performing final checkpoint.")
            self.storage.checkpoint()

    async def scan_or_wait(
        self,
        requested_quantum_id: uuid.UUID | None,
        pending: set[asyncio.Task[ScanResult]],
        return_when: Literal["FIRST_COMPLETED", "ALL_COMPLETED"] = "FIRST_COMPLETED",
    ) -> set[asyncio.Task[ScanResult]]:
        """Handle a scan request (or the absence of one) and do general
        housekeeping in one of the polling loops.

        Parameters
        ----------
        requested_quantum_id : `uuid.UUID` or `None`
            ID of the quantum to scan.
        pending : `set` [ `asyncio.Task [ `ScanResult` ] ]
            Pending async tasks for scanning quanta.
        return_when : `str`, optional
            Whether to wait for one pending scan to complete (default) or all
            of them.

        Returns
        -------
        pending : `set` [ `asyncio.Task [ `ScanResult` ] ]
            The new set of pending tasks.
        """
        timeout: float = self.comms.config.worker_sleep
        if requested_quantum_id is not None:
            self.comms.log.debug("Creating scan task for %s.", requested_quantum_id)
            pending.add(asyncio.create_task(self.scan_quantum(requested_quantum_id)))
            timeout = 0.0
        if pending:
            self.comms.log.debug("Awaiting %d pending scans.", len(pending))
            _, pending = await asyncio.wait(pending, timeout=timeout, return_when=return_when)
        else:
            self.comms.log.debug("Nothing to do; sleeping for %fs.", timeout)
            await asyncio.sleep(timeout)
        if self.storage is not None and self.storage.should_checkpoint_now:
            self.comms.log.debug("Running periodic checkpoint.", timeout)
            self.storage.checkpoint()
        return pending

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

    async def scan_quantum(self, quantum_id: uuid.UUID) -> ScanResult:
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
            times_for_task = self.comms.config.get_times_for_task(predicted_quantum.task_label)
            wait_interval: float = times_for_task["wait"]
            while True:
                log_id = self._read_and_compress_log(predicted_quantum, result)
                if self.comms.config.assume_complete or result.log:
                    break
                wait_interval = await self._wait_for_quantum(times_for_task, wait_interval)
            first_failure_time: float | None = None
            while True:
                metadata_id = self._read_and_compress_metadata(predicted_quantum, result)
                if result.metadata:
                    result.status = ScanStatus.SUCCESSFUL
                    result.existing_outputs.add(metadata_id)
                    break
                elif self.comms.config.assume_complete:
                    result.status = ScanStatus.FAILED
                    break
                else:
                    # We found the log dataset, but no metadata; this means the
                    # quantum failed, but a retry might still happen that could
                    # turn it into a success if we can't yet assume the run is
                    # complete.
                    if first_failure_time is None:
                        first_failure_time = time.time()
                    else:
                        if time.time() - first_failure_time > times_for_task["retry_timeout"]:
                            # Give up on scanning this quantum and all that
                            # follow it. A later invocation of the scanner with
                            # assume_complete=True will recover it in the
                            # unlikely event it's still retrying, but
                            # (according to timeout configuration) that should
                            # have already happened if it was going to.
                            self.comms.log.debug("Abandoning scan for %s.", quantum_id)
                            result.status = ScanStatus.ABANDONED
                            self.comms.report_scan(ScanReport(result.quantum_id, result.status))
                            return result
                    wait_interval = await self._wait_for_quantum(times_for_task, wait_interval)
            if result.log:
                result.existing_outputs.add(log_id)
        for predicted_output in itertools.chain.from_iterable(predicted_quantum.outputs.values()):
            if predicted_output.dataset_id not in result.existing_outputs and self.scan_dataset(
                predicted_output
            ):
                result.existing_outputs.add(predicted_output.dataset_id)
        to_ingest = self._make_ingest_request(predicted_quantum, result)
        if self.storage is not None:
            self.storage.save_quantum(result, to_ingest=to_ingest)
        self.comms.report_scan(ScanReport(result.quantum_id, result.status))
        assert result.status is not ScanStatus.INCOMPLETE
        assert result.status is not ScanStatus.ABANDONED
        if self.comms.config.output_path is not None:
            self.comms.request_write(result)
        self.comms.request_ingest(to_ingest)
        self.comms.log.debug("Finished scan for %s.", quantum_id)
        return result

    def _make_ingest_request(
        self, predicted_quantum: PredictedQuantumDatasetsModel, result: ScanResult
    ) -> IngestRequest:
        """Make an ingest request from a quantum scan.

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
        for dataset_id in result.existing_outputs:
            predicted_output = predicted_outputs_by_id[dataset_id]
            to_ingest_predicted.append(predicted_output)
            to_ingest_refs.append(self.reader.components.make_dataset_ref(predicted_output))
        to_ingest_records = self.qbb._datastore.export_predicted_records(to_ingest_refs)
        return IngestRequest.pack(result.quantum_id, to_ingest_predicted, to_ingest_records)

    @staticmethod
    async def _wait_for_quantum(times_for_task: ScannerTimeConfigDict, wait_interval: float) -> float:
        """Wait for a pending quantum by the configured amount.

        Parameters
        ----------
        times_for_task : `dict`
            Dictionary of task wait-time configuration.
        wait_interval : `float`
            Last wait interval in seconds.

        Returns
        -------
        wait_interval : `float`
            New wait interval in seconds.
        """
        await asyncio.sleep(wait_interval)
        wait_interval *= times_for_task["wait_factor"]
        if wait_interval > times_for_task["wait_max"]:
            wait_interval = times_for_task["wait_max"]
        return wait_interval

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
            if not self.comms.config.assume_complete:
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
            if not self.comms.config.assume_complete:
                return ref.id
        else:
            result.log = content.model_dump_json().encode()
            if self.compressor is not None:
                result.log = self.compressor.compress(result.log)
                result.is_compressed = True
        return ref.id
