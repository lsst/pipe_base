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

__all__ = ("Writer",)

import itertools
import uuid
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager

import networkx

from ... import automatic_connection_constants as acc
from ...pipeline_graph import TaskImportMode
from .._multiblock import Compressor, MultiblockWriter
from .._predicted import PredictedQuantumGraphReader
from .._provenance import (
    ProvenanceDatasetModel,
    ProvenanceQuantumGraphWriter,
    ProvenanceQuantumModel,
)
from ._communicators import CancelError, WriterCommunicator
from ._config import AggregatorConfig
from ._progress import WorkerProgress
from ._structs import ScanResult


class Writer:
    def __init__(
        self,
        config: AggregatorConfig,
        progress: WorkerProgress,
        comms: WriterCommunicator,
        predicted_reader: PredictedQuantumGraphReader,
        provenance_writer: ProvenanceQuantumGraphWriter,
        datasets_writer: MultiblockWriter,
        quanta_writer: MultiblockWriter,
        metadata_writer: MultiblockWriter | None = None,
        logs_writer: MultiblockWriter | None = None,
    ):
        self._config = config
        self._progress = progress
        self._comms = comms
        self._predicted_reader = predicted_reader
        self._provenance_writer = provenance_writer
        self._datasets_writer = datasets_writer
        self._quanta_writer = quanta_writer
        self._metadata_writer = metadata_writer
        self._logs_writer = logs_writer
        self._predicted_reader.read_init_quanta()
        self._existing_init_outputs: dict[uuid.UUID, set[uuid.UUID]] = {
            q.quantum_id: set() for q in self._predicted_reader.components.init_quanta.root
        }
        self._xgraph = networkx.DiGraph()
        self._populate_xgraph()

    @classmethod
    @contextmanager
    def open(
        cls, config: AggregatorConfig, progress: WorkerProgress, comms: WriterCommunicator
    ) -> Iterator[Writer]:
        """Construct a scanner worker in a context manager.

        Parameters
        ----------
        config : `ScannerConfig`
            Configuration for the scanner.
        progress : `BaseProgress`
            Progress reporting helper.
        comms : `WriterCommunicator`
            Helper object for communicated with the supervisor and other
            workers.
        """
        assert config.output_path is not None, "Writer should not be used if writing is disabled."
        with PredictedQuantumGraphReader.open(
            config.predicted_path, import_mode=TaskImportMode.DO_NOT_IMPORT
        ) as reader:
            with ExitStack() as exit_stack:
                writer = exit_stack.enter_context(
                    ProvenanceQuantumGraphWriter.from_predicted_reader(config.output_path, reader)
                )
                if config.aggregate_logs:
                    logs_writer = exit_stack.enter_context(writer.logs(use_tempfile=True))
                else:
                    logs_writer = None
                if config.aggregate_metadata:
                    metadata_writer = exit_stack.enter_context(writer.metadata(use_tempfile=True))
                else:
                    metadata_writer = None
                datasets_writer = exit_stack.enter_context(writer.datasets(use_tempfile=True))
                quanta_writer = exit_stack.enter_context(writer.quanta(use_tempfile=True))
                yield cls(
                    config,
                    progress,
                    comms,
                    reader,
                    writer,
                    datasets_writer=datasets_writer,
                    quanta_writer=quanta_writer,
                    metadata_writer=metadata_writer,
                    logs_writer=logs_writer,
                )

    @property
    def compressor(self) -> Compressor:
        return self._provenance_writer.compressor

    @property
    def indices(self) -> dict[uuid.UUID, int]:
        return self._provenance_writer.address_writer.indices

    def _populate_xgraph(self) -> None:
        for predicted_quantum in itertools.chain(
            self._predicted_reader.components.init_quanta.root[1:],
            self._predicted_reader.components.quantum_datasets.values(),
        ):
            quantum_index = self.indices[predicted_quantum.quantum_id]
            for predicted_input in itertools.chain.from_iterable(predicted_quantum.inputs.values()):
                self._xgraph.add_edge(self.indices[predicted_input.dataset_id], quantum_index)
            for predicted_output in itertools.chain.from_iterable(predicted_quantum.outputs.values()):
                self._xgraph.add_edge(quantum_index, self.indices[predicted_output.dataset_id])

    def write_init_outputs(self) -> None:
        self._progress.log.verbose("Writing init outputs.")
        for predicted_init_quantum in self._predicted_reader.components.init_quanta.root[1:]:
            existing_outputs = self._existing_init_outputs[predicted_init_quantum.quantum_id]
            for predicted_output in itertools.chain.from_iterable(predicted_init_quantum.outputs.values()):
                dataset_index = self.indices[predicted_output.dataset_id]
                provenance_output = ProvenanceDatasetModel.from_predicted(
                    predicted_output,
                    producer=self.indices[predicted_init_quantum.quantum_id],
                    consumers=self._xgraph.successors(dataset_index),
                )
                provenance_output.exists = predicted_output.dataset_id in existing_outputs
                self._datasets_writer.write_model(
                    provenance_output.dataset_id, provenance_output, self.compressor
                )

    def write_overall_inputs(self) -> None:
        self._progress.log.verbose("Writing overall inputs.")
        for predicted_quantum in itertools.chain(
            self._predicted_reader.components.init_quanta.root[1:],
            self._predicted_reader.components.quantum_datasets.values(),
        ):
            for predicted_input in itertools.chain.from_iterable(predicted_quantum.inputs.values()):
                if predicted_input.dataset_id not in self._provenance_writer.output_dataset_ids:
                    if predicted_input.dataset_id not in self._datasets_writer.addresses:
                        dataset_index = self.indices[predicted_input.dataset_id]
                        self._datasets_writer.write_model(
                            predicted_input.dataset_id,
                            ProvenanceDatasetModel.from_predicted(
                                predicted_input,
                                producer=None,
                                consumers=self._xgraph.successors(dataset_index),
                            ),
                            self.compressor,
                        )

    def _loop(self) -> bool:
        try:
            self.write_overall_inputs()
            for request in self._comms.poll():
                self._handle_request(request)
            self.write_init_outputs()
            self._comms.send_writer_done()
            return True
        except CancelError:
            self._progress.log.debug("Cancel event set.")
            return False

    def _handle_request(self, request: ScanResult) -> None:
        if (existing_init_outputs := self._existing_init_outputs.get(request.quantum_id)) is not None:
            existing_init_outputs.update(request.existing_outputs)
            self._comms.report_write()
            return
        predicted_quantum = self._predicted_reader.components.quantum_datasets[request.quantum_id]
        quantum_index = self.indices[predicted_quantum.quantum_id]
        (metadata_output,) = predicted_quantum.outputs[acc.METADATA_OUTPUT_CONNECTION_NAME]
        (log_output,) = predicted_quantum.outputs[acc.LOG_OUTPUT_CONNECTION_NAME]
        for predicted_output in itertools.chain.from_iterable(predicted_quantum.outputs.values()):
            dataset_index = self.indices[predicted_output.dataset_id]
            provenance_output = ProvenanceDatasetModel.from_predicted(
                predicted_output,
                producer=quantum_index,
                consumers=self._xgraph.successors(dataset_index),
            )
            provenance_output.exists = provenance_output.dataset_id in request.existing_outputs
            self._datasets_writer.write_model(
                provenance_output.dataset_id, provenance_output, self.compressor
            )
        provenance_quantum = ProvenanceQuantumModel.from_predicted(predicted_quantum, self.indices)
        provenance_quantum.status = request.get_run_status()
        provenance_quantum.caveats = request.caveats
        provenance_quantum.exception = request.exception
        self._quanta_writer.write_model(
            provenance_quantum.quantum_id, provenance_quantum, compressor=self.compressor
        )
        if request.metadata and self._metadata_writer is not None:
            address = self._metadata_writer.write_bytes(predicted_quantum.quantum_id, request.metadata)
            self._metadata_writer.addresses[metadata_output.dataset_id] = address
        if request.log and self._logs_writer is not None:
            address = self._logs_writer.write_bytes(predicted_quantum.quantum_id, request.log)
            self._logs_writer.addresses[log_output.dataset_id] = address
        self._comms.report_write()

    @staticmethod
    def run(config: AggregatorConfig, comms: WriterCommunicator) -> None:
        with WorkerProgress("writer", config) as progress:
            with comms.handling_errors():
                with Writer.open(config, progress, comms) as writer:
                    writer._loop()
            progress.log.debug("Shutting down writer.")
