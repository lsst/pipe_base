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

import dataclasses
import enum
import itertools
import operator
import uuid
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from types import TracebackType
from typing import Self

import networkx
import zstandard

from ... import automatic_connection_constants as acc
from ...pipeline_graph import TaskImportMode
from .._common import BaseQuantumGraphWriter
from .._multiblock import Compressor, MultiblockWriter
from .._predicted import PredictedQuantumGraphReader
from .._provenance import (
    DATASET_ADDRESS_INDEX,
    DATASET_MB_NAME,
    LOG_ADDRESS_INDEX,
    LOG_MB_NAME,
    METADATA_ADDRESS_INDEX,
    METADATA_MB_NAME,
    QUANTUM_ADDRESS_INDEX,
    QUANTUM_MB_NAME,
    ProvenanceDatasetModel,
    ProvenanceInitQuantaModel,
    ProvenanceInitQuantumModel,
    ProvenanceQuantumModel,
)
from ._communicators import CancelError, WriterCommunicator
from ._config import AggregatorConfig
from ._progress import WorkerProgress
from ._structs import ScanResult


class CompressionState(enum.Enum):
    NOT_COMPRESSED = enum.auto()
    LOG_AND_METADATA_COMPRESSED = enum.auto()
    ALL_COMPRESSED = enum.auto()


@dataclasses.dataclass
class _ScanData:
    quantum_id: uuid.UUID
    log_id: uuid.UUID
    metadata_id: uuid.UUID
    quantum: bytes = b""
    datasets: dict[uuid.UUID, bytes] = dataclasses.field(default_factory=dict)
    log: bytes = b""
    metadata: bytes = b""
    compression: CompressionState = CompressionState.NOT_COMPRESSED

    def compress(self, compressor: Compressor) -> None:
        if self.compression is CompressionState.NOT_COMPRESSED:
            self.metadata = compressor.compress(self.metadata)
            self.log = compressor.compress(self.log)
            self.compression = CompressionState.LOG_AND_METADATA_COMPRESSED
        if self.compression is CompressionState.LOG_AND_METADATA_COMPRESSED:
            self.quantum = compressor.compress(self.quantum)
            for key in self.datasets.keys():
                self.datasets[key] = compressor.compress(self.datasets[key])
        self.compression = CompressionState.ALL_COMPRESSED


@dataclasses.dataclass
class DataWriters:
    graph: BaseQuantumGraphWriter
    datasets: MultiblockWriter
    quanta: MultiblockWriter
    metadata: MultiblockWriter | None = None
    logs: MultiblockWriter | None = None

    @classmethod
    @contextmanager
    def open(
        cls,
        reader: PredictedQuantumGraphReader,
        output_path: str,
        indices: dict[uuid.UUID, int],
        *,
        compressor: Compressor,
        cdict_data: bytes | None = None,
        aggregate_logs: bool,
        aggregate_metadata: bool,
    ) -> Iterator[DataWriters]:
        header = reader.header.model_copy()
        header.graph_type = "provenance"
        with ExitStack() as exit_stack:
            graph = exit_stack.enter_context(
                BaseQuantumGraphWriter.open(
                    output_path,
                    header,
                    reader.pipeline_graph,
                    indices,
                    address_filename="nodes",
                    compressor=compressor,
                    cdict_data=cdict_data,
                )
            )
            graph.address_writer.addresses = [{}, {}, {}, {}]
            logs: MultiblockWriter | None = None
            if aggregate_logs:
                logs = exit_stack.enter_context(
                    MultiblockWriter.open_in_zip(graph.zf, LOG_MB_NAME, header.int_size, use_tempfile=True)
                )
                graph.address_writer.addresses[LOG_ADDRESS_INDEX] = logs.addresses
            metadata: MultiblockWriter | None = None
            if aggregate_metadata:
                metadata = exit_stack.enter_context(
                    MultiblockWriter.open_in_zip(
                        graph.zf, METADATA_MB_NAME, header.int_size, use_tempfile=True
                    )
                )
                graph.address_writer.addresses[METADATA_ADDRESS_INDEX] = metadata.addresses
            datasets = exit_stack.enter_context(
                MultiblockWriter.open_in_zip(graph.zf, DATASET_MB_NAME, header.int_size, use_tempfile=True)
            )
            graph.address_writer.addresses[DATASET_ADDRESS_INDEX] = datasets.addresses
            quanta = exit_stack.enter_context(
                MultiblockWriter.open_in_zip(graph.zf, QUANTUM_MB_NAME, header.int_size, use_tempfile=True)
            )
            graph.address_writer.addresses[QUANTUM_ADDRESS_INDEX] = quanta.addresses
            yield cls(
                graph,
                datasets=datasets,
                quanta=quanta,
                metadata=metadata,
                logs=logs,
            )

    @property
    def compressor(self) -> Compressor:
        return self.graph.compressor


class Writer:
    def __init__(self, config: AggregatorConfig, comms: WriterCommunicator):
        self._config = config
        self._comms = comms
        self._progress: WorkerProgress
        self._existing_init_outputs: dict[uuid.UUID, set[uuid.UUID]] = {}
        self._indices: dict[uuid.UUID, int] = {}
        self._output_dataset_ids: set[uuid.UUID] = set()
        self._xgraph = networkx.DiGraph()
        self._pending_compression_training: list[_ScanData] = []
        self._exit_stack = ExitStack()

    def __enter__(self) -> Self:
        assert self._config.output_path is not None, "Writer should not be used if writing is disabled."
        self._exit_stack.enter_context(self._comms.handling_errors())
        self._progress = self._exit_stack.enter_context(WorkerProgress("writer", self._config))
        self._predicted_reader = self._exit_stack.enter_context(
            PredictedQuantumGraphReader.open(
                self._config.predicted_path, import_mode=TaskImportMode.DO_NOT_IMPORT
            )
        )
        self._predicted_reader.read_init_quanta()
        self._predicted_reader.read_quantum_datasets()
        for predicted_init_quantum in self._predicted_reader.components.init_quanta.root:
            self._existing_init_outputs[predicted_init_quantum.quantum_id] = set()
        self._populate_indices_and_outputs()
        self._populate_xgraph()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        return self._exit_stack.__exit__(exc_type, exc_value, traceback)

    def _populate_indices_and_outputs(self) -> None:
        all_uuids = set(self._predicted_reader.components.quantum_indices.keys())
        for quantum in itertools.chain(
            self._predicted_reader.components.init_quanta.root[1:],
            self._predicted_reader.components.quantum_datasets.values(),
        ):
            all_uuids.update(quantum.iter_input_dataset_ids())
            self._output_dataset_ids.update(quantum.iter_output_dataset_ids())
        all_uuids.update(self._output_dataset_ids)
        self._indices = {
            node_id: node_index
            for node_index, node_id in enumerate(sorted(all_uuids, key=operator.attrgetter("int")))
        }

    def _populate_xgraph(self) -> None:
        for predicted_quantum in itertools.chain(
            self._predicted_reader.components.init_quanta.root[1:],
            self._predicted_reader.components.quantum_datasets.values(),
        ):
            quantum_index = self._indices[predicted_quantum.quantum_id]
            for predicted_input in itertools.chain.from_iterable(predicted_quantum.inputs.values()):
                self._xgraph.add_edge(self._indices[predicted_input.dataset_id], quantum_index)
            for predicted_output in itertools.chain.from_iterable(predicted_quantum.outputs.values()):
                self._xgraph.add_edge(quantum_index, self._indices[predicted_output.dataset_id])

    def make_data_writers(self) -> DataWriters:
        cdict = self.make_compression_dictionary()
        self._comms.send_compression_dict(cdict.as_bytes())
        assert self._config.output_path is not None
        data_writers = self._exit_stack.enter_context(
            DataWriters.open(
                self._predicted_reader,
                self._config.output_path,
                self._indices,
                compressor=zstandard.ZstdCompressor(self._config.zstd_level, cdict),
                cdict_data=cdict.as_bytes(),
                aggregate_logs=self._config.aggregate_logs,
                aggregate_metadata=self._config.aggregate_metadata,
            )
        )
        for scan_data in self._pending_compression_training:
            self.write_scan_data(scan_data, data_writers)
        return data_writers

    def loop(self) -> bool:
        try:
            data_writers: DataWriters | None = None
            if not self._config.zstd_dict_size:
                data_writers = self.make_data_writers()
            for request in self._comms.poll():
                if data_writers is None:
                    self._pending_compression_training.extend(self.make_scan_data(request))
                    if len(self._pending_compression_training) >= self._config.zstd_dict_n_inputs:
                        data_writers = self.make_data_writers()
                else:
                    for scan_data in self.make_scan_data(request):
                        self.write_scan_data(scan_data, data_writers)
            if data_writers is None:
                data_writers = self.make_data_writers()
            self.write_overall_inputs(data_writers)
            self.write_init_outputs(data_writers)
            self._comms.send_writer_done()
            self._progress.log.debug("Shutting down writer.")
            return True
        except CancelError:
            self._progress.log.debug("Cancel event set.")
            return False

    def make_compression_dictionary(self) -> zstandard.ZstdCompressionDict:
        if not self._config.zstd_dict_size:
            return zstandard.ZstdCompressionDict(b"")
        training_inputs: list[bytes] = []
        # We start the dictionary training with *predicted* quantum dataset
        # models, since those have almost all of the same attributes as the
        # provenance quantum and dataset models, and we can get a nice random
        # sample from just the first N, since they're ordered by UUID.  We
        # chop out the datastore records since those don't appear in the
        # provenance graph.
        for predicted_quantum in self._predicted_reader.components.quantum_datasets.values():
            if len(training_inputs) == self._config.zstd_dict_n_inputs:
                break
            predicted_quantum.datastore_records.clear()
            training_inputs.append(predicted_quantum.model_dump_json().encode())
        # Add the provenance quanta, metadata, and logs we've accumulated.
        for scan_data in self._pending_compression_training:
            assert scan_data.compression is CompressionState.NOT_COMPRESSED
            training_inputs.append(scan_data.quantum)
            training_inputs.append(scan_data.metadata)
            training_inputs.append(scan_data.log)
        return zstandard.train_dictionary(self._config.zstd_dict_size, training_inputs)

    def write_init_outputs(self, data_writers: DataWriters) -> None:
        self._progress.log.verbose("Writing init outputs.")
        init_quanta = ProvenanceInitQuantaModel()
        for predicted_init_quantum in self._predicted_reader.components.init_quanta.root[1:]:
            existing_outputs = self._existing_init_outputs[predicted_init_quantum.quantum_id]
            for predicted_output in itertools.chain.from_iterable(predicted_init_quantum.outputs.values()):
                dataset_index = self._indices[predicted_output.dataset_id]
                provenance_output = ProvenanceDatasetModel.from_predicted(
                    predicted_output,
                    producer=self._indices[predicted_init_quantum.quantum_id],
                    consumers=self._xgraph.successors(dataset_index),
                )
                provenance_output.exists = predicted_output.dataset_id in existing_outputs
                data_writers.datasets.write_model(
                    provenance_output.dataset_id, provenance_output, data_writers.compressor
                )
            init_quanta.root.append(
                ProvenanceInitQuantumModel.from_predicted(predicted_init_quantum, self._indices)
            )
        data_writers.graph.write_single_model("init_quanta", init_quanta)

    def write_overall_inputs(self, data_writers: DataWriters) -> None:
        self._progress.log.verbose("Writing overall inputs.")
        for predicted_quantum in itertools.chain(
            self._predicted_reader.components.init_quanta.root[1:],
            self._predicted_reader.components.quantum_datasets.values(),
        ):
            for predicted_input in itertools.chain.from_iterable(predicted_quantum.inputs.values()):
                if predicted_input.dataset_id not in self._output_dataset_ids:
                    if predicted_input.dataset_id not in data_writers.datasets.addresses:
                        dataset_index = self._indices[predicted_input.dataset_id]
                        data_writers.datasets.write_model(
                            predicted_input.dataset_id,
                            ProvenanceDatasetModel.from_predicted(
                                predicted_input,
                                producer=None,
                                consumers=self._xgraph.successors(dataset_index),
                            ),
                            data_writers.compressor,
                        )

    def make_scan_data(self, request: ScanResult) -> list[_ScanData]:
        if (existing_init_outputs := self._existing_init_outputs.get(request.quantum_id)) is not None:
            existing_init_outputs.update(request.existing_outputs)
            self._comms.report_write()
            return []
        predicted_quantum = self._predicted_reader.components.quantum_datasets[request.quantum_id]
        quantum_index = self._indices[predicted_quantum.quantum_id]
        (metadata_output,) = predicted_quantum.outputs[acc.METADATA_OUTPUT_CONNECTION_NAME]
        (log_output,) = predicted_quantum.outputs[acc.LOG_OUTPUT_CONNECTION_NAME]
        data = _ScanData(
            request.quantum_id,
            metadata_id=metadata_output.dataset_id,
            log_id=log_output.dataset_id,
            compression=(
                CompressionState.LOG_AND_METADATA_COMPRESSED
                if request.is_compressed
                else CompressionState.NOT_COMPRESSED
            ),
        )
        for predicted_output in itertools.chain.from_iterable(predicted_quantum.outputs.values()):
            dataset_index = self._indices[predicted_output.dataset_id]
            provenance_output = ProvenanceDatasetModel.from_predicted(
                predicted_output,
                producer=quantum_index,
                consumers=self._xgraph.successors(dataset_index),
            )
            provenance_output.exists = provenance_output.dataset_id in request.existing_outputs
            data.datasets[provenance_output.dataset_id] = provenance_output.model_dump_json().encode()
        provenance_quantum = ProvenanceQuantumModel.from_predicted(predicted_quantum, self._indices)
        provenance_quantum.status = request.get_run_status()
        provenance_quantum.caveats = request.caveats
        provenance_quantum.exception = request.exception
        data.quantum = provenance_quantum.model_dump_json().encode()
        data.metadata = request.metadata
        data.log = request.log
        return [data]

    def write_scan_data(self, scan_data: _ScanData, data_writers: DataWriters) -> None:
        scan_data.compress(data_writers.compressor)
        data_writers.quanta.write_bytes(scan_data.quantum_id, scan_data.quantum)
        for dataset_id, dataset_data in scan_data.datasets.items():
            data_writers.datasets.write_bytes(dataset_id, dataset_data)
        if scan_data.metadata and data_writers.metadata is not None:
            address = data_writers.metadata.write_bytes(scan_data.quantum_id, scan_data.metadata)
            data_writers.metadata.addresses[scan_data.metadata_id] = address
        if scan_data.log and data_writers.logs is not None:
            address = data_writers.logs.write_bytes(scan_data.quantum_id, scan_data.log)
            data_writers.logs.addresses[scan_data.log_id] = address
        self._comms.report_write()

    @staticmethod
    def run(config: AggregatorConfig, comms: WriterCommunicator) -> None:
        with comms.handling_errors():
            with Writer(config, comms) as writer:
                writer.loop()
