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

import networkx
import zstandard

from lsst.utils.packages import Packages

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
from ._communicators import WriterCommunicator
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
    metadata: MultiblockWriter
    logs: MultiblockWriter

    def __init__(
        self,
        comms: WriterCommunicator,
        reader: PredictedQuantumGraphReader,
        indices: dict[uuid.UUID, int],
        compressor: Compressor,
        cdict_data: bytes | None = None,
    ) -> None:
        assert comms.config.output_path is not None
        header = reader.header.model_copy()
        header.graph_type = "provenance"
        self.graph = comms.enter(
            BaseQuantumGraphWriter.open(
                comms.config.output_path,
                header,
                reader.pipeline_graph,
                indices,
                address_filename="nodes",
                compressor=compressor,
                cdict_data=cdict_data,
            ),
            on_close="Closing zip archive.",
            is_progress_log=True,
        )
        self.graph.address_writer.addresses = [{}, {}, {}, {}]
        self.logs = comms.enter(
            MultiblockWriter.open_in_zip(self.graph.zf, LOG_MB_NAME, header.int_size, use_tempfile=True),
            on_close="Copying logs into zip archive.",
            is_progress_log=True,
        )
        self.graph.address_writer.addresses[LOG_ADDRESS_INDEX] = self.logs.addresses
        self.metadata = comms.enter(
            MultiblockWriter.open_in_zip(self.graph.zf, METADATA_MB_NAME, header.int_size, use_tempfile=True),
            on_close="Copying metadata into zip archive.",
            is_progress_log=True,
        )
        self.graph.address_writer.addresses[METADATA_ADDRESS_INDEX] = self.metadata.addresses
        self.datasets = comms.enter(
            MultiblockWriter.open_in_zip(self.graph.zf, DATASET_MB_NAME, header.int_size, use_tempfile=True),
            on_close="Copying dataset provenance into zip archive.",
            is_progress_log=True,
        )
        self.graph.address_writer.addresses[DATASET_ADDRESS_INDEX] = self.datasets.addresses
        self.quanta = comms.enter(
            MultiblockWriter.open_in_zip(self.graph.zf, QUANTUM_MB_NAME, header.int_size, use_tempfile=True),
            on_close="Copying quantum provenance into zip archive.",
            is_progress_log=True,
        )
        self.graph.address_writer.addresses[QUANTUM_ADDRESS_INDEX] = self.quanta.addresses

    @property
    def compressor(self) -> Compressor:
        return self.graph.compressor


@dataclasses.dataclass
class Writer:
    comms: WriterCommunicator
    existing_init_outputs: dict[uuid.UUID, set[uuid.UUID]] = dataclasses.field(default_factory=dict)
    indices: dict[uuid.UUID, int] = dataclasses.field(default_factory=dict)
    output_dataset_ids: set[uuid.UUID] = dataclasses.field(default_factory=set)
    xgraph: networkx.DiGraph = dataclasses.field(default_factory=networkx.DiGraph)
    pending_compression_training: list[_ScanData] = dataclasses.field(default_factory=list)

    def __post_init__(self) -> None:
        assert self.comms.config.output_path is not None, "Writer should not be used if writing is disabled."
        self.comms.log.info("Reading predicted quantum graph.")
        self._predicted_reader = self.comms.enter(
            PredictedQuantumGraphReader.open(
                self.comms.config.predicted_path, import_mode=TaskImportMode.DO_NOT_IMPORT
            )
        )
        self._predicted_reader.read_init_quanta()
        self._predicted_reader.read_quantum_datasets()
        for predicted_init_quantum in self._predicted_reader.components.init_quanta.root:
            self.existing_init_outputs[predicted_init_quantum.quantum_id] = set()
        self.comms.log.info("Generating integer indexes and identifying outputs.")
        self._populate_indices_and_outputs()
        self._populate_xgraph()

    def _populate_indices_and_outputs(self) -> None:
        all_uuids = set(self._predicted_reader.components.quantum_indices.keys())
        for quantum in itertools.chain(
            self._predicted_reader.components.init_quanta.root,
            self._predicted_reader.components.quantum_datasets.values(),
        ):
            if not quantum.task_label:
                # Skip the 'packages' producer quantum.
                continue
            all_uuids.update(quantum.iter_input_dataset_ids())
            self.output_dataset_ids.update(quantum.iter_output_dataset_ids())
        all_uuids.update(self.output_dataset_ids)
        self.indices = {
            node_id: node_index
            for node_index, node_id in enumerate(sorted(all_uuids, key=operator.attrgetter("int")))
        }

    def _populate_xgraph(self) -> None:
        for predicted_quantum in itertools.chain(
            self._predicted_reader.components.init_quanta.root,
            self._predicted_reader.components.quantum_datasets.values(),
        ):
            if not predicted_quantum.task_label:
                # Skip the 'packages' producer quantum.
                continue
            quantum_index = self.indices[predicted_quantum.quantum_id]
            for predicted_input in itertools.chain.from_iterable(predicted_quantum.inputs.values()):
                self.xgraph.add_edge(self.indices[predicted_input.dataset_id], quantum_index)
            for predicted_output in itertools.chain.from_iterable(predicted_quantum.outputs.values()):
                self.xgraph.add_edge(quantum_index, self.indices[predicted_output.dataset_id])

    @staticmethod
    def run(comms: WriterCommunicator) -> None:
        with comms:
            writer = Writer(comms)
            writer.loop()

    def loop(self) -> None:
        data_writers: DataWriters | None = None
        if not self.comms.config.zstd_dict_size:
            data_writers = self.make_data_writers()
        self.comms.log.info("Polling for write requests from scanners.")
        for request in self.comms.poll():
            if data_writers is None:
                self.pending_compression_training.extend(self.make_scan_data(request))
                if len(self.pending_compression_training) >= self.comms.config.zstd_dict_n_inputs:
                    data_writers = self.make_data_writers()
            else:
                for scan_data in self.make_scan_data(request):
                    self.write_scan_data(scan_data, data_writers)
        if data_writers is None:
            data_writers = self.make_data_writers()
        self.write_init_outputs(data_writers)

    def make_data_writers(self) -> DataWriters:
        cdict = self.make_compression_dictionary()
        self.comms.send_compression_dict(cdict.as_bytes())
        assert self.comms.config.output_path is not None
        self.comms.log.info("Opening output files.")
        data_writers = DataWriters(
            self.comms,
            self._predicted_reader,
            self.indices,
            compressor=zstandard.ZstdCompressor(self.comms.config.zstd_level, cdict),
            cdict_data=cdict.as_bytes(),
        )
        self.comms.log.info("Compressing and writing queued scan requests.")
        for scan_data in self.pending_compression_training:
            self.write_scan_data(scan_data, data_writers)
        self.write_overall_inputs(data_writers)
        self.write_packages(data_writers)
        return data_writers

    def make_compression_dictionary(self) -> zstandard.ZstdCompressionDict:
        if not self.comms.config.zstd_dict_size:
            self.comms.log.info("Making compressor with no dictionary.")
            return zstandard.ZstdCompressionDict(b"")
        self.comms.log.info("Training compression dictionary.")
        training_inputs: list[bytes] = []
        # We start the dictionary training with *predicted* quantum dataset
        # models, since those have almost all of the same attributes as the
        # provenance quantum and dataset models, and we can get a nice random
        # sample from just the first N, since they're ordered by UUID.  We
        # chop out the datastore records since those don't appear in the
        # provenance graph.
        for predicted_quantum in self._predicted_reader.components.quantum_datasets.values():
            if len(training_inputs) == self.comms.config.zstd_dict_n_inputs:
                break
            predicted_quantum.datastore_records.clear()
            training_inputs.append(predicted_quantum.model_dump_json().encode())
        # Add the provenance quanta, metadata, and logs we've accumulated.
        for scan_data in self.pending_compression_training:
            assert scan_data.compression is CompressionState.NOT_COMPRESSED
            training_inputs.append(scan_data.quantum)
            training_inputs.append(scan_data.metadata)
            training_inputs.append(scan_data.log)
        return zstandard.train_dictionary(self.comms.config.zstd_dict_size, training_inputs)

    def write_init_outputs(self, data_writers: DataWriters) -> None:
        self.comms.log.info("Writing init outputs.")
        init_quanta = ProvenanceInitQuantaModel()
        for predicted_init_quantum in self._predicted_reader.components.init_quanta.root:
            if not predicted_init_quantum.task_label:
                # Skip the 'packages' producer quantum.
                continue
            existing_outputs = self.existing_init_outputs[predicted_init_quantum.quantum_id]
            for predicted_output in itertools.chain.from_iterable(predicted_init_quantum.outputs.values()):
                dataset_index = self.indices[predicted_output.dataset_id]
                provenance_output = ProvenanceDatasetModel.from_predicted(
                    predicted_output,
                    producer=self.indices[predicted_init_quantum.quantum_id],
                    consumers=self.xgraph.successors(dataset_index),
                )
                provenance_output.exists = predicted_output.dataset_id in existing_outputs
                data_writers.datasets.write_model(
                    provenance_output.dataset_id, provenance_output, data_writers.compressor
                )
            init_quanta.root.append(
                ProvenanceInitQuantumModel.from_predicted(predicted_init_quantum, self.indices)
            )
        data_writers.graph.write_single_model("init_quanta", init_quanta)

    def write_overall_inputs(self, data_writers: DataWriters) -> None:
        self.comms.log.info("Writing overall inputs.")
        for predicted_quantum in itertools.chain(
            self._predicted_reader.components.init_quanta.root,
            self._predicted_reader.components.quantum_datasets.values(),
        ):
            for predicted_input in itertools.chain.from_iterable(predicted_quantum.inputs.values()):
                if predicted_input.dataset_id not in self.output_dataset_ids:
                    if predicted_input.dataset_id not in data_writers.datasets.addresses:
                        dataset_index = self.indices[predicted_input.dataset_id]
                        data_writers.datasets.write_model(
                            predicted_input.dataset_id,
                            ProvenanceDatasetModel.from_predicted(
                                predicted_input,
                                producer=None,
                                consumers=self.xgraph.successors(dataset_index),
                            ),
                            data_writers.compressor,
                        )

    def write_packages(self, data_writers: DataWriters) -> None:
        packages = Packages.fromSystem(include_all=True)
        data = packages.toBytes("json")
        data_writers.graph.write_single_block("packages", data)

    def make_scan_data(self, request: ScanResult) -> list[_ScanData]:
        if (existing_init_outputs := self.existing_init_outputs.get(request.quantum_id)) is not None:
            self.comms.log.debug("Handling init-output scan for %s.", request.quantum_id)
            existing_init_outputs.update(request.existing_outputs)
            self.comms.report_write()
            return []
        self.comms.log.debug("Handling quantum scan for %s.", request.quantum_id)
        predicted_quantum = self._predicted_reader.components.quantum_datasets[request.quantum_id]
        quantum_index = self.indices[predicted_quantum.quantum_id]
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
            dataset_index = self.indices[predicted_output.dataset_id]
            provenance_output = ProvenanceDatasetModel.from_predicted(
                predicted_output,
                producer=quantum_index,
                consumers=self.xgraph.successors(dataset_index),
            )
            provenance_output.exists = provenance_output.dataset_id in request.existing_outputs
            data.datasets[provenance_output.dataset_id] = provenance_output.model_dump_json().encode()
        provenance_quantum = ProvenanceQuantumModel.from_predicted(predicted_quantum, self.indices)
        provenance_quantum.status = request.get_run_status()
        provenance_quantum.caveats = request.caveats
        provenance_quantum.exception = request.exception
        provenance_quantum.resource_usage = request.resource_usage
        data.quantum = provenance_quantum.model_dump_json().encode()
        data.metadata = request.metadata
        data.log = request.log
        return [data]

    def write_scan_data(self, scan_data: _ScanData, data_writers: DataWriters) -> None:
        self.comms.log.debug("Writing quantum %s.", scan_data.quantum_id)
        scan_data.compress(data_writers.compressor)
        data_writers.quanta.write_bytes(scan_data.quantum_id, scan_data.quantum)
        for dataset_id, dataset_data in scan_data.datasets.items():
            data_writers.datasets.write_bytes(dataset_id, dataset_data)
        if scan_data.metadata:
            address = data_writers.metadata.write_bytes(scan_data.quantum_id, scan_data.metadata)
            data_writers.metadata.addresses[scan_data.metadata_id] = address
        if scan_data.log:
            address = data_writers.logs.write_bytes(scan_data.quantum_id, scan_data.log)
            data_writers.logs.addresses[scan_data.log_id] = address
        self.comms.report_write()
