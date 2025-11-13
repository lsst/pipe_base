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
import itertools
import logging
import operator
import uuid
from typing import TypeVar

import networkx
import zstandard

from lsst.utils.packages import Packages

from ... import automatic_connection_constants as acc
from ...pipeline_graph import TaskImportMode
from .._common import BaseQuantumGraphWriter
from .._multiblock import Compressor, MultiblockWriter
from .._predicted import PredictedDatasetModel, PredictedQuantumGraphComponents, PredictedQuantumGraphReader
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
from ._structs import WriteRequest


@dataclasses.dataclass
class _DataWriters:
    """A struct of low-level writer objects for the main components of a
    provenance quantum graph.

    Parameters
    ----------
    comms : `WriterCommunicator`
        Communicator helper object for the writer.
    predicted : `.PredictedQuantumGraphComponents`
        Components of the predicted graph.
    indices : `dict` [ `uuid.UUID`, `int` ]
        Mapping from UUID to internal integer ID, including both quanta and
        datasets.
    compressor : `Compressor`
        Object that can compress `bytes`.
    cdict_data : `bytes` or `None`, optional
        Bytes representation of the compression dictionary used by the
        compressor.
    """

    def __init__(
        self,
        comms: WriterCommunicator,
        predicted: PredictedQuantumGraphComponents,
        indices: dict[uuid.UUID, int],
        compressor: Compressor,
        cdict_data: bytes | None = None,
    ) -> None:
        assert comms.config.output_path is not None
        header = predicted.header.model_copy()
        header.graph_type = "provenance"
        self.graph = comms.enter(
            BaseQuantumGraphWriter.open(
                comms.config.output_path,
                header,
                predicted.pipeline_graph,
                indices,
                address_filename="nodes",
                compressor=compressor,
                cdict_data=cdict_data,
            ),
            on_close="Finishing writing provenance quantum graph.",
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

    graph: BaseQuantumGraphWriter
    """The parent graph writer."""

    datasets: MultiblockWriter
    """A writer for dataset provenance."""

    quanta: MultiblockWriter
    """A writer for quantum provenance."""

    metadata: MultiblockWriter
    """A writer for metadata content."""

    logs: MultiblockWriter
    """A writer for log content."""

    @property
    def compressor(self) -> Compressor:
        """Object that should be used to compress all JSON blocks."""
        return self.graph.compressor


@dataclasses.dataclass
class Writer:
    """A helper class for the provenance aggregator actually writes the
    provenance quantum graph file.
    """

    predicted_path: str
    """Path to the predicted quantum graph."""

    comms: WriterCommunicator
    """Communicator object for this worker."""

    predicted: PredictedQuantumGraphComponents = dataclasses.field(init=False)
    """Components of the predicted quantum graph."""

    existing_init_outputs: dict[uuid.UUID, set[uuid.UUID]] = dataclasses.field(default_factory=dict)
    """Mapping that tracks which init-outputs exist.

    This mapping is updated as scanners inform the writer about init-output
    existence, since we want to write that provenance information out only at
    the end.
    """

    indices: dict[uuid.UUID, int] = dataclasses.field(default_factory=dict)
    """Mapping from UUID to internal integer ID, including both quanta and
    datasets.

    This is fully initialized at construction.
    """

    output_dataset_ids: set[uuid.UUID] = dataclasses.field(default_factory=set)
    """The IDs of all datasets that are produced by this graph.

    This is fully initialized at construction.
    """

    overall_inputs: dict[uuid.UUID, PredictedDatasetModel] = dataclasses.field(default_factory=dict)
    """All datasets that are not produced by any quantum in this graph."""

    xgraph: networkx.DiGraph = dataclasses.field(default_factory=networkx.DiGraph)
    """A bipartite NetworkX graph linking datasets to quanta and quanta to
    datasets.

    This is fully initialized at construction.  There are no node or edge
    attributes in this graph; we only need it to store adjacency information
    with datasets as well as with quanta.
    """

    pending_compression_training: list[WriteRequest] = dataclasses.field(default_factory=list)
    """Unprocessed quantum scans that are being accumulated in order to
    build a compression dictionary.
    """

    def __post_init__(self) -> None:
        assert self.comms.config.output_path is not None, "Writer should not be used if writing is disabled."
        self.comms.log.info("Reading predicted quantum graph.")
        with PredictedQuantumGraphReader.open(
            self.predicted_path, import_mode=TaskImportMode.DO_NOT_IMPORT
        ) as reader:
            self.comms.check_for_cancel()
            reader.read_init_quanta()
            self.comms.check_for_cancel()
            reader.read_quantum_datasets()
            self.predicted = reader.components
        for predicted_init_quantum in self.predicted.init_quanta.root:
            self.existing_init_outputs[predicted_init_quantum.quantum_id] = set()
        self.comms.check_for_cancel()
        self.comms.log.info("Generating integer indexes and identifying outputs.")
        self._populate_indices_and_outputs()
        self.comms.check_for_cancel()
        self._populate_xgraph_and_inputs()
        self.comms.check_for_cancel()
        self.comms.log_progress(
            # We add one here for 'packages', which we do ingest but don't
            # record provenance for.
            logging.INFO,
            f"Graph has {len(self.output_dataset_ids) + 1} predicted output dataset(s).",
        )

    def _populate_indices_and_outputs(self) -> None:
        all_uuids = set(self.predicted.quantum_datasets.keys())
        for quantum in self.comms.periodically_check_for_cancel(
            itertools.chain(
                self.predicted.init_quanta.root,
                self.predicted.quantum_datasets.values(),
            )
        ):
            if not quantum.task_label:
                # Skip the 'packages' producer quantum.
                continue
            all_uuids.update(quantum.iter_input_dataset_ids())
            self.output_dataset_ids.update(quantum.iter_output_dataset_ids())
        all_uuids.update(self.output_dataset_ids)
        self.indices = {
            node_id: node_index
            for node_index, node_id in self.comms.periodically_check_for_cancel(
                enumerate(sorted(all_uuids, key=operator.attrgetter("int")))
            )
        }

    def _populate_xgraph_and_inputs(self) -> None:
        for predicted_quantum in self.comms.periodically_check_for_cancel(
            itertools.chain(
                self.predicted.init_quanta.root,
                self.predicted.quantum_datasets.values(),
            )
        ):
            if not predicted_quantum.task_label:
                # Skip the 'packages' producer quantum.
                continue
            for predicted_input in itertools.chain.from_iterable(predicted_quantum.inputs.values()):
                self.xgraph.add_edge(predicted_input.dataset_id, predicted_quantum.quantum_id)
                if predicted_input.dataset_id not in self.output_dataset_ids:
                    self.overall_inputs.setdefault(predicted_input.dataset_id, predicted_input)
            for predicted_output in itertools.chain.from_iterable(predicted_quantum.outputs.values()):
                self.xgraph.add_edge(predicted_quantum.quantum_id, predicted_output.dataset_id)

    @staticmethod
    def run(predicted_path: str, comms: WriterCommunicator) -> None:
        """Run the writer.

        Parameters
        ----------
        predicted_path : `str`
            Path to the predicted quantum graph.
        comms : `WriterCommunicator`
            Communicator for the writer.

        Notes
        -----
        This method is designed to run as the ``target`` in
        `WorkerContext.make_worker`.
        """
        with comms:
            writer = Writer(predicted_path, comms)
            writer.loop()

    def loop(self) -> None:
        """Run the main loop for the writer."""
        data_writers: _DataWriters | None = None
        if not self.comms.config.zstd_dict_size:
            data_writers = self.make_data_writers()
        self.comms.log.info("Polling for write requests from scanners.")
        for request in self.comms.poll():
            if data_writers is None:
                self.pending_compression_training.append(request)
                if len(self.pending_compression_training) >= self.comms.config.zstd_dict_n_inputs:
                    data_writers = self.make_data_writers()
            else:
                self.process_request(request, data_writers)
        if data_writers is None:
            data_writers = self.make_data_writers()
        self.write_init_outputs(data_writers)

    def make_data_writers(self) -> _DataWriters:
        """Make a compression dictionary, open the low-level writers, and
        write any accumulated scans that were needed to make the compression
        dictionary.

        Returns
        -------
        data_writers : `_DataWriters`
            Low-level writers struct.
        """
        cdict = self.make_compression_dictionary()
        self.comms.send_compression_dict(cdict.as_bytes())
        assert self.comms.config.output_path is not None
        self.comms.log.info("Opening output files.")
        data_writers = _DataWriters(
            self.comms,
            self.predicted,
            self.indices,
            compressor=zstandard.ZstdCompressor(self.comms.config.zstd_level, cdict),
            cdict_data=cdict.as_bytes(),
        )
        self.comms.check_for_cancel()
        self.comms.log.info("Compressing and writing queued scan requests.")
        for request in self.pending_compression_training:
            self.process_request(request, data_writers)
        del self.pending_compression_training
        self.comms.check_for_cancel()
        self.write_overall_inputs(data_writers)
        self.write_packages(data_writers)
        self.comms.log.info("Returning to write request loop.")
        return data_writers

    def make_compression_dictionary(self) -> zstandard.ZstdCompressionDict:
        """Make the compression dictionary.

        Returns
        -------
        cdict : `zstandard.ZstdCompressionDict`
            The compression dictionary.
        """
        if (
            not self.comms.config.zstd_dict_size
            or len(self.pending_compression_training) < self.comms.config.zstd_dict_n_inputs
        ):
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
        for predicted_quantum in self.predicted.quantum_datasets.values():
            if len(training_inputs) == self.comms.config.zstd_dict_n_inputs:
                break
            predicted_quantum.datastore_records.clear()
            training_inputs.append(predicted_quantum.model_dump_json().encode())
        # Add the provenance quanta, metadata, and logs we've accumulated.
        for write_request in self.pending_compression_training:
            assert not write_request.is_compressed, "We can't compress without the compression dictionary."
            training_inputs.append(write_request.quantum)
            training_inputs.append(write_request.metadata)
            training_inputs.append(write_request.logs)
        return zstandard.train_dictionary(self.comms.config.zstd_dict_size, training_inputs)

    def write_init_outputs(self, data_writers: _DataWriters) -> None:
        """Write provenance for init-output datasets and init-quanta.

        Parameters
        ----------
        data_writers : `_DataWriters`
            Low-level writers struct.
        """
        self.comms.log.info("Writing init outputs.")
        init_quanta = ProvenanceInitQuantaModel()
        for predicted_init_quantum in self.predicted.init_quanta.root:
            if not predicted_init_quantum.task_label:
                # Skip the 'packages' producer quantum.
                continue
            existing_outputs = self.existing_init_outputs[predicted_init_quantum.quantum_id]
            for predicted_output in itertools.chain.from_iterable(predicted_init_quantum.outputs.values()):
                provenance_output = ProvenanceDatasetModel.from_predicted(
                    predicted_output,
                    producer=predicted_init_quantum.quantum_id,
                    consumers=self.xgraph.successors(predicted_output.dataset_id),
                )
                provenance_output.produced = predicted_output.dataset_id in existing_outputs
                data_writers.datasets.write_model(
                    provenance_output.dataset_id, provenance_output, data_writers.compressor
                )
            init_quanta.root.append(ProvenanceInitQuantumModel.from_predicted(predicted_init_quantum))
        data_writers.graph.write_single_model("init_quanta", init_quanta)

    def write_overall_inputs(self, data_writers: _DataWriters) -> None:
        """Write provenance for overall-input datasets.

        Parameters
        ----------
        data_writers : `_DataWriters`
            Low-level writers struct.
        """
        self.comms.log.info("Writing overall inputs.")
        for predicted_input in self.comms.periodically_check_for_cancel(self.overall_inputs.values()):
            if predicted_input.dataset_id not in data_writers.datasets.addresses:
                data_writers.datasets.write_model(
                    predicted_input.dataset_id,
                    ProvenanceDatasetModel.from_predicted(
                        predicted_input,
                        producer=None,
                        consumers=self.xgraph.successors(predicted_input.dataset_id),
                    ),
                    data_writers.compressor,
                )
        del self.overall_inputs

    @staticmethod
    def write_packages(data_writers: _DataWriters) -> None:
        """Write package version information to the provenance graph.

        Parameters
        ----------
        data_writers : `_DataWriters`
            Low-level writers struct.
        """
        packages = Packages.fromSystem(include_all=True)
        data = packages.toBytes("json")
        data_writers.graph.write_single_block("packages", data)

    def process_request(self, request: WriteRequest, data_writers: _DataWriters) -> None:
        """Process a `WriteRequest` into `_ScanData`.

        Parameters
        ----------
        request : `WriteRequest`
            Result of a quantum scan.
        data_writers : `_DataWriters`
            Low-level writers struct.
        """
        if (existing_init_outputs := self.existing_init_outputs.get(request.quantum_id)) is not None:
            self.comms.log.debug("Handling init-output scan for %s.", request.quantum_id)
            existing_init_outputs.update(request.existing_outputs)
            self.comms.report_write()
            return
        self.comms.log.debug("Handling quantum scan for %s.", request.quantum_id)
        predicted_quantum = self.predicted.quantum_datasets[request.quantum_id]
        outputs: dict[uuid.UUID, bytes] = {}
        for predicted_output in itertools.chain.from_iterable(predicted_quantum.outputs.values()):
            provenance_output = ProvenanceDatasetModel.from_predicted(
                predicted_output,
                producer=predicted_quantum.quantum_id,
                consumers=self.xgraph.successors(predicted_output.dataset_id),
            )
            provenance_output.produced = provenance_output.dataset_id in request.existing_outputs
            outputs[provenance_output.dataset_id] = data_writers.compressor.compress(
                provenance_output.model_dump_json().encode()
            )
        if not request.quantum:
            request.quantum = (
                ProvenanceQuantumModel.from_predicted(predicted_quantum).model_dump_json().encode()
            )
            if request.is_compressed:
                request.quantum = data_writers.compressor.compress(request.quantum)
        if not request.is_compressed:
            request.quantum = data_writers.compressor.compress(request.quantum)
            if request.metadata:
                request.metadata = data_writers.compressor.compress(request.metadata)
            if request.logs:
                request.logs = data_writers.compressor.compress(request.logs)
        self.comms.log.debug("Writing quantum %s.", request.quantum_id)
        data_writers.quanta.write_bytes(request.quantum_id, request.quantum)
        for dataset_id, dataset_data in outputs.items():
            data_writers.datasets.write_bytes(dataset_id, dataset_data)
        if request.metadata:
            (metadata_output,) = predicted_quantum.outputs[acc.METADATA_OUTPUT_CONNECTION_NAME]
            address = data_writers.metadata.write_bytes(request.quantum_id, request.metadata)
            data_writers.metadata.addresses[metadata_output.dataset_id] = address
        if request.logs:
            (log_output,) = predicted_quantum.outputs[acc.LOG_OUTPUT_CONNECTION_NAME]
            address = data_writers.logs.write_bytes(request.quantum_id, request.logs)
            data_writers.logs.addresses[log_output.dataset_id] = address
        # We shouldn't need this predicted quantum anymore; delete it in the
        # hopes that'll free up some memory.
        del self.predicted.quantum_datasets[request.quantum_id]
        self.comms.report_write()


_T = TypeVar("_T")
